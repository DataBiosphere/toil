# Modified from: https://github.com/common-workflow-language/workflow-service
import functools
import json
import logging
import os
from abc import abstractmethod
from typing import Any, Callable, Optional, Union
from urllib.parse import urldefrag

import connexion  # type: ignore
from werkzeug.utils import secure_filename

from toil.lib.io import mkdtemp

logger = logging.getLogger(__name__)

# Define a type for WES task log entries in responses
# TODO: make this a typed dict with all the WES task log field names and their types.
TaskLog = dict[str, Union[str, int, None]]


class VersionNotImplementedException(Exception):
    """
    Raised when the requested workflow version is not implemented.
    """

    def __init__(
        self,
        wf_type: str,
        version: Optional[str] = None,
        supported_versions: Optional[list[str]] = None,
    ) -> None:
        if version:
            message = (
                "workflow_type '{}' requires 'workflow_type_version' to be one of '{}'.  "
                "Got '{}' instead.".format(wf_type, str(supported_versions), version)
            )
        else:
            message = f"workflow_type '{wf_type}' is not supported."

        super().__init__(message)


class MalformedRequestException(Exception):
    """
    Raised when the request is malformed.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class WorkflowNotFoundException(Exception):
    """
    Raised when the requested run ID is not found.
    """

    def __init__(self) -> None:
        super().__init__("The requested workflow run wasn't found.")


class WorkflowConflictException(Exception):
    """
    Raised when the requested workflow is not in the expected state.
    """

    def __init__(self, run_id: str):
        super().__init__(f"Workflow {run_id} exists when it shouldn't.")


class OperationForbidden(Exception):
    """
    Raised when the request is forbidden.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class WorkflowExecutionException(Exception):
    """
    Raised when an internal error occurred during the execution of the workflow.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


def handle_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    This decorator catches errors from the wrapped function and returns a JSON
    formatted error message with the appropriate status code defined by the
    GA4GH WES spec.
    """

    def error(msg: Any, code: int = 500) -> tuple[dict[str, Any], int]:
        logger.warning(
            f"Exception raised when calling '{func.__name__}()':", exc_info=True
        )
        return {"msg": str(msg), "status_code": code}, code

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except MalformedRequestException as e:
            return error(e, code=400)
        except VersionNotImplementedException as e:
            return error(e, code=400)
        except OperationForbidden as e:
            return error(e, code=403)
        except (FileNotFoundError, WorkflowNotFoundException) as e:
            return error(e, code=404)
        except WorkflowConflictException as e:
            return error(e, code=400)
        except WorkflowExecutionException as e:
            return error(e, code=500)
        except Exception as e:
            return error(e, code=500)

    return wrapper


class WESBackend:
    """
    A class to represent a GA4GH Workflow Execution Service (WES) API backend.
    Intended to be inherited. Subclasses should implement all abstract methods
    to handle user requests when they hit different endpoints.
    """

    def __init__(self, options: list[str]):
        """
        :param options: A list of default engine options to use when executing
                        a workflow.  Example options:
                        ["--logLevel=CRITICAL","--workDir=/path/to/dir",
                            "--tag=Name=default", "--tag=Owner=shared", ...]
        """
        self.options = options or []

    def resolve_operation_id(self, operation_id: str) -> Any:
        """
        Map an operationId defined in the OpenAPI or swagger yaml file to a
        function.

        :param operation_id: The operation ID defined in the specification.
        :returns: A function that should be called when the given endpoint is
                  reached.
        """
        return getattr(self, operation_id.split(".")[-1])

    @abstractmethod
    def get_service_info(self) -> dict[str, Any]:
        """
        Get information about the Workflow Execution Service.

        GET /service-info
        """
        raise NotImplementedError

    @abstractmethod
    def list_runs(
        self, page_size: Optional[int] = None, page_token: Optional[str] = None
    ) -> dict[str, Any]:
        """
        List the workflow runs.

        GET /runs
        """
        raise NotImplementedError

    @abstractmethod
    def run_workflow(self) -> dict[str, str]:
        """
        Run a workflow. This endpoint creates a new workflow run and returns
        a `RunId` to monitor its progress.

        POST /runs
        """
        raise NotImplementedError

    @abstractmethod
    def get_run_log(self, run_id: str) -> dict[str, Any]:
        """
        Get detailed info about a workflow run.

        GET /runs/{run_id}
        """
        raise NotImplementedError

    @abstractmethod
    def cancel_run(self, run_id: str) -> dict[str, str]:
        """
        Cancel a running workflow.

        POST /runs/{run_id}/cancel
        """
        raise NotImplementedError

    @abstractmethod
    def get_run_status(self, run_id: str) -> dict[str, str]:
        """
        Get quick status info about a workflow run, returning a simple result
        with the overall state of the workflow run.

        GET /runs/{run_id}/status
        """
        raise NotImplementedError

    @staticmethod
    def log_for_run(run_id: Optional[str], message: str) -> None:
        if run_id:
            logging.info("Workflow %s: %s", run_id, message)
        else:
            logging.info(message)

    @staticmethod
    def secure_path(path: str) -> str:
        return os.path.join(
            *[
                str(secure_filename(p))
                for p in path.split("/")
                if p not in ("", ".", "..")
            ]
        )

    def collect_attachments(
        self, args: dict[str, Any], run_id: Optional[str], temp_dir: Optional[str]
    ) -> tuple[str, dict[str, Any]]:
        """
        Collect attachments from the current request by staging uploaded files
        to temp_dir, and return the temp_dir and parsed body of the request.

        :param run_id: The run ID for logging.
        :param temp_dir: The directory where uploaded files should be staged.
                         If None, a temporary directory is created.
        """
        if not temp_dir:
            temp_dir = mkdtemp()
        body: dict[str, Any] = {}
        has_attachments = False
        for k, v in args.items():
            if k == "workflow_attachment":
                for file in (v or []):
                    dest = os.path.join(temp_dir, self.secure_path(file.filename))
                    if not os.path.isdir(os.path.dirname(dest)):
                        os.makedirs(os.path.dirname(dest))
                    self.log_for_run(
                        run_id,
                        f"Staging attachment '{file.filename}' to '{dest}'",
                    )
                    file.save(dest)
                    has_attachments = True
                    body["workflow_attachment"] = (
                            "file://%s" % temp_dir
                    )  # Reference to temp working dir.
            elif k in ("workflow_params", "tags", "workflow_engine_parameters"):
                if v is not None:
                    body[k] = json.loads(v)
            else:
                body[k] = v
        if "workflow_url" in body:
            url, ref = urldefrag(body["workflow_url"])
            if ":" not in url:
                if not has_attachments:
                    raise MalformedRequestException(
                        "Relative 'workflow_url' but missing 'workflow_attachment'"
                    )
                body["workflow_url"] = self.secure_path(url)  # keep this relative
                if ref:
                    # append "#ref" after the url
                    body["workflow_url"] += "#" + self.secure_path(ref)
            self.log_for_run(
                run_id, "Using workflow_url '%s'" % body.get("workflow_url")
            )
        else:
            raise MalformedRequestException("Missing 'workflow_url' in submission")

        if "workflow_params" in body and not isinstance(body["workflow_params"], dict):
            # They sent us something silly like "workflow_params": "5"
            raise MalformedRequestException(
                "Got a 'workflow_params' which does not decode to a JSON object"
            )

        return temp_dir, body
