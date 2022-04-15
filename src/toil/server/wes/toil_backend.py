# Copyright (C) 2015-2021 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import os
import shutil
import uuid
from collections import Counter
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from typing import Optional, List, Dict, Any, overload, Generator, TextIO, Tuple

from flask import send_from_directory
from flask.globals import request as flask_request
from werkzeug.utils import redirect
from werkzeug.wrappers.response import Response


from toil.server.utils import WorkflowStateMachine, connect_to_workflow_state_store
from toil.server.wes.abstract_backend import (WESBackend,
                                              handle_errors,
                                              WorkflowNotFoundException,
                                              WorkflowConflictException,
                                              VersionNotImplementedException,
                                              WorkflowExecutionException,
                                              OperationForbidden)
from toil.server.wes.tasks import run_wes, cancel_run

from toil.common import getNodeID
from toil.version import baseVersion

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ToilWorkflow:
    def __init__(self, base_scratch_dir: str, state_store_url: str, run_id: str, ):
        """
        Class to represent a Toil workflow. This class is responsible for
        launching workflow runs via Celery and retrieving data generated from
        them.

        :param base_scratch_dir: The directory where workflows keep their
                                 output/scratch directories under their run
                                 IDs.

        :param state_store_url: URL or file path at which we communicate with
                                running workflows.

        :param run_id: A uuid string.  Used to name the folder that contains
                       all of the files containing this particular workflow
                       instance's information.
        """
        self.base_scratch_dir = base_scratch_dir
        self.state_store_url = state_store_url
        self.run_id = run_id

        self.scratch_dir = os.path.join(self.base_scratch_dir, self.run_id)
        self.exec_dir = os.path.join(self.scratch_dir, "execution")

        # TODO: share a base class with ToilWorkflowRunner for some of this stuff?

        # Connect to the workflow state store
        self.store = connect_to_workflow_state_store(state_store_url, self.run_id)
        # And use a state machine over that to look at workflow state
        self.state_machine = WorkflowStateMachine(self.store)

        # Do a little fixup of workflows that were queued from other machines.
        # We assume that only one server can be running on a state store at a
        # time, so anything another server queued must be old and dead.
        # TODO: Implement multiple servers working together.
        queueing_node = self.store.get("server_node")
        apparent_state = self.state_machine.get_current_state()
        if (apparent_state not in ("UNKNOWN", "COMPLETE", "EXECUTOR_ERROR", "SYSTEM_ERROR", "CANCELED") and
            queueing_node != getNodeID()):

            # This workflow is in a state that suggests it is doing something
            # but the server node for it is gone and therefore so is its
            # leader. Fail it.
            self.state_machine.send_system_error()


    @overload
    def fetch_state(self, key: str, default: str) -> str: ...
    @overload
    def fetch_state(self, key: str, default: None = None) -> Optional[str]: ...

    def fetch_state(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Return the contents of the given key in the workflow's state
        store. If the key does not exist, the default value is returned.
        """
        value = self.store.get(key)
        if value is None:
            return default
        return value

    @contextmanager
    def fetch_scratch(self, filename: str) -> Generator[Optional[TextIO], None, None]:
        """
        Get a context manager for either a stream for the given file from the
        workflow's scratch directory, or None if it isn't there.
        """
        if os.path.exists(os.path.join(self.scratch_dir, filename)):
            with open(os.path.join(self.scratch_dir, filename), "r") as f:
                yield f
        else:
            yield None

    def exists(self) -> bool:
        """ Return True if the workflow run exists."""
        return self.get_state() != "UNKNOWN"

    def get_state(self) -> str:
        """ Return the state of the current run."""
        return self.state_machine.get_current_state()

    def set_up_run(self) -> None:
        """ Set up necessary directories for the run."""
        # Remember the server node responsible for the workflow
        self.store.set("server_node", getNodeID())

        # Go to queued state
        self.state_machine.send_enqueue()

        # Make sure scratch and exec directories exist
        os.makedirs(self.exec_dir, exist_ok=True)

    def clean_up(self) -> None:
        """ Clean directory and files related to the run."""
        shutil.rmtree(self.scratch_dir)
        # Don't remove state; state needs to persist forever.

    def queue_run(self, request: Dict[str, Any], options: List[str]) -> None:
        """This workflow should be ready to run. Hand this to Celery."""
        with open(os.path.join(self.scratch_dir, "request.json"), "w") as f:
            # Save the request to disk for get_run_log()
            json.dump(request, f)

        try:
            run_wes.apply_async(args=(self.base_scratch_dir, self.state_store_url, self.run_id, request, options),
                                task_id=self.run_id,  # set the Celery task ID the same as our run ID
                                ignore_result=True)
        except Exception:
            # Celery or the broker might be down
            self.state_machine.send_system_error()
            raise WorkflowExecutionException(f"Failed to run: internal server error.")

    def get_output_files(self) -> Any:
        """
        Return a collection of output files that this workflow generated.
        """
        with self.fetch_scratch("outputs.json") as f:
            if f is None:
                # No file is there
                return {}
            else:
                # Stream in the file
                return json.load(f)

class ToilBackend(WESBackend):
    """
    WES backend implemented for Toil to run CWL, WDL, or Toil workflows. This
    class is responsible for validating and executing submitted workflows.

    Single machine implementation -
    Use Celery as the task queue and interact with the "workflows/" directory
    in the filesystem to store and retrieve data associated with the runs.
    """

    def __init__(self, work_dir: str, options: List[str], dest_bucket_base: Optional[str]) -> None:
        """
        Make a new ToilBackend for serving WES.

        :param work_dir: Directory to download and run workflows in.

        :param options: Command-line options to pass along to workflows. Must
        use = syntax to set values instead of ordering.

        :param dest_bucket_base: If specified, direct CWL workflows to use
        paths under the given URL for storing output files.
        """
        for opt in options:
            if not opt.startswith('-'):
                # We don't allow a value to be set across multiple arguments
                # that would need to remain in the same order.
                raise ValueError(f'Option {opt} does not begin with -')
        super(ToilBackend, self).__init__(options)
        self.work_dir = os.path.abspath(work_dir)
        self.dest_bucket_base = dest_bucket_base

        # Where should we talk to the Celery tasks about workflow state?
        # For now just do it in a dedicated directory of the work_dir (so we don't forget workflows when we clean them up).
        self.state_store_url = os.path.join(self.work_dir, 'state_store')

        self.supported_versions = {
            "py": ["3.6", "3.7", "3.8", "3.9"],
            "cwl": ["v1.0", "v1.1", "v1.2"],
            "wdl": ["draft-2", "1.0"]
        }

    def _get_run(self, run_id: str, should_exists: Optional[bool] = None) -> ToilWorkflow:
        """
        Helper method to instantiate a ToilWorkflow object.

        :param run_id: The run ID.
        :param should_exists: If set, ensures that the workflow run exists (or
                              does not exist) according to the value.
        """
        run = ToilWorkflow(self.work_dir, self.state_store_url, run_id)

        if should_exists and not run.exists():
            raise WorkflowNotFoundException
        if should_exists is False and run.exists():
            raise WorkflowConflictException(run_id)

        return run

    def get_runs(self) -> Generator[Tuple[str, str], None, None]:
        """ A generator of a list of run ids and their state."""
        if not os.path.exists(self.work_dir):
            return

        for run_id in os.listdir(self.work_dir):
            run = self._get_run(run_id)
            if run.exists():
                yield run_id, run.get_state()

    def get_state(self, run_id: str) -> str:
        """
        Return the state of the workflow run with the given run ID. May raise
        an error if the workflow does not exist.
        """
        return self._get_run(run_id, should_exists=True).get_state()

    @handle_errors
    def get_service_info(self) -> Dict[str, Any]:
        """ Get information about the Workflow Execution Service."""

        state_counts = Counter(state for _, state in self.get_runs())

        engine_parameters = []
        for option in self.options:
            if '=' not in option:  # flags like "--logDebug"
                k, v = option, None
            else:
                k, v = option.split('=', 1)
            engine_parameters.append((k, v))

        return {
            "version": baseVersion,
            "workflow_type_versions": {
                k: {
                    "workflow_type_version": v
                } for k, v in self.supported_versions.items()
            },
            "supported_wes_versions": ["1.0.0"],
            "supported_filesystem_protocols": ["file", "http", "https"],
            "workflow_engine_versions": {"toil": baseVersion},
            # TODO: How can we report --destBucket here, since we pass it only
            # for CWL workflows?
            "default_workflow_engine_parameters": [
                {
                    "name": key,
                    "default_value": value
                }
                for key, value in engine_parameters
            ],
            "system_state_counts": state_counts,
            "tags": {},
        }

    @handle_errors
    def list_runs(self, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Dict[str, Any]:
        """ List the workflow runs."""
        # TODO: implement pagination
        return {
            "workflows": [
                {
                    "run_id": run_id,
                    "state": state
                } for run_id, state in self.get_runs()
            ],
            "next_page_token": ""
        }

    @handle_errors
    def run_workflow(self) -> Dict[str, str]:
        """ Run a workflow."""
        run_id = uuid.uuid4().hex
        run = self._get_run(run_id, should_exists=False)

        # set up necessary directories for the run
        run.set_up_run()

        # stage the uploaded files to the execution directory, so that we can run the workflow file directly
        temp_dir = run.exec_dir
        try:
            _, request = self.collect_attachments(run_id, temp_dir=temp_dir)
        except ValueError:
            run.clean_up()
            raise

        logger.info("Received workflow run request %s with parameters: %s", run_id, list(request.keys()))

        wf_type = request["workflow_type"].lower().strip()
        version = request["workflow_type_version"]

        # validate workflow request
        supported_versions = self.supported_versions.get(wf_type, None)
        if not supported_versions:
            run.clean_up()
            raise VersionNotImplementedException(wf_type)
        if version not in supported_versions:
            run.clean_up()
            raise VersionNotImplementedException(wf_type, version, supported_versions)

        # Generate workflow options
        workflow_options = list(self.options)
        if wf_type == "cwl" and self.dest_bucket_base:
            # Output to a directory under out base destination bucket URL.
            workflow_options.append('--destBucket=' + os.path.join(self.dest_bucket_base, run_id))

        logger.info(f"Putting workflow {run_id} into the queue. Waiting to be picked up...")
        run.queue_run(request, options=workflow_options)

        return {
            "run_id": run_id
        }

    @handle_errors
    def get_run_log(self, run_id: str) -> Dict[str, Any]:
        """ Get detailed info about a workflow run."""
        run = self._get_run(run_id, should_exists=True)
        state = run.get_state()

        with run.fetch_scratch("request.json") as f:
            if f is None:
                request = {}
            else:
                request = json.load(f)

        cmd = run.fetch_state("cmd", "").split("\n")
        start_time = run.fetch_state("start_time")
        end_time = run.fetch_state("end_time")

        stdout = ""
        stderr = ""
        if os.path.isfile(os.path.join(run.scratch_dir, 'stdout')):
            # We can't use flask_request.host_url here because that's just the
            # hostname, and we need to work mounted at a proxy hostname *and*
            # path under that hostname. So we need to use a relative URL to the
            # logs.
            stdout = f"../../../../toil/wes/v1/logs/{run_id}/stdout"
            stderr = f"../../../../toil/wes/v1/logs/{run_id}/stderr"

        exit_code = run.fetch_state("exit_code")

        output_obj = {}
        if state == "COMPLETE":
            output_obj = run.get_output_files()

        return {
            "run_id": run_id,
            "request": request,
            "state": state,
            "run_log": {
                "cmd": cmd,
                "start_time": start_time,
                "end_time": end_time,
                "stdout": stdout,
                "stderr": stderr,
                "exit_code": int(exit_code) if exit_code is not None else None,
            },
            "task_logs": [
            ],
            "outputs": output_obj,
        }

    @handle_errors
    def cancel_run(self, run_id: str) -> Dict[str, str]:
        """ Cancel a running workflow."""
        run = self._get_run(run_id, should_exists=True)

        # Do some preflight checks on the current state.
        # We won't catch all cases where the cancel won't go through, but we can catch some.
        state = run.get_state()
        if state in ("CANCELING", "CANCELED", "COMPLETE"):
            # We don't need to do anything.
            logger.warning(f"A user is attempting to cancel a workflow in state: '{state}'.")
        elif state in ("EXECUTOR_ERROR", "SYSTEM_ERROR"):
            # Something went wrong. Let the user know.
            raise OperationForbidden(f"Workflow is in state: '{state}', which cannot be cancelled.")
        else:
            # Go to canceling state if allowed
            run.state_machine.send_cancel()
            # Stop the run in Celery if it is there.
            cancel_run(run_id)

        return {
            "run_id": run_id
        }

    @handle_errors
    def get_run_status(self, run_id: str) -> Dict[str, str]:
        """
        Get quick status info about a workflow run, returning a simple result
        with the overall state of the workflow run.
        """

        return {
            "run_id": run_id,
            "state": self.get_state(run_id)
        }

    # Toil custom endpoints that are not part of the GA4GH WES spec

    @handle_errors
    def get_stdout(self, run_id: str) -> Any:
        """
        Get the stdout of a workflow run as a static file.
        """
        self._get_run(run_id, should_exists=True)
        return send_from_directory(self.work_dir, os.path.join(run_id, "stdout"), mimetype="text/plain")

    @handle_errors
    def get_stderr(self, run_id: str) -> Any:
        """
        Get the stderr of a workflow run as a static file.
        """
        self._get_run(run_id, should_exists=True)
        return send_from_directory(self.work_dir, os.path.join(run_id, "stderr"), mimetype="text/plain")

    @handle_errors
    def get_health(self) -> Response:
        """
        Return successfully if the server is healthy.
        """
        return Response("OK", mimetype="text/plain")

    @handle_errors
    def get_homepage(self) -> Response:
        """
        Provide a sensible result for / other than 404.
        """
        # For now just go to the service info endpoint
        return redirect('ga4gh/wes/v1/service-info', code=302)


