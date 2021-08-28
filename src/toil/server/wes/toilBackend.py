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
import uuid
from multiprocessing import Process
from typing import Optional, List, Dict, Any, overload, Generator, Tuple, Type

from toil.server.api.abstractBackend import WESBackend
from toil.server.api.utils import handle_errors, WorkflowNotFoundError, get_iso_time
from toil.server.wes.runners import WorkflowRunner
from toil.version import baseVersion

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ToilWorkflow:
    def __init__(self, run_id: str, work_dir: str):
        """
        Class to represent a Toil workflow.

        :param run_id: A uuid string.  Used to name the folder that contains
                       all of the files containing this particular workflow
                       instance's information.
        :param work_dir: The parent working directory.
        """
        self.run_id = run_id
        self.work_dir = work_dir

        self.exec_dir = os.path.join(self.work_dir, "execution")
        self.out_dir = os.path.join(self.work_dir, "outputs")

    @overload
    def fetch(self, filename: str, default: str) -> str: ...
    @overload
    def fetch(self, filename: str, default: None = None) -> Optional[str]: ...

    def fetch(self, filename: str, default: Optional[str] = None) -> Optional[str]:
        """
        Return the contents of the given file. If the file does not exist, the
        default value is returned.
        """
        if os.path.exists(os.path.join(self.work_dir, filename)):
            with open(os.path.join(self.work_dir, filename), "r") as f:
                return f.read()
        return default

    def _write(self, filename: str, content: str) -> None:
        """
        Write a file under the working directory of the current run.
        """
        with open(os.path.join(self.work_dir, filename), "w") as f:
            f.write(content)

    def exists(self) -> bool:
        """
        Return True if the workflow run exists.
        """
        return os.path.isdir(self.work_dir)

    def get_state(self) -> str:
        """
        Return the state of the current run.
        """
        return self.fetch("state", "UNKNOWN")

    def _set_state(self, state: str) -> None:
        """
        Set the state for the current run.
        """
        logger.info(f"Workflow {self.run_id}: {state}")
        self._write("state", state)

    def prepare_run(self) -> None:
        """
        Prepare necessary directories for the run and set state to QUEUED.
        """
        if not os.path.exists(self.exec_dir):
            os.makedirs(self.exec_dir)
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)
        self._set_state("QUEUED")

    def prepare_workflow(self, request: Dict[str, Any], engine_options: List[str]) -> None:
        """
        Prepare workflow and input files and construct a shell command that can
        be executed.
        """
        wf_type = request["workflow_type"].lower().strip()
        version = request["workflow_type_version"]

        # TODO: verify that the workflow type and version is supported

        logger.info(f"Beginning Toil Workflow ID: {self.run_id}")

        self._set_state("INITIALIZING")

        self._write("start_time", get_iso_time())
        self._write("request.json", json.dumps(request))
        self._write("wes_input.json", json.dumps(request["workflow_params"]))

    def run(self) -> None:
        """
        This can be a blocking call.
        """


class ToilBackend(WESBackend):
    """
    WES backend implemented for Toil to run CWL, WDL, or Toil workflows. This
    class is responsible for validating and executing submitted workflows.

    Local implementation -
    Interact with the "workflows/" directory to store and retrieve data
    associated with all the workflow runs in the filesystem.
    """

    def __init__(self, opts: List[str]) -> None:
        super(ToilBackend, self).__init__(opts)
        self.work_dir = os.path.join(os.getcwd(), "workflows")

        self.runners: Dict[str, Type[WorkflowRunner]] = {}
        self.processes: Dict[str, "Process"] = {}

    def register_runner(self, name: str, runner: Type[WorkflowRunner]) -> None:
        """
        Register a workflow runner that this backend should support.
        """
        self.runners[name.lower()] = runner

    def _get_run(self, run_id: str, should_exists: Optional[bool] = None) -> ToilWorkflow:
        """
        Helper method to instantiate a ToilWorkflow object.

        :param run_id: The run ID.
        :param should_exists: If set, ensures that the workflow run exists (or
                              does not exist) according to the value.
        """
        run = ToilWorkflow(run_id, work_dir=os.path.join(self.work_dir, run_id))

        if should_exists is not None:
            if should_exists and not run.exists():
                raise WorkflowNotFoundError
            if should_exists is False and run.exists():
                raise RuntimeError(f"Workflow {run_id} exists when it shouldn't.")

        return run

    def get_runs(self) -> Generator[Tuple[str, str], None, None]:
        """
        A generator of a list of run ids and their state.
        """
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
        """ Get information about Workflow Execution Service."""

        # state_counts = {state: 0 for state in (
        #     "QUEUED", "INITIALIZING", "RUNNING", "COMPLETE", "EXECUTOR_ERROR", "SYSTEM_ERROR", "CANCELING", "CANCELED"
        # )}
        # for _, state in self.get_runs():
        #     state_counts[state] += 1

        return {
            "workflow_type_versions": {
                k: {
                    "workflow_type_version": v.supported_versions()
                } for k, v in self.runners.items()
            },
            "supported_wes_versions": ["1.0.0"],
            "supported_filesystem_protocols": ["file", "http", "https"],
            "workflow_engine_versions": {"toil": baseVersion},
            "system_state_counts": {},
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

        # prepare necessary directories for the run
        run.prepare_run()

        # stage the uploaded files to the execution directory, so that we can run the workflow file directly
        temp_dir = run.exec_dir
        _, body = self.collect_attachments(run_id, temp_dir=temp_dir)

        # validate the request and prepare workflow and input files
        run.prepare_workflow(request=body, engine_options=[])

        # Call run() using multiprocessing
        # run.run()

        return {
            "run_id": run_id
        }

    @handle_errors
    def get_run_log(self, run_id: str) -> Dict[str, Any]:
        """ Get detailed info about a workflow run."""
        run = self._get_run(run_id, should_exists=True)
        state = run.get_state()

        request = json.loads(run.fetch("request.json", "{}"))
        job_store = run.fetch("job_store", "")

        cmd = run.fetch("cmd", "").split("\n")
        start_time = run.fetch("start_time")
        end_time = run.fetch("end_time")

        # TODO: stdout and stderr should be a URL that points to the output file, not the actual contents.
        stdout = run.fetch("stdout")
        stderr = run.fetch("stderr")
        exit_code = run.fetch("exit_code")

        # output_obj = {}
        if state == "COMPLETE":
            # only tested locally
            if job_store.startswith("file:"):
                # TODO: get output files
                pass

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
                "exit_code": exit_code,
            },
            "task_logs": [],
            "outputs": None,
        }

    @handle_errors
    def cancel_run(self, run_id: str) -> Dict[str, str]:
        """ Cancel a running workflow."""

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
