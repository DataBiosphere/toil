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
import signal
import subprocess
import uuid
from typing import Optional, List, Dict, Any, overload, Generator, Tuple

from toil import resolveEntryPoint
from toil.server.wes.abstract_backend import (WESBackend,
                                              handle_errors,
                                              WorkflowNotFoundError)
from toil.version import baseVersion

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ToilWorkflow:
    def __init__(self, run_id: str, work_dir: str):
        """
        Class to represent a Toil workflow. This class is responsible for
        launching workflow runs and retrieving data generated from them.

        :param run_id: A uuid string.  Used to name the folder that contains
                       all of the files containing this particular workflow
                       instance's information.
        :param work_dir: The parent working directory.
        """
        self.run_id = run_id
        self.work_dir = work_dir
        self.exec_dir = os.path.join(self.work_dir, "execution")

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

    def exists(self) -> bool:
        """ Return True if the workflow run exists."""
        return os.path.isdir(self.work_dir)

    def get_state(self) -> str:
        """ Return the state of the current run."""
        return self.fetch("state", "UNKNOWN")

    def set_state(self, state: str) -> None:
        """ Set the state for the current run."""
        with open(os.path.join(self.work_dir, "state"), "w") as f:
            f.write(state)

    def set_up_run(self) -> None:
        """ Set up necessary directories for the run."""
        if not os.path.exists(self.exec_dir):
            os.makedirs(self.exec_dir)

    def clean_up(self) -> None:
        """ Clean directory and files related to the run."""
        shutil.rmtree(os.path.join(self.work_dir))

    def get_run_command(self, request: Dict[str, Any], options: List[str]) -> List[str]:
        """
        Return a list of commands, and the working directory for the run.
        """
        with open(os.path.join(self.work_dir, "request.json"), "w") as f:
            json.dump(request, f)

        options = [f"--opt={option}" for option in options]
        return [resolveEntryPoint("_toil_wes_runner"), "--work_dir", self.work_dir, *options]

    def get_output_files(self) -> Any:
        """
        Return a collection of output files that this workflow generated.
        """
        return json.loads(self.fetch("outputs.json", "{}"))


class ToilBackend(WESBackend):
    """
    WES backend implemented for Toil to run CWL, WDL, or Toil workflows. This
    class is responsible for validating and executing submitted workflows.

    Local implementation -
    Interact with the "workflows/" directory in the filesystem to store and
    retrieve data associated with the runs.
    """

    def __init__(self, work_dir: str, options: List[str]) -> None:
        super(ToilBackend, self).__init__(options)
        self.work_dir = work_dir
        self.supported_versions: Dict[str, List[str]] = {}
        self.processes: Dict[str, "subprocess.Popen[bytes]"] = {}

    def register_wf_type(self, name: str, supported_versions: List[str]) -> None:
        """
        Register a workflow type that this backend should support.
        """
        self.supported_versions[name.lower()] = supported_versions

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

        # Since we can't schedule tasks with Flask, we do the cleanup here..
        process = self.processes.get(run_id)
        if process and process.poll() is not None:
            process.terminate()
            self.processes.pop(run_id)

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

        state_counts = {state: 0 for state in (
            "QUEUED", "INITIALIZING", "RUNNING", "COMPLETE", "EXECUTOR_ERROR", "SYSTEM_ERROR", "CANCELING", "CANCELED"
        )}
        for _, state in self.get_runs():
            state_counts[state] += 1

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
        job = self._get_run(run_id, should_exists=False)

        # set up necessary directories for the run
        job.set_up_run()

        # stage the uploaded files to the execution directory, so that we can run the workflow file directly
        temp_dir = job.exec_dir
        try:
            _, request = self.collect_attachments(run_id, temp_dir=temp_dir)
        except ValueError:
            job.clean_up()
            raise

        wf_type = request["workflow_type"].lower().strip()
        version = request["workflow_type_version"]

        # validate workflow request
        supported_versions = self.supported_versions.get(wf_type, None)
        if not supported_versions:
            job.clean_up()
            raise RuntimeError(f"workflow_type '{wf_type}' is not supported.")
        if version not in supported_versions:
            job.clean_up()
            raise RuntimeError("workflow_type '{}' requires 'workflow_type_version' to be one of '{}'.  Got '{}'"
                               "instead.".format(wf_type, str(supported_versions), version))

        job.set_state("QUEUED")

        command = job.get_run_command(request=request, options=self.options)

        # TODO: Flask recommends using a task queue (like Celery or rq) to run tasks like these, since this way
        #  will likely leave behind zombie child processes until we read the return code of the process.
        # https://flask.palletsprojects.com/en/2.0.x/patterns/celery/
        # https://python-rq.org/

        with open(os.path.join(job.work_dir, "runner.log"), "w") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log, cwd=job.work_dir)

        with open(os.path.join(job.work_dir, "runner.pid"), "w") as f:
            f.write(str(process.pid))

        self.processes[run_id] = process
        logger.info(f"Spawned child process ({process.pid}) to run the requested workflow.")

        return {
            "run_id": run_id
        }

    @handle_errors
    def get_run_log(self, run_id: str) -> Dict[str, Any]:
        """ Get detailed info about a workflow run."""
        run = self._get_run(run_id, should_exists=True)
        state = run.get_state()

        request = json.loads(run.fetch("request.json", "{}"))

        cmd = run.fetch("cmd", "").split("\n")
        start_time = run.fetch("start_time")
        end_time = run.fetch("end_time")

        # TODO: stdout and stderr should be a URL that points to the output file, not the actual contents.
        stdout = run.fetch("stdout")
        stderr = run.fetch("stderr")
        exit_code = run.fetch("exit_code")

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
            "task_logs": [],
            "outputs": output_obj,
        }

    @handle_errors
    def cancel_run(self, run_id: str) -> Dict[str, str]:
        """ Cancel a running workflow."""
        run = self._get_run(run_id, should_exists=True)
        state = run.get_state()

        if state not in ("QUEUED", "INITIALIZING", "RUNNING"):
            raise RuntimeError(f"Workflow is in state: '{state}', which cannot be cancelled.")
        run.set_state("CANCELING")

        with open(os.path.join(run.work_dir, "runner.pid"), "r") as f:
            pid = int(f.read())
        os.kill(pid, signal.SIGINT)

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
