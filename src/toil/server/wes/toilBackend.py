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
import subprocess
import uuid
from multiprocessing import Process
from typing import Optional, List, Dict, Any, overload, Generator, Tuple, Type, Union

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

    def set_state(self, state: str) -> None:
        """
        Set the state for the current run.
        """
        logger.info(f"Workflow {self.run_id}: {state}")
        self._write("state", state)

    @staticmethod
    def _get_file_class(location: str) -> str:
        """ Return the file class as a human readable string."""
        if os.path.islink(location):
            return "Link"
        elif os.path.isfile(location):
            return "File"
        elif os.path.isdir(location):
            return "Directory"
        return "Unknown"

    def get_output_files(self) -> Dict[str, Any]:
        """
        Return a collection of output files that this workflow generated.
        """
        output_obj = {}
        job_store = self.fetch("job_store", "")

        if job_store.startswith("file:"):
            for file in os.listdir(self.out_dir):
                if file.startswith("out_tmpdir"):
                    shutil.rmtree(os.path.join(self.out_dir, file))

            for file in os.listdir(self.out_dir):
                location = os.path.join(self.out_dir, file)
                output_obj[file] = {
                    "location": location,
                    "size": os.stat(location).st_size,
                    "class": self._get_file_class(location),
                }
        # TODO: other job stores

        return output_obj

    def set_up_run(self) -> None:
        """
        Set up necessary directories for the run and set state to QUEUED.
        """
        if not os.path.exists(self.exec_dir):
            os.makedirs(self.exec_dir)
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)
        self.set_state("QUEUED")

    @staticmethod
    def _link_file(src: str, dest: str) -> None:
        try:
            os.link(src, dest)
        except OSError:
            os.symlink(src, dest)

    def write_workflow(self, runner: WorkflowRunner, request: Dict[str, Any], engine_options: List[str]) -> List[str]:
        """
        Write workflow and input files and construct a shell command that can
        be executed.
        """
        workflow_url = request["workflow_url"]
        input_json = os.path.join(self.exec_dir, "wes_input.json")
        job_store = "file:" + os.path.join(self.work_dir, "jobStore")  # default

        # link the workflow file into the cwd
        if workflow_url.startswith("file://"):
            dest = os.path.join(self.exec_dir, "wes_workflow" + os.path.splitext(workflow_url)[1])
            self._link_file(src=workflow_url[7:], dest=dest)
            workflow_url = dest

        args = runner.sort_options(job_store=job_store,
                                   out_dir=self.out_dir,
                                   default_engine_options=engine_options)

        return runner.construct_command(workflow_url, input_json=input_json, options=args)

    def call_cmd(self, cmd: Union[List[str], str], cwd: str) -> int:
        """
        Calls a command with Popen. Writes stdout, stderr, and the command to
        separate files. This is a blocking call.

        :returns: The exit code of the command.
        """
        self._write("cmd", " ".join(cmd))

        stdout_f = os.path.join(self.work_dir, "stdout")
        stderr_f = os.path.join(self.work_dir, "stderr")

        with open(stdout_f, "w") as stdout, open(stderr_f, "w") as stderr:
            logger.info("Calling: " + " ".join(cmd))
            process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, close_fds=True, cwd=cwd)

        self._write("pid", str(process.pid))

        # FIXME: should we wait on a multiprocessing Process?
        try:
            return process.wait()
        except KeyboardInterrupt:
            logger.info("Child process terminated by parent")
            return 130

    def run(self, runner: WorkflowRunner, request: Dict[str, Any], engine_options: List[str]) -> str:
        """
        Construct a command to run a the requested workflow with the options,
        run it, and deposit the outputs in the output directory.
        """
        self.set_state("INITIALIZING")
        logger.info(f"Beginning Toil Workflow ID: {self.run_id}")

        self._write("start_time", get_iso_time())
        self._write("request.json", json.dumps(request))
        with open(os.path.join(self.exec_dir, "wes_input.json"), "w") as f:
            json.dump(request["workflow_params"], f)

        command = self.write_workflow(runner, request=request, engine_options=engine_options)
        if runner.job_store:
            self._write("job_store", runner.job_store)

        self.set_state("RUNNING")

        exit_code = self.call_cmd(cmd=command, cwd=self.exec_dir)

        self._write("end_time", get_iso_time())
        self._write("exit_code", str(exit_code))

        if exit_code == 0:
            self.set_state("COMPLETE")
        else:
            # non-zero exit code indicates failure
            self.set_state("EXECUTOR_ERROR")

        return self.get_state()


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
        """ Get information about Workflow Execution Service."""

        state_counts = {state: 0 for state in (
            "QUEUED", "INITIALIZING", "RUNNING", "COMPLETE", "EXECUTOR_ERROR", "SYSTEM_ERROR", "CANCELING", "CANCELED"
        )}
        for _, state in self.get_runs():
            state_counts[state] += 1

        return {
            "workflow_type_versions": {
                k: {
                    "workflow_type_version": v.supported_versions()
                } for k, v in self.runners.items()
            },
            "supported_wes_versions": ["1.0.0"],
            "supported_filesystem_protocols": ["file", "http", "https"],
            "workflow_engine_versions": {"toil": baseVersion},
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
        _, request = self.collect_attachments(run_id, temp_dir=temp_dir)

        wf_type = request["workflow_type"].lower().strip()
        version = request["workflow_type_version"]

        runner = self.runners.get(wf_type, None)
        if not runner:
            job.set_state("EXECUTOR_ERROR")
            raise RuntimeError(f"workflow_type '{wf_type}' is not supported.")
        if version not in runner.supported_versions():
            job.set_state("EXECUTOR_ERROR")
            raise RuntimeError("workflow_type '{}' requires 'workflow_type_version' to be one of '{}'.  Got '{}'"
                               "instead.".format(wf_type, str(runner.supported_versions()), version))

        p = Process(target=job.run, args=(runner.create(run_id, version=version),
                                          request,
                                          self.opts.get_options("extra")))
        p.start()
        self.processes[run_id] = p

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
