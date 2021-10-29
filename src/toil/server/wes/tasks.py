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
import fcntl
import json
import logging
import os
import subprocess
from typing import Dict, Any, List, Union
from urllib.parse import urldefrag

from celery.exceptions import SoftTimeLimitExceeded  # type: ignore
from toil.common import Toil
from toil.server.celery_app import celery
from toil.server.utils import (get_iso_time,
                               link_file,
                               download_file_from_internet,
                               get_file_class,
                               safe_read_file,
                               safe_write_file)

logger = logging.getLogger(__name__)


class ToilWorkflowRunner:
    """
    A class to represent a workflow runner to run the requested workflow.

    Responsible for parsing the user request into a shell command, executing
    that command, and collecting the outputs of the resulting workflow run.
    """

    def __init__(self, work_dir: str, request: Dict[str, Any], engine_options: List[str]):
        self.work_dir = work_dir
        self.request = request
        self.engine_options = engine_options

        self.wf_type: str = request["workflow_type"].lower().strip()
        self.version: str = request["workflow_type_version"]

        self.exec_dir = os.path.join(self.work_dir, "execution")
        self.out_dir = os.path.join(self.work_dir, "outputs")

        self.default_job_store = "file:" + os.path.join(self.work_dir, "toil_job_store")
        self.job_store = self.default_job_store

    def write(self, filename: str, contents: str) -> None:
        with open(os.path.join(self.work_dir, filename), "w") as f:
            f.write(contents)

    def get_state(self) -> str:
        return safe_read_file(os.path.join(self.work_dir, "state")) or "UNKNOWN"

    def set_state(self, state: str) -> None:
        safe_write_file(os.path.join(self.work_dir, "state"), state)

    def write_workflow(self, src_url: str) -> str:
        """
        Fetch the workflow file from its source and write it to a destination
        file.
        """
        logger.info(f"Processing workflow_url: '{src_url}'...")
        dest = os.path.join(self.exec_dir, os.path.basename(src_url))

        if src_url.startswith("file://"):
            logger.info("Linking workflow from filesystem.")
            link_file(src=src_url[7:], dest=dest)

        elif src_url.startswith(("http://", "https://")):
            logger.info("Downloading workflow_url from the Internet.")
            download_file_from_internet(src=src_url, dest=dest, content_type="text/")
        else:
            logger.info("Using workflow from relative URL.")
            dest = os.path.join(self.exec_dir, src_url)

        # Make sure that the destination file actually exists
        if not os.path.isfile(urldefrag(dest)[0]):
            raise RuntimeError(f"Cannot resolve workflow file from: '{src_url}'.")

        return dest

    def sort_options(self) -> List[str]:
        """
        Sort the command line arguments in the order that can be recognized by
        the workflow execution engine.
        """
        options = []

        # First, we pass the default engine parameters
        options.extend(self.engine_options)

        # Then, we pass the user options specific for this workflow run. This should override the default
        for key, value in self.request.get("workflow_engine_parameters", {}).items():
            if value is None:  # flags
                options.append(key)
            else:
                options.append(f"{key}={value}")

        # determine job store and set a new default if the user did not set one
        cloud = False
        for option in options:
            if option.startswith("--jobStore="):
                self.job_store = option[11:]
                options.remove(option)
            if option.startswith(("--outdir=", "-o=")):
                options.remove(option)

        job_store_type, _ = Toil.parseLocator(self.job_store)
        if job_store_type in ("aws", "google", "azure"):
            cloud = True

        if self.wf_type in ("cwl", "wdl"):
            if not cloud:
                options.append("--outdir=" + self.out_dir)
            options.append("--jobStore=" + self.job_store)
        else:
            # TODO: find a way to communicate the out_dir to the Toil workflow.

            # append the positional jobStore argument at the end for Toil workflows
            options.append(self.job_store)

        return options

    def initialize_run(self) -> List[str]:
        """
        Write workflow and input files and construct a list of shell commands
        to be executed. Return that list of shell commands that should be
        executed in order to complete this workflow run.
        """
        # write main workflow file and its secondary input file
        workflow_url = self.write_workflow(src_url=self.request["workflow_url"])
        input_json = os.path.join(self.exec_dir, "wes_inputs.json")

        # write input file as JSON
        with open(input_json, "w") as f:
            json.dump(self.request["workflow_params"], f)

        # create output directory
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)

        # sort commands
        options = self.sort_options()

        # construct and return the command to run
        if self.wf_type == "cwl":
            command_args = (
                ["toil-cwl-runner"] + options + [workflow_url, input_json]
            )
        elif self.wf_type == "wdl":
            command_args = (
                ["toil-wdl-runner"] + options + [workflow_url, input_json]
            )
        elif self.wf_type == "py":
            command_args = ["python", workflow_url] + options
        else:
            # This shouldn't happen if this was executed from the Toil WES server.
            raise RuntimeError(f"Unsupported workflow type: {self.wf_type}")

        return command_args

    def call_cmd(self, cmd: Union[List[str], str], cwd: str) -> "subprocess.Popen[bytes]":
        """
        Calls a command with Popen. Writes stdout, stderr, and the command to
        separate files.
        """
        stdout_f = os.path.join(self.work_dir, "stdout")
        stderr_f = os.path.join(self.work_dir, "stderr")

        with open(stdout_f, "w") as stdout, open(stderr_f, "w") as stderr:
            logger.info(f"Calling: '{' '.join(cmd)}'")
            process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, close_fds=True, cwd=cwd)

        return process

    def run(self) -> None:
        """
        Construct a command to run a the requested workflow with the options,
        run it, and deposit the outputs in the output directory.
        """

        # the task has been picked up by the runner and is currently preparing to run
        self.set_state("INITIALIZING")
        commands = self.initialize_run()

        # store the job store location
        with open(os.path.join(self.work_dir, "job_store"), "w") as f:
            f.write(self.job_store)

        # lock the state file until we start the subprocess
        file_obj = open(os.path.join(self.work_dir, "state"), "r+")
        fcntl.flock(file_obj.fileno(), fcntl.LOCK_EX)

        state = file_obj.read()
        if state in ("CANCELING", "CANCELED"):
            logger.info("Workflow canceled.")
            return

        # https://stackoverflow.com/a/15976014
        file_obj.seek(0)
        file_obj.write("RUNNING")
        file_obj.truncate()

        process = self.call_cmd(cmd=commands, cwd=self.exec_dir)

        # Now the command is running, we can allow state changes again
        fcntl.flock(file_obj.fileno(), fcntl.LOCK_UN)
        file_obj.close()

        self.write("start_time", get_iso_time())
        self.write("cmd", " ".join(commands))

        try:
            exit_code = process.wait()
        except (KeyboardInterrupt, SystemExit, SoftTimeLimitExceeded) as e:
            logger.warning(str(e))
            process.terminate()

            if process.wait(timeout=5) is None:
                process.kill()

            logger.info("Child process terminated by interruption.")
            exit_code = 130

        self.write("end_time", get_iso_time())
        self.write("exit_code", str(exit_code))

        if exit_code == 0:
            self.set_state("COMPLETE")
        # non-zero exit code indicates failure
        elif exit_code == 130:
            self.set_state("CANCELED")
        else:
            self.set_state("EXECUTOR_ERROR")

    def write_output_files(self) -> None:
        """
        Fetch all the files that this workflow generated and output information
        about them to `outputs.json`.
        """
        output_obj = {}
        job_store_type, _ = Toil.parseLocator(self.job_store)

        # For CWL workflows, the stdout should be a JSON object containing the outputs
        if self.wf_type == "cwl":
            try:
                with open(os.path.join(self.work_dir, "stdout")) as f:
                    output_obj = json.load(f)
            except Exception as e:
                logger.warning("Failed to read outputs object from stdout:", exc_info=e)

        elif job_store_type == "file":
            for file in os.listdir(self.out_dir):
                location = os.path.join(self.out_dir, file)
                output_obj[file] = {
                    "location": location,
                    "size": os.stat(location).st_size,
                    "class": get_file_class(location),
                }

        # TODO: fetch files from other job stores

        self.write("outputs.json", json.dumps(output_obj))


@celery.task(name="run_wes")
def run_wes(work_dir: str, request: Dict[str, Any], engine_options: List[str]) -> str:
    """
    A celery task to run a requested workflow.
    """
    runner = ToilWorkflowRunner(work_dir, request=request, engine_options=engine_options)

    try:
        runner.run()

        state = runner.get_state()
        logger.info(f"Workflow run completed with state: '{state}'.")

        if state == "COMPLETE":
            logger.info(f"Fetching output files.")
            runner.write_output_files()
    except (KeyboardInterrupt, SystemExit, SoftTimeLimitExceeded):
        runner.set_state("CANCELED")
    except Exception as e:
        runner.set_state("EXECUTOR_ERROR")
        raise e

    return runner.get_state()


def cancel_run(task_id: str) -> None:
    """
    Send a SIGTERM signal to the process that is running task_id.
    """
    celery.control.terminate(task_id, signal='SIGUSR1')
