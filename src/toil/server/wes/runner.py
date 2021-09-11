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
import argparse
import json
import logging
import os
import shutil
import signal
import subprocess
from typing import Dict, Any, List, Union

from toil.server.utils import get_iso_time, get_file_class, link_file
from toil.statsAndLogging import configure_root_logger, set_log_level

logger = logging.getLogger(__name__)


"""
Toil WES workflow runner.

Helper script to run a requested WES workflow through subprocess. Responsible
for parsing the user request into a shell command, executing that command, and
collecting the outputs of the resulting workflow run.
This script expects the given `--work_dir` directory to contain an `execution`
directory where workflow attachments are staged, and a `request.json` JSON file
containing the body of the request:

.
├── execution/
│   └── ...
└── request.json

During execution, this script may write the following files to the working
directory:

.
├── cmd
├── end_time
├── exit_code
├── job_store
├── outputs.json
├── pid
├── start_time
├── state
├── stderr
└── stdout
"""


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
        with open(os.path.join(self.work_dir, "state"), "r") as f:
            return f.read()

    def set_state(self, state: str) -> None:
        self.write("state", state)

    def write_workflow(self, src_url: str) -> str:
        """
        Fetch the workflow file from its source and write it to a destination
        file. The destination is returned.
        """
        logger.info(f"Processing workflow_url: '{src_url}'...")
        dest = src_url

        if src_url.startswith("file://"):
            logger.info(f"Linking workflow from filesystem.")
            dest = os.path.join(self.exec_dir, os.path.basename(src_url))
            link_file(src=src_url[7:], dest=dest)
        elif src_url.startswith(("http://", "https://")):
            logger.info(f"Downloading workflow_url from the Internet.")
            # TODO: Download workflow files from the Internet
            raise NotImplementedError
        else:
            logger.info(f"Using workflow from relative URL.")
            dest = os.path.join(self.exec_dir, src_url)

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
        if self.job_store.startswith(("aws", "google", "azure")):
            cloud = True

        if self.wf_type.lower() in ("cwl", "wdl"):
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

    def call_cmd(self, cmd: Union[List[str], str], cwd: str) -> int:
        """
        Calls a command with Popen. Writes stdout, stderr, and the command to
        separate files. This is a blocking call.

        :returns: The exit code of the command.
        """
        stdout_f = os.path.join(self.work_dir, "stdout")
        stderr_f = os.path.join(self.work_dir, "stderr")

        with open(stdout_f, "w") as stdout, open(stderr_f, "w") as stderr:
            logger.info(f"Calling: '{' '.join(cmd)}'")
            process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, close_fds=True, cwd=cwd)

        self.write("pid", str(process.pid))

        # handle SIGTERM as SIGINT to properly shut down Toil
        signal.signal(signal.SIGTERM, signal.getsignal(signal.SIGINT))

        try:
            return process.wait()
        except KeyboardInterrupt:
            # signal an interrupt to kill the process gently
            process.send_signal(signal.SIGINT)
            process.wait()
            logger.info("Child process terminated by interruption.")
            return 130

    def run(self) -> str:
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

        if self.get_state() in ("CANCELING", "CANCELED"):
            logger.info("Workflow canceled.")
            return "CANCELED"

        self.set_state("RUNNING")
        self.write("start_time", get_iso_time())
        self.write("cmd", " ".join(commands))

        exit_code = self.call_cmd(cmd=commands, cwd=self.exec_dir)

        self.write("end_time", get_iso_time())
        self.write("exit_code", str(exit_code))

        if exit_code == 0:
            self.set_state("COMPLETE")
        # non-zero exit code indicates failure
        elif exit_code == 130:
            self.set_state("CANCELED")
        else:
            self.set_state("EXECUTOR_ERROR")

        return self.get_state()

    def fetch_output_files(self) -> Dict[str, Any]:
        """
        Fetch all the files that this workflow generated and output information
        about them to `outputs.json`.
        """
        output_obj = {}

        # TODO: read the STDOUT file if it's a CWL workflow? This will work with all job stores. I think.

        if self.job_store.startswith("file:"):
            for file in os.listdir(self.out_dir):
                if file.startswith("out_tmpdir"):
                    shutil.rmtree(os.path.join(self.out_dir, file))

            for file in os.listdir(self.out_dir):
                location = os.path.join(self.out_dir, file)
                output_obj[file] = {
                    "location": location,
                    "size": os.stat(location).st_size,
                    "class": get_file_class(location),
                }

        # TODO: other job stores

        self.write("outputs.json", json.dumps(output_obj))
        return output_obj


def main() -> None:
    configure_root_logger()
    set_log_level("INFO")
    logger.info("Preparing requested workflow to run.")

    parser = argparse.ArgumentParser("Execution script for the workflow submitted through the Toil WES servers.")
    parser.add_argument("--work_dir", type=str, default=os.getcwd(),
                        help=f"The working directory of the workflow run. (default: {os.getcwd()}).")
    parser.add_argument("--engine_option", default=[], action="append",
                        help="A list of default engine parameters that should be passed into the workflow "
                             "execution engine if they are not overwritten by the user request.")
    args = parser.parse_args()

    work_dir = args.work_dir
    engine_options = args.engine_option

    if not os.path.isfile(os.path.join(work_dir, "request.json")):
        logger.error(f"Cannot process the requested workflow: 'request.json' is not found in '{work_dir}'.")
        exit(1)

    with open(os.path.join(work_dir, "request.json"), "r") as f:
        request = json.load(f)

    runner = ToilWorkflowRunner(work_dir, request=request, engine_options=engine_options)

    try:
        state = runner.run()
        logger.info(f"Workflow run completed with state: '{state}'.")

        if state == "COMPLETE":
            logger.info(f"Fetching output files.")
            runner.fetch_output_files()
    except KeyboardInterrupt:
        runner.set_state("CANCELED")
    except:
        runner.set_state("EXECUTOR_ERROR")
        raise


if __name__ == '__main__':
    main()
