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
import multiprocessing
import os
import signal
import subprocess
import sys
import tempfile
import zipfile
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urldefrag

from celery.exceptions import SoftTimeLimitExceeded  # type: ignore

import toil.server.wes.amazon_wes_utils as amazon_wes_utils
from toil.common import Toil
from toil.jobStores.utils import generate_locator
from toil.server.celery_app import celery
from toil.server.utils import (WorkflowStateMachine,
                               connect_to_workflow_state_store,
                               download_file_from_internet,
                               download_file_from_s3,
                               get_file_class,
                               get_iso_time,
                               link_file)

logger = logging.getLogger(__name__)

# How many seconds should we give a Toil workflow to gracefully shut down
# before we kill it?
# Ought to be long enough to let it clean up its job store, but shorter than
# our patience for CANCELING WES workflows to time out to CANCELED.
WAIT_FOR_DEATH_TIMEOUT = 20

class ToilWorkflowRunner:
    """
    A class to represent a workflow runner to run the requested workflow.

    Responsible for parsing the user request into a shell command, executing
    that command, and collecting the outputs of the resulting workflow run.
    """

    def __init__(self, base_scratch_dir: str, state_store_url: str, workflow_id: str, request: Dict[str, Any], engine_options: List[str]):
        """
        Make a new ToilWorkflowRunner to actually run a workflow leader based
        on a WES request.

        :param base_scratch_dir: Base work directory. Workflow scratch directory
               will be in here under the workflow ID.
        :param state_store_url: URL to the state store through which we will
               communicate about workflow state with the WES server.
        :param workflow_id: ID of the workflow run.
        :param request: WES request information.
        :param engine_options: Extra options to pass to Toil.
        """

        # Find the scratch directory inside the base scratch directory
        self.scratch_dir = os.path.join(base_scratch_dir, workflow_id)

        # Connect to the workflow state store
        self.store = connect_to_workflow_state_store(state_store_url, workflow_id)
        # And use a state machine over that to look at workflow state
        self.state_machine = WorkflowStateMachine(self.store)

        self.request = request
        self.engine_options = engine_options

        self.wf_type: str = request["workflow_type"].lower().strip()
        self.version: str = request["workflow_type_version"]

        self.exec_dir = os.path.join(self.scratch_dir, "execution")
        self.out_dir = os.path.join(self.scratch_dir, "outputs")

        # Compose the right kind of job store to use it the user doesn't specify one.
        default_type = os.getenv('TOIL_WES_JOB_STORE_TYPE', 'file')
        self.default_job_store = generate_locator(default_type, local_suggestion=os.path.join(self.scratch_dir, "toil_job_store"))

        self.job_store = self.default_job_store

    def write_scratch_file(self, filename: str, contents: str) -> None:
        """
        Write a file to the scratch directory.
        """
        with open(os.path.join(self.scratch_dir, filename), "w") as f:
            f.write(contents)

    def get_state(self) -> str:
        return self.state_machine.get_current_state()

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
        elif src_url.startswith("s3://"):
            logger.info("Downloading workflow_url from Amazon S3.")
            download_file_from_s3(src=src_url, dest=dest)
        else:
            logger.info("Using workflow from relative URL.")
            dest = os.path.join(self.exec_dir, src_url)

        # Make sure that the destination file actually exists
        if not os.path.isfile(urldefrag(dest)[0]):
            raise RuntimeError(f"Cannot resolve workflow file from: '{src_url}'.")

        return dest

    def sort_options(self, workflow_engine_parameters: Optional[Dict[str, Optional[str]]] = None) -> List[str]:
        """
        Sort the command line arguments in the order that can be recognized by
        the workflow execution engine.

        :param workflow_engine_parameters: User-specified parameters for this
        particular workflow. Keys are command-line options, and values are
        option arguments, or None for options that are flags.
        """
        options = []

        # First, we pass the default engine parameters
        options.extend(self.engine_options)

        if workflow_engine_parameters:
            # Then, we pass the user options specific for this workflow run.
            # This should override the default
            for key, value in workflow_engine_parameters.items():
                if value is None:  # flags
                    options.append(key)
                else:
                    options.append(f"{key}={value}")

        # We want to clean always by default, unless a particular job store or
        # a clean option was passed.
        clean = None

        # Parse options and drop options we may need to override.
        for option in options:
            if option.startswith("--jobStore="):
                self.job_store = option[11:]
                options.remove(option)
            if option.startswith(("--outdir=", "-o=")):
                # We need to generate this one ourselves.
                options.remove(option)
            if option.startswith("--clean="):
                clean = option[8:]

        cloud = False
        job_store_type, _ = Toil.parseLocator(self.job_store)
        if job_store_type in ("aws", "google", "azure"):
            cloud = True

        if self.job_store == self.default_job_store and clean is None:
            # User didn't specify a clean option, and we're on a default,
            # randomly generated job store, so we should clean it up even if we
            # crash.
            options.append("--clean=always")

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
        # Obtain CWL-style workflow parameters from the request. Default to an
        # empty dict if not found, because we want to tolerate omitting this.
        workflow_params = self.request.get("workflow_params", {})

        # And any workflow engine parameters the user specified.
        workflow_engine_parameters = self.request.get("workflow_engine_parameters", {})

        # Obtain main workflow file to a path (no-scheme file URL).
        workflow_url = self.write_workflow(src_url=self.request["workflow_url"])

        if os.path.basename(workflow_url) == "workflow.zip" and zipfile.is_zipfile(workflow_url):
            # We've been sent a zip file. We should interpret this as an Amazon Genomics CLI-style zip file.
            # Extract everything next to the zip and find and open relvant files.
            logger.info("Extracting and parsing Amazon-style workflow bundle...")
            zip_info = amazon_wes_utils.parse_workflow_zip_file(workflow_url, self.wf_type)

            # Now parse Amazon's internal format into our own.

            # Find the real workflow source for the entrypoint file
            if 'workflowSource' in zip_info['files']:
                # They sent a file, which has been opened, so grab its path
                workflow_url = zip_info['files']['workflowSource'].name
                logger.info("Workflow source file: '%s'", workflow_url)
            elif 'workflowUrl' in zip_info['data']:
                # They are pointing to another URL.
                # TODO: What does Amazon expect this to mean? Are we supposed to recurse?
                # For now just forward it.
                workflow_url = zip_info['data']['workflowUrl']
                logger.info("Workflow reference URL: '%s'", workflow_url)
            else:
                # The parser is supposed to throw so we can't get here
                raise RuntimeError("Parser could not find workflow source or URL in zip")

            if 'workflowInputFiles' in zip_info['files'] and len(zip_info['files']['workflowInputFiles']) > 0:
                # The bundle contains a list of input files.
                # We interpret these as JSON, and layer them on top of each
                # other, and then apply workflow_params from the request.
                logger.info("Workflow came with %d bundled inputs files; coalescing final parameters",
                            len(zip_info['files']['workflowInputFiles']))

                coalesced_parameters = {}
                for binary_file in zip_info['files']['workflowInputFiles']:
                    try:
                        # Read each input file as a JSON
                        loaded_parameters = json.load(binary_file)
                    except json.JSONDecodeError as e:
                        raise RuntimeError(f"Could not parse inputs JSON {os.path.basename(binary_file.name)}: {e}")
                    # And merge them together in order
                    coalesced_parameters.update(loaded_parameters)
                # Then apply and replace the parameters that came with the request
                coalesced_parameters.update(workflow_params)
                workflow_params = coalesced_parameters

            if 'workflowOptions' in zip_info['files']:
                # The bundle contains an options JSON. We interpret these as
                # defaults for workflow_engine_parameters.
                logger.info(f"Workflow came with bundled options JSON")
                try:
                    # Read as a JSON
                    loaded_options = json.load(zip_info['files']['workflowOptions'])
                except json.JSONDecodeError as e:
                    raise RuntimeError(f"Could not parse options JSON: {e}")
                # Apply and replace the engine parameters that came with the
                # request.
                loaded_options.update(workflow_engine_parameters)
                workflow_engine_parameters = loaded_options

        # Write out CWL-style input file
        input_json = os.path.join(self.exec_dir, "wes_inputs.json")
        with open(input_json, "w") as f:
            json.dump(workflow_params, f)

        # create output directory
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)

        # sort commands
        options = self.sort_options(workflow_engine_parameters)

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
        stdout_f = os.path.join(self.scratch_dir, "stdout")
        stderr_f = os.path.join(self.scratch_dir, "stderr")

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
        self.state_machine.send_initialize()
        commands = self.initialize_run()

        # store the job store location
        self.store.set("job_store", self.job_store)

        # Check if we are supposed to cancel
        state = self.get_state()
        if state in ("CANCELING", "CANCELED"):
            logger.info("Workflow canceled.")
            return

        # Otherwise start to run
        self.state_machine.send_run()
        process = self.call_cmd(cmd=commands, cwd=self.exec_dir)

        self.store.set("start_time", get_iso_time())
        self.store.set("cmd", " ".join(commands))

        try:
            exit_code = process.wait()
        except (KeyboardInterrupt, SystemExit, SoftTimeLimitExceeded) as e:
            logger.warning("Workflow interrupted: %s", type(e))
            process.terminate()

            try:
                process.wait(timeout=WAIT_FOR_DEATH_TIMEOUT)
            except subprocess.TimeoutExpired:
                # We need to actually stop now.
                process.kill()

            logger.info("Child process terminated by interruption.")
            exit_code = 130

        self.store.set("end_time", get_iso_time())
        self.store.set("exit_code", str(exit_code))

        logger.info('Toil child finished with code %s', exit_code)

        if exit_code == 0:
            self.state_machine.send_complete()
        # non-zero exit code indicates failure
        elif exit_code == 130:
            self.state_machine.send_canceled()
        else:
            self.state_machine.send_executor_error()

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
                with open(os.path.join(self.scratch_dir, "stdout")) as f:
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

        self.write_scratch_file("outputs.json", json.dumps(output_obj))

def run_wes_task(base_scratch_dir: str, state_store_url: str, workflow_id: str, request: Dict[str, Any], engine_options: List[str]) -> str:
    """
    Run a requested workflow.

    :param base_scratch_dir: Directory where the workflow's scratch dir will live, under the workflow's ID.

    :param state_store_url: URL/path at which the server and Celery task communicate about workflow state.

    :param workflow_id: ID of the workflow run.

    :returns: the state of the workflow run.
    """

    logger.info("Starting WES workflow")

    runner = ToilWorkflowRunner(base_scratch_dir, state_store_url, workflow_id,
                                request=request, engine_options=engine_options)

    try:
        runner.run()

        state = runner.get_state()
        logger.info(f"Workflow run completed with state: '{state}'.")

        if state == "COMPLETE":
            logger.info(f"Fetching output files.")
            runner.write_output_files()
    except (KeyboardInterrupt, SystemExit, SoftTimeLimitExceeded):
        # We canceled the workflow run
        logger.info('Canceling the workflow')
        runner.state_machine.send_canceled()
    except Exception:
        # The workflow run broke. We still count as the executor here.
        logger.exception('Running Toil produced an exception.')
        runner.state_machine.send_executor_error()
        raise

    return runner.get_state()

# Wrap the task function as a Celery task
run_wes = celery.task(name="run_wes")(run_wes_task)

def cancel_run(task_id: str) -> None:
    """
    Send a SIGTERM signal to the process that is running task_id.
    """
    celery.control.terminate(task_id, signal='SIGUSR1')

class TaskRunner:
    """
    Abstraction over the Celery API. Runs our run_wes task and allows canceling it.

    We can swap this out in the server to allow testing without Celery.
    """

    @staticmethod
    def run(args: Tuple[str, str, str, Dict[str, Any], List[str]], task_id: str) -> None:
        """
        Run the given task args with the given ID on Celery.
        """
        run_wes.apply_async(args=args,
                            task_id=task_id,
                            ignore_result=True)

    @staticmethod
    def cancel(task_id: str) -> None:
        """
        Cancel the task with the given ID on Celery.
        """
        cancel_run(task_id)

    @staticmethod
    def is_ok(task_id: str) -> bool:
        """
        Make sure that the task running system is working for the given task.
        If the task system has detected an internal failure, return False.
        """
        # Nothing to do for Celery
        return True

# If Celery can't be set up, we can just use this fake version instead.

class MultiprocessingTaskRunner(TaskRunner):
    """
    Version of TaskRunner that just runs tasks with Multiprocessing.

    Can't use threading because there's no way to send a cancel signal or
    exception to a Python thread, if loops in the task (i.e.
    ToilWorkflowRunner) don't poll for it.
    """

    _id_to_process: Dict[str, multiprocessing.Process] = {}
    _id_to_log: Dict[str, str] = {}

    @staticmethod
    def set_up_and_run_task(output_path: str, args: Tuple[str, str, str, Dict[str, Any], List[str]]) -> None:
        """
        Set up logging for the process into the given file and then call
        run_wes_task with the given arguments.

        If the process finishes successfully, it will clean up the log, but if
        the process crashes, the caller must clean up the log.
        """

        # Multiprocessing and the server manage to hide actual task output from
        # the tests. Logging messages will appear in pytest's "live" log but
        # not in the captured log. And replacing sys.stdout and sys.stderr
        # don't seem to work to collect tracebacks. So we make sure to
        # configure logging in the multiprocessing process and log to a file
        # that we were told about, so the server can come get the log if we
        # unexpectedly die.

        output_file = open(output_path, 'w')
        output_file.write('Initializing task log\n')
        output_file.flush()

        # Take over logging.
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(output_file)
        root_logger.addHandler(handler)

        def handle_sigterm(_: Any, __: Any) -> None:
            """
            Multiprocessing will send SIGTERM to stop us; translate that to
            something the run_wes_task shutdown code understands to avoid
            always waiting for the cancel timeout.

            Caller still needs to handle a sigterm exit code, in case we get
            canceled before the handler is set!
            """
            sys.exit()

        signal.signal(signal.SIGTERM, handle_sigterm)

        try:
            logger.info('Running task')
            output_file.flush()
            run_wes_task(*args)
        except Exception:
            logger.exception('Exception in task!')
            raise
        else:
            # If the task does not crash, clean up the log
            os.unlink(output_path)
        finally:
            logger.debug('Finishing task log')
            output_file.flush()
            output_file.close()

    @classmethod
    def run(cls, args: Tuple[str, str, str, Dict[str, Any], List[str]], task_id: str) -> None:
        """
        Run the given task args with the given ID.
        """

        # We need to send the child process's output somewhere or it will get lost during testing
        fd, path = tempfile.mkstemp()
        os.close(fd)
        # Store the log filename before the process, like is_ok() expects.
        cls._id_to_log[task_id] = path

        logger.info("Starting task %s in a process that should log to %s", task_id, path)

        cls._id_to_process[task_id] = multiprocessing.Process(target=cls.set_up_and_run_task, args=(path, args))
        cls._id_to_process[task_id].start()

    @classmethod
    def cancel(cls, task_id: str) -> None:
        """
        Cancel the task with the given ID.
        """
        if task_id in cls._id_to_process:
            logger.info("Stopping process for task %s", task_id)
            cls._id_to_process[task_id].terminate()
        else:
            logger.error("Tried to kill nonexistent task %s", task_id)

    @classmethod
    def is_ok(cls, task_id: str) -> bool:
        """
        Make sure that the task running system is working for the given task.
        If the task system has detected an internal failure, return False.
        """

        process = cls._id_to_process.get(task_id)
        if process is None:
            # Never heard of this task, so it's not broken.
            return True

        # If the process exited normally, or with an error code consistent with
        # being canceled by cancel(), then it is OK.
        ACCEPTABLE_EXIT_CODES = [0, -signal.SIGTERM]

        if process.exitcode is not None and process.exitcode not in ACCEPTABLE_EXIT_CODES:
            # Something went wring in the task and it couldn't handle it.
            logger.error("Process for running %s failed with code %s", task_id, process.exitcode)
            try:
                for line in open(cls._id_to_log[task_id]):
                    # Dump the task log
                    logger.error("Task said: %s", line.rstrip())
                # Remove the task log because it is no longer needed
                os.unlink(cls._id_to_log[task_id])
            except FileNotFoundError:
                # Log is already gone
                pass
            return False

        return True


