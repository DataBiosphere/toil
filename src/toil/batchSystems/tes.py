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
"""
Batch system for running Toil workflows on GA4GH TES.

Useful with network-based job stores when the TES server provides tasks with
credentials, and filesystem-based job stores when the TES server lets tasks
mount the job store.

Additional containers should be launched with Singularity, not Docker.
"""
import base64
import datetime
import logging
import os
import pickle
import time
from argparse import ArgumentParser, _ArgumentGroup
from typing import Any, Callable, Dict, List, Optional, Union

import tes
from requests.exceptions import HTTPError

from toil import applianceSelf
from toil.batchSystems.abstractBatchSystem import (EXIT_STATUS_UNAVAILABLE_VALUE,
                                                   BatchJobExitReason,
                                                   UpdatedBatchJobInfo)
from toil.batchSystems.cleanup_support import BatchSystemCleanupSupport
from toil.common import Config, Toil
from toil.job import JobDescription
from toil.lib.misc import get_public_ip, slow_down, utc_now
from toil.resource import Resource

logger = logging.getLogger(__name__)


# Map from TES terminal states to Toil batch job exit reasons
STATE_TO_EXIT_REASON: Dict[str, BatchJobExitReason] = {
    'COMPLETE': BatchJobExitReason.FINISHED,
    'CANCELED': BatchJobExitReason.KILLED,
    'EXECUTOR_ERROR': BatchJobExitReason.FAILED,
    'SYSTEM_ERROR': BatchJobExitReason.ERROR,
    'UNKNOWN': BatchJobExitReason.ERROR
}


class TESBatchSystem(BatchSystemCleanupSupport):
    @classmethod
    def supportsAutoDeployment(cls) -> bool:
        return True

    @classmethod
    def get_default_tes_endpoint(cls) -> str:
        """
        Get the default TES endpoint URL to use.

        (unless overridden by an option or environment variable)
        """
        return f'http://{get_public_ip()}:8000'

    def __init__(self, config: Config, maxCores: float, maxMemory: int, maxDisk: int) -> None:
        super().__init__(config, maxCores, maxMemory, maxDisk)
        # Connect to TES, using Funnel-compatible environment variables to fill in credentials if not specified.
        self.tes = tes.HTTPClient(config.tes_endpoint,
                                  user=config.tes_user,
                                  password=config.tes_password,
                                  token=config.tes_bearer_token)

        # Get service info from the TES server and pull out supported storages.
        # We need this so we can tell if the server is likely to be able to
        # mount any of our local files. These are URL bases that the server
        # supports.
        server_info = self.tes.get_service_info()
        logger.debug("Detected TES server info: %s", server_info)
        self.server_storages = server_info.storage or []

        # Define directories to mount for each task, as py-tes Input objects
        self.mounts: List[tes.Input] = []

        if config.jobStore:
            job_store_type, job_store_path = Toil.parseLocator(config.jobStore)
            if job_store_type == 'file':
                # If we have a file job store, we want to mount it at the same path, if we can
                self.__mount_local_path_if_possible(job_store_path, job_store_path)

        # If we have AWS credentials, we want to mount them in our home directory if we can.
        aws_credentials_path = os.path.join(os.path.expanduser("~"), '.aws')
        if os.path.isdir(aws_credentials_path):
            self.__mount_local_path_if_possible(aws_credentials_path, '/root/.aws')

        # We assign job names based on a numerical job ID. This functionality
        # is managed by the BatchSystemLocalSupport.

        # Here is where we will store the user script resource object if we get one.
        self.user_script: Optional[Resource] = None

        # Ge the image to deploy from Toil's configuration
        self.docker_image = applianceSelf()

        # We need a way to map between our batch system ID numbers, and TES task IDs from the server.
        self.bs_id_to_tes_id: Dict[int, str] = {}
        self.tes_id_to_bs_id: Dict[str, int] = {}

    def __server_can_mount(self, url: str) -> bool:
        """
        Return true if the given URL is under a supported storage location for
        the TES server, and false otherwise.
        """
        # TODO: build some kind of fast matcher in case there are a lot of
        # storages supported.

        for base_url in self.server_storages:
            if url.startswith(base_url):
                return True
        return False

    def __mount_local_path_if_possible(self, local_path: str, container_path: str) -> None:
        """
        If a local path is somewhere the server thinks it can access, mount it
        into all the tasks.
        """
        # TODO: We aren't going to work well with linked imports if we're mounting the job store into the container...

        path_url = 'file://' + os.path.abspath(local_path)
        if os.path.exists(local_path) and self.__server_can_mount(path_url):
            # We can access this file from the server. Probably.
            self.mounts.append(tes.Input(url=path_url,
                                         path=container_path,
                                         type="DIRECTORY" if os.path.isdir(local_path) else "FILE"))

    def setUserScript(self, user_script: Resource) -> None:
        logger.debug('Setting user script for deployment: {}'.format(user_script))
        self.user_script = user_script

    # setEnv is provided by BatchSystemSupport, updates self.environment

    def issueBatchJob(self, job_desc: JobDescription, job_environment: Optional[Dict[str, str]] = None) -> int:
        # TODO: get a sensible self.maxCores, etc. so we can checkResourceRequest.
        # How do we know if the cluster will autoscale?

        # Try the job as local
        local_id = self.handleLocalJob(job_desc)
        if local_id is not None:
            # It is a local job
            return local_id
        else:
            # We actually want to send to the cluster

            # Check resource requirements (managed by BatchSystemSupport)
            self.checkResourceRequest(job_desc.memory, job_desc.cores, job_desc.disk)

            # Make a batch system scope job ID
            bs_id = self.getNextJobID()
            # Make a vaguely human-readable name.
            # TES does not require it to be unique.
            # We could add a per-workflow prefix to use with ListTasks, but
            # ListTasks doesn't let us filter for newly done tasks, so it's not
            # actually useful for us over polling each task.
            job_name = str(job_desc)

            # Launch the job on TES

            # Determine job environment
            environment = self.environment.copy()
            if job_environment:
                environment.update(job_environment)

            # Make a job dict to send to the executor.
            # TODO: Factor out executor setup from here and Kubernetes
            job: Dict[str, Any] = {"command": job_desc.command}
            if self.user_script is not None:
                # If there's a user script resource be sure to send it along
                job['userScript'] = self.user_script
            # Encode it in a form we can send in a command-line argument. Pickle in
            # the highest protocol to prevent mixed-Python-version workflows from
            # trying to work. Make sure it is text so we can ship it to Kubernetes
            # via JSON.
            encoded_job = base64.b64encode(pickle.dumps(job, pickle.HIGHEST_PROTOCOL)).decode('utf-8')
            # Make a command to run it in the exacutor
            command_list = ['_toil_contained_executor', encoded_job]

            # Make the sequence of TES containers ("executors") to run.
            # We just run one which is the Toil executor to grab the user
            # script and do the job.
            task_executors = [tes.Executor(image=self.docker_image,
                command=command_list,
                env=environment
            )]

            # Prepare inputs.
            task_inputs = list(self.mounts)
            # If we had any per-job input files they would come in here.

            # Package into a TES Task
            task = tes.Task(name=job_name, executors=task_executors, inputs=task_inputs)

            # Launch it and get back the TES ID that we can use to poll the task
            tes_id = self.tes.create_task(task)

            # Tie it to the numeric ID
            self.bs_id_to_tes_id[bs_id] = tes_id
            self.tes_id_to_bs_id[tes_id] = bs_id

            logger.debug('Launched job: %s', job_name)

            return bs_id

    def __get_runtime(self, task: tes.Task) -> Optional[float]:
        """
        Get the time that the given job ran/has been running for, in seconds,
        or None if that time is not available. Never returns 0.
        """
        start_time = None
        end_time = utc_now()
        for log in (task.logs or []):
            if log.start_time:
                # Find the first start time that is set
                start_time = log.start_time
                break

        if not start_time:
            # It hasn't been running for a measurable amount of time.
            return None

        for log in reversed(task.logs or []):
             if log.end_time:
                # Find the last end time that is set, and override now
                end_time = log.end_time
                break
        # We have a set start time, so it is/was running. Return the time
        # it has been running for.
        return slow_down((end_time - start_time).total_seconds())

    def __get_exit_code(self, task: tes.Task) -> int:
        """
        Get the exit code of the last executor with a log in the task, or
        EXIT_STATUS_UNAVAILABLE_VALUE if no executor has a log.
        """
        for task_log in reversed(task.logs or []):
             for executor_log in reversed(task_log.logs or []):
                 if isinstance(executor_log.exit_code, int):
                    # Find the last executor exit code that is a number and return it
                    return executor_log.exit_code

        if task.state == 'COMPLETE':
            # If the task completes without error but has no code logged, the
            # code must be 0.
            return 0

        # If we get here we couldn't find an exit code.
        return EXIT_STATUS_UNAVAILABLE_VALUE

    def getUpdatedBatchJob(self, maxWait: int) -> Optional[UpdatedBatchJobInfo]:
        # Remember when we started, for respecting the timeout
        entry = datetime.datetime.now()
        # This is the updated job we have found, if any
        result = None
        while result is None and ((datetime.datetime.now() - entry).total_seconds() < maxWait or not maxWait):
            result = self.getUpdatedLocalJob(0)

            if result:
                return result

            # Collect together the list of TES and batch system IDs for tasks we
            # are acknowledging and don't care about anymore.
            acknowledged = []

            for tes_id, bs_id in self.tes_id_to_bs_id.items():
                # Immediately poll all the jobs we issued.
                # TODO: There's no way to acknowledge a finished job, so there's no
                # faster way to find the newly finished jobs than polling
                task = self.tes.get_task(tes_id)
                if task.state in ["COMPLETE", "CANCELED", "EXECUTOR_ERROR", "SYSTEM_ERROR"]:
                    # This task is done!
                    logger.debug("Found stopped task: %s", task)

                    # Acknowledge it
                    acknowledged.append((tes_id, bs_id))

                    if task.state == "CANCELED":
                        # Killed jobs aren't allowed to appear as updated.
                        continue

                    # Otherwise, it stopped running and it wasn't our fault.

                    # Record runtime
                    runtime = self.__get_runtime(task)

                    # Determine if it succeeded
                    exit_reason = STATE_TO_EXIT_REASON[task.state]

                    # Get its exit code
                    exit_code = self.__get_exit_code(task)

                    # Compose a result
                    result = UpdatedBatchJobInfo(jobID=bs_id, exitStatus=exit_code, wallTime=runtime, exitReason=exit_reason)

                    # No more iteration needed, we found a result.
                    break

            # After the iteration, drop all the records for tasks we acknowledged
            for (tes_id, bs_id) in acknowledged:
                del self.tes_id_to_bs_id[tes_id]
                del self.bs_id_to_tes_id[bs_id]

            if not maxWait:
                # Don't wait at all
                break
            elif result is None:
                # Wait a bit and poll again
                time.sleep(min(maxWait/2, 1.0))

        # When we get here we have all the result we can get
        return result

    def shutdown(self) -> None:

        # Shutdown local processes first
        self.shutdownLocal()

        for tes_id in self.tes_id_to_bs_id.keys():
            # Shut down all the TES jobs we issued.
            self.__try_cancel(tes_id)

    def __try_cancel(self, tes_id: str) -> None:
        """
        Try to cancel a TES job.

        Succeed if it can't be canceled because it has stopped,
        but fail if it can't be canceled for some other reason.
        """
        try:
            # Kill each of our tasks in TES
            self.tes.cancel_task(tes_id)
        except HTTPError as e:
            if e.response is not None and e.response.status_code in [409, 500]:
                # TODO: This is what we probably get when trying to cancel
                # something that is actually done. But can we rely on that?
                pass
            elif '500' in str(e) or '409' in str(e):
                # TODO: drop this after <https://github.com/ohsu-comp-bio/py-tes/pull/36> merges.
                # py-tes might be hiding the actual code and just putting it in a string
                pass
            else:
                raise

    def getIssuedBatchJobIDs(self) -> List[int]:
        return self.getIssuedLocalJobIDs() + list(self.bs_id_to_tes_id.keys())

    def getRunningBatchJobIDs(self) -> Dict[int, float]:
        # We need a dict from job_id (integer) to seconds it has been running
        bs_id_to_runtime = {}

        for tes_id, bs_id in self.tes_id_to_bs_id.items():
            # Poll every issued task.
            # TODO: use list_tasks filtering by name prefix and running state!
            task = self.tes.get_task(tes_id)
            logger.debug("Observed task: %s", task)
            if task.state in ["INITIALIZING", "RUNNING"]:
                # We count INITIALIZING tasks because they may be e.g. pulling
                # Docker containers, and we don't want to time out on them in
                # the tests. But they may not have any runtimes, so it might
                # not really help.
                runtime = self.__get_runtime(task)
                if runtime:
                    # We can measure a runtime
                    bs_id_to_runtime[bs_id] = runtime
                # If we can't find a runtime, we can't say it's running
                # because we can't say how long it has been running for.

        # Give back the times all our running jobs have been running for.
        return bs_id_to_runtime

    def killBatchJobs(self, job_ids: List[int]) -> None:
        # Kill all the ones that are local
        self.killLocalJobs(job_ids)

        # TODO: implement
        for bs_id in job_ids:
            if bs_id in self.bs_id_to_tes_id:
                # We sent this to TES. So try to cancel it.
                self.__try_cancel(self.bs_id_to_tes_id[bs_id])
                # But don't forget the mapping until we actually get the finish
                # notification for the job.

    @classmethod
    def add_tes_options(cls, parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
        parser.add_argument("--tesEndpoint", dest="tes_endpoint", default=cls.get_default_tes_endpoint(),
                            help="The http(s) URL of the TES server.  (default: %(default)s)")
        parser.add_argument("--tesUser", dest="tes_user", default=None,
                            help="User name to use for basic authentication to TES server.")
        parser.add_argument("--tesPassword", dest="tes_password", default=None,
                            help="Password to use for basic authentication to TES server.")
        parser.add_argument("--tesBearerToken", dest="tes_bearer_token", default=None,
                            help="Bearer token to use for authentication to TES server.")

    @classmethod
    def setOptions(cls, setOption: Callable[..., None]) -> None:
        # Because we use the keyword arguments, we can't specify a type for setOption without using Protocols.
        # TODO: start using Protocols, or just start returning objects to represent the options.
        # When actually parsing options, remember to check the environment variables
        setOption("tes_endpoint", default=cls.get_default_tes_endpoint(), env=["TOIL_TES_ENDPOINT"])
        setOption("tes_user", default=None, env=["TOIL_TES_USER"])
        setOption("tes_password", default=None, env=["TOIL_TES_PASSWORD"])
        setOption("tes_bearer_token", default=None, env=["TOIL_TES_BEARER_TOKEN"])
