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
Batch system for running Toil workflows on AWS Batch.

Useful with the AWS job store.

AWS Batch has no means for scheduling based on disk usage, so the backing
machines need to have "enough" disk and other constraints need to guarantee
that disk does not fill.

Assumes that an AWS Batch Queue name or ARN is already provided.

Handles creating and destroying a JobDefinition for the workflow run.

Additional containers should be launched with Singularity, not Docker.
"""
import base64
import datetime
import logging
import math
import os
import pickle
import time
import uuid
from argparse import ArgumentParser, _ArgumentGroup
from typing import Any, Callable, Dict, List, Optional, Set, Union

from requests.exceptions import HTTPError
from boto.exception import BotoServerError

from toil import applianceSelf
from toil.batchSystems.abstractBatchSystem import (EXIT_STATUS_UNAVAILABLE_VALUE,
                                                   BatchJobExitReason,
                                                   UpdatedBatchJobInfo)
from toil.batchSystems.cleanup_support import BatchSystemCleanupSupport
from toil.common import Config, Toil
from toil.job import JobDescription
from toil.lib.aws import client
from toil.lib.conversions import to_mib, from_mib
from toil.lib.misc import slow_down, utc_now, unix_now_ms
from toil.lib.retry import retry
from toil.resource import Resource

logger = logging.getLogger(__name__)


# Map from AWS Batch terminal states to Toil batch job exit reasons
STATE_TO_EXIT_REASON: Dict[str, BatchJobExitReason] = {
    'SUCCEEDED': BatchJobExitReason.FINISHED,
    'FAILED': BatchJobExitReason.FAILED
}

# What's the max polling list size?
MAX_POLL_COUNT = 100

class AWSBatchBatchSystem(BatchSystemCleanupSupport):
    @classmethod
    def supportsAutoDeployment(cls) -> bool:
        return True

    def __init__(self, config: Config, maxCores: float, maxMemory: int, maxDisk: int) -> None:
        super().__init__(config, maxCores, maxMemory, maxDisk)
        # Connect to AWS Batch
        self.client = client('batch')
        
        # Determine our batch queue
        self.queue = config.aws_batch_queue
        if self.queue is None:
            # Make sure we actually have a queue
            raise RuntimeError("To use AWS Batch, --awsBatchQueue or TOIL_AWS_BATCH_QUEUE must be set")
        self.job_role_arn = config.aws_batch_job_role_arn
        
        # Try and guess what Toil work dir the workers will use.
        # We need to be able to provision (possibly shared) space there.
        # TODO: Deduplicate with Kubernetes batch system.
        self.worker_work_dir = Toil.getToilWorkDir(config.workDir)
        if (config.workDir is None and
            os.getenv('TOIL_WORKDIR') is None and
            self.worker_work_dir == tempfile.gettempdir()):

            # We defaulted to the system temp directory. But we think the
            # worker Dockerfiles will make them use /var/lib/toil instead.
            # TODO: Keep this in sync with the Dockerfile.
            self.worker_work_dir = '/var/lib/toil'

        # We assign job names based on a numerical job ID. This functionality
        # is managed by the BatchSystemLocalSupport.

        # Here is where we will store the user script resource object if we get one.
        self.user_script: Optional[Resource] = None

        # Get the image to deploy from Toil's configuration
        self.docker_image = applianceSelf()

        # We can't use AWS Batch without a job definition. But we can use one
        # of them for all the jobs. We want to lazily initialize it. This will
        # be an ARN.
        self.job_definition: Optional[str] = None

        # We need a way to map between our batch system ID numbers, and AWS Batch job IDs from the server.
        self.bs_id_to_aws_id: Dict[int, str] = {}
        self.aws_id_to_bs_id: Dict[str, int] = {}
        # We need to track if jobs were killed so they don't come out as updated
        self.killed_job_aws_ids: Set[str] = set()

    def setUserScript(self, user_script: Resource) -> None:
        logger.debug('Setting user script for deployment: {}'.format(user_script))
        self.user_script = user_script

    # setEnv is provided by BatchSystemSupport, updates self.environment

    def issueBatchJob(self, job_desc: JobDescription, job_environment: Optional[Dict[str, str]] = None) -> int:
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
            # We could add a per-workflow prefix to use with ListTasks, but
            # ListTasks doesn't let us filter for newly done tasks, so it's not
            # actually useful for us over polling each task.
            job_name = str(job_desc)

            # Launch the job on AWS Batch

            # Determine job environment
            environment = self.environment.copy()
            if job_environment:
                environment.update(job_environment)

            # Make a job dict to send to the executor.
            # TODO: Factor out executor setup from here and Kubernetes and TES
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
            
            # Compose a job sepc to submit
            job_spec = {
                'jobName': job_name,
                'jobQueue': self.queue,
                'jobDefinition': self.__get_or_create_job_definition(),
                'containerOverrides': {
                    'command': command_list,
                    'environment': [{'name': k, 'value': v} for k, v in job_environment.items()}],
                    'resourceRequirements': [
                        {'type': 'MEMORY', 'value': math.ceil(math.max(4, to_mib(job_desc.memory)))},  # A min of 4 is demanded by the API
                        {'type': 'VCPU', 'value', math.ceil(job_desc.cores)}  # Must be at least 1 on EC2, probably can't be 1.5
                    ]
                }
            }

            # Launch it and get back the AWS ID that we can use to poll the task.
            # TODO: retry!
            response = self.client.submit_job(**job_spec)
            aws_id = response['jobId']

            # Tie it to the numeric ID
            self.bs_id_to_aws_id[bs_id] = aws_id
            self.aws_id_to_bs_id[aws_id] = bs_id

            logger.debug('Launched job: %s', job_name)

            return bs_id
            

    def __get_runtime(self, job_detail: Dict[str, Any]) -> Optional[float]:
        """
        Get the time that the given job ran/has been running for, in seconds,
        or None if that time is not available. Never returns 0.
        
        Takes an AWS JobDetail as a dict.
        """
        
        if job_detail['status'] not in ['RUNNING', 'SUCCEEDED', 'FAILED']:
            # Job is not running yet.
            return None
            
        start_ms = job_detail['startedAt']
        end_ms = unix_now_ms()
        
        if 'stoppedAt' in job_detail:
            end_ms = job_detail['stoppedAt']
        
        # We have a set start time, so it is/was running. Return the time
        # it has been running for.
        return slow_down((end_ms - start_ms) / 1000)

    def __get_exit_code(self, job_detail: Dict[str, Any]) -> int:
        """
        Get the exit code of the given JobDetail, or
        EXIT_STATUS_UNAVAILABLE_VALUE if it cannot be gotten.
        """
        
        return job_detail.get('container', {}).get('exitCode', EXIT_STATUS_UNAVAILABLE_VALUE) 

    def getUpdatedBatchJob(self, maxWait: int) -> Optional[UpdatedBatchJobInfo]:
        # Remember when we started, for respecting the timeout
        entry = datetime.datetime.now()
        # This is the updated job we have found, if any
        result = None
        while result is None and ((datetime.datetime.now() - entry).total_seconds() < maxWait or not maxWait):
            result = self.getUpdatedLocalJob(0)

            if result:
                return result

            # Collect together the list of AWS and batch system IDs for tasks we
            # are acknowledging and don't care about anymore.
            acknowledged = []

            # Get all the AWS IDs to poll
            to_check = list(aws_and_bs_id[0] for aws_and_bs_id in self.aws_id_to_bs_id.items())
            
            while len(to_check) > 0:
                # Go through jobs we want to poll in batches of the max size
                check_batch = to_check[-MAX_POLL_COUNT:]
                to_check = to_check[:MAX_POLL_COUNT - 1]
                
                # TODO: retry
                response = self.client.describe_jobs(jobs=to_check)
                
                for job_detail in response.get('jobs', []):
                    if job_detail.get('status') in ['SUCCEEDED', 'FAILED']:
                        # This job is done!

                    logger.debug("Found stopped job: %s", job_detail)
                    aws_id = job_detail['jobId']
                    bs_id = self.aws_id_to_bs_id[aws_id]

                    # Acknowledge it
                    acknowledged.append((aws_id, bs_id))

                    if job_detail['jobId'] in self.killed_job_aws_ids:
                        # Killed jobs aren't allowed to appear as updated.
                        continue

                    # Otherwise, it stopped running and it wasn't our fault.

                    # Record runtime
                    runtime = self.__get_runtime(job_detail)

                    # Determine if it succeeded
                    exit_reason = STATE_TO_EXIT_REASON[job_detail['status']]

                    # Get its exit code
                    exit_code = self.__get_exit_code(job_detail)

                    # Compose a result
                    result = UpdatedBatchJobInfo(jobID=bs_id, exitStatus=exit_code, wallTime=runtime, exitReason=exit_reason)

                    # No more iteration needed, we found a result.
                    break

            # After the iteration, drop all the records for tasks we acknowledged
            for (aws_id, bs_id) in acknowledged:
                del self.aws_id_to_bs_id[aws_id]
                del self.bs_id_to_aws_id[bs_id]
                if aws_id in self.killed_job_aws_ids:
                    # We don't need to remember that we killed this job anymore.
                    self.killed_job_aws_ids.remove(aws_id)

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

        for aws_id in self.aws_id_to_bs_id.keys():
            # Shut down all the AWS jobs we issued.
            self.__try_terminate(aws_id)
            
        # Get rid of the job definition we are using if we can.
        self.__destroy_job_definition()

    @retry(errors=[BotoServerError])
    def __try_terminate(self, aws_id: str) -> None:
        """
        Try to terminate an AWS Batch job.

        Succeed if it can't be canceled because it has stopped,
        but fail if it can't be canceled for some other reason.
        """
        # Remember that we killed this job so we don't show it as updated
        # later.
        self.killed_job_aws_ids.add(aws_id)
        # Kill the AWS Batch job
        self.client.terminate_job(jobId=aws_id, reason='Killed by Toil')
    
    @retry(errors=[BotoServerError])
    def __get_or_create_job_definition(self) -> str:
        """
        Create, if not already created, and return the ARN for the
        JobDefinition for this workflow run.
        """
        if self.job_definition is None:
            job_def_spec = {
                'jobDefinitionName': 'toil-' + str(uuid.uuid4()),
                'type': 'container',
                'containerProperties': {'image': self.docker_image},
                # Unlike the Lubernetes batch system we always mount the Toil
                # workDir onto the host. Hopefully it has its ephemeral dosks mounted there.
                # TODO: Where do the default batch AMIs mount their ephemeral disks, if anywhere?
                'volumes': [{'name': 'workdir', 'host': {'sourcePath': '/var/lib/toil'}}],
                'mountPoints': [{'containerPath': self.worker_work_dir, 'sourceVolume': 'workdir'}],
                'retryStrategy': {'attempts': 1}
            }
            if self.job_role_arn:
                # We need to give the job a role.
                # We might not be able to do much job store access without this!
                job_def_spec['jobRoleArn'] = self.job_role_arn
            response = self.client.register_job_definition(**job_def_spec)
            self.job_definition = response['jobDefinitionArn']
    
        return self.job_definition
    
    @retry(errors=[BotoServerError]) 
    def __destroy_job_definition(self) -> None:
        """
        Destroy any job definition we have created for this workflow run.
        """
        if self.job_definition is not None:
            self.client.deregister_job_definition(jobDefinition=self.job_definition)
            # TODO: How do we tolerate it not existing anymore?
            self.job_definition = None

    def getIssuedBatchJobIDs(self) -> List[int]:
        return self.getIssuedLocalJobIDs() + list(self.bs_id_to_aws_id.keys())

    def getRunningBatchJobIDs(self) -> Dict[int, float]:
        # We need a dict from job_id (integer) to seconds it has been running
        bs_id_to_runtime = {}

        # Get all the AWS IDs to poll
        to_check = list(aws_and_bs_id[0] for aws_and_bs_id in self.aws_id_to_bs_id.items())
        
        while len(to_check) > 0:
            # Go through jobs we want to poll in batches of the max size
            check_batch = to_check[-MAX_POLL_COUNT:]
            to_check = to_check[:MAX_POLL_COUNT - 1]
            
            # TODO: retry
            response = self.client.describe_jobs(jobs=to_check)
            
            for job_detail in response.get('jobs', []):
                if job_detail.get('state') in ['STARTING', 'RUNNING']:
                    runtime = self.__get_runtime(job_detail)
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

        for bs_id in job_ids:
            if bs_id in self.bs_id_to_aws_id:
                # We sent this to AWS Batch. So try to cancel it.
                self.__try_terminate(self.bs_id_to_aws_id[bs_id])
                # But don't forget the mapping until we actually get the finish
                # notification for the job.

    @classmethod
    def add_options(cls, parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
        parser.add_argument("--awsBatchQueue", dest="aws_batch_queue", default=None,
                            help="The name or ARN of the AWS Batch queue to submit to.")
        parser.add_argument("--awsBatchJobRoleArn", dest="aws_batch_job_role_arn", default=None,
                            help=("The ARN of an IAM role to run AWS Batch jobs as, so they "
                                  "can e.g. access a job store."))

    @classmethod
    def setOptions(cls, setOption: Callable[..., None]) -> None:
        setOption("aws_batch_queue", default=None, env=["TOIL_AWS_BATCH_QUEUE"])
        setOption("aws_batch_job_role_arn", default=None, env=["TOIL_AWS_BATCH_JOB_ROLE_ARN"])
