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
import getpass
import logging
import os
import pickle
import string
import subprocess
import sys
import tempfile
import time
import uuid
from typing import Optional, Dict

from toil import applianceSelf
from toil.batchSystems.abstractBatchSystem import (EXIT_STATUS_UNAVAILABLE_VALUE,
                                                   BatchJobExitReason,
                                                   BatchSystemCleanupSupport,
                                                   UpdatedBatchJobInfo)
from toil.common import Toil
from toil.job import JobDescription
from toil.lib.conversions import human2bytes
from toil.lib.retry import ErrorCondition, retry
from toil.resource import Resource
from toil.statsAndLogging import configure_root_logger, set_log_level

logger = logging.getLogger(__name__)


class TESBatchSystem(BatchSystemCleanupSupport):
    @classmethod
    def supportsAutoDeployment(cls):
        return True

    def __init__(self, config, max_cores, max_memory, max_disk):
        super().__init__(config, max_cores, max_memory, max_disk)

        # Generate a unique ID for this execution.
        self.unique_id = uuid.uuid4()

        # Create a prefix for jobs
        self.job_prefix = 'toil-{}-'.format(self.unique_id)

        # We assign job names based on a numerical job ID. This functionality
        # is managed by the BatchSystemLocalSupport.

        # Here is where we will store the user script resource object if we get one.
        self.user_script = None

        # Ge the image to deploy from Toil's configuration
        self.docker_image = applianceSelf()
        
    def setUserScript(self, user_script):
        logger.info('Setting user script for deployment: {}'.format(user_script))
        self.user_script = user_script

    # setEnv is provided by BatchSystemSupport, updates self.environment

    def issueBatchJob(self, job_desc, job_environment: Optional[Dict[str, str]] = None):
        # TODO: get a sensible self.maxCores, etc. so we can checkResourceRequest.
        # How do we know if the cluster will autoscale?

        # Try the job as local
        localID = self.handleLocalJob(job_desc)
        if localID is not None:
            # It is a local job
            return localID
        else:
            # We actually want to send to the cluster

            # Check resource requirements (managed by BatchSystemSupport)
            self.checkResourceRequest(job_desc.memory, job_desc.cores, job_desc.disk)

            # Make a batch system scope job ID
            job_id = self.getNextJobID()
            # Make a unique name
            job_name = self.job_prefix + str(job_id)

            # TODO: launch the job on TES 

            logger.debug('Launched job: %s', job_name)

            return job_id

    def getUpdatedBatchJob(self, maxWait):
        pass
        # TODO: implement

    def shutdown(self):

        # Shutdown local processes first
        self.shutdownLocal()


        # TODO: kill our tasks in TES


    def getIssuedBatchJobIDs(self):
        # TODO: implement
        return self.getIssuedLocalJobIDs() + []

    def getRunningBatchJobIDs(self):
        # We need a dict from job_id (integer) to seconds it has been running
        # TODO: implement
        pass

    def killBatchJobs(self, jobIDs):

        # Kill all the ones that are local
        self.killLocalJobs(jobIDs)

        # TODO: implement

# TODO: factor out the Kubernetes executor, and use it for anything we can't/don't translate into idiomatic TES.
