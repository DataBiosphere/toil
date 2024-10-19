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
import logging
from typing import Optional

from toil.batchSystems.abstractBatchSystem import (
    BatchSystemSupport,
    UpdatedBatchJobInfo,
)
from toil.batchSystems.singleMachine import SingleMachineBatchSystem
from toil.common import Config
from toil.job import JobDescription
from toil.lib.threading import cpu_count

logger = logging.getLogger(__name__)


class BatchSystemLocalSupport(BatchSystemSupport):
    """Adds a local queue for helper jobs, useful for CWL & others."""

    def __init__(
        self, config: Config, maxCores: float, maxMemory: int, maxDisk: int
    ) -> None:
        super().__init__(config, maxCores, maxMemory, maxDisk)
        max_local_jobs = (
            config.max_local_jobs if config.max_local_jobs is not None else cpu_count()
        )
        self.localBatch: SingleMachineBatchSystem = SingleMachineBatchSystem(
            config, maxCores, maxMemory, maxDisk, max_jobs=max_local_jobs
        )

    def handleLocalJob(self, command: str, jobDesc: JobDescription) -> Optional[int]:
        """
        To be called by issueBatchJob.

        Returns the jobID if the jobDesc has been submitted to the local queue,
        otherwise returns None
        """
        if not self.config.run_local_jobs_on_workers and jobDesc.local:
            # Since singleMachine.py doesn't typecheck yet and MyPy is ignoring
            # it, it will raise errors here unless we add type annotations to
            # everything we get back from it. The easiest way to do that seems
            # to be to put it in a variable with a type annotation on it. That
            # somehow doesn't error whereas just returning the value complains
            # we're returning an Any. TODO: When singleMachine.py typechecks,
            # remove all these extra variables.
            local_id: int = self.localBatch.issueBatchJob(command, jobDesc)
            return local_id
        else:
            return None

    def killLocalJobs(self, jobIDs: list[int]) -> None:
        """
        Will kill all local jobs that match the provided jobIDs.

        To be called by killBatchJobs.
        """
        self.localBatch.killBatchJobs(jobIDs)

    def getIssuedLocalJobIDs(self) -> list[int]:
        """To be called by getIssuedBatchJobIDs."""
        local_ids: list[int] = self.localBatch.getIssuedBatchJobIDs()
        return local_ids

    def getRunningLocalJobIDs(self) -> dict[int, float]:
        """To be called by getRunningBatchJobIDs()."""
        local_running: dict[int, float] = self.localBatch.getRunningBatchJobIDs()
        return local_running

    def getUpdatedLocalJob(self, maxWait: int) -> Optional[UpdatedBatchJobInfo]:
        """To be called by getUpdatedBatchJob()."""
        return self.localBatch.getUpdatedBatchJob(maxWait)

    def getNextJobID(self) -> int:
        """Must be used to get job IDs so that the local and batch jobs do not conflict."""
        # TODO: This reaches deep into SingleMachineBatchSystem when it probably shouldn't
        with self.localBatch.jobIndexLock:
            jobID: int = self.localBatch.jobIndex
            self.localBatch.jobIndex += 1
        return jobID

    def shutdownLocal(self) -> None:
        """To be called from shutdown()."""
        self.localBatch.shutdown()
