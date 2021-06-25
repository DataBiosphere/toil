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

from toil.job import CheckpointJobDescription, JobDescription
from toil.jobStores.abstractJobStore import AbstractJobStore
from typing import List, Dict, Set, Tuple, Iterator, Optional

logger = logging.getLogger(__name__)


class ToilState:
    """
    Holds the leader's scheduling information that does not need to be
    persisted back to the JobStore (such as information on completed and
    outstanding predecessors).

    Only holds JobDescription objects, not Job objects, and those
    JobDescription objects only exist in single copies.

    Everything else in the leader should reference JobDescriptions by ID
    instead of moving them around between lists.
    """
    def __init__(self, jobStore: AbstractJobStore, rootJob: JobDescription, jobCache: Optional[Dict[str, JobDescription]] = None):
        """
        Loads the state from the jobStore, using the rootJob
        as the root of the job DAG.

        The jobCache is a map from jobStoreID to JobDescription or None. Is
        used to speed up the building of the state when loading initially from
        the JobStore, and is not preserved.

        :param jobStore: The job store to use.

        :param rootJob: The description for the root job of the workflow being run.

        :param jobCache: A dict to cache downloaded job descriptions in, keyed by ID.
        """

        # Maps from successor (child or follow-on) jobStoreID to predecessor jobStoreID
        self.successorJobStoreIDToPredecessorJobs: Dict[str, List[JobDescription]] = {}

        # Hash of jobStoreIDs to counts of numbers of successors issued.
        # There are no entries for jobs without successors in this map.
        self.successorCounts: Dict[str, int] = {}

        # This is a hash of service jobs, referenced by jobStoreID, to their predecessor job
        self.serviceJobStoreIDToPredecessorJob: Dict[str, JobDescription] = {}

        # Hash of jobStoreIDs mapping to dict from service ID to service host JobDescription for issued services
        self.servicesIssued: Dict[str, JobDescription] = {}

        # Jobs that are ready to be processed.
        # Stored as a dict from job store ID to a pair of (job, result status),
        # where a status other than 0 indicates that an error occurred when
        # running the job.
        self.updatedJobs: Dict[str, Tuple[JobDescription, int]] = {}

        # The set of totally failed jobs - this needs to be filtered at the
        # end to remove jobs that were removed by checkpoints
        self.totalFailedJobs: Set[JobDescription] = set()

        # Jobs (as jobStoreIDs) with successors that have totally failed
        self.hasFailedSuccessors: Set[str] = set()

        # The set of successors of failed jobs as a set of jobStoreIds
        self.failedSuccessors: Set[str] = set()

        # Set of jobs that have multiple predecessors that have one or more predecessors
        # finished, but not all of them. This acts as a cache for these jobs.
        # Stored as hash from jobStoreIDs to JobDescriptions
        self.jobsToBeScheduledWithMultiplePredecessors: Dict[str, JobDescription] = {}

        # Algorithm to build this information
        self._buildToilState(rootJob, jobStore, jobCache)


    def allJobDescriptions(self) -> Iterator[JobDescription]:
        """
        Returns an iterator over all JobDescription objects referenced by the
        ToilState, with some possibly being visited multiple times.
        """

        for item in self.serviceJobStoreIDToPredecessorJob.values():
            assert isinstance(item, JobDescription)
            yield item

        for item in (desc for mapping in self.servicesIssued.values() for desc in mapping.values()):
            assert isinstance(item, JobDescription)
            yield item

        for item in (pair[0] for pair in self.updatedJobs.values()):
            assert isinstance(item, JobDescription)
            yield item

        for item in self.totalFailedJobs:
            assert isinstance(item, JobDescription)
            yield item

        for item in self.jobsToBeScheduledWithMultiplePredecessors.values():
            assert isinstance(item, JobDescription)
            yield item

    def _buildToilState(self, jobDesc: JobDescription, jobStore: AbstractJobStore, jobCache: Optional[Dict[str, JobDescription]] = None) -> None:
        """
        Traverses tree of jobs from the root JobDescription (rootJob) building the
        ToilState class.

        If jobCache is passed, it must be a dict from job ID to JobDescription
        object. Jobs will be loaded from the cache (which can be downloaded from
        the jobStore in a batch) instead of piecemeal when recursed into.

        :param jobDesc: The description for the root job of the workflow being run.

        :param jobStore: The job store to use.

        :param jobCache: A dict to cache downloaded job descriptions in, keyed by ID.
        """

        def getJob(jobId: str) -> JobDescription:
            if jobCache is not None:
                if jobId in jobCache:
                    return jobCache[jobId]
            return jobStore.load(jobId)

        # If the job description has a command, is a checkpoint, has services
        # or is ready to be deleted it is ready to be processed
        if jobDesc.command is not None or (isinstance(jobDesc, CheckpointJobDescription) and jobDesc.checkpoint is not None) or len(jobDesc.services) > 0 or jobDesc.nextSuccessors() is None:
            logger.debug('Found job to run: %s, with command: %s, with checkpoint: %s, '
                         'with  services: %s, with no next successors: %s', jobDesc.jobStoreID,
                         jobDesc.command is not None, isinstance(jobDesc, CheckpointJobDescription) and jobDesc.checkpoint is not None,
                         len(jobDesc.services) > 0, jobDesc.nextSuccessors() is None)
            self.updatedJobs[jobDesc.jobStoreID] = (jobDesc, 0)

            if isinstance(jobDesc, CheckpointJobDescription) and jobDesc.checkpoint is not None:
                jobDesc.command = jobDesc.checkpoint

        else: # There exist successors
            logger.debug("Adding job: %s to the state with %s successors" % (jobDesc.jobStoreID, len(jobDesc.nextSuccessors())))

            # Record the number of successors
            self.successorCounts[jobDesc.jobStoreID] = len(jobDesc.nextSuccessors())

            def processSuccessorWithMultiplePredecessors(successor: JobDescription) -> None:
                # If jobDesc is not reported as complete by the successor
                if jobDesc.jobStoreID not in successor.predecessorsFinished:

                    # Update the successor's status to mark the predecessor complete
                    successor.predecessorsFinished.add(jobDesc.jobStoreID)

                # If the successor has no predecessors to finish
                assert len(successor.predecessorsFinished) <= successor.predecessorNumber
                if len(successor.predecessorsFinished) == successor.predecessorNumber:

                    # It is ready to be run, so remove it from the cache
                    self.jobsToBeScheduledWithMultiplePredecessors.pop(successorJobStoreID)

                    # Recursively consider the successor
                    self._buildToilState(successor, jobStore, jobCache=jobCache)

            # For each successor
            for successorJobStoreID in jobDesc.nextSuccessors():

                # If the successor does not yet point back at a
                # predecessor we have not yet considered it
                if successorJobStoreID not in self.successorJobStoreIDToPredecessorJobs:

                    # Add the job as a predecessor
                    self.successorJobStoreIDToPredecessorJobs[successorJobStoreID] = [jobDesc]

                    # We load the successor job
                    successor = getJob(successorJobStoreID)

                    # If predecessor number > 1 then the successor has multiple predecessors
                    if successor.predecessorNumber > 1:

                        # We put the successor job in the cache of successor jobs with multiple predecessors
                        assert successorJobStoreID not in self.jobsToBeScheduledWithMultiplePredecessors
                        self.jobsToBeScheduledWithMultiplePredecessors[successorJobStoreID] = successor

                        # Process successor
                        processSuccessorWithMultiplePredecessors(successor)

                    else:
                        # The successor has only this job as a predecessor so
                        # recursively consider the successor
                        self._buildToilState(successor, jobStore, jobCache=jobCache)

                else:
                    # We've already seen the successor

                    # Add the job as a predecessor
                    assert jobDesc not in self.successorJobStoreIDToPredecessorJobs[successorJobStoreID]
                    self.successorJobStoreIDToPredecessorJobs[successorJobStoreID].append(jobDesc)

                    # If the successor has multiple predecessors
                    if successorJobStoreID in self.jobsToBeScheduledWithMultiplePredecessors:

                        # Get the successor from cache
                        successor = self.jobsToBeScheduledWithMultiplePredecessors[successorJobStoreID]

                        # Process successor
                        processSuccessorWithMultiplePredecessors(successor)
