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

from toil.bus import MessageBus, JobUpdatedMessage
from toil.job import CheckpointJobDescription, JobDescription
from toil.jobStores.abstractJobStore import AbstractJobStore, NoSuchJobException
from typing import List, Dict, Set, Tuple, Iterator, Optional

logger = logging.getLogger(__name__)


class ToilState:
    """
    Holds the leader's scheduling information that does not need to be
    persisted back to the JobStore (such as information on completed and
    outstanding predecessors).

    Holds the true single copies of all JobDescription objects that the Leader
    and ServiceManager will use. The leader and service manager shouldn't do
    their own load() and update() calls on the JobStore; they should go through
    this class.

    Everything in the leader should reference JobDescriptions by ID.

    Only holds JobDescription objects, not Job objects, and those
    JobDescription objects only exist in single copies.
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

        # We need to keep the job store so we can load and save jobs.
        self.__job_store = jobStore

        # This holds the one true copy of every JobDescription in the leader.
        # TODO: Do in-place update instead of assignment when we load so we
        # can't let any non-true copies escape.
        self.__job_database = {}

        # Scheduling messages should go over this bus.
        self.bus = MessageBus()

        # Maps from successor (child or follow-on) jobStoreID to predecessor jobStoreIDs
        self.successor_to_predecessors: Dict[str, Set[str]] = {}

        # Hash of jobStoreIDs to counts of numbers of successors issued.
        # There are no entries for jobs without successors in this map.
        self.successorCounts: Dict[str, int] = {}

        # This is a hash of service jobs, referenced by jobStoreID, to their client's ID
        self.service_to_client: Dict[str, str] = {}

        # Holds, for each client job ID, the job IDs of its services that are
        # currently issued.
        self.servicesIssued: Dict[str, Set[str]] = {}

        # The set of totally failed jobs - this needs to be filtered at the
        # end to remove jobs that were removed by checkpoints
        self.totalFailedJobs: Set[str] = set()

        # Jobs (as jobStoreIDs) with successors that have totally failed
        self.hasFailedSuccessors: Set[str] = set()

        # The set of successors of failed jobs as a set of jobStoreIds
        self.failedSuccessors: Set[str] = set()

        # Set of jobs that have multiple predecessors that have one or more predecessors
        # finished, but not all of them.
        self.jobsToBeScheduledWithMultiplePredecessors: Set[str] = set()

        if jobCache is not None:
            # Load any pre-cached JobDescriptions we were given
            self.__job_database.update(jobCache)

        # Build the state from the jobs
        self._buildToilState(rootJob)

    def job_exists(self, job_id: str) -> bool:
        """
        Returns True if the given job exists right now, and false if it hasn't
        been created or it has been deleted elsewhere.

        Doesn't guarantee that the job will or will not be gettable, if racing
        another process, or if it is still cached.
        """

        return self.__job_store.exists(job_id)

    def get_job(self, job_id: str) -> JobDescription:
        """
        Get the one true copy of the JobDescription with the given ID.
        """
        if job_id not in self.__job_database:
            # Go get the job for the first time
            self.__job_database[job_id] = self.__job_store.load(job_id)
        return self.__job_database[job_id]

    def commit_job(self, job_id: str) -> None:
        """
        Save back any modifications made to a JobDescription retrieved from get_job()
        """
        self.__job_store.update(self.__job_database[job_id])

    def reset_job(self, job_id: str) -> None:
        """
        Discard any local modifications to a JobDescription and make
        modifications from other hosts visible.
        """
        try:
            new_truth = self.__job_store.load(job_id)
        except NoSuchJobException:
            # The job is gone now.
            if job_id in self.__job_database:
                # So forget about it
                del self.__job_database[job_id]
                # TODO: Other collections may still reference it.
            return
        if job_id in self.__job_database:
            # Update the one true copy in place
            old_truth = self.__job_database[job_id]
            old_truth.__dict__.update(new_truth.__dict__)
        else:
            # Just keep the new one
            self.__job_database[job_id] = new_truth

    # The next 3 functions provide tracking of how many successor jobs a given job is waiting on, exposing only legit operations.
    # TODO: turn these into messages?
    def successors_pending(self, predecessor_id: str, count: int) -> None:
        """
        Remember that the given job has the given number more pending
        successors, that have not yet succeeded or failed.
        """
        if predecessor_id not in self.successorCounts:
            self.successorCounts[predecessor_id] = count

        else:
            self.successorCounts[predecessor_id] += count
        logger.debug("Successors: %d more for %s, now have %d", count, predecessor_id, self.successorCounts[predecessor_id])
    def successor_returned(self, predecessor_id: str) -> None:
        """
        Remember that the given job has one fewer pending successors, because one has succeeded or failed.
        """
        if predecessor_id not in self.successorCounts:
            raise RuntimeError(f"Tried to remove successor of {predecessor_id} that wasn't added!")
        else:
            self.successorCounts[predecessor_id] -= 1
            logger.debug("Successors: one fewer for %s, now have %d", predecessor_id, self.successorCounts[predecessor_id])
            if self.successorCounts[predecessor_id] == 0:
                del self.successorCounts[predecessor_id]
    def count_pending_successors(self, predecessor_id: str) -> int:
        """
        Get the number of pending successors of the given job, which have not
        yet succeeded or failed.
        """
        if predecessor_id not in self.successorCounts:
            return 0
        else:
            return self.successorCounts[predecessor_id]


    def _buildToilState(self, jobDesc: JobDescription) -> None:
        """
        Traverses tree of jobs down from the subtree root JobDescription
        (jobDesc), building the ToilState class.

        :param jobDesc: The description for the root job of the workflow being run.
        """

        # If the job description has a command, is a checkpoint, has services
        # or is ready to be deleted it is ready to be processed (i.e. it is updated)
        if jobDesc.command is not None or (isinstance(jobDesc, CheckpointJobDescription) and jobDesc.checkpoint is not None) or len(jobDesc.services) > 0 or jobDesc.nextSuccessors() is None:
            logger.debug('Found job to run: %s, with command: %s, with checkpoint: %s, '
                         'with  services: %s, with no next successors: %s', jobDesc.jobStoreID,
                         jobDesc.command is not None, isinstance(jobDesc, CheckpointJobDescription) and jobDesc.checkpoint is not None,
                         len(jobDesc.services) > 0, jobDesc.nextSuccessors() is None)
            # Set the job updated because we should be able to make progress on it.
            self.bus.put(JobUpdatedMessage(jobDesc.jobStoreID, 0))

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

                    # It is ready to be run, so remove it from the set of waiting jobs
                    self.jobsToBeScheduledWithMultiplePredecessors.remove(successorJobStoreID)

                    # Recursively consider the successor
                    self._buildToilState(successor)

            # For each successor
            for successorJobStoreID in jobDesc.nextSuccessors():

                # If the successor does not yet point back at a
                # predecessor we have not yet considered it
                if successorJobStoreID not in self.successor_to_predecessors:

                    # Add the job as a predecessor
                    self.successor_to_predecessors[successorJobStoreID] = set([jobDesc.jobStoreID])

                    # We load the successor job
                    successor = self.get_job(successorJobStoreID)

                    # If predecessor number > 1 then the successor has multiple predecessors
                    if successor.predecessorNumber > 1:

                        # We put the successor job in the set of waiting successor jobs with multiple predecessors
                        assert successorJobStoreID not in self.jobsToBeScheduledWithMultiplePredecessors
                        self.jobsToBeScheduledWithMultiplePredecessors.add(successorJobStoreID)

                        # Process successor
                        processSuccessorWithMultiplePredecessors(successor)

                    else:
                        # The successor has only this job as a predecessor so
                        # recursively consider the successor
                        self._buildToilState(successor)

                else:
                    # We've already seen the successor

                    # Add the job as a predecessor
                    assert jobDesc not in self.successor_to_predecessors[successorJobStoreID]
                    self.successor_to_predecessors[successorJobStoreID].add(jobDesc.jobStoreID)

                    # If the successor has multiple predecessors
                    if successorJobStoreID in self.jobsToBeScheduledWithMultiplePredecessors:

                        # Get the successor from cache
                        successor = self.get_job(successorJobStoreID)

                        # Process successor
                        processSuccessorWithMultiplePredecessors(successor)
