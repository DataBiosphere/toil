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
import time
from typing import Optional

from toil.bus import JobUpdatedMessage, MessageBus
from toil.job import CheckpointJobDescription, JobDescription
from toil.jobStores.abstractJobStore import AbstractJobStore, NoSuchJobException

logger = logging.getLogger(__name__)


class ToilState:
    """
    Holds the leader's scheduling information.

    But onlt that which does not need to be persisted back to the JobStore (such
    as information on completed and outstanding predecessors)

    Holds the true single copies of all JobDescription objects that the Leader
    and ServiceManager will use. The leader and service manager shouldn't do
    their own load() and update() calls on the JobStore; they should go through
    this class.

    Everything in the leader should reference JobDescriptions by ID.

    Only holds JobDescription objects, not Job objects, and those
    JobDescription objects only exist in single copies.
    """

    def __init__(
        self,
        jobStore: AbstractJobStore,
    ) -> None:
        """
        Create an empty ToilState over the given job store.

        After calling this, you probably want to call load_workflow() to
        initialize the state given the root job, correct jobs to reflect a
        consistent state, and find jobs that need leader attention.

        :param jobStore: The job store to use.
        """
        # We have a public bus for messages about job state changes.
        self.bus = MessageBus()

        # We need to keep the job store so we can load and save jobs.
        self.__job_store = jobStore

        # This holds the one true copy of every JobDescription in the leader.
        # TODO: Do in-place update instead of assignment when we load so we
        # can't let any non-true copies escape.
        self.__job_database: dict[str, JobDescription] = {}

        # Maps from successor (child or follow-on) jobStoreID to predecessor jobStoreIDs
        self.successor_to_predecessors: dict[str, set[str]] = {}

        # Hash of jobStoreIDs to counts of numbers of successors issued.
        # There are no entries for jobs without successors in this map.
        self.successorCounts: dict[str, int] = {}

        # This is a hash of service jobs, referenced by jobStoreID, to their client's ID
        self.service_to_client: dict[str, str] = {}

        # Holds, for each client job ID, the job IDs of its services that are
        # possibly currently issued. Includes every service host that has been
        # given to the service manager by the leader, and hasn't been seen by
        # the leader as stopped yet.
        self.servicesIssued: dict[str, set[str]] = {}

        # Holds the IDs of jobs that are currently issued to the batch system
        # and haven't come back yet.
        # TODO: a bit redundant with leader's issued_jobs_by_batch_system_id
        self.jobs_issued: set[str] = set()

        # The set of totally failed jobs - this needs to be filtered at the
        # end to remove jobs that were removed by checkpoints
        self.totalFailedJobs: set[str] = set()

        # Jobs (as jobStoreIDs) with successors that have totally failed
        self.hasFailedSuccessors: set[str] = set()

        # The set of successors of failed jobs as a set of jobStoreIds
        self.failedSuccessors: set[str] = set()

        # Set of jobs that have multiple predecessors that have one or more predecessors
        # finished, but not all of them.
        self.jobsToBeScheduledWithMultiplePredecessors: set[str] = set()

    def load_workflow(
        self,
        rootJob: JobDescription,
        jobCache: Optional[dict[str, JobDescription]] = None,
    ) -> None:
        """
        Load the workflow rooted at the given job.

        If jobs are loaded that have updated and need to be dealt with by the
        leader, JobUpdatedMessage messages will be sent to the message bus.

        The jobCache is a map from jobStoreID to JobDescription or None. Is
        used to speed up the building of the state when loading initially from
        the JobStore, and is not preserved.

        :param rootJob: The description for the root job of the workflow being run.

        :param jobCache: A dict to cache downloaded job descriptions in, keyed by ID.
        """
        if jobCache is not None:
            # Load any pre-cached JobDescriptions we were given
            self.__job_database.update(jobCache)

        # Build the state from the jobs
        self._buildToilState(rootJob)

    def job_exists(self, job_id: str) -> bool:
        """
        Test if the givin job exists now.

        Returns True if the given job exists right now, and false if it hasn't
        been created or it has been deleted elsewhere.

        Doesn't guarantee that the job will or will not be gettable, if racing
        another process, or if it is still cached.
        """
        return self.__job_store.job_exists(job_id)

    def get_job(self, job_id: str) -> JobDescription:
        """Get the one true copy of the JobDescription with the given ID."""
        if job_id not in self.__job_database:
            # Go get the job for the first time
            self.__job_database[job_id] = self.__job_store.load_job(job_id)
        return self.__job_database[job_id]

    def commit_job(self, job_id: str) -> None:
        """
        Save back any modifications made to a JobDescription.

        (one retrieved from get_job())
        """
        self.__job_store.update_job(self.__job_database[job_id])

    def delete_job(self, job_id: str) -> None:
        """
        Destroy a JobDescription.

        May raise an exception if the job could not be cleaned up (i.e. files
        belonging to it failed to delete).
        """
        # Do the backing delete first
        self.__job_store.delete_job(job_id)
        # If that succeeds, drop from cache
        if job_id in self.__job_database:
            del self.__job_database[job_id]

    def reset_job(self, job_id: str) -> None:
        """
        Discard any local modifications to a JobDescription.

        Will make modifications from other hosts visible.
        """
        try:
            new_truth = self.__job_store.load_job(job_id)
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
            old_truth.assert_is_not_newer_than(new_truth)
            old_truth.__dict__.update(new_truth.__dict__)
        else:
            # Just keep the new one
            self.__job_database[job_id] = new_truth

    def reset_job_expecting_change(self, job_id: str, timeout: float) -> bool:
        """
        Discard any local modifications to a JobDescription.

        Will make modifications from other hosts visible.

        Will wait for up to timeout seconds for a modification (or deletion)
        from another host to actually be visible.

        Always replaces the JobDescription with what is stored in the job
        store, even if no modification ends up being visible.

        Returns True if an update was detected in time, and False otherwise.
        """

        start_time = time.time()
        wait_time = 0.1
        initially_known = job_id in self.__job_database
        new_truth: Optional[JobDescription] = None
        while True:
            try:
                new_truth = self.__job_store.load_job(job_id)
            except NoSuchJobException:
                # The job is gone now.
                if job_id in self.__job_database:
                    # So forget about it
                    del self.__job_database[job_id]
                    # TODO: Other collections may still reference it.
                if initially_known:
                    # Job was deleted, that's an update
                    return True
            else:
                if job_id in self.__job_database:
                    # We have an old version to compare against
                    old_truth = self.__job_database[job_id]
                    old_truth.assert_is_not_newer_than(new_truth)
                    if old_truth.is_updated_by(new_truth):
                        # Do the update
                        old_truth.__dict__.update(new_truth.__dict__)
                        return True
                else:
                    # Just keep the new one. That's an update.
                    self.__job_database[job_id] = new_truth
                    return True
            # We looked but didn't get a good update
            time_elapsed = time.time() - start_time
            if time_elapsed >= timeout:
                # We're out of time to check.
                if new_truth is not None:
                    # Commit whatever we managed to load to accomplish a real
                    # reset.
                    old_truth.__dict__.update(new_truth.__dict__)
                return False
            # Wait a little and poll again
            time.sleep(min(timeout - time_elapsed, wait_time))
            # Using exponential backoff
            wait_time *= 2

    # The next 3 functions provide tracking of how many successor jobs a given job
    # is waiting on, exposing only legit operations.
    # TODO: turn these into messages?
    def successors_pending(self, predecessor_id: str, count: int) -> None:
        """
        Remember that the given job has the given number more pending successors.

        (that have not yet succeeded or failed.)
        """
        if predecessor_id not in self.successorCounts:
            self.successorCounts[predecessor_id] = count

        else:
            self.successorCounts[predecessor_id] += count
        logger.debug(
            "Successors: %d more for %s, now have %d",
            count,
            predecessor_id,
            self.successorCounts[predecessor_id],
        )

    def successor_returned(self, predecessor_id: str) -> None:
        """
        Remember that the given job has one fewer pending successors.

        (because one has succeeded or failed.)
        """
        if predecessor_id not in self.successorCounts:
            raise RuntimeError(
                f"Tried to remove successor of {predecessor_id} that wasn't added!"
            )
        else:
            self.successorCounts[predecessor_id] -= 1
            logger.debug(
                "Successors: one fewer for %s, now have %d",
                predecessor_id,
                self.successorCounts[predecessor_id],
            )
            if self.successorCounts[predecessor_id] == 0:
                del self.successorCounts[predecessor_id]

    def count_pending_successors(self, predecessor_id: str) -> int:
        """
        Count number of pending successors of the given job.

        Pending successors are those which have not yet succeeded or failed.
        """
        if predecessor_id not in self.successorCounts:
            return 0
        else:
            return self.successorCounts[predecessor_id]

    def _buildToilState(self, jobDesc: JobDescription) -> None:
        """
        Build the ToilState class from the subtree root JobDescription.

        Updated jobs that the leader needs to deal with will have messages sent
        to the message bus.

        :param jobDesc: The description for the root job of the workflow being run.
        """
        # If the job description has a body, is a checkpoint, has services
        # or is ready to be deleted it is ready to be processed (i.e. it is updated)
        if (
            jobDesc.has_body()
            or (
                isinstance(jobDesc, CheckpointJobDescription)
                and jobDesc.checkpoint is not None
            )
            or len(jobDesc.services) > 0
            or jobDesc.nextSuccessors() is None
        ):
            logger.debug(
                "Found job to run: %s, with body: %s, with checkpoint: %s, with "
                "services: %s, with no next successors: %s",
                jobDesc.jobStoreID,
                jobDesc.has_body(),
                isinstance(jobDesc, CheckpointJobDescription)
                and jobDesc.checkpoint is not None,
                len(jobDesc.services) > 0,
                jobDesc.nextSuccessors() is None,
            )
            # Set the job updated because we should be able to make progress on it.
            self.bus.publish(JobUpdatedMessage(str(jobDesc.jobStoreID), 0))

            if (
                isinstance(jobDesc, CheckpointJobDescription)
                and jobDesc.checkpoint is not None
            ):
                jobDesc.restore_checkpoint()

        else:  # There exist successors
            logger.debug(
                "Adding job: %s to the state with %s successors",
                jobDesc.jobStoreID,
                len(jobDesc.nextSuccessors() or set()),
            )

            # Record the number of successors
            self.successorCounts[str(jobDesc.jobStoreID)] = len(
                jobDesc.nextSuccessors() or set()
            )

            def processSuccessorWithMultiplePredecessors(
                successor: JobDescription,
            ) -> None:
                # If jobDesc is not reported as complete by the successor
                if jobDesc.jobStoreID not in successor.predecessorsFinished:

                    # Update the successor's status to mark the predecessor complete
                    successor.predecessorsFinished.add(jobDesc.jobStoreID)

                # If the successor has no predecessors to finish
                if len(successor.predecessorsFinished) > successor.predecessorNumber:
                    raise RuntimeError(
                        "There are more finished predecessors than possible."
                    )
                if len(successor.predecessorsFinished) == successor.predecessorNumber:

                    # It is ready to be run, so remove it from the set of waiting jobs
                    self.jobsToBeScheduledWithMultiplePredecessors.remove(
                        successorJobStoreID
                    )

                    # Recursively consider the successor
                    self._buildToilState(successor)

            # For each successor
            for successorJobStoreID in jobDesc.nextSuccessors() or set():

                # If the successor does not yet point back at a
                # predecessor we have not yet considered it
                if successorJobStoreID not in self.successor_to_predecessors:

                    # Add the job as a predecessor
                    self.successor_to_predecessors[successorJobStoreID] = {
                        str(jobDesc.jobStoreID)
                    }

                    # We load the successor job
                    successor = self.get_job(successorJobStoreID)

                    # If predecessor number > 1 then the successor has multiple predecessors
                    if successor.predecessorNumber > 1:

                        # We put the successor job in the set of waiting successor
                        # jobs with multiple predecessors
                        if (
                            successorJobStoreID
                            in self.jobsToBeScheduledWithMultiplePredecessors
                        ):
                            raise RuntimeError(
                                "Failed to schedule the successor job. The successor job is already scheduled."
                            )
                        self.jobsToBeScheduledWithMultiplePredecessors.add(
                            successorJobStoreID
                        )

                        # Process successor
                        processSuccessorWithMultiplePredecessors(successor)

                    else:
                        # The successor has only this job as a predecessor so
                        # recursively consider the successor
                        self._buildToilState(successor)

                else:
                    # We've already seen the successor

                    # Add the job as a predecessor
                    if (
                        jobDesc.jobStoreID
                        in self.successor_to_predecessors[successorJobStoreID]
                    ):
                        raise RuntimeError(
                            "Failed to add the job as a predecessor. The job is already added as a predecessor."
                        )
                    self.successor_to_predecessors[successorJobStoreID].add(
                        str(jobDesc.jobStoreID)
                    )

                    # If the successor has multiple predecessors
                    if (
                        successorJobStoreID
                        in self.jobsToBeScheduledWithMultiplePredecessors
                    ):

                        # Get the successor from cache
                        successor = self.get_job(successorJobStoreID)

                        # Process successor
                        processSuccessorWithMultiplePredecessors(successor)
