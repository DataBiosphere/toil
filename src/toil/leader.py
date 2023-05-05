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

"""The leader script (of the leader/worker pair) for running jobs."""

import base64
import glob
import logging
import os
import pickle
import sys
import time
from typing import Any, Dict, List, Optional, Set, Union

import enlighten

from toil import resolveEntryPoint
from toil.batchSystems import DeadlockException
from toil.batchSystems.abstractBatchSystem import (AbstractBatchSystem,
                                                   BatchJobExitReason)
from toil.bus import (JobAnnotationMessage,
                      JobCompletedMessage,
                      JobFailedMessage,
                      JobIssuedMessage,
                      JobMissingMessage,
                      JobUpdatedMessage,
                      QueueSizeMessage)
from toil.common import Config, Toil, ToilMetrics
from toil.cwl.utils import CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE
from toil.job import (CheckpointJobDescription,
                      JobDescription,
                      ServiceJobDescription,
                      TemporaryID)
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             NoSuchFileException,
                                             NoSuchJobException)
from toil.lib.throttle import LocalThrottle
from toil.provisioners.abstractProvisioner import AbstractProvisioner
from toil.provisioners.clusterScaler import ScalerThread
from toil.serviceManager import ServiceManager
from toil.statsAndLogging import StatsAndLogging
from toil.toilState import ToilState
from toil.exceptions import FailedJobsException

logger = logging.getLogger(__name__)

###############################################################################
# Implementation Notes
#
# Multiple predecessors:
#   There is special-case handling for jobs with multiple predecessors as a
#   performance optimization. This minimize number of expensive loads of
#   JobDescriptions from jobStores.  However, this special case could be unnecessary.
#   The JobDescription is loaded to update predecessorsFinished, in
#   _checkSuccessorReadyToRunMultiplePredecessors, however it doesn't appear to
#   write the JobDescription back the jobStore.  Thus predecessorsFinished may really
#   be leader state and could moved out of the JobDescription.  This would make this
#   special-cases handling unnecessary and simplify the leader.
#   Issue #2136
###############################################################################


class Leader:
    """
    Represents the Toil leader.

    Responsible for determining what jobs are ready to be scheduled, by
    consulting the job store, and issuing them in the batch system.
    """

    def __init__(self,
                 config: Config,
                 batchSystem: AbstractBatchSystem,
                 provisioner: Optional[AbstractProvisioner],
                 jobStore: AbstractJobStore,
                 rootJob: JobDescription,
                 jobCache: Optional[Dict[Union[str, TemporaryID], JobDescription]] = None) -> None:
        """
        Create a Toil Leader object.

        If jobCache is passed, it must be a dict from job ID to pre-existing
        JobDescription objects. Jobs will be loaded from the cache (which can be
        downloaded from the jobStore in a batch) when loading into the ToilState object.

        :param config:      A Config object holding the original user options/settings/args.
        :param batchSystem: The Batch System object containing functions to communicate with the
                            specific batch system (run jobs, check jobs, etc.).
        :param provisioner: Only for environments where we need to provision our own compute
                            resources prior to launching jobs, for example, spin up an AWS instance.
                            This is a Provisioner object with functions to create/remove resources.
        :param jobStore:    The Jobstore object for storage, containing functions to talk to a
                            central location where we store all of our files (jobs, inputs, etc.).
        :param rootJob:     The first job of the workflow that the leader will run.
        """
        # Object containing parameters for the run
        self.config = config

        # The job store
        self.jobStore = jobStore
        self.jobStoreLocator = config.jobStore

        # The ToilState will be the authority on the current state of the jobs
        # in the jobStore, and its bus is the one true place to listen for
        # state change information about jobs.
        self.toilState = ToilState(self.jobStore)

        if self.config.write_messages is not None:
            # Message bus messages need to go to the given file.
            # Keep a reference to the return value so the listener stays alive.
            self._message_subscription = self.toilState.bus.connect_output_file(self.config.write_messages)

        # Connect to the message bus, so we will get all the messages of these
        # types in an inbox.
        self._messages = self.toilState.bus.connect([JobUpdatedMessage])

        # Connect the batch system to the bus so it can e.g. annotate jobs
        batchSystem.set_message_bus(self.toilState.bus)

        # Load the jobs into the ToilState, now that we are able to receive any
        # resulting messages.
        # TODO: Give other components a chance to connect to the bus before
        # this, somehow, so they can also see messages from this?
        self.toilState.load_workflow(rootJob, jobCache=jobCache)

        logger.debug("Found %s jobs to start and %i jobs with successors to run",
                     self._messages.count(JobUpdatedMessage), len(self.toilState.successorCounts))

        # Batch system
        self.batchSystem = batchSystem
        assert len(self.batchSystem.getIssuedBatchJobIDs()) == 0  # Batch system must start with no active jobs!
        logger.debug("Checked batch system has no running jobs and no updated jobs")

        # Map of batch system IDs to job store IDs
        self.issued_jobs_by_batch_system_id: Dict[int, str] = {}

        # Number of preemptible jobs currently being run by batch system
        self.preemptibleJobsIssued = 0

        # Tracking the number service jobs issued,
        # this is used limit the number of services issued to the batch system
        self.serviceJobsIssued = 0
        self.serviceJobsToBeIssued: List[str] = [] # A queue of IDs of service jobs that await scheduling
        # Equivalents for service jobs to be run on preemptible nodes
        self.preemptibleServiceJobsIssued = 0
        self.preemptibleServiceJobsToBeIssued: List[str] = []

        # Timing of the rescuing method
        self.timeSinceJobsLastRescued = None

        # For each issued job's batch system ID, how many times did we not see
        # it when we should have? If this hits a threshold, the job is declared
        # missing and killed and possibly retried.
        self.reissueMissingJobs_missingHash: Dict[int, int] = {}

        # Class used to create/destroy nodes in the cluster, may be None if
        # using a statically defined cluster
        self.provisioner = provisioner

        # Create cluster scaling thread if the provisioner is not None
        self.clusterScaler = None
        if self.provisioner is not None and self.provisioner.hasAutoscaledNodeTypes():
            self.clusterScaler = ScalerThread(self.provisioner, self, self.config)

        # A service manager thread to start and terminate services
        self.serviceManager = ServiceManager(jobStore, self.toilState)

        # A thread to manage the aggregation of statistics and logging from the run
        self.statsAndLogging = StatsAndLogging(self.jobStore, self.config)

        # Set used to monitor deadlocked jobs
        self.potentialDeadlockedJobs: Set[str] = set()
        self.potentialDeadlockTime = 0

        # A dashboard that runs on the leader node in AWS clusters to track the state
        # of the cluster
        self.toilMetrics: Optional[ToilMetrics] = None

        # internal jobs we should not expose at top level debugging
        self.debugJobNames = ("CWLJob", "CWLWorkflow", "CWLScatter", "CWLGather",
                              "ResolveIndirect")

        self.deadlockThrottler = LocalThrottle(self.config.deadlockCheckInterval)

        self.statusThrottler = LocalThrottle(self.config.statusWait)

        # Control how often to poll the job store to see if we have been killed
        self.kill_throttler = LocalThrottle(self.config.kill_polling_interval)

        # For fancy console UI, we use an Enlighten counter that displays running / queued jobs
        # This gets filled in in run() and updated periodically.
        self.progress_overall = None
        # Also count failed/killed jobs
        self.progress_failed = None
        # Assign colors for them
        self.GOOD_COLOR = (0, 60, 108)
        self.BAD_COLOR = (253, 199, 0)
        # And set a format that shows failures
        self.PROGRESS_BAR_FORMAT = ('{desc}{desc_pad}{percentage:3.0f}%|{bar}| {count:{len_total}d}/{total:d} '
                                    '({count_1:d} failures) [{elapsed}<{eta}, {rate:.2f}{unit_pad}{unit}/s]')
        # TODO: No way to set background color on the terminal for the bar.

        # What exit code should the process use if the workflow failed?
        # Needed in case a worker detects a CWL issue that a CWL runner must
        # report to its caller.
        self.recommended_fail_exit_code = 1

    def run(self) -> Any:
        """
        Run the leader process to issue and manage jobs.

        :raises: toil.exceptions.FailedJobsException if failed jobs remain after running.

        :return: The return value of the root job's run function.
        """
        self.jobStore.write_kill_flag(kill=False)

        with enlighten.get_manager(stream=sys.stderr, enabled=not self.config.disableProgress) as manager:
            # Set up the fancy console UI if desirable
            self.progress_overall = manager.counter(total=0, desc='Workflow Progress', unit='jobs',
                                                    color=self.GOOD_COLOR, bar_format=self.PROGRESS_BAR_FORMAT)
            self.progress_failed = self.progress_overall.add_subcounter(self.BAD_COLOR)

            # Start the stats/logging aggregation thread
            self.statsAndLogging.start()
            if self.config.metrics:
                self.toilMetrics = ToilMetrics(self.toilState.bus, provisioner=self.provisioner)

            try:

                # Start service manager thread
                self.serviceManager.start()
                try:

                    # Create cluster scaling processes if not None
                    if self.clusterScaler is not None:
                        self.clusterScaler.start()

                    try:
                        # Run the main loop
                        self.innerLoop()
                    finally:
                        if self.clusterScaler is not None:
                            logger.debug('Waiting for workers to shutdown.')
                            startTime = time.time()
                            self.clusterScaler.shutdown()
                            logger.debug('Worker shutdown complete in %s seconds.', time.time() - startTime)

                finally:
                    # Ensure service manager thread is properly shutdown
                    self.serviceManager.shutdown()

            finally:
                # Ensure the stats and logging thread is properly shutdown
                self.statsAndLogging.shutdown()
                if self.toilMetrics:
                    self.toilMetrics.shutdown()

            # Filter the failed jobs
            self.toilState.totalFailedJobs = [j for j in self.toilState.totalFailedJobs if self.toilState.job_exists(j)]

            try:
                self.create_status_sentinel_file(self.toilState.totalFailedJobs)
            except OSError as e:
                logger.debug(f'Error from importFile with hardlink=True: {e}')

            logger.info("Finished toil run %s" %
                         ("successfully." if not self.toilState.totalFailedJobs \
                    else ("with %s failed jobs." % len(self.toilState.totalFailedJobs))))

            if len(self.toilState.totalFailedJobs):
                failed_jobs = []
                for job_id in self.toilState.totalFailedJobs:
                    # Refresh all the failed jobs to get e.g. the log file IDs that the workers wrote
                    self.toilState.reset_job(job_id)
                    failed_jobs.append(self.toilState.get_job(job_id))

                logger.info("Failed jobs at end of the run: %s", ' '.join(str(j) for j in failed_jobs))
                raise FailedJobsException(self.jobStore, failed_jobs, exit_code=self.recommended_fail_exit_code)

            return self.jobStore.get_root_job_return_value()

    def create_status_sentinel_file(self, fail: bool) -> None:
        """Create a file in the jobstore indicating failure or success."""
        logName = 'failed.log' if fail else 'succeeded.log'
        localLog = os.path.join(os.getcwd(), logName)
        open(localLog, 'w').close()
        self.jobStore.import_file('file://' + localLog, logName, hardlink=True)

        if os.path.exists(localLog):  # Bandaid for Jenkins tests failing stochastically and unexplainably.
            os.remove(localLog)

    def _handledFailedSuccessor(self, successor_id: str, predecessor_id: str) -> bool:
        """
        Deal with the successor having failed.

        :param successor_id: The successor which has failed.
        :param predecessor_id: The job which the successor comes after.
        :returns: True if there are still active successors.
                  False if all successors have failed and the job is queued to run to handle the failed successors.
        """
        logger.debug("Successor job: %s of job: %s has failed """
                     "predecessors", self.toilState.get_job(successor_id), self.toilState.get_job(predecessor_id))

        # Add the job to the set having failed successors
        self.toilState.hasFailedSuccessors.add(predecessor_id)

        # Reduce active successor count and remove the successor as an active successor of the job
        self.toilState.successor_returned(predecessor_id)
        self.toilState.successor_to_predecessors[successor_id].remove(predecessor_id)
        if len(self.toilState.successor_to_predecessors[successor_id]) == 0:
            self.toilState.successor_to_predecessors.pop(successor_id)

        # If the job now has no active successors, add to active jobs
        # so it can be processed as a job with failed successors.
        if self.toilState.count_pending_successors(predecessor_id) == 0:
            logger.debug("Job: %s has no successors to run "
                         "and some are failed, adding to list of jobs "
                         "with failed successors", self.toilState.get_job(predecessor_id))
            self._messages.publish(JobUpdatedMessage(predecessor_id, 0))
            # Report no successors are running
            return False
        else:
            # Some successors are still active
            return True

    def _checkSuccessorReadyToRunMultiplePredecessors(self, successor_id: str, predecessor_id: str) -> bool:
        """
        Check if a successor job is ready to run when there are multiple predecessors.

        :param successor_id: The successor which has failed.
        :param predecessor_id: The job which the successor comes after.

        :returns: True if the successor is ready to run now, and False otherwise.
        """
        # See implementation note at the top of this file for discussion of multiple predecessors

        # Remember that we have multiple predecessors, if we haven't already
        # TODO: do we ever consult this after building the ToilState?
        self.toilState.jobsToBeScheduledWithMultiplePredecessors.add(successor_id)

        # Grab the cached version of the job, where we have the in-memory finished predecessor info.
        successor = self.toilState.get_job(successor_id)

        # Grab the predecessor for reporting
        predecessor = self.toilState.get_job(predecessor_id)

        logger.debug("Successor job: %s of job: %s has multiple "
                     "predecessors", successor, predecessor)

        # Add the predecessor as a finished predecessor to the successor
        successor.predecessorsFinished.add(predecessor_id)
        # TODO: commit back???

        # If the successor is in the set of successors of failed jobs
        if successor_id in self.toilState.failedSuccessors:
            if not self._handledFailedSuccessor(successor_id, predecessor_id):
                # The job is not ready to run
                return False

        # If the successor job's predecessors have all not all completed then
        # ignore the successor as is not yet ready to run
        assert len(successor.predecessorsFinished) <= successor.predecessorNumber
        if len(successor.predecessorsFinished) == successor.predecessorNumber:
            # All the successor's predecessors are done now.
            # Remove the successor job from the set of waiting multi-predecessor jobs.
            self.toilState.jobsToBeScheduledWithMultiplePredecessors.remove(successor_id)
            return True
        else:
            # The job is not ready to run
            return False

    def _makeJobSuccessorReadyToRun(self, successor_id: str, predecessor_id: str) -> bool:
        """
        Make a successor job ready to run if possible.

        :param successor_id: The successor which should become ready.
        :param predecessor_id: The job which the successor comes after.
        :returns: False if the successor job should not yet be run or True otherwise.
        """
        #Build map from successor to predecessors.
        if successor_id not in self.toilState.successor_to_predecessors:
            self.toilState.successor_to_predecessors[successor_id] = set()
        assert isinstance(successor_id, str)
        assert isinstance(predecessor_id, str)
        self.toilState.successor_to_predecessors[successor_id].add(predecessor_id)

        # Grab the successor
        successor = self.toilState.get_job(successor_id)
        logger.debug("Added job %s as coming after job %s", successor, self.toilState.get_job(predecessor_id))
        if successor.predecessorNumber > 1:
            return self._checkSuccessorReadyToRunMultiplePredecessors(successor_id, predecessor_id)
        else:
            return True

    def _runJobSuccessors(self, predecessor_id: str) -> None:
        """
        Issue the successors of a job.

        :param predecessor_id: The ID of the job which the successors come after.
        """
        # TODO: rewrite!

        # Grab the predecessor's JobDescription
        predecessor = self.toilState.get_job(predecessor_id)

        # Grap the successors
        next_successors = predecessor.nextSuccessors()

        if next_successors is None or len(next_successors) == 0:
            raise RuntimeError(f"Job {self} trying to run successors, but it doesn't have any")
        logger.debug("Job: %s has %i successors to schedule",
                     predecessor_id, len(next_successors))
        #Record the number of successors that must be completed before
        #the job can be considered again
        assert self.toilState.count_pending_successors(predecessor_id) == 0, 'Attempted to schedule successors of the same job twice!'
        self.toilState.successors_pending(predecessor_id, len(next_successors))

        # For each successor schedule if all predecessors have been completed
        successors = []
        for successor_id in next_successors:
            try:
                successor = self.toilState.get_job(successor_id)
            except NoSuchJobException:
                # Job already done and gone, but probably shouldn't be. Or maybe isn't visible yet.
                # TODO: Shouldn't this be an error?
                logger.warning("Job %s is a successor of %s but is already done and gone.", successor_id, predecessor_id)
                # Don't try and run it
                continue
            if self._makeJobSuccessorReadyToRun(successor_id, predecessor_id):
                successors.append(successor)
        self.issueJobs(successors)

    def _processFailedSuccessors(self, predecessor_id: str):
        """
        Deal with some of a job's successors having failed.

        Either fail the job, or restart it if it has retries left and is a checkpoint
        job.
        """

        # Get the description
        predecessor = self.toilState.get_job(predecessor_id)

        if predecessor_id in self.toilState.servicesIssued:
            # The job has services running; signal for them to be killed.
            # Once they are killed, then the job will be updated again and then
            # scheduled to be removed.
            logger.warning("Telling job %s to terminate its services due to successor failure",
                           predecessor)
            self.serviceManager.kill_services(self.toilState.servicesIssued[predecessor_id],
                                              error=True)
        elif self.toilState.count_pending_successors(predecessor_id) > 0:
            # The job has non-service jobs running; wait for them to finish.
            # the job will be re-added to the updated jobs when these jobs
            # are done
            logger.debug("Job %s with ID: %s with failed successors still has successor jobs running",
                         predecessor, predecessor_id)
        elif (isinstance(predecessor, CheckpointJobDescription) and
              predecessor.checkpoint is not None and
              predecessor.remainingTryCount > 1):
            # If the job is a checkpoint and has remaining retries...
            # The logic behind using > 1 rather than > 0 here: Since this job has
            # been tried once (without decreasing its try count as the job
            # itself was successful), and its subtree failed, it shouldn't be retried
            # unless it has more than 1 try.
            if predecessor_id in self.toilState.jobs_issued:
                logger.debug('Checkpoint job %s was updated while issued', predecessor_id)
            else:
                # It hasn't already been reissued.
                # This check lets us be robust against repeated job update
                # messages (such as from services starting *and* failing), by
                # making sure that we don't stay in a state that where we
                # reissue the job every time we get one.
                logger.warning('Job: %s is being restarted as a checkpoint after the total '
                               'failure of jobs in its subtree.', predecessor_id)
                self.issueJob(predecessor)
        else:
            # Mark it totally failed
            logger.debug("Job %s is being processed as completely failed", predecessor_id)
            self.processTotallyFailedJob(predecessor_id)

    def _processReadyJob(self, job_id: str, result_status: int):
        # We operate on the JobDescription mostly.
        readyJob = self.toilState.get_job(job_id)

        logger.debug('Updating status of job %s with result status: %s',
                     readyJob, result_status)

        # TODO: Filter out nonexistent successors/services now, so we can tell
        # if they are all done and the job needs deleting?

        if self.serviceManager.services_are_starting(readyJob.jobStoreID):
            # This stops a job with services being issued by the serviceManager
            # from being considered further in this loop. This catch is
            # necessary because the job's service's can fail while being
            # issued, causing the job to be sent an update message. We don't
            # want to act on it; we want to wait until it gets the update it
            # gets when the service manager is done trying to start its
            # services.
            logger.debug("Got a job to update which is still owned by the service "
                         "manager: %s", readyJob.jobStoreID)
        elif readyJob.jobStoreID in self.toilState.hasFailedSuccessors:
            self._processFailedSuccessors(job_id)
        elif readyJob.command is not None or result_status != 0:
            # The job has a command it must be run before any successors.
            # Similarly, if the job previously failed we rerun it, even if it doesn't have a
            # command to run, to eliminate any parts of the stack now completed.
            isServiceJob = readyJob.jobStoreID in self.toilState.service_to_client

            # We want to run the job, and expend one of its "tries" (possibly
            # the initial one)

            # If the job has run out of tries or is a service job whose error flag has
            # been indicated, fail the job.
            if (readyJob.remainingTryCount == 0 or
                (isServiceJob and not self.jobStore.file_exists(readyJob.errorJobStoreID))):
                self.processTotallyFailedJob(job_id)
                logger.warning("Job %s is completely failed", readyJob)
            else:
                # Otherwise try the job again
                self.issueJob(readyJob)
        elif next(readyJob.serviceHostIDsInBatches(), None) is not None:
            # the job has services to run, which have not been started, start them
            # Build a map from the service jobs to the job and a map
            # of the services created for the job
            assert readyJob.jobStoreID not in self.toilState.servicesIssued
            self.toilState.servicesIssued[readyJob.jobStoreID] = set()
            for serviceJobList in readyJob.serviceHostIDsInBatches():
                for serviceID in serviceJobList:
                    assert serviceID not in self.toilState.service_to_client
                    self.toilState.reset_job(serviceID)
                    serviceHost = self.toilState.get_job(serviceID)
                    self.toilState.service_to_client[serviceID] = readyJob.jobStoreID
                    self.toilState.servicesIssued[readyJob.jobStoreID].add(serviceID)

            logger.debug("Giving job: %s to service manager to schedule its jobs", readyJob)
            # Use the service manager to start the services
            self.serviceManager.put_client(job_id)
        elif readyJob.nextSuccessors() is not None:
            # There are successors to run
            self._runJobSuccessors(job_id)
        elif readyJob.jobStoreID in self.toilState.servicesIssued:
            logger.debug("Telling job: %s to terminate its services due to the "
                         "successful completion of its successor jobs",
                         readyJob)
            self.serviceManager.kill_services(self.toilState.servicesIssued[readyJob.jobStoreID], error=False)
        else:
            # There are no remaining tasks to schedule within the job.
            #
            # We want it to go away. Usually the worker is responsible for
            # deleting completed jobs. But when the job has a
            # child/followOn/service dependents that have to finish first, it
            # gets kicked back here when they are done.
            #
            # We don't want to just issueJob() again because then we will
            # forget that this job is actually done and not a retry. A
            # successful-subtree checkpoint job can't be distinguished from a
            # being-retried one.
            #
            # So we do the cleanup here, to avoid reserving any real resources.
            # See: https://github.com/DataBiosphere/toil/issues/4158 and
            # https://github.com/DataBiosphere/toil/issues/3188
            #
            # TODO: Does this want to happen in another thread? What if we
            # don't manage to do all the jobstore file deletes and need to
            # retry them? What if there are 1000 of them?

            if readyJob.remainingTryCount > 0:
                # add attribute to let issueJob know that this is an empty job and should be deleted
                logger.debug("Job: %s is empty, we are cleaning it up", readyJob)

                try:
                    self.toilState.delete_job(readyJob.jobStoreID)
                except Exception as e:
                    logger.exception("Re-processing success for job we could not remove: %s", readyJob)
                    # Kick it back to being handled as succeeded again. We
                    # don't want to have a failure here cause a Toil-level
                    # retry which causes more actual jobs to try to run.
                    # TODO: If there's some kind of permanent failure deleting
                    # from the job store, we will loop on this job forever!
                    self.process_finished_job_description(readyJob, 0)
                else:
                    # No exception during removal. Note that the job is removed.
                    self.processRemovedJob(readyJob, 0)
            else:
                self.processTotallyFailedJob(job_id)
                logger.error("Job: %s is empty but completely failed - something is very wrong", readyJob.jobStoreID)

    def _processReadyJobs(self):
        """Process jobs that are ready to be scheduled/have successors to schedule."""
        logger.debug('Built the jobs list, currently have %i jobs to update and %i jobs issued',
                     self._messages.count(JobUpdatedMessage), self.getNumberOfJobsIssued())

        # Now go through and, for each job that has updated this tick, process it.

        # We are in trouble if the same job is updated with multiple statuses,
        # and we want to coalesce multiple updates with the same status in the
        # same tick, so we remember what we've done so far in this dict from
        # job ID to status.
        handled_with_status = {}
        for message in self._messages.for_each(JobUpdatedMessage):
            # Handle all the job update messages that came in.

            if message.job_id in handled_with_status:
                if handled_with_status[message.job_id] == message.result_status:
                    # This is a harmless duplicate
                    logger.debug("Job %s already updated this tick with status %s and "
                                 "we've received duplicate message %s", message.job_id,
                                 handled_with_status[message.job_id], message)
                else:
                    # This is a conflicting update. We may have already treated
                    # a job as succeeding but now we've heard it's failed, or
                    # visa versa.
                    # This probably shouldn't happen, but does because the
                    # scheduler is not correct somehow and hasn't been for a
                    # long time. Complain about it.
                    logger.warning("Job %s already updated this tick with status %s "
                                   "but we've now received %s", message.job_id,
                                   handled_with_status[message.job_id], message)
                # Either way, we only want to handle one update per tick, like
                # the old dict-based implementation.
                continue
            else:
                # New job for this tick so actually handle that it is updated
                self._processReadyJob(message.job_id, message.result_status)
                handled_with_status[message.job_id] = message.result_status

    def _startServiceJobs(self):
        """Start any service jobs available from the service manager."""
        self.issueQueingServiceJobs()
        while True:
            service_id = self.serviceManager.get_startable_service(0)
            # Stop trying to get jobs when function returns None
            if service_id is None:
                break

            logger.debug('Launching service job: %s', self.toilState.get_job(service_id))
            self.issueServiceJob(service_id)

    def _processJobsWithRunningServices(self):
        """Get jobs whose services have started."""
        while True:
            client_id = self.serviceManager.get_ready_client(0)
            if client_id is None: # Stop trying to get jobs when function returns None
                break
            logger.debug('Job: %s has established its services; all services are running', client_id)

            # Grab the client job description
            client = self.toilState.get_job(client_id)

            # Drop all service relationships
            client.filterServiceHosts(lambda ignored: False)
            self._messages.publish(JobUpdatedMessage(client_id, 0))

    def _processJobsWithFailedServices(self):
        """Get jobs whose services have failed to start."""
        while True:
            client_id = self.serviceManager.get_unservable_client(0)
            if client_id is None: # Stop trying to get jobs when function returns None
                break
            logger.debug('Job: %s has failed to establish its services.', client_id)

            # Grab the client job description
            client = self.toilState.get_job(client_id)

            # Make sure services still want to run
            assert next(client.serviceHostIDsInBatches(), None) is not None

            # Mark the service job updated so we don't stop here.
            self._messages.publish(JobUpdatedMessage(client_id, 1))

    def _gatherUpdatedJobs(self, updatedJobTuple):
        """Gather any new, updated JobDescriptions from the batch system."""
        bsID, exitStatus, exitReason, wallTime = (
            updatedJobTuple.jobID, updatedJobTuple.exitStatus, updatedJobTuple.exitReason,
            updatedJobTuple.wallTime)
        # easy, track different state
        try:
            updatedJob = self.toilState.get_job(self.issued_jobs_by_batch_system_id[bsID])
        except KeyError:
            logger.warning("A result seems to already have been processed for job %s", bsID)
        else:
            if exitStatus == 0:
                logger.debug('Job ended: %s', updatedJob)
            else:
                logger.warning(f'Job failed with exit value {exitStatus}: {updatedJob}\n'
                               f'Exit reason: {exitReason}')
                if exitStatus == CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE:
                    # This is a CWL job informing us that the workflow is
                    # asking things of us that Toil can't do. When we raise an
                    # exception because of this, make sure to forward along
                    # this exit code.
                    logger.warning("This indicates an unsupported CWL requirement!")
                    self.recommended_fail_exit_code = CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE
            # Tell everyone it stopped running.
            self._messages.publish(JobCompletedMessage(updatedJob.get_job_kind(), updatedJob.jobStoreID, exitStatus))
            self.process_finished_job(bsID, exitStatus, wall_time=wallTime, exit_reason=exitReason)

    def _processLostJobs(self):
        """Process jobs that have gone awry."""
        # In the case that there is nothing happening (no updated jobs to
        # gather for rescueJobsFrequency seconds) check if there are any jobs
        # that have run too long (see self.reissueOverLongJobs) or which have
        # gone missing from the batch system (see self.reissueMissingJobs)
        if ((time.time() - self.timeSinceJobsLastRescued) >= self.config.rescueJobsFrequency):
            # We only rescue jobs every N seconds, and when we have apparently
            # exhausted the current job supply
            self.reissueOverLongJobs()

            hasNoMissingJobs = self.reissueMissingJobs()
            if hasNoMissingJobs:
                self.timeSinceJobsLastRescued = time.time()
            else:
                # This means we'll try again in a minute, providing things are quiet
                self.timeSinceJobsLastRescued += 60

    def innerLoop(self):
        """
        Process jobs.

        This is the leader's main loop.
        """
        self.timeSinceJobsLastRescued = time.time()

        while self._messages.count(JobUpdatedMessage) > 0 or \
              self.getNumberOfJobsIssued() or \
              self.serviceManager.get_job_count():

            if self._messages.count(JobUpdatedMessage) > 0:
                self._processReadyJobs()

            # deal with service-related jobs
            self._startServiceJobs()
            self._processJobsWithRunningServices()
            self._processJobsWithFailedServices()

            # check in with the batch system
            updatedJobTuple = self.batchSystem.getUpdatedBatchJob(maxWait=2)
            if updatedJobTuple is not None:
                # Collect and process all the updates
                self._gatherUpdatedJobs(updatedJobTuple)
                # As long as we are getting updates we definitely can't be
                # deadlocked.
                self.feed_deadlock_watchdog()
            else:
                # If nothing is happening, see if any jobs have wandered off
                self._processLostJobs()

                if self.deadlockThrottler.throttle(wait=False):
                    # Nothing happened this round and it's been long
                    # enough since we last checked. Check for deadlocks.
                    self.checkForDeadlocks()

            # Check on the associated threads and exit if a failure is detected
            self.statsAndLogging.check()
            self.serviceManager.check()
            # the cluster scaler object will only be instantiated if autoscaling is enabled
            if self.clusterScaler is not None:
                self.clusterScaler.check()

            if self.statusThrottler.throttle(wait=False):
                # Time to tell the user how things are going
                self._reportWorkflowStatus()

            if self.kill_throttler.throttle(wait=False):
                if self.jobStore.read_kill_flag():
                    logger.warning("Received kill via job store. Shutting down.")
                    raise KeyboardInterrupt("killed via job store")

            # Make sure to keep elapsed time and ETA up to date even when no jobs come in
            self.progress_overall.update(incr=0)

        logger.debug("Finished the main loop: no jobs left to run.")

        # Consistency check the toil state
        assert self._messages.empty(), f"Pending messages at shutdown: {self._messages}"
        assert self.toilState.successorCounts == {}, f"Jobs waiting on successors at shutdown: {self.toilState.successorCounts}"
        assert self.toilState.successor_to_predecessors == {}, f"Successors pending for their predecessors at shutdown: {self.toilState.successor_to_predecessors}"
        assert self.toilState.service_to_client == {}, f"Services pending for their clients at shutdown: {self.toilState.service_to_client}"
        assert self.toilState.servicesIssued == {}, f"Services running at shutdown: {self.toilState.servicesIssued}"
        # assert self.toilState.jobsToBeScheduledWithMultiplePredecessors # These are not properly emptied yet
        # assert self.toilState.hasFailedSuccessors == set() # These are not properly emptied yet

    def checkForDeadlocks(self):
        """Check if the system is deadlocked running service jobs."""
        totalRunningJobs = len(self.batchSystem.getRunningBatchJobIDs())
        totalServicesIssued = self.serviceJobsIssued + self.preemptibleServiceJobsIssued

        # If there are no updated jobs and at least some jobs running
        if totalServicesIssued >= totalRunningJobs and totalRunningJobs > 0:
            # Collect all running service job store IDs into a set to compare with the deadlock set
            running_service_ids: Set[str] = set()
            for js_id in self.issued_jobs_by_batch_system_id.values():
                job = self.toilState.get_job(js_id)
                if isinstance(job, ServiceJobDescription) and self.serviceManager.is_running(js_id):
                    running_service_ids.add(js_id)

            if len(running_service_ids) > totalRunningJobs:
                # This is too many services.
                # TODO: couldn't more jobs have started since we polled the
                # running job count?
                raise RuntimeError(f"Supposedly running {len(running_service_ids)} services, which is"
                                   f"more than the {totalRunningJobs} currently running jobs overall.")

            # If all the running jobs are active services then we have a potential deadlock
            if len(running_service_ids) == totalRunningJobs:
                # There could be trouble; we are 100% services.
                # See if the batch system has anything to say for itself about its failure to run our jobs.
                message = self.batchSystem.getSchedulingStatusMessage()
                if message is not None:
                    # Prepend something explaining the message
                    message = f"The batch system reports: {message}"
                else:
                    # Use a generic message if none is available
                    message = "Cluster may be too small."


                # See if this is a new potential deadlock
                if self.potentialDeadlockedJobs != running_service_ids:
                    logger.warning(("Potential deadlock detected! All %s running jobs are service jobs, "
                                    "with no normal jobs to use them! %s"), totalRunningJobs, message)
                    self.potentialDeadlockedJobs = running_service_ids
                    self.potentialDeadlockTime = time.time()
                else:
                    # We wait self.config.deadlockWait seconds before declaring the system deadlocked
                    stuckFor = time.time() - self.potentialDeadlockTime
                    if stuckFor >= self.config.deadlockWait:
                        logger.error("We have been deadlocked since %s on these service jobs: %s",
                                     self.potentialDeadlockTime, self.potentialDeadlockedJobs)
                        raise DeadlockException(("The workflow is service deadlocked - all %d running jobs "
                                                 "have been the same active services for at least %s seconds") % (totalRunningJobs, self.config.deadlockWait))
                    else:
                        # Complain that we are still stuck.
                        waitingNormalJobs = self.getNumberOfJobsIssued() - totalServicesIssued
                        logger.warning(("Potentially deadlocked for %.0f seconds. Waiting at most %.0f more seconds "
                                        "for any of %d issued non-service jobs to schedule and start. %s"),
                                       stuckFor, self.config.deadlockWait - stuckFor, waitingNormalJobs, message)
            else:
                # We have observed non-service jobs running, so reset the potential deadlock
                self.feed_deadlock_watchdog()
        else:
            # We have observed non-service jobs running, so reset the potential deadlock.
            self.feed_deadlock_watchdog()

    def feed_deadlock_watchdog(self) -> None:
        """Note that progress has been made and any pending deadlock checks should be reset."""
        if len(self.potentialDeadlockedJobs) > 0:
            # We thought we had a deadlock. Tell the user it is fixed.
            logger.warning("Potential deadlock has been resolved; detected progress")

        self.potentialDeadlockedJobs = set()
        self.potentialDeadlockTime = 0

    def issueJob(self, jobNode: JobDescription) -> None:
        """Add a job to the queue of jobs currently trying to run."""
        # Never issue the same job multiple times simultaneously
        assert jobNode.jobStoreID not in self.toilState.jobs_issued, \
            f"Attempted to issue {jobNode} multiple times simultaneously!"

        workerCommand = [resolveEntryPoint('_toil_worker'),
                         jobNode.jobName,
                         self.jobStoreLocator,
                         jobNode.jobStoreID]

        for context in self.batchSystem.getWorkerContexts():
            # For each context manager hook the batch system wants to run in
            # the worker, serialize and send it.
            workerCommand.append('--context')
            workerCommand.append(base64.b64encode(pickle.dumps(context)).decode('utf-8'))

        # We locally override the command. This shouldn't get persisted back to
        # the job store, or we will detach the job body from the job
        # description. TODO: Don't do it this way! It's weird!
        jobNode.command = ' '.join(workerCommand)

        omp_threads = os.environ.get('OMP_NUM_THREADS') \
            or str(max(1, int(jobNode.cores)))  # make sure OMP_NUM_THREADS is a positive integer

        job_environment = {
            # Set the number of cores used by OpenMP applications
            'OMP_NUM_THREADS': omp_threads,
        }

        # jobBatchSystemID is an int for each job
        jobBatchSystemID = self.batchSystem.issueBatchJob(jobNode, job_environment=job_environment)
        # Record the job by the ID the batch system will use to talk about it with us
        self.issued_jobs_by_batch_system_id[jobBatchSystemID] = jobNode.jobStoreID
        # Record that this job is issued right now and shouldn't e.g. be issued again.
        self.toilState.jobs_issued.add(jobNode.jobStoreID)
        if jobNode.preemptible:
            # len(issued_jobs_by_batch_system_id) should always be greater than or equal to preemptibleJobsIssued,
            # so increment this value after the job is added to the issuedJob dict
            self.preemptibleJobsIssued += 1
        cur_logger = logger.debug if jobNode.local else logger.info
        cur_logger("Issued job %s with job batch system ID: "
                   "%s and %s",
                   jobNode, str(jobBatchSystemID), jobNode.requirements_string())
        # Tell everyone it is issued and the queue size changed
        self._messages.publish(JobIssuedMessage(jobNode.get_job_kind(), jobNode.jobStoreID, jobBatchSystemID))
        self._messages.publish(QueueSizeMessage(self.getNumberOfJobsIssued()))
        # Tell the user there's another job to do
        self.progress_overall.total += 1
        self.progress_overall.update(incr=0)

    def issueJobs(self, jobs):
        """Add a list of jobs, each represented as a jobNode object."""
        for job in jobs:
            self.issueJob(job)

    def issueServiceJob(self, service_id: str) -> None:
        """
        Issue a service job.

        Put it on a queue if the maximum number of service jobs to be scheduled has been reached.
        """
        # Grab the service job description
        service = self.toilState.get_job(service_id)
        assert isinstance(service, ServiceJobDescription)

        if service.preemptible:
            self.preemptibleServiceJobsToBeIssued.append(service_id)
        else:
            self.serviceJobsToBeIssued.append(service_id)
        self.issueQueingServiceJobs()

    def issueQueingServiceJobs(self):
        """Issues any queuing service jobs up to the limit of the maximum allowed."""
        while len(self.serviceJobsToBeIssued) > 0 and self.serviceJobsIssued < self.config.maxServiceJobs:
            self.issueJob(self.toilState.get_job(self.serviceJobsToBeIssued.pop()))
            self.serviceJobsIssued += 1
        while len(self.preemptibleServiceJobsToBeIssued) > 0 and self.preemptibleServiceJobsIssued < self.config.maxPreemptibleServiceJobs:
            self.issueJob(self.toilState.get_job(self.preemptibleServiceJobsToBeIssued.pop()))
            self.preemptibleServiceJobsIssued += 1

    def getNumberOfJobsIssued(self, preemptible: Optional[bool]=None) -> int:
        """
        Get number of jobs that have been added by issueJob(s) and not removed by removeJob.

        :param preemptible: If none, return all types of jobs.
          If true, return just the number of preemptible jobs. If false, return
          just the number of non-preemptible jobs.
        """
        if preemptible is None:
            return len(self.issued_jobs_by_batch_system_id)
        elif preemptible:
            return self.preemptibleJobsIssued
        else:
            assert len(self.issued_jobs_by_batch_system_id) >= self.preemptibleJobsIssued
            return len(self.issued_jobs_by_batch_system_id) - self.preemptibleJobsIssued

    def _getStatusHint(self) -> str:
        """
        Get a short string describing the current state of the workflow for a human.

        Should include number of currently running jobs, number of issued jobs, etc.

        Don't call this too often; it will talk to the batch system, which may
        make queries of the backing scheduler.

        :return: A one-line description of the current status of the workflow.
        """
        # Grab a snapshot of everything. May be inconsistent since it's not atomic.
        issued_job_count = self.getNumberOfJobsIssued()
        running_job_count = len(self.batchSystem.getRunningBatchJobIDs())
        # TODO: When a job stops running but has yet to be collected from the
        # batch system, it will show up here as waiting to run.
        return f"{running_job_count} jobs are running, {issued_job_count - running_job_count} jobs are issued and waiting to run"

    def _reportWorkflowStatus(self) -> None:
        """Report the current status of the workflow to the user."""
        # For now just log our scheduling status message to the log.
        # TODO: make this update fast enough to put it in the progress
        # bar/status line.
        logger.info(self._getStatusHint())

    def removeJob(self, jobBatchSystemID: int) -> JobDescription:
        """
        Remove a job from the system by batch system ID.

        :return: Job description as it was issued.
        """
        assert jobBatchSystemID in self.issued_jobs_by_batch_system_id
        issuedDesc = self.toilState.get_job(self.issued_jobs_by_batch_system_id[jobBatchSystemID])
        if issuedDesc.preemptible:
            # len(issued_jobs_by_batch_system_id) should always be greater than or equal to preemptibleJobsIssued,
            # so decrement this value before removing the job from the issuedJob map
            assert self.preemptibleJobsIssued > 0
            self.preemptibleJobsIssued -= 1
        # It's not issued anymore.
        del self.issued_jobs_by_batch_system_id[jobBatchSystemID]
        assert issuedDesc.jobStoreID in self.toilState.jobs_issued, f"Job {issuedDesc} came back without being issued"
        self.toilState.jobs_issued.remove(issuedDesc.jobStoreID)
        # If service job
        if issuedDesc.jobStoreID in self.toilState.service_to_client:
            # Decrement the number of services
            if issuedDesc.preemptible:
                self.preemptibleServiceJobsIssued -= 1
            else:
                self.serviceJobsIssued -= 1

        # Tell the user that job is done, for progress purposes.
        self.progress_overall.update(incr=1)

        return issuedDesc

    def getJobs(self, preemptible: Optional[bool] = None) -> List[JobDescription]:
        """
        Get all issued jobs.

        :param preemptible: If specified, select only preemptible or only non-preemptible jobs.
        """

        jobs = [self.toilState.get_job(job_store_id) for job_store_id in self.issued_jobs_by_batch_system_id.values()]
        if preemptible is not None:
            jobs = [job for job in jobs if job.preemptible == preemptible]
        return jobs

    def killJobs(self, jobsToKill):
        """
        Kills the given set of jobs and then sends them for processing.

        Returns the jobs that, upon processing, were reissued.
        """
        # If we are rerunning a job we put the ID in this list.
        jobsRerunning = []

        if len(jobsToKill) > 0:
            # Kill the jobs with the batch system. They will now no longer come in as updated.
            self.batchSystem.killBatchJobs(jobsToKill)
            for jobBatchSystemID in jobsToKill:
                # Reissue immediately, noting that we killed the job
                willRerun = self.process_finished_job(jobBatchSystemID, 1, exit_reason=BatchJobExitReason.KILLED)

                if willRerun:
                    # Compose a list of all the jobs that will run again
                    jobsRerunning.append(jobBatchSystemID)

        return jobsRerunning


    #Following functions handle error cases for when jobs have gone awry with the batch system.

    def reissueOverLongJobs(self) -> None:
        """
        Check each issued job.

        If a job is running for longer than desirable issue a kill instruction.
        Wait for the job to die then we pass the job to process_finished_job.
        """
        maxJobDuration = self.config.maxJobDuration
        jobsToKill = []
        if maxJobDuration < 10000000:  # We won't bother doing anything if rescue time > 16 weeks.
            runningJobs = self.batchSystem.getRunningBatchJobIDs()
            for jobBatchSystemID in list(runningJobs.keys()):
                if runningJobs[jobBatchSystemID] > maxJobDuration:
                    logger.warning("The job: %s has been running for: %s seconds, more than the "
                                "max job duration: %s, we'll kill it",
                                self.issued_jobs_by_batch_system_id[jobBatchSystemID],
                                str(runningJobs[jobBatchSystemID]),
                                str(maxJobDuration))
                    jobsToKill.append(jobBatchSystemID)
            reissued = self.killJobs(jobsToKill)
            if len(jobsToKill) > 0:
                # Summarize our actions
                logger.info("Killed %d over long jobs and reissued %d of them", len(jobsToKill), len(reissued))

    def reissueMissingJobs(self, killAfterNTimesMissing=3):
        """
        Check all the current job ids are in the list of currently issued batch system jobs.

        If a job is missing, we mark it as so, if it is missing for a number of runs of
        this function (say 10).. then we try deleting the job (though its probably lost), we wait
        then we pass the job to process_finished_job.
        """
        issuedJobs = set(self.batchSystem.getIssuedBatchJobIDs())
        jobBatchSystemIDsSet = set(list(self.issued_jobs_by_batch_system_id.keys()))
        #Clean up the reissueMissingJobs_missingHash hash, getting rid of jobs that have turned up
        missingJobIDsSet = set(list(self.reissueMissingJobs_missingHash.keys()))
        for jobBatchSystemID in missingJobIDsSet.difference(jobBatchSystemIDsSet):
            self.reissueMissingJobs_missingHash.pop(jobBatchSystemID)
            logger.warning("Batch system id: %s is no longer missing", str(jobBatchSystemID))
        assert issuedJobs.issubset(jobBatchSystemIDsSet) #Assert checks we have
        #no unexpected jobs running
        jobsToKill = []
        for jobBatchSystemID in set(jobBatchSystemIDsSet.difference(issuedJobs)):
            jobStoreID = self.issued_jobs_by_batch_system_id[jobBatchSystemID]
            if jobBatchSystemID in self.reissueMissingJobs_missingHash:
                self.reissueMissingJobs_missingHash[jobBatchSystemID] += 1
            else:
                self.reissueMissingJobs_missingHash[jobBatchSystemID] = 1
            timesMissing = self.reissueMissingJobs_missingHash[jobBatchSystemID]
            logger.warning("Job store ID %s with batch system id %s is missing for the %i time",
                        jobStoreID, str(jobBatchSystemID), timesMissing)
            # Tell everyone it is missing
            self._messages.publish(JobMissingMessage(jobStoreID))
            if timesMissing == killAfterNTimesMissing:
                self.reissueMissingJobs_missingHash.pop(jobBatchSystemID)
                jobsToKill.append(jobBatchSystemID)
        self.killJobs(jobsToKill)
        return len( self.reissueMissingJobs_missingHash ) == 0 #We use this to inform
        #if there are missing jobs

    def processRemovedJob(self, issuedJob, result_status):
        if result_status != 0:
            logger.warning("Despite the batch system claiming failure the "
                        "job %s seems to have finished and been removed", issuedJob)
        self._updatePredecessorStatus(issuedJob.jobStoreID)

    def process_finished_job(self, batch_system_id, result_status, wall_time=None, exit_reason=None) -> bool:
        """
        Process finished jobs.

        Called when an attempt to run a job finishes, either successfully or otherwise.

        Takes the job out of the issued state, and then works out what
        to do about the fact that it succeeded or failed.

        :returns: True if the job is going to run again, and False if the job is
                  fully done or completely failed.
        """
        # De-issue the job.
        issued_job = self.removeJob(batch_system_id)

        if result_status != 0:
            # Show job as failed in progress (and take it from completed)
            self.progress_overall.update(incr=-1)
            self.progress_failed.update(incr=1)

        # Delegate to the vers
        return self.process_finished_job_description(issued_job, result_status, wall_time, exit_reason, batch_system_id)

    def process_finished_job_description(self, finished_job: JobDescription, result_status: int,
                                         wall_time: Optional[float] = None,
                                         exit_reason: Optional[BatchJobExitReason] = None,
                                         batch_system_id: Optional[int] = None) -> bool:
        """
        Process a finished JobDescription based upon its succees or failure.

        If wall-clock time is available, informs the cluster scaler about the
        job finishing.

        If the job failed and a batch system ID is available, checks for and
        reports batch system logs.

        Checks if it succeeded and was removed, or if it failed and needs to be
        set up after failure, and dispatches to the appropriate function.

        :returns: True if the job is going to run again, and False if the job is
                  fully done or completely failed.
        """
        job_store_id = finished_job.jobStoreID
        if wall_time is not None and self.clusterScaler is not None:
            # Tell the cluster scaler a job finished.
            # TODO: Use message bus?
            self.clusterScaler.addCompletedJob(finished_job, wall_time)
        if self.toilState.job_exists(job_store_id):
            logger.debug("Job %s continues to exist (i.e. has more to do)", finished_job)
            try:
                # Reload the job as modified by the worker
                self.toilState.reset_job(job_store_id)
                replacement_job = self.toilState.get_job(job_store_id)
            except NoSuchJobException:
                # We have a ghost job - the job has been deleted but a stale
                # read from e.g. a non-POSIX-compliant filesystem gave us a
                # false positive when we checked for its existence. Process the
                # job from here as any other job removed from the job store.
                # This is a hack until we can figure out how to actually always
                # have a strongly-consistent communications channel. See
                # https://github.com/BD2KGenomics/toil/issues/1091
                logger.warning('Got a stale read for job %s; caught its '
                'completion in time, but other jobs may try to run twice! Fix '
                'the consistency of your job store storage!', finished_job)
                self.processRemovedJob(finished_job, result_status)
                return False
            if replacement_job.logJobStoreFileID is not None:
                with replacement_job.getLogFileHandle(self.jobStore) as log_stream:
                    # more memory efficient than read().striplines() while leaving off the
                    # trailing \n left when using readlines()
                    # http://stackoverflow.com/a/15233739
                    StatsAndLogging.logWithFormatting(job_store_id, log_stream, method=logger.warning,
                                                      message='The job seems to have left a log file, indicating failure: %s' % replacement_job)
                if self.config.writeLogs or self.config.writeLogsGzip:
                    with replacement_job.getLogFileHandle(self.jobStore) as log_stream:
                        StatsAndLogging.writeLogFiles(replacement_job.chainedJobs, log_stream, self.config, failed=True)
            if result_status != 0:
                # If the batch system returned a non-zero exit code then the worker
                # is assumed not to have captured the failure of the job, so we
                # reduce the try count here.
                if replacement_job.logJobStoreFileID is None:
                    logger.warning("No log file is present, despite job failing: %s", replacement_job)

                if batch_system_id is not None:
                    # Look for any standard output/error files created by the batch system.
                    # They will only appear if the batch system actually supports
                    # returning logs to the machine that submitted jobs, or if
                    # --workDir / TOIL_WORKDIR is on a shared file system.
                    # They live directly in the Toil work directory because that is
                    # guaranteed to exist on the leader and workers.
                    file_list = glob.glob(self.batchSystem.format_std_out_err_glob(batch_system_id))
                    for log_file in file_list:
                        try:
                            log_stream = open(log_file, 'rb')
                        except:
                            logger.warning('The batch system left a file %s, but it could not be opened' % log_file)
                        else:
                            with log_stream:
                                if os.path.getsize(log_file) > 0:
                                    StatsAndLogging.logWithFormatting(job_store_id, log_stream, method=logger.warning,
                                                                      message='The batch system left a non-empty file %s:' % log_file)
                                    if self.config.writeLogs or self.config.writeLogsGzip:
                                        file_root, _ = os.path.splitext(os.path.basename(log_file))
                                        job_names = replacement_job.chainedJobs
                                        if job_names is None:   # For jobs that fail this way, replacement_job.chainedJobs is not guaranteed to be set
                                            job_names = [str(replacement_job)]
                                        job_names = [j + '_' + file_root for j in job_names]
                                        log_stream.seek(0)
                                        StatsAndLogging.writeLogFiles(job_names, log_stream, self.config, failed=True)
                                else:
                                    logger.warning('The batch system left an empty file %s' % log_file)

                # Tell the job to reset itself after a failure.
                # It needs to know the failure reason if available; some are handled specially.
                replacement_job.setupJobAfterFailure(exit_status=result_status, exit_reason=exit_reason)
                self.toilState.commit_job(job_store_id)

            elif job_store_id in self.toilState.hasFailedSuccessors:
                # If the job has completed okay, we can remove it from the list of jobs with failed successors
                self.toilState.hasFailedSuccessors.remove(job_store_id)

            # Now that we know the job is done we can add it to the list of updated jobs
            self._messages.publish(JobUpdatedMessage(replacement_job.jobStoreID, result_status))
            logger.debug("Added job: %s to updated jobs", replacement_job)

            # Return True if it will rerun (still has retries) and false if it
            # is completely failed.
            return replacement_job.remainingTryCount > 0
        else:  #The job is done
            self.processRemovedJob(finished_job, result_status)
            # Being done, it won't run again.
            return False

    def getSuccessors(self, job_id: str, alreadySeenSuccessors: Set[str]) -> Set[str]:
        """
        Get successors of the given job by walking the job graph recursively.

        :param alreadySeenSuccessors: any successor seen here is ignored and not traversed.
        :returns: The set of found successors. This set is added to alreadySeenSuccessors.
        """
        successors = set()
        def successorRecursion(job_id: str) -> None:
            # TODO: do we need to reload from the job store here, or is the cache OK?
            jobDesc = self.toilState.get_job(job_id)

            # For lists of successors
            for successorID in jobDesc.allSuccessors():
                # If successor not already visited
                if successorID not in alreadySeenSuccessors:

                    # Add to set of successors
                    successors.add(successorID)
                    alreadySeenSuccessors.add(successorID)

                    # Recurse if job exists
                    # (job may not exist if already completed)
                    if self.toilState.job_exists(successorID):
                        successorRecursion(successorID)

        successorRecursion(job_id)  # Recurse from passed job

        return successors

    def processTotallyFailedJob(self, job_id: str) -> None:
        """Process a totally failed job."""
        # Mark job as a totally failed job
        self.toilState.totalFailedJobs.add(job_id)

        job_desc = self.toilState.get_job(job_id)

        # Tell everyone it failed

        self._messages.publish(JobFailedMessage(job_desc.get_job_kind(), job_id))

        if job_id in self.toilState.service_to_client:
            # Is a service job
            logger.debug("Service job is being processed as a totally failed job: %s", job_desc)

            assert isinstance(job_desc, ServiceJobDescription)

            # Grab the client, which is the predecessor.
            client_id = self.toilState.service_to_client[job_id]

            assert client_id in self.toilState.servicesIssued

            # Leave the service job as a service of its predecessor, because it
            # didn't work.

            # Poke predecessor job that had the service to kill its services,
            # and drop predecessor relationship from ToilState.
            self._updatePredecessorStatus(job_id)

            # Signal to all other services in the group that they should
            # terminate. We do this to prevent other services in the set
            # of services from deadlocking waiting for this service to start
            # properly, and to remember that this service failed with an error
            # and possibly never started.
            if client_id in self.toilState.servicesIssued:
                self.serviceManager.kill_services(self.toilState.servicesIssued[client_id], error=True)
                logger.warning("Job: %s is instructing all other services of its parent job to quit", job_desc)

            # This ensures that the job will not attempt to run any of it's
            # successors on the stack
            self.toilState.hasFailedSuccessors.add(client_id)

            # Remove the start flag, if it still exists. This indicates
            # to the service manager that the job has "started", and prevents
            # the service manager from waiting for it to start forever. It also
            # lets it continue, now that we have issued kill orders for them,
            # to start dependent services, which all need to actually fail
            # before we can finish up with the services' predecessor job.
            self.jobStore.delete_file(job_desc.startJobStoreID)
        else:
            # Is a non-service job
            assert job_id not in self.toilState.servicesIssued
            assert not isinstance(job_desc, ServiceJobDescription)

            # Traverse failed job's successor graph and get the jobStoreID of new successors.
            # Any successor already in toilState.failedSuccessors will not be traversed
            # All successors traversed will be added to toilState.failedSuccessors and returned
            # as a set (unseenSuccessors).
            unseenSuccessors = self.getSuccessors(job_id, self.toilState.failedSuccessors)
            logger.debug("Found new failed successors: %s of job: %s", " ".join(
                         unseenSuccessors), job_desc)

            # For each newly found successor
            for successorJobStoreID in unseenSuccessors:

                # If the successor is a successor of other jobs that have already tried to schedule it
                if successorJobStoreID in self.toilState.successor_to_predecessors:

                    # For each such predecessor job
                    # (we remove the successor from toilState.successor_to_predecessors to avoid doing
                    # this multiple times for each failed predecessor)
                    for predecessor_id in self.toilState.successor_to_predecessors.pop(successorJobStoreID):

                        predecessor = self.toilState.get_job(predecessor_id)

                        # Reduce the predecessor job's successor count.
                        self.toilState.successor_returned(predecessor_id)

                        # Indicate that it has failed jobs.
                        self.toilState.hasFailedSuccessors.add(predecessor_id)
                        logger.debug("Marking job: %s as having failed successors (found by "
                                     "reading successors failed job)", predecessor)

                        # If the predecessor has no remaining successors, add to list of updated jobs
                        if self.toilState.count_pending_successors(predecessor_id) == 0:
                            self._messages.publish(JobUpdatedMessage(predecessor_id, 0))

            # If the job has predecessor(s)
            if job_id in self.toilState.successor_to_predecessors:

                # For each predecessor of the job
                for predecessor_id in self.toilState.successor_to_predecessors[job_id]:

                    # Mark the predecessor as failed
                    self.toilState.hasFailedSuccessors.add(predecessor_id)
                    logger.debug("Totally failed job: %s is marking direct predecessor: %s "
                                 "as having failed jobs", job_desc, self.toilState.get_job(predecessor_id))

                self._updatePredecessorStatus(job_id)

    def _updatePredecessorStatus(self, jobStoreID: str) -> None:
        """Update status of predecessors for finished (possibly failed) successor job."""
        if jobStoreID in self.toilState.service_to_client:
            # Is a service host job, so its predecessor is its client
            client_id = self.toilState.service_to_client.pop(jobStoreID)
            self.toilState.servicesIssued[client_id].remove(jobStoreID)
            if len(self.toilState.servicesIssued[client_id]) == 0: # Predecessor job has
                # all its services terminated
                self.toilState.servicesIssued.pop(client_id) # The job has no running services

                logger.debug('Job %s is no longer waiting on services; all services have stopped', self.toilState.get_job(client_id))

                # Now we know the job is done we can add it to the list of
                # updated job files
                self._messages.publish(JobUpdatedMessage(client_id, 0))
            else:
                logger.debug('Job %s is still waiting on %d services',
                             self.toilState.get_job(client_id),
                             len(self.toilState.servicesIssued[client_id]))
        elif jobStoreID not in self.toilState.successor_to_predecessors:
            #We have reach the root job
            assert self._messages.count(JobUpdatedMessage) == 0, "Root job is done but other jobs are still updated"
            assert len(self.toilState.successor_to_predecessors) == 0, \
                ("Job {} is finished and had no predecessor, but we have other outstanding jobs "
                 "with predecessors: {}".format(jobStoreID, self.toilState.successor_to_predecessors.keys()))
            assert len(self.toilState.successorCounts) == 0, f"Root job is done but jobs waiting on successors: {self.toilState.successorCounts}"
            logger.debug("Reached root job %s so no predecessors to clean up" % jobStoreID)

        else:
            # Is a non-root, non-service job
            logger.debug("Cleaning the predecessors of %s" % jobStoreID)

            # For each predecessor
            for predecessor_id in self.toilState.successor_to_predecessors.pop(jobStoreID):
                assert isinstance(predecessor_id, str), f"Predecessor ID should be str but is {type(predecessor_id)}"
                predecessor = self.toilState.get_job(predecessor_id)

                # Tell the predecessor that this job is done (keep only other successor jobs)
                predecessor.filterSuccessors(lambda jID: jID != jobStoreID)

                # Reduce the predecessor's number of successors by one, as
                # tracked by us, to indicate the completion of the jobStoreID
                # job
                self.toilState.successor_returned(predecessor_id)

                # If the predecessor job is done and all the successors are complete
                if self.toilState.count_pending_successors(predecessor_id) == 0:
                    # Now we know the job is done we can add it to the list of updated job files
                    self._messages.publish(JobUpdatedMessage(predecessor_id, 0))
