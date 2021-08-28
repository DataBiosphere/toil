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
The leader script (of the leader/worker pair) for running jobs.
"""

import base64
import glob
import logging
import os
import pickle
import sys
import time
from typing import List

import enlighten

from toil import resolveEntryPoint
from toil.batchSystems import DeadlockException
from toil.batchSystems.abstractBatchSystem import BatchJobExitReason
from toil.common import Toil, ToilMetrics
from toil.job import JobDescription, CheckpointJobDescription, ServiceJobDescription
from toil.jobStores.abstractJobStore import AbstractJobStore, NoSuchJobException
from toil.lib.conversions import bytes2human
from toil.lib.throttle import LocalThrottle
from toil.provisioners.abstractProvisioner import AbstractProvisioner
from toil.provisioners.clusterScaler import ScalerThread
from toil.serviceManager import ServiceManager
from toil.statsAndLogging import StatsAndLogging
from toil.toilState import ToilState

try:
    from toil.cwl.cwltoil import CWL_INTERNAL_JOBS, CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE
except ImportError:
    # CWL extra not installed
    CWL_INTERNAL_JOBS = ()
    CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE = 0


logger = logging.getLogger( __name__ )

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


class FailedJobsException(Exception):
    def __init__(self, jobStoreLocator: str, failedJobs: List[JobDescription], jobStore: AbstractJobStore, exit_code: int = 1):
        """
        Make an exception to report failed jobs.

        :param jobStoreLocator: The job store locator that says what job store has the failed jobs.
        :param failedJobs: All the failed jobs
        :param jobStore: The actual open job store with the failed jobs in it.
        :param exit_code: Recommended process exit code.

        """
        self.msg = "The job store '%s' contains %i failed jobs" % (jobStoreLocator, len(failedJobs))
        self.exit_code = exit_code
        try:
            self.msg += ": %s" % ", ".join((str(failedJob) for failedJob in failedJobs))
            for jobDesc in failedJobs:
                # Reload from JobStore. TODO: avoid this!
                jobDesc = jobStore.load(jobDesc.jobStoreID)
                if jobDesc.logJobStoreFileID:
                    with jobDesc.getLogFileHandle(jobStore) as fH:
                        self.msg += "\n" + StatsAndLogging.formatLogStream(fH, jobDesc)
        # catch failures to prepare more complex details and only return the basics
        except:
            logger.exception('Exception when compiling information about failed jobs')
        self.msg = self.msg.rstrip('\n')
        super().__init__()
        self.jobStoreLocator = jobStoreLocator
        self.numberOfFailedJobs = len(failedJobs)

    def __str__(self):
        """
        Stringify the exception, including the message.
        """
        return self.msg


####################################################
##Following class represents the leader
####################################################

class Leader(object):
    """ Class that encapsulates the logic of the leader.
    """
    def __init__(self, config, batchSystem, provisioner: AbstractProvisioner, jobStore, rootJob, jobCache=None):
        """
        :param toil.common.Config config:
        :param toil.batchSystems.abstractBatchSystem.AbstractBatchSystem batchSystem:
        :param toil.provisioners.abstractProvisioner.AbstractProvisioner provisioner:
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore:
        :param toil.job.JobDescription rootJob:

        If jobCache is passed, it must be a dict from job ID to pre-existing
        JobDescription objects. Jobs will be loaded from the cache (which can be
        downloaded from the jobStore in a batch) during the construction of the ToilState object.
        """
        # Object containing parameters for the run
        self.config = config

        # The job store
        self.jobStore = jobStore
        self.jobStoreLocator = config.jobStore

        # Get a snap shot of the current state of the jobs in the jobStore
        self.toilState = ToilState(jobStore, rootJob, jobCache=jobCache)
        logger.debug("Found %s jobs to start and %i jobs with successors to run",
                     len(self.toilState.updatedJobs), len(self.toilState.successorCounts))

        # Batch system
        self.batchSystem = batchSystem
        assert len(self.batchSystem.getIssuedBatchJobIDs()) == 0  # Batch system must start with no active jobs!
        logger.debug("Checked batch system has no running jobs and no updated jobs")

        # Map of batch system IDs to IssuedJob tuples
        self.jobBatchSystemIDToIssuedJob = {}

        # Number of preemptible jobs currently being run by batch system
        self.preemptableJobsIssued = 0

        # Tracking the number service jobs issued,
        # this is used limit the number of services issued to the batch system
        self.serviceJobsIssued = 0
        self.serviceJobsToBeIssued = [] # A queue of service jobs that await scheduling
        #Equivalents for service jobs to be run on preemptible nodes
        self.preemptableServiceJobsIssued = 0
        self.preemptableServiceJobsToBeIssued = []

        # Timing of the rescuing method
        self.timeSinceJobsLastRescued = None

        # Hash to store number of times a job is lost by the batch system,
        # used to decide if to reissue an apparently missing job
        self.reissueMissingJobs_missingHash = {}

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
        self.potentialDeadlockedJobs = set()
        self.potentialDeadlockTime = 0

        # A dashboard that runs on the leader node in AWS clusters to track the state
        # of the cluster
        self.toilMetrics = None

        # internal jobs we should not expose at top level debugging
        self.debugJobNames = ("CWLJob", "CWLWorkflow", "CWLScatter", "CWLGather",
                              "ResolveIndirect")

        self.deadlockThrottler = LocalThrottle(self.config.deadlockCheckInterval)

        self.statusThrottler = LocalThrottle(self.config.statusWait)

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

        # What exit code should the process use if the workflow failed?
        # Needed in case a worker detects a CWL issue that a CWL runner must
        # report to its caller.
        self.recommended_fail_exit_code = 1

        # TODO: No way to set background color on the terminal for the bar.

    def run(self):
        """
        This runs the leader process to issue and manage jobs.

        :raises: toil.leader.FailedJobsException if failed jobs remain after running.

        :return: The return value of the root job's run function.
        :rtype: Any
        """

        with enlighten.get_manager(stream=sys.stderr, enabled=not self.config.disableProgress) as manager:
            # Set up the fancy console UI if desirable
            self.progress_overall = manager.counter(total=0, desc='Workflow Progress', unit='jobs',
                                                    color=self.GOOD_COLOR, bar_format=self.PROGRESS_BAR_FORMAT)
            self.progress_failed = self.progress_overall.add_subcounter(self.BAD_COLOR)

            # Start the stats/logging aggregation thread
            self.statsAndLogging.start()
            if self.config.metrics:
                self.toilMetrics = ToilMetrics(provisioner=self.provisioner)

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
            self.toilState.totalFailedJobs = [j for j in self.toilState.totalFailedJobs if self.jobStore.exists(j.jobStoreID)]

            try:
                self.create_status_sentinel_file(self.toilState.totalFailedJobs)
            except IOError as e:
                logger.debug('Error from importFile with hardlink=True: {}'.format(e))

            logger.info("Finished toil run %s" %
                         ("successfully." if not self.toilState.totalFailedJobs \
                    else ("with %s failed jobs." % len(self.toilState.totalFailedJobs))))

            if len(self.toilState.totalFailedJobs):
                logger.info("Failed jobs at end of the run: %s", ' '.join(str(job) for job in self.toilState.totalFailedJobs))
                raise FailedJobsException(self.config.jobStore, self.toilState.totalFailedJobs,
                                          self.jobStore, exit_code=self.recommended_fail_exit_code)

            return self.jobStore.getRootJobReturnValue()

    def create_status_sentinel_file(self, fail):
        """Create a file in the jobstore indicating failure or success."""
        logName = 'failed.log' if fail else 'succeeded.log'
        localLog = os.path.join(os.getcwd(), logName)
        open(localLog, 'w').close()
        self.jobStore.importFile('file://' + localLog, logName, hardlink=True)

        if os.path.exists(localLog):  # Bandaid for Jenkins tests failing stochastically and unexplainably.
            os.remove(localLog)

    def _handledFailedSuccessor(self, successor, predecessor):
        """
        Deal with the successor having failed. Return True if there are
        still active successors. Return False if all successors have failed
        and the job is queued to run to handle the failed successors.

        :param toil.job.JobDescription successor: The successor which has failed.
        :param toil.job.JobDescription predecessor: The job which the successor comes after.

        """
        logger.debug("Successor job: %s of job: %s has failed """
                     "predecessors", successor, predecessor)

        # Add the job to the set having failed successors
        self.toilState.hasFailedSuccessors.add(predecessor.jobStoreID)

        # Reduce active successor count and remove the successor as an active successor of the job
        self.toilState.successorCounts[predecessor.jobStoreID] -= 1
        assert self.toilState.successorCounts[predecessor.jobStoreID] >= 0
        self.toilState.successorJobStoreIDToPredecessorJobs[successor.jobStoreID].remove(predecessor)
        if len(self.toilState.successorJobStoreIDToPredecessorJobs[successor.jobStoreID]) == 0:
            self.toilState.successorJobStoreIDToPredecessorJobs.pop(successor.jobStoreID)

        # If the job now has no active successors, add to active jobs
        # so it can be processed as a job with failed successors.
        if self.toilState.successorCounts[predecessor.jobStoreID] == 0:
            logger.debug("Job: %s has no successors to run "
                         "and some are failed, adding to list of jobs "
                         "with failed successors", predecessor)
            self.toilState.successorCounts.pop(predecessor.jobStoreID)
            if predecessor.jobStoreID not in self.toilState.updatedJobs:
                self.toilState.updatedJobs[predecessor.jobStoreID] = (predecessor, 0)
            return False


    def _checkSuccessorReadyToRunMultiplePredecessors(self, successor, predecessor):
        """
        Handle the special cases of checking if a successor job is
        ready to run when there are multiple predecessors.

        :param toil.job.JobDescription successor: The successor which has failed.
        :param toil.job.JobDescription predecessor: The job which the successor comes after.

        """
        # See implementation note at the top of this file for discussion of multiple predecessors
        logger.debug("Successor job: %s of job: %s has multiple "
                     "predecessors", successor, predecessor)

        # Get the successor JobDescription, which is cached
        if successor.jobStoreID not in self.toilState.jobsToBeScheduledWithMultiplePredecessors:
            # TODO: We're loading from the job store in an ad-hoc way!
            loaded = self.jobStore.load(successor.jobStoreID)
            self.toilState.jobsToBeScheduledWithMultiplePredecessors[successor.jobStoreID] = loaded
        # TODO: we're clobbering a JobDescription we're passing around by value.
        successor = self.toilState.jobsToBeScheduledWithMultiplePredecessors[successor.jobStoreID]

        # Add the predecessor as a finished predecessor to the successor
        successor.predecessorsFinished.add(predecessor.jobStoreID)

        # If the successor is in the set of successors of failed jobs
        if successor.jobStoreID in self.toilState.failedSuccessors:
            if not self._handledFailedSuccessor(successor, predecessor):
                return False

        # If the successor job's predecessors have all not all completed then
        # ignore the successor as is not yet ready to run
        assert len(successor.predecessorsFinished) <= successor.predecessorNumber
        if len(successor.predecessorsFinished) < successor.predecessorNumber:
            return False
        else:
            # Remove the successor job from the cache
            self.toilState.jobsToBeScheduledWithMultiplePredecessors.pop(successor.jobStoreID)
            return True

    def _makeJobSuccessorReadyToRun(self, successor, predecessor):
        """
        Make a successor job ready to run if possible, returning False if it should
        not yet be run or True otherwise.

        :param toil.job.JobDescription successor: The successor which should become ready.
        :param toil.job.JobDescription predecessor: The job which the successor comes after.
        """
        #Build map from successor to predecessors.
        if successor.jobStoreID not in self.toilState.successorJobStoreIDToPredecessorJobs:
            self.toilState.successorJobStoreIDToPredecessorJobs[successor.jobStoreID] = []
        self.toilState.successorJobStoreIDToPredecessorJobs[successor.jobStoreID].append(predecessor)
        logger.debug("Added job %s as coming after job %s", successor, predecessor)

        if successor.predecessorNumber > 1:
            return self._checkSuccessorReadyToRunMultiplePredecessors(successor, predecessor)
        else:
            return True

    def _runJobSuccessors(self, predecessor):
        """
        Issue the successors of a job.

        :param toil.job.JobDescription predecessor: The job which the successors come after.
        """

        # TODO: rewrite!

        assert len(predecessor.stack[-1]) > 0
        logger.debug("Job: %s has %i successors to schedule",
                     predecessor.jobStoreID, len(predecessor.stack[-1]))
        #Record the number of successors that must be completed before
        #the job can be considered again
        assert predecessor.jobStoreID not in self.toilState.successorCounts, 'Attempted to schedule successors of the same job twice!'
        self.toilState.successorCounts[predecessor.jobStoreID] = len(predecessor.stack[-1])

        # For each successor schedule if all predecessors have been completed
        successors = []
        for successorID in predecessor.stack[-1]:
            try:
                successor = self.jobStore.load(successorID)
            except NoSuchJobException:
                # Job already done and gone
                logger.warning("Job %s is a successor of %s but is already done and gone.", successorID, predecessor.jobStoreID)
                # Don't try and run it
                continue
            if self._makeJobSuccessorReadyToRun(successor, predecessor):
                successors.append(successor)
        self.issueJobs(successors)

    def _processFailedSuccessors(self, predecessor):
        """Some of the jobs successors failed then either fail the job
        or restart it if it has retries left and is a checkpoint job"""

        if predecessor.jobStoreID in self.toilState.servicesIssued:
            # The job has services running; signal for them to be killed.
            # Once they are killed, then the job will be re-added to
            # the updatedJobs dict and then scheduled to be removed.
            logger.debug("Telling job %s to terminate its services due to successor failure",
                         predecessor)
            self.serviceManager.killServices(self.toilState.servicesIssued[predecessor.jobStoreID],
                                             error=True)
        elif predecessor.jobStoreID in self.toilState.successorCounts:
            # The job has non-service jobs running; wait for them to finish.
            # the job will be re-added to the updated jobs when these jobs
            # are done
            logger.debug("Job %s with ID: %s with failed successors still has successor jobs running",
                         predecessor, predecessor.jobStoreID)
        elif isinstance(predecessor, CheckpointJobDescription) and predecessor.checkpoint is not None and predecessor.remainingTryCount > 1:
            # If the job is a checkpoint and has remaining retries then reissue it.
            # The logic behind using > 1 rather than > 0 here: Since this job has
            # been tried once (without decreasing its try count as the job
            # itself was successful), and its subtree failed, it shouldn't be retried
            # unless it has more than 1 try.
            logger.warning('Job: %s is being restarted as a checkpoint after the total '
                        'failure of jobs in its subtree.', predecessor.jobStoreID)
            self.issueJob(predecessor)
        else:
            # Mark it totally failed
            logger.debug("Job %s is being processed as completely failed", predecessor.jobStoreID)
            self.processTotallyFailedJob(predecessor)

    def _processReadyJob(self, readyJob, resultStatus):
        logger.debug('Updating status of job %s with result status: %s',
                     readyJob, resultStatus)

        if readyJob in self.serviceManager.jobDescriptionsWithServicesBeingStarted:
            # This stops a job with services being issued by the serviceManager from
            # being considered further in this loop. This catch is necessary because
            # the job's service's can fail while being issued, causing the job to be
            # added to updated jobs.
            logger.debug("Got a job to update which is still owned by the service "
                         "manager: %s", readyJob.jobStoreID)
        elif readyJob.jobStoreID in self.toilState.hasFailedSuccessors:
            self._processFailedSuccessors(readyJob)
        elif readyJob.command is not None or resultStatus != 0:
            # The job has a command it must be run before any successors.
            # Similarly, if the job previously failed we rerun it, even if it doesn't have a
            # command to run, to eliminate any parts of the stack now completed.
            isServiceJob = readyJob.jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob

            # We want to run the job, and expend one of its "tries" (possibly
            # the initial one)

            # If the job has run out of tries or is a service job whose error flag has
            # been indicated, fail the job.
            if (readyJob.remainingTryCount == 0 or
                (isServiceJob and not self.jobStore.fileExists(readyJob.errorJobStoreID))):
                self.processTotallyFailedJob(readyJob)
                logger.warning("Job %s is completely failed", readyJob)
            else:
                # Otherwise try the job again
                self.issueJob(readyJob)
        elif next(readyJob.serviceHostIDsInBatches(), None) is not None:
            # the job has services to run, which have not been started, start them
            # Build a map from the service jobs to the job and a map
            # of the services created for the job
            assert readyJob.jobStoreID not in self.toilState.servicesIssued
            self.toilState.servicesIssued[readyJob.jobStoreID] = {}
            for serviceJobList in readyJob.serviceHostIDsInBatches():
                for serviceID in serviceJobList:
                    assert serviceID not in self.toilState.serviceJobStoreIDToPredecessorJob
                    serviceHost = self.jobStore.load(serviceID)
                    self.toilState.serviceJobStoreIDToPredecessorJob[serviceID] = readyJob
                    self.toilState.servicesIssued[readyJob.jobStoreID][serviceID] = serviceHost

            # Use the service manager to start the services
            self.serviceManager.scheduleServices(readyJob)

            logger.debug("Giving job: %s to service manager to schedule its jobs", readyJob.jobStoreID)
        elif len(readyJob.stack) > 0:
            # There are exist successors to run
            self._runJobSuccessors(readyJob)
        elif readyJob.jobStoreID in self.toilState.servicesIssued:
            logger.debug("Telling job: %s to terminate its services due to the "
                         "successful completion of its successor jobs",
                         readyJob)
            self.serviceManager.killServices(self.toilState.servicesIssued[readyJob.jobStoreID], error=False)
        else:
            #There are no remaining tasks to schedule within the job, but
            #we schedule it anyway to allow it to be deleted. Remove the job

            # TODO: resize down here so it doesn't schedule at full original size just to delete itself!

            #TODO: An alternative would be simple delete it here and add it to the
            #list of jobs to process, or (better) to create an asynchronous
            #process that deletes jobs and then feeds them back into the set
            #of jobs to be processed
            if readyJob.remainingTryCount > 0:
                self.issueJob(readyJob)
                logger.debug("Job: %s is empty, we are scheduling to clean it up", readyJob.jobStoreID)
            else:
                self.processTotallyFailedJob(readyJob)
                logger.warning("Job: %s is empty but completely failed - something is very wrong", readyJob.jobStoreID)

    def _processReadyJobs(self):
        """Process jobs that are ready to be scheduled/have successors to schedule"""
        logger.debug('Built the jobs list, currently have %i jobs to update and %i jobs issued',
                     len(self.toilState.updatedJobs), self.getNumberOfJobsIssued())

        updatedJobs = self.toilState.updatedJobs # The updated jobs to consider below
        self.toilState.updatedJobs = {} # Resetting the collection for the next group of updated jobs

        for updatedJob, resultStatus in updatedJobs.values():
            self._processReadyJob(updatedJob, resultStatus)

    def _startServiceJobs(self):
        """Start any service jobs available from the service manager"""
        self.issueQueingServiceJobs()
        while True:
            serviceJob = self.serviceManager.getServiceJobsToStart(0)
            # Stop trying to get jobs when function returns None
            if serviceJob is None:
                break

            logger.debug('Launching service job: %s', serviceJob)
            self.issueServiceJob(serviceJob)

    def _processJobsWithRunningServices(self):
        """Get jobs whose services have started"""
        while True:
            jobDesc = self.serviceManager.getJobDescriptionWhoseServicesAreRunning(0)
            if jobDesc is None: # Stop trying to get jobs when function returns None
                break
            logger.debug('Job: %s has established its services.', jobDesc.jobStoreID)

            # Drop all service relationships
            jobDesc.filterServiceHosts(lambda ignored: False)
            if jobDesc.jobStoreID not in self.toilState.updatedJobs:
                self.toilState.updatedJobs[jobDesc.jobStoreID] = (jobDesc, 0)

    def _processJobsWithFailedServices(self):
        """Get jobs whose services have failed to start"""
        while True:
            jobDesc = self.serviceManager.getJobDescriptionWhoseServicesFailedToStart(0)
            if jobDesc is None: # Stop trying to get jobs when function returns None
                break
            logger.debug('Job: %s has failed to establish its services.', jobDesc.jobStoreID)

            # Make sure services still want to run
            assert next(jobDesc.serviceHostIDsInBatches(), None) is not None

            if jobDesc.jobStoreID not in self.toilState.updatedJobs:
                # Mark the service job updated so we don't stop here.
                self.toilState.updatedJobs[jobDesc.jobStoreID] = (jobDesc, 1)

    def _gatherUpdatedJobs(self, updatedJobTuple):
        """Gather any new, updated JobDescriptions from the batch system"""
        jobID, exitStatus, exitReason, wallTime = (
            updatedJobTuple.jobID, updatedJobTuple.exitStatus, updatedJobTuple.exitReason,
            updatedJobTuple.wallTime)
        # easy, track different state
        try:
            updatedJob = self.jobBatchSystemIDToIssuedJob[jobID]
        except KeyError:
            logger.warning("A result seems to already have been processed for job %s", jobID)
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
            if self.toilMetrics:
                self.toilMetrics.logCompletedJob(updatedJob)
            self.processFinishedJob(jobID, exitStatus, wallTime=wallTime, exitReason=exitReason)

    def _processLostJobs(self):
        """Process jobs that have gone awry"""
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
        The main loop for processing jobs by the leader.
        """
        self.timeSinceJobsLastRescued = time.time()

        while self.toilState.updatedJobs or \
              self.getNumberOfJobsIssued() or \
              self.serviceManager.jobsIssuedToServiceManager:

            if self.toilState.updatedJobs:
                self._processReadyJobs()

            # deal with service-related jobs
            self._startServiceJobs()
            self._processJobsWithRunningServices()
            self._processJobsWithFailedServices()

            # check in with the batch system
            updatedJobTuple = self.batchSystem.getUpdatedBatchJob(maxWait=2)
            if updatedJobTuple is not None:
                self._gatherUpdatedJobs(updatedJobTuple)
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

            # Make sure to keep elapsed time and ETA up to date even when no jobs come in
            self.progress_overall.update(incr=0)

        logger.debug("Finished the main loop: no jobs left to run.")

        # Consistency check the toil state
        assert self.toilState.updatedJobs == {}
        assert self.toilState.successorCounts == {}
        assert self.toilState.successorJobStoreIDToPredecessorJobs == {}
        assert self.toilState.serviceJobStoreIDToPredecessorJob == {}
        assert self.toilState.servicesIssued == {}
        # assert self.toilState.jobsToBeScheduledWithMultiplePredecessors # These are not properly emptied yet
        # assert self.toilState.hasFailedSuccessors == set() # These are not properly emptied yet

    def checkForDeadlocks(self):
        """
        Checks if the system is deadlocked running service jobs.
        """

        totalRunningJobs = len(self.batchSystem.getRunningBatchJobIDs())
        totalServicesIssued = self.serviceJobsIssued + self.preemptableServiceJobsIssued

        # If there are no updated jobs and at least some jobs running
        if totalServicesIssued >= totalRunningJobs and totalRunningJobs > 0:
            serviceJobs = [x for x in list(self.jobBatchSystemIDToIssuedJob.keys()) if isinstance(self.jobBatchSystemIDToIssuedJob[x], ServiceJobDescription)]
            runningServiceJobs = set([x for x in serviceJobs if self.serviceManager.isRunning(self.jobBatchSystemIDToIssuedJob[x])])
            assert len(runningServiceJobs) <= totalRunningJobs

            # If all the running jobs are active services then we have a potential deadlock
            if len(runningServiceJobs) == totalRunningJobs:
                # There could be trouble; we are 100% services.
                # See if the batch system has anything to say for itself about its failure to run our jobs.
                message = self.batchSystem.getSchedulingStatusMessage()
                if message is not None:
                    # Prepend something explaining the message
                    message = "The batch system reports: {}".format(message)
                else:
                    # Use a generic message if none is available
                    message = "Cluster may be too small."


                # See if this is a new potential deadlock
                if self.potentialDeadlockedJobs != runningServiceJobs:
                    logger.warning(("Potential deadlock detected! All %s running jobs are service jobs, "
                                    "with no normal jobs to use them! %s"), totalRunningJobs, message)
                    self.potentialDeadlockedJobs = runningServiceJobs
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

                if len(self.potentialDeadlockedJobs) > 0:
                    # We thought we had a deadlock. Tell the user it is fixed.
                    logger.warning("Potential deadlock has been resolved; non-service jobs are now running.")

                self.potentialDeadlockedJobs = set()
                self.potentialDeadlockTime = 0
        else:
            # We have observed non-service jobs running, so reset the potential deadlock.
            # TODO: deduplicate with above

            if len(self.potentialDeadlockedJobs) > 0:
                # We thought we had a deadlock. Tell the user it is fixed.
                logger.warning("Potential deadlock has been resolved; non-service jobs are now running.")

            self.potentialDeadlockedJobs = set()
            self.potentialDeadlockTime = 0

    def issueJob(self, jobNode):
        """Add a job to the queue of jobs."""

        workerCommand = [resolveEntryPoint('_toil_worker'),
                         jobNode.jobName,
                         self.jobStoreLocator,
                         jobNode.jobStoreID]

        for context in self.batchSystem.getWorkerContexts():
            # For each context manager hook the batch system wants to run in
            # the worker, serialize and send it.
            workerCommand.append('--context')
            workerCommand.append(base64.b64encode(pickle.dumps(context)).decode('utf-8'))

        jobNode.command = ' '.join(workerCommand)

        omp_threads = os.environ.get('OMP_NUM_THREADS') \
            or str(max(1, int(jobNode.cores)))  # make sure OMP_NUM_THREADS is a positive integer

        job_environment = {
            # Set the number of cores used by OpenMP applications
            'OMP_NUM_THREADS': omp_threads,
        }

        # jobBatchSystemID is an int that is an incremented counter for each job
        jobBatchSystemID = self.batchSystem.issueBatchJob(jobNode, job_environment=job_environment)
        self.jobBatchSystemIDToIssuedJob[jobBatchSystemID] = jobNode
        if jobNode.preemptable:
            # len(jobBatchSystemIDToIssuedJob) should always be greater than or equal to preemptableJobsIssued,
            # so increment this value after the job is added to the issuedJob dict
            self.preemptableJobsIssued += 1
        cur_logger = logger.debug if jobNode.jobName.startswith(CWL_INTERNAL_JOBS) else logger.info
        cur_logger("Issued job %s with job batch system ID: "
                   "%s and cores: %s, disk: %s, and memory: %s",
                   jobNode, str(jobBatchSystemID), int(jobNode.cores),
                   bytes2human(jobNode.disk), bytes2human(jobNode.memory))
        if self.toilMetrics:
            self.toilMetrics.logIssuedJob(jobNode)
            self.toilMetrics.logQueueSize(self.getNumberOfJobsIssued())
        # Tell the user there's another job to do
        self.progress_overall.total += 1
        self.progress_overall.update(incr=0)

    def issueJobs(self, jobs):
        """Add a list of jobs, each represented as a jobNode object."""
        for job in jobs:
            self.issueJob(job)

    def issueServiceJob(self, jobNode):
        """
        Issue a service job, putting it on a queue if the maximum number of service
        jobs to be scheduled has been reached.
        """
        if jobNode.preemptable:
            self.preemptableServiceJobsToBeIssued.append(jobNode)
        else:
            self.serviceJobsToBeIssued.append(jobNode)
        self.issueQueingServiceJobs()

    def issueQueingServiceJobs(self):
        """Issues any queuing service jobs up to the limit of the maximum allowed."""
        while len(self.serviceJobsToBeIssued) > 0 and self.serviceJobsIssued < self.config.maxServiceJobs:
            self.issueJob(self.serviceJobsToBeIssued.pop())
            self.serviceJobsIssued += 1
        while len(self.preemptableServiceJobsToBeIssued) > 0 and self.preemptableServiceJobsIssued < self.config.maxPreemptableServiceJobs:
            self.issueJob(self.preemptableServiceJobsToBeIssued.pop())
            self.preemptableServiceJobsIssued += 1

    def getNumberOfJobsIssued(self, preemptable=None):
        """
        Gets number of jobs that have been added by issueJob(s) and not
        removed by removeJob

        :param None or boolean preemptable: If none, return all types of jobs.
          If true, return just the number of preemptable jobs. If false, return
          just the number of non-preemptable jobs.
        """
        if preemptable is None:
            return len(self.jobBatchSystemIDToIssuedJob)
        elif preemptable:
            return self.preemptableJobsIssued
        else:
            assert len(self.jobBatchSystemIDToIssuedJob) >= self.preemptableJobsIssued
            return len(self.jobBatchSystemIDToIssuedJob) - self.preemptableJobsIssued

    def _getStatusHint(self):
        """
        Get a short string describing the current state of the workflow for a human.

        Should include number of currently running jobs, number of issued jobs, etc.

        Don't call this too often; it will talk to the batch system, which may
        make queries of the backing scheduler.

        :return: A one-line description of the current status of the workflow.
        :rtype: str
        """

        issuedJobCount = self.getNumberOfJobsIssued()
        runningJobCount = len(self.batchSystem.getRunningBatchJobIDs())

        return "%d jobs are running, %d jobs are issued and waiting to run" % (runningJobCount, issuedJobCount - runningJobCount)

    def _reportWorkflowStatus(self):
        """
        Report the current status of the workflow to the user.
        """

        # For now just log our scheduling status message to the log.
        # TODO: make this update fast enought to put it in the progress
        # bar/status line.
        logger.info(self._getStatusHint())

    def removeJob(self, jobBatchSystemID):
        """
        Removes a job from the system.

        :return: Job description as it was issued.
        :rtype: toil.job.JobDescription
        """
        assert jobBatchSystemID in self.jobBatchSystemIDToIssuedJob
        issuedDesc = self.jobBatchSystemIDToIssuedJob[jobBatchSystemID]
        if issuedDesc.preemptable:
            # len(jobBatchSystemIDToIssuedJob) should always be greater than or equal to preemptableJobsIssued,
            # so decrement this value before removing the job from the issuedJob map
            assert self.preemptableJobsIssued > 0
            self.preemptableJobsIssued -= 1
        del self.jobBatchSystemIDToIssuedJob[jobBatchSystemID]
        # If service job
        if issuedDesc.jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob:
            # Decrement the number of services
            if issuedDesc.preemptable:
                self.preemptableServiceJobsIssued -= 1
            else:
                self.serviceJobsIssued -= 1

        # Tell the user that job is done, for progress purposes.
        self.progress_overall.update(incr=1)

        return issuedDesc

    def getJobs(self, preemptable=None):
        jobs = self.jobBatchSystemIDToIssuedJob.values()
        if preemptable is not None:
            jobs = [job for job in jobs if job.preemptable == preemptable]
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
                willRerun = self.processFinishedJob(jobBatchSystemID, 1, exitReason=BatchJobExitReason.KILLED)

                if willRerun:
                    # Compose a list of all the jobs that will run again
                    jobsRerunning.append(jobBatchSystemID)

        return jobsRerunning


    #Following functions handle error cases for when jobs have gone awry with the batch system.

    def reissueOverLongJobs(self):
        """
        Check each issued job - if it is running for longer than desirable
        issue a kill instruction.
        Wait for the job to die then we pass the job to processFinishedJob.
        """
        maxJobDuration = self.config.maxJobDuration
        jobsToKill = []
        if maxJobDuration < 10000000:  # We won't bother doing anything if rescue time > 16 weeks.
            runningJobs = self.batchSystem.getRunningBatchJobIDs()
            for jobBatchSystemID in list(runningJobs.keys()):
                if runningJobs[jobBatchSystemID] > maxJobDuration:
                    logger.warning("The job: %s has been running for: %s seconds, more than the "
                                "max job duration: %s, we'll kill it",
                                str(self.jobBatchSystemIDToIssuedJob[jobBatchSystemID].jobStoreID),
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
        then we pass the job to processFinishedJob.
        """
        issuedJobs = set(self.batchSystem.getIssuedBatchJobIDs())
        jobBatchSystemIDsSet = set(list(self.jobBatchSystemIDToIssuedJob.keys()))
        #Clean up the reissueMissingJobs_missingHash hash, getting rid of jobs that have turned up
        missingJobIDsSet = set(list(self.reissueMissingJobs_missingHash.keys()))
        for jobBatchSystemID in missingJobIDsSet.difference(jobBatchSystemIDsSet):
            self.reissueMissingJobs_missingHash.pop(jobBatchSystemID)
            logger.warning("Batch system id: %s is no longer missing", str(jobBatchSystemID))
        assert issuedJobs.issubset(jobBatchSystemIDsSet) #Assert checks we have
        #no unexpected jobs running
        jobsToKill = []
        for jobBatchSystemID in set(jobBatchSystemIDsSet.difference(issuedJobs)):
            jobStoreID = self.jobBatchSystemIDToIssuedJob[jobBatchSystemID].jobStoreID
            if jobBatchSystemID in self.reissueMissingJobs_missingHash:
                self.reissueMissingJobs_missingHash[jobBatchSystemID] += 1
            else:
                self.reissueMissingJobs_missingHash[jobBatchSystemID] = 1
            timesMissing = self.reissueMissingJobs_missingHash[jobBatchSystemID]
            logger.warning("Job store ID %s with batch system id %s is missing for the %i time",
                        jobStoreID, str(jobBatchSystemID), timesMissing)
            if self.toilMetrics:
                self.toilMetrics.logMissingJob()
            if timesMissing == killAfterNTimesMissing:
                self.reissueMissingJobs_missingHash.pop(jobBatchSystemID)
                jobsToKill.append(jobBatchSystemID)
        self.killJobs(jobsToKill)
        return len( self.reissueMissingJobs_missingHash ) == 0 #We use this to inform
        #if there are missing jobs

    def processRemovedJob(self, issuedJob, resultStatus):
        if resultStatus != 0:
            logger.warning("Despite the batch system claiming failure the "
                        "job %s seems to have finished and been removed", issuedJob)
        self._updatePredecessorStatus(issuedJob.jobStoreID)

    def processFinishedJob(self, batchSystemID, resultStatus, wallTime=None, exitReason=None):
        """
        Function reads a processed JobDescription file and updates its state.

        Return True if the job is going to run again, and False if the job is
        fully done or completely failed.
        """
        issuedJob = self.removeJob(batchSystemID)
        jobStoreID = issuedJob.jobStoreID
        if wallTime is not None and self.clusterScaler is not None:
            self.clusterScaler.addCompletedJob(issuedJob, wallTime)
        if self.jobStore.exists(jobStoreID):
            logger.debug("Job %s continues to exist (i.e. has more to do)", issuedJob)
            try:
                # Reload the job as modified by the worker
                replacementJob = self.jobStore.load(jobStoreID)
            except NoSuchJobException:
                # Avoid importing AWSJobStore as the corresponding extra might be missing
                if self.jobStore.__class__.__name__ == 'AWSJobStore':
                    # We have a ghost job - the job has been deleted but a stale read from
                    # SDB gave us a false positive when we checked for its existence.
                    # Process the job from here as any other job removed from the job store.
                    # This is a temporary work around until https://github.com/BD2KGenomics/toil/issues/1091
                    # is completed
                    logger.warning('Got a stale read from SDB for job %s', issuedJob)
                    self.processRemovedJob(issuedJob, resultStatus)
                    return
                else:
                    raise
            if replacementJob.logJobStoreFileID is not None:
                with replacementJob.getLogFileHandle(self.jobStore) as logFileStream:
                    # more memory efficient than read().striplines() while leaving off the
                    # trailing \n left when using readlines()
                    # http://stackoverflow.com/a/15233739
                    StatsAndLogging.logWithFormatting(jobStoreID, logFileStream, method=logger.warning,
                                                      message='The job seems to have left a log file, indicating failure: %s' % replacementJob)
                if self.config.writeLogs or self.config.writeLogsGzip:
                    with replacementJob.getLogFileHandle(self.jobStore) as logFileStream:
                        StatsAndLogging.writeLogFiles(replacementJob.chainedJobs, logFileStream, self.config, failed=True)
            if resultStatus != 0:
                # If the batch system returned a non-zero exit code then the worker
                # is assumed not to have captured the failure of the job, so we
                # reduce the try count here.
                if replacementJob.logJobStoreFileID is None:
                    logger.warning("No log file is present, despite job failing: %s", replacementJob)

                # Look for any standard output/error files created by the batch system.
                # They will only appear if the batch system actually supports
                # returning logs to the machine that submitted jobs, or if
                # --workDir / TOIL_WORKDIR is on a shared file system.
                # They live directly in the Toil work directory because that is
                # guaranteed to exist on the leader and workers.
                workDir = Toil.getToilWorkDir(self.config.workDir)
                # This must match the format in AbstractBatchSystem.formatStdOutErrPath()
                batchSystemFilePrefix =  f'toil_{self.config.workflowID}.{batchSystemID}'
                batchSystemFileGlob = os.path.join(workDir, batchSystemFilePrefix + '*.log')
                batchSystemFiles = glob.glob(batchSystemFileGlob)
                for batchSystemFile in batchSystemFiles:
                    try:
                        batchSystemFileStream = open(batchSystemFile, 'rb')
                    except:
                        logger.warning('The batch system left a file %s, but it could not be opened' % batchSystemFile)
                    else:
                        with batchSystemFileStream:
                            if os.path.getsize(batchSystemFile) > 0:
                                StatsAndLogging.logWithFormatting(jobStoreID, batchSystemFileStream, method=logger.warning,
                                                                  message='The batch system left a non-empty file %s:' % batchSystemFile)
                                if self.config.writeLogs or self.config.writeLogsGzip:
                                    batchSystemFileRoot, _ = os.path.splitext(os.path.basename(batchSystemFile))
                                    jobNames = replacementJob.chainedJobs
                                    if jobNames is None:   # For jobs that fail this way, replacementJob.chainedJobs is not guaranteed to be set
                                        jobNames = [str(replacementJob)]
                                    jobNames = [jobName + '_' + batchSystemFileRoot for jobName in jobNames]
                                    batchSystemFileStream.seek(0)
                                    StatsAndLogging.writeLogFiles(jobNames, batchSystemFileStream, self.config, failed=True)
                            else:
                                logger.warning('The batch system left an empty file %s' % batchSystemFile)

                replacementJob.setupJobAfterFailure(exitReason=exitReason)
                self.jobStore.update(replacementJob)

                # Show job as failed in progress (and take it from completed)
                self.progress_overall.update(incr=-1)
                self.progress_failed.update(incr=1)

            elif jobStoreID in self.toilState.hasFailedSuccessors:
                # If the job has completed okay, we can remove it from the list of jobs with failed successors
                self.toilState.hasFailedSuccessors.remove(jobStoreID)

            if replacementJob.jobStoreID not in self.toilState.updatedJobs:
                # Now that we know the job is done we can add it to the list of updated jobs
                self.toilState.updatedJobs[replacementJob.jobStoreID] = (replacementJob, resultStatus)
            logger.debug("Added job: %s to active jobs", replacementJob)

            # Return True if it will rerun (still has retries) and false if it
            # is completely failed.
            return replacementJob.remainingTryCount > 0
        else:  #The job is done
            self.processRemovedJob(issuedJob, resultStatus)
            # Being done, it won't run again.
            return False

    def getSuccessors(self, jobDesc, alreadySeenSuccessors, jobStore):
        """
        Gets successors of the given job by walking the job graph recursively.
        Any successor in alreadySeenSuccessors is ignored and not traversed.
        Returns the set of found successors. This set is added to alreadySeenSuccessors.
        """
        successors = set()
        def successorRecursion(jobDesc):
            # For lists of successors
            for successorList in jobDesc.stack:

                # For each successor in list of successors
                for successorID in successorList:

                    # If successor not already visited
                    if successorID not in alreadySeenSuccessors:

                        # Add to set of successors
                        successors.add(successorID)
                        alreadySeenSuccessors.add(successorID)

                        # Recurse if job exists
                        # (job may not exist if already completed)
                        if jobStore.exists(successorID):
                            loaded = jobStore.load(successorID)
                            successorRecursion(loaded)

        successorRecursion(jobDesc)  # Recurse from passed job

        return successors

    def processTotallyFailedJob(self, jobDesc):
        """
        Processes a totally failed job.
        """
        # Mark job as a totally failed job
        self.toilState.totalFailedJobs.add(jobDesc)
        if self.toilMetrics:
            self.toilMetrics.logFailedJob(jobDesc)

        if jobDesc.jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob:
            # Is a service job
            logger.debug("Service job is being processed as a totally failed job: %s", jobDesc)

            assert isinstance(jobDesc, ServiceJobDescription)

            # Grab the predecessor
            predecesssor = self.toilState.serviceJobStoreIDToPredecessorJob[jobDesc.jobStoreID]

            assert predecesssor.jobStoreID in self.toilState.servicesIssued

            # Leave the service job as a service of ist predecessor, because it
            # didn't work.

            # Poke predecessor job that had the service to kill its services,
            # and drop predecessor relationship from ToilState.
            self._updatePredecessorStatus(jobDesc.jobStoreID)

            # Signal to all other services in the group that they should
            # terminate. We do this to prevent other services in the set
            # of services from deadlocking waiting for this service to start
            # properly, and to remember that this service failed with an error
            # and possibly never started.
            if predecesssor.jobStoreID in self.toilState.servicesIssued:
                self.serviceManager.killServices(self.toilState.servicesIssued[predecesssor.jobStoreID], error=True)
                logger.debug("Job: %s is instructing all other services of its parent job to quit", jobDesc)

            # This ensures that the job will not attempt to run any of it's
            # successors on the stack
            self.toilState.hasFailedSuccessors.add(predecesssor.jobStoreID)

            # Remove the start flag, if it still exists. This indicates
            # to the service manager that the job has "started", and prevents
            # the service manager from waiting for it to start forever. It also
            # lets it continue, now that we have issued kill orders for them,
            # to start dependent services, which all need to actually fail
            # before we can finish up with the services' predecessor job.
            self.jobStore.deleteFile(jobDesc.startJobStoreID)
        else:
            # Is a non-service job
            assert jobDesc.jobStoreID not in self.toilState.servicesIssued
            assert not isinstance(jobDesc, ServiceJobDescription)

            # Traverse failed job's successor graph and get the jobStoreID of new successors.
            # Any successor already in toilState.failedSuccessors will not be traversed
            # All successors traversed will be added to toilState.failedSuccessors and returned
            # as a set (unseenSuccessors).
            unseenSuccessors = self.getSuccessors(jobDesc, self.toilState.failedSuccessors,
                                                  self.jobStore)
            logger.debug("Found new failed successors: %s of job: %s", " ".join(
                         unseenSuccessors), jobDesc)

            # For each newly found successor
            for successorJobStoreID in unseenSuccessors:

                # If the successor is a successor of other jobs that have already tried to schedule it
                if successorJobStoreID in self.toilState.successorJobStoreIDToPredecessorJobs:

                    # For each such predecessor job
                    # (we remove the successor from toilState.successorJobStoreIDToPredecessorJobs to avoid doing
                    # this multiple times for each failed predecessor)
                    for predecessorJob in self.toilState.successorJobStoreIDToPredecessorJobs.pop(successorJobStoreID):

                        # Reduce the predecessor job's successor count.
                        self.toilState.successorCounts[predecessorJob.jobStoreID] -= 1

                        # Indicate that it has failed jobs.
                        self.toilState.hasFailedSuccessors.add(predecessorJob.jobStoreID)
                        logger.debug("Marking job: %s as having failed successors (found by "
                                     "reading successors failed job)", predecessorJob)

                        # If the predecessor has no remaining successors, add to list of active jobs
                        assert self.toilState.successorCounts[predecessorJob.jobStoreID] >= 0
                        if self.toilState.successorCounts[predecessorJob.jobStoreID] == 0:
                            if predecessorJob.jobStoreID not in self.toilState.updatedJobs:
                                self.toilState.updatedJobs[predecessorJob.jobStoreID] = (predecessorJob, 0)

                            # Remove the predecessor job from the set of jobs with successors.
                            self.toilState.successorCounts.pop(predecessorJob.jobStoreID)

            # If the job has predecessor(s)
            if jobDesc.jobStoreID in self.toilState.successorJobStoreIDToPredecessorJobs:

                # For each predecessor of the job
                for predecessor in self.toilState.successorJobStoreIDToPredecessorJobs[jobDesc.jobStoreID]:

                    # Mark the predecessor as failed
                    self.toilState.hasFailedSuccessors.add(predecessor.jobStoreID)
                    logger.debug("Totally failed job: %s is marking direct predecessor: %s "
                                 "as having failed jobs", jobDesc, predecessor)

                self._updatePredecessorStatus(jobDesc.jobStoreID)

    def _updatePredecessorStatus(self, jobStoreID):
        """
        Update status of predecessors for finished (possibly failed) successor job.
        """
        if jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob:
            # Is a service job
            predecessorJob = self.toilState.serviceJobStoreIDToPredecessorJob.pop(jobStoreID)
            self.toilState.servicesIssued[predecessorJob.jobStoreID].pop(jobStoreID)
            if len(self.toilState.servicesIssued[predecessorJob.jobStoreID]) == 0: # Predecessor job has
                # all its services terminated
                self.toilState.servicesIssued.pop(predecessorJob.jobStoreID) # The job has no running services

                logger.debug('Job %s is no longer waiting on services', predecessorJob)

                if predecessorJob.jobStoreID not in self.toilState.updatedJobs:
                    # Now we know the job is done we can add it to the list of
                    # updated job files
                    self.toilState.updatedJobs[predecessorJob.jobStoreID] = (predecessorJob, 0)
            else:
                logger.debug('Job %s is still waiting on %d services', predecessorJob, len(self.toilState.servicesIssued[predecessorJob.jobStoreID]))
        elif jobStoreID not in self.toilState.successorJobStoreIDToPredecessorJobs:
            #We have reach the root job
            assert len(self.toilState.updatedJobs) == 0
            assert len(self.toilState.successorJobStoreIDToPredecessorJobs) == 0, \
                ("Job {} is finished and had no predecessor, but we have other outstanding jobs "
                 "with predecessors: {}".format(jobStoreID, self.toilState.successorJobStoreIDToPredecessorJobs.keys()))
            assert len(self.toilState.successorCounts) == 0
            logger.debug("Reached root job %s so no predecessors to clean up" % jobStoreID)

        else:
            # Is a non-root, non-service job
            logger.debug("Cleaning the predecessors of %s" % jobStoreID)

            # For each predecessor
            for predecessorJob in self.toilState.successorJobStoreIDToPredecessorJobs.pop(jobStoreID):

                # Tell the predecessor that this job is done (keep only other successor jobs)
                predecessorJob.filterSuccessors(lambda jID: jID != jobStoreID)

                # Reduce the predecessor's number of successors by one, as
                # tracked by us, to indicate the completion of the jobStoreID
                # job
                self.toilState.successorCounts[predecessorJob.jobStoreID] -= 1

                # If the predecessor job is done and all the successors are complete
                if self.toilState.successorCounts[predecessorJob.jobStoreID] == 0:

                    # Remove it from the set of jobs with active successors
                    self.toilState.successorCounts.pop(predecessorJob.jobStoreID)

                    # Now we know the job is done we can add it to the list of updated job files
                    assert predecessorJob.jobStoreID not in self.toilState.updatedJobs
                    self.toilState.updatedJobs[predecessorJob.jobStoreID] = (predecessorJob, 0)
