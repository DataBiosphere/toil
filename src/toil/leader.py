# Copyright (C) 2015-2016 Regents of the University of California
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
from __future__ import absolute_import

import cPickle
import logging
import gzip
import os
import time
from collections import namedtuple

from bd2k.util.expando import Expando

from toil import resolveEntryPoint
from toil.jobStores.abstractJobStore import NoSuchJobException
from toil.provisioners.clusterScaler import ClusterScaler
from toil.serviceManager import ServiceManager
from toil.statsAndLogging import StatsAndLogging
from toil.jobGraph import JobNode
from toil.toilState import ToilState

logger = logging.getLogger( __name__ )

####################################################
# Exception thrown by the Leader class when one or more jobs fails
####################################################

class FailedJobsException( Exception ):
    def __init__(self, jobStoreLocator, failedJobs, jobStore):
        msg = "The job store '%s' contains %i failed jobs" % (jobStoreLocator, len(failedJobs))
        try:
            msg += ": %s" % ", ".join((str(failedJob) for failedJob in failedJobs))
            for jobNode in failedJobs:
                job = jobStore.load(jobNode.jobStoreID)
                if job.logJobStoreFileID:
                    msg += "\n=========> Failed job %s \n" % jobNode
                    with job.getLogFileHandle(jobStore) as fH:
                        msg += fH.read()
                    msg += "<=========\n"
        # catch failures to prepare more complex details and only return the basics
        except:
            logger.exception('Exception when compiling information about failed jobs')
        super( FailedJobsException, self ).__init__(msg)
        self.jobStoreLocator = jobStoreLocator
        self.numberOfFailedJobs = len(failedJobs)

####################################################
# Exception thrown by the Leader class when a deadlock is encountered due to insufficient
# resources to run the workflow
####################################################

class DeadlockException( Exception ):
    def __init__(self, msg):
        msg = "Deadlock encountered: " + msg
        super( DeadlockException, self ).__init__(msg)

####################################################
##Following class represents the leader
####################################################

class Leader:
    """ Class that encapsulates the logic of the leader.
    """
    def __init__(self, config, batchSystem, provisioner, jobStore, rootJob, jobCache=None):
        """
        :param toil.common.Config config:
        :param toil.batchSystems.abstractBatchSystem.AbstractBatchSystem batchSystem:
        :param toil.provisioners.abstractProvisioner.AbstractProvisioner provisioner
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore:
        :param toil.jobGraph.JobGraph rootJob

        If jobCache is passed, it must be a dict from job ID to pre-existing
        JobGraph objects. Jobs will be loaded from the cache (which can be
        downloaded from the jobStore in a batch) during the construction of the ToilState object.
        """
        # Object containing parameters for the run
        self.config = config

        # The job store
        self.jobStore = jobStore
        self.jobStoreLocator = config.jobStore

        # Get a snap shot of the current state of the jobs in the jobStore
        self.toilState = ToilState(jobStore, rootJob, jobCache=jobCache)
        logger.info("Found %s jobs to start and %i jobs with successors to run",
                        len(self.toilState.updatedJobs), len(self.toilState.successorCounts))

        # Batch system
        self.batchSystem = batchSystem
        assert len(self.batchSystem.getIssuedBatchJobIDs()) == 0 #Batch system must start with no active jobs!
        logger.info("Checked batch system has no running jobs and no updated jobs")

        # Map of batch system IDs to IsseudJob tuples
        self.jobBatchSystemIDToIssuedJob = {}

        # Number of preempetable jobs currently being run by batch system
        self.preemptableJobsIssued = 0

        # Tracking the number service jobs issued,
        # this is used limit the number of services issued to the batch system
        self.serviceJobsIssued = 0
        self.serviceJobsToBeIssued = [] # A queue of service jobs that await scheduling
        #Equivalents for service jobs to be run on preemptable nodes
        self.preemptableServiceJobsIssued = 0
        self.preemptableServiceJobsToBeIssued = []

        # Hash to store number of times a job is lost by the batch system,
        # used to decide if to reissue an apparently missing job
        self.reissueMissingJobs_missingHash = {}

        # Class used to create/destroy nodes in the cluster, may be None if
        # using a statically defined cluster
        self.provisioner = provisioner

        # Create cluster scaling thread if the provisioner is not None
        self.clusterScaler = None if self.provisioner is None else ClusterScaler(self.provisioner, self, self.config)

        # A service manager thread to start and terminate services
        self.serviceManager = ServiceManager(jobStore, self.toilState)

        # A thread to manage the aggregation of statistics and logging from the run
        self.statsAndLogging = StatsAndLogging(self.jobStore, self.config)

        # Set used to monitor deadlocked jobs
        self.potentialDeadlockedJobs = None
        self.potentialDeadlockTime = 0

    def run(self):
        """
        This runs the leader process to issue and manage jobs.

        :raises: toil.leader.FailedJobsException if at the end of function their remain \
        failed jobs

        :return: The return value of the root job's run function.
        :rtype: Any
        """
        # Start the stats/logging aggregation thread
        self.statsAndLogging.start()
        try:

            # Start service manager thread
            self.serviceManager.start()
            try:

                # Create cluster scaling processes if not None
                if self.clusterScaler != None:
                    self.clusterScaler.start()

                try:
                    # Run the main loop
                    self.innerLoop()
                finally:
                    if self.clusterScaler is not None:
                        logger.info('Waiting for workers to shutdown')
                        startTime = time.time()
                        self.clusterScaler.shutdown()
                        logger.info('Worker shutdown complete in %s seconds', time.time() - startTime)

            finally:
                # Ensure service manager thread is properly shutdown
                self.serviceManager.shutdown()

        finally:
            # Ensure the stats and logging thread is properly shutdown
            self.statsAndLogging.shutdown()

        # Filter the failed jobs
        self.toilState.totalFailedJobs = filter(lambda j : self.jobStore.exists(j.jobStoreID), self.toilState.totalFailedJobs)

        logger.info("Finished toil run %s" %
                     ("successfully" if len(self.toilState.totalFailedJobs) == 0 else ("with %s failed jobs" % len(self.toilState.totalFailedJobs))))

        if len(self.toilState.totalFailedJobs):
            logger.info("Failed jobs at end of the run: %s", ' '.join(str(job) for job in self.toilState.totalFailedJobs))
        # Cleanup
        if len(self.toilState.totalFailedJobs) > 0:
            raise FailedJobsException(self.config.jobStore, self.toilState.totalFailedJobs, self.jobStore)

        # Parse out the return value from the root job
        with self.jobStore.readSharedFileStream('rootJobReturnValue') as fH:
            try:
                return cPickle.load(fH)
            except EOFError:
                logger.exception('Failed to unpickle root job return value')
                raise FailedJobsException(self.config.jobStore, self.toilState.totalFailedJobs, self.jobStore)

    def innerLoop(self):
        """
        The main loop for processing jobs by the leader.
        """
        # Sets up the timing of the jobGraph rescuing method
        timeSinceJobsLastRescued = time.time()

        logger.info("Starting the main loop")
        while True:
            # Process jobs that are ready to be scheduled/have successors to schedule
            if len(self.toilState.updatedJobs) > 0:
                logger.debug('Built the jobs list, currently have %i jobs to update and %i jobs issued',
                             len(self.toilState.updatedJobs), self.getNumberOfJobsIssued())

                updatedJobs = self.toilState.updatedJobs # The updated jobs to consider below
                self.toilState.updatedJobs = set() # Resetting the list for the next set

                for jobGraph, resultStatus in updatedJobs:

                    logger.debug('Updating status of job %s with ID %s: with result status: %s',
                                 jobGraph, jobGraph.jobStoreID, resultStatus)

                    # This stops a job with services being issued by the serviceManager from
                    # being considered further in this loop. This catch is necessary because
                    # the job's service's can fail while being issued, causing the job to be
                    # added to updated jobs.
                    if jobGraph in self.serviceManager.jobGraphsWithServicesBeingStarted:
                        logger.debug("Got a job to update which is still owned by the service "
                                     "manager: %s", jobGraph.jobStoreID)
                        continue

                    # If some of the jobs successors failed then either fail the job
                    # or restart it if it has retries left and is a checkpoint job
                    if jobGraph.jobStoreID in self.toilState.hasFailedSuccessors:

                        # If the job has services running, signal for them to be killed
                        # once they are killed then the jobGraph will be re-added to the
                        # updatedJobs set and then scheduled to be removed
                        if jobGraph.jobStoreID in self.toilState.servicesIssued:
                            logger.debug("Telling job: %s to terminate its services due to successor failure",
                                         jobGraph.jobStoreID)
                            self.serviceManager.killServices(self.toilState.servicesIssued[jobGraph.jobStoreID],
                                                        error=True)

                        # If the job has non-service jobs running wait for them to finish
                        # the job will be re-added to the updated jobs when these jobs are done
                        elif jobGraph.jobStoreID in self.toilState.successorCounts:
                            logger.debug("Job %s with ID: %s with failed successors still has successor jobs running",
                                         jobGraph, jobGraph.jobStoreID)
                            continue

                        # If the job is a checkpoint and has remaining retries then reissue it.
                        elif jobGraph.checkpoint is not None and jobGraph.remainingRetryCount > 0:
                            logger.warn('Job: %s is being restarted as a checkpoint after the total '
                                        'failure of jobs in its subtree.', jobGraph.jobStoreID)
                            self.issueJob(JobNode.fromJobGraph(jobGraph))
                        else: # Mark it totally failed
                            logger.debug("Job %s is being processed as completely failed", jobGraph.jobStoreID)
                            self.processTotallyFailedJob(jobGraph)

                    # If the jobGraph has a command it must be run before any successors.
                    # Similarly, if the job previously failed we rerun it, even if it doesn't have a
                    # command to run, to eliminate any parts of the stack now completed.
                    elif jobGraph.command is not None or resultStatus != 0:
                        isServiceJob = jobGraph.jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob

                        # If the job has run out of retries or is a service job whose error flag has
                        # been indicated, fail the job.
                        if (jobGraph.remainingRetryCount == 0
                            or isServiceJob and not self.jobStore.fileExists(jobGraph.errorJobStoreID)):
                            self.processTotallyFailedJob(jobGraph)
                            logger.warn("Job %s with ID %s is completely failed",
                                        jobGraph, jobGraph.jobStoreID)
                        else:
                            # Otherwise try the job again
                            self.issueJob(JobNode.fromJobGraph(jobGraph))

                    # If the job has services to run, which have not been started, start them
                    elif len(jobGraph.services) > 0:
                        # Build a map from the service jobs to the job and a map
                        # of the services created for the job
                        assert jobGraph.jobStoreID not in self.toilState.servicesIssued
                        self.toilState.servicesIssued[jobGraph.jobStoreID] = {}
                        for serviceJobList in jobGraph.services:
                            for serviceTuple in serviceJobList:
                                serviceID = serviceTuple.jobStoreID
                                assert serviceID not in self.toilState.serviceJobStoreIDToPredecessorJob
                                self.toilState.serviceJobStoreIDToPredecessorJob[serviceID] = jobGraph
                                self.toilState.servicesIssued[jobGraph.jobStoreID][serviceID] = serviceTuple

                        # Use the service manager to start the services
                        self.serviceManager.scheduleServices(jobGraph)

                        logger.debug("Giving job: %s to service manager to schedule its jobs", jobGraph.jobStoreID)

                    # There exist successors to run
                    elif len(jobGraph.stack) > 0:
                        assert len(jobGraph.stack[-1]) > 0
                        logger.debug("Job: %s has %i successors to schedule",
                                     jobGraph.jobStoreID, len(jobGraph.stack[-1]))
                        #Record the number of successors that must be completed before
                        #the jobGraph can be considered again
                        assert jobGraph.jobStoreID not in self.toilState.successorCounts
                        self.toilState.successorCounts[jobGraph.jobStoreID] = len(jobGraph.stack[-1])
                        #List of successors to schedule
                        successors = []

                        #For each successor schedule if all predecessors have been completed
                        for jobNode in jobGraph.stack[-1]:
                            successorJobStoreID = jobNode.jobStoreID
                            #Build map from successor to predecessors.
                            if successorJobStoreID not in self.toilState.successorJobStoreIDToPredecessorJobs:
                                self.toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID] = []
                            self.toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID].append(jobGraph)
                            #Case that the jobGraph has multiple predecessors
                            if jobNode.predecessorNumber > 1:
                                logger.debug("Successor job: %s of job: %s has multiple "
                                             "predecessors", jobNode, jobGraph)

                                # Get the successor job, using a cache
                                # (if the successor job has already been seen it will be in this cache,
                                # but otherwise put it in the cache)
                                if successorJobStoreID not in self.toilState.jobsToBeScheduledWithMultiplePredecessors:
                                    self.toilState.jobsToBeScheduledWithMultiplePredecessors[successorJobStoreID] = self.jobStore.load(successorJobStoreID)
                                successorJobGraph = self.toilState.jobsToBeScheduledWithMultiplePredecessors[successorJobStoreID]

                                #Add the jobGraph as a finished predecessor to the successor
                                successorJobGraph.predecessorsFinished.add(jobGraph.jobStoreID)

                                # If the successor is in the set of successors of failed jobs
                                if successorJobStoreID in self.toilState.failedSuccessors:
                                    logger.debug("Successor job: %s of job: %s has failed "
                                                 "predecessors", jobNode, jobGraph)

                                    # Add the job to the set having failed successors
                                    self.toilState.hasFailedSuccessors.add(jobGraph.jobStoreID)

                                    # Reduce active successor count and remove the successor as an active successor of the job
                                    self.toilState.successorCounts[jobGraph.jobStoreID] -= 1
                                    assert self.toilState.successorCounts[jobGraph.jobStoreID] >= 0
                                    self.toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID].remove(jobGraph)
                                    if len(self.toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID]) == 0:
                                        self.toilState.successorJobStoreIDToPredecessorJobs.pop(successorJobStoreID)

                                    # If the job now has no active successors add to active jobs
                                    # so it can be processed as a job with failed successors
                                    if self.toilState.successorCounts[jobGraph.jobStoreID] == 0:
                                        logger.debug("Job: %s has no successors to run "
                                                     "and some are failed, adding to list of jobs "
                                                     "with failed successors", jobGraph)
                                        self.toilState.successorCounts.pop(jobGraph.jobStoreID)
                                        self.toilState.updatedJobs.add((jobGraph, 0))
                                        continue

                                # If the successor job's predecessors have all not all completed then
                                # ignore the jobGraph as is not yet ready to run
                                assert len(successorJobGraph.predecessorsFinished) <= successorJobGraph.predecessorNumber
                                if len(successorJobGraph.predecessorsFinished) < successorJobGraph.predecessorNumber:
                                    continue
                                else:
                                    # Remove the successor job from the cache
                                    self.toilState.jobsToBeScheduledWithMultiplePredecessors.pop(successorJobStoreID)

                            # Add successor to list of successors to schedule
                            successors.append(jobNode)
                        self.issueJobs(successors)

                    elif jobGraph.jobStoreID in self.toilState.servicesIssued:
                        logger.debug("Telling job: %s to terminate its services due to the "
                                     "successful completion of its successor jobs",
                                     jobGraph)
                        self.serviceManager.killServices(self.toilState.servicesIssued[jobGraph.jobStoreID], error=False)

                    #There are no remaining tasks to schedule within the jobGraph, but
                    #we schedule it anyway to allow it to be deleted.

                    #TODO: An alternative would be simple delete it here and add it to the
                    #list of jobs to process, or (better) to create an asynchronous
                    #process that deletes jobs and then feeds them back into the set
                    #of jobs to be processed
                    else:
                        # Remove the job
                        if jobGraph.remainingRetryCount > 0:
                            self.issueJob(JobNode.fromJobGraph(jobGraph))
                            logger.debug("Job: %s is empty, we are scheduling to clean it up", jobGraph.jobStoreID)
                        else:
                            self.processTotallyFailedJob(jobGraph)
                            logger.warn("Job: %s is empty but completely failed - something is very wrong", jobGraph.jobStoreID)

            # Start any service jobs available from the service manager
            self.issueQueingServiceJobs()
            while True:
                serviceJob = self.serviceManager.getServiceJobsToStart(0)
                # Stop trying to get jobs when function returns None
                if serviceJob is None:
                    break
                logger.debug('Launching service job: %s', serviceJob)
                self.issueServiceJob(serviceJob)

            # Get jobs whose services have started
            while True:
                jobGraph = self.serviceManager.getJobGraphWhoseServicesAreRunning(0)
                if jobGraph is None: # Stop trying to get jobs when function returns None
                    break
                logger.debug('Job: %s has established its services.', jobGraph.jobStoreID)
                jobGraph.services = []
                self.toilState.updatedJobs.add((jobGraph, 0))

            # Gather any new, updated jobGraph from the batch system
            updatedJobTuple = self.batchSystem.getUpdatedBatchJob(2)
            if updatedJobTuple is not None:
                jobID, result, wallTime = updatedJobTuple
                # easy, track different state
                try:
                    updatedJob = self.jobBatchSystemIDToIssuedJob[jobID]
                except KeyError:
                    logger.warn("A result seems to already have been processed "
                                "for job %s", jobID)
                else:
                    if result == 0:
                        logger.debug('Batch system is reporting that the job %s ended successfully',
                                     updatedJob)
                    else:
                        logger.warn('Batch system is reporting that the job %s failed with exit value %i',
                                    updatedJob, result)
                    self.processFinishedJob(jobID, result, wallTime=wallTime)

            else:
                # Process jobs that have gone awry

                #In the case that there is nothing happening
                #(no updated jobs to gather for 10 seconds)
                #check if there are any jobs that have run too long
                #(see self.reissueOverLongJobs) or which
                #have gone missing from the batch system (see self.reissueMissingJobs)
                if (time.time() - timeSinceJobsLastRescued >=
                    self.config.rescueJobsFrequency): #We only
                    #rescue jobs every N seconds, and when we have
                    #apparently exhausted the current jobGraph supply
                    self.reissueOverLongJobs()
                    logger.info("Reissued any over long jobs")

                    hasNoMissingJobs = self.reissueMissingJobs()
                    if hasNoMissingJobs:
                        timeSinceJobsLastRescued = time.time()
                    else:
                        timeSinceJobsLastRescued += 60 #This means we'll try again
                        #in a minute, providing things are quiet
                    logger.info("Rescued any (long) missing jobs")

            # Check on the associated threads and exit if a failure is detected
            self.statsAndLogging.check()
            self.serviceManager.check()
            # the cluster scaler object will only be instantiated if autoscaling is enabled
            if self.clusterScaler is not None:
                self.clusterScaler.check()

            # The exit criterion
            if len(self.toilState.updatedJobs) == 0 and self.getNumberOfJobsIssued() == 0 and self.serviceManager.jobsIssuedToServiceManager == 0:
                logger.info("No jobs left to run so exiting.")
                break

            # Check for deadlocks
            self.checkForDeadlocks()

        logger.info("Finished the main loop")

        # Consistency check the toil state
        assert self.toilState.updatedJobs == set()
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
        # If there are no updated jobs and at least some jobs issued
        if len(self.toilState.updatedJobs) == 0 and self.getNumberOfJobsIssued() > 0:

            # If all scheduled jobs are services
            assert self.serviceJobsIssued + self.preemptableServiceJobsIssued <= self.getNumberOfJobsIssued()
            if self.serviceJobsIssued + self.preemptableServiceJobsIssued == self.getNumberOfJobsIssued():

                # Sanity check that all issued jobs are actually services
                for jobNode in self.jobBatchSystemIDToIssuedJob.values():
                    assert jobNode.jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob

                # An active service job is one that is not in the process of terminating
                activeServiceJobs = filter(lambda x : self.serviceManager.isActive(x), self.jobBatchSystemIDToIssuedJob.values())

                # If all the service jobs are active then we have a potential deadlock
                if len(activeServiceJobs) == len(self.jobBatchSystemIDToIssuedJob):
                    # We wait self.config.deadlockWait seconds before declaring the system deadlocked
                    if self.potentialDeadlockedJobs != activeServiceJobs:
                        self.potentialDeadlockedJobs = activeServiceJobs
                        self.potentialDeadlockTime = time.time()
                    elif time.time() - self.potentialDeadlockTime >= self.config.deadlockWait:
                        raise DeadlockException("The system is service deadlocked - all issued jobs %s are active services" % self.getNumberOfJobsIssued())


    def issueJob(self, jobNode):
        """
        Add a job to the queue of jobs
        """
        if jobNode.preemptable:
            self.preemptableJobsIssued += 1
        jobNode.command = ' '.join((resolveEntryPoint('_toil_worker'),
                                    self.jobStoreLocator, jobNode.jobStoreID))
        jobBatchSystemID = self.batchSystem.issueBatchJob(jobNode)
        self.jobBatchSystemIDToIssuedJob[jobBatchSystemID] = jobNode
        logger.debug("Issued job with job store ID: %s and job batch system ID: "
                     "%s and cores: %.2f, disk: %.2f, and memory: %.2f",
                     jobNode.jobStoreID, str(jobBatchSystemID), jobNode.cores,
                     jobNode.disk, jobNode.memory)

    def issueJobs(self, jobs):
        """
        Add a list of jobs, each represented as a jobNode object
        """
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
        """
        Issues any queuing service jobs up to the limit of the maximum allowed.
        """
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
        #assert self.jobsIssued >= 0 and self._preemptableJobsIssued >= 0
        if preemptable is None:
            return len(self.jobBatchSystemIDToIssuedJob)
        elif preemptable:
            return self.preemptableJobsIssued
        else:
            assert len(self.jobBatchSystemIDToIssuedJob) >= self.preemptableJobsIssued
            return len(self.jobBatchSystemIDToIssuedJob) - self.preemptableJobsIssued

    def getNumberAndAvgRuntimeOfCurrentlyRunningJobs(self):
        """
        Returns a tuple (x, y) where x is number of currently running jobs and y
        is the average number of seconds (as a float)
        the jobs have been running for.
        """
        runningJobs = self.batchSystem.getRunningBatchJobIDs()
        return len(runningJobs), 0 if len(runningJobs) == 0 else float(sum(runningJobs.values()))/len(runningJobs)

    def getJobStoreID(self, jobBatchSystemID):
        """
        Gets the job file associated the a given id
        """
        return self.jobBatchSystemIDToIssuedJob[jobBatchSystemID].jobStoreID

    def removeJob(self, jobBatchSystemID):
        """
        Removes a job from the system.
        """
        assert jobBatchSystemID in self.jobBatchSystemIDToIssuedJob
        jobNode = self.jobBatchSystemIDToIssuedJob.pop(jobBatchSystemID)
        if jobNode.preemptable:
            assert self.preemptableJobsIssued > 0
            self.preemptableJobsIssued -= 1

        # If service job
        if jobNode.jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob:
            # Decrement the number of services
            if jobNode.preemptable:
                self.preemptableServiceJobsIssued -= 1
            else:
                self.serviceJobsIssued -= 1

        return jobNode

    def getJobIDs(self):
        """
        Gets the set of jobs currently issued.
        """
        return self.jobBatchSystemIDToIssuedJob.keys()

    def killJobs(self, jobsToKill):
        """
        Kills the given set of jobs and then sends them for processing
        """
        if len(jobsToKill) > 0:
            self.batchSystem.killBatchJobs(jobsToKill)
            for jobBatchSystemID in jobsToKill:
                self.processFinishedJob(jobBatchSystemID, 1)

    #Following functions handle error cases for when jobs have gone awry with the batch system.

    def reissueOverLongJobs(self):
        """
        Check each issued job - if it is running for longer than desirable
        issue a kill instruction.
        Wait for the job to die then we pass the job to processFinishedJob.
        """
        maxJobDuration = self.config.maxJobDuration
        jobsToKill = []
        if maxJobDuration < 10000000:  # We won't bother doing anything if the rescue
            # time is more than 16 weeks.
            runningJobs = self.batchSystem.getRunningBatchJobIDs()
            for jobBatchSystemID in runningJobs.keys():
                if runningJobs[jobBatchSystemID] > maxJobDuration:
                    logger.warn("The job: %s has been running for: %s seconds, more than the "
                                "max job duration: %s, we'll kill it",
                                str(self.getJobStoreID(jobBatchSystemID)),
                                str(runningJobs[jobBatchSystemID]),
                                str(maxJobDuration))
                    jobsToKill.append(jobBatchSystemID)
            self.killJobs(jobsToKill)

    def reissueMissingJobs(self, killAfterNTimesMissing=3):
        """
        Check all the current job ids are in the list of currently running batch system jobs.
        If a job is missing, we mark it as so, if it is missing for a number of runs of
        this function (say 10).. then we try deleting the job (though its probably lost), we wait
        then we pass the job to processFinishedJob.
        """
        runningJobs = set(self.batchSystem.getIssuedBatchJobIDs())
        jobBatchSystemIDsSet = set(self.getJobIDs())
        #Clean up the reissueMissingJobs_missingHash hash, getting rid of jobs that have turned up
        missingJobIDsSet = set(self.reissueMissingJobs_missingHash.keys())
        for jobBatchSystemID in missingJobIDsSet.difference(jobBatchSystemIDsSet):
            self.reissueMissingJobs_missingHash.pop(jobBatchSystemID)
            logger.warn("Batch system id: %s is no longer missing", str(jobBatchSystemID))
        assert runningJobs.issubset(jobBatchSystemIDsSet) #Assert checks we have
        #no unexpected jobs running
        jobsToKill = []
        for jobBatchSystemID in set(jobBatchSystemIDsSet.difference(runningJobs)):
            jobStoreID = self.getJobStoreID(jobBatchSystemID)
            if self.reissueMissingJobs_missingHash.has_key(jobBatchSystemID):
                self.reissueMissingJobs_missingHash[jobBatchSystemID] += 1
            else:
                self.reissueMissingJobs_missingHash[jobBatchSystemID] = 1
            timesMissing = self.reissueMissingJobs_missingHash[jobBatchSystemID]
            logger.warn("Job store ID %s with batch system id %s is missing for the %i time",
                        jobStoreID, str(jobBatchSystemID), timesMissing)
            if timesMissing == killAfterNTimesMissing:
                self.reissueMissingJobs_missingHash.pop(jobBatchSystemID)
                jobsToKill.append(jobBatchSystemID)
        self.killJobs(jobsToKill)
        return len( self.reissueMissingJobs_missingHash ) == 0 #We use this to inform
        #if there are missing jobs

    def processFinishedJob(self, batchSystemID, resultStatus, wallTime=None):
        """
        Function reads a processed jobGraph file and updates it state.
        """
        def processRemovedJob(issuedJob):
            if resultStatus != 0:
                logger.warn("Despite the batch system claiming failure the "
                            "job %s seems to have finished and been removed", issuedJob)
            self._updatePredecessorStatus(issuedJob.jobStoreID)
        jobNode = self.removeJob(batchSystemID)
        jobStoreID = jobNode.jobStoreID
        if wallTime is not None and self.clusterScaler is not None:
            self.clusterScaler.addCompletedJob(jobNode, wallTime)
        if self.jobStore.exists(jobStoreID):
            logger.debug("Job %s continues to exist (i.e. has more to do)", jobNode)
            try:
                jobGraph = self.jobStore.load(jobStoreID)
            except NoSuchJobException:
                # Avoid importing AWSJobStore as the corresponding extra might be missing
                if self.jobStore.__class__.__name__ == 'AWSJobStore':
                    # We have a ghost job - the job has been deleted but a stale read from
                    # SDB gave us a false positive when we checked for its existence.
                    # Process the job from here as any other job removed from the job store.
                    # This is a temporary work around until https://github.com/BD2KGenomics/toil/issues/1091
                    # is completed
                    logger.warn('Got a stale read from SDB for job %s', jobNode)
                    processRemovedJob(jobNode)
                    return
                else:
                    raise
            if jobGraph.logJobStoreFileID is not None:
                with jobGraph.getLogFileHandle( self.jobStore ) as logFileStream:
                    # more memory efficient than read().striplines() while leaving off the
                    # trailing \n left when using readlines()
                    # http://stackoverflow.com/a/15233739
                    messages = [line.rstrip('\n') for line in logFileStream]
                    logFormat = '\n%s    ' % jobStoreID
                    logger.warn('The job seems to have left a log file, indicating failure: %s\n%s',
                                jobGraph, logFormat.join(messages))
                    StatsAndLogging.writeLogFiles(jobGraph.chainedJobs, messages, self.config)
            if resultStatus != 0:
                # If the batch system returned a non-zero exit code then the worker
                # is assumed not to have captured the failure of the job, so we
                # reduce the retry count here.
                if jobGraph.logJobStoreFileID is None:
                    logger.warn("No log file is present, despite job failing: %s", jobNode)
                jobGraph.setupJobAfterFailure(self.config)
                self.jobStore.update(jobGraph)
            elif jobStoreID in self.toilState.hasFailedSuccessors:
                # If the job has completed okay, we can remove it from the list of jobs with failed successors
                self.toilState.hasFailedSuccessors.remove(jobStoreID)

            self.toilState.updatedJobs.add((jobGraph, resultStatus)) #Now we know the
            #jobGraph is done we can add it to the list of updated jobGraph files
            logger.debug("Added job: %s to active jobs", jobGraph)
        else:  #The jobGraph is done
            processRemovedJob(jobNode)

    @staticmethod
    def getSuccessors(jobGraph, alreadySeenSuccessors, jobStore):
        """
        Gets successors of the given job by walking the job graph recursively.
        Any successor in alreadySeenSuccessors is ignored and not traversed.
        Returns the set of found successors. This set is added to alreadySeenSuccessors.
        """
        successors = set()

        def successorRecursion(jobGraph):
            # For lists of successors
            for successorList in jobGraph.stack:

                # For each successor in list of successors
                for successorJobNode in successorList:

                    # Id of the successor
                    successorJobStoreID = successorJobNode.jobStoreID

                    # If successor not already visited
                    if successorJobStoreID not in alreadySeenSuccessors:

                        # Add to set of successors
                        successors.add(successorJobStoreID)
                        alreadySeenSuccessors.add(successorJobStoreID)

                        # Recurse if job exists
                        # (job may not exist if already completed)
                        if jobStore.exists(successorJobStoreID):
                            successorRecursion(jobStore.load(successorJobStoreID))

        successorRecursion(jobGraph) # Recurse from jobGraph

        return successors

    def processTotallyFailedJob(self, jobGraph):
        """
        Processes a totally failed job.
        """
        # Mark job as a totally failed job
        self.toilState.totalFailedJobs.add(JobNode.fromJobGraph(jobGraph))

        if jobGraph.jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob: # Is
            # a service job
            logger.debug("Service job is being processed as a totally failed job: %s", jobGraph)

            predecesssorJobGraph = self.toilState.serviceJobStoreIDToPredecessorJob[jobGraph.jobStoreID]

            # This removes the service job as a service of the predecessor
            # and potentially makes the predecessor active
            self._updatePredecessorStatus(jobGraph.jobStoreID)

            # Remove the start flag, if it still exists. This indicates
            # to the service manager that the job has "started", this prevents
            # the service manager from deadlocking while waiting
            self.jobStore.deleteFile(jobGraph.startJobStoreID)

            # Signal to any other services in the group that they should
            # terminate. We do this to prevent other services in the set
            # of services from deadlocking waiting for this service to start properly
            if predecesssorJobGraph.jobStoreID in self.toilState.servicesIssued:
                self.serviceManager.killServices(self.toilState.servicesIssued[predecesssorJobGraph.jobStoreID], error=True)
                logger.debug("Job: %s is instructing all the services of its parent job to quit", jobGraph)

            self.toilState.hasFailedSuccessors.add(predecesssorJobGraph.jobStoreID) # This ensures that the
            # job will not attempt to run any of it's successors on the stack
        else:
            # Is a non-service job
            assert jobGraph.jobStoreID not in self.toilState.servicesIssued

            # Traverse failed job's successor graph and get the jobStoreID of new successors.
            # Any successor already in toilState.failedSuccessors will not be traversed
            # All successors traversed will be added to toilState.failedSuccessors and returned
            # as a set (unseenSuccessors).
            unseenSuccessors = self.getSuccessors(jobGraph, self.toilState.failedSuccessors,
                                                  self.jobStore)
            logger.debug("Found new failed successors: %s of job: %s", " ".join(
                         unseenSuccessors), jobGraph)

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
                            self.toilState.updatedJobs.add((predecessorJob, 0))

                            # Remove the predecessor job from the set of jobs with successors.
                            self.toilState.successorCounts.pop(predecessorJob.jobStoreID)

            # If the job has predecessor(s)
            if jobGraph.jobStoreID in self.toilState.successorJobStoreIDToPredecessorJobs:

                # For each predecessor of the job
                for predecessorJobGraph in self.toilState.successorJobStoreIDToPredecessorJobs[jobGraph.jobStoreID]:

                    # Mark the predecessor as failed
                    self.toilState.hasFailedSuccessors.add(predecessorJobGraph.jobStoreID)
                    logger.debug("Totally failed job: %s is marking direct predecessor: %s "
                                 "as having failed jobs", jobGraph, predecessorJobGraph)

                self._updatePredecessorStatus(jobGraph.jobStoreID)

    def _updatePredecessorStatus(self, jobStoreID):
        """
        Update status of predecessors for finished successor job.
        """
        if jobStoreID in self.toilState.serviceJobStoreIDToPredecessorJob:
            # Is a service job
            predecessorJob = self.toilState.serviceJobStoreIDToPredecessorJob.pop(jobStoreID)
            self.toilState.servicesIssued[predecessorJob.jobStoreID].pop(jobStoreID)
            if len(self.toilState.servicesIssued[predecessorJob.jobStoreID]) == 0: # Predecessor job has
                # all its services terminated
                self.toilState.servicesIssued.pop(predecessorJob.jobStoreID) # The job has no running services
                self.toilState.updatedJobs.add((predecessorJob, 0)) # Now we know
                # the job is done we can add it to the list of updated job files
                logger.debug("Job %s services have completed or totally failed, adding to updated jobs", predecessorJob)

        elif jobStoreID not in self.toilState.successorJobStoreIDToPredecessorJobs:
            #We have reach the root job
            assert len(self.toilState.updatedJobs) == 0
            assert len(self.toilState.successorJobStoreIDToPredecessorJobs) == 0
            assert len(self.toilState.successorCounts) == 0
            logger.debug("Reached root job %s so no predecessors to clean up" % jobStoreID)

        else:
            # Is a non-root, non-service job
            logger.debug("Cleaning the predecessors of %s" % jobStoreID)

            # For each predecessor
            for predecessorJob in self.toilState.successorJobStoreIDToPredecessorJobs.pop(jobStoreID):

                # Reduce the predecessor's number of successors by one to indicate the
                # completion of the jobStoreID job
                self.toilState.successorCounts[predecessorJob.jobStoreID] -= 1

                # If the predecessor job is done and all the successors are complete
                if self.toilState.successorCounts[predecessorJob.jobStoreID] == 0:

                    # Remove it from the set of jobs with active successors
                    self.toilState.successorCounts.pop(predecessorJob.jobStoreID)

                    # Pop stack at this point, as we can get rid of its successors
                    predecessorJob.stack.pop()

                    # Now we know the job is done we can add it to the list of updated job files
                    assert predecessorJob not in self.toilState.updatedJobs
                    self.toilState.updatedJobs.add((predecessorJob, 0))

                    logger.debug('Job %s has all its non-service successors completed or totally '
                                 'failed', predecessorJob)