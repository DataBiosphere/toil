# This should be deleted. In the autoscaling branch B extracted this from JobBatcher in leader.py
# but while reviving that branch I decided to undo the extraction. For one, JobBatcher and
# leader.py in general had changed too much (e.g. services) and two, the this would derail the
# caching branch as well. The main semantic changes to JobDispatcher were wall time and the
# IssuedJob stuff.

# Copyright (C) 2015 UCSC Computational Genomics Lab
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

from __future__ import absolute_import
import logging
from toil import resolveEntryPoint
from toil.lib.bioio import logStream
from collections import namedtuple
import time
from toil.toilState import ToilState

logger = logging.getLogger( __name__ )

# Represents a job and its requirements as issued to the batch system
IssuedJob = namedtuple("IssuedJob", "jobStoreID memory cores disk preemptable")

class JobDispatcher(object):
    """
    Class manages dispatching jobs to the batch system.
    """
    def __init__(self, config, batchSystem, jobStore, rootJobWrapper):
        """
        """
        self.config = config
        self.jobStore = jobStore
        self.batchSystem = batchSystem
        self.clusterScaler = None # This an optional parameter which may be set
        # if doing autoscaling
        self.toilState = ToilState(jobStore, rootJobWrapper)
        self.jobBatchSystemIDToIssuedJob = {} # Map of batch system IDs to IsseudJob tuples
        self.reissueMissingJobs_missingHash = {} #Hash to store number of observed misses
        
    def dispatch(self):
        """
        Starts a loop with the batch system to run the Toil workflow
        """

        # Kill any jobs on the batch system queue from the last time.
        assert len(self.batchSystem.getIssuedBatchJobIDs()) == 0 #Batch system must start with no active jobs!
        logger.info("Checked batch system has no running jobs and no updated jobs")
    
        logger.info("Found %s jobs to start and %i jobs with successors to run",
                    len(self.toilState.updatedJobs), len(self.toilState.successorCounts))
    
        # The main loop in which jobs are scheduled/processed
        
        # Sets up the timing of the jobWrapper rescuing method
        timeSinceJobsLastRescued = time.time()
        # Number of jobs that can not be completed successful after exhausting retries
        totalFailedJobs = 0
        logger.info("Starting the main loop")
        while True:
            # Process jobs that are ready to be scheduled/have successors to schedule
            
            if len(self.toilState.updatedJobs) > 0:
                logger.debug("Built the jobs list, currently have %i jobs to update and %i jobs issued",
                             len(self.toilState.updatedJobs), self.getNumberOfJobsIssued())
    
                for jobWrapper, resultStatus in self.toilState.updatedJobs:
                    #If the jobWrapper has a command it must be run before any successors
                    #Similarly, if the job previously failed we rerun it, even if it doesn't have a command to
                    #run, to eliminate any parts of the stack now completed.
                    if jobWrapper.command != None or resultStatus != 0:
                        if jobWrapper.remainingRetryCount > 0:
                            iJ = IssuedJob(jobWrapper.jobStoreID, jobWrapper.memory, 
                                           jobWrapper.cores, jobWrapper.disk, jobWrapper.preemptable)
                            self.issueJob(iJ)
                        else:
                            totalFailedJobs += 1
                            logger.warn("Job: %s is completely failed", jobWrapper.jobStoreID)
    
                    #There exist successors to run
                    elif len(jobWrapper.stack) > 0:
                        assert len(jobWrapper.stack[-1]) > 0
                        logger.debug("Job: %s has %i successors to schedule",
                                     jobWrapper.jobStoreID, len(jobWrapper.stack[-1]))
                        #Record the number of successors that must be completed before
                        #the jobWrapper can be considered again
                        assert jobWrapper not in self.toilState.successorCounts
                        self.toilState.successorCounts[jobWrapper] = len(jobWrapper.stack[-1])
                        #List of successors to schedule
                        successors = []
                        #For each successor schedule if all predecessors have been completed
                        for successorJobStoreID, memory, cores, disk, preemptable, predecessorID in jobWrapper.stack.pop():
                            #Build map from successor to predecessors.
                            if successorJobStoreID not in self.toilState.successorJobStoreIDToPredecessorJobs:
                                self.toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID] = []
                            self.toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID].append(jobWrapper)
                            #Case that the jobWrapper has multiple predecessors
                            if predecessorID != None:
                                #Load the wrapped jobWrapper
                                job2 = self.jobStore.load(successorJobStoreID)
                                #Remove the predecessor from the list of predecessors
                                job2.predecessorsFinished.add(predecessorID)
                                #Checkpoint
                                self.jobStore.update(job2)
                                #If the jobs predecessors have all not all completed then
                                #ignore the jobWrapper
                                assert len(job2.predecessorsFinished) >= 1
                                assert len(job2.predecessorsFinished) <= job2.predecessorNumber
                                if len(job2.predecessorsFinished) < job2.predecessorNumber:
                                    continue
                            successors.append(IssuedJob(successorJobStoreID, memory, cores, disk, preemptable))
                        map(self.issueJob, successors)
    
                    # There are no remaining tasks to schedule within the jobWrapper, but
                    # we schedule it anyway to allow it to be deleted.
    
                    # TODO: An alternative would be simple delete it here and add it to the
                    # list of jobs to process, or (better) to create an asynchronous
                    # process that deletes jobs and then feeds them back into the set
                    # of jobs to be processed
                    else:
                        if jobWrapper.remainingRetryCount > 0:
                            iJ = IssuedJob(jobWrapper.jobStoreID, 
                                           self.config.defaultMemory, self.config.defaultCores,
                                           self.config.defaultDisk, True)
                            self.issueJob(iJ) #We allow this cleanup to potentially occur on a preemptable instance
                            logger.debug("Job: %s is empty, we are scheduling to clean it up", jobWrapper.jobStoreID)
                        else:
                            totalFailedJobs += 1
                            logger.warn("Job: %s is empty but completely failed - something is very wrong", jobWrapper.jobStoreID)
    
                self.toilState.updatedJobs = set() #We've considered them all, so reset
    
            # The exit criterion
    
            if self.getNumberOfJobsIssued() == 0:
                logger.info("Only failed jobs and their dependents (%i total) are remaining, so exiting.", totalFailedJobs)
                return totalFailedJobs
    
            # Gather any new, updated jobs from the batch system
        
            if self.processAnyUpdatedJob(10) == None:  # Asks the batch system to 
                # process a job that has been completed,
                # In the case that there is nothing happening
                # (no updated jobWrapper to gather for 10 seconds)
                # check if their are any jobs that have run too long
                # (see JobBatcher.reissueOverLongJobs) or which
                # have gone missing from the batch system (see JobBatcher.reissueMissingJobs)
                if (time.time() - timeSinceJobsLastRescued >=
                    self.config.rescueJobsFrequency): #We only
                    #rescue jobs every N seconds, and when we have
                    #apparently exhausted the current jobWrapper supply
                    self.reissueOverLongJobs()
                    logger.info("Reissued any over long jobs")
    
                    hasNoMissingJobs = self.reissueMissingJobs()
                    if hasNoMissingJobs:
                        timeSinceJobsLastRescued = time.time()
                    else:
                        timeSinceJobsLastRescued += 60 #This means we'll try again
                        #in a minute, providing things are quiet
                    logger.info("Rescued any (long) missing jobs")


    def issueJob(self, issuedJob):
        """
        Add a job to the queue of jobs. 
        """
        jobCommand = ' '.join((resolveEntryPoint('_toil_worker'), 
                               self.config.jobStore, issuedJob.jobStoreID))
        jobBatchSystemID = self.batchSystem.issueBatchJob(jobCommand, issuedJob.memory, 
                                issuedJob.cores, issuedJob.disk, issuedJob.preemptable)
        self.jobBatchSystemIDToIssuedJob[jobBatchSystemID] = issuedJob
        logger.debug("Issued job with job store ID: %s and job batch system ID: "
                     "%s and cores: %i, disk: %i, and memory: %i",
                     issuedJob.jobStoreID, str(jobBatchSystemID), issuedJob.cores, 
                     issuedJob.disk, issuedJob.memory)
        
    def processAnyUpdatedJob(self, block=10):
        """
        Get an updated job from the batch system, blocking for up to block 
        seconds while waiting for the job. 
        """
        updatedJob = self.batchSystem.getUpdatedBatchJob(block)
        if updatedJob is not None:
            jobBatchSystemID, exitValue, wallTime = updatedJob
            if self.clusterScaler is not None:
                issuedJob = self.jobBatchSystemIDToIssuedJob[jobBatchSystemID]
                self.clusterScaler.addCompletedJob(issuedJob, wallTime)
            if self.hasJob(jobBatchSystemID):
                if exitValue == 0:
                    logger.debug("Batch system is reporting that the jobWrapper with "
                                 "batch system ID: %s and jobWrapper store ID: %s ended successfully",
                                 jobBatchSystemID, self.getJobStoreID(jobBatchSystemID))
                else:
                    logger.warn("Batch system is reporting that the jobWrapper with "
                                "batch system ID: %s and jobWrapper store ID: %s failed with exit value %i",
                                jobBatchSystemID, self.getJobStoreID(jobBatchSystemID), exitValue)
                self.processFinishedJob(jobBatchSystemID, exitValue)
            else:
                logger.warn("A result seems to already have been processed "
                            "for jobWrapper with batch system ID: %i", jobBatchSystemID)
        return updatedJob

    def getNumberOfJobsIssued(self):
        """
        Gets number of jobs that have been added by issueJob(s) and not
        removed by removeJobID
        """
        return len(self.jobBatchSystemIDToIssuedJob)

    def getJobStoreID(self, jobBatchSystemID):
        """
        Gets the jobStoreID associated the a given id
        """
        return self.jobBatchSystemIDToIssuedJob[jobBatchSystemID].jobStoreID

    def hasJob(self, jobBatchSystemID):
        """
        Returns true if the jobBatchSystemID is in the list of jobs.
        """
        return self.jobBatchSystemIDToIssuedJob.has_key(jobBatchSystemID)

    def getIssuedJobStoreIDs(self):
        """
        Gets the set of jobStoreIDs of jobs currently issued.
        """
        return self.jobBatchSystemIDToIssuedJob.keys()

    def removeJob(self, jobBatchSystemID):
        """
        Removes a job from the jobBatcher.
        """
        issuedJob = self.jobBatchSystemIDToIssuedJob.pop(jobBatchSystemID)
        return issuedJob

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
        jobBatchSystemIDsSet = set(self.getIssuedJobStoreIDs())
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
                self.reissueMissingJobs_missingHash[jobBatchSystemID] = \
                self.reissueMissingJobs_missingHash[jobBatchSystemID]+1
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

    def processFinishedJob(self, jobBatchSystemID, resultStatus):
        """
        Function reads a processed jobWrapper file and updates it state.
        """
        jobStoreID = self.removeJob(jobBatchSystemID).jobStoreID
        if self.jobStore.exists(jobStoreID):
            jobWrapper = self.jobStore.load(jobStoreID)
            if jobWrapper.logJobStoreFileID is not None:
                logger.warn("The jobWrapper seems to have left a log file, indicating failure: %s", jobStoreID)
                with jobWrapper.getLogFileHandle( self.jobStore ) as logFileStream:
                    logStream( logFileStream, jobStoreID, logger.warn )
            if resultStatus != 0:
                if jobWrapper.logJobStoreFileID is None:
                    logger.warn("No log file is present, despite jobWrapper failing: %s", jobStoreID)
                jobWrapper.setupJobAfterFailure(self.config)
            self.toilState.updatedJobs.add((jobWrapper, resultStatus)) #Now we know the
            #jobWrapper is done we can add it to the list of updated jobWrapper files
            logger.debug("Added jobWrapper: %s to active jobs", jobStoreID)
        else:  #The jobWrapper is done
            if resultStatus != 0:
                logger.warn("Despite the batch system claiming failure the "
                            "jobWrapper %s seems to have finished and been removed", jobStoreID)
            self._updatePredecessorStatus(jobStoreID)

    def _updatePredecessorStatus(self, jobStoreID):
        """
        Update status of a predecessor for finished successor job.
        """
        if jobStoreID not in self.toilState.successorJobStoreIDToPredecessorJobs:
            #We have reach the root job
            assert len(self.toilState.updatedJobs) == 0
            assert len(self.toilState.successorJobStoreIDToPredecessorJobs) == 0
            assert len(self.toilState.successorCounts) == 0
            return
        for predecessorJob in self.toilState.successorJobStoreIDToPredecessorJobs.pop(jobStoreID):
            self.toilState.successorCounts[predecessorJob] -= 1
            assert self.toilState.successorCounts[predecessorJob] >= 0
            if self.toilState.successorCounts[predecessorJob] == 0: #Job is done
                self.toilState.successorCounts.pop(predecessorJob)
                logger.debug("Job %s has all its successors run successfully", \
                             predecessorJob.jobStoreID)
                assert predecessorJob not in self.toilState.updatedJobs
                self.toilState.updatedJobs.add((predecessorJob, 0)) #Now we know
                #the job is done we can add it to the list of updated job files
