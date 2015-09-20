#!/usr/bin/env python

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

"""
The leader script (of the leader/worker pair) for running jobs.
"""
from __future__ import absolute_import
import logging
import sys
import os.path
import time
import xml.etree.cElementTree as ET

from toil import Process, Queue
from toil.lib.bioio import getTotalCpuTime, logStream
from toil.common import toilPackageDirPath

logger = logging.getLogger( __name__ )

####################################################
##Stats/logging aggregation
####################################################

def statsAndLoggingAggregatorProcess(jobStore, stop):
    """
    The following function is used for collating stats/reporting log messages from the workers.
    Works inside of a separate process, collates as long as the stop flag is not True.
    """
    #Overall timing
    startTime = time.time()
    startClock = getTotalCpuTime()

    #Start off the stats file
    with jobStore.writeSharedFileStream("statsAndLogging.xml") as fileHandle:
        fileHandle.write('<?xml version="1.0" ?><stats>')
       
        #Call back function
        def statsAndLoggingCallBackFn(fileHandle2):
            node = ET.parse(fileHandle2).getroot()
            nodesNamed = node.find("messages").findall
            for message in nodesNamed("message"):
                logger.warn("Got message from job at time: %s : %s",
                                    time.strftime("%m-%d-%Y %H:%M:%S"), message.text)
            for log in nodesNamed("log"):
                logger.info("%s:     %s" %
                                    tuple(log.text.split("!",1)))# the jobID is separated from log by "!"
            ET.ElementTree(node).write(fileHandle)
        
        #The main loop
        timeSinceOutFileLastFlushed = time.time()
        while True:
            if not stop.empty(): #This is a indirect way of getting a message to
                #the process to exit
                jobStore.readStatsAndLogging(statsAndLoggingCallBackFn)
                break
            if jobStore.readStatsAndLogging(statsAndLoggingCallBackFn) == 0:
                time.sleep(0.5) #Avoid cycling too fast
            if time.time() - timeSinceOutFileLastFlushed > 60: #Flush the
                #results file every minute
                fileHandle.flush()
                timeSinceOutFileLastFlushed = time.time()
        
        #Finish the stats file
        fileHandle.write("<total_time time='%s' clock='%s'/></stats>" % \
                         (str(time.time() - startTime), str(getTotalCpuTime() - startClock)))

####################################################
##Following encapsulates interactions with the batch system class.
####################################################

class JobBatcher:
    """
    Class works with jobBatcherWorker to submit jobs to the batch system.
    """
    def __init__(self, config, batchSystem, jobStore, toilState):
        self.config = config
        self.jobStore = jobStore
        self.jobStoreString = config.jobStore
        self.toilState = toilState
        self.jobBatchSystemIDToJobStoreIDHash = {}
        self.batchSystem = batchSystem
        self.jobsIssued = 0
        self.workerPath = os.path.join(toilPackageDirPath(), "worker.py")
        self.reissueMissingJobs_missingHash = {} #Hash to store number of observed misses

    def issueJob(self, jobStoreID, memory, cores, disk):
        """
        Add a job to the queue of jobs
        """
        self.jobsIssued += 1
        jobCommand = "%s -E %s %s %s" % (sys.executable, self.workerPath, self.jobStoreString, jobStoreID)
        jobBatchSystemID = self.batchSystem.issueBatchJob(jobCommand, memory, cores, disk)
        self.jobBatchSystemIDToJobStoreIDHash[jobBatchSystemID] = jobStoreID
        logger.debug("Issued job with job store ID: %s and job batch system ID: "
                     "%s and cores: %i, disk: %i, and memory: %i",
                     jobStoreID, str(jobBatchSystemID), cores, disk, memory)

    def issueJobs(self, jobs):
        """
        Add a list of jobs, each represented as a tuple of
        (jobStoreID, memory, cores, disk).
        """
        for jobStoreID, memory, cores, disk in jobs:
            self.issueJob(jobStoreID, memory, cores, disk)

    def getNumberOfJobsIssued(self):
        """
        Gets number of jobs that have been added by issueJob(s) and not
        removed by removeJobID
        """
        assert self.jobsIssued >= 0
        return self.jobsIssued

    def getJob(self, jobBatchSystemID):
        """
        Gets the job file associated the a given id
        """
        return self.jobBatchSystemIDToJobStoreIDHash[jobBatchSystemID]

    def hasJob(self, jobBatchSystemID):
        """
        Returns true if the jobBatchSystemID is in the list of jobs.
        """
        return self.jobBatchSystemIDToJobStoreIDHash.has_key(jobBatchSystemID)

    def getJobIDs(self):
        """
        Gets the set of jobs currently issued.
        """
        return self.jobBatchSystemIDToJobStoreIDHash.keys()

    def removeJobID(self, jobBatchSystemID):
        """
        Removes a job from the jobBatcher.
        """
        assert jobBatchSystemID in self.jobBatchSystemIDToJobStoreIDHash
        self.jobsIssued -= 1
        jobStoreID = self.jobBatchSystemIDToJobStoreIDHash.pop(jobBatchSystemID)
        return jobStoreID
    
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
                                str(self.getJob(jobBatchSystemID)),
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
            jobStoreID = self.getJob(jobBatchSystemID)
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
        Function reads a processed job file and updates it state.
        """    
        jobStoreID = self.removeJobID(jobBatchSystemID)
        if self.jobStore.exists(jobStoreID):
            job = self.jobStore.load(jobStoreID)
            if job.logJobStoreFileID is not None:
                logger.warn("The job seems to have left a log file, indicating failure: %s", jobStoreID)
                with job.getLogFileHandle( self.jobStore ) as logFileStream:
                    logStream( logFileStream, jobStoreID, logger.warn )
            assert job not in self.toilState.updatedJobs
            if resultStatus != 0:
                if job.logJobStoreFileID is None:
                    logger.warn("No log file is present, despite job failing: %s", jobStoreID)
                job.setupJobAfterFailure(self.config)
            self.toilState.updatedJobs.add(job) #Now we know the
            #job is done we can add it to the list of updated job files
            logger.debug("Added job: %s to active jobs", jobStoreID)
        else:  #The job is done
            if resultStatus != 0:
                logger.warn("Despite the batch system claiming failure the "
                            "job %s seems to have finished and been removed", jobStoreID)
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
                self.toilState.updatedJobs.add(predecessorJob) #Now we know
                #the job is done we can add it to the list of updated job files

##########################################
#Class to represent the state of the toil in memory. Loads this
#representation from the toil, in the process cleaning up
#the state of the jobs in the jobtree.
##########################################    
  
class ToilState( object ):
    """
    Represents a snapshot of the jobs in the jobStore.
    """
    def __init__( self, jobStore, rootJob ):
        # This is a hash of jobs, referenced by jobStoreID, to their predecessor jobs.
        self.successorJobStoreIDToPredecessorJobs = { }
        # Hash of jobs to counts of numbers of successors issued.
        # There are no entries for jobs
        # without successors in this map. 
        self.successorCounts = { }
        # Jobs that are ready to be processed
        self.updatedJobs = set( )
        ##Algorithm to build this information
        self._buildToilState(rootJob, jobStore)

    def _buildToilState(self, job, jobStore):
        """
        Traverses tree of jobs from the root job (rootJob) building the
        ToilState class.
        """
        if job.command != None or len(job.stack) == 0: #If the job has a command
            #or is ready to be deleted it is ready to be processed
            self.updatedJobs.add(job)
        else: #There exist successors
            self.successorCounts[job] = len(job.stack[-1])
            for successorJobStoreTuple in job.stack[-1]:
                successorJobStoreID = successorJobStoreTuple[0]
                if successorJobStoreID not in self.successorJobStoreIDToPredecessorJobs:
                    #Given that the successor job does not yet point back at a
                    #predecessor we have not yet considered it, so we call the function
                    #on the successor
                    self.successorJobStoreIDToPredecessorJobs[successorJobStoreID] = [job]
                    self._buildToilState(jobStore.load(successorJobStoreID), jobStore)
                else:
                    #We have already looked at the successor, so we don't recurse, 
                    #but we add back a predecessor link
                    self.successorJobStoreIDToPredecessorJobs[successorJobStoreID].append(job)

class FailedJobsException( Exception ):
    def __init__( self, jobStoreString, numberOfFailedJobs ):
        super( FailedJobsException, self ).__init__( "The job store '%s' contains %i failed jobs" % (jobStoreString, numberOfFailedJobs))
        self.jobStoreString = jobStoreString
        self.numberOfFailedJobs = numberOfFailedJobs
        
def mainLoop(config, batchSystem, jobStore, rootJob):
    """
    This is the main loop from which jobs are issued and processed.
    
    :raises: toil.leader.FailedJobsException if at the end of function their remain
    failed jobs
    """

    ##########################################
    #Get a snap shot of the current state of the jobs in the jobStore
    ##########################################

    toilState = ToilState(jobStore, rootJob)

    ##########################################
    #Load the jobBatcher class - used to track jobs submitted to the batch-system
    ##########################################

    #Kill any jobs on the batch system queue from the last time.
    assert len(batchSystem.getIssuedBatchJobIDs()) == 0 #Batch system must start with no active jobs!
    logger.info("Checked batch system has no running jobs and no updated jobs")

    jobBatcher = JobBatcher(config, batchSystem, jobStore, toilState)
    logger.info("Found %s jobs to start and %i jobs with successors to run",
                len(toilState.updatedJobs), len(toilState.successorCounts))

    ##########################################
    #Start the stats/logging aggregation process
    ##########################################

    stopStatsAndLoggingAggregatorProcess = Queue() #When this is s
    worker = Process(target=statsAndLoggingAggregatorProcess,
                     args=(jobStore, stopStatsAndLoggingAggregatorProcess))
    worker.start() 

    ##########################################
    #The main loop in which jobs are scheduled/processed
    ##########################################

    #Sets up the timing of the job rescuing method
    timeSinceJobsLastRescued = time.time()
    #Number of jobs that can not be completed successful after exhausting retries
    totalFailedJobs = 0
    logger.info("Starting the main loop")
    while True:

        ##########################################
        #Process jobs that are ready to be scheduled/have successors to schedule
        ##########################################

        if len(toilState.updatedJobs) > 0:
            logger.debug("Built the jobs list, currently have %i jobs to update and %i jobs issued",
                         len(toilState.updatedJobs), jobBatcher.getNumberOfJobsIssued())

            for job in toilState.updatedJobs:
                #If the job has a command it must be run before any successors
                if job.command != None:
                    if job.remainingRetryCount > 0:
                        jobBatcher.issueJob(job.jobStoreID, job.memory, job.cores, job.disk)
                    else:
                        totalFailedJobs += 1
                        logger.warn("Job: %s is completely failed", job.jobStoreID)

                #There exist successors to run
                elif len(job.stack) > 0:
                    assert len(job.stack[-1]) > 0
                    logger.debug("Job: %s has %i successors to schedule",
                                 job.jobStoreID, len(job.stack[-1]))
                    #Record the number of successors that must be completed before
                    #the job can be considered again
                    assert job not in toilState.successorCounts
                    toilState.successorCounts[job] = len(job.stack[-1])
                    #List of successors to schedule
                    successors = []
                    #For each successor schedule if all predecessors have been
                    #completed
                    for successorJobStoreID, memory, cores, disk, predecessorID in job.stack.pop():
                        #Build map from successor to predecessors.
                        if successorJobStoreID not in toilState.successorJobStoreIDToPredecessorJobs:
                            toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID] = []
                        toilState.successorJobStoreIDToPredecessorJobs[successorJobStoreID].append(job)
                        #Case that the job has multiple predecessors
                        if predecessorID != None:
                            #Load the wrapped job
                            job2 = jobStore.load(successorJobStoreID)
                            #Remove the predecessor from the list of predecessors
                            job2.predecessorsFinished.add(predecessorID)
                            #Checkpoint
                            jobStore.update(job2)
                            #If the jobs predecessors have all not all completed then
                            #ignore the job
                            assert len(job2.predecessorsFinished) >= 1
                            assert len(job2.predecessorsFinished) <= job2.predecessorNumber
                            if len(job2.predecessorsFinished) < job2.predecessorNumber:
                                continue
                        successors.append((successorJobStoreID, memory, cores, disk))
                    jobBatcher.issueJobs(successors)

                #There are no remaining tasks to schedule within the job, but
                #we schedule it anyway to allow it to be deleted.

                #TODO: An alternative would be simple delete it here and add it to the
                #list of jobs to process, or (better) to create an asynchronous
                #process that deletes jobs and then feeds them back into the set
                #of jobs to be processed
                else:
                    if job.remainingRetryCount > 0:
                        jobBatcher.issueJob(job.jobStoreID,
                                            config.defaultMemory,
                                            config.defaultCores,
                                            config.defaultDisk)
                        logger.debug("Job: %s is empty, we are scheduling to clean it up", job.jobStoreID)
                    else:
                        totalFailedJobs += 1
                        logger.warn("Job: %s is empty but completely failed - something is very wrong", job.jobStoreID)

            toilState.updatedJobs = set() #We've considered them all, so reset

        ##########################################
        #The exit criterion
        ##########################################

        if jobBatcher.getNumberOfJobsIssued() == 0:
            logger.info("Only failed jobs and their dependents (%i total) are remaining, so exiting.", totalFailedJobs)
            break

        ##########################################
        #Gather any new, updated job from the batch system
        ##########################################

        #Asks the batch system what jobs have been completed,
        #give
        updatedJob = batchSystem.getUpdatedBatchJob(10)
        if updatedJob != None:
            jobBatchSystemID, result = updatedJob
            if jobBatcher.hasJob(jobBatchSystemID):
                if result == 0:
                    logger.debug("Batch system is reporting that the job with "
                                 "batch system ID: %s and job store ID: %s ended successfully",
                                 jobBatchSystemID, jobBatcher.getJob(jobBatchSystemID))
                else:
                    logger.warn("Batch system is reporting that the job with "
                                "batch system ID: %s and job store ID: %s failed with exit value %i",
                                jobBatchSystemID, jobBatcher.getJob(jobBatchSystemID), result)
                jobBatcher.processFinishedJob(jobBatchSystemID, result)
            else:
                logger.warn("A result seems to already have been processed "
                            "for job with batch system ID: %i", jobBatchSystemID)
        else:
            ##########################################
            #Process jobs that have gone awry
            ##########################################

            #In the case that there is nothing happening
            #(no updated job to gather for 10 seconds)
            #check if their are any jobs that have run too long
            #(see JobBatcher.reissueOverLongJobs) or which
            #have gone missing from the batch system (see JobBatcher.reissueMissingJobs)
            if (time.time() - timeSinceJobsLastRescued >=
                config.rescueJobsFrequency): #We only
                #rescue jobs every N seconds, and when we have
                #apparently exhausted the current job supply
                jobBatcher.reissueOverLongJobs()
                logger.info("Reissued any over long jobs")

                hasNoMissingJobs = jobBatcher.reissueMissingJobs()
                if hasNoMissingJobs:
                    timeSinceJobsLastRescued = time.time()
                else:
                    timeSinceJobsLastRescued += 60 #This means we'll try again
                    #in a minute, providing things are quiet
                logger.info("Rescued any (long) missing jobs")

    logger.info("Finished the main loop")

    ##########################################
    #Finish up the stats/logging aggregation process
    ##########################################
    logger.info("Waiting for stats and logging collator process to finish")
    startTime = time.time()
    stopStatsAndLoggingAggregatorProcess.put(True)
    worker.join()
    logger.info("Stats/logging finished collating in %s seconds", time.time() - startTime)
    # in addition to cleaning on exceptions, onError should clean if there are any failed jobs
    
    if totalFailedJobs > 0:
        if config.clean == "onError" or config.clean == "always" :
            jobStore.deleteJobStore()
        raise FailedJobsException( config.jobStore, totalFailedJobs )
    
    if config.clean == "onSuccess" or config.clean == "always":
        jobStore.deleteJobStore()

