#!/usr/bin/env python

#Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE. 

"""The master component (of a master slave pattern) for a job manager used by 
sontrace programs (cactus etc) for running hierarchical trees of jobs on the 
cluster.

Takes a crash-only philosophy so that any part of the process can be failed 
and then restarted at will (see the accompanying tests).
"""

import os
import sys
import os.path 
import xml.etree.cElementTree as ET
import time
import shutil
from collections import deque
#from threading import Thread, Queue
from multiprocessing import Process, JoinableQueue

from job import Job
from sonLib.bioio import logger, getTotalCpuTime
from sonLib.bioio import logFile
from sonLib.bioio import system
from jobTree.src.bioio import workflowRootPath
from sonLib.bioio import TempFileTree

def getEnvironmentFileName(jobTreePath):
    return os.path.join(jobTreePath, "environ.pickle")

def getJobFileDirName(jobTreePath):
    return os.path.join(jobTreePath, "jobs")

def getStatsFileName(jobTreePath):
    return os.path.join(jobTreePath, "stats.xml")

def getParasolResultsFileName(jobTreePath):
    return os.path.join(jobTreePath, "results.txt")

def getConfigFileName(jobTreePath):
    return os.path.join(jobTreePath, "config.xml")

class JobBatcher:
    """Class works with jobBatcherWorker to submit jobs to the batch system.
    """
    def __init__(self, config, batchSystem):
        self.maxCpus = int(config.attrib["max_jobs"])
        self.jobTree = config.attrib["job_tree"]
        self.jobIDsToJobsHash = {}
        self.batchSystem = batchSystem
        self.jobsIssued = 0
        self.jobTreeSlavePath = os.path.join(workflowRootPath(), "bin", "jobTreeSlave")
        self.rootPath = os.path.split(workflowRootPath())[0]
        
    def issueJob(self, jobFile, memory, cpu):
        """Add a job to the queue of jobs
        """
        self.jobsIssued += 1
        if cpu > self.maxCpus:
            raise RuntimeError("Requesting more cpus than available. Requested: %s, Available: %s" % (cpu, self.maxCpus))
        jobCommand = "%s -E %s %s %s %s" % (sys.executable, self.jobTreeSlavePath, self.rootPath, self.jobTree, jobFile)
        jobID = self.batchSystem.issueJob(jobCommand, memory, cpu)
        self.jobIDsToJobsHash[jobID] = jobFile
        logger.debug("Issued the job: %s with job id: %i and cpus: %i" % (jobFile, jobID, cpu))
    
    def issueJobs(self, jobs):
        """Add a list of jobs
        """
        for jobFile, memory, cpu in jobs:
            self.issueJob(jobFile, memory, cpu)
    
    def getNumberOfJobsIssued(self):
        """Gets number of jobs that have been added by issueJob(s) and not removed by removeJobID
        """
        assert self.jobsIssued >= 0
        return self.jobsIssued
    
    def getJob(self, jobID):
        """Gets the job file associated the a given id
        """
        return self.jobIDsToJobsHash[jobID]
    
    def hasJob(self, jobID):
        """Returns true if the jobID is in the list of jobs.
        """
        return self.jobIDsToJobsHash.has_key(jobID)
        
    def getJobIDs(self):
        """Gets the set of jobs currently issued.
        """
        return self.jobIDsToJobsHash.keys()
    
    def removeJobID(self, jobID):
        """Removes a job from the jobBatcher.
        """
        assert jobID in self.jobIDsToJobsHash
        self.jobsIssued -= 1
        jobFile = self.jobIDsToJobsHash.pop(jobID)
        return jobFile
    
def processFinishedJob(jobID, resultStatus, updatedJobFiles, jobBatcher, childJobFileToParentJob, childCounts):
    """Function reads a processed job file and updates it state.
    """
    jobFile = jobBatcher.removeJobID(jobID)
    
    jobFileIsPresent = os.path.isfile(jobFile)
    updatingFileIsPresent = os.path.isfile(jobFile + ".updating")
    newFileIsPresent = os.path.isfile(jobFile + ".new")
    
    if resultStatus == 0 and updatingFileIsPresent:
        logger.critical("Despite the batch system claiming success there is an .updating file present: %s" % (jobFile + ".updating"))
        
    if resultStatus == 0 and newFileIsPresent:
        logger.critical("Despite the batch system claiming success there is a .new file present: %s" % (jobFile + ".new"))

    if not jobFileIsPresent and not newFileIsPresent: #The job is done
        if resultStatus != 0:
            logger.critical("Despite the batch system claiming failure the job %s seems to have finished and been removed" % jobFile)
            if os.path.exists(job.getLogFileName()):
                logFile(job.getLogFileName(), logger.critical)
        #Deal with parent
        parentJob = childJobFileToParentJob.pop(jobFile)
        childCounts[parentJob] -= 1
        assert childCounts[parentJob] >= 0
        if childCounts[parentJob] == 0: #Job is done
            childCounts.pop(parentJob)
            logger.debug("Parent job %s has has all its children run successfully", parentJobFile)
            assert parentJobFile not in updatedJobFiles
            updatedJobFiles.add(parentJob) #Now we know the job is done we can add it to the list of updated job files    
        return
    
    if resultStatus != 0 or newFileIsPresent or updatingFileIsPresent: #Job not successful according to batchsystem, or according to the existance of a .new or .updating file
        if updatingFileIsPresent: #The job failed while attempting to write the job file.
            logger.critical("There was an .updating file for the crashed job: %s" % jobFile)
            if os.path.isfile(jobFile + ".new"): #The job failed while writing the updated job file.
                logger.critical("There was an .new file for the crashed job: %s" % jobFile)
                os.remove(jobFile + ".new") #The existance of the .updating file means it wasn't complete
            os.remove(jobFile + ".updating") #Delete second the updating file second to preserve a correct state
            assert os.path.isfile(jobFile)
            job = readJob(jobFile) #The original must still be there.
            assert len(job.children) == 0
            for f in os.listdir(job.jobDir):
                try:
                    int(f)
                    logger.critical("Removing broken child %s\n" % f)
                    system("rm -rf %s" % f)
                except ValueError:
                    pass
            logger.critical("We've reverted to the original job file and marked it as failed: %s" % jobFile)
        else:
            if newFileIsPresent: #The job was not properly updated before crashing
                logger.critical("There is a valid .new file %s" % jobFile)
                if os.path.isfile(jobFile):
                    os.remove(jobFile)
                os.rename(jobFile + ".new", jobFile)
                job = readJob(jobFile)
            else:
                logger.critical("There was no valid .new file %s" % jobFile)
                assert os.path.isfile(jobFile)
                job = readJob(jobFile) #The job may have failed before or after creating this file, we check the state.
        job.remainingRetryCount -= 1
    else:
        job = readJob(jobFile)
        
    #Check for existance of log file and log if present
    if os.path.exists(job.getLogFileName()):
        logFile(job.getLogFileName(), logger.critical)

    assert job not in updatedJobFiles
    updatedJobFiles.add(job) #Now we know the job is done we can add it to the list of updated job files
    logger.debug("Added job: %s to active jobs" % jobFile)
    
def killJobs(jobsToKill, updatedJobFiles, jobBatcher, batchSystem):
    """Kills the given set of jobs and then sends them for processing
    """
    if len(jobsToKill) > 0:
        batchSystem.killJobs(jobsToKill)
        for jobID in jobsToKill:
            processFinishedJob(jobID, 1, updatedJobFiles, jobBatcher)

def reissueOverLongJobs(updatedJobFiles, jobBatcher, config, batchSystem):
    """Check each issued job - if it is running for longer than desirable.. issue a kill instruction.
    Wait for the job to die then we pass the job to processFinishedJob.
    """
    maxJobDuration = float(config.attrib["max_job_duration"])
    idealJobTime = float(config.attrib["job_time"])
    if maxJobDuration < idealJobTime * 10:
        logger.info("The max job duration is less than 10 times the ideal the job time, so I'm setting it to the ideal job time, sorry, but I don't want to crash your jobs because of limitations in jobTree ")
        maxJobDuration = idealJobTime * 10
    jobsToKill = []
    if maxJobDuration < 10000000: #We won't both doing anything is the rescue time is more than 16 weeks.
        runningJobs = batchSystem.getRunningJobIDs()
        for jobID in runningJobs.keys():
            if runningJobs[jobID] > maxJobDuration:
                logger.critical("The job: %s has been running for: %s seconds, more than the max job duration: %s, we'll kill it" % \
                            (jobBatcher.getJob(jobID), str(runningJobs[jobID]), str(maxJobDuration)))
                jobsToKill.append(jobID)
        killJobs(jobsToKill, updatedJobFiles, jobBatcher, batchSystem)

reissueMissingJobs_missingHash = {} #Hash to store number of observed misses
def reissueMissingJobs(updatedJobFiles, jobBatcher, batchSystem, killAfterNTimesMissing=3):
    """Check all the current job ids are in the list of currently running batch system jobs. 
    If a job is missing, we mark it as so, if it is missing for a number of runs of 
    this function (say 10).. then we try deleting the job (though its probably lost), we wait
    then we pass the job to processFinishedJob.
    """
    runningJobs = set(batchSystem.getIssuedJobIDs())
    jobIDsSet = set(jobBatcher.getJobIDs())
    #Clean up the reissueMissingJobs_missingHash hash
    missingJobIDsSet = set(reissueMissingJobs_missingHash.keys())
    for jobID in missingJobIDsSet.difference(jobIDsSet):
        reissueMissingJobs_missingHash.pop(jobID)
    assert runningJobs.issubset(jobIDsSet) #Assert checks we have no unexpected jobs running
    jobsToKill = []
    for jobID in set(jobIDsSet.difference(runningJobs)):
        jobFile = jobBatcher.getJob(jobID)
        if reissueMissingJobs_missingHash.has_key(jobID):
            reissueMissingJobs_missingHash[jobID] = reissueMissingJobs_missingHash[jobID]+1
        else:
            reissueMissingJobs_missingHash[jobID] = 1
        timesMissing = reissueMissingJobs_missingHash[jobID]
        logger.critical("Job %s with id %i is missing for the %i time" % (jobFile, jobID, timesMissing))
        if timesMissing == killAfterNTimesMissing:
            reissueMissingJobs_missingHash.pop(jobID)
            jobsToKill.append(jobID)
    killJobs(jobsToKill, updatedJobFiles, jobBatcher, batchSystem)
    return len(reissueMissingJobs_missingHash) == 0 #We use this to inform if there are missing jobs

def fixJobsList(config, jobFiles):
    """Traverses through and finds any .old files, using there saved state to recover a 
    valid state of the job tree.
    """
    for updatingFileName in jobFiles[:]:
        if ".updating" == updatingFileName[-9:]: #Things crashed while the state was updating, so we should remove the 'updating' and '.new' files
            logger.critical("Found a .updating file: %s" % updatingFileName)
            fileHandle = open(updatingFileName, 'r')
            for fileName in fileHandle.readline().split():
                if os.path.isfile(fileName):
                    logger.critical("File %s was listed in an updating file and will be removed %s" % fileName)
                    config.attrib["job_file_tree"].destroyTempFile(fileName)
                    jobFiles.remove(fileName)
                else:
                    logger.critical("File %s was listed in an updating file but does not exist %s" % fileName)
            fileHandle.close()
            config.attrib["job_file_tree"].destroyTempFile(updatingFileName)
            jobFiles.remove(updatingFileName)
    
    for fileName in jobFiles[:]: #Else any '.new' files will be switched in place of the original file. 
        if fileName[-4:] == '.new':
            originalFileName = fileName[:-4]
            os.rename(fileName, originalFileName)
            jobFiles.remove(fileName)
            if originalFileName not in jobFiles:
                jobFiles.append(originalFileName)
            logger.critical("Fixing the file: %s from %s" % (originalFileName, fileName))

def getJobFiles2(jobTreeJobsRoot, updatedJobFiles, childJobFileToParentJob, childCounts):
    if os.path.exists(getJobFileName(jobTreeJobsRoot)):
        pass
    for childDir in getChildDirs(jobTreeJobsRoot) 

def getJobsFiles(jobTreeJobsRoot, updatedJobFiles, childJobFileToParentJob, childCounts):
    
    
    #Read job
    
    
    #Get children
    childJobFiles = reduce(lambda x,y:x+y, [ getJobsFiles(childDir, updatedJobFiles, childJobFileToParentJob, childCounts) for childDir in getChildDirs(jobTreeJobsRoot) ], [])
    if len(childJobFiles) > 0:
        childCounts[job] = len(childJobFiles)
        for childJobFile in children:
            childJobFileToParentJob[job] = childJobFile
    else:
        updatedJobFiles.append(job)
    return [ job ]
    
def mainLoop(config, batchSystem):
    """This is the main loop from which jobs are issued and processed.
    """
    rescueJobsFrequency = float(config.attrib["rescue_jobs_frequency"])
    maxJobDuration = float(config.attrib["max_job_duration"])
    assert maxJobDuration >= 0
    logger.info("Got parameters,rescue jobs frequency: %s max job duration: %s" % \
                (rescueJobsFrequency, maxJobDuration))
    
    #Kill any jobs on the batch system queue from the last time.
    assert len(batchSystem.getIssuedJobIDs()) == 0 #Batch system must start with no active jobs!
    logger.info("Checked batch system has no running jobs and no updated jobs")
    
    childJobFileToParentJob, childCounts, updatedJobFiles = {}, {}, set()
    getJobsFiles(config.attrib["job_file_tree"], updatedJobFile, childJobFileToParentJob, childCounts)
    jobBatcher = JobBatcher(config, batchSystem)
    
    stats = config.attrib.has_key("stats")
    if stats:
        startTime = time.time()
        startClock = getTotalCpuTime()
        
    timeSinceJobsLastRescued = time.time() #Sets up the timing of the job rescuing method
    totalFailedJobs = 0
    logger.info("Starting the main loop")
    while True: 
        if len(updatedJobFiles) > 0:
            logger.debug("Built the jobs list, currently have %i job files, %i jobs to update and %i jobs currently issued" % (totalJobFiles, len(updatedJobFiles), jobBatcher.getNumberOfJobsIssued()))
        
        for message in job.messages:
            logger.critical("Got message from job: %s", message)
        job.messages = []
        
        for job in list(updatedJobFiles):
            updatedJobFiles.remove(job)
            
            if len(job.children) > 0:
                logger.debug("Job: %s has %i children to schedule" % (job.getJobFileName(), len(unbornChildren)))
                children = job.children
                job.children = []
                job.update()
                for childJobFile, memory, cpu in children:
                    childJobFileToParentJob[childJobFile] = job
                assert job not in childCounts
                childCounts[job] = len(children)
                jobBatcher.issueJobs(children)
            else:
                if job.remainingRetryCount > 0:
                    logger.debug("Job: %s has a new command that we can now issue" % job.getJobFileName())
                    assert len(job.followOnCommands) > 0
                    memory, cpu = job.followOnCommands[-1][1:2]
                    jobBatcher.issueJob(job.getJobFileName, memory, cpu)
                else:
                    totalFailedJobs += 1
                    logger.critical("Job: %s is completely failed" % job.getJobFileName())
                   
        if len(updatedJobFiles) == 0:
            if jobBatcher.getNumberOfJobsIssued() == 0:
                logger.info("Only failed jobs and their dependents (%i total) are remaining, so exiting." % totalFailedJobs)
                break 
            updatedJob = batchSystem.getUpdatedJob(10) #pauseForUpdatedJob(batchSystem.getUpdatedJob) #Asks the batch system what jobs have been completed.
            if updatedJob != None: #Runs through a map of updated jobs and there status, 
                jobID, result = updatedJob
                if jobBatcher.hasJob(jobID): 
                    if result == 0:
                        logger.debug("Batch system is reporting that the job %s ended successfully" % jobBatcher.getJob(jobID))   
                    else:
                        logger.critical("Batch system is reporting that the job %s failed with exit value %i" % (jobBatcher.getJob(jobID), result))  
                    processFinishedJob(jobID, result, updatedJobFiles, jobBatcher)
                else:
                    logger.info("A result seems to already have been processed: %i" % jobID)
        
        if len(updatedJobFiles) == 0 and time.time() - timeSinceJobsLastRescued >= rescueJobsFrequency: #We only rescue jobs every N seconds, and when we have apparently exhausted the current job supply
            reissueOverLongJobs(updatedJobFiles, jobBatcher, config, batchSystem)
            logger.info("Reissued any over long jobs")
            
            hasNoMissingJobs = reissueMissingJobs(updatedJobFiles, jobBatcher, batchSystem)
            if hasNoMissingJobs:
                timeSinceJobsLastRescued = time.time()
            else:
                timeSinceJobsLastRescued += 60 #This means we'll try again in 60 seconds
            logger.info("Rescued any (long) missing jobs")
    
    logger.info("Finished the main loop")   
    
    if stats:
        fileHandle = open(getStatsFileName(config.attrib["job_tree"]), 'ab')
        fileHandle.write("<total_time time='%s' clock='%s'/></stats>" % (str(time.time() - startTime), str(getTotalCpuTime() - startClock)))
        fileHandle.close()
    
    return totalFailedJobs #Returns number of failed jobs
