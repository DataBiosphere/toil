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

from job import Job, readJob, getJobFileName, getJobStatsFileName
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
    
class JobRemover:
    """Class asynchronously deletes jobs
    """
    def __init__(self, config):
        self.inputQueue = JoinableQueue()
        
        def jobDeleter(inputQueue, makeStats, statsFile, fileTreeFile):
            #Designed to not share any state bar the queue and two strings
            fileTree = TempFileTree(fileTreeFile) #only use for deletions, so okay
            if makeStats:
                statsFileHandle = open(statsFile, 'ab')
                while True:
                    globalTempDir = inputQueue.get()
                    jobStatsFile = getJobStatsFileName(globalTempDir)
                    fH = open(jobStatsFile,'rb')
                    shutil.copyfileobj(fH, statsFileHandle)
                    fH.close()
                    os.remove(jobStatsFile)
                    os.remove(getJobFileName(globalTempDir))
                    fileTree.destroyTempDir(globalTempDir)
                    statsFileHandle.flush()
                    inputQueue.task_done()
            else:
                while True:
                    globalTempDir = inputQueue.get()
                    os.remove(getJobFileName(globalTempDir))
                    fileTree.destroyTempDir(globalTempDir)
                    inputQueue.task_done()
   
        self.worker = Process(target=jobDeleter, 
                              args=(self.inputQueue, config.attrib.has_key("stats"), 
                                    getStatsFileName(config.attrib["job_tree"]), 
                                    getJobFileDirName(config.attrib["job_tree"])))
        self.worker.start()
    
    def deleteJob(self, job):
        self.inputQueue.put(job.getGlobalTempDirName())
    
    def deleteJobs(self, jobs):
        for job in jobs:
            self.deleteJob(job.getGlobalTempDirName())
            
    def join(self):
        self.inputQueue.join()
        self.worker.terminate()
    
def writeJob(job, noCheckPoints=False):
    """Writes a single job to file
    """
    if noCheckPoints: #This avoids the expense of atomic updates
        job.write(job.getJobFileName())
    else:
        tempJobFileName = job.getJobFileName() + ".tmp"
        job.write(tempJobFileName)
        os.rename(tempJobFileName, job.getJobFileName())

def writeJobs(jobs, noCheckPoints=False):
    """Writes a list of jobs to file, ensuring that the previous
    state is maintained until after the write is complete
    """
    if noCheckPoints: #This avoids the expense of atomic updates
        for job in jobs:
            job.write(job.getJobFileName())
        return
    
    if len(jobs) == 0:
        return
    assert len(set(jobs)) == len(jobs)
    #Create a unique updating name using the first file in the list
    fileName = jobs[0].getJobFileName()
    updatingFile = fileName + ".updating"
    
    #The existence of the the updating file signals we are in the process of creating an update to the state of the files
    assert not os.path.isfile(updatingFile)
    fileHandle = open(updatingFile, 'w')
    fileHandle.write(" ".join([ job.getJobFileName() + ".new" for job in jobs ]))
    fileHandle.close()
    
    #Update the current files.
    for job in jobs:
        newFileName = job.getJobFileName() + ".new"
        assert not os.path.isfile(newFileName)
        job.write(newFileName)
       
    os.remove(updatingFile) #Remove the updating file, now the new files represent the valid state
    
    for job in jobs:
        if os.path.isfile(job.getJobFileName()):
            os.remove(job.getJobFileName())
        os.rename(job.getJobFileName() + ".new", job.getJobFileName())

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
        
    def issueJob(self, job):
        """Add a job to the queue of jobs
        """
        self.jobsIssued += 1
        memory, cpu = job.getNextFollowOnCommandToIssue()[1:]
        if cpu > self.maxCpus:
            raise RuntimeError("Requesting more cpus than available. Requested: %s, Available: %s" % (cpu, self.maxCpus))
        jobFile = job.getJobFileName()
        jobCommand = "%s -E %s %s %s %s" % (sys.executable, self.jobTreeSlavePath, self.rootPath, self.jobTree, jobFile)
        jobID = self.batchSystem.issueJob(jobCommand, memory, cpu)
        self.jobIDsToJobsHash[jobID] = jobFile
        logger.debug("Issued the job: %s with job id: %i and cpus: %i" % (jobFile, jobID, cpu))
    
    def issueJobs(self, jobs):
        """Add a list of jobs
        """
        for job in jobs:
            self.issueJob(job)
    
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
    
def fixJobsList(config, jobFiles):
    """Traverses through and finds any .old files, using there saved state to recover a 
    valid state of the job tree.
    """
    for updatingFileName in jobFiles[:]:
        if ".updating" == updatingFileName[-9:]: #Things crashed while the state was updating, so we should remove the 'updating' and '.new' files
            fileHandle = open(updatingFileName, 'r')
            for fileName in fileHandle.readline().split():
                if os.path.isfile(fileName):
                    config.attrib["job_file_tree"].destroyTempFile(fileName)
                    jobFiles.remove(fileName)
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

def restartFailedJobs(config, jobFiles):
    """Traverses through the file tree and resets the restart count of all jobs.
    """
    for absFileName in jobFiles:
        if os.path.isfile(absFileName):
            job = readJob(absFileName)
            logger.info("Restarting job: %s" % job.getJobFileName())
            job.setRemainingRetryCount(int(config.attrib["retry_count"]))
            if job.getColour() == Job.red:
                job.setColour(Job.grey)
            writeJob(job)
    
def reportJobLogFile(job):
    logger.critical("The log file of the job")
    if os.path.exists(job.getLogFileName()):
        logFile(job.getLogFileName(), logger.critical)
    else:
        logger.critical("Log file does not exist: %s" % job.getLogFileName())

def processFinishedJob(jobID, resultStatus, updatedJobFiles, jobBatcher):
    """Function reads a processed job file and updates it state.
    """
    jobFile = jobBatcher.removeJobID(jobID)
    
    updatingFileIsPresent = os.path.isfile(jobFile + ".updating")
    newFileIsPresent = os.path.isfile(jobFile + ".new")
    
    if resultStatus == 0 and updatingFileIsPresent:
        logger.critical("Despite the batch system claiming success there is an .updating file present: %s", jobFile + ".updating")
        
    if resultStatus == 0 and newFileIsPresent:
        logger.critical("Despite the batch system claiming success there is a .new file present: %s", jobFile + ".new")

    if resultStatus != 0 or newFileIsPresent or updatingFileIsPresent: #Job not successful according to batchsystem, or according to the existance of a .new or .updating file
        if updatingFileIsPresent: #The job failed while attempting to write the job file.
            logger.critical("There was an .updating file for the crashed job: %s" % jobFile)
            if os.path.isfile(jobFile + ".new"): #The job failed while writing the updated job file.
                logger.critical("There was an .new file for the crashed job: %s" % jobFile)
                os.remove(jobFile + ".new") #The existance of the .updating file means it wasn't complete
            os.remove(jobFile + ".updating") #Delete second the updating file second to preserve a correct state
            assert os.path.isfile(jobFile)
            job = readJob(jobFile) #The original must still be there.
            reportJobLogFile(job)
            assert job.getNumberOfChildCommandsToIssue() == 0 #The original can not reflect the end state of the job.
            assert job.getCompletedChildCount() == job.getIssuedChildCount()
            job.setColour(Job.red) #It failed, so we mark it so and continue.
            writeJob(job)
            logger.critical("We've reverted to the original job file and marked it as failed: %s" % jobFile)
        else:
            if newFileIsPresent: #The job was not properly updated before crashing
                logger.critical("There is a valid .new file %s" % jobFile)
                if os.path.isfile(jobFile):
                    os.remove(jobFile)
                os.rename(jobFile + ".new", jobFile)
                job = readJob(jobFile)
                reportJobLogFile(job)
                if job.getColour() == Job.grey: #The job failed while preparing to run another job on the slave
                    assert job.getNumberOfChildCommandsToIssue() == 0 #File 
                    job.setColour(Job.red)
                    writeJob(job)
                assert job.getColour() in (Job.black, Job.red)
            else:
                logger.critical("There was no valid .new file %s" % jobFile)
                assert os.path.isfile(jobFile)
                job = readJob(jobFile) #The job may have failed before or after creating this file, we check the state.
                reportJobLogFile(job)
                if job.getColour() == Job.black: #The job completed okay, so we'll keep it
                    logger.critical("Despite the batch system job failing, the job appears to have completed okay")
                else:
                    assert job.getColour() in (Job.grey, Job.red)
                    assert job.getNumberOfChildCommandsToIssue() == 0 
                    assert job.getCompletedChildCount() == job.getIssuedChildCount()
                    if job.getColour() == Job.grey:
                        job.setColour(Job.red)
                        writeJob(job)
                    logger.critical("We've reverted to the original job file and marked it as failed: %s" % jobFile)
    else:
        job = readJob(jobFile)

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
    
    jobFiles = []
    for globalTempDir in config.attrib["job_file_tree"].listFiles():
        assert os.path.isdir(globalTempDir)
        for tempFile in os.listdir(globalTempDir):
            if "job.xml" == tempFile[:7]:
                jobFiles.append(os.path.join(globalTempDir, tempFile))
    logger.info("Got a list of job files")
    
    #Repair the job tree using any .old files
    fixJobsList(config, jobFiles)
    logger.info("Fixed the job files using any .old files")
    
    #Get jobs that were running, or that had failed reset to 'grey' status
    restartFailedJobs(config, jobFiles)
    logger.info("Reworked failed jobs")
    
    openParentsHash = {} #Parents that are already in memory, saves loading and reloading
    updatedJobFiles = set() #Jobs whose status needs updating, either because they have finished, or because they need to be started.
    for jobFile in jobFiles:
        job = readJob(jobFile)
        if job.getColour() == Job.blue:
            openParentsHash[job.getJobFileName()] = job
        else:
            updatedJobFiles.add(job)
    logger.info("Got the active (non blue) job files")
    
    totalJobFiles = len(jobFiles) #Total number of job files we have.
    jobBatcher = JobBatcher(config, batchSystem)
    
    idealJobTime = float(config.attrib["job_time"]) 
    assert idealJobTime > 0.0
    
    reportAllJobLogFiles = config.attrib.has_key("reportAllJobLogFiles")
    
    stats = config.attrib.has_key("stats")
    if stats:
        startTime = time.time()
        startClock = getTotalCpuTime()
        
    timeSinceJobsLastRescued = time.time() #Sets up the timing of the job rescuing method
    
    noCheckPoints = config.attrib.has_key("no_check_points") #Switch off most checkpointing
    
    jobRemover = JobRemover(config) #Process for deleting jobs
    
    logger.info("Starting the main loop")
    while True: 
        if len(updatedJobFiles) > 0:
            logger.debug("Built the jobs list, currently have %i job files, %i jobs to update and %i jobs currently issued" % (totalJobFiles, len(updatedJobFiles), jobBatcher.getNumberOfJobsIssued()))
        
        for job in list(updatedJobFiles):
            updatedJobFiles.remove(job)
            assert job.getColour() != Job.blue
            
            def reissueJob(job):
                #Reset the log files for the job.
                assert job.getColour() == Job.grey
                jobBatcher.issueJob(job)
                
            def makeGreyAndReissueJob(job):
                job.setColour(Job.grey)
                writeJob(job, noCheckPoints=noCheckPoints)
                reissueJob(job)
            
            if job.getColour() == Job.grey: #Get ready to start the job, should only happen when restarting from failure or with first job
                reissueJob(job)
            elif job.getColour() == Job.black: #Job has finished okay
                logger.debug("Job: %s has finished okay" % job.getJobFileName())
                if reportAllJobLogFiles:
                    reportJobLogFile(job)
                #Messages
                for message in job.removeMessages():
                    logger.critical("Received the following message from job: %s" % message)
                childCount = job.getIssuedChildCount()
                blackChildCount = job.getCompletedChildCount()
                assert childCount == blackChildCount #Has no currently running child jobs
                #Launch any unborn children
                unbornChildren = job.removeChildrenToIssue()
                assert job.getNumberOfChildCommandsToIssue() == 0
                if len(unbornChildren) > 0: #unbornChild != None: #We must give birth to the unborn children
                    logger.debug("Job: %s has %i children to schedule" % (job.getJobFileName(), len(unbornChildren)))
                    newChildren = []
                    for unbornCommand, unbornMemory, unbornCpu in unbornChildren:
                        newJob = Job(unbornCommand, unbornMemory, unbornCpu, job.getJobFileName(), config)
                        totalJobFiles += 1
                        newChildren.append(newJob)
                    job.setIssuedChildCount(childCount + len(newChildren))
                    job.setColour(Job.blue) #Blue - has children running.
                    assert job.getJobFileName() not in openParentsHash
                    openParentsHash[job.getJobFileName()] = job
                    if noCheckPoints:
                        writeJobs(newChildren, noCheckPoints=True) #In this case only the children need be written
                    else:
                        writeJobs([ job ] + newChildren, noCheckPoints=False) #Check point, including the parent
                    jobBatcher.issueJobs(newChildren)
                elif job.getNumberOfFollowOnCommandsToIssue() != 0: #Has another job
                    if job.getNextFollowOnCommandToIssue()[0] == "": #Was a stub job
                        job.popNextFollowOnCommandToIssue()
                        updatedJobFiles.add(job)
                        logger.debug("Filtering out stub job")
                    else:
                        logger.debug("Job: %s has a new command that we can now issue" % job.getJobFileName())
                        ##Reset the job run info
                        job.setRemainingRetryCount(int(config.attrib["retry_count"]))
                        makeGreyAndReissueJob(job)
                else: #Job has finished, so we can defer to any parent
                    logger.debug("Job: %s is now dead" % job.getJobFileName())
                    if job.getParentJobFile() != None:
                        assert openParentsHash.has_key(job.getParentJobFile())
                        parent = openParentsHash[job.getParentJobFile()]
                        assert job.getParentJobFile() != job.getJobFileName()
                        assert parent.getColour() == Job.blue
                        assert parent.getCompletedChildCount() < parent.getIssuedChildCount()
                        job.setColour(Job.dead)
                        parent.setCompletedChildCount(parent.getCompletedChildCount()+1)
                        if parent.getIssuedChildCount() == parent.getCompletedChildCount():
                            parent.setColour(Job.black)
                            assert parent.getJobFileName() not in updatedJobFiles
                            updatedJobFiles.add(parent)
                            openParentsHash.pop(job.getParentJobFile())
                        if not noCheckPoints: #If no checkpoint then this is all unneccesary 
                            writeJobs([ parent, job ], noCheckPoints=False)
                    #Else if no parent then we're at the end at we need not check point further
                    totalJobFiles -= 1
                    jobRemover.deleteJob(job)
                         
            elif job.getColour() == Job.red: #Job failed
                logger.critical("Job: %s failed" % job.getJobFileName())
                reportJobLogFile(job)
                #Checks
                assert job.getNumberOfChildCommandsToIssue() == 0
                assert job.getIssuedChildCount() == job.getCompletedChildCount()
                
                remainingRetryCount = job.getRemainingRetryCount()
                if remainingRetryCount > 0: #Give it another try, maybe there is a bad node somewhere
                    job.setRemainingRetryCount(job.getRemainingRetryCount()-1)
                    logger.critical("Job: %s will be restarted, it has %s goes left out of %i" % (job.getJobFileName(), job.getRemainingRetryCount(), int(config.attrib["retry_count"])))
                    makeGreyAndReissueJob(job)
                else:
                    assert remainingRetryCount == 0
                    logger.critical("Job: %s is completely failed" % job.getJobFileName())
                    
            else: #This case should only occur after failure
                logger.debug("Job: %s is already dead, we'll get rid of it" % job.getJobFileName())
                assert job.getColour() == Job.dead
                totalJobFiles -= 1
                jobRemover.deleteJob(job)
                   
        if len(updatedJobFiles) == 0:
            if jobBatcher.getNumberOfJobsIssued() == 0:
                logger.info("Only failed jobs and their dependents (%i total) are remaining, so exiting." % totalJobFiles)
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
    
    logger.info("Finished the main loop, now must finish deleting files")
    startTimeForRemovingFiles = time.time()
    jobRemover.join()
    logger.critical("It took %i seconds to finish deleting files" % (time.time() - startTimeForRemovingFiles))    
    
    if stats:
        fileHandle = open(getStatsFileName(config.attrib["job_tree"]), 'ab')
        fileHandle.write("<total_time time='%s' clock='%s'/></stats>" % (str(time.time() - startTime), str(getTotalCpuTime() - startClock)))
        fileHandle.close()
    
    return totalJobFiles #Returns number of failed jobs
