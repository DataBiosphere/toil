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
import xml.etree.ElementTree as ET
import time
from collections import deque
#from threading import Thread, Queue
from multiprocessing import Process, JoinableQueue
#from multiprocessing.queues import SimpleQueue
#from bz2 import BZ2File

from sonLib.bioio import logger, getTotalCpuTime
from sonLib.bioio import getLogLevelString
from sonLib.bioio import logFile
from sonLib.bioio import system
from jobTree.src.bioio import workflowRootPath

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

def getJobFileName(job):
    return os.path.join(job.attrib["global_temp_dir"], "job.xml")

def getSlaveLogFileName(job):
    return os.path.join(job.attrib["global_temp_dir"], "slave_log.txt")
    
def getLogFileName(job):
    return os.path.join(job.attrib["global_temp_dir"], "log.txt")
    
def getGlobalTempDirName(job):
    return job.attrib["global_temp_dir"]
    
def getJobStatsFileName(job):
    return os.path.join(job.attrib["global_temp_dir"], "stats.txt")

def createJob(attrib, parent, config):
    """Creates an XML record for the job in a file within the hierarchy of jobs.
    """
    job = ET.Element("job")
    job.attrib["global_temp_dir"] = config.attrib["job_file_tree"].getTempDirectory()
    job.attrib["remaining_retry_count"] = config.attrib["retry_count"]
    job.attrib["colour"] = "grey"
    followOns = ET.SubElement(job, "followOns")
    ET.SubElement(followOns, "followOn", attrib.copy())
    if parent != None:
        job.attrib["parent"] = parent
    job.attrib["child_count"] = "0"
    job.attrib["black_child_count"] = "0"
    ET.SubElement(job, "children") 
    return job
    
class JobRemover:
    """Class asynchronously deletes jobs
    """
    def __init__(self, config):
        self.inputQueue = JoinableQueue()
        def jobDeleter(inputQueue, config):
            stats = config.attrib.has_key("stats")
            fileTree = config.attrib["job_file_tree"]
            while True:
                job = inputQueue.get()
                #Try explicitly removing these files, leaving empty dir
                if stats: 
                    os.remove(getJobStatsFileName(job))
                os.remove(getJobFileName(job))
                fileTree.destroyTempDir(getGlobalTempDirName(job))
                inputQueue.task_done()
        self.worker = Process(target=jobDeleter, args=(self.inputQueue, config))
        #worker.setDaemon(True)
        self.worker.start()
    
    def deleteJob(self, job):
        self.inputQueue.put(job)
    
    def deleteJobs(self, jobs):
        for job in jobs:
            self.deleteJob(job)
            
    def join(self):
        self.inputQueue.join()
        self.worker.terminate()
        
def readJob(jobFile):
    logger.debug("Going to load the file %s" % jobFile)
    return ET.parse(jobFile).getroot()
    #fileHandle = open(jobFile, 'r')
    #fileHandle = BZ2File(jobFile, 'r')
    #job = ET.parse(fileHandle).getroot()
    #fileHandle.close()
    #return job
        
def writeJobFile(job, jobFileName):
    tree = ET.ElementTree(job)
    fileHandle = open(jobFileName, 'w') 
    #fileHandle = BZ2File(jobFileName, 'w', compresslevel=5)
    tree.write(fileHandle)
    fileHandle.close()

def writeJob(job, noCheckPoints=False):
    """Writes a single job to file
    """
    if noCheckPoints: #This avoids the expense of atomic updates
        writeJobFile(job, getJobFileName(job))
    else:
        tempJobFileName = getJobFileName(job) + ".tmp"
        writeJobFile(job, tempJobFileName)
        os.rename(tempJobFileName, getJobFileName(job))

def writeJobs(jobs, noCheckPoints=False):
    """Writes a list of jobs to file, ensuring that the previous
    state is maintained until after the write is complete
    """
    if noCheckPoints: #This avoids the expense of atomic updates
        for job in jobs:
            writeJobFile(job, getJobFileName(job))
        return
    
    if len(jobs) == 0:
        return
    assert len(set(jobs)) == len(jobs)
    #Create a unique updating name using the first file in the list
    fileName = getJobFileName(jobs[0])
    updatingFile = fileName + ".updating"
    
    #The existence of the the updating file signals we are in the process of creating an update to the state of the files
    assert not os.path.isfile(updatingFile)
    fileHandle = open(updatingFile, 'w')
    fileHandle.write(" ".join([ getJobFileName(job) + ".new" for job in jobs ]))
    fileHandle.close()
    
    #Update the current files.
    for job in jobs:
        newFileName = getJobFileName(job) + ".new"
        assert not os.path.isfile(newFileName)
        writeJobFile(job, newFileName)
       
    os.remove(updatingFile) #Remove the updating file, now the new files represent the valid state
    
    for job in jobs:
        if os.path.isfile(getJobFileName(job)):
            os.remove(getJobFileName(job))
        os.rename(getJobFileName(job) + ".new", getJobFileName(job))

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
        followOnJob = job.find("followOns").findall("followOn")[-1]
        memory = int(followOnJob.attrib["memory"])
        cpu = int(followOnJob.attrib["cpu"])
        assert cpu < sys.maxint
        assert memory < sys.maxint
        if cpu > self.maxCpus:
            raise RuntimeError("Requesting more cpus than available. Requested: %s, Available: %s" % (cpu, self.maxCpus))
        jobFile = getJobFileName(job)
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
            logger.info("Restarting job: %s" % getJobFileName(job))
            job.attrib["remaining_retry_count"] = config.attrib["retry_count"]
            if job.attrib["colour"] == "red":
                job.attrib["colour"] = "grey"
            writeJobs([ job ])
            
def reportSlaveJobLogFile(job):
    logger.critical("The log file of the slave for the job")
    if os.path.exists(getSlaveLogFileName(job)):
        logFile(getSlaveLogFileName(job), logger.critical) #We log the job log file in the main loop
    else:
        logger.critical("Slave file does not exist: %s" % getSlaveLogFileName(job))

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
            reportSlaveJobLogFile(job)
            assert job.find("children").find("child") == None #The original can not reflect the end state of the job.
            assert int(job.attrib["black_child_count"]) == int(job.attrib["child_count"])
            job.attrib["colour"] = "red" #It failed, so we mark it so and continue.
            writeJobs([ job ])
            logger.critical("We've reverted to the original job file and marked it as failed: %s" % jobFile)
        else:
            if newFileIsPresent: #The job was not properly updated before crashing
                logger.critical("There is a valid .new file %s" % jobFile)
                if os.path.isfile(jobFile):
                    os.remove(jobFile)
                os.rename(jobFile + ".new", jobFile)
                job = readJob(jobFile)
                reportSlaveJobLogFile(job)
                if job.attrib["colour"] == "grey": #The job failed while preparing to run another job on the slave
                    assert job.find("children").find("child") == None #File 
                    job.attrib["colour"] = "red"
                    writeJobs([ job ])
                assert job.attrib["colour"] in ("black", "red")
            else:
                logger.critical("There was no valid .new file %s" % jobFile)
                assert os.path.isfile(jobFile)
                job = readJob(jobFile) #The job may have failed before or after creating this file, we check the state.
                reportSlaveJobLogFile(job)
                if job.attrib["colour"] == "black": #The job completed okay, so we'll keep it
                    logger.critical("Despite the batch system job failing, the job appears to have completed okay")
                else:
                    assert job.attrib["colour"] in ("grey", "red")
                    assert job.find("children").find("child") == None #File 
                    assert int(job.attrib["black_child_count"]) == int(job.attrib["child_count"])
                    if job.attrib["colour"] == "grey":
                        job.attrib["colour"] = "red"
                        writeJobs([ job ])
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
    
def reportJobLogFiles(job):
    logger.critical("The log file of the slave for the job")
    if os.path.exists(getSlaveLogFileName(job)):
        logFile(getSlaveLogFileName(job), logger.critical) #We log the job log file in the main loop
    else:
        logger.critical("Slave file does not exist: %s" % getSlaveLogFileName(job))
    logger.critical("The log file of the job")
    if os.path.exists(getLogFileName(job)):
        logFile(getLogFileName(job), logger.critical)
    else:
        logger.critical("Log file does not exist: %s" % getLogFileName(job))
    
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
            print "temp file", tempFile, "job.xml" == tempFile[:7]
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
        if job.attrib["colour"] in ("blue"):
            openParentsHash[getJobFileName(job)] = job
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
            assert job.attrib["colour"] is not "blue"
            
            def reissueJob(job):
                #Reset the log files for the job.
                assert job.attrib["colour"] == "grey"
                jobBatcher.issueJob(job)
                
            def makeGreyAndReissueJob(job):
                job.attrib["colour"] = "grey"
                writeJob(job, noCheckPoints=noCheckPoints)
                reissueJob(job)
            
            if job.attrib["colour"] == "grey": #Get ready to start the job, should only happen when restarting from failure or with first job
                reissueJob(job)
            elif job.attrib["colour"] == "black": #Job has finished okay
                logger.debug("Job: %s has finished okay" % getJobFileName(job))
                if reportAllJobLogFiles:
                    reportJobLogFiles(job)
                #Deal with stats
                if stats:
                    system("cat %s >> %s" % (getJobStatsFileName(job), getStatsFileName(config.attrib["job_tree"])))
                if job.find("messages") != None:
                    for message in job.find("messages").findall("message"):
                        logger.critical("Received the following message from job: %s" % message.attrib["message"])
                    job.remove(job.find("messages"))
                childCount = int(job.attrib["child_count"])
                blackChildCount = int(job.attrib["black_child_count"])
                assert childCount == blackChildCount #Has no currently running child jobs
                #Launch any unborn children
                unbornChildren = job.find("children").findall("child")
                if len(unbornChildren) > 0: #unbornChild != None: #We must give birth to the unborn children
                    logger.debug("Job: %s has %i children to schedule" % (getJobFileName(job), len(unbornChildren)))
                    newChildren = []
                    for unbornChild in unbornChildren:
                        newJob = createJob(unbornChild.attrib, getJobFileName(job), config)
                        totalJobFiles += 1
                        newChildren.append(newJob)
                    job.find("children").clear() #removeall("child")
                    job.attrib["child_count"] = str(childCount + len(newChildren))
                    job.attrib["colour"] = "blue" #Blue - has children running.
                    assert getJobFileName(job) not in openParentsHash
                    openParentsHash[getJobFileName(job)] = job
                    if noCheckPoints:
                        writeJobs(newChildren, noCheckPoints=True) #In this case only the children need be written
                    else:
                        writeJobs([ job ] + newChildren, noCheckPoints=False) #Check point, including the parent
                    jobBatcher.issueJobs(newChildren)
                    
                elif len(job.find("followOns").findall("followOn")) != 0: #Has another job
                    logger.debug("Job: %s has a new command that we can now issue" % getJobFileName(job))
                    ##Reset the job run info
                    job.attrib["remaining_retry_count"] = config.attrib["retry_count"]
                    makeGreyAndReissueJob(job)
                    
                else: #Job has finished, so we can defer to any parent
                    logger.debug("Job: %s is now dead" % getJobFileName(job))
                    if job.attrib.has_key("parent"):
                        assert openParentsHash.has_key(job.attrib["parent"])
                        parent = openParentsHash[job.attrib["parent"]]
                        assert job.attrib["parent"] != getJobFileName(job)
                        assert parent.attrib["colour"] == "blue"
                        assert int(parent.attrib["black_child_count"]) < int(parent.attrib["child_count"])
                        job.attrib["colour"] == "dead"
                        parent.attrib["black_child_count"] = str(int(parent.attrib["black_child_count"]) + 1)
                        if int(parent.attrib["child_count"]) == int(parent.attrib["black_child_count"]):
                            parent.attrib["colour"] = "black"
                            assert getJobFileName(parent) not in updatedJobFiles
                            updatedJobFiles.add(parent)
                            openParentsHash.pop(job.attrib["parent"])
                        if not noCheckPoints: #If no checkpoint then this is all unneccesary 
                            writeJobs([ parent, job ], noCheckPoints=False)
                    #Else if no parent then we're at the end at we need not check point further
                    totalJobFiles -= 1
                    jobRemover.deleteJob(job)
                         
            elif job.attrib["colour"] == "red": #Job failed
                logger.critical("Job: %s failed" % getJobFileName(job))
                reportJobLogFiles(job)
                #Checks
                assert len(job.find("children").findall("child")) == 0
                assert int(job.attrib["child_count"]) == int(job.attrib["black_child_count"])
                
                remainingRetryCount = int(job.attrib["remaining_retry_count"])
                if remainingRetryCount > 0: #Give it another try, maybe there is a bad node somewhere
                    job.attrib["remaining_retry_count"] = str(remainingRetryCount-1)
                    logger.critical("Job: %s will be restarted, it has %s goes left" % (getJobFileName(job), job.attrib["remaining_retry_count"]))
                    makeGreyAndReissueJob(job)
                else:
                    assert remainingRetryCount == 0
                    logger.critical("Job: %s is completely failed" % getJobFileName(job))
                    
            else: #This case should only occur after failure
                logger.debug("Job: %s is already dead, we'll get rid of it" % getJobFileName(job))
                assert job.attrib["colour"] == "dead"
                totalJobFiles -= 1
                jobRemover.deleteJob(job)
                   
        if len(updatedJobFiles) == 0:
            if jobBatcher.getNumberOfJobsIssued() == 0:
                logger.info("Only failed jobs and their dependents (%i total) are remaining, so exiting." % totalJobFiles)
                break 
            updatedJob = batchSystem.getUpdatedJob(5) #pauseForUpdatedJob(batchSystem.getUpdatedJob) #Asks the batch system what jobs have been completed.
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
    
    if stats:
        fileHandle = open(getStatsFileName(config.attrib["job_tree"]), 'a')
        fileHandle.write("<total_time time='%s' clock='%s'/></stats>" % (str(time.time() - startTime), str(getTotalCpuTime() - startClock)))
        fileHandle.close()
    
    logger.info("Finished the main loop, now must finish deleting files")
    startTime = time.time()
    jobRemover.join()
    logger.critical("It took %i seconds to finish deleting files" % (time.time() - startTime))    
    
    return totalJobFiles #Returns number of failed jobs
