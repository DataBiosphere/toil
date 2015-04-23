
import marshal as pickler
#import cPickle as pickler
#import pickle as pickler
#import json as pickler    
import os
import re
import time
from jobTree.src.bioio import system
from sonLib.bioio import logFile
from sonLib.bioio import logger

class JobTreeState:
    """Represents the state of the jobTree.
    """
    def __init__(self):
        self.childJobStoreIdToParentJob = {}
        self.childCounts = {}
        self.updatedJobs = set()
        self.shellJobs = set()

class JobDB:
    """Represents the jobTree on disk/in a db.
    """
    def __init__(self, config, create=True):
        self.jobTreeState = None #This will be none until "loadJobTree" is called.
        self.config = config
        if create and not os.path.exists(self._getJobFileDirName()):
            os.mkdir(self._getJobFileDirName())
    
    def createFirstJob(self, command, memory, cpu):
        return Job(command=command, memory=memory, cpu=cpu, 
              tryCount=int(self.config.attrib["try_count"]), 
              jobStoreID=self._getJobFileDirName())
    
    def exists(self, jobStoreID):
        """Returns true if the job is in the store, else false.
        """
        return os.path.exists(self._getJobFileName(jobStoreID))
    
    def load(self, jobStoreID):
        """Loads a job.
        """
        fileHandle = open(self._getJobFileName(jobStoreID), 'r')
        job = Job.convertJsonJobToJob(pickler.load(fileHandle))
        fileHandle.close()
        return job
    
    def write(self, job):
        """Updates a job's status in store atomically
        """
        self._write(job, ".new")
        os.rename(self._getJobFileName(job.jobStoreID) + ".new", self._getJobFileName(job.jobStoreID))
        return job.jobStoreID
    
    def update(self, job, childCommands):
        """Creates a set of child jobs for the given job and updates state of job 
        atomically on disk with new children.
        """
        updatingFile = self._getJobFileName(job.jobStoreID) + ".updating"
        open(updatingFile, 'w').close()
        for ((command, memory, cpu), tempDir) in zip(childCommands, \
                    self._createTempDirectories(job.jobStoreID, len(childCommands))):
            childJob = Job(command, memory, cpu, \
                    int(self.config.attrib["try_count"]), tempDir)
            self.write(childJob)
            job.children.append((childJob.jobStoreID, memory, cpu))
        self._write(job, ".new")
        os.remove(updatingFile)
        os.rename(self._getJobFileName(job.jobStoreID) + ".new", self._getJobFileName(job.jobStoreID))
    
    def delete(self, job):
        """Removes from store atomically, can not then subsequently call load(), write(), update(), etc. with the job.
        """
        os.remove(self._getJobFileName(job.jobStoreID)) #This is the atomic operation, if this file is not present the job is deleted.
        dirToRemove = job.jobStoreID
        while 1:
            head, tail = os.path.split(dirToRemove)
            if re.match("t[0-9]+$", tail):
                command = "rm -rf %s" % dirToRemove
            else:
                command = "rm -rf %s/*" % dirToRemove #We're at the root
            try:
                system(command)
            except RuntimeError:
                pass #This is not a big deal, as we expect collisions
            dirToRemove = head
            try:
                if len(os.listdir(dirToRemove)) != 0:
                    break
            except os.error: #In case stuff went wrong, but as this is not critical we let it slide
                break
    
    def transmitJobLogFile(self, jobStoreID, localLogFile):
        pass
    
    def getJobLogFile(self, jobStoreID):
        pass
            
    def getJobLogFileName(self, jobStoreID):
        return os.path.join(jobStoreID, "log.txt")
    
    def loadJobTreeState(self):
        self.jobTreeState = JobTreeState()
        return self._loadJobTreeState(self._getJobFileDirName())
    
    def processFinishedJob(self, jobStoreID, resultStatus):
        """Function reads a processed job file and updates it state.
        """
        jobFile = self._getJobFileName(jobStoreID)
        updatingFilePresent = self._processAnyUpdatingFile(jobFile)
        newFilePresent = self._processAnyNewFile(jobFile)
        if os.path.exists(self.getJobLogFileName(jobStoreID)):
            logger.critical("The job seems to have left a log file, indicating failure: %s", jobStoreID)
            logFile(self.getJobLogFileName(jobStoreID), logger.critical)
        
        if self.exists(jobStoreID):
            job = self.load(jobStoreID)
            assert job not in self.jobTreeState.updatedJobs
            if resultStatus != 0 or newFilePresent or updatingFilePresent:
                if not os.path.exists(self.getJobLogFileName(job.jobStoreID)):
                    logger.critical("No log file is present, despite job failing: %s", jobStoreID)
                job.setupJobAfterFailure(self.config)
            if len(job.followOnCommands) > 0 or len(job.children) > 0:
                self.jobTreeState.updatedJobs.add(job) #Now we know the job is done we can add it to the list of updated job files
                logger.debug("Added job: %s to active jobs" % jobStoreID)
            else:
                for message in job.messages: #This is here because jobs with no children or follow ons may log to master.
                    logger.critical("Got message from job at time: %s : %s" % (time.strftime("%m-%d-%Y %H:%M:%S"), message))
                logger.debug("Job has no follow-ons or children despite job file being present so we'll consider it done: %s" % jobStoreID)
                self._updateParentStatus(jobStoreID)
        else:  #The job is done
            if resultStatus != 0:
                logger.critical("Despite the batch system claiming failure the job %s seems to have finished and been removed" % jobFile)
            self._updateParentStatus(jobStoreID)
            
    def _updateParentStatus(self, jobStoreID):
        """Update status of parent for finished child job.
        """
        while True:
            if jobStoreID not in self.jobTreeState.childJobStoreIdToParentJob:
                assert len(self.jobTreeState.updatedJobs) == 0
                assert len(self.jobTreeState.childJobStoreIdToParentJob) == 0
                assert len(self.jobTreeState.childCounts) == 0
                break
            parentJob = self.jobTreeState.childJobStoreIdToParentJob.pop(jobStoreID)
            self.jobTreeState.childCounts[parentJob] -= 1
            assert self.jobTreeState.childCounts[parentJob] >= 0
            if self.jobTreeState.childCounts[parentJob] == 0: #Job is done
                self.jobTreeState.childCounts.pop(parentJob)
                logger.debug("Parent job %s has all its children run successfully", parentJob.jobStoreID)
                assert parentJob not in self.jobTreeState.updatedJobs
                if len(parentJob.followOnCommands) > 0:
                    self.jobTreeState.updatedJobs.add(parentJob) #Now we know the job is done we can add it to the list of updated job files
                    break
                else:
                    jobStoreID = parentJob.jobStoreID
            else:
                break
    
    ####
    #Private methods
    ####
    
    def _getJobFileDirName(self):
        return os.path.join(self.config.attrib["job_tree"], "jobs")
    
    def _getJobFileName(self, jobStoreID):
        return os.path.join(jobStoreID, "job")
    
    def _loadJobTreeState2(self, jobTreeJobsRoot):
        #Read job
        job = self.load(jobTreeJobsRoot)
        #Reset the job
        job.messages = []
        job.children = []
        job.remainingRetryCount = int(self.config.attrib["try_count"])
        #Get children
        childJobs = reduce(lambda x,y:x+y, map(lambda childDir : \
            self._loadJobTreeState(childDir), JobDB._listChildDirs(jobTreeJobsRoot)), [])
        if len(childJobs) > 0:
            self.jobTreeState.childCounts[job] = len(childJobs)
            for childJob in childJobs:
                self.jobTreeState.childJobStoreIdToParentJob[childJob.jobStoreID] = job
        elif len(job.followOnCommands) > 0:
            self.jobTreeState.updatedJobs.add(job)
        else: #Job is stub with nothing left to do, so ignore
            self.jobTreeState.shellJobs.add(job)
            return []
        return [ job ]
    
    def _loadJobTreeState(self, jobTreeJobsRoot):
        jobFile = self._getJobFileName(jobTreeJobsRoot)
        if self._processAnyUpdatingFile(jobFile) or self._processAnyNewFile(jobFile) or os.path.exists(jobFile):
            return self._loadJobTreeState2(jobTreeJobsRoot)
        return reduce(lambda x,y:x+y, map(lambda childDir : \
            self._loadJobTreeState2(childDir), JobDB._listChildDirs(jobTreeJobsRoot)), [])
    
    def _processAnyUpdatingFile(self, jobFile):
        if os.path.isfile(jobFile + ".updating"):
            logger.critical("There was an .updating file for job: %s" % jobFile)
            if os.path.isfile(jobFile + ".new"): #The job failed while writing the updated job file.
                logger.critical("There was a .new file for the job: %s" % jobFile)
                os.remove(jobFile + ".new") #The existance of the .updating file means it wasn't complete
            for f in JobDB._listChildDirs(os.path.split(jobFile)[0]):
                logger.critical("Removing broken child %s\n" % f)
                system("rm -rf %s" % f)
            assert os.path.isfile(jobFile)
            os.remove(jobFile + ".updating") #Delete second the updating file second to preserve a correct state
            logger.critical("We've reverted to the original job file: %s" % jobFile)
            return True
        return False
    
    def _processAnyNewFile(self, jobFile):
        if os.path.isfile(jobFile + ".new"): #The job was not properly updated before crashing
            logger.critical("There was a .new file for the job and no .updating file %s" % jobFile)
            if os.path.isfile(jobFile):
                os.remove(jobFile)
            os.rename(jobFile + ".new", jobFile)
            return True
        return False
            
    def _write(self, job, suffix=""):
        fileHandle = open(self._getJobFileName(job.jobStoreID) + suffix, 'w')
        pickler.dump(job.convertJobToJson(), fileHandle)
        fileHandle.close()
    
    def _createTempDirectories(self, rootDir, number, filesPerDir=4):
        def fn(i):
            dirName = os.path.join(rootDir, "t%i" % i)
            os.mkdir(dirName)
            return dirName
        if number > filesPerDir:
            if number % filesPerDir != 0:
                return reduce(lambda x,y:x+y, [ self._createTempDirectories(fn(i+1), number/filesPerDir, filesPerDir) \
        for i in range(filesPerDir-1) ], self._createTempDirectories(fn(0), (number % filesPerDir) + number/filesPerDir, filesPerDir)) 
            else:
                return reduce(lambda x,y:x+y, [ self._createTempDirectories(fn(i+1), number/filesPerDir, filesPerDir) \
                                               for i in range(filesPerDir) ], []) 
        else:
            return [ fn(i) for i in xrange(number) ]
    
    @staticmethod
    def _listChildDirsUnsafe(jobDir):
        """Directories of child jobs for given job (not recursive).
        """
        return [ os.path.join(jobDir, f) for f in os.listdir(jobDir) if re.match("t[0-9]+$", f) ]
    
    @staticmethod
    def _listChildDirs(jobDir):
        try:
            return JobDB._listChildDirsUnsafe(jobDir)
        except:
            logger.info("Encountered error while parsing job dir %s, so we will ignore it" % jobDir)
        return []

class Job:
    def __init__(self, command, memory, cpu, tryCount, jobStoreID):
        self.remainingRetryCount = tryCount
        self.jobStoreID = jobStoreID
        self.children = []
        self.followOnCommands = []
        self.followOnCommands.append((command, memory, cpu, 0))
        self.messages = []
        
    def getGlobalTempDirName(self):
        return os.path.join(self.jobStoreID, "gTD")
        
    def setupJobAfterFailure(self, config):
        if len(self.followOnCommands) > 0:
            self.remainingRetryCount = max(0, self.remainingRetryCount-1)
            logger.critical("Due to failure we are reducing the remaining retry count of job %s to %s" % (self.jobStoreID, self.remainingRetryCount))
            #Set the default memory to be at least as large as the default, in case this was a malloc failure (we do this because of the combined
            #batch system)
            self.followOnCommands[-1] = (self.followOnCommands[-1][0], max(self.followOnCommands[-1][1], float(config.attrib["default_memory"]))) + self.followOnCommands[-1][2:]
            logger.critical("We have set the default memory of the failed job to %s bytes" % self.followOnCommands[-1][1])
        else:
            logger.critical("The job %s has no follow on jobs to reset" % self.jobStoreID)
            
    def convertJobToJson(job):
        jsonJob = [ job.remainingRetryCount,
                    job.jobStoreID,
                    job.children,
                    job.followOnCommands,
                    job.messages ]
        return jsonJob
    
    @staticmethod
    def convertJsonJobToJob(jsonJob):
        job = Job("", 0, 0, 0, None)
        job.remainingRetryCount = jsonJob[0] 
        job.jobStoreID = jsonJob[1]
        job.children = jsonJob[2] 
        job.followOnCommands = jsonJob[3] 
        job.messages = jsonJob[4] 
        return job
