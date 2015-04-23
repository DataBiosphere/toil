
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
from jobTree.src.abstractJobStore import AbstractJobStore, JobTreeState
from jobTree.src.job import Job

class FileJobStore(AbstractJobStore):
    """Represents the jobTree on disk/in a db.
    """
    def __init__(self, config, create=True):
        AbstractJobStore.__init__(self, config, create)
        if create and not os.path.exists(self._getJobFileDirName()):
            os.mkdir(self._getJobFileDirName())
    
    def createFirstJob(self, command, memory, cpu):
        """Creates the root job of the jobTree from which all others must be created.
        """
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
        jobFile = self._getJobFileName(jobStoreID)
        #The following clean up any issues resulting from the failure of the job 
        #during writing by the batch system.
        updatingFilePresent = self._processAnyUpdatingFile(jobFile)
        newFilePresent = self._processAnyNewFile(jobFile)
        #Now load the job
        fileHandle = open(jobFile, 'r')
        job = Job.convertJsonJobToJob(pickler.load(fileHandle))
        fileHandle.close()
        #Deal with failure by lowering the retry limit
        if updatingFilePresent or newFilePresent:
            job.setupJobAfterFailure(self.config)
        return job
    
    def write(self, job):
        """Updates a job's status in store atomically
        """
        self._write(job, ".new")
        os.rename(self._getJobFileName(job.jobStoreID) + ".new", self._getJobFileName(job.jobStoreID))
        return job.jobStoreID
    
    def update(self, job, childCommands):
        """Creates a set of child jobs for the given job using the list of child-commands 
        and updates state of job atomically on disk with new children.
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
        """Removes from store atomically, can not then subsequently call load(), 
        write(), update(), etc. with the job.
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
            self._loadJobTreeState(childDir), FileJobStore._listChildDirs(jobTreeJobsRoot)), [])
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
        if os.path.exists(jobFile):
            return self._loadJobTreeState2(jobTreeJobsRoot)
        return reduce(lambda x,y:x+y, map(lambda childDir : \
            self._loadJobTreeState2(childDir), FileJobStore._listChildDirs(jobTreeJobsRoot)), [])
    
    def _processAnyUpdatingFile(self, jobFile):
        if os.path.isfile(jobFile + ".updating"):
            logger.critical("There was an .updating file for job: %s" % jobFile)
            if os.path.isfile(jobFile + ".new"): #The job failed while writing the updated job file.
                logger.critical("There was a .new file for the job: %s" % jobFile)
                os.remove(jobFile + ".new") #The existance of the .updating file means it wasn't complete
            for f in FileJobStore._listChildDirs(os.path.split(jobFile)[0]):
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
            return FileJobStore._listChildDirsUnsafe(jobDir)
        except:
            logger.info("Encountered error while parsing job dir %s, so we will ignore it" % jobDir)
        return []