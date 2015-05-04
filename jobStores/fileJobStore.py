import marshal as pickler
#import cPickle as pickler
#import pickle as pickler
#import json as pickler    
import socket
import random
import shutil
import os
import re
from sonLib.bioio import logger, makeSubDir, getTempFile, system, absSymPath
from jobTree.jobStores.abstractJobStore import AbstractJobStore, JobTreeState
from jobTree.src.job import Job

class FileJobStore(AbstractJobStore):
    """Represents the jobTree on using a network file system. For doc-strings
    of functions see AbstractJobStore.
    """

    def __init__(self, jobStoreDir, config=None):
        self.jobStoreDir = absSymPath(jobStoreDir)
        logger.info("Jobstore directory is: %s" % self.jobStoreDir)
        if not os.path.exists(self.jobStoreDir):
            os.mkdir(self.jobStoreDir)
        super( FileJobStore, self ).__init__( config=config )
        self._setupStatsDirs(create=config is not None)
    
    def createFirstJob(self, command, memory, cpu):
        if not os.path.exists(self._getJobFileDirName()):
            os.mkdir(self._getJobFileDirName())
        return self._makeJob(command, memory, cpu, self._getJobFileDirName())
    
    def exists(self, jobStoreID):
        return os.path.exists(self._getJobFileName(jobStoreID))
    
    def load(self, jobStoreID):
        jobFile = self._getJobFileName(jobStoreID)
        # The following clean up any issues resulting from the failure of the job during writing
        # by the batch system.
        updatingFilePresent = self._processAnyUpdatingFile(jobFile)
        newFilePresent = self._processAnyNewFile(jobFile)
        # Now load the job
        with open(jobFile, 'r') as fileHandle:
            job = Job.fromList(pickler.load(fileHandle))
        # Deal with failure by lowering the retry limit
        if updatingFilePresent or newFilePresent:
            job.setupJobAfterFailure(self.config)
        return job   
    
    def store(self, job):
        self._write(job, ".new")
        os.rename(self._getJobFileName(job.jobStoreID) + ".new", self._getJobFileName(job.jobStoreID))

    def addChildren(self, job, childCommands):
        updatingFile = self._getJobFileName(job.jobStoreID) + ".updating"
        open(updatingFile, 'w').close()
        for ((command, memory, cpu), tempDir) in zip(childCommands, \
                    self._createTempDirectories(job.jobStoreID, len(childCommands))):
            childJob = self._makeJob(command, memory, cpu, tempDir)
            self.store(childJob)
            job.children.append((childJob.jobStoreID, memory, cpu))
        self._write(job, ".new")
        os.remove(updatingFile)
        os.rename(self._getJobFileName(job.jobStoreID) + ".new", self._getJobFileName(job.jobStoreID))
    
    def delete(self, job):
        os.remove(self._getJobFileName(job.jobStoreID)) #This is the atomic operation, if this file is not present the job is deleted.
        dirToRemove = job.jobStoreID
        # FIXME: could we use shutil.rmtree here? It'll be significantly faster, especially
        # considering that system() launches a full shell
        while 1:
            head, tail = os.path.split(dirToRemove)
            if re.match("t[0-9]+$", tail):
                command = "rm -rf %s" % dirToRemove
            else:
                command = "rm -rf %s/*" % dirToRemove #We're at the root
            try:
                system(command)
            except RuntimeError:
                # FIXME: This dangerous, we should be as specific as possible with the expected exception
                pass #This is not a big deal, as we expect collisions
            dirToRemove = head
            try:
                if len(os.listdir(dirToRemove)) != 0:
                    break
            except os.error: #In case stuff went wrong, but as this is not critical we let it slide
                # FIXME: should still log a warning-level message
                break
    
    def loadJobTreeState(self):
        jobTreeState = JobTreeState()
        if not os.path.exists(self._getJobFileDirName()):
            return jobTreeState
        jobTreeState.started = True
        self._loadJobTreeState(self._getJobFileDirName(), jobTreeState)
        return jobTreeState
    
    def writeFile(self, jobStoreID, localFilePath):
        if not os.path.exists(jobStoreID):
            raise RuntimeError("JobStoreID %s does not exist" % jobStoreID)
        if not os.path.isdir(jobStoreID):
            raise RuntimeError("Path %s is not a dir" % jobStoreID)
        jobStoreFileID = getTempFile(".tmp", os.path.join(jobStoreID, "g"))
        shutil.copyfile(localFilePath, jobStoreFileID)
        return jobStoreFileID
    
    def updateFile(self, jobStoreFileID, localFilePath):
        if not os.path.exists(jobStoreFileID):
            raise RuntimeError("File %s does not exist" % jobStoreFileID)
        if not os.path.isfile(jobStoreFileID):
            raise RuntimeError("Path %s is not a file" % jobStoreFileID)
        shutil.copyfile(localFilePath, jobStoreFileID)
    
    def readFile(self, jobStoreFileID, localFilePath):
        if not os.path.exists(jobStoreFileID):
            raise RuntimeError("File %s does not exist" % jobStoreFileID)
        if not os.path.isfile(jobStoreFileID):
            raise RuntimeError("Path %s is not a file" % jobStoreFileID)
        shutil.copyfile(jobStoreFileID, localFilePath)
    
    def deleteFile(self, jobStoreFileID):
        if not os.path.exists(jobStoreFileID):
            raise RuntimeError("File %s does not exist" % jobStoreFileID)
        os.remove(jobStoreFileID)
        
    def writeFileStream(self, jobStoreID):
        jobStoreFileID = getTempFile(".tmp", rootDir=os.path.join(jobStoreID, "g"))
        return open(jobStoreFileID, 'w'), jobStoreFileID
    
    def updateFileStream(self, jobStoreFileID):
        if not os.path.exists(jobStoreFileID):
            raise RuntimeError("File %s does not exist" % jobStoreFileID)
        if not os.path.isfile(jobStoreFileID):
            raise RuntimeError("Path %s is not a file" % jobStoreFileID)
        return open(jobStoreFileID, 'w')
    
    def getEmptyFileStoreID(self, jobStoreID):
        fileHandle, jobStoreFileID = self.writeFileStream(jobStoreID)
        fileHandle.close()
        return jobStoreFileID
    
    def readFileStream(self, jobStoreFileID):
        if not os.path.exists(jobStoreFileID):
            raise RuntimeError("File %s does not exist" % jobStoreFileID)
        return open(jobStoreFileID, 'r')
    
    def writeSharedFileStream(self, sharedFileName):
        assert self._validateSharedFileName( sharedFileName )
        return open( os.path.join( self.jobStoreDir, sharedFileName ), 'w' )
    
    def readSharedFileStream(self, sharedFileName):
        assert self._validateSharedFileName( sharedFileName )
        return open(os.path.join(self.jobStoreDir, sharedFileName), 'r')
    
    def writeStats(self, statsString):
        tempStatsFile = os.path.join(random.choice(self.statsDirs), \
                            "%s_%s.xml" % (socket.gethostname(), os.getpid()))
        fileHandle = open(tempStatsFile + ".new", "w")
        fileHandle.write(statsString)
        fileHandle.close()
        os.rename(tempStatsFile + ".new", tempStatsFile) #This operation is atomic
    
    def readStats(self, fileHandle):
        numberOfFilesProcessed = 0
        for dir in self.statsDirs:
            for tempFile in os.listdir(dir):
                if tempFile[-3:] != "new":
                    absTempFile = os.path.join(dir, tempFile)
                    fH = open(absTempFile, 'r')
                    for line in fH.readlines():
                        fileHandle.write(line)
                    fH.close()
                    os.remove(absTempFile)
                    numberOfFilesProcessed += 1
        return numberOfFilesProcessed
    
    ####
    #Private methods
    ####

    def _makeJob(self, command, memory, cpu, jobDir):
        # Sub directory to put temporary files associated with the job in
        makeSubDir(os.path.join(jobDir, "g"))
        return Job.create(command=command, memory=memory, cpu=cpu,
                          tryCount=self._defaultTryCount( ), jobStoreID=jobDir, logJobStoreFileID=None)
    
    def _getJobFileDirName(self):
        return os.path.join(self.jobStoreDir, "jobs")
    
    def _getJobFileName(self, jobStoreID):
        return os.path.join(jobStoreID, "job")
    
    def _loadJobTreeState2(self, jobTreeJobsRoot, jobTreeState):
        #Read job
        job = self.load(jobTreeJobsRoot)
        #Reset the job
        job.messages = []
        job.children = []
        job.remainingRetryCount = self._defaultTryCount( )
        #Get children
        childJobs = reduce(lambda x,y:x+y, map(lambda childDir : \
            self._loadJobTreeState(childDir, jobTreeState), FileJobStore._listChildDirs(jobTreeJobsRoot)), [])
        if len(childJobs) > 0:
            jobTreeState.childCounts[job] = len(childJobs)
            for childJob in childJobs:
                jobTreeState.childJobStoreIdToParentJob[childJob.jobStoreID] = job
        elif len(job.followOnCommands) > 0:
            jobTreeState.updatedJobs.add(job)
        else: #Job is stub with nothing left to do, so ignore
            jobTreeState.shellJobs.add(job)
            return []
        return [ job ]
    
    def _loadJobTreeState(self, jobTreeJobsRoot, jobTreeState):
        jobFile = self._getJobFileName(jobTreeJobsRoot)
        if os.path.exists(jobFile):
            return self._loadJobTreeState2(jobTreeJobsRoot, jobTreeState)
        return reduce(lambda x,y:x+y, map(lambda childDir : \
            self._loadJobTreeState2(childDir, jobTreeState), \
            FileJobStore._listChildDirs(jobTreeJobsRoot)), [])
    
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
        # The job was not properly updated before crashing
        if os.path.isfile(jobFile + ".new"):
            logger.critical("There was a .new file for the job and no .updating file %s" % jobFile)
            if os.path.isfile(jobFile):
                os.remove(jobFile)
            os.rename(jobFile + ".new", jobFile)
            return True
        return False
            
    def _write(self, job, suffix=""):
        fileHandle = open(self._getJobFileName(job.jobStoreID) + suffix, 'w')
        pickler.dump(job.toList(), fileHandle)
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
    
    ##Stats private methods
    
    def _setupStatsDirs(self, create=False):
        #Temp dirs
        def fn(dir, subDir):
            absSubDir = os.path.join(dir, str(subDir))
            if create and not os.path.exists(absSubDir):
                os.mkdir(absSubDir)
            return absSubDir
        statsDir = fn(self.jobStoreDir, "stats")
        self.statsDirs = reduce(lambda x,y: x+y, [ [ fn(absSubDir, subSubDir) \
                for subSubDir in xrange(10) ] \
                for absSubDir in [ fn(statsDir, subDir) for subDir in xrange(10) ] ], [])
