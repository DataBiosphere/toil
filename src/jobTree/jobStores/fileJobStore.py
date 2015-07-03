from contextlib import contextmanager
import logging
import marshal as pickler
#import cPickle as pickler
#import pickle as pickler
#import json as pickler
import socket
import random
import shutil
import os
import re
import errno
from jobTree.lib.bioio import makeSubDir, getTempFile, system, absSymPath
from jobTree.jobStores.abstractJobStore import AbstractJobStore, JobTreeState, NoSuchJobException, \
    NoSuchFileException
from jobTree.job import Job

logger = logging.getLogger( __name__ )


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

    def getPublicUrl( self,  jobStoreFileID):
        if os.path.exists(jobStoreFileID):
            return 'file:'+jobStoreFileID
        else:
            raise NoSuchFileException(jobStoreFileID)

    def getSharedPublicUrl( self,  FileName):
        return self.getPublicUrl(jobStoreFileID=self.jobStoreDir +'/'+FileName)

    def load(self, jobStoreID):
        jobFile = self._getJobFileName(jobStoreID)
        # The following clean up any issues resulting from the failure of the job during writing
        # by the batch system.
        updatingFilePresent = self._processAnyUpdatingFile(jobFile)
        newFilePresent = self._processAnyNewFile(jobFile)
        # Now load the job
        try:
            with open(jobFile, 'r') as fileHandle:
                job = Job.fromList(pickler.load(fileHandle))
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise NoSuchJobException( jobStoreID )
            else:
                raise
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
        try:
            # This is the atomic operation, if this file is not present the job is deleted.
            os.remove( self._getJobFileName( job.jobStoreID ) )
        except OSError as e:
            if e.errno == errno.ENOENT:
                pass
            else:
                raise

        dirToRemove = job.jobStoreID
        # FIXME: could we use shutil.rmtree here? It'll be significantly faster, especially ...
        # FIXME: ... considering that system() launches a full shell
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
            
    ##TO BE DELETED
    def loadJobTreeState(self):
        jobTreeState = JobTreeState()
        if not os.path.exists(self._getJobFileDirName()):
            return jobTreeState
        jobTreeState.started = True
        self._loadJobTreeState(self._getJobFileDirName(), jobTreeState)
        return jobTreeState
    ##END TO BE DELETED
    
    def loadJobsInStore(self):
        if not os.path.exists(self._getJobFileDirName()): #Case dir does not yet exist
            return []
        jobs = []
        def _loadJobs(jobTreeJobsRoot):
            #Fn recursively walk over hierarchy of jobs and parses the jobs files.
            jobFile = self._getJobFileName(jobTreeJobsRoot)
            if os.path.exists(jobFile):
                job = self.load(jobTreeJobsRoot)
                jobs.append(job)
            for childDir in FileJobStore._listChildDirs(jobTreeJobsRoot):
                _loadJobs(childDir)
        _loadJobs(self._getJobFileDirName())
        return jobs
    
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
        
    @contextmanager
    def writeFileStream(self, jobStoreID):
        jobStoreFileID = getTempFile(".tmp", rootDir=os.path.join(jobStoreID, "g"))
        with open(jobStoreFileID, 'w') as f:
            yield f, jobStoreFileID

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        if not os.path.exists(jobStoreFileID):
            raise RuntimeError("File %s does not exist" % jobStoreFileID)
        if not os.path.isfile(jobStoreFileID):
            raise RuntimeError("Path %s is not a file" % jobStoreFileID)
        # File objects are context managers (CM) so we could simply return what open returns.
        # However, it is better to wrap it in another CM so as to prevent users from accessing
        # the file object directly, without a with statement.
        with open(jobStoreFileID, 'w') as f:
            yield f
    
    def getEmptyFileStoreID(self, jobStoreID):
        with self.writeFileStream(jobStoreID) as ( fileHandle, jobStoreFileID ):
            return jobStoreFileID
    
    @contextmanager
    def readFileStream(self, jobStoreFileID):
        try:
            with open(jobStoreFileID, 'r') as f:
                yield f
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise NoSuchFileException( jobStoreFileID )
            else:
                raise

    @contextmanager
    def writeSharedFileStream(self, sharedFileName):
        assert self._validateSharedFileName( sharedFileName )
        with open( os.path.join( self.jobStoreDir, sharedFileName ), 'w' ) as f:
            yield f

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        assert self._validateSharedFileName( sharedFileName )
        with open(os.path.join(self.jobStoreDir, sharedFileName), 'r') as f:
            yield f
    
    def writeStatsAndLogging(self, statsAndLoggingString):
        tempStatsFile = os.path.join(random.choice(self.statsDirs), \
                            "%s_%s.xml" % (socket.gethostname(), os.getpid()))
        fileHandle = open(tempStatsFile + ".new", "w")
        fileHandle.write(statsAndLoggingString)
        fileHandle.close()
        os.rename(tempStatsFile + ".new", tempStatsFile) #This operation is atomic
    
    def readStatsAndLogging( self, statsAndLoggingCallBackFn):
        numberOfFilesProcessed = 0
        for dir in self.statsDirs:
            for tempFile in os.listdir(dir):
                if not tempFile.endswith( '.new' ):
                    absTempFile = os.path.join(dir, tempFile)
                    with open(absTempFile, 'r') as fH:
                        statsAndLoggingCallBackFn(fH)
                    os.remove(absTempFile)
                    numberOfFilesProcessed += 1
        return numberOfFilesProcessed
    
    def deleteJobStore(self):
        """
        Removes the jobStore from the disk/store. Careful!
        """
        system("rm -rf %s" % self.jobStoreDir)
    
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
        
    ##START TO BE DELETED WHEN LOAD JOBTREE STATE IS REMOVED    
    def _loadJobTreeState2(self, jobTreeJobsRoot, jobTreeState):
        #Read job
        job = self.load(jobTreeJobsRoot)
        # FIXME: This is not a good place to do this. Firstly, this behaviour is not documented
        # FIXME: ... in the abstract superclass. Secondly, this is behavior shared by all
        # FIXME: ... implementations so it would be nice if it were factored out, either in the
        # FIXME: ... caller or in the superclass.
        # Reset the job
        job.children = []
        job.remainingRetryCount = self._defaultTryCount( )
        #Get children
        childJobs = reduce(lambda x,y:x+y, map(lambda childDir : 
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
    ##END TO BE DELETED WHEN LOAD JOBTREE STATE IS REMOVED
    
    def _processAnyUpdatingFile(self, jobFile):
        if os.path.isfile(jobFile + ".updating"):
            logger.warn("There was an .updating file for job: %s" % jobFile)
            if os.path.isfile(jobFile + ".new"): #The job failed while writing the updated job file.
                logger.warn("There was a .new file for the job: %s" % jobFile)
                os.remove(jobFile + ".new") #The existance of the .updating file means it wasn't complete
            for f in FileJobStore._listChildDirs(os.path.split(jobFile)[0]):
                logger.warn("Removing broken child %s\n" % f)
                system("rm -rf %s" % f)
            assert os.path.isfile(jobFile)
            os.remove(jobFile + ".updating") #Delete second the updating file second to preserve a correct state
            logger.warn("We've reverted to the original job file: %s" % jobFile)
            return True
        return False
    
    def _processAnyNewFile(self, jobFile):
        # The job was not properly updated before crashing
        if os.path.isfile(jobFile + ".new"):
            logger.warn("There was a .new file for the job and no .updating file %s" % jobFile)
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
                return reduce(
                    lambda x,y:x+y,
                    [ self._createTempDirectories(fn(i+1), number/filesPerDir, filesPerDir)
                        for i in range(filesPerDir-1) ],
                    self._createTempDirectories(fn(0), (number % filesPerDir) + number/filesPerDir, filesPerDir))
            else:
                return reduce(
                    lambda x,y:x+y,
                    [ self._createTempDirectories(fn(i+1), number/filesPerDir, filesPerDir) for i in range(filesPerDir) ],
                    [])
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
