from contextlib import contextmanager
import logging
import marshal as pickler
#import cPickle as pickler
#import pickle as pickler
#import json as pickler    
import random
import shutil
import os
import errno
import tempfile
from jobTree.lib.bioio import system, absSymPath
from jobTree.jobStores.abstractJobStore import AbstractJobStore, NoSuchJobException, \
    NoSuchFileException
from jobTree.job import Job

logger = logging.getLogger( __name__ )

class FileJobStore(AbstractJobStore):
    """Represents the jobTree using a network file system. For doc-strings
    of functions see AbstractJobStore.
    """

    def __init__(self, jobStoreDir, config=None):
        self.jobStoreDir = absSymPath(jobStoreDir)
        logger.info("Jobstore directory is: %s" % self.jobStoreDir)
        self.tempFilesDir = os.path.join(self.jobStoreDir, "tmp")
        if not os.path.exists(self.jobStoreDir):
            os.mkdir(self.jobStoreDir)
            os.mkdir(self.tempFilesDir)
        super( FileJobStore, self ).__init__( config=config )
        #Parameters for creating temporary files
        self.validDirs = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        self.levels = 2
    
    def deleteJobStore(self):
        system("rm -rf %s" % self.jobStoreDir)
    
    ##########################################
    #The following methods deal with creating/loading/updating/writing/checking for the
    #existence of jobs
    ########################################## 
    
    def create(self, command, memory, cpu, updateID=None,
               predecessorNumber=0):
        #The absolute path to the job directory and the path relative to the
        #self.jobStoreDir directory, the latter serving as the jobStoreID.  
        #Gets a valid temporary directory in which to create a job.    
        absTempDir = tempFile.mkdtemp(prefix="job", dir=self._getTempSharedDir())
        relativeJobDir = self._getRelativePath(absTempDir)
        # Sub directory to put temporary files associated with the job in
        os.mkdir(os.path.join(absJobDir, "g"))
        #Make the job
        job = Job(command=command, memory=memory, cpu=cpu, 
                  jobStoreID=relativeJobDir, tryCount=self._defaultTryCount( ), 
                  updateID=updateID,
                  predecessorNumber=predecessorNumber)
        #Write job file to disk
        self.update(job)
        return job
    
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
        self._checkJobStoreId(jobStoreID)
        jobFile = self._getJobFileName(jobStoreID)
        # Now load a valid version of the job
        try:
            with open(jobFile, 'r') as fileHandle:
                job = Job.fromList(pickler.load(fileHandle))
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise NoSuchJobException( jobStoreID )
            else:
                raise
        #The following cleans up any issues resulting from the failure of the 
        #job during writing by the batch system.
        if os.path.isfile(jobFile + ".new"):
            logger.warn("There was a .new file for the job" % jobFile)
            os.remove(os.path.isfile(jobFile + ".new"))
            job.setupJobAfterFailure(self.config)
        return job
    
    def update(self, job):
        #The job is serialised to a file suffixed by ".new"
        #The file is then moved to its correct path.
        #Atomicity guarantees use the fact the underlying file systems "move file"
        #function is atomic. 
        with open(self._getJobFileName(job.jobStoreID) + ".new", 'w') as f:
            pickler.dump(job.toList(), fileHandle)
        os.rename(self._getJobFileName(job.jobStoreID) + ".new", self._getJobFileName(job.jobStoreID))
    
    def delete(self, jobStoreID):
        #The jobStoreID is the relative path to the directory containing the job,
        #removing this directory deletes the job.
        if self.exists(jobStoreID):
            system("rm -rf %s" % self._getAbsPath(jobStoreID))
 
    def jobs(self):
        #Walk through list of temporary directories searching for jobs
        for dir in self._tempDirectories():
            for i in os.listdir(path):
                if i.startswith( 'job' ):
                    yield self.load(os.path.join(dir, i))
 
    ##########################################
    #Functions that deal with temporary files associated with jobs
    ##########################################    
    
    def writeFile(self, jobStoreID, localFilePath):
        self._checkJobStoreId(jobStoreID)
        absPath = self._getJobTempFile(jobStoreID)
        shutil.copyfile(localFilePath, absPath)
        return self._getRelativePath(absPath)

    def updateFile(self, jobStoreFileID, localFilePath):
        self._checkJobStoreFileID(jobStoreFileID)
        shutil.copyfile(localFilePath, self._getAbsPath(jobStoreFileID))
    
    def readFile(self, jobStoreFileID, localFilePath):
        self._checkJobStoreFileID(jobStoreFileID)
        shutil.copyfile(self._getAbsPath(jobStoreFileID), localFilePath)
    
    def deleteFile(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        os.remove(self._getAbsPath(jobStoreFileID))
    
    @contextmanager
    def writeFileStream(self, jobStoreID):
        self._checkJobStoreId(jobStoreID)
        absPath = self._getJobTempFile(jobStoreID)
        with open(absPath, 'w') as f:
            yield f, self._getRelativePath(absPath)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        # File objects are context managers (CM) so we could simply return what open returns.
        # However, it is better to wrap it in another CM so as to prevent users from accessing
        # the file object directly, without a with statement.
        with open(self._getAbsPath(jobStoreFileID), 'w') as f:
            yield f
    
    def getEmptyFileStoreID(self, jobStoreID):
        with self.writeFileStream(jobStoreID) as ( fileHandle, jobStoreFileID ):
            return jobStoreFileID
    
    @contextmanager
    def readFileStream(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        with open(self._getAbsPath(jobStoreFileID), 'r') as f:
            yield f
            
    ##########################################
    #The following methods deal with shared files, i.e. files not associated 
    #with specific jobs.
    ##########################################  

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
        #Temporary files are placed in the set of temporary files/directoies
        tempStatsFile = tempFile.mkstemp(prefix="stats", dir=self._getTempSharedDir())
        with open(tempStatsFile + ".new", "w") as f:
            f.write(statsAndLoggingString)
        os.rename(tempStatsFile + ".new", tempStatsFile) #This operation is atomic
    
    def readStatsAndLogging( self, statsAndLoggingCallBackFn):
        numberOfFilesProcessed = 0
        for dir in self._tempDirectories():
            for tempFile in os.listdir(dir):
                if tempFile.startswith( 'stats' ):
                    if not tempFile.endswith( '.new' ):
                        absTempFile = os.path.join(dir, tempFile)
                        with open(absTempFile, 'r') as fH:
                            statsAndLoggingCallBackFn(fH)
                        numberOfFilesProcessed += 1
                    os.remove(absTempFile)
        return numberOfFilesProcessed
    
    ##########################################
    #Private methods
    ##########################################   
        
    def _getAbsPath(self, relativePath):
        """
        :rtype : string, string is the absolute path to a file path relative
        to the self.tempFilesDir.
        """
        return os.path.join(self.tempFilesDir, jobStoreID)
    
    def _getRelativePath(self, absPath):
        """
        absPath  is the absolute path to a file in the store,.
        
        :rtype : string, string is the path to the absPath file relative to the 
        self.tempFilesDir
        
        """
        return absPath[:len(self.tempFilesDir)+1]
    
    def _getJobFileName(self, jobStoreID):
        """
        :rtype : string, string is the file containing the serialised Job.Job instance
        for the given job.
        """
        return os.path.join(self._getAbsPath(jobStoreID), "job")

    def _getJobTempFile(self, jobStoreID):
        """
        :rtype : string, string is absolute path to a temporary file within
        the given job's (referenced by jobStoreID's) temporary file directory.
        """
        tempfile.mkstemp(suffix=".tmp", dir=os.path.join(self._getAbsPath(jobStoreID), "g"))
    
    def _checkJobStoreId(self, jobStoreID):
        """
        Raises a RuntimeError if the jobStoreID does not exist.
        """
        if not self.exists(jobStoreID):
            raise RuntimeError("JobStoreID %s does not exist" % jobStoreID)
    
    def _checkJobStoreFileID(self, jobStoreFileID):
        """
        Raises RuntimeError if the jobStoreFileID does not exist or is not a file.
        """
        absPath = os.path.join(self.tempFilesDir, jobStoreFileID)
        if not os.path.exists(absPath):
            raise RuntimeError("File %s does not exist in jobStore" % jobStoreFileID)
        if not os.path.isfile(absPath):
            raise RuntimeError("Path %s is not a file in the jobStore" % jobStoreFileID) 
    
    def _getTempSharedDir(self):
        """
        Gets a temporary directory in the hierarchy of directories in self.tempFilesDir.
        This directory may contain multiple shared jobs/files.
        
        :rtype : string, path to temporary directory in which to place files/directories.
        """
        dir = self.tempFilesDir
        for i in xrange(levels):
            dir = os.path.join(dir, random.choice(self.validDirs))
            if not os.path.exists(dir):
                os.mkdir(dir)
        return dir
     
    def _tempDirectories(self):
        """
        :rtype : an iterator to the temporary directories containing jobs/stats files
        in the hierarchy of directories in self.tempFilesDir
        """
        def _dirs(path, levels):
            if levels > 0:
                for subDir in os.listdir(path):
                    for job in _dirs(os.path.join(path, subDir), levels-1):
                        yield path
            else:
                yield path
        for dir in _dirs(self.tempFilesDir, self.levels):
            yield dir
