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
from contextlib import contextmanager
import logging
import marshal as pickler
#import cPickle as pickler
#import pickle as pickler
#import json as pickler    
import random
import shutil
import os
import tempfile
import stat
from toil.lib.bioio import absSymPath
from toil.jobStores.abstractJobStore import AbstractJobStore, NoSuchJobException, \
    NoSuchFileException
from toil.jobWrapper import JobWrapper

logger = logging.getLogger( __name__ )

class FileJobStore(AbstractJobStore):
    """Represents the toil using a network file system. For doc-strings
    of functions see AbstractJobStore.
    """

    def __init__(self, jobStoreDir, config=None):
        """
        :param jobStoreDir: Place to create jobStore
        :param config: See jobStores.abstractJobStore.AbstractJobStore.__init__
        :raise RuntimeError: if config != None and the jobStore already exists or
        config == None and the jobStore does not already exists. 
        """
        #This is root directory in which everything in the store is kept
        self.jobStoreDir = absSymPath(jobStoreDir)
        logger.info("Jobstore directory is: %s", self.jobStoreDir)
        #Safety checks for existing jobStore
        self._checkJobStoreCreation(config != None, os.path.exists(self.jobStoreDir), self.jobStoreDir)
        #Directory where temporary files go
        self.tempFilesDir = os.path.join(self.jobStoreDir, "tmp")
        #Creation of jobStore, if necessary
        if config != None:
            os.mkdir(self.jobStoreDir)
            os.mkdir(self.tempFilesDir)  
        #Parameters for creating temporary files
        self.validDirs = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        self.levels = 2
        super( FileJobStore, self ).__init__( config=config )
        
    def deleteJobStore(self):
        if os.path.exists(self.jobStoreDir):
            shutil.rmtree(self.jobStoreDir)
    
    ##########################################
    #The following methods deal with creating/loading/updating/writing/checking for the
    #existence of jobs
    ########################################## 
    
    def create(self, command, memory, cores, disk, updateID=None,
               predecessorNumber=0):
        #The absolute path to the job directory.
        absJobDir = tempfile.mkdtemp(prefix="job", dir=self._getTempSharedDir())
        #Sub directory to put temporary files associated with the job in
        os.mkdir(os.path.join(absJobDir, "g"))
        #Make the job
        job = JobWrapper(command=command, memory=memory, cores=cores, disk=disk,
                  jobStoreID=self._getRelativePath(absJobDir), 
                  remainingRetryCount=self._defaultTryCount( ), 
                  updateID=updateID,
                  predecessorNumber=predecessorNumber)
        #Write job file to disk
        self.update(job)
        return job
    
    def exists(self, jobStoreID):
        return os.path.exists(self._getJobFileName(jobStoreID))
    
    def getPublicUrl( self,  jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        jobStorePath = self._getAbsPath(jobStoreFileID)
        if os.path.exists(jobStorePath):
            return 'file:'+jobStorePath
        else:
            raise NoSuchFileException(jobStoreFileID)

    def getSharedPublicUrl( self,  FileName):
        jobStorePath = self.jobStoreDir+'/'+FileName
        if os.path.exists(jobStorePath):
            return 'file:'+jobStorePath
        else:
            raise NoSuchFileException(FileName)

    def load(self, jobStoreID):
        self._checkJobStoreId(jobStoreID)
        #Load a valid version of the job
        jobFile = self._getJobFileName(jobStoreID)
        with open(jobFile, 'r') as fileHandle:
            job = JobWrapper.fromDict(pickler.load(fileHandle))
        #The following cleans up any issues resulting from the failure of the 
        #job during writing by the batch system.
        if os.path.isfile(jobFile + ".new"):
            logger.warn("There was a .new file for the job: %s", jobStoreID)
            os.remove(jobFile + ".new")
            job.setupJobAfterFailure(self.config)
        return job
    
    def update(self, job):
        #The job is serialised to a file suffixed by ".new"
        #The file is then moved to its correct path.
        #Atomicity guarantees use the fact the underlying file systems "move"
        #function is atomic. 
        with open(self._getJobFileName(job.jobStoreID) + ".new", 'w') as f:
            pickler.dump(job.toDict(), f)
        #This should be atomic for the file system
        os.rename(self._getJobFileName(job.jobStoreID) + ".new", self._getJobFileName(job.jobStoreID))
    
    def delete(self, jobStoreID):
        #The jobStoreID is the relative path to the directory containing the job,
        #removing this directory deletes the job.
        if self.exists(jobStoreID):
            shutil.rmtree(self._getAbsPath(jobStoreID))
 
    def jobs(self):
        #Walk through list of temporary directories searching for jobs
        for tempDir in self._tempDirectories():
            for i in os.listdir(tempDir):
                if i.startswith( 'job' ):
                    yield self.load(self._getRelativePath(os.path.join(tempDir, i)))
 
    ##########################################
    #Functions that deal with temporary files associated with jobs
    ##########################################    
    
    def writeFile(self, localFilePath, jobStoreID=None):
        fd, absPath = self._getTempFile(jobStoreID)
        shutil.copyfile(localFilePath, absPath)
        os.close(fd)
        return self._getRelativePath(absPath)
    
    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        fd, absPath = self._getTempFile(jobStoreID)
        with open(absPath, 'w') as f:
            yield f, self._getRelativePath(absPath)
        os.close(fd) #Close the os level file descriptor
        
    def getEmptyFileStoreID(self, jobStoreID=None):
        with self.writeFileStream(jobStoreID) as ( fileHandle, jobStoreFileID ):
            return jobStoreFileID

    def updateFile(self, jobStoreFileID, localFilePath):
        self._checkJobStoreFileID(jobStoreFileID)
        shutil.copyfile(localFilePath, self._getAbsPath(jobStoreFileID))
    
    def readFile(self, jobStoreFileID, localFilePath):
        self._checkJobStoreFileID(jobStoreFileID)
        shutil.copyfile(self._getAbsPath(jobStoreFileID), localFilePath)
    
    def deleteFile(self, jobStoreFileID):
        if not self.fileExists(jobStoreFileID):
            return
        os.remove(self._getAbsPath(jobStoreFileID))
        
    def fileExists(self, jobStoreFileID):
        absPath = self._getAbsPath(jobStoreFileID)
        try:
            st = os.stat(absPath)
        except os.error:
            return False
        if not stat.S_ISREG(st.st_mode):
            raise NoSuchFileException("Path %s is not a file in the jobStore" % jobStoreFileID)
        return True

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        # File objects are context managers (CM) so we could simply return what open returns.
        # However, it is better to wrap it in another CM so as to prevent users from accessing
        # the file object directly, without a with statement.
        with open(self._getAbsPath(jobStoreFileID), 'w') as f:
            yield f
    
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
    def writeSharedFileStream(self, sharedFileName, isProtected=True):
        # the isProtected parameter has no effect on the fileStore, but is needed on the awsJobStore
        assert self._validateSharedFileName( sharedFileName )
        with open( os.path.join( self.jobStoreDir, sharedFileName ), 'w' ) as f:
            yield f

    @contextmanager
    def readSharedFileStream(self, sharedFileName, isProtected=True):
        # the isProtected parameter has no effect on the fileStore, but is needed on the awsJobStore
        assert self._validateSharedFileName( sharedFileName )
        with open(os.path.join(self.jobStoreDir, sharedFileName), 'r') as f:
            yield f
             
    def writeStatsAndLogging(self, statsAndLoggingString):
        #Temporary files are placed in the set of temporary files/directoies
        fd, tempStatsFile = tempfile.mkstemp(prefix="stats", suffix=".new", dir=self._getTempSharedDir())
        with open(tempStatsFile, "w") as f:
            f.write(statsAndLoggingString)
        os.close(fd)
        os.rename(tempStatsFile, tempStatsFile[:-4]) #This operation is atomic
        
    def readStatsAndLogging( self, statsAndLoggingCallBackFn):
        numberOfFilesProcessed = 0
        for tempDir in self._tempDirectories():
            for tempFile in os.listdir(tempDir):
                if tempFile.startswith( 'stats' ):
                    absTempFile = os.path.join(tempDir, tempFile)
                    if not tempFile.endswith( '.new' ):
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
        return os.path.join(self.tempFilesDir, relativePath)
    
    def _getRelativePath(self, absPath):
        """
        absPath  is the absolute path to a file in the store,.
        
        :rtype : string, string is the path to the absPath file relative to the 
        self.tempFilesDir
        
        """
        return absPath[len(self.tempFilesDir)+1:]
    
    def _getJobFileName(self, jobStoreID):
        """
        :rtype : string, string is the file containing the serialised JobWrapper instance
        for the given job.
        """
        return os.path.join(self._getAbsPath(jobStoreID), "job")
    
    def _checkJobStoreId(self, jobStoreID):
        """
        Raises a NoSuchJobException if the jobStoreID does not exist.
        """
        if not self.exists(jobStoreID):
            raise NoSuchJobException(jobStoreID)
    
    def _checkJobStoreFileID(self, jobStoreFileID):
        """
        Raises NoSuchFileException if the jobStoreFileID does not exist or is not a file.
        """
        if not self.fileExists(jobStoreFileID):
            raise NoSuchFileException("File %s does not exist in jobStore" % jobStoreFileID)
    
    def _getTempSharedDir(self):
        """
        Gets a temporary directory in the hierarchy of directories in self.tempFilesDir.
        This directory may contain multiple shared jobs/files.
        
        :rtype : string, path to temporary directory in which to place files/directories.
        """
        tempDir = self.tempFilesDir
        for i in xrange(self.levels):
            tempDir = os.path.join(tempDir, random.choice(self.validDirs))
            if not os.path.exists(tempDir):
                try:
                    os.mkdir(tempDir)
                except os.error:
                    if not os.path.exists(tempDir): #In the case that a collision occurs and
                        #it is created while we wait then we ignore
                        raise
        return tempDir
     
    def _tempDirectories(self):
        """
        :rtype : an iterator to the temporary directories containing jobs/stats files
        in the hierarchy of directories in self.tempFilesDir
        """
        def _dirs(path, levels):
            if levels > 0:
                for subPath in os.listdir(path):
                    for i in _dirs(os.path.join(path, subPath), levels-1):
                        yield i
            else:
                yield path
        for tempDir in _dirs(self.tempFilesDir, self.levels):
            yield tempDir
            
    def _getTempFile(self, jobStoreID=None):
        """
        :rtype : file-descriptor, string, string is the absolute path to a temporary file within
        the given job's (referenced by jobStoreID's) temporary file directory. The file-descriptor
        is integer pointing to open operating system file handle. Should be closed using os.close()
        after writing some material to the file.
        """
        if jobStoreID != None:
            #Make a temporary file within the job's directory
            self._checkJobStoreId(jobStoreID)
            return tempfile.mkstemp(suffix=".tmp", 
                                dir=os.path.join(self._getAbsPath(jobStoreID), "g"))
        else:
            #Make a temporary file within the temporary file structure 
            return tempfile.mkstemp(prefix="tmp", suffix=".tmp", dir=self._getTempSharedDir())
