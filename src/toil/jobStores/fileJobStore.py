# Copyright (C) 2015-2016 Regents of the University of California
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
import random
import shutil
import os
import tempfile
import stat
import errno

from bd2k.util.exceptions import require

from toil.lib.bioio import absSymPath
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             NoSuchJobException,
                                             NoSuchFileException,
                                             JobStoreExistsException,
                                             NoSuchJobStoreException)
from toil.jobWrapper import JobWrapper

logger = logging.getLogger( __name__ )


class FileJobStore(AbstractJobStore):
    """
    A job store that uses a directory on a locally attached file system. To be compatible with
    distributed batch systems, that file system must be shared by all worker nodes.
    """

    # Parameters controlling the creation of temporary files
    validDirs = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    levels = 2

    def __init__(self, path):
        """
        :param str path: Path to directory holding the job store
        """
        super(FileJobStore, self).__init__()
        self.jobStoreDir = absSymPath(path)
        logger.info("Path to job store directory is '%s'.", self.jobStoreDir)
        # Directory where temporary files go
        self.tempFilesDir = os.path.join(self.jobStoreDir, 'tmp')

    def initialize(self, config):
        try:
            os.mkdir(self.jobStoreDir)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise JobStoreExistsException(self.jobStoreDir)
            else:
                raise
        os.mkdir(self.tempFilesDir)
        super(FileJobStore, self).initialize(config)

    def resume(self):
        if not os.path.exists(self.jobStoreDir):
            raise NoSuchJobStoreException(self.jobStoreDir)
        require( os.path.isdir, "'%s' is not a directory", self.jobStoreDir)
        super(FileJobStore, self).resume()

    def destroy(self):
        if os.path.exists(self.jobStoreDir):
            shutil.rmtree(self.jobStoreDir)

    ##########################################
    # The following methods deal with creating/loading/updating/writing/checking for the
    # existence of jobs
    ########################################## 

    def create(self, command, memory, cores, disk, preemptable,
               predecessorNumber=0):
        # The absolute path to the job directory.
        absJobDir = tempfile.mkdtemp(prefix="job", dir=self._getTempSharedDir())
        # Sub directory to put temporary files associated with the job in
        os.mkdir(os.path.join(absJobDir, "g"))
        # Make the job
        job = JobWrapper(command=command, memory=memory, cores=cores, disk=disk,
                         preemptable=preemptable,
                         jobStoreID=self._getRelativePath(absJobDir),
                         remainingRetryCount=self._defaultTryCount( ),
                         predecessorNumber=predecessorNumber)
        # Write job file to disk
        self.update(job)
        return job

    def exists(self, jobStoreID):
        return os.path.exists(self._getJobFileName(jobStoreID))

    def getPublicUrl(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        jobStorePath = self._getAbsPath(jobStoreFileID)
        if os.path.exists(jobStorePath):
            return 'file:' + jobStorePath
        else:
            raise NoSuchFileException(jobStoreFileID)

    def getSharedPublicUrl(self, sharedFileName):
        jobStorePath = self.jobStoreDir + '/' + sharedFileName
        if os.path.exists(jobStorePath):
            return 'file:' + jobStorePath
        else:
            raise NoSuchFileException(sharedFileName)

    def load(self, jobStoreID):
        self._checkJobStoreId(jobStoreID)
        # Load a valid version of the job
        jobFile = self._getJobFileName(jobStoreID)
        with open(jobFile, 'r') as fileHandle:
            job = JobWrapper.fromDict(pickler.load(fileHandle))
        # The following cleans up any issues resulting from the failure of the
        # job during writing by the batch system.
        if os.path.isfile(jobFile + ".new"):
            logger.warn("There was a .new file for the job: %s", jobStoreID)
            os.remove(jobFile + ".new")
            job.setupJobAfterFailure(self.config)
        return job

    def update(self, job):
        # The job is serialised to a file suffixed by ".new"
        # The file is then moved to its correct path.
        # Atomicity guarantees use the fact the underlying file systems "move"
        # function is atomic.
        with open(self._getJobFileName(job.jobStoreID) + ".new", 'w') as f:
            pickler.dump(job.toDict(), f)
        # This should be atomic for the file system
        os.rename(self._getJobFileName(job.jobStoreID) + ".new", self._getJobFileName(job.jobStoreID))

    def delete(self, jobStoreID):
        # The jobStoreID is the relative path to the directory containing the job,
        # removing this directory deletes the job.
        if self.exists(jobStoreID):
            shutil.rmtree(self._getAbsPath(jobStoreID))

    def jobs(self):
        # Walk through list of temporary directories searching for jobs
        for tempDir in self._tempDirectories():
            for i in os.listdir(tempDir):
                if i.startswith( 'job' ):
                    try:
                        yield self.load(self._getRelativePath(os.path.join(tempDir, i)))
                    except NoSuchJobException:
                        # An orphaned job may leave an empty or incomplete job file which we can safely ignore
                        pass

    ##########################################
    # Functions that deal with temporary files associated with jobs
    ##########################################

    def _importFile(self, otherCls, url, sharedFileName=None):
        if issubclass(otherCls, FileJobStore):
            if sharedFileName is None:
                fd, absPath = self._getTempFile()
                shutil.copyfile(self._extractPathFromUrl(url), absPath)
                os.close(fd)
                return self._getRelativePath(absPath)
            else:
                self._requireValidSharedFileName(sharedFileName)
                with self.writeSharedFileStream(sharedFileName) as writable:
                    with open(self._extractPathFromUrl(url), 'r') as readable:
                        shutil.copyfileobj(readable, writable)
                return None
        else:
            return super(FileJobStore, self)._importFile(otherCls, url,
                                                         sharedFileName=sharedFileName)

    def _exportFile(self, otherCls, jobStoreFileID, url):
        if issubclass(otherCls, FileJobStore):
            shutil.copyfile(self._getAbsPath(jobStoreFileID), self._extractPathFromUrl(url))
        else:
            super(FileJobStore, self)._exportFile(otherCls, jobStoreFileID, url)

    @classmethod
    def _readFromUrl(cls, url, writable):
        with open(cls._extractPathFromUrl(url), 'r') as f:
            writable.write(f.read())

    @classmethod
    def _writeToUrl(cls, readable, url):
        with open(cls._extractPathFromUrl(url), 'w') as f:
            f.write(readable.read())

    @staticmethod
    def _extractPathFromUrl(url):
        """
        :return: local file path of file pointed at by the given URL
        """
        if url.netloc != '' and url.netloc != 'localhost':
            raise RuntimeError("The URL '%s' is invalid" % url.geturl())
        return url.netloc + url.path

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() == 'file'

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
        os.close(fd)  # Close the os level file descriptor

    def getEmptyFileStoreID(self, jobStoreID=None):
        with self.writeFileStream(jobStoreID) as (fileHandle, jobStoreFileID):
            return jobStoreFileID

    def updateFile(self, jobStoreFileID, localFilePath):
        self._checkJobStoreFileID(jobStoreFileID)
        shutil.copyfile(localFilePath, self._getAbsPath(jobStoreFileID))

    def readFile(self, jobStoreFileID, localFilePath):
        self._checkJobStoreFileID(jobStoreFileID)
        jobStoreFilePath = self._getAbsPath(jobStoreFileID)
        localDirPath = os.path.dirname(localFilePath)
        # If local file would end up on same file system as the one hosting this job store ...
        if os.stat(jobStoreFilePath).st_dev == os.stat(localDirPath).st_dev:
            # ... we can hard-link the file, ...
            try:
                os.link(jobStoreFilePath, localFilePath)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    # Overwrite existing file, emulating shutil.copyfile().
                    os.unlink(localFilePath)
                    # It would be very unlikely to fail again for same reason but possible
                    # nonetheless in which case we should just give up.
                    os.link(jobStoreFilePath, localFilePath)
                else:
                    raise
        else:
            # ... otherwise we have to copy it.
            shutil.copyfile(jobStoreFilePath, localFilePath)

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
    # The following methods deal with shared files, i.e. files not associated
    # with specific jobs.
    ##########################################  

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        # the isProtected parameter has no effect on the fileStore
        assert self._validateSharedFileName( sharedFileName )
        with open( os.path.join( self.jobStoreDir, sharedFileName ), 'w' ) as f:
            yield f

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        assert self._validateSharedFileName( sharedFileName )
        try:
            with open(os.path.join(self.jobStoreDir, sharedFileName), 'r') as f:
                yield f
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise NoSuchFileException(sharedFileName,sharedFileName)
            else:
                raise

    def writeStatsAndLogging(self, statsAndLoggingString):
        # Temporary files are placed in the set of temporary files/directoies
        fd, tempStatsFile = tempfile.mkstemp(prefix="stats", suffix=".new", dir=self._getTempSharedDir())
        with open(tempStatsFile, "w") as f:
            f.write(statsAndLoggingString)
        os.close(fd)
        os.rename(tempStatsFile, tempStatsFile[:-4])  # This operation is atomic

    def readStatsAndLogging(self, callback, readAll=False):
        numberOfFilesProcessed = 0
        for tempDir in self._tempDirectories():
            for tempFile in os.listdir(tempDir):
                if tempFile.startswith('stats'):
                    absTempFile = os.path.join(tempDir, tempFile)
                    if readAll or not tempFile.endswith('.new'):
                        with open(absTempFile, 'r') as fH:
                            callback(fH)
                        numberOfFilesProcessed += 1
                        newName = tempFile.rsplit('.', 1)[0] + '.new'
                        newAbsTempFile = os.path.join(tempDir, newName)
                        # Mark this item as read
                        os.rename(absTempFile, newAbsTempFile)
        return numberOfFilesProcessed

    ##########################################
    # Private methods
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
        Return the path to the file containing the serialised JobWrapper instance for the given
        job.

        :rtype: str
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
        :raise NoSuchFileException: if the jobStoreFileID does not exist or is not a file
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
                    if not os.path.exists(tempDir): # In the case that a collision occurs and
                        # it is created while we wait then we ignore
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
            # Make a temporary file within the job's directory
            self._checkJobStoreId(jobStoreID)
            return tempfile.mkstemp(suffix=".tmp",
                                dir=os.path.join(self._getAbsPath(jobStoreID), "g"))
        else:
            # Make a temporary file within the temporary file structure
            return tempfile.mkstemp(prefix="tmp", suffix=".tmp", dir=self._getTempSharedDir())
