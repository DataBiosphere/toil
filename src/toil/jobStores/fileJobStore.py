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

# python 2/3 compatibility
from __future__ import absolute_import
from builtins import range

# standard library
from contextlib import contextmanager
import logging
import random
import shutil
import os
import re
import tempfile
import stat
import errno
import time
try:
    import cPickle as pickle
except ImportError:
    import pickle

# toil dependencies
from toil.fileStores import FileID
from toil.lib.bioio import absSymPath
from toil.lib.misc import mkdir_p, robust_rmtree
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             NoSuchJobException,
                                             NoSuchFileException,
                                             JobStoreExistsException,
                                             NoSuchJobStoreException)
from toil.jobGraph import JobGraph

logger = logging.getLogger( __name__ )


class FileJobStore(AbstractJobStore):
    """
    A job store that uses a directory on a locally attached file system. To be compatible with
    distributed batch systems, that file system must be shared by all worker nodes.
    """

    # Valid chars for the creation of temporary "spray" directories.
    # Note that on case-insensitive filesystems we're twice as likely to use
    # letter directories as number directories.
    validDirs = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    validDirsSet = set(validDirs)

    # What prefix should be on the per-job job directories, to distinguish them
    # from the spray directories?
    JOB_DIR_PREFIX = 'instance'

    # What prefix do we put on the per-job-name directories we sort jobs into?
    JOB_NAME_DIR_PREFIX = 'kind-'

    # 10Mb RAM chunks when reading/writing files
    BUFFER_SIZE = 10485760 # 10Mb

    def __init__(self, path, fanOut=1000):
        """
        :param str path: Path to directory holding the job store
        :param int fanOut: Number of items to have in a directory before making
                           subdirectories
        """
        super(FileJobStore, self).__init__()
        self.jobStoreDir = absSymPath(path)
        logger.debug("Path to job store directory is '%s'.", self.jobStoreDir)

        # Directory where actual job files go, and their job-associated temp files
        self.jobsDir = os.path.join(self.jobStoreDir, 'jobs')
        # Directory where stats files go
        self.statsDir = os.path.join(self.jobStoreDir, 'stats')
        # Directory where non-job-associated files for the file store go
        self.filesDir = os.path.join(self.jobStoreDir, 'files/no-job')
        # Directory where job-associated files for the file store go.
        # Each per-job directory in here will have separate directories for
        # files to clean up and files to not clean up when the job is deleted.
        self.jobFilesDir = os.path.join(self.jobStoreDir, 'files/for-job')
        # Directory where shared files go
        self.sharedFilesDir = os.path.join(self.jobStoreDir, 'files/shared')

        self.fanOut = fanOut

        self.linkImports = None
        
    def __repr__(self):
        return 'FileJobStore({})'.format(self.jobStoreDir)

    def initialize(self, config):
        try:
            os.mkdir(self.jobStoreDir)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise JobStoreExistsException(self.jobStoreDir)
            else:
                raise
        mkdir_p(self.jobsDir)
        mkdir_p(self.statsDir)
        mkdir_p(self.filesDir)
        mkdir_p(self.jobFilesDir)
        mkdir_p(self.sharedFilesDir)
        self.linkImports = config.linkImports
        super(FileJobStore, self).initialize(config)

    def resume(self):
        if not os.path.isdir(self.jobStoreDir):
            raise NoSuchJobStoreException(self.jobStoreDir)
        super(FileJobStore, self).resume()

    def destroy(self):
        if os.path.exists(self.jobStoreDir):
            robust_rmtree(self.jobStoreDir)

    ##########################################
    # The following methods deal with creating/loading/updating/writing/checking for the
    # existence of jobs
    ##########################################

    def create(self, jobNode):
        # Get the job's name. We want to group jobs with the same name together.
        # This will be e.g. the function name for wrapped-function jobs.
        # Make sure to render it filename-safe
        usefulFilename = self._makeStringFilenameSafe(jobNode.jobName)

        # Make a unique temp directory under a directory for this job name,
        # possibly sprayed across multiple levels of subdirectories.
        absJobDir = tempfile.mkdtemp(prefix=self.JOB_DIR_PREFIX,
                                     dir=self._getArbitraryJobsDirForName(usefulFilename))
        # Make the job to save
        job = JobGraph.fromJobNode(jobNode, jobStoreID=self._getJobIdFromDir(absJobDir),
                                   tryCount=self._defaultTryCount())
        if hasattr(self, "_batchedJobGraphs") and self._batchedJobGraphs is not None:
            # Save it later
            self._batchedJobGraphs.append(job)
        else:
            # Save it now
            self.update(job)
        return job

    @contextmanager
    def batch(self):
        self._batchedJobGraphs = []
        yield
        for jobGraph in self._batchedJobGraphs:
            self.update(jobGraph)
        self._batchedJobGraphs = None

    def waitForExists(self, jobStoreID, maxTries=35, sleepTime=1):
        """Spin-wait and block for a file to appear before returning False if it does not.
        
        The total max wait time is maxTries * sleepTime. The current default is
        tuned to match Linux NFS defaults where the client's cache of the directory
        listing on the server is supposed to become coherent within 30 sec.
        Delayes beyond that would probably indicate a pathologically slow file system
        that just should not be used for the jobStore.
        
        The warning will be sent to the log only on the first retry.
        
        In practice, the need for retries happens rarely, but it does happen
        over the course of large workflows with a jobStore on a busy NFS."""
        for iTry in range(1,maxTries+1):
            jobFile = self._getJobFileName(jobStoreID)
            if os.path.exists(jobFile):
                return True
            if iTry >= maxTries:
                return False
            elif iTry == 1:
                logger.warning(("Job file `{}` for job `{}` does not exist (yet). We will try #{} more times with {}s "
                        "intervals.").format(jobFile, jobStoreID, maxTries - iTry, sleepTime))
            time.sleep(sleepTime)
        return False

    def exists(self, jobStoreID):
        return os.path.exists(self._getJobFileName(jobStoreID))
    
    def getPublicUrl(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        jobStorePath = self._getFilePathFromId(jobStoreFileID)
        if os.path.exists(jobStorePath):
            return 'file:' + jobStorePath
        else:
            raise NoSuchFileException(jobStoreFileID)

    def getSharedPublicUrl(self, sharedFileName):
        jobStorePath = os.path.join(self.sharedFilesDir, sharedFileName)
        if os.path.exists(jobStorePath):
            return 'file:' + jobStorePath
        else:
            raise NoSuchFileException(sharedFileName)

    def load(self, jobStoreID):
        self._checkJobStoreId(jobStoreID)
        # Load a valid version of the job
        jobFile = self._getJobFileName(jobStoreID)
        with open(jobFile, 'rb') as fileHandle:
            job = pickle.load(fileHandle)
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
        with open(self._getJobFileName(job.jobStoreID) + ".new", 'wb') as f:
            pickle.dump(job, f)
        # This should be atomic for the file system
        os.rename(self._getJobFileName(job.jobStoreID) + ".new", self._getJobFileName(job.jobStoreID))

    def delete(self, jobStoreID):
        # The jobStoreID is the relative path to the directory containing the job,
        # removing this directory deletes the job.
        if self.exists(jobStoreID):
            # Remove the job-associated files in need of cleanup, which may or
            # may not live under the job's directory.
            robust_rmtree(self._getJobFilesCleanupDir(jobStoreID))
            # Remove the job's directory itself.
            robust_rmtree(self._getJobDirFromId(jobStoreID))

    def jobs(self):
        # Walk through list of temporary directories searching for jobs.
        # Jobs are files that start with 'job'.
        # Note that this also catches jobWhatever.new which exists if an update
        # is in progress.
        for tempDir in self._jobDirectories():
            for i in os.listdir(tempDir):
                logger.warning('Job Dir: %s' % i)
                if i.startswith(self.JOB_DIR_PREFIX):
                    # This is a job instance directory 
                    try:
                        yield self.load(self._getJobIdFromDir(os.path.join(tempDir, i)))
                    except NoSuchJobException:
                        # An orphaned job may leave an empty or incomplete job file which we can safely ignore
                        pass

    ##########################################
    # Functions that deal with temporary files associated with jobs
    ##########################################

    @contextmanager
    def optionalHardCopy(self, hardlink):
        if hardlink:
            saved = self.linkImports
            self.linkImports = False
        yield
        if hardlink:
            self.linkImports = saved

    def _copyOrLink(self, srcURL, destPath):
        # linking is not done be default because of issue #1755
        srcPath = self._extractPathFromUrl(srcURL)
        if self.linkImports:
            os.symlink(os.path.realpath(srcPath), destPath)
        else:
            shutil.copyfile(srcPath, destPath)

    def _importFile(self, otherCls, url, sharedFileName=None, hardlink=False):
        if issubclass(otherCls, FileJobStore):
            if sharedFileName is None:
                absPath = self._getUniqueFilePath(url.path)  # use this to get a valid path to write to in job store
                with self.optionalHardCopy(hardlink):
                    self._copyOrLink(url, absPath)
                # TODO: os.stat(absPath).st_size consistently gives values lower than
                # getDirSizeRecursively()
                return FileID(self._getFileIdFromPath(absPath), os.stat(absPath).st_size)
            else:
                self._requireValidSharedFileName(sharedFileName)
                path = self._getSharedFilePath(sharedFileName)
                with self.optionalHardCopy(hardlink):
                    self._copyOrLink(url, path)
                return None
        else:
            return super(FileJobStore, self)._importFile(otherCls, url,
                                                         sharedFileName=sharedFileName)

    def _exportFile(self, otherCls, jobStoreFileID, url):
        if issubclass(otherCls, FileJobStore):
            shutil.copyfile(self._getFilePathFromId(jobStoreFileID), self._extractPathFromUrl(url))
        else:
            super(FileJobStore, self)._exportFile(otherCls, jobStoreFileID, url)

    @classmethod
    def getSize(cls, url):
        return os.stat(cls._extractPathFromUrl(url)).st_size

    @classmethod
    def _readFromUrl(cls, url, writable):
        """
        Writes the contents of a file to a source (writes url to writable)
        using a ~10Mb buffer.

        :param str url: A path as a string of the file to be read from.
        :param object writable: An open file object to write to.
        """
        # we use a ~10Mb buffer to improve speed
        with open(cls._extractPathFromUrl(url), 'rb') as readable:
            shutil.copyfileobj(readable, writable, length=cls.BUFFER_SIZE)

    @classmethod
    def _writeToUrl(cls, readable, url):
        """
        Writes the contents of a file to a source (writes readable to url)
        using a ~10Mb buffer.

        :param str url: A path as a string of the file to be written to.
        :param object readable: An open file object to read from.
        """
        # we use a ~10Mb buffer to improve speed
        with open(cls._extractPathFromUrl(url), 'wb') as writable:
            shutil.copyfileobj(readable, writable, length=cls.BUFFER_SIZE)

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

    def _makeStringFilenameSafe(self, arbitraryString):
        """
        Given an arbitrary string, produce a filename-safe though not
        necessarily unique string based on it.

        The input string may be discarded altogether and replaced with any
        other nonempty filename-safe string.

        :param str arbitraryString: An arbitrary string

        :return: A filename-safe string
        """

        # We will fill this in with the filename-safe parts we find.
        parts = []

        for substring in re.findall("[A-Za-z0-9._-]+", arbitraryString):
            # Collect all the matching substrings
            parts.append(substring)

        if len(parts) == 0:
            parts.append("UNPRINTABLE")

        # Glue it all together
        return '_'.join(parts)

    def writeFile(self, localFilePath, jobStoreID=None, cleanup=False):
        absPath = self._getUniqueFilePath(localFilePath, jobStoreID, cleanup)
        relPath = self._getFileIdFromPath(absPath)
        shutil.copyfile(localFilePath, absPath)
        return relPath

    @contextmanager
    def writeFileStream(self, jobStoreID=None, cleanup=False):
        absPath = self._getUniqueFilePath('stream', jobStoreID, cleanup)
        relPath = self._getFileIdFromPath(absPath)
        with open(absPath, 'wb') as f:
            # Don't yield while holding an open file descriptor to the temp
            # file. That can result in temp files still being open when we try
            # to clean ourselves up, somehow, for certain workloads.
            yield f, relPath

    def getEmptyFileStoreID(self, jobStoreID=None, cleanup=False):
        with self.writeFileStream(jobStoreID, cleanup) as (fileHandle, jobStoreFileID):
            return jobStoreFileID

    def updateFile(self, jobStoreFileID, localFilePath):
        self._checkJobStoreFileID(jobStoreFileID)
        jobStoreFilePath = self._getFilePathFromId(jobStoreFileID)

        if os.path.samefile(jobStoreFilePath, localFilePath):
            # The files are already the same file. We can't copy on eover the other.
            return

        shutil.copyfile(localFilePath, jobStoreFilePath)

    def readFile(self, jobStoreFileID, localFilePath, symlink=False):
        self._checkJobStoreFileID(jobStoreFileID)
        jobStoreFilePath = self._getFilePathFromId(jobStoreFileID)
        localDirPath = os.path.dirname(localFilePath)

        if not symlink and os.path.islink(localFilePath):
            # We had a symlink and want to clobber it with a hardlink or copy.
            os.unlink(localFilePath)

        if os.path.exists(localFilePath) and os.path.samefile(jobStoreFilePath, localFilePath):
            # The files are already the same: same name, hardlinked, or
            # symlinked. There is nothing to do, and trying to shutil.copyfile
            # one over the other will fail.
            return

        if symlink:
            # If the reader will accept a symlink, so always give them one.
            # There's less that can go wrong.
            try:
                os.symlink(jobStoreFilePath, localFilePath)
                # It worked!
                return
            except OSError as e:
                if e.errno == errno.EEXIST:
                    # Overwrite existing file, emulating shutil.copyfile().
                    os.unlink(localFilePath)
                    # It would be very unlikely to fail again for same reason but possible
                    # nonetheless in which case we should just give up.
                    os.symlink(jobStoreFilePath, localFilePath)

                    # Now we succeeded and don't need to copy
                    return
                else:
                    raise

        # If we get here, symlinking isn't an option.
        if os.stat(jobStoreFilePath).st_dev == os.stat(localDirPath).st_dev:
            # It is possible that we can hard link the file.
            # Note that even if the device numbers match, we can end up trying
            # to create a "cross-device" link.

            try:
                os.link(jobStoreFilePath, localFilePath)
                # It worked!
                return
            except OSError as e:
                if e.errno == errno.EEXIST:
                    # Overwrite existing file, emulating shutil.copyfile().
                    os.unlink(localFilePath)
                    # It would be very unlikely to fail again for same reason but possible
                    # nonetheless in which case we should just give up.
                    os.link(jobStoreFilePath, localFilePath)

                    # Now we succeeded and don't need to copy
                    return
                elif e.errno == errno.EXDEV:
                    # It's a cross-device link even though it didn't appear to be.
                    # Just keep going and hit the file copy case.
                    pass
                else:
                    logger.critical('Unexpected OSError when reading file from job store')
                    logger.critical('jobStoreFilePath: ' + jobStoreFilePath + ' ' + str(os.path.exists(jobStoreFilePath)))
                    logger.critical('localFilePath: ' + localFilePath + ' ' + str(os.path.exists(localFilePath)))
                    raise

        # If we get here, neither a symlink nor a hardlink will work.
        # Make a complete copy.
        shutil.copyfile(jobStoreFilePath, localFilePath)

    def deleteFile(self, jobStoreFileID):
        if not self.fileExists(jobStoreFileID):
            return
        os.remove(self._getFilePathFromId(jobStoreFileID))

    def fileExists(self, jobStoreFileID):
        absPath = self._getFilePathFromId(jobStoreFileID)

        if (not absPath.startswith(self.jobsDir) and
            not absPath.startswith(self.filesDir) and
            not absPath.startswith(self.jobFilesDir)):
            # Don't even look for it, it is out of bounds.
            raise NoSuchFileException(jobStoreFileID)
            
        try:
            st = os.stat(absPath)
        except os.error:
            return False
        if not stat.S_ISREG(st.st_mode):
            raise NoSuchFileException(jobStoreFileID)
        return True

    def getFileSize(self, jobStoreFileID):
        # Duplicate a bunch of fileExists to save on stat calls
        absPath = self._getFilePathFromId(jobStoreFileID)

        if (not absPath.startswith(self.jobsDir) and
            not absPath.startswith(self.filesDir) and
            not absPath.startswith(self.jobFilesDir)):
            # Don't even look for it, it is out of bounds.
            raise NoSuchFileException(jobStoreFileID)
            
        try:
            st = os.stat(absPath)
        except os.error:
            return 0
        return st.st_size

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        # File objects are context managers (CM) so we could simply return what open returns.
        # However, it is better to wrap it in another CM so as to prevent users from accessing
        # the file object directly, without a with statement.
        with open(self._getFilePathFromId(jobStoreFileID), 'wb') as f:
            yield f

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        self._checkJobStoreFileID(jobStoreFileID)
        with open(self._getFilePathFromId(jobStoreFileID), 'rb') as f:
            yield f

    ##########################################
    # The following methods deal with shared files, i.e. files not associated
    # with specific jobs.
    ##########################################

    def _getSharedFilePath(self, sharedFileName):
        return os.path.join(self.sharedFilesDir, sharedFileName)

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        # the isProtected parameter has no effect on the fileStore
        assert self._validateSharedFileName( sharedFileName )
        with open(self._getSharedFilePath(sharedFileName), 'wb') as f:
            yield f

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        assert self._validateSharedFileName( sharedFileName )
        try:
            with open(self._getSharedFilePath(sharedFileName), 'rb') as f:
                yield f
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise NoSuchFileException(sharedFileName)
            else:
                raise

    def writeStatsAndLogging(self, statsAndLoggingString):
        # Temporary files are placed in the stats directory tree 
        fd, tempStatsFile = tempfile.mkstemp(prefix="stats", suffix=".new", dir=self._getArbitraryStatsDir())
        writeFormat = 'w' if isinstance(statsAndLoggingString, str) else 'wb'
        with open(tempStatsFile, writeFormat) as f:
            f.write(statsAndLoggingString)
        os.close(fd)
        os.rename(tempStatsFile, tempStatsFile[:-4])  # This operation is atomic

    def readStatsAndLogging(self, callback, readAll=False):
        numberOfFilesProcessed = 0
        for tempDir in self._statsDirectories():
            for tempFile in os.listdir(tempDir):
                if tempFile.startswith('stats'):
                    absTempFile = os.path.join(tempDir, tempFile)
                    if os.path.isfile(absTempFile):
                        if readAll or not tempFile.endswith('.new'):
                            with open(absTempFile, 'rb') as fH:
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

    def _getJobDirFromId(self, jobStoreID):
        """

        Find the directory for a job, which holds its job file.

        :param str jobStoreID: ID of a job, which is a relative to self.jobsDir.
        :rtype : string, string is the absolute path to a job directory inside self.jobsDir.
        """
        return os.path.join(self.jobsDir, jobStoreID)

    def _getJobIdFromDir(self, absPath):
        """
        :param str absPath: The absolute path to a job directory under self.jobsDir which represents a job.
        :rtype : string, string is the job ID, which is a path relative to self.jobsDir
        """
        return absPath[len(self.jobsDir)+1:]

    def _getJobFileName(self, jobStoreID):
        """
        Return the path to the file containing the serialised JobGraph instance for the given
        job.

        :rtype: str
        """
        return os.path.join(self._getJobDirFromId(jobStoreID), "job")

    def _getJobFilesDir(self, jobStoreID):
        """
        Return the path to the directory that should hold files made by the
        given job that should survive its deletion.

        This directory will only be created if files are to be put in it.

        :rtype : string, string is the absolute path to the job's files 
                 directory
        """

        return os.path.join(self.jobFilesDir, jobStoreID)

    def _getJobFilesCleanupDir(self, jobStoreID):
        """
        Return the path to the directory that should hold files made by the
        given job that will be deleted when the job is deleted.

        This directory will only be created if files are to be put in it.

        It may or may not be a subdirectory of the job's own directory.

        :rtype : string, string is the absolute path to the job's cleanup
                 files directory
        """

        return os.path.join(self.jobFilesDir, jobStoreID, "cleanup")

    def _checkJobStoreId(self, jobStoreID):
        """
        Raises a NoSuchJobException if the job with ID jobStoreID does not exist.
        """
        if not self.waitForExists(jobStoreID,30):
            raise NoSuchJobException(jobStoreID)

    def _getFilePathFromId(self, jobStoreFileID):
        """
        :param str jobStoreFileID: The ID of a file

        :rtype : string, string is the absolute path that that file should
                 appear at on disk, under either self.jobsDir if it is to be
                 cleaned up with a job, or self.filesDir otherwise.
        """

        # We just make the file IDs paths under the job store overall.
        absPath = os.path.join(self.jobStoreDir, jobStoreFileID)

        # Don't validate here, we are called by the validation logic

        return absPath

    def _getFileIdFromPath(self, absPath):
        """
        :param str absPath: The absolute path of a file. 

        :rtype : string, string is the file ID.
        """

        return absPath[len(self.jobStoreDir)+1:]

    def _checkJobStoreFileID(self, jobStoreFileID):
        """
        :raise NoSuchFileException: if the file with ID jobStoreFileID does
                                    not exist or is not a file
        """
        if not self.fileExists(jobStoreFileID):
            raise NoSuchFileException(jobStoreFileID)

    def _getArbitraryJobsDirForName(self, jobNameSlug):
        """
        Gets a temporary directory in a multi-level hierarchy in self.jobsDir.
        The directory is not unique and may already have other jobs' directories in it.
        We organize them at the top level by job name, to be user-inspectable.

        We make sure to prepend a string so that job names can't collide with
        spray directory names.

        :param str jobNameSlug: A partial filename derived from the job name.
                                Used as the first level of the directory hierarchy.

        :rtype : string, path to temporary directory in which to place files/directories.

        
        """

        
        if len(os.listdir(self.jobsDir)) > self.fanOut:
            # Make sure that we don't over-fill the root with too many unique job names.
            # Go in a subdirectory tree, and then go by job name and make another tree.
            return self._getDynamicSprayDir(os.path.join(self._getDynamicSprayDir(self.jobsDir),
                                                                                  self.JOB_NAME_DIR_PREFIX + jobNameSlug))
        else:
            # Just go in the root
            return self._getDynamicSprayDir(os.path.join(self.jobsDir, self.JOB_NAME_DIR_PREFIX + jobNameSlug))

    def _getArbitraryStatsDir(self):
        """
        Gets a temporary directory in a multi-level hierarchy in self.statsDir.
        The directory is not unique and may already have other stats files in it.

        :rtype : string, path to temporary directory in which to place files/directories.

        
        """

        return self._getDynamicSprayDir(self.statsDir)

    def _getArbitraryFilesDir(self):
        """
        Gets a temporary directory in a multi-level hierarchy in self.filesDir.
        The directory is not unique and may already have other user files in it.

        :rtype : string, path to temporary directory in which to place files/directories.

        
        """

        return self._getDynamicSprayDir(self.filesDir)
    
    def _getDynamicSprayDir(self, root):
        """
        Gets a temporary directory in a possibly multi-level hierarchy of
        directories under the given root.

        Each time a directory in the hierarchy starts to fill up, additional
        hierarchy levels are created under it, and we randomly "spray" further
        files and directories across them.

        We can't actually enforce that we never go over our internal limit for
        files in a directory, because any number of calls to this function can
        be happening simultaneously. But we can enforce that, once too many
        files are visible on disk, only subdirectories will be created.

        The returned directory will exist, and may contain other data already.

        The caller may not create any files or directories in the returned
        directory with single-character names that are in self.validDirs.

        :param str root : directory to put the hierarchy under, which will 
                          fill first.

        :rtype : string, path to temporary directory in which to place
                 files/directories.
        """
        tempDir = root

        # Make sure the root exists
        mkdir_p(tempDir)

        while len(os.listdir(tempDir)) >= self.fanOut:
            # We need to use a layer of directories under here to avoid over-packing the directory
            tempDir = os.path.join(tempDir, random.choice(self.validDirs))
            mkdir_p(tempDir)
        
        # When we get here, we found a sufficiently empty directory
        return tempDir

    def _walkDynamicSprayDir(self, root):
        """
        Walks over a directory tree filled in by _getDynamicSprayDir.

        Yields each directory _getDynamicSprayDir has ever returned, and no
        directories it has not returned (besides the root).

        If the caller looks in the directory, they must ignore subdirectories
        with single-character names in self.validDirs.

        :param str root : directory the hierarchy was put under

        :rtype : an iterator over directories
        """

        # Always yield the root.
        # The caller is responsible for dealing with it if it has gone away.
        yield root

        children = []

        try:
            # Look for children
            children = os.listdir(root)
        except:
            # Don't care if they are gone
            pass

        for child in children:
            # Go over all the children
            if child not in self.validDirsSet:
                # Only look at our reserved names we use for fan-out
                continue
            
            # We made this directory, so go look in it
            childPath = os.path.join(root, child)

            # Recurse
            for item in self._walkDynamicSprayDir(childPath):
                yield item

    def _jobDirectories(self):
        """
        :rtype : an iterator to the temporary directories containing job
                 files. They may also contain directories containing more
                 job files.
        """

        # Walking the job directories is more complicated.
        # We have one layer of spray (which is sometimes bypassed, but that's OK), then a job name, then another layer.
        # We can tell the job name directories from the spray directories because they start with self.JOB_NAME_DIR_PREFIX.
        # We never look at the directories containing the job name directories,
        # so they aren't mistaken for the leaf-level per-job job directories.

        for jobHoldingDir in self._walkDynamicSprayDir(self.jobsDir):
            # For every directory in the first spray, look at children
            children = []

            try:
                children = os.listdir(jobHoldingDir)
            except:
                pass
            
            for jobNameDir in children:
                if not jobNameDir.startswith(self.JOB_NAME_DIR_PREFIX):
                    continue

                # Now we have only the directories that are named after jobs. Look inside them.
                for inner in self._walkDynamicSprayDir(os.path.join(jobHoldingDir, jobNameDir)):
                    yield inner
                

    def _statsDirectories(self):
        """
        :rtype : an iterator to the temporary directories containing stats
                 files. They may also contain directories containing more
                 stats files.
        """

        return self._walkDynamicSprayDir(self.statsDir)

    def _getUniqueFilePath(self, fileName, jobStoreID=None, cleanup=False):
        """
        Create unique file name within a jobStore directory or tmp directory.

        :param fileName: A file name, which can be a full path as only the
        basename will be used.
        :param jobStoreID: If given, the path returned will be in a directory including the job's ID as part of its path.
        :param bool cleanup: If True and jobStoreID is set, the path will be in
            a place such that it gets deleted when the job is deleted.
        :return: The full path with a unique file name.
        """

        # Give the file a unique directory that either will be cleaned up with a job or won't.
        directory = self._getFileDirectory(jobStoreID, cleanup)
        # And then a path under it
        uniquePath = os.path.join(directory, os.path.basename(fileName))
        # No need to check if it exists already; it is in a unique directory.
        return uniquePath

    def _getFileDirectory(self, jobStoreID=None, cleanup=False):
        """
        Get a new empty directory path for a file to be stored at.
        
        
        :param str jobStoreID: If the jobStoreID is not None, the file wil
               be associated with the job with that ID.
        
        :param bool cleanup: If cleanup is also True, this directory
               will be cleaned up when the job is deleted.

        :rtype :string, string is the absolute path to a directory to put the file in.
        """
        if jobStoreID != None:
            # Make a temporary file within the job's files directory

            # Make sure the job is legit
            self._checkJobStoreId(jobStoreID)
            # Find where all its created files should live, depending on if
            # they need to go away when the job is deleted or not.
            jobFilesDir = self._getJobFilesDir(jobStoreID) if not cleanup \
                else self._getJobFilesCleanupDir(jobStoreID)
            
            # Lazily create the parent directory.
            # We don't want our tree filled with confusingly empty directories.
            mkdir_p(jobFilesDir)
            
            # Then make a temp directory inside it
            return tempfile.mkdtemp(prefix='file-', dir=jobFilesDir)
        else:
            # Make a temporary file within the non-job-associated files hierarchy
            return tempfile.mkdtemp(prefix='file-', dir=self._getArbitraryFilesDir())
