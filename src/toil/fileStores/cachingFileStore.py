# Copyright (C) 2015-2018 Regents of the University of California
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

from __future__ import absolute_import, print_function
from future import standard_library
standard_library.install_aliases()
from builtins import map
from builtins import str
from builtins import range
from builtins import object
from abc import abstractmethod, ABCMeta
from collections import namedtuple, defaultdict
from contextlib import contextmanager
from fcntl import flock, LOCK_EX, LOCK_UN
from functools import partial
from hashlib import sha1
from threading import Thread, Semaphore, Event
from future.utils import with_metaclass
from six.moves.queue import Empty, Queue
import base64
import dill
import errno
import logging
import os
import shutil
import stat
import tempfile
import time
import uuid

from toil.common import cacheDirName, getDirSizeRecursively, getFileSystemSize
from toil.lib.bioio import makePublicDir
from toil.lib.humanize import bytes2human
from toil.lib.misc import mkdir_p
from toil.lib.objects import abstractclassmethod
from toil.resource import ModuleDescriptor
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.fileStores import FileID

logger = logging.getLogger(__name__)

class CacheError(Exception):
    """
    Error Raised if the user attempts to add a non-local file to cache
    """

    def __init__(self, message):
        super(CacheError, self).__init__(message)


class CacheUnbalancedError(CacheError):
    """
    Raised if file store can't free enough space for caching
    """
    message = 'Unable unable to free enough space for caching.  This error frequently arises due ' \
              'to jobs using more disk than they have requested.  Turn on debug logging to see ' \
              'more information leading up to this error through cache usage logs.'

    def __init__(self):
        super(CacheUnbalancedError, self).__init__(self.message)


class IllegalDeletionCacheError(CacheError):
    """
    Error Raised if the Toil detects the user deletes a cached file
    """

    def __init__(self, deletedFile):
        message = 'Cache tracked file (%s) deleted explicitly by user. Use deleteLocalFile to ' \
                  'delete such files.' % deletedFile
        super(IllegalDeletionCacheError, self).__init__(message)


class InvalidSourceCacheError(CacheError):
    """
    Error Raised if the user attempts to add a non-local file to cache
    """

    def __init__(self, message):
        super(InvalidSourceCacheError, self).__init__(message)

class CachingFileStore(AbstractFileStore):
    """
    A cache-enabled file store.

    Provides files that are read out as symlinks or hard links into a cache
    directory for the node, if permitted by the workflow.

    Also attempts to write files back to the backing JobStore asynchronously,
    after quickly taking them into the cache.
    
    TODO: is that even possible? How can another node tell a file being
    asynchronously written from one that doesn't exist?
    
    """

    def __init__(self, jobStore, jobGraph, localTempDir, waitForPreviousCommit):
        super(CachingFileStore, self).__init__(jobStore, jobGraph, localTempDir, waitForPreviousCommit)
        
        # Variables related to caching
        # Decide where the cache directory will be. We put it next to the
        # local temp dirs for all of the jobs run on this machine.
        # At this point in worker startup, when we are setting up caching,
        # localTempDir is the worker directory, not the job directory.
        self.localCacheDir = os.path.join(os.path.dirname(localTempDir),
                                          cacheDirName(self.jobStore.config.workflowID))
        
        # Since each worker has it's own unique CachingFileStore instance, and only one Job can run
        # at a time on a worker, we can track some stuff about the running job in ourselves.
        self.jobName = str(self.jobGraph)
        self.jobID = sha1(self.jobName.encode('utf-8')).hexdigest()
        logger.debug('Starting job (%s) with ID (%s).', self.jobName, self.jobID)

        # We need to track what attempt of the workflow we are, to prevent crosstalk between attempts' caches.
        self.workflowAttemptNumber = self.jobStore.config.workflowAttemptNumber

        # Make sure the cache directory exists
        mkdir_p(self.localCacheDir)

        # TODO: Connect to the cache database in there, or create it if not present

        # We need to track what local files exist for this job for a given
        # global file ID, because we expose an operation to delete them all.
        self.localFileMap = defaultdict(list)

    # Caching-specific API

    def getCacheLimit(self):
        """
        Return the total number of bytes to which the cache is limited.

        If no limit is in place, return None.
        """

        return None

    def getCacheUsed(self):
        """
        Return the total number of bytes used in the cache.

        If no value is available, return None.
        """

        return None

    def getCacheExtraJobSpace(self):
        """
        Return the total number of bytes of disk space requested by jobs
        running against this cache but not yet used.

        If no value is available, return None.
        """

        return None

    def getCacheJobRequirement(self):
        """
        Return the total number of bytes of disk space requested by the current
        job.

        The cache tracks this in order to enable the cache's disk space to grow
        and shrink as jobs start and stop using it.

        If no value is available, return None.
        """

        return None


    def adjustCacheLimit(self, newTotalBytes):
        """
        Adjust the total cache size limit to the given number of megabytes.
        """

        pass

    def fileIsCached(self, fileID):
        """
        Return true if the given file is currently cached, and false otherwise.
        """

        return False

    def getFileReaderCount(self, fileID):
        """
        Return the number of current outstanding reads of the given file from
        the cache.
        """

        return 0
        

    def cachingIsFree(self):
        """
        Return true if files can be cached for free, without taking up space.
        Return false otherwise.

        This will be true when working with certain job stores in certain
        configurations, most notably the FileJobStore.
        """

        return True

    # Normal AbstractFileStore API

    @contextmanager
    def open(self, job):
        """
        This context manager decorated method allows cache-specific operations to be conducted
        before and after the execution of a job in worker.py
        """
        # Create a working directory for the job
        startingDir = os.getcwd()
        # Move self.localTempDir from the worker directory set up in __init__ to a per-job directory.
        self.localTempDir = makePublicDir(os.path.join(self.localTempDir, str(uuid.uuid4())))
        # Check the status of all jobs on this node. If there are jobs that started and died before
        # cleaning up their presence from the database, clean them up ourselves.
        self._removeDeadJobs()
        # Get the requirements for the job and clean the cache if necessary. _freeUpBytes will
        # ensure that the requirements for this job are stored in the state file.
        jobReqs = job.disk
        # Cleanup the cache to free up enough space for this job (if needed)
        # TODO: Atomically record that jobReqs bytes of disk space are in use by this job.
        self._freeUpBytes(jobReqs)
        try:
            os.chdir(self.localTempDir)
            yield
        finally:
            # See how much disk space is used at the end of the job.
            # Not a real peak disk usage, but close enough to be useful for warning the user.
            # TODO: Push this logic into the abstract file store
            diskUsed = getDirSizeRecursively(self.localTempDir)
            logString = ("Job {jobName} used {percent:.2f}% ({humanDisk}B [{disk}B] used, "
                         "{humanRequestedDisk}B [{requestedDisk}B] requested) at the end of "
                         "its run.".format(jobName=self.jobName,
                                           percent=(float(diskUsed) / jobReqs * 100 if
                                                    jobReqs > 0 else 0.0),
                                           humanDisk=bytes2human(diskUsed),
                                           disk=diskUsed,
                                           humanRequestedDisk=bytes2human(jobReqs),
                                           requestedDisk=jobReqs))
            self.logToMaster(logString, level=logging.DEBUG)
            if diskUsed > jobReqs:
                self.logToMaster("Job used more disk than requested. Please reconsider modifying "
                                 "the user script to avoid the chance  of failure due to "
                                 "incorrectly requested resources. " + logString,
                                 level=logging.WARNING)

            # Go back up to the per-worker local temp directory.
            os.chdir(startingDir)
            self.cleanupInProgress = True
            
            # TODO: record that jobReqs bytes of disk space are now no longer used by this job.

    def writeGlobalFile(self, localFileName, cleanup=False):
        absLocalFileName = self._resolveAbsoluteLocalPath(localFileName)
        creatorID = self.jobGraph.jobStoreID
        fileStoreID = self.jobStore.writeFile(absLocalFileName, creatorID, cleanup)
        self.localFileMap[fileStoreID].append(absLocalFileName)
        return FileID.forPath(fileStoreID, absLocalFileName)

    def readGlobalFile(self, fileStoreID, userPath=None, cache=True, mutable=False, symlink=False):
        if userPath is not None:
            localFilePath = self._resolveAbsoluteLocalPath(userPath)
            if os.path.exists(localFilePath):
                raise RuntimeError(' File %s ' % localFilePath + ' exists. Cannot Overwrite.')
        else:
            localFilePath = self.getLocalTempFileName()

        self.jobStore.readFile(fileStoreID, localFilePath, symlink=symlink)
        self.localFileMap[fileStoreID].append(localFilePath)
        return localFilePath

    def readGlobalFileStream(self, fileStoreID):
        return self.jobStore.readFileStream(fileStoreID)

    def deleteLocalFile(self, fileStoreID):
        try:
            localFilePaths = self.localFileMap.pop(fileStoreID)
        except KeyError:
            raise OSError(errno.ENOENT, "Attempting to delete a non-local file")
        else:
            for localFilePath in localFilePaths:
                os.remove(localFilePath) 

    def deleteGlobalFile(self, fileStoreID):
        jobStateIsPopulated = False
        with self._CacheState.open(self) as cacheInfo:
            if self.jobID in cacheInfo.jobState:
                jobState = self._JobState(cacheInfo.jobState[self.jobID])
                jobStateIsPopulated = True
        if jobStateIsPopulated and fileStoreID in list(jobState.jobSpecificFiles.keys()):
            # Use deleteLocalFile in the backend to delete the local copy of the file.
            self.deleteLocalFile(fileStoreID)
            # At this point, the local file has been deleted, and possibly the cached copy. If
            # the cached copy exists, it is either because another job is using the file, or
            # because retaining the file in cache doesn't unbalance the caching equation. The
            # first case is unacceptable for deleteGlobalFile and the second requires explicit
            # deletion of the cached copy.
        # Check if the fileStoreID is in the cache. If it is, ensure only the current job is
        # using it.
        cachedFile = self.encodedFileID(fileStoreID)
        if os.path.exists(cachedFile):
            self.removeSingleCachedFile(fileStoreID)
        # Add the file to the list of files to be deleted once the run method completes.
        self.filesToDelete.add(fileStoreID)
        self.logToMaster('Added file with ID \'%s\' to the list of files to be' % fileStoreID +
                         ' globally deleted.', level=logging.DEBUG)

    def exportFile(self, jobStoreFileID, dstUrl):
        self.jobStore.exportFile(jobStoreFileID, dstUrl)

    def _freeUpBytes(self, newJobReqs):
        """
        Cleanup all files in the cache directory to ensure that at lead newJobReqs are available
        for use.
        :param float newJobReqs: the total number of bytes of files allowed in the cache.
        """
       
        pass

    def waitForCommit(self):
        # there is no asynchronicity in this file store so no need to block at all
        return True

    def commitCurrentJob(self):
        try:
            # Indicate any files that should be deleted once the update of
            # the job wrapper is completed.
            self.jobGraph.filesToDelete = list(self.filesToDelete)
            # Complete the job
            self.jobStore.update(self.jobGraph)
            # Delete any remnant jobs
            list(map(self.jobStore.delete, self.jobsToDelete))
            # Delete any remnant files
            list(map(self.jobStore.deleteFile, self.filesToDelete))
            # Remove the files to delete list, having successfully removed the files
            if len(self.filesToDelete) > 0:
                self.jobGraph.filesToDelete = []
                # Update, removing emptying files to delete
                self.jobStore.update(self.jobGraph)
        except:
            self._terminateEvent.set()
            raise

    @classmethod
    def shutdown(cls, dir_):
        """
        :param dir_: The directory that will contain the cache state file.
        """
        
        cls._removeDeadJobs(cacheInfo, batchSystemShutdown=True)
        shutil.rmtree(dir_)

    def __del__(self):
        """
        Cleanup function that is run when destroying the class instance that ensures that all the
        file writing threads exit.
        """
        pass

    @classmethod
    def _removeDeadJobs(cls, nodeInfo, batchSystemShutdown=False):
        """
        Look at the state of all jobs registered in the database, and handle them
        (clean up the disk)

        :param str nodeInfo: The location of the workflow directory on the node.
        :param bool batchSystemShutdown: Is the batch system in the process of shutting down?
        :return:
        """

        pass

