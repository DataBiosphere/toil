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

import re
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from datetime import timedelta

try:
    import cPickle
except ImportError:
    import pickle as cPickle

import logging

logger = logging.getLogger(__name__)


class NoSuchJobException(Exception):
    def __init__(self, jobStoreID):
        super(NoSuchJobException, self).__init__("The job '%s' does not exist" % jobStoreID)


class ConcurrentFileModificationException(Exception):
    def __init__(self, jobStoreFileID):
        super(ConcurrentFileModificationException, self).__init__(
            'Concurrent update to file %s detected.' % jobStoreFileID)


class NoSuchFileException(Exception):
    def __init__(self, jobStoreFileID, customName=None):
        if customName is None:
            message = "File '%s' does not exist" % jobStoreFileID
        else:
            message = "File '%s' (%s) does not exist" % (customName, jobStoreFileID)
        super(NoSuchFileException, self).__init__(message)


class JobStoreCreationException(Exception):
    def __init__(self, message):
        super(JobStoreCreationException, self).__init__(message)


class AbstractJobStore(object):
    """ 
    Represents the physical storage for the jobs and associated files in a toil.
    """
    __metaclass__ = ABCMeta

    def __init__(self, config=None):
        """
        :param config: If config is not None then the \
        given configuration object will be written to the shared file "config.pickle" which can \
        later be retrieved using the readSharedFileStream. See writeConfigToStore. \
        If this file already exists it will be overwritten. If config is None, \
        the shared file "config.pickle" is assumed to exist and is retrieved. See loadConfigFromStore.

        """
        # Now get on with reading or writing the config
        if config is None:
            with self.readSharedFileStream("config.pickle") as fileHandle:
                self.__config = cPickle.load(fileHandle)
        else:
            self.__config = config
            self.writeConfigToStore()

    def writeConfigToStore(self):
        """
        Re-writes the config attribute to the jobStore, so that its values can be retrieved 
        if the jobStore is reloaded.
        """
        with self.writeSharedFileStream("config.pickle", isProtected=False) as fileHandle:
            cPickle.dump(self.__config, fileHandle, cPickle.HIGHEST_PROTOCOL)

    @property
    def config(self):
        return self.__config

    @staticmethod
    def _checkJobStoreCreation(create, exists, jobStoreString):
        """
        Consistency checks which will result in exceptions if we attempt to overwrite an existing
        jobStore.

        :type create: boolean

        :type exists: boolean

        :raise JobStoreCreationException:  Thrown if create=True and exists=True or create=False
                                           and exists=False
        """
        if create and exists:
            raise JobStoreCreationException("The job store '%s' already exists. "
                                            "Use --restart or 'toil restart' to resume this jobStore, "
                                            "else remove it to start from scratch" % jobStoreString)
        if not create and not exists:
            raise JobStoreCreationException("The job store '%s' does not exist, so there "
                                            "is nothing to restart." % jobStoreString)

    @abstractmethod
    def deleteJobStore(self):
        """
        Removes the jobStore from the disk/store. Careful!
        """
        raise NotImplementedError()

    def getEnv(self):
        """
        Returns a dictionary of environment variables that this job store requires to be set in
        order to function properly on a worker.

        :rtype: dict[str,str]
        """
        return {}

    ##Cleanup functions

    def clean(self, rootJobWrapper, jobCache=None):
        """
        Function to cleanup the state of a jobStore after a restart.
        Fixes jobs that might have been partially updated.
        Resets the try counts.
        Removes jobs that are not successors of the rootJobWrapper.
        
        If jobCache is passed, it must be a dict from job ID to JobWrapper
        object. Jobs will be loaded from the cache (which can be downloaded from
        the jobStore in a batch) instead of piecemeal when recursed into.
        """
        # Iterate from the root jobWrapper and collate all jobs that are reachable from it
        # All other jobs returned by self.jobs() are orphaned and can be removed
        reachableFromRoot = set()

        if jobCache is None:
            logger.warning("Cleaning jobStore recursively. This may be slow.")

        def getJob(jobId):
            if jobCache is not None:
                return jobCache[jobId]
            else:
                return self.load(jobId)
                
        def haveJob(jobId):
            if jobCache is not None:
                return jobCache.has_key(jobId)
            else:
                return self.exists(jobId)
                
        def getJobs():
            if jobCache is not None:
                return jobCache.itervalues()
            else:
                return self.jobs()

        def getConnectedJobs(jobWrapper):
            if jobWrapper.jobStoreID in reachableFromRoot:
                return
            reachableFromRoot.add(jobWrapper.jobStoreID)
            for jobs in jobWrapper.stack:
                for successorJobStoreID in map(lambda x: x[0], jobs):
                    if successorJobStoreID not in reachableFromRoot and haveJob(successorJobStoreID):
                        getConnectedJobs(getJob(successorJobStoreID))

        logger.info("Checking job graph connectivity...")
        getConnectedJobs(rootJobWrapper)
        logger.info("%d jobs reachable from root." % len(reachableFromRoot))

        # Cleanup the state of each jobWrapper
        for jobWrapper in getJobs():
            changed = False  # Flag to indicate if we need to update the jobWrapper
            # on disk

            if len(jobWrapper.filesToDelete) != 0:
                # Delete any files that should already be deleted
                for fileID in jobWrapper.filesToDelete:
                    logger.critical(
                        "Removing file in job store: %s that was marked for deletion but not previously removed" % fileID)
                    self.deleteFile(fileID)
                jobWrapper.filesToDelete = []
                changed = True

            # Delete a jobWrapper if it is not reachable from the rootJobWrapper
            if jobWrapper.jobStoreID not in reachableFromRoot:
                logger.critical(
                    "Removing job: %s that is not a successor of the root job in cleanup" % jobWrapper.jobStoreID)
                self.delete(jobWrapper.jobStoreID)
                continue

            # While jobs at the end of the stack are already deleted remove
            # those jobs from the stack (this cleans up the case that the jobWrapper
            # had successors to run, but had not been updated to reflect this)
            while len(jobWrapper.stack) > 0:
                jobs = [command for command in jobWrapper.stack[-1] if haveJob(command[0])]
                if len(jobs) < len(jobWrapper.stack[-1]):
                    changed = True
                    if len(jobs) > 0:
                        jobWrapper.stack[-1] = jobs
                        break
                    else:
                        jobWrapper.stack.pop()
                else:
                    break

            # Reset the retry count of the jobWrapper
            if jobWrapper.remainingRetryCount != self._defaultTryCount():
                jobWrapper.remainingRetryCount = self._defaultTryCount()
                changed = True

            # This cleans the old log file which may
            # have been left if the jobWrapper is being retried after a jobWrapper failure.
            if jobWrapper.logJobStoreFileID != None:
                self.delete(jobWrapper.logJobStoreFileID)
                jobWrapper.logJobStoreFileID = None
                changed = True

            if changed:  # Update, but only if a change has occurred
                logger.critical("Repairing job: %s" % jobWrapper.jobStoreID)
                self.update(jobWrapper)

        # Remove any crufty stats/logging files from the previous run
        logger.info("Discarding old statistics and logs...")
        self.readStatsAndLogging(lambda x: None)
        
        logger.info("Job store is clean")

    ##########################################
    # The following methods deal with creating/loading/updating/writing/checking for the
    # existence of jobs
    ##########################################  

    @abstractmethod
    def create(self, command, memory, cores, disk,
               predecessorNumber=0):
        """
        Creates a job, adding it to the store.
        
        Command, memory, cores and predecessorNumber
        are all arguments to the job's constructor.

        :rtype : toil.jobWrapper.JobWrapper
        """
        raise NotImplementedError()

    @abstractmethod
    def exists(self, jobStoreID):
        """
        Returns true if the job is in the store, else false.

        :rtype : bool
        """
        raise NotImplementedError()

    # One year should be sufficient to finish any pipeline ;-)
    publicUrlExpiration = timedelta(days=365)

    @abstractmethod
    def getPublicUrl(self, fileName):
        """
        Returns a publicly accessible URL to the given file in the job store. The returned URL
        starts with 'http:',  'https:' or 'file:'. The returned URL may expire as early as 1h
        after its been returned. Throw an exception if the file does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def getSharedPublicUrl(self, sharedFileName):
        """
        Returns a publicly accessible URL to the given file in the job store. The returned URL
        starts with 'http:',  'https:' or 'file:'. The returned URL may expire as early as 1h
        after its been returned. Throw an exception if the file does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def load(self, jobStoreID):
        """
        Loads a job for the given jobStoreID and returns it.

        :rtype: toil.jobWrapper.JobWrapper

        :raises: NoSuchJobException if there is no job with the given jobStoreID
        """
        raise NotImplementedError()

    @abstractmethod
    def update(self, job):
        """
        Persists the job in this store atomically.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, jobStoreID):
        """
        Removes from store atomically, can not then subsequently call load(), write(), update(),
        etc. with the job.

        This operation is idempotent, i.e. deleting a job twice or deleting a non-existent job
        will succeed silently.
        """
        raise NotImplementedError()

    def jobs(self):
        """
        Returns iterator on all jobs in the store. The iterator will contain all jobs, but may also
        contain orphaned jobs that have already finished succesfully and should not be rerun.
        To guarantee you only get jobs that can be run, instead construct a ToilState object
        
        :rtype : iterator
        """
        raise NotImplementedError()

    ##########################################
    # The following provide an way of creating/reading/writing/updating files
    # associated with a given job.
    ##########################################  

    @abstractmethod
    def writeFile(self, localFilePath, jobStoreID=None):
        """
        Takes a file (as a path) and places it in this job store. Returns an ID that can be used
        to retrieve the file at a later time. 
        
        jobStoreID is the id of a job, or None. If specified, when delete(job) 
        is called all files written with the given job.jobStoreID will be 
        removed from the jobStore.
        """
        raise NotImplementedError()

    @abstractmethod
    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        """
        Similar to writeFile, but returns a context manager yielding a tuple of 
        1) a file handle which can be written to and 2) the ID of the resulting 
        file in the job store. The yielded file handle does not need to and 
        should not be closed explicitly.
        """
        raise NotImplementedError()

    @abstractmethod
    def getEmptyFileStoreID(self, jobStoreID=None):
        """
        :rtype : string, the ID of a new, empty file. 
        
        jobStoreID is the id of a job, or None. If specified, when delete(job) 
        is called all files written with the given job.jobStoreID will be 
        removed from the jobStore.
        
        Call to fileExists(getEmptyFileStoreID(jobStoreID)) will return True.
        """
        raise NotImplementedError()

    @abstractmethod
    def readFile(self, jobStoreFileID, localFilePath):
        """
        Copies the file referenced by jobStoreFileID to the given local file path. The version
        will be consistent with the last copy of the file written/updated.
        """
        raise NotImplementedError()

    @abstractmethod
    @contextmanager
    def readFileStream(self, jobStoreFileID):
        """
        Similar to readFile, but returns a context manager yielding a file handle which can be
        read from. The yielded file handle does not need to and should not be closed explicitly.
        """
        raise NotImplementedError()

    @abstractmethod
    def deleteFile(self, jobStoreFileID):
        """
        Deletes the file with the given ID from this job store.
        This operation is idempotent, i.e. deleting a file twice or deleting a non-existent file
        will succeed silently.
        """
        raise NotImplementedError()

    @abstractmethod
    def fileExists(self, jobStoreFileID):
        """
        :rtype : True if the jobStoreFileID exists in the jobStore, else False
        """
        raise NotImplementedError()

    @abstractmethod
    def updateFile(self, jobStoreFileID, localFilePath):
        """
        Replaces the existing version of a file in the jobStore. Throws an exception if the file
        does not exist.

        :raises ConcurrentFileModificationException: if the file was modified concurrently during \
        an invocation of this method
        """
        raise NotImplementedError()

    @abstractmethod
    def updateFileStream(self, jobStoreFileID):
        """
        Similar to writeFile, but returns a context manager yielding a file handle 
        which can be written to. The yielded file handle does not need to and 
        should not be closed explicitly.

        :raises ConcurrentFileModificationException: if the file was modified concurrently during \
        an invocation of this method
        """
        raise NotImplementedError()

    ##########################################
    # The following methods deal with shared files, i.e. files not associated
    # with specific jobs.
    ##########################################  

    sharedFileNameRegex = re.compile(r'^[a-zA-Z0-9._-]+$')

    # FIXME: Rename to updateSharedFileStream

    @abstractmethod
    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        """
        Returns a context manager yielding a writable file handle to the global file referenced \
        by the given name.

        :param sharedFileName: A file name matching AbstractJobStore.fileNameRegex, unique within \
        the physical storage represented by this job store

        :param isProtected: True if the file must be encrypted, None if it may be encrypted or \
        False if it must be stored in the clear.

        :raises ConcurrentFileModificationException: if the file was modified concurrently during \
        an invocation of this method
        """
        raise NotImplementedError()

    @abstractmethod
    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        """
        Returns a context manager yielding a readable file handle to the global file referenced
        by the given name.
        """
        raise NotImplementedError()

    @abstractmethod
    def writeStatsAndLogging(self, statsAndLoggingString):
        """
        Adds the given statistics/logging string to the store of statistics info.
        """
        raise NotImplementedError()

    @abstractmethod
    def readStatsAndLogging(self, callback, readAll=False):
        """
        Reads stats/logging strings accumulated by the writeStatsAndLogging() method. For each
        stats/logging string this method calls the given callback function with an open,
        readable file handle from which the stats string can be read. Returns the number of
        stats/logging strings processed. Each stats/logging string is only processed once unless
        the readAll parameter is set, in which case the given callback will be invoked for all
        existing stats/logging strings, including the ones from a previous invocation of this
        method.

        :type callback: callable
        :type readAll: bool
        """
        raise NotImplementedError()

    ## Helper methods for subclasses

    def _defaultTryCount(self):
        return int(self.config.retryCount + 1)

    @classmethod
    def _validateSharedFileName(cls, sharedFileName):
        return bool(cls.sharedFileNameRegex.match(sharedFileName))
