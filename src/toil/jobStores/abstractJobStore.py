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

import urlparse

import re
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from datetime import timedelta
from uuid import uuid4
from toil.job import JobException
from bd2k.util import memoize
from bd2k.util.objects import abstractstaticmethod, abstractclassmethod

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


@memoize
def getJobStoreClasses():
    """
    Compiles as list of the classes of all jobStores whose dependencies
    are installed.

    Note that job store names must be manually added.
    """
    jobStoreClassNames = (
        "toil.jobStores.azureJobStore.AzureJobStore",
        "toil.jobStores.fileJobStore.FileJobStore",
        "toil.jobStores.aws.jobStore.AWSJobStore"
    )

    jobStoreClasses = []
    for className in jobStoreClassNames:
        moduleName, className = className.rsplit('.', 1)
        from importlib import import_module
        try:
            module = import_module(moduleName)
        except ImportError:
            logger.info("Unable to import '%s'" % moduleName)
        else:
            jobStoreClasses.append(getattr(module, className))
    return jobStoreClasses


def findJobStoreForUrl(url):
    """
    Returns the AbstractJobStore subclass that supports the given URL.

    :param urlparse.ParseResult url: The given URL
    :rtype: toil.jobStore.AbstractJobStore
    """
    for cls in getJobStoreClasses():
        if cls._supportsUrl(url):
            return cls
    raise RuntimeError("No existing job store supports the URL '%s'" % url)


class AbstractJobStore(object):
    """ 
    Represents the physical storage for the jobs and associated files in a toil.
    """
    __metaclass__ = ABCMeta

    def __init__(self, config=None):
        """
        :param config: If config is not None then the given configuration object will be written
               to the shared file "config.pickle" which can later be retrieved using the
               readSharedFileStream. See writeConfigToStore. If this file already exists it will be
               overwritten. If config is None, the shared file "config.pickle" is assumed to exist
               and is retrieved. See loadConfigFromStore.
        """
        # Now get on with reading or writing the config
        if config is None:
            with self.readSharedFileStream("config.pickle") as fileHandle:
                config = cPickle.load(fileHandle)
                assert config.workflowID is not None
                self.__config = config
        else:
            assert config.workflowID is None
            config.workflowID = str(uuid4())
            logger.info("The workflow ID is: '%s'" % config.workflowID)
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

    def setRootJob(self, rootJobStoreID):
        """
        Set the root job of the workflow backed by this job store

        :param str rootJobStoreID: The ID of the job to set as root
        """
        with self.writeSharedFileStream('rootJobStoreID') as f:
            f.write(rootJobStoreID)

    def loadRootJob(self):
        """
        Loads the root job in the current job store.

        :raises toil.job.JobException: If not root job is set or if the root job doesn't exist in
                this jobstore
        :return: The root job.
        :rtype: toil.jobWrapper.JobWrapper
        """
        try:
            with self.readSharedFileStream('rootJobStoreID') as f:
                rootJobID = f.read()
        except NoSuchFileException:
            raise JobException('No job has been set as the root in this job store')

        if not self.exists(rootJobID):
            raise JobException("The root job '%s' doesn't exist. Either the Toil workflow "
                               "is finished or has never been started" % rootJobID)
        return self.load(rootJobID)

    def createRootJob(self, command, memory, cores, disk, predecessorNumber=0):
        """
        Create a new job and set it as the root job in this job store

        :rtype : toil.jobWrapper.JobWrapper
        """
        rootJob = self.create(command=command,
                              memory=memory,
                              cores=cores,
                              disk=disk,
                              predecessorNumber=predecessorNumber)
        self.setRootJob(rootJob.jobStoreID)
        return rootJob

    @staticmethod
    def _checkJobStoreCreation(create, exists, jobStoreString):
        """
        Consistency checks which will result in exceptions if we attempt to overwrite an existing
        jobStore. This method must be called by the constructor of a subclass before any
        modification are made.

        :type create: bool

        :type exists: bool

        :raise JobStoreCreationException:  if create == exists
        """
        if create and exists:
            raise JobStoreCreationException("The job store '%s' already exists. "
                                            "Use --restart or 'toil restart' to resume this jobStore, "
                                            "else remove it to start from scratch" % jobStoreString)
        if not create and not exists:
            raise JobStoreCreationException("The job store '%s' does not exist, so there "
                                            "is nothing to restart." % jobStoreString)

    def importFile(self, srcUrl):
        """
        Imports the file pointed at by sourceUrl into job store. The jobStoreFileId of the new
        file is returned.

        Note that the helper method _importFile is used to read from the source and write to
        destination (which is the current job store in this case). To implement any optimizations that
        circumvent this, the _importFile method should be overridden by subclasses of AbstractJobStore.

        Currently supported schemes are:
            - 's3' for objects in Amazon S3
                e.g. s3://bucket/key

            - 'wasb' for blobs in Azure Blob Storage
                e.g. wasb://container/blob

            - 'file' for local files
                e.g. file://local/file/path

        :param str srcUrl: URL that points to a file or object in the storage mechanism of a
                supported URL scheme e.g. a blob in an Azure Blob Storage container.
        :return The jobStoreFileId of imported file.
        :rtype: str
        """
        url = urlparse.urlparse(srcUrl)
        return self._importFile(findJobStoreForUrl(url), url)

    def _importFile(self, otherCls, url):
        """
        Refer to importFile docstring for information about this method.

        :param type otherCls: The concrete subclass of AbstractJobStore that supports exporting to the given URL.
        :param urlparse.ParseResult url: The parsed url given to importFile.
        :return: The job store file ID of the imported file
        :rtype: str
        """
        with self.writeFileStream() as (writable, jobStoreFileID):
            otherCls._readFromUrl(url, writable)
            return jobStoreFileID

    def exportFile(self, jobStoreFileID, dstUrl):
        """
        Exports file to destination pointed at by the destination URL.

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        Note that the helper method _exportFile is used to read from the source and write to
        destination. To implement any optimizations that circumvent this, the _exportFile method
        should be overridden by subclasses of AbstractJobStore.

        :param str jobStoreFileID: The id of the file in the job store that should be exported.
        :param str dstUrl: URL that points to a file or object in the storage mechanism of a
                supported URL scheme e.g. a blob in an Azure Blob Storage container.
        """
        url = urlparse.urlparse(dstUrl)
        return self._exportFile(findJobStoreForUrl(url), jobStoreFileID, url)

    def _exportFile(self, otherCls, jobStoreFileID, url):
        """
        Refer to exportFile docstring for information about this method.

        :param type otherCls: The concrete subclass of AbstractJobStore that supports exporting to the given URL.
        :param str jobStoreFileID: The id of the file that will be exported.
        :param urlparse.ParseResult url: The parsed url given to importFile.
        """
        with self.readFileStream(jobStoreFileID) as readable:
            otherCls._writeToUrl(readable, url)

    @abstractclassmethod
    def _readFromUrl(cls, url, writable):
        """
        Reads the contents of the object at the specified location and writes it to the given
        writable stream.

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param str url: URL that points to a file or object in the storage mechanism of a
                supported URL scheme e.g. a blob in an Azure Blob Storage container.
        :param writable: a writable stream
        """
        raise NotImplementedError()

    @abstractclassmethod
    def _writeToUrl(cls, readable, url):
        """
        Reads the contents of the given readable stream and writes it to the object at the
        specified location.

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param str url: URL that points to a file or object in the storage mechanism of a
                supported URL scheme e.g. a blob in an Azure Blob Storage container.
        :param readable: a readable stream
        """
        raise NotImplementedError()

    @abstractclassmethod
    def _supportsUrl(cls, url):
        """
        Returns True if the job store supports the URL's scheme.

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param urlparse.ParseResult url: a parsed URL that may be supported
        :return bool: returns true if the cls supports the URL
        """
        raise NotImplementedError()

    @abstractmethod
    def deleteJobStore(self):
        """
        Removes the job store from the disk/store. Careful!
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

    def clean(self, jobCache=None):
        """
        Function to cleanup the state of a jobStore after a restart.
        Fixes jobs that might have been partially updated.
        Resets the try counts.
        Removes jobs that are not successors of the current root job.
        
        If jobCache is passed, it must be a dict from job ID to JobWrapper
        object. Jobs will be loaded from the cache (which can be downloaded from
        the jobStore in a batch) instead of piecemeal when recursed into.

        :return: The root job of the workflow backed by this job store
        :rtype: toil.jobWrapper.JobWrapper
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
            # Traverse jobs in stack
            for jobs in jobWrapper.stack:
                for successorJobStoreID in map(lambda x: x[0], jobs):
                    if successorJobStoreID not in reachableFromRoot and haveJob(successorJobStoreID):
                        getConnectedJobs(getJob(successorJobStoreID))
            # Traverse service jobs
            for jobs in jobWrapper.services:
                for serviceJobStoreID in map(lambda x: x[0], jobs):
                    assert serviceJobStoreID not in reachableFromRoot
                    reachableFromRoot.add(serviceJobStoreID)

        logger.info("Checking job graph connectivity...")
        getConnectedJobs(self.loadRootJob())
        logger.info("%d jobs reachable from root." % len(reachableFromRoot))

        # Cleanup the state of each jobWrapper
        for jobWrapper in getJobs():
            changed = [False]  # Flag to indicate if we need to update the jobWrapper
            # on disk

            if len(jobWrapper.filesToDelete) != 0:
                # Delete any files that should already be deleted
                for fileID in jobWrapper.filesToDelete:
                    logger.critical(
                        "Removing file in job store: %s that was marked for deletion but not previously removed" % fileID)
                    self.deleteFile(fileID)
                jobWrapper.filesToDelete = []
                changed[0] = True

            # Delete a jobWrapper if it is not reachable from the rootJob
            if jobWrapper.jobStoreID not in reachableFromRoot:
                logger.critical(
                    "Removing job: %s that is not a successor of the root job in cleanup" % jobWrapper.jobStoreID)
                self.delete(jobWrapper.jobStoreID)
                continue

            # For a job whose command is already execute, remove jobs from the 
            # stack that are already deleted. 
            # This cleans up the case that the jobWrapper
            # had successors to run, but had not been updated to reflect this
            
            if jobWrapper.command is None:
                stackSize = sum(map(len, jobWrapper.stack))
                # Remove deleted jobs
                jobWrapper.stack = map(lambda x : filter(lambda y : self.exists(y[0]), x), jobWrapper.stack)
                # Remove empty stuff from the stack
                jobWrapper.stack = filter(lambda x : len(x) > 0, jobWrapper.stack)
                # Check if anything go removed
                if sum(map(len, jobWrapper.stack)) != stackSize:
                    changed[0] = True
                
            # Cleanup any services that have already been finished
            # Filter out deleted services and update the flags for services that exist
            # If there are services then renew  
            # the start and terminate flags if they have been removed
            def subFlagFile(jobStoreID, jobStoreFileID, flag):
                if self.fileExists(jobStoreFileID):
                    return jobStoreFileID
                
                # Make a new flag
                newFlag = self.getEmptyFileStoreID()
                
                # Load the jobWrapper for the service and initialise the link
                serviceJobWrapper = getJob(jobStoreID)
                
                if flag == 1:
                    logger.debug("Recreating a start service flag for job: %s, flag: %s", jobStoreID, newFlag)
                    serviceJobWrapper.startJobStoreID = newFlag
                elif flag == 2:
                    logger.debug("Recreating a terminate service flag for job: %s, flag: %s", jobStoreID, newFlag)
                    serviceJobWrapper.terminateJobStoreID = newFlag
                else:
                    logger.debug("Recreating a error service flag for job: %s, flag: %s", jobStoreID, newFlag)
                    assert flag == 3
                    serviceJobWrapper.errorJobStoreID = newFlag
                    
                # Update the service job on disk
                self.update(serviceJobWrapper)
                
                changed[0] = True
                
                return newFlag
            
            servicesSize = sum(map(len, jobWrapper.services))
            jobWrapper.services = filter(lambda z : len(z) > 0, map(lambda serviceJobList : 
                                        map(lambda x : x[:4] + (subFlagFile(x[0], x[4], 1), 
                                                                subFlagFile(x[0], x[5], 2), 
                                                                subFlagFile(x[0], x[6], 3)), 
                                        filter(lambda y : self.exists(y[0]), serviceJobList)), jobWrapper.services)) 
            if sum(map(len, jobWrapper.services)) != servicesSize:
                changed[0] = True

            # Reset the retry count of the jobWrapper
            if jobWrapper.remainingRetryCount != self._defaultTryCount():
                jobWrapper.remainingRetryCount = self._defaultTryCount()
                changed[0] = True

            # This cleans the old log file which may
            # have been left if the jobWrapper is being retried after a jobWrapper failure.
            if jobWrapper.logJobStoreFileID != None:
                self.delete(jobWrapper.logJobStoreFileID)
                jobWrapper.logJobStoreFileID = None
                changed[0] = True

            if changed[0]:  # Update, but only if a change has occurred
                logger.critical("Repairing job: %s" % jobWrapper.jobStoreID)
                self.update(jobWrapper)

        # Remove any crufty stats/logging files from the previous run
        logger.info("Discarding old statistics and logs...")
        self.readStatsAndLogging(lambda x: None)

        logger.info("Job store is clean")
        # TODO: reloading of the rootJob may be redundant here
        return self.loadRootJob()

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
        removed from the job store.
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
        removed from the job store.
        
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
