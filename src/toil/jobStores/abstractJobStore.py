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

import urllib2
import urlparse

import shutil

import re
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager, closing
from datetime import timedelta
from uuid import uuid4

from bd2k.util.retry import retry_http

from toil.job import JobException
from bd2k.util import memoize
from bd2k.util.objects import abstractclassmethod

try:
    import cPickle
except ImportError:
    import pickle as cPickle

import logging

logger = logging.getLogger(__name__)


class InvalidImportExportUrlException(Exception):
    def __init__(self, url):
        """
        :param urlparse.ParseResult url:
        """
        super(InvalidImportExportUrlException, self).__init__(
            "The URL '%s' is invalid" % url.geturl())


class NoSuchJobException(Exception):
    def __init__(self, jobStoreID):
        """
        Indicates that the specified job does not exist

        :param str jobStoreID: the jobStoreID that was mistakenly assumed to exist
        """
        super(NoSuchJobException, self).__init__("The job '%s' does not exist" % jobStoreID)


class ConcurrentFileModificationException(Exception):
    def __init__(self, jobStoreFileID):
        """
        Indicates that the file was attempted to be modified by multiple processes at once.

        :param str jobStoreFileID: the ID of the file that was modified by multiple workers
               or processes concurrently
        """
        super(ConcurrentFileModificationException, self).__init__(
            'Concurrent update to file %s detected.' % jobStoreFileID)


class NoSuchFileException(Exception):
    def __init__(self, jobStoreFileID, customName=None):
        """
        Indicates that the specified file does not exist

        :param str jobStoreFileID: the ID of the file that was mistakenly assumed to exist

        :param str customName: optionally, an alternate name for the nonexistent file
        """
        if customName is None:
            message = "File '%s' does not exist" % jobStoreFileID
        else:
            message = "File '%s' (%s) does not exist" % (customName, jobStoreFileID)
        super(NoSuchFileException, self).__init__(message)


class NoSuchJobStoreException(Exception):
    def __init__(self, locator):
        super(NoSuchJobStoreException, self).__init__(
            "The job store '%s' does not exist, so there is nothing to restart" % locator)


class JobStoreExistsException(Exception):
    def __init__(self, locator):
        super(JobStoreExistsException, self).__init__(
            "The job store '%s' already exists. Use --restart to resume the workflow, or remove "
            "the job store with 'toil clean' to start the workflow from scratch" % locator)


class AbstractJobStore(object):
    """ 
    Represents the physical storage for the jobs and files in a Toil workflow.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        """
        Create an instance of the job store. The instance will not be fully functional until
        either :meth:`.initialize` or :meth:`.resume` is invoked. Note that the :meth:`.destroy`
        method may be invoked on the object with or without prior invocation of either of these two
        methods.
        """
        self.__config = None

    def initialize(self, config):
        """
        Create the physical storage for this job store, allocate a workflow ID and persist the
        given Toil configuration to the store.

        :param toil.common.Config config: the Toil configuration to initialize this job store
               with. The given configuration will be updated with the newly allocated workflow ID.

        :raises JobStoreExistsException: if the physical storage for this job store already exists
        """
        assert config.workflowID is None
        config.workflowID = str(uuid4())
        logger.info("The workflow ID is: '%s'" % config.workflowID)
        self.__config = config
        self.writeConfig()

    def writeConfig(self):
        """
        Persists the value of the :attr:`.config` attribute to the job store, so that it can be
        retrieved later by other instances of this class.
        """
        with self.writeSharedFileStream('config.pickle', isProtected=False) as fileHandle:
            cPickle.dump(self.__config, fileHandle, cPickle.HIGHEST_PROTOCOL)

    def resume(self):
        """
        Connect this instance to the physical storage it represents and load the Toil configuration
        into the :attr:`.config` attribute.

        :raises NoSuchJobStoreException: if the physical storage for this job store doesn't exist
        """
        with self.readSharedFileStream('config.pickle') as fileHandle:
            config = cPickle.load(fileHandle)
            assert config.workflowID is not None
            self.__config = config

    @property
    def config(self):
        """
        The Toil configuration associated with this job store.

        :rtype: toil.common.Config
        """
        return self.__config

    rootJobStoreIDFileName = 'rootJobStoreID'

    def setRootJob(self, rootJobStoreID):
        """
        Set the root job of the workflow backed by this job store

        :param str rootJobStoreID: The ID of the job to set as root
        """
        with self.writeSharedFileStream(self.rootJobStoreIDFileName) as f:
            f.write(rootJobStoreID)

    def loadRootJob(self):
        """
        Loads the root job in the current job store.

        :raises toil.job.JobException: If no root job is set or if the root job doesn't exist in
                this job store
        :return: The root job.
        :rtype: toil.jobWrapper.JobWrapper
        """
        try:
            with self.readSharedFileStream(self.rootJobStoreIDFileName) as f:
                rootJobStoreID = f.read()
        except NoSuchFileException:
            raise JobException('No job has been set as the root in this job store')
        if not self.exists(rootJobStoreID):
            raise JobException("The root job '%s' doesn't exist. Either the Toil workflow "
                               "is finished or has never been started" % rootJobStoreID)
        return self.load(rootJobStoreID)

    # FIXME: This is only used in tests, why do we have it?

    def createRootJob(self, *args, **kwargs):
        """
        Create a new job and set it as the root job in this job store

        :rtype : toil.jobWrapper.JobWrapper
        """
        rootJob = self.create(*args, **kwargs)
        self.setRootJob(rootJob.jobStoreID)
        return rootJob

    @property
    @memoize
    def _jobStoreClasses(self):
        """
        A list of concrete AbstractJobStore implementations whose dependencies are installed.

        :rtype: list[AbstractJobStore]
        """
        jobStoreClassNames = (
            "toil.jobStores.azureJobStore.AzureJobStore",
            "toil.jobStores.fileJobStore.FileJobStore",
            "toil.jobStores.googleJobStore.GoogleJobStore",
            "toil.jobStores.aws.jobStore.AWSJobStore",
            "toil.jobStores.abstractJobStore.JobStoreSupport")
        jobStoreClasses = []
        for className in jobStoreClassNames:
            moduleName, className = className.rsplit('.', 1)
            from importlib import import_module
            try:
                module = import_module(moduleName)
            except ImportError:
                logger.info("Unable to import '%s'. You may want to try reinstalling Toil with "
                            "additional extras.", moduleName)
            else:
                jobStoreClass = getattr(module, className)
                jobStoreClasses.append(jobStoreClass)
        return jobStoreClasses

    def _findJobStoreForUrl(self, url, export=False):
        """
        Returns the AbstractJobStore subclass that supports the given URL.

        :param urlparse.ParseResult url: The given URL
        :param bool export: The URL for
        :rtype: toil.jobStore.AbstractJobStore
        """
        for jobStoreCls in self._jobStoreClasses:
            if jobStoreCls._supportsUrl(url, export):
                return jobStoreCls
        raise RuntimeError("No job store implementation supports %sporting for URL '%s'" %
                           ('ex' if export else 'im', url.geturl()))

    def importFile(self, srcUrl, sharedFileName=None):
        """
        Imports the file at the given URL into job store. The ID of the newly imported file is
        returned. If the name of a shared file name is provided, the file will be imported as
        such and None is returned.

        Currently supported schemes are:

            - 's3' for objects in Amazon S3
                e.g. s3://bucket/key

            - 'wasb' for blobs in Azure Blob Storage
                e.g. wasb://container/blob

            - 'file' for local files
                e.g. file:///local/file/path

            - 'http'
                e.g. http://someurl.com/path

        :param str srcUrl: URL that points to a file or object in the storage mechanism of a
                supported URL scheme e.g. a blob in an Azure Blob Storage container.

        :param str sharedFileName: Optional name to assign to the imported file within the job store

        :return The jobStoreFileId of the imported file or None if sharedFileName was given
        :rtype: str|None
        """
        # Note that the helper method _importFile is used to read from the source and write to
        # destination (which is the current job store in this case). To implement any
        # optimizations that circumvent this, the _importFile method should be overridden by
        # subclasses of AbstractJobStore.
        srcUrl = urlparse.urlparse(srcUrl)
        otherCls = self._findJobStoreForUrl(srcUrl)
        return self._importFile(otherCls, srcUrl, sharedFileName=sharedFileName)

    def _importFile(self, otherCls, url, sharedFileName=None):
        """
        Import the file at the given URL using the given job store class to retrieve that file.
        See also :meth:`.importFile`. This method applies a generic approach to importing: it
        asks the other job store class for a stream and writes that stream as eiher a regular or
        a shared file.

        :param AbstractJobStore  otherCls: The concrete subclass of AbstractJobStore that supports
               reading from the given URL.

        :param urlparse.ParseResult url: The location of the file to import.

        :param str sharedFileName: Optional name to assign to the imported file within the job store

        :return The jobStoreFileId of imported file or None if sharedFileName was given
        :rtype: str|None
        """
        if sharedFileName is None:
            with self.writeFileStream() as (writable, jobStoreFileID):
                otherCls._readFromUrl(url, writable)
                return jobStoreFileID
        else:
            self._requireValidSharedFileName(sharedFileName)
            with self.writeSharedFileStream(sharedFileName) as writable:
                otherCls._readFromUrl(url, writable)
                return None

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
        dstUrl = urlparse.urlparse(dstUrl)
        otherCls = self._findJobStoreForUrl(dstUrl, export=True)
        return self._exportFile(otherCls, jobStoreFileID, dstUrl)

    def _exportFile(self, otherCls, jobStoreFileID, url):
        """
        Refer to exportFile docstring for information about this method.

        :param AbstractJobStore otherCls: The concrete subclass of AbstractJobStore that supports
               exporting to the given URL. Note that the type annotation here is not completely
               accurate. This is not an instance, it's a class, but there is no way to reflect
               that in PEP-484 type hints.

        :param str jobStoreFileID: The id of the file that will be exported.

        :param urlparse.ParseResult url: The parsed URL of the file to export to.
        """
        with self.readFileStream(jobStoreFileID) as readable:
            otherCls._writeToUrl(readable, url)

    @abstractclassmethod
    def _readFromUrl(cls, url, writable):
        """
        Reads the contents of the object at the specified location and writes it to the given
        writable stream.

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param urlparse.ParseResult url: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an Azure Blob Storage container.

        :param writable: a writable stream
        """
        raise NotImplementedError()

    @abstractclassmethod
    def _writeToUrl(cls, readable, url):
        """
        Reads the contents of the given readable stream and writes it to the object at the
        specified location.

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param urlparse.ParseResult url: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an Azure Blob Storage container.

        :param readable: a readable stream
        """
        raise NotImplementedError()

    @abstractclassmethod
    def _supportsUrl(cls, url, export=False):
        """
        Returns True if the job store supports the URL's scheme.

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param bool export: Determines if the url is supported for exported
        :param urlparse.ParseResult url: a parsed URL that may be supported
        :return bool: returns true if the cls supports the URL
        """
        raise NotImplementedError()

    @abstractmethod
    def destroy(self):
        """
        The inverse of :meth:`.initialize`, this method deletes the physical storage represented
        by this instance. While not being atomic, this method *is* at least idempotent,
        as a means to counteract potential issues with eventual consistency exhibited by the
        underlying storage mechanisms. This means that if the method fails (raises an exception),
        it may (and should be) invoked again. If the underlying storage mechanism is eventually
        consistent, even a successful invocation is not an ironclad guarantee that the physical
        storage vanished completely and immediately. A successful invocation only guarantees that
        the deletion will eventually happen. It is therefore recommended to not immediately reuse
        the same job store location for a new Toil workflow.
        """
        raise NotImplementedError()

    def getEnv(self):
        """
        Returns a dictionary of environment variables that this job store requires to be set in
        order to function properly on a worker.

        :rtype: dict[str,str]
        """
        return {}

    # Cleanup functions

    def clean(self, jobCache=None):
        """
        Function to cleanup the state of a job store after a restart.
        Fixes jobs that might have been partially updated. Resets the try counts and removes jobs
        that are not successors of the current root job.

        :param dict[str,toil.jobWrapper.JobWrapper] jobCache: if a value it must be a dict
               from job ID keys to JobWrapper object values. Jobs will be loaded from the cache
               (which can be downloaded from the job store in a batch) instead of piecemeal when 
               recursed into.
        """
        if jobCache is None:
            logger.warning("Cleaning jobStore recursively. This may be slow.")

        # Functions to get and check the existence of jobs, using the jobCache
        # if present
        def getJob(jobId):
            if jobCache is not None:
                try:
                    return jobCache[jobId]
                except KeyError:
                    self.load(jobId)
            else:
                return self.load(jobId)

        def haveJob(jobId):
            if jobCache is not None:
                if jobCache.has_key(jobId):
                    return True
                else:
                    return self.exists(jobId)
            else:
                return self.exists(jobId)

        def getJobs():
            if jobCache is not None:
                return jobCache.itervalues()
            else:
                return self.jobs()

        # Iterate from the root jobWrapper and collate all jobs that are reachable from it
        # All other jobs returned by self.jobs() are orphaned and can be removed
        reachableFromRoot = set()

        def getConnectedJobs(jobWrapper):
            if jobWrapper.jobStoreID in reachableFromRoot:
                return
            reachableFromRoot.add(jobWrapper.jobStoreID)
            # Traverse jobs in stack
            for jobs in jobWrapper.stack:
                for successorJobStoreID in map(lambda x: x[0], jobs):
                    if (successorJobStoreID not in reachableFromRoot
                        and haveJob(successorJobStoreID)):
                        getConnectedJobs(getJob(successorJobStoreID))
            # Traverse service jobs
            for jobs in jobWrapper.services:
                for serviceJobStoreID in map(lambda x: x[0], jobs):
                    if haveJob(serviceJobStoreID):
                        assert serviceJobStoreID not in reachableFromRoot
                        reachableFromRoot.add(serviceJobStoreID)

        logger.info("Checking job graph connectivity...")
        getConnectedJobs(self.loadRootJob())
        logger.info("%d jobs reachable from root." % len(reachableFromRoot))

        # Cleanup jobs that are not reachable from the root, and therefore orphaned
        jobsToDelete = filter(lambda x: x.jobStoreID not in reachableFromRoot, getJobs())
        for jobWrapper in jobsToDelete:
            # clean up any associated files before deletion
            for fileID in jobWrapper.filesToDelete:
                # Delete any files that should already be deleted
                logger.warn("Deleting file '%s'. It is marked for deletion but has not yet been "
                            "removed.", fileID)
                self.deleteFile(fileID)
            # Delete the job
            self.delete(jobWrapper.jobStoreID)

        # Clean up jobs that are in reachable from the root
        for jobWrapper in (getJob(x) for x in reachableFromRoot):
            # jobWrappers here are necessarily in reachable from root.

            changed = [False]  # This is a flag to indicate the jobWrapper state has
            # changed

            # If the job has files to delete delete them.
            if len(jobWrapper.filesToDelete) != 0:
                # Delete any files that should already be deleted
                for fileID in jobWrapper.filesToDelete:
                    logger.critical("Removing file in job store: %s that was "
                                    "marked for deletion but not previously removed" % fileID)
                    self.deleteFile(fileID)
                jobWrapper.filesToDelete = []
                changed[0] = True

            # For a job whose command is already executed, remove jobs from the stack that are
            # already deleted. This cleans up the case that the jobWrapper had successors to run,
            # but had not been updated to reflect this.
            if jobWrapper.command is None:
                stackSizeFn = lambda: sum(map(len, jobWrapper.stack))
                startStackSize = stackSizeFn()
                # Remove deleted jobs
                jobWrapper.stack = map(lambda x: filter(lambda y: self.exists(y[0]), x),
                                       jobWrapper.stack)
                # Remove empty stuff from the stack
                jobWrapper.stack = filter(lambda x: len(x) > 0, jobWrapper.stack)
                # Check if anything got removed
                if stackSizeFn() != startStackSize:
                    changed[0] = True

            # Cleanup any services that have already been finished.
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
                    logger.debug("Recreating a start service flag for job: %s, flag: %s",
                                 jobStoreID, newFlag)
                    serviceJobWrapper.startJobStoreID = newFlag
                elif flag == 2:
                    logger.debug("Recreating a terminate service flag for job: %s, flag: %s",
                                 jobStoreID, newFlag)
                    serviceJobWrapper.terminateJobStoreID = newFlag
                else:
                    logger.debug("Recreating a error service flag for job: %s, flag: %s",
                                 jobStoreID, newFlag)
                    assert flag == 3
                    serviceJobWrapper.errorJobStoreID = newFlag

                # Update the service job on disk
                self.update(serviceJobWrapper)

                changed[0] = True

                return newFlag

            servicesSizeFn = lambda: sum(map(len, jobWrapper.services))
            startServicesSize = servicesSizeFn()
            jobWrapper.services = filter(
                lambda z: len(z) > 0,
                map(lambda serviceJobList:
                    map(lambda x: x[:4] + (subFlagFile(x[0], x[4], 1),
                                           subFlagFile(x[0], x[5], 2),
                                           subFlagFile(x[0], x[6], 3)),
                        filter(lambda y: self.exists(y[0]), serviceJobList)), jobWrapper.services))
            if servicesSizeFn() != startServicesSize:
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
    def create(self, command, memory, cores, disk, preemptable, predecessorNumber=0):
        """
        Creates a jobWrapper with specified resources and command, adds it to the job store and
        returns it.
        
        :param str command: the shell command that will be executed when the job is being run

        :param int memory: the amount of RAM in bytes needed to run the job

        :param float cores: the number of cores needed to run the job

        :param int disk: the amount of disk in bytes needed to run the job

        :param bool preemptable: whether the job can be run on a preemptable node

        :param int predecessorNumber: argument to the job constructor. Specifies the number of
               other jobWrappers that specify this job in their stack

        :return: the newly created jobWrapper object
        :rtype: toil.jobWrapper.JobWrapper
        """
        raise NotImplementedError()

    @abstractmethod
    def exists(self, jobStoreID):
        """
        Indicates whether the job with the specified jobStoreID exists in the job store

        :rtype: bool
        """
        raise NotImplementedError()

    # One year should be sufficient to finish any pipeline ;-)
    publicUrlExpiration = timedelta(days=365)

    @abstractmethod
    def getPublicUrl(self, fileName):
        """
        Returns a publicly accessible URL to the given file in the job store. The returned URL may
        expire as early as 1h after its been returned. Throw an exception if the file does not
        exist.

        :param str fileName: the jobStoreFileID of the file to generate a URL for

        :raise NoSuchFileException: if the specified file does not exist in this job store

        :rtype: str
        """
        raise NotImplementedError()

    @abstractmethod
    def getSharedPublicUrl(self, sharedFileName):
        """
        Differs from :meth:`getPublicUrl` in that this method is for generating URLs for shared
        files written by :meth:`writeSharedFileStream`.

        Returns a publicly accessible URL to the given file in the job store. The returned URL
        starts with 'http:',  'https:' or 'file:'. The returned URL may expire as early as 1h
        after its been returned. Throw an exception if the file does not exist.

        :param str sharedFileName: The name of the shared file to generate a publically accessible url for.

        :raise NoSuchFileException: raised if the specified file does not exist in the store

        :rtype: str
        """
        raise NotImplementedError()

    @abstractmethod
    def load(self, jobStoreID):
        """
        Loads the job referenced by the given ID and returns it.

        :param str jobStoreID: the ID of the job to load

        :raise NoSuchJobException: if there is no job with the given ID

        :rtype: toil.jobWrapper.JobWrapper
        """
        raise NotImplementedError()

    @abstractmethod
    def update(self, job):
        """
        Persists the job in this store atomically.

        :param toil.jobWrapper.JobWrapper job: the job to write to this job store
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, jobStoreID):
        """
        Removes from store atomically, can not then subsequently call load(), write(), update(),
        etc. with the job.

        This operation is idempotent, i.e. deleting a job twice or deleting a non-existent job
        will succeed silently.

        :param str jobStoreID: the ID of the job to delete from this job store
        """
        raise NotImplementedError()

    def jobs(self):
        """
        Best effort attempt to return iterator on all jobs in the store. The iterator may not
        return all jobs and may also contain orphaned jobs that have already finished succesfully
        and should not be rerun. To guarantee you get any and all jobs that can be run instead
        construct a more expensive ToilState object

        :return: Returns iterator on jobs in the store. The iterator may or may not contain all jobs and may contain
                 invalid jobs
        :rtype: Iterator[toil.jobWrapper.JobWrapper]
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

        :param str localFilePath: the path to the local file that will be uploaded to the job store.

        :param str|None jobStoreID: If specified the file will be associated with that job and when
               jobStore.delete(job) is called all files written with the given job.jobStoreID will
               be removed from the job store.

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method

        :raise NoSuchJobException: if the job specified via jobStoreID does not exist

        FIXME: some implementations may not raise this

        :return: an ID referencing the newly created file and can be used to read the
                 file in the future.
        :rtype: str
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

        :param str jobStoreID: the id of a job, or None. If specified, the file will be associated
               with that job and when when jobStore.delete(job) is called all files written with the
               given job.jobStoreID will be removed from the job store.

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method

        :raise NoSuchJobException: if the job specified via jobStoreID does not exist

        FIXME: some implementations may not raise this

        :return: an ID that references the newly created file and can be used to read the
                 file in the future.
        :rtype: str
        """
        raise NotImplementedError()

    @abstractmethod
    def getEmptyFileStoreID(self, jobStoreID=None):
        """
        Creates an empty file in the job store and returns its ID.
        Call to fileExists(getEmptyFileStoreID(jobStoreID)) will return True.
        
        :param str jobStoreID: the id of a job, or None. If specified, the file will be associated with
               that job and when jobStore.delete(job) is called a best effort attempt is made to delete
               all files written with the given job.jobStoreID

        :return: a jobStoreFileID that references the newly created file and can be used to reference the
                 file in the future.
        :rtype: str
        """
        raise NotImplementedError()

    @abstractmethod
    def readFile(self, jobStoreFileID, localFilePath):
        """
        Copies the file referenced by jobStoreFileID to the given local file path. The version 
        will be consistent with the last copy of the file written/updated. 
        
        The file at the given local path may not be modified after this method returns! 

        :param str jobStoreFileID: ID of the file to be copied

        :param str localFilePath: the local path indicating where to place the contents of the 
               given file in the job store 
        """
        raise NotImplementedError()

    @abstractmethod
    @contextmanager
    def readFileStream(self, jobStoreFileID):
        """
        Similar to readFile, but returns a context manager yielding a file handle which can be
        read from. The yielded file handle does not need to and should not be closed explicitly.

        :param str jobStoreFileID: ID of the file to get a readable file handle for
        """
        raise NotImplementedError()

    @abstractmethod
    def deleteFile(self, jobStoreFileID):
        """
        Deletes the file with the given ID from this job store. This operation is idempotent, i.e.
        deleting a file twice or deleting a non-existent file will succeed silently.

        :param str jobStoreFileID: ID of the file to delete
        """
        raise NotImplementedError()

    @abstractmethod
    def fileExists(self, jobStoreFileID):
        """
        Determine whether a file exists in this job store.

        :param str jobStoreFileID: an ID referencing the file to be checked

        :rtype: bool
        """
        raise NotImplementedError()

    @abstractmethod
    def updateFile(self, jobStoreFileID, localFilePath):
        """
        Replaces the existing version of a file in the job store. Throws an exception if the file
        does not exist.

        :param str jobStoreFileID: the ID of the file in the job store to be updated

        :param str localFilePath: the local path to a file that will overwrite the current version
          in the job store

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method

        :raise NoSuchFileException: if the specified file does not exist
        """
        raise NotImplementedError()

    @abstractmethod
    def updateFileStream(self, jobStoreFileID):
        """
        Replaces the existing version of a file in the job store. Similar to writeFile, but
        returns a context manager yielding a file handle which can be written to. The
        yielded file handle does not need to and should not be closed explicitly.

        :param str jobStoreFileID: the ID of the file in the job store to be updated

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method

        :raise NoSuchFileException: if the specified file does not exist
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
        Returns a context manager yielding a writable file handle to the global file referenced
        by the given name.

        :param str sharedFileName: A file name matching AbstractJobStore.fileNameRegex, unique within
               this job store

        :param bool isProtected: True if the file must be encrypted, None if it may be encrypted or
               False if it must be stored in the clear.

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method
        """
        raise NotImplementedError()

    @abstractmethod
    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        """
        Returns a context manager yielding a readable file handle to the global file referenced
        by the given name.

        :param str sharedFileName: A file name matching AbstractJobStore.fileNameRegex, unique within
               this job store
        """
        raise NotImplementedError()

    @abstractmethod
    def writeStatsAndLogging(self, statsAndLoggingString):
        """
        Adds the given statistics/logging string to the store of statistics info.

        :param str statsAndLoggingString: the string to be written to the stats file

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method
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

        :param Callable callback: a function to be applied to each of the stats file handles found

        :param bool readAll: a boolean indicating whether to read the already processed stats files
               in addition to the unread stats files

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method

        :return: the number of stats files processed
        :rtype: int
        """
        raise NotImplementedError()

    ## Helper methods for subclasses

    def _defaultTryCount(self):
        return int(self.config.retryCount + 1)

    @classmethod
    def _validateSharedFileName(cls, sharedFileName):
        return bool(cls.sharedFileNameRegex.match(sharedFileName))

    @classmethod
    def _requireValidSharedFileName(cls, sharedFileName):
        if not cls._validateSharedFileName(sharedFileName):
            raise ValueError("Not a valid shared file name: '%s'." % sharedFileName)


class JobStoreSupport(AbstractJobStore):
    __metaclass__ = ABCMeta

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() in ('http', 'https') and not export

    @classmethod
    def _readFromUrl(cls, url, writable):
        for attempt in retry_http():
            with attempt:
                with closing(urllib2.urlopen(url.geturl())) as readable:
                    shutil.copyfileobj(readable, writable)
