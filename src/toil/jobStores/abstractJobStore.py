# Copyright (C) 2015-2021 Regents of the University of California
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
import logging
import pickle
import re
import shutil
import urllib.parse as urlparse
from abc import ABC, ABCMeta, abstractmethod
from contextlib import closing, contextmanager
from datetime import timedelta
from http.client import BadStatusLine
from urllib.request import urlopen
from uuid import uuid4
from typing import Set, Union
from requests.exceptions import HTTPError

from toil.common import safeUnpickleFromStream
from toil.fileStores import FileID
from toil.job import (CheckpointJobDescription,
                      TemporaryID,
                      JobException,
                      ServiceJobDescription)
from toil.lib.memoize import memoize
from toil.lib.io import WriteWatchingStream
from toil.lib.retry import ErrorCondition, retry

logger = logging.getLogger(__name__)

try:
    from botocore.exceptions import ProxyConnectionError
except ImportError:
    class ProxyConnectionError(BaseException):
        pass

class InvalidImportExportUrlException(Exception):
    def __init__(self, url):
        """
        :param urlparse.ParseResult url:
        """
        super().__init__("The URL '%s' is invalid." % url.geturl())


class NoSuchJobException(Exception):
    """Indicates that the specified job does not exist."""
    def __init__(self, jobStoreID):
        """
        :param str jobStoreID: the jobStoreID that was mistakenly assumed to exist
        """
        super().__init__("The job '%s' does not exist." % jobStoreID)


class ConcurrentFileModificationException(Exception):
    """Indicates that the file was attempted to be modified by multiple processes at once."""
    def __init__(self, jobStoreFileID):
        """
        :param str jobStoreFileID: the ID of the file that was modified by multiple workers
               or processes concurrently
        """
        super().__init__('Concurrent update to file %s detected.' % jobStoreFileID)


class NoSuchFileException(Exception):
    """Indicates that the specified file does not exist."""
    def __init__(self, jobStoreFileID, customName=None, *extra):
        """
        :param str jobStoreFileID: the ID of the file that was mistakenly assumed to exist
        :param str customName: optionally, an alternate name for the nonexistent file
        :param list extra: optional extra information to add to the error message
        """
        # Having the extra argument may help resolve the __init__() takes at
        # most three arguments error reported in
        # https://github.com/DataBiosphere/toil/issues/2589#issuecomment-481912211
        if customName is None:
            message = "File '%s' does not exist." % jobStoreFileID
        else:
            message = "File '%s' (%s) does not exist." % (customName, jobStoreFileID)

        if extra:
            # Append extra data.
            message += " Extra info: " + " ".join((str(x) for x in extra))

        super().__init__(message)


class NoSuchJobStoreException(Exception):
    """Indicates that the specified job store does not exist."""
    def __init__(self, locator):
        super().__init__("The job store '%s' does not exist, so there is nothing to restart." % locator)


class JobStoreExistsException(Exception):
    """Indicates that the specified job store already exists."""
    def __init__(self, locator):
        super().__init__(
            "The job store '%s' already exists. Use --restart to resume the workflow, or remove "
            "the job store with 'toil clean' to start the workflow from scratch." % locator)


class AbstractJobStore(ABC):
    """
    Represents the physical storage for the jobs and files in a Toil workflow.

    JobStores are responsible for storing :class:`toil.job.JobDescription`
    (which relate jobs to each other) and files.

    Actual :class:`toil.job.Job` objects are stored in files, referenced by
    JobDescriptions. All the non-file CRUD methods the JobStore provides deal
    in JobDescriptions and not full, executable Jobs.

    To actually get ahold of a :class:`toil.job.Job`, use
    :meth:`toil.job.Job.loadJob` with a JobStore and the relevant JobDescription.
    """

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
        logger.debug("The workflow ID is: '%s'" % config.workflowID)
        self.__config = config
        self.writeConfig()

    def writeConfig(self):
        """
        Persists the value of the :attr:`AbstractJobStore.config` attribute to the
        job store, so that it can be retrieved later by other instances of this class.
        """
        with self.writeSharedFileStream('config.pickle', isProtected=False) as fileHandle:
            pickle.dump(self.__config, fileHandle, pickle.HIGHEST_PROTOCOL)

    def resume(self):
        """
        Connect this instance to the physical storage it represents and load the Toil configuration
        into the :attr:`AbstractJobStore.config` attribute.

        :raises NoSuchJobStoreException: if the physical storage for this job store doesn't exist
        """
        with self.readSharedFileStream('config.pickle') as fileHandle:
            config = safeUnpickleFromStream(fileHandle)
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
            f.write(rootJobStoreID.encode('utf-8'))

    def loadRootJob(self):
        """
        Loads the JobDescription for the root job in the current job store.

        :raises toil.job.JobException: If no root job is set or if the root job doesn't exist in
                this job store
        :return: The root job.
        :rtype: toil.job.JobDescription
        """
        try:
            with self.readSharedFileStream(self.rootJobStoreIDFileName) as f:
                rootJobStoreID = f.read().decode('utf-8')
        except NoSuchFileException:
            raise JobException('No job has been set as the root in this job store')
        if not self.exists(rootJobStoreID):
            raise JobException("The root job '%s' doesn't exist. Either the Toil workflow "
                               "is finished or has never been started" % rootJobStoreID)
        return self.load(rootJobStoreID)

    # FIXME: This is only used in tests, why do we have it?

    def createRootJob(self, desc):
        """
        Create the given JobDescription and set it as the root job in this job store

        :param toil.job.JobDescription desc: JobDescription to save and make the root job.
        :rtype: toil.job.JobDescription
        """
        self.create(desc)
        self.setRootJob(desc.jobStoreID)
        return desc

    def getRootJobReturnValue(self):
        """
        Parse the return value from the root job.

        Raises an exception if the root job hasn't fulfilled its promise yet.
        """
        # Parse out the return value from the root job
        with self.readSharedFileStream('rootJobReturnValue') as fH:
            return safeUnpickleFromStream(fH)

    @property
    @memoize
    def _jobStoreClasses(self):
        """
        A list of concrete AbstractJobStore implementations whose dependencies are installed.

        :rtype: list[AbstractJobStore]
        """
        jobStoreClassNames = (
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
            except (ImportError, ProxyConnectionError):
                logger.debug("Unable to import '%s' as is expected if the corresponding extra was "
                             "omitted at installation time.", moduleName)
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

    def importFile(self, srcUrl, sharedFileName=None, hardlink=False, symlink=False):
        """
        Imports the file at the given URL into job store. The ID of the newly imported file is
        returned. If the name of a shared file name is provided, the file will be imported as
        such and None is returned. If an executable file on the local filesystem is uploaded, its
        executability will be preserved when it is downloaded.

        Currently supported schemes are:

            - 's3' for objects in Amazon S3
                e.g. s3://bucket/key

            - 'file' for local files
                e.g. file:///local/file/path

            - 'http'
                e.g. http://someurl.com/path

            - 'gs'
                e.g. gs://bucket/file

        :param str srcUrl: URL that points to a file or object in the storage mechanism of a
                supported URL scheme e.g. a blob in an AWS s3 bucket.

        :param str sharedFileName: Optional name to assign to the imported file within the job store

        :return: The jobStoreFileId of the imported file or None if sharedFileName was given
        :rtype: toil.fileStores.FileID or None
        """
        # Note that the helper method _importFile is used to read from the source and write to
        # destination (which is the current job store in this case). To implement any
        # optimizations that circumvent this, the _importFile method should be overridden by
        # subclasses of AbstractJobStore.
        srcUrl = urlparse.urlparse(srcUrl)
        otherCls = self._findJobStoreForUrl(srcUrl)
        return self._importFile(otherCls, srcUrl, sharedFileName=sharedFileName, hardlink=hardlink, symlink=symlink)

    def _importFile(self, otherCls, url, sharedFileName=None, hardlink=False, symlink=False):
        """
        Import the file at the given URL using the given job store class to retrieve that file.
        See also :meth:`.importFile`. This method applies a generic approach to importing: it
        asks the other job store class for a stream and writes that stream as either a regular or
        a shared file.

        :param AbstractJobStore otherCls: The concrete subclass of AbstractJobStore that supports
               reading from the given URL and getting the file size from the URL.

        :param urlparse.ParseResult url: The location of the file to import.

        :param str sharedFileName: Optional name to assign to the imported file within the job store

        :return The jobStoreFileId of imported file or None if sharedFileName was given
        :rtype: toil.fileStores.FileID or None
        """
        if sharedFileName is None:
            with self.writeFileStream() as (writable, jobStoreFileID):
                size, executable = otherCls._readFromUrl(url, writable)
                return FileID(jobStoreFileID, size, executable)
        else:
            self._requireValidSharedFileName(sharedFileName)
            with self.writeSharedFileStream(sharedFileName) as writable:
                otherCls._readFromUrl(url, writable)
                return None

    def exportFile(self, jobStoreFileID, dstUrl):
        """
        Exports file to destination pointed at by the destination URL. The exported file will be
        executable if and only if it was originally uploaded from an executable file on the
        local filesystem.

        Refer to :meth:`.AbstractJobStore.importFile` documentation for currently supported URL schemes.

        Note that the helper method _exportFile is used to read from the source and write to
        destination. To implement any optimizations that circumvent this, the _exportFile method
        should be overridden by subclasses of AbstractJobStore.

        :param str jobStoreFileID: The id of the file in the job store that should be exported.
        :param str dstUrl: URL that points to a file or object in the storage mechanism of a
                supported URL scheme e.g. a blob in an AWS s3 bucket.
        """
        dstUrl = urlparse.urlparse(dstUrl)
        otherCls = self._findJobStoreForUrl(dstUrl, export=True)
        self._exportFile(otherCls, jobStoreFileID, dstUrl)

    def _exportFile(self, otherCls, jobStoreFileID, url):
        """
        Refer to exportFile docstring for information about this method.

        :param AbstractJobStore otherCls: The concrete subclass of AbstractJobStore that supports
               exporting to the given URL. Note that the type annotation here is not completely
               accurate. This is not an instance, it's a class, but there is no way to reflect
               that in :pep:`484` type hints.

        :param str jobStoreFileID: The id of the file that will be exported.

        :param urlparse.ParseResult url: The parsed URL of the file to export to.
        """
        self._defaultExportFile(otherCls, jobStoreFileID, url)

    def _defaultExportFile(self, otherCls, jobStoreFileID, url):
        """
        Refer to exportFile docstring for information about this method.

        :param AbstractJobStore otherCls: The concrete subclass of AbstractJobStore that supports
               exporting to the given URL. Note that the type annotation here is not completely
               accurate. This is not an instance, it's a class, but there is no way to reflect
               that in :pep:`484` type hints.

        :param str jobStoreFileID: The id of the file that will be exported.

        :param urlparse.ParseResult url: The parsed URL of the file to export to.
        """
        executable = False
        with self.readFileStream(jobStoreFileID) as readable:
            if getattr(jobStoreFileID, 'executable', False):
                executable = jobStoreFileID.executable
            otherCls._writeToUrl(readable, url, executable)

    @classmethod
    @abstractmethod
    def getSize(cls, url):
        """
        Get the size in bytes of the file at the given URL, or None if it cannot be obtained.

        :param urlparse.ParseResult url: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def _readFromUrl(cls, url, writable):
        """
        Reads the contents of the object at the specified location and writes it to the given
        writable stream.

        Refer to :func:`~AbstractJobStore.importFile` documentation for currently supported URL schemes.

        :param urlparse.ParseResult url: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.

        :param writable: a writable stream

        :return: The size of the file in bytes and whether the executable permission bit is set
        :rtype: Tuple[int, bool]
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def _writeToUrl(cls, readable, url):
        """
        Reads the contents of the given readable stream and writes it to the object at the
        specified location.

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param urlparse.ParseResult url: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.

        :param readable: a readable stream
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
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

        :param dict[str,toil.job.JobDescription] jobCache: if a value it must be a dict
               from job ID keys to JobDescription object values. Jobs will be loaded from the cache
               (which can be downloaded from the job store in a batch) instead of piecemeal when
               recursed into.
        """
        if jobCache is None:
            logger.warning("Cleaning jobStore recursively. This may be slow.")

        # Functions to get and check the existence of jobs, using the jobCache if present
        def getJobDescription(jobId):
            if jobCache is not None:
                try:
                    return jobCache[jobId]
                except KeyError:
                    return self.load(jobId)
            else:
                return self.load(jobId)

        def haveJob(jobId):
            assert len(jobId) > 1, "Job ID {} too short; is a string being used as a list?".format(jobId)
            if jobCache is not None:
                if jobId in jobCache:
                    return True
                else:
                    return self.exists(jobId)
            else:
                return self.exists(jobId)

        def deleteJob(jobId):
            if jobCache is not None:
                if jobId in jobCache:
                    del jobCache[jobId]
            self.delete(jobId)

        def updateJobDescription(jobDescription):
            jobCache[jobDescription.jobStoreID] = jobDescription
            self.update(jobDescription)

        def getJobDescriptions():
            if jobCache is not None:
                return jobCache.values()
            else:
                return self.jobs()

        def get_jobs_reachable_from_root() -> Set[Union[TemporaryID, str]]:
            """
            Traverse the job graph from the root job and return a flattened set of all active jobstore IDs.

            Note: Jobs returned by self.jobs(), but not this function, are orphaned, and can be removed as dead jobs.
            """
            # Iterate from the root JobDescription and collate all jobs that are reachable from it.
            root_job_description = self.loadRootJob()
            reachable_from_root = set()

            # Add first root job outside of the loop below.
            reachable_from_root.add(root_job_description.jobStoreID)
            # add all of root's linked service jobs as well
            for service_jobstore_id in root_job_description.services:
                if haveJob(service_jobstore_id):
                    reachable_from_root.add(service_jobstore_id)

            # Unprocessed means it might have successor jobs we need to add.
            unprocessed_job_descriptions = [root_job_description]

            while unprocessed_job_descriptions:
                new_job_descriptions_to_process = []  # Reset.
                for job_description in unprocessed_job_descriptions:
                    for jobs in job_description.stack:
                        for successor_jobstore_id in jobs:
                            if successor_jobstore_id not in reachable_from_root and haveJob(successor_jobstore_id):
                                successor_job_description = getJobDescription(successor_jobstore_id)

                                # Add each successor job.
                                reachable_from_root.add(successor_job_description.jobStoreID)
                                # Add all of the successor's linked service jobs as well.
                                for service_jobstore_id in successor_job_description.services:
                                    if haveJob(service_jobstore_id):
                                        reachable_from_root.add(service_jobstore_id)

                                new_job_descriptions_to_process.append(successor_job_description)
                unprocessed_job_descriptions = new_job_descriptions_to_process

            logger.debug(f"{len(reachable_from_root)} jobs reachable from root.")
            return reachable_from_root

        reachable_from_root = get_jobs_reachable_from_root()

        # Cleanup jobs that are not reachable from the root, and therefore orphaned
        # TODO: Avoid reiterating reachable_from_root (which may be very large)
        jobsToDelete = [x for x in getJobDescriptions() if x.jobStoreID not in reachable_from_root]
        for jobDescription in jobsToDelete:
            # clean up any associated files before deletion
            for fileID in jobDescription.filesToDelete:
                # Delete any files that should already be deleted
                logger.warning(f"Deleting file '{fileID}'. It is marked for deletion but has not yet been removed.")
                self.deleteFile(fileID)
            # Delete the job from us and the cache
            deleteJob(jobDescription.jobStoreID)

        jobDescriptionsReachableFromRoot = {id: getJobDescription(id) for id in reachable_from_root}

        # Clean up any checkpoint jobs -- delete any successors it
        # may have launched, and restore the job to a pristine state
        jobsDeletedByCheckpoints = set()
        for jobDescription in [desc for desc in jobDescriptionsReachableFromRoot.values() if isinstance(desc, CheckpointJobDescription)]:
            if jobDescription.jobStoreID in jobsDeletedByCheckpoints:
                # This is a checkpoint that was nested within an
                # earlier checkpoint, so it and all its successors are
                # already gone.
                continue
            if jobDescription.checkpoint is not None:
                # The checkpoint actually started and needs to be restarted
                logger.debug("Restarting checkpointed job %s" % jobDescription)
                deletedThisRound = jobDescription.restartCheckpoint(self)
                jobsDeletedByCheckpoints |= set(deletedThisRound)
                updateJobDescription(jobDescription)
        for jobID in jobsDeletedByCheckpoints:
            del jobDescriptionsReachableFromRoot[jobID]

        # Clean up jobs that are in reachable from the root
        for jobDescription in jobDescriptionsReachableFromRoot.values():
            # jobDescription here are necessarily in reachable from root.

            changed = [False]  # This is a flag to indicate the jobDescription state has
            # changed

            # If the job has files to delete delete them.
            if len(jobDescription.filesToDelete) != 0:
                # Delete any files that should already be deleted
                for fileID in jobDescription.filesToDelete:
                    logger.critical("Removing file in job store: %s that was "
                                    "marked for deletion but not previously removed" % fileID)
                    self.deleteFile(fileID)
                jobDescription.filesToDelete = []
                changed[0] = True

            # For a job whose command is already executed, remove jobs from the stack that are
            # already deleted. This cleans up the case that the jobDescription had successors to run,
            # but had not been updated to reflect this.
            if jobDescription.command is None:
                stackSizeFn = lambda: sum(map(len, jobDescription.stack))
                startStackSize = stackSizeFn()
                # Remove deleted jobs
                jobDescription.filterSuccessors(haveJob)
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
                newFlag = self.getEmptyFileStoreID(jobStoreID, cleanup=False)

                # Load the jobDescription for the service and initialise the link
                serviceJobDescription = getJobDescription(jobStoreID)

                # Make sure it really is a service
                assert isinstance(serviceJobDescription, ServiceJobDescription)

                if flag == 1:
                    logger.debug("Recreating a start service flag for job: %s, flag: %s",
                                 jobStoreID, newFlag)
                    serviceJobDescription.startJobStoreID = newFlag
                elif flag == 2:
                    logger.debug("Recreating a terminate service flag for job: %s, flag: %s",
                                 jobStoreID, newFlag)
                    serviceJobDescription.terminateJobStoreID = newFlag
                else:
                    logger.debug("Recreating a error service flag for job: %s, flag: %s",
                                 jobStoreID, newFlag)
                    assert flag == 3
                    serviceJobDescription.errorJobStoreID = newFlag

                # Update the service job on disk
                updateJobDescription(serviceJobDescription)

                changed[0] = True

                return newFlag

            servicesSizeFn = lambda: len(jobDescription.services)
            startServicesSize = servicesSizeFn()

            def replaceFlagsIfNeeded(serviceJobDescription):
                # Make sure it really is a service
                assert isinstance(serviceJobDescription, ServiceJobDescription)
                serviceJobDescription.startJobStoreID = subFlagFile(serviceJobDescription.jobStoreID, serviceJobDescription.startJobStoreID, 1)
                serviceJobDescription.terminateJobStoreID = subFlagFile(serviceJobDescription.jobStoreID, serviceJobDescription.terminateJobStoreID, 2)
                serviceJobDescription.errorJobStoreID = subFlagFile(serviceJobDescription.jobStoreID, serviceJobDescription.errorJobStoreID, 3)

            # remove all services that no longer exist
            jobDescription.filterServiceHosts(haveJob)

            for serviceID in jobDescription.services:
                replaceFlagsIfNeeded(getJobDescription(serviceID))

            if servicesSizeFn() != startServicesSize:
                changed[0] = True

            # Reset the try count of the JobDescription so it will use the default.
            changed[0] |= jobDescription.clearRemainingTryCount()

            # This cleans the old log file which may
            # have been left if the job is being retried after a failure.
            if jobDescription.logJobStoreFileID != None:
                self.deleteFile(jobDescription.logJobStoreFileID)
                jobDescription.logJobStoreFileID = None
                changed[0] = True

            if changed[0]:  # Update, but only if a change has occurred
                logger.critical("Repairing job: %s" % jobDescription.jobStoreID)
                updateJobDescription(jobDescription)

        # Remove any crufty stats/logging files from the previous run
        logger.debug("Discarding old statistics and logs...")
        # We have to manually discard the stream to avoid getting
        # stuck on a blocking write from the job store.
        def discardStream(stream):
            """Read the stream 4K at a time until EOF, discarding all input."""
            while len(stream.read(4096)) != 0:
                pass
        self.readStatsAndLogging(discardStream)

        logger.debug("Job store is clean")
        # TODO: reloading of the rootJob may be redundant here
        return self.loadRootJob()

    ##########################################
    # The following methods deal with creating/loading/updating/writing/checking for the
    # existence of jobs
    ##########################################

    @abstractmethod
    def assignID(self, jobDescription):
        """
        Get a new jobStoreID to be used by the described job, and assigns it to the JobDescription.

        Files associated with the assigned ID will be accepted even if the JobDescription has never been created or updated.

        :param toil.job.JobDescription jobDescription: The JobDescription to give an ID to
        """
        raise NotImplementedError()

    @contextmanager
    def batch(self):
        """
        If supported by the batch system, calls to create() with this context
        manager active will be performed in a batch after the context manager
        is released.
        :rtype: None
        """
        yield

    @abstractmethod
    def create(self, jobDescription):
        """
        Writes the given JobDescription to the job store. The job must have an ID assigned already.

        :return: The JobDescription passed.
        :rtype: toil.job.JobDescription
        """
        raise NotImplementedError()

    @abstractmethod
    def exists(self, jobStoreID):
        """
        Indicates whether a description of the job with the specified jobStoreID exists in the job store

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
        Loads the description of the job referenced by the given ID, assigns it
        the job store's config, and returns it.

        May declare the job to have failed (see
        :meth:`toil.job.JobDescription.setupJobAfterFailure`) if there is
        evidence of a failed update attempt.

        :param str jobStoreID: the ID of the job to load

        :raise NoSuchJobException: if there is no job with the given ID

        :rtype: toil.job.JobDescription
        """
        raise NotImplementedError()

    @abstractmethod
    def update(self, jobDescription):
        """
        Persists changes to the state of the given JobDescription in this store atomically.

        :param toil.job.JobDescription job: the job to write to this job store
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, jobStoreID):
        """
        Removes the JobDescription from the store atomically. You may not then
        subsequently call load(), write(), update(), etc. with the same
        jobStoreID or any JobDescription bearing it.

        This operation is idempotent, i.e. deleting a job twice or deleting a non-existent job
        will succeed silently.

        :param str jobStoreID: the ID of the job to delete from this job store
        """
        raise NotImplementedError()

    def jobs(self):
        """
        Best effort attempt to return iterator on JobDescriptions for all jobs
        in the store. The iterator may not return all jobs and may also contain
        orphaned jobs that have already finished successfully and should not be
        rerun. To guarantee you get any and all jobs that can be run instead
        construct a more expensive ToilState object

        :return: Returns iterator on jobs in the store. The iterator may or may not contain all jobs and may contain
                 invalid jobs
        :rtype: Iterator[toil.job.jobDescription]
        """
        raise NotImplementedError()

    ##########################################
    # The following provide an way of creating/reading/writing/updating files
    # associated with a given job.
    ##########################################

    @abstractmethod
    def writeFile(self, localFilePath, jobStoreID=None, cleanup=False):
        """
        Takes a file (as a path) and places it in this job store. Returns an ID that can be used
        to retrieve the file at a later time.  The file is written in a atomic manner.  It will
        not appear in the jobStore until the write has successfully completed.

        :param str localFilePath: the path to the local file that will be uploaded to the job store.
               The last path component (basename of the file) will remain
               associated with the file in the file store, if supported, so
               that the file can be searched for by name or name glob.

        :param str jobStoreID: the id of a job, or None. If specified, the may be associated
               with that job in a job-store-specific way. This may influence the returned ID.

        :param bool cleanup: Whether to attempt to delete the file when the job
               whose jobStoreID was given as jobStoreID is deleted with
               jobStore.delete(job). If jobStoreID was not given, does nothing.

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
    def writeFileStream(self, jobStoreID=None, cleanup=False, basename=None, encoding=None, errors=None):
        """
        Similar to writeFile, but returns a context manager yielding a tuple of
        1) a file handle which can be written to and 2) the ID of the resulting
        file in the job store. The yielded file handle does not need to and
        should not be closed explicitly.  The file is written in a atomic manner.
        It will not appear in the jobStore until the write has successfully
        completed.

        :param str jobStoreID: the id of a job, or None. If specified, the may be associated
               with that job in a job-store-specific way. This may influence the returned ID.

        :param bool cleanup: Whether to attempt to delete the file when the job
               whose jobStoreID was given as jobStoreID is deleted with
               jobStore.delete(job). If jobStoreID was not given, does nothing.

        :param str basename: If supported by the implementation, use the given
               file basename so that when searching the job store with a query
               matching that basename, the file will be detected.

        :param str encoding: the name of the encoding used to encode the file. Encodings are the same
                as for encode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.

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
    def getEmptyFileStoreID(self, jobStoreID=None, cleanup=False, basename=None):
        """
        Creates an empty file in the job store and returns its ID.
        Call to fileExists(getEmptyFileStoreID(jobStoreID)) will return True.

        :param str jobStoreID: the id of a job, or None. If specified, the may be associated
               with that job in a job-store-specific way. This may influence the returned ID.

        :param bool cleanup: Whether to attempt to delete the file when the job
               whose jobStoreID was given as jobStoreID is deleted with
               jobStore.delete(job). If jobStoreID was not given, does nothing.

        :param str basename: If supported by the implementation, use the given
               file basename so that when searching the job store with a query
               matching that basename, the file will be detected.

        :return: a jobStoreFileID that references the newly created file and can be used to reference the
                 file in the future.
        :rtype: str
        """
        raise NotImplementedError()

    @abstractmethod
    def readFile(self, jobStoreFileID, localFilePath, symlink=False):
        """
        Copies or hard links the file referenced by jobStoreFileID to the given
        local file path. The version will be consistent with the last copy of
        the file written/updated. If the file in the job store is later
        modified via updateFile or updateFileStream, it is
        implementation-defined whether those writes will be visible at
        localFilePath.  The file is copied in an atomic manner.  It will not
        appear in the local file system until the copy has completed.

        The file at the given local path may not be modified after this method returns!
        
        Note!  Implementations of readFile need to respect/provide the executable attribute on FileIDs.
        
        :param str jobStoreFileID: ID of the file to be copied

        :param str localFilePath: the local path indicating where to place the contents of the
               given file in the job store

        :param bool symlink: whether the reader can tolerate a symlink. If set to true, the job
               store may create a symlink instead of a full copy of the file or a hard link.
        """
        raise NotImplementedError()

    @abstractmethod
    @contextmanager
    def readFileStream(self, jobStoreFileID, encoding=None, errors=None):
        """
        Similar to readFile, but returns a context manager yielding a file handle which can be
        read from. The yielded file handle does not need to and should not be closed explicitly.

        :param str jobStoreFileID: ID of the file to get a readable file handle for

        :param str encoding: the name of the encoding used to decode the file. Encodings are the same as
                for decode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.
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
    def getFileSize(self, jobStoreFileID):
        """
        Get the size of the given file in bytes, or 0 if it does not exist when queried.

        Note that job stores which encrypt files might return overestimates of
        file sizes, since the encrypted file may have been padded to the
        nearest block, augmented with an initialization vector, etc.

        :param str jobStoreFileID: an ID referencing the file to be checked

        :rtype: int
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
    def updateFileStream(self, jobStoreFileID, encoding=None, errors=None):
        """
        Replaces the existing version of a file in the job store. Similar to writeFile, but
        returns a context manager yielding a file handle which can be written to. The
        yielded file handle does not need to and should not be closed explicitly.

        :param str jobStoreFileID: the ID of the file in the job store to be updated

        :param str encoding: the name of the encoding used to encode the file. Encodings are the same
                as for encode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.

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
    def writeSharedFileStream(self, sharedFileName, isProtected=None, encoding=None, errors=None):
        """
        Returns a context manager yielding a writable file handle to the global file referenced
        by the given name.  File will be created in an atomic manner.

        :param str sharedFileName: A file name matching AbstractJobStore.fileNameRegex, unique within
               this job store

        :param bool isProtected: True if the file must be encrypted, None if it may be encrypted or
               False if it must be stored in the clear.

        :param str encoding: the name of the encoding used to encode the file. Encodings are the same
                as for encode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method
        """
        raise NotImplementedError()

    @abstractmethod
    @contextmanager
    def readSharedFileStream(self, sharedFileName, encoding=None, errors=None):
        """
        Returns a context manager yielding a readable file handle to the global file referenced
        by the given name.

        :param str sharedFileName: A file name matching AbstractJobStore.fileNameRegex, unique within
               this job store

        :param str encoding: the name of the encoding used to decode the file. Encodings are the same
                as for decode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.
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


class JobStoreSupport(AbstractJobStore, metaclass=ABCMeta):
    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() in ('http', 'https', 'ftp') and not export

    @classmethod
    @retry(errors=[BadStatusLine] + [
                         ErrorCondition(
                             error=HTTPError,
                             error_codes=[408, 500, 503]
                         )
                     ])
    def getSize(cls, url):
        if url.scheme.lower() == 'ftp':
            return None
        with closing(urlopen(url.geturl())) as readable:
            # just read the header for content length
            size = readable.info().get('content-length')
            return int(size) if size is not None else None

    @classmethod
    @retry(errors=[BadStatusLine] + [
                         ErrorCondition(
                             error=HTTPError,
                             error_codes=[408, 500, 503]
                         )
                     ])
    def _readFromUrl(cls, url, writable):
        # We can only retry on errors that happen as responses to the request.
        # If we start getting file data, and the connection drops, we fail.
        # So we don't have to worry about writing the start of the file twice.
        with closing(urlopen(url.geturl())) as readable:
            # Make something to count the bytes we get
            # We need to put the actual count in a container so our
            # nested function can modify it without creating its own
            # local with the same name.
            size = [0]
            def count(l):
                size[0] += l
            counter = WriteWatchingStream(writable)
            counter.onWrite(count)

            # Do the download
            shutil.copyfileobj(readable, counter)
            return size[0], False
