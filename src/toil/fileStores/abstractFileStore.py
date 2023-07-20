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
import os
import tempfile
from abc import ABC, abstractmethod
from contextlib import contextmanager
from threading import Event, Semaphore
from typing import (IO,
                    TYPE_CHECKING,
                    Any,
                    Callable,
                    ContextManager,
                    Dict,
                    Generator,
                    Iterator,
                    List,
                    Optional,
                    Set,
                    Tuple,
                    Type,
                    Union,
                    cast)

import dill

from toil.common import Toil, cacheDirName
from toil.fileStores import FileID
from toil.job import Job, JobDescription
from toil.jobStores.abstractJobStore import AbstractJobStore
from toil.lib.compatibility import deprecated
from toil.lib.io import WriteWatchingStream

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from toil.fileStores.cachingFileStore import CachingFileStore
    from toil.fileStores.nonCachingFileStore import NonCachingFileStore


class AbstractFileStore(ABC):
    """
    Interface used to allow user code run by Toil to read and write files.

    Also provides the interface to other Toil facilities used by user code,
    including:

     * normal (non-real-time) logging
     * finding the correct temporary directory for scratch work
     * importing and exporting files into and out of the workflow

    Stores user files in the jobStore, but keeps them separate from actual
    jobs.

    May implement caching.

    Passed as argument to the :meth:`toil.job.Job.run` method.

    Access to files is only permitted inside the context manager provided by
    :meth:`toil.fileStores.abstractFileStore.AbstractFileStore.open`.

    Also responsible for committing completed jobs back to the job store with
    an update operation, and allowing that commit operation to be waited for.
    """
    # Variables used for syncing reads/writes
    _pendingFileWritesLock = Semaphore()
    _pendingFileWrites: Set[str] = set()
    _terminateEvent = Event()  # Used to signify crashes in threads

    def __init__(
        self,
        jobStore: AbstractJobStore,
        jobDesc: JobDescription,
        file_store_dir: str,
        waitForPreviousCommit: Callable[[], Any],
    ) -> None:
        """
        Create a new file store object.

        :param jobStore: the job store in use for the current Toil run.
        :param jobDesc: the JobDescription object for the currently
               running job.

        :param file_store_dir: the per-worker local temporary directory where
               the file store should store local files. Per-job directories will be
               created under here by the file store.

        :param waitForPreviousCommit: the waitForCommit method of the previous job's
               file store, when jobs are running in sequence on the same worker. Used to
               prevent this file store's startCommit and the previous job's
               startCommit methods from running at the same time and racing. If
               they did race, it might be possible for the later job to be fully
               marked as completed in the job store before the eralier job was.
        """
        self.jobStore = jobStore
        self.jobDesc = jobDesc
        # This gets replaced with a subdirectory of itself on open()
        self.localTempDir: str = os.path.abspath(file_store_dir)
        assert self.jobStore.config.workflowID is not None
        self.workflow_dir: str = Toil.getLocalWorkflowDir(self.jobStore.config.workflowID, self.jobStore.config.workDir)
        self.coordination_dir: str =Toil.get_local_workflow_coordination_dir(self.jobStore.config.workflowID, self.jobStore.config.workDir, self.jobStore.config.coordination_dir)
        self.jobName: str = (
            self.jobDesc.command.split()[1] if self.jobDesc.command else ""
        )
        self.waitForPreviousCommit = waitForPreviousCommit
        self.loggingMessages: List[Dict[str, Union[int, str]]] = []
        # Records file IDs of files deleted during the current job. Doesn't get
        # committed back until the job is completely successful, because if the
        # job is re-run it will need to be able to re-delete these files.
        # This is a set of str objects, not FileIDs.
        self.filesToDelete: Set[str] = set()
        # Records IDs of jobs that need to be deleted when the currently
        # running job is cleaned up.
        # May be modified by the worker to actually delete jobs!
        self.jobsToDelete: Set[str] = set()
        # Holds records of file ID, or file ID and local path, for reporting
        # the accessed files of failed jobs.
        self._accessLog: List[Tuple[str, ...]] = []

    @staticmethod
    def createFileStore(
        jobStore: AbstractJobStore,
        jobDesc: JobDescription,
        file_store_dir: str,
        waitForPreviousCommit: Callable[[], Any],
        caching: Optional[bool],
    ) -> Union["NonCachingFileStore", "CachingFileStore"]:
        """Create a concreate FileStore."""
        # Defer these imports until runtime, since these classes depend on us
        from toil.fileStores.cachingFileStore import CachingFileStore
        from toil.fileStores.nonCachingFileStore import NonCachingFileStore

        fileStoreCls: Union[Type["CachingFileStore"], Type["NonCachingFileStore"]] = (
            CachingFileStore if caching else NonCachingFileStore
        )
        return fileStoreCls(jobStore, jobDesc, file_store_dir, waitForPreviousCommit)

    @staticmethod
    def shutdownFileStore(workflowID: str, config_work_dir: Optional[str], config_coordination_dir: Optional[str]) -> None:
        """
        Carry out any necessary filestore-specific cleanup.

        This is a destructive operation and it is important to ensure that there
        are no other running processes on the system that are modifying or using
        the file store for this workflow.

        This is the intended to be the last call to the file store in a Toil run,
        called by the batch system cleanup function upon batch system shutdown.

        :param workflowID: The workflow ID for this invocation of the workflow
        :param config_work_dir: The path to the work directory in the Toil Config.
        :param config_coordination_dir: The path to the coordination directory in the Toil Config.
        """
        # Defer these imports until runtime, since these classes depend on our file
        from toil.fileStores.cachingFileStore import CachingFileStore
        from toil.fileStores.nonCachingFileStore import NonCachingFileStore

        workflowDir = Toil.getLocalWorkflowDir(workflowID, config_work_dir)
        coordination_dir = Toil.get_local_workflow_coordination_dir(workflowID, config_work_dir, config_coordination_dir)
        cacheDir = os.path.join(workflowDir, cacheDirName(workflowID))
        if os.path.exists(cacheDir):
            # The presence of the cacheDir suggests this was a cached run. We don't need
            # the cache lock for any of this since this is the final cleanup of a job
            # and there should be  no other conflicting processes using the cache.
            CachingFileStore.shutdown((coordination_dir, cacheDir))
        else:
            # This absence of cacheDir suggests otherwise.
            NonCachingFileStore.shutdown(coordination_dir)

    @contextmanager
    def open(self, job: Job) -> Generator[None, None, None]:
        """
        Create the context manager around tasks prior and after a job has been run.

        File operations are only permitted inside the context manager.

        Implementations must only yield from within `with super().open(job):`.

        :param job: The job instance of the toil job to run.
        """
        failed = True
        try:
            yield
            failed = False
        finally:
            # Do a finally instead of an except/raise because we don't want
            # to appear as "another exception occurred" in the stack trace.
            if failed:
                self._dumpAccessLogs()

    # Functions related to temp files and directories
    def getLocalTempDir(self) -> str:
        """
        Get a new local temporary directory in which to write files.

        The directory will only persist for the duration of the job.

        :return: The absolute path to a new local temporary directory. This directory
                 will exist for the duration of the job only, and is guaranteed
                 to be deleted once the job terminates, removing all files it
                 contains recursively.
        """
        return os.path.abspath(tempfile.mkdtemp(dir=self.localTempDir))

    def getLocalTempFile(self, suffix: Optional[str] = None, prefix: Optional[str] = None) -> str:
        """
        Get a new local temporary file that will persist for the duration of the job.

        :param suffix: If not None, the file name will end with this string.
               Otherwise, default value ".tmp" will be used

        :param prefix: If not None, the file name will start with this string.
               Otherwise, default value "tmp" will be used

        :return: The absolute path to a local temporary file. This file will exist
                 for the duration of the job only, and is guaranteed to be deleted
                 once the job terminates.
        """
        handle, tmpFile = tempfile.mkstemp(
            suffix=".tmp" if suffix is None else suffix,
            prefix="tmp" if prefix is None else prefix,
            dir=self.localTempDir
        )
        os.close(handle)
        return os.path.abspath(tmpFile)

    def getLocalTempFileName(self, suffix: Optional[str] = None, prefix: Optional[str] = None) -> str:
        """
        Get a valid name for a new local file. Don't actually create a file at the path.

        :param suffix: If not None, the file name will end with this string.
               Otherwise, default value ".tmp" will be used

        :param prefix: If not None, the file name will start with this string.
               Otherwise, default value "tmp" will be used

        :return: Path to valid file
        """
        # Create, and then delete a temp file. Creating will guarantee you a unique,
        # unused file name. There is a very, very, very low chance that another
        # job will create the same file name in the span of this one being deleted
        # and then being used by the user.
        tempFile = self.getLocalTempFile(suffix, prefix)
        os.remove(tempFile)
        return tempFile

    # Functions related to reading, writing and removing files to/from the job store
    @abstractmethod
    def writeGlobalFile(self, localFileName: str, cleanup: bool = False) -> FileID:
        """
        Upload a file (as a path) to the job store.

        If the file is in a FileStore-managed temporary directory (i.e. from
        :func:`toil.fileStores.abstractFileStore.AbstractFileStore.getLocalTempDir`),
        it will become a local copy of the file, eligible for deletion by
        :func:`toil.fileStores.abstractFileStore.AbstractFileStore.deleteLocalFile`.

        If an executable file on the local filesystem is uploaded, its executability
        will be preserved when it is downloaded again.

        :param localFileName: The path to the local file to upload. The
               last path component (basename of the file) will remain
               associated with the file in the file store, if supported by the
               backing JobStore, so that the file can be searched for by name
               or name glob.
        :param cleanup: if True then the copy of the global file will be deleted once
               the job and all its successors have completed running.  If not the global
               file must be deleted manually.

        :return: an ID that can be used to retrieve the file.
        """
        raise NotImplementedError()

    @contextmanager
    def writeGlobalFileStream(
        self,
        cleanup: bool = False,
        basename: Optional[str] = None,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Iterator[Tuple[WriteWatchingStream, FileID]]:
        """
        Similar to writeGlobalFile, but allows the writing of a stream to the job store.
        The yielded file handle does not need to and should not be closed explicitly.

        :param encoding: The name of the encoding used to decode the file. Encodings
            are the same as for decode(). Defaults to None which represents binary mode.

        :param errors: Specifies how encoding errors are to be handled. Errors are the
            same as for open(). Defaults to 'strict' when an encoding is specified.

        :param cleanup: is as in
            :func:`toil.fileStores.abstractFileStore.AbstractFileStore.writeGlobalFile`.

        :param basename: If supported by the backing JobStore, use the given
               file basename so that when searching the job store with a query
               matching that basename, the file will be detected.

        :return: A context manager yielding a tuple of
                  1) a file handle which can be written to and
                  2) the toil.fileStores.FileID of the resulting file in the job store.
        """
        with self.jobStore.write_file_stream(
            str(self.jobDesc.jobStoreID), cleanup, basename, encoding, errors
        ) as (backingStream, fileStoreID):
            # We have a string version of the file ID, and the backing stream.
            # We need to yield a stream the caller can write to, and a FileID
            # that accurately reflects the size of the data written to the
            # stream. We assume the stream is not seekable.

            # Make and keep a reference to the file ID, which is currently empty
            fileID = FileID(fileStoreID, 0)

            # Wrap the stream to increment the file ID's size for each byte written
            wrappedStream = WriteWatchingStream(backingStream)

            # When the stream is written to, count the bytes
            def handle(numBytes: int) -> None:
                # No scope problem here, because we don't assign to a fileID local
                fileID.size += numBytes
            wrappedStream.onWrite(handle)

            yield wrappedStream, fileID

    def _dumpAccessLogs(self) -> None:
        """
        When something goes wrong, log a report.

        Includes the files that were accessed while the file store was open.
        """
        if len(self._accessLog) > 0:
            logger.warning('Failed job accessed files:')

            for item in self._accessLog:
                # For each access record
                if len(item) == 2:
                    # If it has a name, dump wit the name
                    file_id, dest_path = item
                    if os.path.exists(dest_path):
                        if os.path.islink(dest_path):
                            logger.warning('Symlinked file \'%s\' to path \'%s\'', file_id, dest_path)
                        else:
                            logger.warning('Downloaded file \'%s\' to path \'%s\'', file_id, dest_path)
                    else:
                        logger.warning('Downloaded file \'%s\' to path \'%s\' (gone!)', file_id, dest_path)
                else:
                    # Otherwise dump without the name
                    logger.warning('Streamed file \'%s\'', *item)

    def logAccess(
        self, fileStoreID: Union[FileID, str], destination: Union[str, None] = None
    ) -> None:
        """
        Record that the given file was read by the job.

        (to be announced if the job fails)

        If destination is not None, it gives the path that the file
        was downloaded to. Otherwise, assumes that the file was streamed.

        Must be called by :meth:`readGlobalFile` and :meth:`readGlobalFileStream`
        implementations.
        """
        if destination is not None:
            self._accessLog.append((fileStoreID, destination))
        else:
            self._accessLog.append((fileStoreID,))

    @abstractmethod
    def readGlobalFile(
        self,
        fileStoreID: str,
        userPath: Optional[str] = None,
        cache: bool = True,
        mutable: bool = False,
        symlink: bool = False,
    ) -> str:
        """
        Make the file associated with fileStoreID available locally.

        If mutable is True, then a copy of the file will be created locally so
        that the original is not modified and does not change the file for other
        jobs. If mutable is False, then a link can be created to the file, saving
        disk resources. The file that is downloaded will be executable if and only
        if it was originally uploaded from an executable file on the local filesystem.

        If a user path is specified, it is used as the destination. If a user path isn't
        specified, the file is stored in the local temp directory with an encoded name.

        The destination file must not be deleted by the user; it can only be
        deleted through deleteLocalFile.

        Implementations must call :meth:`logAccess` to report the download.

        :param fileStoreID: job store id for the file
        :param userPath: a path to the name of file to which the global file will
               be copied or hard-linked (see below).
        :param cache: Described in
                      :func:`toil.fileStores.CachingFileStore.readGlobalFile`
        :param mutable: Described in
                        :func:`toil.fileStores.CachingFileStore.readGlobalFile`
        :param symlink: True if caller can accept symlink, False if caller can only accept a normal file or hardlink

        :return: An absolute path to a local, temporary copy of the file keyed
                 by fileStoreID.
        """
        raise NotImplementedError()

    @abstractmethod
    def readGlobalFileStream(
        self,
        fileStoreID: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> ContextManager[Union[IO[bytes], IO[str]]]:
        """
        Read a stream from the job store; similar to readGlobalFile.

        The yielded file handle does not need to and should not be closed explicitly.

        :param encoding: the name of the encoding used to decode the file. Encodings
            are the same as for decode(). Defaults to None which represents binary mode.

        :param errors: an optional string that specifies how encoding errors are
            to be handled. Errors are the same as for open(). Defaults to 'strict'
            when an encoding is specified.

        Implementations must call :meth:`logAccess` to report the download.

        :return: a context manager yielding a file handle which can be read from.
        """
        raise NotImplementedError()

    def getGlobalFileSize(self, fileStoreID: Union[FileID, str]) -> int:
        """
        Get the size of the file pointed to by the given ID, in bytes.

        If a FileID or something else with a non-None 'size' field, gets that.

        Otherwise, asks the job store to poll the file's size.

        Note that the job store may overestimate the file's size, for example
        if it is encrypted and had to be augmented with an IV or other
        encryption framing.

        :param fileStoreID: File ID for the file

        :return: File's size in bytes, as stored in the job store
        """
        # First try and see if the size is still attached
        size = getattr(fileStoreID, 'size', None)

        if size is None:
            # It fell off
            # Someone is mixing FileStore and JobStore file APIs,
            # or serializing FileIDs as strings.
            size = self.jobStore.get_file_size(fileStoreID)

        return cast(int, size)

    @abstractmethod
    def deleteLocalFile(self, fileStoreID: Union[FileID, str]) -> None:
        """
        Delete local copies of files associated with the provided job store ID.

        Raises an OSError with an errno of errno.ENOENT if no such local copies
        exist. Thus, cannot be called multiple times in succession.

        The files deleted are all those previously read from this file ID via
        readGlobalFile by the current job into the job's file-store-provided
        temp directory, plus the file that was written to create the given file
        ID, if it was written by the current job from the job's
        file-store-provided temp directory.

        :param fileStoreID: File Store ID of the file to be deleted.
        """
        raise NotImplementedError()

    @abstractmethod
    def deleteGlobalFile(self, fileStoreID: Union[FileID, str]) -> None:
        """
        Delete local files and then permanently deletes them from the job store.

        To ensure that the job can be restarted if necessary, the delete will not
        happen until after the job's run method has completed.

        :param fileStoreID: the File Store ID of the file to be deleted.
        """
        raise NotImplementedError()

    # Functions used to read and write files directly between a source url
    # and the job store.
    @deprecated(new_function_name="import_file")
    def importFile(
        self, srcUrl: str, sharedFileName: Optional[str] = None
    ) -> Optional[FileID]:
        return self.import_file(srcUrl, sharedFileName)

    def import_file(
        self, src_uri: str, shared_file_name: Optional[str] = None
    ) -> Optional[FileID]:
        return self.jobStore.import_file(src_uri, shared_file_name=shared_file_name)

    @deprecated(new_function_name='export_file')
    def exportFile(self, jobStoreFileID: FileID, dstUrl: str) -> None:
        return self.export_file(jobStoreFileID, dstUrl)

    def export_file(self, file_id: FileID, dst_uri: str) -> None:
        raise NotImplementedError()

    # A utility method for accessing filenames
    def _resolveAbsoluteLocalPath(self, filePath: str) -> str:
        """
        Return the absolute path to filePath.

        This is a wrapper for os.path.abspath because mac OS
        symlinks /tmp and /var (the most common places for a default tempdir) to
        /private/tmp and /private/var respectively.

        :param filePath: The absolute or relative path to the file. If relative,
            it must be relative to the local temp working dir

        :return: Absolute path to key
        """
        if os.path.isabs(filePath):
            return os.path.abspath(filePath)
        else:
            return os.path.join(self.localTempDir, filePath)

    class _StateFile:
        """Read and write dill-ed state dictionaries from/to a file into a namespace."""

        def __init__(self, stateDict: Dict[str, Any]):
            assert isinstance(stateDict, dict)
            self.__dict__.update(stateDict)

        @classmethod
        @abstractmethod
        @contextmanager
        def open(cls, outer: Optional[Any] = None) -> Iterator[Any]:
            """
            This is a context manager that state file and reads it into an object that is returned
            to the user in the yield.

            :param outer: Instance of the calling class (to use outer methods).
            """
            raise NotImplementedError()

        @classmethod
        def _load(cls, fileName: str) -> Any:
            """
            Load the state of the cache from the state file.

            :param fileName: Path to the cache state file.

            :return: An instance of the state as a namespace.
            :rtype: _StateFile
            """
            # Read the value from the cache state file then initialize and instance of
            # _CacheState with it.
            with open(fileName, 'rb') as fH:
                infoDict = dill.load(fH)
            return cls(infoDict)

        def write(self, fileName: str) -> None:
            """
            Write the current state to the main state file.

            :param fileName: Path to the state file.
            """
            with open(fileName + '.tmp', 'wb') as fH:
                # Based on answer by user "Mark" at:
                # http://stackoverflow.com/questions/2709800/how-to-pickle-yourself
                # We can't pickle nested classes. So we have to pickle the variables
                # of the class.
                # If we ever change this, we need to ensure it doesn't break FileID
                dill.dump(self.__dict__, fH)
            os.rename(fileName + '.tmp', fileName)

    # Functions related to logging
    def logToMaster(self, text: str, level: int = logging.INFO) -> None:
        """
        Send a logging message to the leader. The message will also be \
        logged by the worker at the same level.

        :param text: The string to log.
        :param level: The logging level.
        """
        logger.log(level=level, msg=("LOG-TO-MASTER: " + text))
        self.loggingMessages.append(dict(text=text, level=level))

    # Functions run after the completion of the job.
    @abstractmethod
    def startCommit(self, jobState: bool = False) -> None:
        """
        Update the status of the job on the disk.

        May start an asynchronous process. Call waitForCommit() to wait on that process.

        :param jobState: If True, commit the state of the FileStore's job,
                    and file deletes. Otherwise, commit only file creates/updates.

        """
        raise NotImplementedError()

    @abstractmethod
    def waitForCommit(self) -> bool:
        """
        Blocks while startCommit is running.

        This function is called by this job's successor to ensure that it does
        not begin modifying the job store until after this job has finished doing so.

        Might be called when startCommit is never called on a particular
        instance, in which case it does not block.

        :return: Always returns True
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def shutdown(cls, shutdown_info: Any) -> None:
        """
        Shutdown the filestore on this node.

        This is intended to be called on batch system shutdown.

        :param shutdown_info: The implementation-specific shutdown information,
              for shutting down the file store and removing all its state and all job
              local temp directories from the node.
        """
        raise NotImplementedError()
