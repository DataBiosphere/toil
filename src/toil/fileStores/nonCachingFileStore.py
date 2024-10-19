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
import errno
import logging
import os
import tempfile
from collections import defaultdict
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from typing import (
    IO,
    Any,
    Callable,
    ContextManager,
    DefaultDict,
    Literal,
    Optional,
    Union,
    cast,
    overload,
)

import dill

from toil.common import getFileSystemSize
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import Job, JobDescription
from toil.jobStores.abstractJobStore import AbstractJobStore
from toil.lib.compatibility import deprecated
from toil.lib.io import make_public_dir, robust_rmtree
from toil.lib.retry import ErrorCondition, retry
from toil.lib.threading import (
    get_process_name,
    process_name_exists,
    safe_lock,
    safe_unlock_and_close,
)

logger: logging.Logger = logging.getLogger(__name__)


class NonCachingFileStore(AbstractFileStore):
    def __init__(
        self,
        jobStore: AbstractJobStore,
        jobDesc: JobDescription,
        file_store_dir: str,
        waitForPreviousCommit: Callable[[], Any],
    ) -> None:
        super().__init__(jobStore, jobDesc, file_store_dir, waitForPreviousCommit)
        # This will be defined in the `open` method.
        self.jobStateFile: Optional[str] = None
        self.localFileMap: DefaultDict[str, list[str]] = defaultdict(list)

        self.check_for_state_corruption()

    @staticmethod
    def check_for_coordination_corruption(coordination_dir: Optional[str]) -> None:
        """
        Make sure the coordination directory hasn't been deleted unexpectedly.

        Slurm has been known to delete XDG_RUNTIME_DIR out from under processes
        it was promised to, so it is possible that in certain misconfigured
        environments the coordination directory and everything in it could go
        away unexpectedly. We are going to regularly make sure that the things
        we think should exist actually exist, and we are going to abort if they
        do not.
        """

        if coordination_dir and not os.path.exists(coordination_dir):
            raise RuntimeError(
                f"The Toil coordination directory at {coordination_dir} "
                f"was removed while the workflow was running! Please provide a "
                f"TOIL_COORDINATION_DIR or --coordinationDir at a location that "
                f"is safe from automated cleanup during the workflow run."
            )

    def check_for_state_corruption(self) -> None:
        """
        Make sure state tracking information hasn't been deleted unexpectedly.
        """

        NonCachingFileStore.check_for_coordination_corruption(self.coordination_dir)

        if self.jobStateFile and not os.path.exists(self.jobStateFile):
            raise RuntimeError(
                f"The job state file {self.jobStateFile} "
                f"was removed while the workflow was running! Please provide a "
                f"TOIL_COORDINATION_DIR or --coordinationDir at a location that "
                f"is safe from automated cleanup during the workflow run."
            )

    @contextmanager
    def open(self, job: Job) -> Generator[None, None, None]:
        startingDir = os.getcwd()
        self.localTempDir: str = make_public_dir(
            self.localTempDir, suggested_name="job"
        )
        self._removeDeadJobs(self.coordination_dir)
        self.jobStateFile = self._createJobStateFile()
        self.check_for_state_corruption()
        freeSpace, diskSize = getFileSystemSize(self.localTempDir)
        if freeSpace <= 0.1 * diskSize:
            logger.warning(
                f"Starting job {self.jobName} with less than 10%% of disk space remaining."
            )
        try:
            os.chdir(self.localTempDir)
            with super().open(job):
                yield
        finally:
            os.chdir(startingDir)
            # Finally delete the job from the worker
            self.check_for_state_corruption()
            try:
                os.remove(self.jobStateFile)
            except FileNotFoundError:
                logger.exception(
                    "Job state file %s has gone missing unexpectedly; some cleanup for failed jobs may be getting skipped!",
                    self.jobStateFile,
                )

    def writeGlobalFile(self, localFileName: str, cleanup: bool = False) -> FileID:
        absLocalFileName = self._resolveAbsoluteLocalPath(localFileName)
        creatorID = str(self.jobDesc.jobStoreID)
        fileStoreID = self.jobStore.write_file(absLocalFileName, creatorID, cleanup)
        if absLocalFileName.startswith(self.localTempDir):
            # Only files in the appropriate directory should become local files
            # we can delete with deleteLocalFile
            self.localFileMap[fileStoreID].append(absLocalFileName)
        return FileID.forPath(fileStoreID, absLocalFileName)

    def readGlobalFile(
        self,
        fileStoreID: str,
        userPath: Optional[str] = None,
        cache: bool = True,
        mutable: bool = False,
        symlink: bool = False,
    ) -> str:
        if userPath is not None:
            localFilePath = self._resolveAbsoluteLocalPath(userPath)
            if os.path.exists(localFilePath):
                raise RuntimeError(
                    " File %s " % localFilePath + " exists. Cannot Overwrite."
                )
        else:
            localFilePath = self.getLocalTempFileName()

        self.jobStore.read_file(fileStoreID, localFilePath, symlink=symlink)
        self.localFileMap[fileStoreID].append(localFilePath)
        self.logAccess(fileStoreID, localFilePath)
        return localFilePath

    @overload
    def readGlobalFileStream(
        self,
        fileStoreID: str,
        encoding: Literal[None] = None,
        errors: Optional[str] = None,
    ) -> ContextManager[IO[bytes]]: ...

    @overload
    def readGlobalFileStream(
        self, fileStoreID: str, encoding: str, errors: Optional[str] = None
    ) -> ContextManager[IO[str]]: ...

    # TODO: This seems to hit https://github.com/python/mypy/issues/11373
    # But that is supposedly fixed.

    @contextmanager  # type: ignore
    def readGlobalFileStream(
        self,
        fileStoreID: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Iterator[Union[IO[bytes], IO[str]]]:
        with self.jobStore.read_file_stream(
            fileStoreID, encoding=encoding, errors=errors
        ) as f:
            self.logAccess(fileStoreID)
            yield f

    @deprecated(new_function_name="export_file")
    def exportFile(self, jobStoreFileID: FileID, dstUrl: str) -> None:
        return self.export_file(jobStoreFileID, dstUrl)

    def export_file(self, file_id: FileID, dst_uri: str) -> None:
        self.jobStore.export_file(file_id, dst_uri)

    def deleteLocalFile(self, fileStoreID: str) -> None:
        try:
            localFilePaths = self.localFileMap.pop(fileStoreID)
        except KeyError:
            raise OSError(
                errno.ENOENT, "Attempting to delete local copies of a file with none"
            )
        else:
            for localFilePath in localFilePaths:
                os.remove(localFilePath)

    def deleteGlobalFile(self, fileStoreID: str) -> None:
        try:
            self.deleteLocalFile(fileStoreID)
        except OSError as e:
            if e.errno == errno.ENOENT:
                # the file does not exist locally, so no local deletion necessary
                pass
            else:
                raise
        self.filesToDelete.add(str(fileStoreID))

    def waitForCommit(self) -> bool:
        # there is no asynchronicity in this file store so no need to block at all
        return True

    def startCommit(self, jobState: bool = False) -> None:
        # Make sure the previous job is committed, if any
        if self.waitForPreviousCommit is not None:
            self.waitForPreviousCommit()

        # We are going to commit synchronously, so no need to clone a snapshot
        # of the job description or mess with its version numbering.

        if not jobState:
            # All our operations that need committing are job state related
            return

        try:
            # Indicate any files that should be seen as deleted once the
            # update of the job description is visible.
            if len(self.jobDesc.filesToDelete) > 0:
                raise RuntimeError("Job is already in the process of being committed!")
            self.jobDesc.filesToDelete = list(self.filesToDelete)
            # Complete the job
            self.jobStore.update_job(self.jobDesc)
            # Delete any remnant files
            list(map(self.jobStore.delete_file, self.filesToDelete))
            # Remove the files to delete list, having successfully removed the files
            if len(self.filesToDelete) > 0:
                self.jobDesc.filesToDelete = []
                # Update, removing emptying files to delete
                self.jobStore.update_job(self.jobDesc)
        except:
            self._terminateEvent.set()
            raise

    def __del__(self) -> None:
        """
        Cleanup function that is run when destroying the class instance.  Nothing to do since there
        are no async write events.
        """

    @classmethod
    def _removeDeadJobs(
        cls, coordination_dir: str, batchSystemShutdown: bool = False
    ) -> None:
        """
        Look at the state of all jobs registered in the individual job state files, and handle them
        (clean up the disk)

        :param str coordination_dir: The location of the coordination directory on the node.
        :param bool batchSystemShutdown: Is the batch system in the process of shutting down?
        :return:
        """

        cls.check_for_coordination_corruption(coordination_dir)

        for jobState in cls._getAllJobStates(coordination_dir):
            if not process_name_exists(coordination_dir, jobState["jobProcessName"]):
                # We need to have a race to pick someone to clean up.

                try:
                    # Open the directory.
                    # We can't open a directory for write, only for read.
                    dirFD = os.open(jobState["jobDir"], os.O_RDONLY)
                except FileNotFoundError:
                    # The cleanup has happened and we can't contest for it
                    continue

                try:
                    # Try and lock it non-blocking
                    safe_lock(dirFD, block=False)
                except OSError as e:
                    os.close(dirFD)
                    if e.errno not in (errno.EACCES, errno.EAGAIN):
                        # Something went wrong
                        raise
                    # Otherwise, we lost the race. Someone else is alive and
                    # has it locked. So loop around again.
                else:
                    # We got it
                    logger.warning(
                        "Detected that job (%s) prematurely terminated.  Fixing the "
                        "state of the job on disk.",
                        jobState["jobName"],
                    )

                    try:
                        if not batchSystemShutdown:
                            logger.debug("Deleting the stale working directory.")
                            # Delete the old work directory if it still exists.  Do this only during
                            # the life of the program and dont' do it during the batch system
                            # cleanup. Leave that to the batch system cleanup code.
                            robust_rmtree(jobState["jobDir"])
                    finally:
                        safe_unlock_and_close(dirFD)

    @classmethod
    def _getAllJobStates(cls, coordination_dir: str) -> Iterator[dict[str, str]]:
        """
        Generator function that deserializes and yields the job state for every job on the node,
        one at a time.

        :param coordination_dir: The location of the coordination directory on the node.

        :return: dict with keys (jobName,  jobProcessName, jobDir)
        """
        jobStateFiles = []

        cls.check_for_coordination_corruption(coordination_dir)

        # Note that the directory may contain files whose names are not decodable to Unicode.
        # So we need to work in bytes.
        for entry in os.scandir(os.fsencode(coordination_dir)):
            # For each job state file in the coordination directory
            if entry.name.endswith(b".jobState"):
                # This is the state of a job
                jobStateFiles.append(os.fsdecode(entry.path))

        for fname in jobStateFiles:
            try:
                yield NonCachingFileStore._readJobState(fname)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    # This is a FileNotFoundError.
                    # job finished & deleted its jobState file since the jobState files were discovered
                    continue
                elif e.errno == 5:
                    # This is a OSError: [Errno 5] Input/output error (jobStatefile seems to disappear
                    # on network file system sometimes)
                    continue
                else:
                    raise

    @staticmethod
    # Retry on any OSError except FileNotFoundError, which we throw immediately
    @retry(
        errors=[
            OSError,
            ErrorCondition(error=FileNotFoundError, retry_on_this_condition=False),
        ]
    )
    def _readJobState(jobStateFileName: str) -> dict[str, str]:
        with open(jobStateFileName, "rb") as fH:
            state = dill.load(fH)
        return cast(dict[str, str], state)

    def _createJobStateFile(self) -> str:
        """
        Create the job state file for the current job and fill in the required
        values.

        Places the file in the coordination directory.

        :return: Path to the job state file
        :rtype: str
        """
        self.check_for_state_corruption()
        jobState = {
            "jobProcessName": get_process_name(self.coordination_dir),
            "jobName": self.jobName,
            "jobDir": self.localTempDir,
        }
        try:
            (fd, jobStateFile) = tempfile.mkstemp(
                suffix=".jobState.tmp", dir=self.coordination_dir
            )
        except Exception as e:
            raise RuntimeError(
                "Could not make state file in " + self.coordination_dir
            ) from e
        with open(fd, "wb") as fH:
            # Write data
            dill.dump(jobState, fH)
        # Drop suffix
        jobStateFile = jobStateFile[: -len(".tmp")]
        # Put in place
        os.rename(jobStateFile + ".tmp", jobStateFile)
        return jobStateFile

    @classmethod
    def shutdown(cls, shutdown_info: str) -> None:
        """
        :param shutdown_info: The coordination directory.
        """
        cls._removeDeadJobs(shutdown_info, batchSystemShutdown=True)
