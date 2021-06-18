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
import fcntl
import logging
import os
from collections import defaultdict
from contextlib import contextmanager
from typing import Callable, Dict, Optional, Generator

import dill

from toil.common import getDirSizeRecursively, getFileSystemSize
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.jobStores.abstractJobStore import AbstractJobStore
from toil.lib.conversions import bytes2human
from toil.lib.io import robust_rmtree, make_public_dir
from toil.lib.threading import get_process_name, process_name_exists
from toil.job import Job, JobDescription

logger: logging.Logger = logging.getLogger(__name__)


class NonCachingFileStore(AbstractFileStore):
    def __init__(self, jobStore: AbstractJobStore, jobDesc: JobDescription, localTempDir: str, waitForPreviousCommit: Callable[[], None]) -> None:
        super().__init__(jobStore, jobDesc, localTempDir, waitForPreviousCommit)
        # This will be defined in the `open` method.
        self.jobStateFile: Optional[str] = None
        self.localFileMap: Dict[str, str] = {}
        self.localFileMap = defaultdict(list)  # type: ignore

    @contextmanager
    def open(self, job: Job) -> Generator[None, None, None]:
        jobReqs = job.disk
        startingDir = os.getcwd()
        self.localTempDir = make_public_dir(in_directory=self.localTempDir)
        self._removeDeadJobs(self.workDir)
        self.jobStateFile = self._createJobStateFile()
        freeSpace, diskSize = getFileSystemSize(self.localTempDir)
        if freeSpace <= 0.1 * diskSize:
            logger.warning(f'Starting job {self.jobName} with less than 10%% of disk space remaining.')
        try:
            os.chdir(self.localTempDir)
            with super().open(job):
                yield
        finally:
            disk = getDirSizeRecursively(self.localTempDir)
            percent = float(disk) / jobReqs * 100 if jobReqs > 0 else 0.0
            disk_usage = (f"Job {self.jobName} used {percent:.2f}% disk ({bytes2human(disk)}B [{disk}B] used, "
                          f"{bytes2human(jobReqs)}B [{jobReqs}B] requested).")
            if disk > jobReqs:
                self.logToMaster("Job used more disk than requested. For CWL, consider increasing the outdirMin "
                                 f"requirement, otherwise, consider increasing the disk requirement. {disk_usage}",
                                 level=logging.WARNING)
            else:
                self.logToMaster(disk_usage, level=logging.DEBUG)
            os.chdir(startingDir)
            # Finally delete the job from the worker
            os.remove(self.jobStateFile)

    def writeGlobalFile(self, localFileName, cleanup=False):
        absLocalFileName = self._resolveAbsoluteLocalPath(localFileName)
        creatorID = self.jobDesc.jobStoreID
        fileStoreID = self.jobStore.writeFile(absLocalFileName, creatorID, cleanup)
        if absLocalFileName.startswith(self.localTempDir):
            # Only files in the appropriate directory should become local files
            # we can delete with deleteLocalFile
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
        self.logAccess(fileStoreID, localFilePath)
        return localFilePath

    @contextmanager
    def readGlobalFileStream(self, fileStoreID, encoding=None, errors=None):
        with self.jobStore.readFileStream(fileStoreID, encoding=encoding, errors=errors) as f:
            self.logAccess(fileStoreID)
            yield f

    def exportFile(self, jobStoreFileID, dstUrl):
        self.jobStore.exportFile(jobStoreFileID, dstUrl)

    def deleteLocalFile(self, fileStoreID):
        try:
            localFilePaths = self.localFileMap.pop(fileStoreID)
        except KeyError:
            raise OSError(errno.ENOENT, "Attempting to delete local copies of a file with none")
        else:
            for localFilePath in localFilePaths:
                os.remove(localFilePath)

    def deleteGlobalFile(self, fileStoreID):
        try:
            self.deleteLocalFile(fileStoreID)
        except OSError as e:
            if e.errno == errno.ENOENT:
                # the file does not exist locally, so no local deletion necessary
                pass
            else:
                raise
        self.filesToDelete.add(str(fileStoreID))

    def waitForCommit(self):
        # there is no asynchronicity in this file store so no need to block at all
        return True

    def startCommit(self, jobState=False):
        # Make sure the previous job is committed, if any
        if self.waitForPreviousCommit is not None:
            self.waitForPreviousCommit()

        if not jobState:
            # All our operations that need committing are job state related
            return

        try:
            # Indicate any files that should be deleted once the update of
            # the job wrapper is completed.
            self.jobDesc.filesToDelete = list(self.filesToDelete)
            # Complete the job
            self.jobStore.update(self.jobDesc)
            # Delete any remnant jobs
            list(map(self.jobStore.delete, self.jobsToDelete))
            # Delete any remnant files
            list(map(self.jobStore.deleteFile, self.filesToDelete))
            # Remove the files to delete list, having successfully removed the files
            if len(self.filesToDelete) > 0:
                self.jobDesc.filesToDelete = []
                # Update, removing emptying files to delete
                self.jobStore.update(self.jobDesc)
        except:
            self._terminateEvent.set()
            raise

    def __del__(self):
        """
        Cleanup function that is run when destroying the class instance.  Nothing to do since there
        are no async write events.
        """

    @classmethod
    def _removeDeadJobs(cls, nodeInfo: str, batchSystemShutdown: bool=False) -> None:
        """
        Look at the state of all jobs registered in the individual job state files, and handle them
        (clean up the disk)

        :param str nodeInfo: The location of the workflow directory on the node.
        :param bool batchSystemShutdown: Is the batch system in the process of shutting down?
        :return:
        """

        for jobState in cls._getAllJobStates(nodeInfo):
            if not process_name_exists(nodeInfo, jobState['jobProcessName']):
                # We need to have a race to pick someone to clean up.

                try:
                    # Open the directory
                    dirFD = os.open(jobState['jobDir'], os.O_RDONLY)
                except FileNotFoundError:
                    # The cleanup has happened and we can't contest for it
                    continue

                try:
                    # Try and lock it
                    fcntl.lockf(dirFD, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as e:
                    # We lost the race. Someone else is alive and has it locked.
                    os.close(dirFD)
                else:
                    # We got it
                    logger.warning('Detected that job (%s) prematurely terminated.  Fixing the '
                                   'state of the job on disk.', jobState['jobName'])

                    try:
                        if not batchSystemShutdown:
                            logger.debug("Deleting the stale working directory.")
                            # Delete the old work directory if it still exists.  Do this only during
                            # the life of the program and dont' do it during the batch system
                            # cleanup. Leave that to the batch system cleanup code.
                            robust_rmtree(jobState['jobDir'])
                    finally:
                        fcntl.lockf(dirFD, fcntl.LOCK_UN)
                        os.close(dirFD)

    @staticmethod
    def _getAllJobStates(workflowDir):
        """
        Generator function that deserializes and yields the job state for every job on the node,
        one at a time.

        :param str workflowDir: The location of the workflow directory on the node.
        :return: dict with keys (jobName,  jobProcessName, jobDir)
        :rtype: dict
        """
        jobStateFiles = []
        # Note that the directory tree may contain files whose names are not decodable to Unicode.
        # So we need to work in bytes.
        # We require that the job state files aren't in any of those directories.
        for root, dirs, files in os.walk(workflowDir.encode('utf-8')):
            for filename in files:
                if filename == '.jobState'.encode('utf-8'):
                    jobStateFiles.append(os.path.join(root, filename).decode('utf-8'))
        for filename in jobStateFiles:
            try:
                yield NonCachingFileStore._readJobState(filename)
            except OSError as e:
                if e.errno == 2:
                    # job finished & deleted its jobState file since the jobState files were discovered
                    continue
                else:
                    raise

    @staticmethod
    def _readJobState(jobStateFileName):
        with open(jobStateFileName, 'rb') as fH:
            state = dill.load(fH)
        return state

    def _createJobStateFile(self) -> str:
        """
        Create the job state file for the current job and fill in the required
        values.

        :return: Path to the job state file
        :rtype: str
        """
        jobStateFile = os.path.join(self.localTempDir, '.jobState')
        jobState = {'jobProcessName': get_process_name(self.workDir),
                    'jobName': self.jobName,
                    'jobDir': self.localTempDir}
        with open(jobStateFile + '.tmp', 'wb') as fH:
            dill.dump(jobState, fH)
        os.rename(jobStateFile + '.tmp', jobStateFile)
        return jobStateFile

    @classmethod
    def shutdown(cls, dir_):
        """
        :param dir_: The workflow directory that will contain all the individual worker directories.
        """
        cls._removeDeadJobs(dir_, batchSystemShutdown=True)
