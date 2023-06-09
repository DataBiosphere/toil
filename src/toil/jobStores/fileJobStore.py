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
import pickle
import random
import re
import shutil
import stat
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from typing import IO, Iterator, List, Optional, Union, overload
from urllib.parse import ParseResult, quote, unquote

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from toil.fileStores import FileID
from toil.job import TemporaryID
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             JobStoreExistsException,
                                             NoSuchFileException,
                                             NoSuchJobException,
                                             NoSuchJobStoreException)
from toil.lib.io import (AtomicFileCreate,
                         atomic_copy,
                         atomic_copyobj,
                         robust_rmtree)

logger = logging.getLogger(__name__)


class FileJobStore(AbstractJobStore):
    """
    A job store that uses a directory on a locally attached file system. To be compatible with
    distributed batch systems, that file system must be shared by all worker nodes.
    """

    # Valid chars for the creation of temporary "spray" directories.
    validDirs = "abcdefghijklmnopqrstuvwxyz0123456789"
    validDirsSet = set(validDirs)

    # What prefix should be on the per-job job directories, to distinguish them
    # from the spray directories?
    JOB_DIR_PREFIX = 'instance-'

    # What prefix do we put on the per-job-name directories we sort jobs into?
    JOB_NAME_DIR_PREFIX = 'kind-'

    # 10Mb RAM chunks when reading/writing files
    BUFFER_SIZE = 10485760  # 10Mb

    def default_caching(self) -> bool:
        """
        Jobstore's preference as to whether it likes caching or doesn't care about it.
        Some jobstores benefit from caching, however on some local configurations it can be flaky.

        see https://github.com/DataBiosphere/toil/issues/4218
        """

        return False

    def __init__(self, path: str, fanOut: int = 1000) -> None:
        """
        :param path: Path to directory holding the job store
        :param fanOut: Number of items to have in a directory before making
                           subdirectories
        """
        super().__init__(path)
        self.jobStoreDir = os.path.abspath(path)
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
        self.moveExports = None

    def __repr__(self):
        return f'FileJobStore({self.jobStoreDir})'

    def initialize(self, config):
        try:
            os.mkdir(self.jobStoreDir)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise JobStoreExistsException(self.jobStoreDir)
            else:
                raise
        os.makedirs(self.jobsDir, exist_ok=True)
        os.makedirs(self.statsDir, exist_ok=True)
        os.makedirs(self.filesDir, exist_ok=True)
        os.makedirs(self.jobFilesDir, exist_ok=True)
        os.makedirs(self.sharedFilesDir, exist_ok=True)
        self.linkImports = config.linkImports
        self.moveExports = config.moveExports
        super().initialize(config)

    def resume(self):
        if not os.path.isdir(self.jobStoreDir):
            raise NoSuchJobStoreException(self.jobStoreDir)
        super().resume()

    def destroy(self):
        if os.path.exists(self.jobStoreDir):
            robust_rmtree(self.jobStoreDir)

    ##########################################
    # The following methods deal with creating/loading/updating/writing/checking for the
    # existence of jobs
    ##########################################

    def assign_job_id(self, job_description):
        # Get the job's name. We want to group jobs with the same name together.
        # This will be e.g. the function name for wrapped-function jobs.
        # Make sure to render it filename-safe
        usefulFilename = self._make_string_filename_safe(job_description.jobName)

        # Make a unique temp directory under a directory for this job name,
        # possibly sprayed across multiple levels of subdirectories.
        absJobDir = tempfile.mkdtemp(prefix=self.JOB_DIR_PREFIX,
                                     dir=self._get_arbitrary_jobs_dir_for_name(usefulFilename))

        job_description.jobStoreID = self._get_job_id_from_dir(absJobDir)

    def create_job(self, job_description):
        if hasattr(self, "_batchedUpdates") and self._batchedUpdates is not None:
            # Save it later
            self._batchedUpdates.append(job_description)
        else:
            # Save it now
            self.update_job(job_description)
        return job_description

    @contextmanager
    def batch(self):
        self._batchedUpdates = []
        yield
        for jobDescription in self._batchedUpdates:
            self.update_job(jobDescription)
        self._batchedUpdates = None

    def _wait_for_exists(self, jobStoreID, maxTries=35, sleepTime=1):
        """
        Spin-wait and block for a job to appear before returning
        False if it does not.
        """
        return self._wait_for_file(self._get_job_file_name(jobStoreID), maxTries=maxTries, sleepTime=sleepTime)

    def _wait_for_file(self, fileName, maxTries=35, sleepTime=1):
        """
        Spin-wait and block for a file or directory to appear before returning
        False if it does not.

        The total max wait time is maxTries * sleepTime. The current default is
        tuned to match Linux NFS defaults where the client's cache of the directory
        listing on the server is supposed to become coherent within 30 sec.
        Delayes beyond that would probably indicate a pathologically slow file system
        that just should not be used for the jobStore.

        The warning will be sent to the log only on the first retry.

        In practice, the need for retries happens rarely, but it does happen
        over the course of large workflows with a jobStore on a busy NFS.
        """
        for iTry in range(1,maxTries+1):
            if os.path.exists(fileName):
                return True
            if iTry >= maxTries:
                return False
            elif iTry == 1:
                logger.warning(("Path `{}` does not exist (yet). We will try #{} more times with {}s "
                        "intervals.").format(fileName, maxTries - iTry, sleepTime))
            time.sleep(sleepTime)
        return False

    def job_exists(self, job_id):
        return os.path.exists(self._get_job_file_name(job_id))

    def get_public_url(self, jobStoreFileID):
        self._check_job_store_file_id(jobStoreFileID)
        jobStorePath = self._get_file_path_from_id(jobStoreFileID)
        if os.path.exists(jobStorePath):
            return 'file:' + jobStorePath
        else:
            raise NoSuchFileException(jobStoreFileID)

    def get_shared_public_url(self, sharedFileName):
        jobStorePath = os.path.join(self.sharedFilesDir, sharedFileName)
        if not os.path.exists(jobStorePath):
            raise NoSuchFileException(sharedFileName)
        return 'file:' + jobStorePath

    def load_job(self, job_id):
        # If the job obviously doesn't exist, note that.
        self._check_job_store_id_exists(job_id)
        # Try to load a valid version of the job.
        jobFile = self._get_job_file_name(job_id)
        try:
            with open(jobFile, 'rb') as fileHandle:
                job = pickle.load(fileHandle)
        except FileNotFoundError:
            # We were racing a delete on a non-POSIX-compliant filesystem.
            # This is the good case; the delete arrived in time.
            # If it didn't, we might go on to re-execute the already-finished job.
            # Anyway, this job doesn't really exist after all.
            raise NoSuchJobException(job_id)

        # Pass along the current config, which is the JobStore's responsibility.
        job.assignConfig(self.config)

        # The following cleans up any issues resulting from the failure of the
        # job during writing by the batch system.
        if os.path.isfile(jobFile + ".new"):
            logger.warning("There was a .new file for the job: %s", job_id)
            os.remove(jobFile + ".new")
            job.setupJobAfterFailure()
        return job

    def update_job(self, job):
        assert job.jobStoreID is not None, f"Tried to update job {job} without an ID"
        assert not isinstance(job.jobStoreID, TemporaryID), f"Tried to update job {job} without an assigned ID"

        job.pre_update_hook()

        # The job is serialised to a file suffixed by ".new"
        # We insist on creating the file; an existing .new file indicates
        # multiple simultaneous attempts to update the job, which will lose
        # updates.
        # The file is then moved to its correct path.
        # Atomicity guarantees use the fact the underlying file systems "move"
        # function is atomic.
        with open(self._get_job_file_name(job.jobStoreID) + ".new", 'xb') as f:
            pickle.dump(job, f)
        # This should be atomic for the file system
        os.rename(self._get_job_file_name(job.jobStoreID) + ".new", self._get_job_file_name(job.jobStoreID))

    def delete_job(self, job_id):
        # The jobStoreID is the relative path to the directory containing the job,
        # removing this directory deletes the job.
        if self.job_exists(job_id):
            # Remove the job-associated files in need of cleanup, which may or
            # may not live under the job's directory.
            robust_rmtree(self._get_job_files_cleanup_dir(job_id))
            # Remove the job's directory itself.
            robust_rmtree(self._get_job_dir_from_id(job_id))

    def jobs(self):
        # Walk through list of temporary directories searching for jobs.
        # Jobs are files that start with 'job'.
        # Note that this also catches jobWhatever.new which exists if an update
        # is in progress.
        for tempDir in self._job_directories():
            for i in os.listdir(tempDir):
                if i.startswith(self.JOB_DIR_PREFIX):
                    # This is a job instance directory
                    jobId = self._get_job_id_from_dir(os.path.join(tempDir, i))
                    try:
                        if self.job_exists(jobId):
                            yield self.load_job(jobId)
                    except NoSuchJobException:
                        # An orphaned job may leave an empty or incomplete job file which we can safely ignore
                        pass

    ##########################################
    # Functions that deal with temporary files associated with jobs
    ##########################################

    @contextmanager
    def optional_hard_copy(self, hardlink):
        if hardlink:
            saved = self.linkImports
            self.linkImports = False
        yield
        if hardlink:
            self.linkImports = saved

    def _copy_or_link(self, src_path, dst_path, symlink=False):
        # linking is not done be default because of issue #1755
        srcPath = self._extract_path_from_url(src_path)
        if self.linkImports and symlink:
            os.symlink(os.path.realpath(srcPath), dst_path)
        else:
            atomic_copy(srcPath, dst_path)

    def _import_file(self, otherCls, uri, shared_file_name=None, hardlink=False, symlink=True):
        # symlink argument says whether the caller can take symlinks or not
        # ex: if false, it implies the workflow cannot work with symlinks and thus will hardlink imports
        # default is true since symlinking everything is ideal
        if issubclass(otherCls, FileJobStore):
            if os.path.isdir(uri.path):
                # Don't allow directories (unless someone is racing us)
                raise IsADirectoryError(f"URI {uri} points to a directory but a file was expected")
            if shared_file_name is None:
                executable = os.stat(uri.path).st_mode & stat.S_IXUSR != 0
                absPath = self._get_unique_file_path(uri.path)  # use this to get a valid path to write to in job store
                with self.optional_hard_copy(hardlink):
                    self._copy_or_link(uri, absPath, symlink=symlink)
                # TODO: os.stat(absPath).st_size consistently gives values lower than
                # getDirSizeRecursively()
                return FileID(self._get_file_id_from_path(absPath), os.stat(absPath).st_size, executable)
            else:
                self._requireValidSharedFileName(shared_file_name)
                path = self._get_shared_file_path(shared_file_name)
                with self.optional_hard_copy(hardlink):
                    self._copy_or_link(uri, path, symlink=symlink)
                return None
        else:
            return super()._import_file(otherCls, uri, shared_file_name=shared_file_name)

    def _export_file(self, otherCls, file_id, uri):
        if issubclass(otherCls, FileJobStore):
            srcPath = self._get_file_path_from_id(file_id)
            destPath = self._extract_path_from_url(uri)
            executable = getattr(file_id, 'executable', False)
            if self.moveExports:
                self._move_and_linkback(srcPath, destPath, executable=executable)
            else:
                atomic_copy(srcPath, destPath, executable=executable)
        else:
            super()._default_export_file(otherCls, file_id, uri)

    def _move_and_linkback(self, srcPath, destPath, executable):
        logger.debug("moveExports option, Moving src=%s to dest=%s ; then symlinking dest to src", srcPath, destPath)
        shutil.move(srcPath, destPath)
        os.symlink(destPath, srcPath)
        if executable:
            os.chmod(destPath, os.stat(destPath).st_mode | stat.S_IXUSR)

    @classmethod
    def get_size(cls, url):
        return os.stat(cls._extract_path_from_url(url)).st_size

    @classmethod
    def _read_from_url(cls, url, writable):
        """
        Writes the contents of a file to a source (writes url to writable)
        using a ~10Mb buffer.

        :param str url: A path as a string of the file to be read from.
        :param object writable: An open file object to write to.
        """

        # we use a ~10Mb buffer to improve speed
        with open(cls._extract_path_from_url(url), 'rb') as readable:
            shutil.copyfileobj(readable, writable, length=cls.BUFFER_SIZE)
            # Return the number of bytes we read when we reached EOF.
            executable = os.stat(readable.name).st_mode & stat.S_IXUSR
            return readable.tell(), executable


    @classmethod
    def _write_to_url(cls, readable, url, executable=False):
        """
        Writes the contents of a file to a source (writes readable to url)
        using a ~10Mb buffer.

        :param str url: A path as a string of the file to be written to.
        :param object readable: An open file object to read from.
        """
        # we use a ~10Mb buffer to improve speed
        atomic_copyobj(readable,
                       cls._extract_path_from_url(url),
                       length=cls.BUFFER_SIZE,
                       executable=executable)

    @classmethod
    def _list_url(cls, url: ParseResult) -> List[str]:
        path = cls._extract_path_from_url(url)
        listing = []
        for p in os.listdir(path):
            # We know there are no slashes in these
            component = quote(p)
            # Return directories with trailing slashes and files without
            listing.append((component + '/') if os.path.isdir(os.path.join(path, p)) else component)
        return listing

    @classmethod
    def _get_is_directory(cls, url: ParseResult) -> bool:
        path = cls._extract_path_from_url(url)
        return os.path.isdir(path)

    @staticmethod
    def _extract_path_from_url(url):
        """
        :return: local file path of file pointed at by the given URL
        """
        if url.netloc != '' and url.netloc != 'localhost':
            raise RuntimeError("The URL '%s' is invalid" % url.geturl())
        return unquote(url.path)

    @classmethod
    def _supports_url(cls, url, export=False):
        return url.scheme.lower() == 'file'

    def _make_string_filename_safe(self, arbitraryString, maxLength=240):
        """
        Given an arbitrary string, produce a filename-safe though not
        necessarily unique string based on it.

        The input string may be discarded altogether and replaced with any
        other nonempty filename-safe string.

        :param str arbitraryString: An arbitrary string
        :param int maxLength: Maximum length of the result, to keep it plus
                              any prefix or suffix under the filesystem's
                              path component length limit

        :return: A filename-safe string
        """

        # We will fill this in with the filename-safe parts we find.
        parts = []

        for substring in re.findall("[A-Za-z0-9._-]+", arbitraryString):
            # Collect all the matching substrings
            parts.append(substring)

        if len(parts) == 0:
            parts.append("UNPRINTABLE")

        # Glue it all together, and truncate to length
        return '_'.join(parts)[:maxLength]

    def write_file(self, local_path, job_id=None, cleanup=False):
        absPath = self._get_unique_file_path(local_path, job_id, cleanup)
        relPath = self._get_file_id_from_path(absPath)
        atomic_copy(local_path, absPath)
        return relPath

    @contextmanager
    def write_file_stream(self, job_id=None, cleanup=False, basename=None, encoding=None, errors=None):
        if not basename:
            basename = 'stream'
        absPath = self._get_unique_file_path(basename, job_id, cleanup)
        relPath = self._get_file_id_from_path(absPath)

        with open(absPath, 'wb' if encoding == None else 'wt', encoding=encoding, errors=errors) as f:
            # Don't yield while holding an open file descriptor to the temp
            # file. That can result in temp files still being open when we try
            # to clean ourselves up, somehow, for certain workloads.
            yield f, relPath

    def get_empty_file_store_id(self, jobStoreID=None, cleanup=False, basename=None):
        with self.write_file_stream(jobStoreID, cleanup, basename) as (fileHandle, jobStoreFileID):
            return jobStoreFileID

    def update_file(self, file_id, local_path):
        self._check_job_store_file_id(file_id)
        jobStoreFilePath = self._get_file_path_from_id(file_id)

        if os.path.samefile(jobStoreFilePath, local_path):
            # The files are already the same file. We can't copy on eover the other.
            return

        atomic_copy(local_path, jobStoreFilePath)

    def read_file(self, file_id, local_path, symlink=False):
        self._check_job_store_file_id(file_id)
        jobStoreFilePath = self._get_file_path_from_id(file_id)
        localDirPath = os.path.dirname(local_path)
        executable = getattr(file_id, 'executable', False)

        if not symlink and os.path.islink(local_path):
            # We had a symlink and want to clobber it with a hardlink or copy.
            os.unlink(local_path)

        if os.path.exists(local_path) and os.path.samefile(jobStoreFilePath, local_path):
            # The files are already the same: same name, hardlinked, or
            # symlinked. There is nothing to do, and trying to shutil.copyfile
            # one over the other will fail.
            return

        if symlink:
            # If the reader will accept a symlink, so always give them one.
            # There's less that can go wrong.
            try:
                os.symlink(jobStoreFilePath, local_path)
                # It worked!
                return
            except OSError as e:
                # For the list of the possible errno codes, see: https://linux.die.net/man/2/symlink
                if e.errno == errno.EEXIST:
                    # Overwrite existing file, emulating shutil.copyfile().
                    os.unlink(local_path)
                    # It would be very unlikely to fail again for same reason but possible
                    # nonetheless in which case we should just give up.
                    os.symlink(jobStoreFilePath, local_path)
                    # Now we succeeded and don't need to copy
                    return
                elif e.errno == errno.EPERM:
                    # On some filesystems, the creation of symbolic links is not possible.
                    # In this case, we try to make a hard link.
                    pass
                else:
                    logger.error(f"Unexpected OSError when reading file '{jobStoreFilePath}' from job store")
                    raise

        # If we get here, symlinking isn't an option.
        # Make sure we are working with the real source path, in case it is a
        # symlinked import.
        jobStoreFilePath = os.path.realpath(jobStoreFilePath)

        if os.stat(jobStoreFilePath).st_dev == os.stat(localDirPath).st_dev:
            # It is possible that we can hard link the file.
            # Note that even if the device numbers match, we can end up trying
            # to create a "cross-device" link.

            try:
                os.link(jobStoreFilePath, local_path)
                # It worked!
                return
            except OSError as e:
                # For the list of the possible errno codes, see: https://linux.die.net/man/2/link
                if e.errno == errno.EEXIST:
                    # Overwrite existing file, emulating shutil.copyfile().
                    os.unlink(local_path)
                    # It would be very unlikely to fail again for same reason but possible
                    # nonetheless in which case we should just give up.
                    os.link(jobStoreFilePath, local_path)
                    # Now we succeeded and don't need to copy
                    return
                elif e.errno == errno.EXDEV:
                    # It's a cross-device link even though it didn't appear to be.
                    # Just keep going and hit the file copy case.
                    pass
                elif e.errno == errno.EPERM:
                    # On some filesystems, hardlinking could be disallowed by permissions.
                    # In this case, we also fall back to making a complete copy.
                    pass
                elif e.errno == errno.ELOOP:
                    # Too many symbolic links were encountered. Just keep going and hit the
                    # file copy case.
                    pass
                elif e.errno == errno.EMLINK:
                    # The maximum number of links to file is reached. Just keep going and
                    # hit the file copy case.
                    pass
                else:
                    logger.error(f"Unexpected OSError when reading file '{jobStoreFilePath}' from job store")
                    raise

        # If we get here, neither a symlink nor a hardlink will work.
        # Make a complete copy.
        atomic_copy(jobStoreFilePath, local_path, executable=executable)

    def delete_file(self, file_id):
        if not self.file_exists(file_id):
            return
        os.remove(self._get_file_path_from_id(file_id))

    def file_exists(self, file_id):
        absPath = self._get_file_path_from_id(file_id)

        if (not absPath.startswith(self.jobsDir) and
            not absPath.startswith(self.filesDir) and
            not absPath.startswith(self.jobFilesDir)):
            # Don't even look for it, it is out of bounds.
            raise NoSuchFileException(file_id)

        try:
            st = os.stat(absPath)
        except os.error:
            return False
        if not stat.S_ISREG(st.st_mode):
            raise NoSuchFileException(file_id)
        return True

    def get_file_size(self, file_id):
        # Duplicate a bunch of fileExists to save on stat calls
        absPath = self._get_file_path_from_id(file_id)

        if (not absPath.startswith(self.jobsDir) and
            not absPath.startswith(self.filesDir) and
            not absPath.startswith(self.jobFilesDir)):
            # Don't even look for it, it is out of bounds.
            raise NoSuchFileException(file_id)

        try:
            st = os.stat(absPath)
        except os.error:
            return 0
        return st.st_size

    @contextmanager
    def update_file_stream(self, file_id, encoding=None, errors=None):
        self._check_job_store_file_id(file_id)
        # File objects are context managers (CM) so we could simply return what open returns.
        # However, it is better to wrap it in another CM so as to prevent users from accessing
        # the file object directly, without a with statement.
        with open(self._get_file_path_from_id(file_id), 'wb' if encoding == None else 'wt', encoding=encoding, errors=errors) as f:
            yield f

    @contextmanager
    @overload
    def read_file_stream(
        self,
        file_id: Union[str, FileID],
        encoding: Literal[None] = None,
        errors: Optional[str] = None,
    ) -> Iterator[IO[bytes]]:
        ...

    @contextmanager
    @overload
    def read_file_stream(
        self, file_id: Union[str, FileID], encoding: str, errors: Optional[str] = None
    ) -> Iterator[IO[str]]:
        ...

    @contextmanager
    @overload
    def read_file_stream(
        self,
        file_id: Union[str, FileID],
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Union[Iterator[IO[bytes]], Iterator[IO[str]]]:
        ...

    @contextmanager
    def read_file_stream(
        self,
        file_id: Union[str, FileID],
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Union[Iterator[IO[bytes]], Iterator[IO[str]]]:
        self._check_job_store_file_id(file_id)
        if encoding is None:
            with open(
                self._get_file_path_from_id(file_id),
                "rb",
                encoding=encoding,
                errors=errors,
            ) as fb:
                yield fb
        else:
            with open(
                self._get_file_path_from_id(file_id),
                buffering=1,  # line buffering
                encoding=encoding,
                errors=errors,
            ) as ft:
                yield ft

    ##########################################
    # The following methods deal with shared files, i.e. files not associated
    # with specific jobs.
    ##########################################

    def _get_shared_file_path(self, sharedFileName):
        return os.path.join(self.sharedFilesDir, sharedFileName)

    @contextmanager
    def write_shared_file_stream(self, shared_file_name, encrypted=None, encoding=None, errors=None):
        # the isProtected parameter has no effect on the fileStore
        self._requireValidSharedFileName(shared_file_name)
        with AtomicFileCreate(self._get_shared_file_path(shared_file_name)) as tmpSharedFilePath:
            with open(tmpSharedFilePath, 'wb' if encoding == None else 'wt', encoding=encoding, errors=None) as f:
                yield f

    @contextmanager
    def read_shared_file_stream(self, shared_file_name, encoding=None, errors=None):
        self._requireValidSharedFileName(shared_file_name)
        try:
            with open(self._get_shared_file_path(shared_file_name), 'rb' if encoding == None else 'rt', encoding=encoding, errors=errors) as f:
                yield f

        except OSError as e:
            if e.errno == errno.ENOENT:
                raise NoSuchFileException(shared_file_name)
            else:
                raise

    def write_logs(self, msg):
        # Temporary files are placed in the stats directory tree
        tempStatsFileName = "stats" + str(uuid.uuid4().hex) + ".new"
        tempStatsFile = os.path.join(self._get_arbitrary_stats_dir(), tempStatsFileName)
        writeFormat = 'w' if isinstance(msg, str) else 'wb'
        with open(tempStatsFile, writeFormat) as f:
            f.write(msg)
        os.rename(tempStatsFile, tempStatsFile[:-4])  # This operation is atomic

    def read_logs(self, callback, read_all=False):
        numberOfFilesProcessed = 0
        for tempDir in self._stats_directories():
            for tempFile in os.listdir(tempDir):
                if tempFile.startswith('stats'):
                    absTempFile = os.path.join(tempDir, tempFile)
                    if os.path.isfile(absTempFile):
                        if read_all or not tempFile.endswith('.new'):
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

    def _get_job_dir_from_id(self, jobStoreID):
        """

        Find the directory for a job, which holds its job file.

        :param str jobStoreID: ID of a job, which is a relative to self.jobsDir.
        :rtype : string, string is the absolute path to a job directory inside self.jobsDir.
        """
        return os.path.join(self.jobsDir, jobStoreID)

    def _get_job_id_from_dir(self, absPath):
        """
        :param str absPath: The absolute path to a job directory under self.jobsDir which represents a job.
        :rtype : string, string is the job ID, which is a path relative to self.jobsDir
        """
        return absPath[len(self.jobsDir)+1:]

    def _get_job_file_name(self, jobStoreID):
        """
        Return the path to the file containing the serialised JobDescription instance for the given
        job.

        :rtype: str
        """
        return os.path.join(self._get_job_dir_from_id(jobStoreID), "job")

    def _get_job_files_dir(self, jobStoreID):
        """
        Return the path to the directory that should hold files made by the
        given job that should survive its deletion.

        This directory will only be created if files are to be put in it.

        :rtype : string, string is the absolute path to the job's files
                 directory
        """

        return os.path.join(self.jobFilesDir, jobStoreID)

    def _get_job_files_cleanup_dir(self, jobStoreID):
        """
        Return the path to the directory that should hold files made by the
        given job that will be deleted when the job is deleted.

        This directory will only be created if files are to be put in it.

        It may or may not be a subdirectory of the job's own directory.

        :rtype : string, string is the absolute path to the job's cleanup
                 files directory
        """

        return os.path.join(self.jobFilesDir, jobStoreID, "cleanup")

    def _check_job_store_id_assigned(self, jobStoreID):
        """
        Do nothing if the given job store ID has been assigned by
        :meth:`assignID`, and the corresponding job has not yet been
        deleted, even if the JobDescription hasn't yet been saved for the first
        time.

        If the ID has not been assigned, raises a NoSuchJobException.
        """

        if not self._wait_for_file(self._get_job_dir_from_id(jobStoreID)):
            raise NoSuchJobException(jobStoreID)

    def _check_job_store_id_exists(self, jobStoreID):
        """
        Raises a NoSuchJobException if the job with ID jobStoreID does not exist.
        """
        if not self._wait_for_exists(jobStoreID, 30):
            raise NoSuchJobException(jobStoreID)

    def _get_file_path_from_id(self, jobStoreFileID):
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

    def _get_file_id_from_path(self, absPath):
        """
        :param str absPath: The absolute path of a file.

        :rtype : string, string is the file ID.
        """

        return absPath[len(self.jobStoreDir)+1:]

    def _check_job_store_file_id(self, jobStoreFileID):
        """
        :raise NoSuchFileException: if the file with ID jobStoreFileID does
                                    not exist or is not a file
        """
        if not self.file_exists(jobStoreFileID):
            raise NoSuchFileException(jobStoreFileID)

    def _get_arbitrary_jobs_dir_for_name(self, jobNameSlug):
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
            return self._get_dynamic_spray_dir(os.path.join(self._get_dynamic_spray_dir(self.jobsDir),
                                                            self.JOB_NAME_DIR_PREFIX + jobNameSlug))
        else:
            # Just go in the root
            return self._get_dynamic_spray_dir(os.path.join(self.jobsDir, self.JOB_NAME_DIR_PREFIX + jobNameSlug))

    def _get_arbitrary_stats_dir(self):
        """
        Gets a temporary directory in a multi-level hierarchy in self.statsDir.
        The directory is not unique and may already have other stats files in it.

        :rtype : string, path to temporary directory in which to place files/directories.


        """

        return self._get_dynamic_spray_dir(self.statsDir)

    def _get_arbitrary_files_dir(self):
        """
        Gets a temporary directory in a multi-level hierarchy in self.filesDir.
        The directory is not unique and may already have other user files in it.

        :rtype : string, path to temporary directory in which to place files/directories.


        """

        return self._get_dynamic_spray_dir(self.filesDir)

    def _get_dynamic_spray_dir(self, root):
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
        os.makedirs(tempDir, exist_ok=True)

        while len(os.listdir(tempDir)) >= self.fanOut:
            # We need to use a layer of directories under here to avoid over-packing the directory
            tempDir = os.path.join(tempDir, random.choice(self.validDirs))
            os.makedirs(tempDir, exist_ok=True)

        # When we get here, we found a sufficiently empty directory
        return tempDir

    def _walk_dynamic_spray_dir(self, root):
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
            yield from self._walk_dynamic_spray_dir(childPath)

    def _job_directories(self):
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

        for jobHoldingDir in self._walk_dynamic_spray_dir(self.jobsDir):
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
                yield from self._walk_dynamic_spray_dir(os.path.join(jobHoldingDir, jobNameDir))


    def _stats_directories(self):
        """
        :rtype : an iterator to the temporary directories containing stats
                 files. They may also contain directories containing more
                 stats files.
        """

        return self._walk_dynamic_spray_dir(self.statsDir)

    def _get_unique_file_path(self, fileName, jobStoreID=None, cleanup=False):
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
        directory = self._get_file_directory(jobStoreID, cleanup)
        # And then a path under it
        uniquePath = os.path.join(directory, os.path.basename(fileName))
        # No need to check if it exists already; it is in a unique directory.
        return uniquePath

    def _get_file_directory(self, jobStoreID=None, cleanup=False):
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
            self._check_job_store_id_assigned(jobStoreID)
            # Find where all its created files should live, depending on if
            # they need to go away when the job is deleted or not.
            jobFilesDir = self._get_job_files_dir(jobStoreID) if not cleanup else self._get_job_files_cleanup_dir(jobStoreID)

            # Lazily create the parent directory.
            # We don't want our tree filled with confusingly empty directories.
            os.makedirs(jobFilesDir, exist_ok=True)

            # Then make a temp directory inside it
            filesDir = os.path.join(jobFilesDir, 'file-' + uuid.uuid4().hex)
            os.mkdir(filesDir)
            return filesDir
        else:
            # Make a temporary file within the non-job-associated files hierarchy
            filesDir = os.path.join(self._get_arbitrary_files_dir(), 'file-' + uuid.uuid4().hex)
            os.mkdir(filesDir)
            return filesDir
