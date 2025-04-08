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
import pickle
import re
import shutil
from abc import ABC, ABCMeta, abstractmethod
from collections.abc import Iterator, ValuesView
from contextlib import closing, contextmanager
from datetime import timedelta
from http.client import BadStatusLine
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Literal,
    Optional,
    Union,
    cast,
    overload,
)
from urllib.error import HTTPError
from urllib.parse import ParseResult, urlparse
from urllib.request import urlopen, Request
from uuid import uuid4

from toil.common import Config, getNodeID, safeUnpickleFromStream
from toil.fileStores import FileID
from toil.job import (
    CheckpointJobDescription,
    JobDescription,
    JobException,
    ServiceJobDescription,
)
from toil.lib.ftp_utils import FtpFsAccess
from toil.lib.compatibility import deprecated
from toil.lib.exceptions import UnimplementedURLException
from toil.lib.io import WriteWatchingStream
from toil.lib.memoize import memoize
from toil.lib.retry import ErrorCondition, retry

if TYPE_CHECKING:
    from toil.job import TemporaryID

logger = logging.getLogger(__name__)

try:
    from botocore.exceptions import ProxyConnectionError
except ImportError:

    class ProxyConnectionError(BaseException):  # type: ignore
        """Dummy class."""


class LocatorException(Exception):
    """
    Base exception class for all locator exceptions.
    For example, job store/aws bucket exceptions where they already exist
    """

    def __init__(self, error_msg: str, locator: str, prefix: Optional[str] = None):
        full_locator = locator if prefix is None else f"{prefix}:{locator}"
        super().__init__(error_msg % full_locator)


class InvalidImportExportUrlException(Exception):
    def __init__(self, url: ParseResult) -> None:
        """
        :param url: The given URL
        """
        super().__init__("The URL '%s' is invalid." % url.geturl())


class NoSuchJobException(Exception):
    """Indicates that the specified job does not exist."""

    def __init__(self, jobStoreID: FileID):
        """
        :param str jobStoreID: the jobStoreID that was mistakenly assumed to exist
        """
        super().__init__("The job '%s' does not exist." % jobStoreID)


class ConcurrentFileModificationException(Exception):
    """Indicates that the file was attempted to be modified by multiple processes at once."""

    def __init__(self, jobStoreFileID: FileID):
        """
        :param jobStoreFileID: the ID of the file that was modified by multiple workers
               or processes concurrently
        """
        super().__init__("Concurrent update to file %s detected." % jobStoreFileID)


class NoSuchFileException(Exception):
    """Indicates that the specified file does not exist."""

    def __init__(
        self, jobStoreFileID: FileID, customName: Optional[str] = None, *extra: Any
    ):
        """
        :param jobStoreFileID: the ID of the file that was mistakenly assumed to exist
        :param customName: optionally, an alternate name for the nonexistent file
        :param list extra: optional extra information to add to the error message
        """
        # Having the extra argument may help resolve the __init__() takes at
        # most three arguments error reported in
        # https://github.com/DataBiosphere/toil/issues/2589#issuecomment-481912211
        if customName is None:
            message = "File '%s' does not exist." % jobStoreFileID
        else:
            message = f"File '{customName}' ({jobStoreFileID}) does not exist."

        if extra:
            # Append extra data.
            message += " Extra info: " + " ".join(str(x) for x in extra)

        super().__init__(message)


class NoSuchJobStoreException(LocatorException):
    """Indicates that the specified job store does not exist."""

    def __init__(self, locator: str, prefix: str):
        """
        :param str locator: The location of the job store
        """
        super().__init__(
            "The job store '%s' does not exist, so there is nothing to restart.",
            locator,
            prefix,
        )


class JobStoreExistsException(LocatorException):
    """Indicates that the specified job store already exists."""

    def __init__(self, locator: str, prefix: str):
        """
        :param str locator: The location of the job store
        """
        super().__init__(
            "The job store '%s' already exists. Use --restart to resume the workflow, or remove "
            "the job store with 'toil clean' to start the workflow from scratch.",
            locator,
            prefix,
        )


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

    def __init__(self, locator: str) -> None:
        """
        Create an instance of the job store.

        The instance will not be fully functional until either :meth:`.initialize`
        or :meth:`.resume` is invoked. Note that the :meth:`.destroy` method may
        be invoked on the object with or without prior invocation of either of
        these two methods.

        Takes and stores the locator string for the job store, which will be
        accessible via self.locator.
        """
        self.__locator = locator

    def initialize(self, config: Config) -> None:
        """
        Initialize this job store.

        Create the physical storage for this job store, allocate a workflow ID
        and persist the given Toil configuration to the store.

        :param config: the Toil configuration to initialize this job store with.
                       The given configuration will be updated with the newly
                       allocated workflow ID.

        :raises JobStoreExistsException: if the physical storage for this job store
                                         already exists
        """
        assert config.workflowID is None
        config.workflowID = str(uuid4())
        logger.debug("The workflow ID is: '%s'" % config.workflowID)
        self.__config = config
        self.write_config()

    @deprecated(new_function_name="write_config")
    def writeConfig(self) -> None:
        return self.write_config()

    def write_config(self) -> None:
        """
        Persists the value of the :attr:`AbstractJobStore.config` attribute to the
        job store, so that it can be retrieved later by other instances of this class.
        """
        with self.write_shared_file_stream(
            "config.pickle", encrypted=False
        ) as fileHandle:
            pickle.dump(self.__config, fileHandle, pickle.HIGHEST_PROTOCOL)

    def resume(self) -> None:
        """
        Connect this instance to the physical storage it represents and load the Toil configuration
        into the :attr:`AbstractJobStore.config` attribute.

        :raises NoSuchJobStoreException: if the physical storage for this job store doesn't exist
        """
        with self.read_shared_file_stream("config.pickle") as fileHandle:
            config = safeUnpickleFromStream(fileHandle)
            assert config.workflowID is not None
            self.__config = config

    @property
    def config(self) -> Config:
        """Return the Toil configuration associated with this job store."""
        return self.__config

    @property
    def locator(self) -> str:
        """
        Get the locator that defines the job store, which can be used to
        connect to it.
        """
        return self.__locator

    rootJobStoreIDFileName = "rootJobStoreID"

    @deprecated(new_function_name="set_root_job")
    def setRootJob(self, rootJobStoreID: FileID) -> None:
        """Set the root job of the workflow backed by this job store."""
        return self.set_root_job(rootJobStoreID)

    def set_root_job(self, job_id: FileID) -> None:
        """
        Set the root job of the workflow backed by this job store.

        :param job_id: The ID of the job to set as root
        """
        with self.write_shared_file_stream(self.rootJobStoreIDFileName) as f:
            f.write(job_id.encode("utf-8"))

    @deprecated(new_function_name="load_root_job")
    def loadRootJob(self) -> JobDescription:
        return self.load_root_job()

    def load_root_job(self) -> JobDescription:
        """
        Loads the JobDescription for the root job in the current job store.

        :raises toil.job.JobException: If no root job is set or if the root job doesn't exist in
                this job store

        :return: The root job.
        """
        try:
            with self.read_shared_file_stream(self.rootJobStoreIDFileName) as f:
                rootJobStoreID = f.read().decode("utf-8")
        except NoSuchFileException:
            raise JobException("No job has been set as the root in this job store")
        if not self.job_exists(rootJobStoreID):
            raise JobException(
                "The root job '%s' doesn't exist. Either the Toil workflow "
                "is finished or has never been started" % rootJobStoreID
            )
        return self.load_job(rootJobStoreID)

    # FIXME: This is only used in tests, why do we have it?
    @deprecated(new_function_name="create_root_job")
    def createRootJob(self, desc: JobDescription) -> JobDescription:
        return self.create_root_job(desc)

    # FIXME: This is only used in tests, why do we have it?
    def create_root_job(self, job_description: JobDescription) -> JobDescription:
        """
        Create the given JobDescription and set it as the root job in this job store.

        :param job_description: JobDescription to save and make the root job.
        """
        self.create_job(job_description)
        if not isinstance(job_description.jobStoreID, FileID):
            raise Exception(f"Must use a registered JobDescription: {job_description}")
        self.set_root_job(job_description.jobStoreID)
        return job_description

    @deprecated(new_function_name="get_root_job_return_value")
    def getRootJobReturnValue(self) -> Any:
        return self.get_root_job_return_value()

    def get_root_job_return_value(self) -> Any:
        """
        Parse the return value from the root job.

        Raises an exception if the root job hasn't fulfilled its promise yet.
        """
        # Parse out the return value from the root job
        with self.read_shared_file_stream("rootJobReturnValue") as fH:
            return safeUnpickleFromStream(fH)

    @staticmethod
    @memoize
    def _get_job_store_classes() -> list["AbstractJobStore"]:
        """
        A list of concrete AbstractJobStore implementations whose dependencies are installed.

        :rtype: List[AbstractJobStore]
        """
        jobStoreClassNames = (
            "toil.jobStores.fileJobStore.FileJobStore",
            "toil.jobStores.googleJobStore.GoogleJobStore",
            "toil.jobStores.aws.jobStore.AWSJobStore",
            "toil.jobStores.abstractJobStore.JobStoreSupport",
        )
        jobStoreClasses = []
        for className in jobStoreClassNames:
            moduleName, className = className.rsplit(".", 1)
            from importlib import import_module

            try:
                module = import_module(moduleName)
            except (ImportError, ProxyConnectionError):
                logger.debug(
                    "Unable to import '%s' as is expected if the corresponding extra was "
                    "omitted at installation time.",
                    moduleName,
                )
            else:
                jobStoreClass = getattr(module, className)
                jobStoreClasses.append(jobStoreClass)
        return jobStoreClasses

    @classmethod
    def _findJobStoreForUrl(
        cls, url: ParseResult, export: bool = False
    ) -> "AbstractJobStore":
        """
        Returns the AbstractJobStore subclass that supports the given URL.

        :param ParseResult url: The given URL

        :param bool export: Determines if the url is supported for exporting

        :rtype: toil.jobStore.AbstractJobStore
        """
        for implementation in cls._get_job_store_classes():
            if implementation._supports_url(url, export):
                return implementation
        raise UnimplementedURLException(url, "export" if export else "import")

    # Importing a file with a shared file name returns None, but without one it
    # returns a file ID. Explain this to MyPy.

    @overload
    def importFile(
        self,
        srcUrl: str,
        sharedFileName: str,
        hardlink: bool = False,
        symlink: bool = True,
    ) -> None: ...

    @overload
    def importFile(
        self,
        srcUrl: str,
        sharedFileName: None = None,
        hardlink: bool = False,
        symlink: bool = True,
    ) -> FileID: ...

    @deprecated(new_function_name="import_file")
    def importFile(
        self,
        srcUrl: str,
        sharedFileName: Optional[str] = None,
        hardlink: bool = False,
        symlink: bool = True,
    ) -> Optional[FileID]:
        return self.import_file(srcUrl, sharedFileName, hardlink, symlink)

    @overload
    def import_file(
        self,
        src_uri: str,
        shared_file_name: str,
        hardlink: bool = False,
        symlink: bool = True,
    ) -> None: ...

    @overload
    def import_file(
        self,
        src_uri: str,
        shared_file_name: None = None,
        hardlink: bool = False,
        symlink: bool = True,
    ) -> FileID: ...

    def import_file(
        self,
        src_uri: str,
        shared_file_name: Optional[str] = None,
        hardlink: bool = False,
        symlink: bool = True,
    ) -> Optional[FileID]:
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

        Raises FileNotFoundError if the file does not exist.

        :param str src_uri: URL that points to a file or object in the storage mechanism of a
                supported URL scheme e.g. a blob in an AWS s3 bucket. It must be a file, not a
                directory or prefix.

        :param str shared_file_name: Optional name to assign to the imported file within the job store

        :return: The jobStoreFileID of the imported file or None if shared_file_name was given
        :rtype: toil.fileStores.FileID or None
        """
        # Note that the helper method _import_file is used to read from the source and write to
        # destination (which is the current job store in this case). To implement any
        # optimizations that circumvent this, the _import_file method should be overridden by
        # subclasses of AbstractJobStore.
        parseResult = urlparse(src_uri)
        otherCls = self._findJobStoreForUrl(parseResult)
        logger.info("Importing input %s...", src_uri)
        return self._import_file(
            otherCls,
            parseResult,
            shared_file_name=shared_file_name,
            hardlink=hardlink,
            symlink=symlink,
        )

    def _import_file(
        self,
        otherCls: "AbstractJobStore",
        uri: ParseResult,
        shared_file_name: Optional[str] = None,
        hardlink: bool = False,
        symlink: bool = True,
    ) -> Optional[FileID]:
        """
        Import the file at the given URL using the given job store class to retrieve that file.
        See also :meth:`.importFile`. This method applies a generic approach to importing: it
        asks the other job store class for a stream and writes that stream as either a regular or
        a shared file.

        Raises FileNotFoundError if the file does not exist.

        :param AbstractJobStore otherCls: The concrete subclass of AbstractJobStore that supports
               reading from the given URL and getting the file size from the URL.

        :param ParseResult uri: The location of the file to import.

        :param str shared_file_name: Optional name to assign to the imported file within the job store

        :return The FileID of imported file or None if sharedFileName was given
        :rtype: toil.fileStores.FileID or None
        """

        if shared_file_name is None:
            with self.write_file_stream() as (writable, jobStoreFileID):
                size, executable = otherCls._read_from_url(uri, writable)
                return FileID(jobStoreFileID, size, executable)
        else:
            self._requireValidSharedFileName(shared_file_name)
            with self.write_shared_file_stream(shared_file_name) as writable:
                otherCls._read_from_url(uri, writable)
                return None

    @deprecated(new_function_name="export_file")
    def exportFile(self, jobStoreFileID: FileID, dstUrl: str) -> None:
        return self.export_file(jobStoreFileID, dstUrl)

    def export_file(self, file_id: FileID, dst_uri: str) -> None:
        """
        Exports file to destination pointed at by the destination URL. The exported file will be
        executable if and only if it was originally uploaded from an executable file on the
        local filesystem.

        Refer to :meth:`.AbstractJobStore.import_file` documentation for currently supported URL schemes.

        Note that the helper method _exportFile is used to read from the source and write to
        destination. To implement any optimizations that circumvent this, the _exportFile method
        should be overridden by subclasses of AbstractJobStore.

        :param str file_id: The id of the file in the job store that should be exported.

        :param str dst_uri: URL that points to a file or object in the storage mechanism of a
                supported URL scheme e.g. a blob in an AWS s3 bucket. May also be a local path.
        """
        from toil.common import Toil
        dst_uri = Toil.normalize_uri(dst_uri)
        parseResult = urlparse(dst_uri)
        otherCls = self._findJobStoreForUrl(parseResult, export=True)
        self._export_file(otherCls, file_id, parseResult)

    def _export_file(
        self, otherCls: "AbstractJobStore", jobStoreFileID: FileID, url: ParseResult
    ) -> None:
        """
        Refer to exportFile docstring for information about this method.

        :param AbstractJobStore otherCls: The concrete subclass of AbstractJobStore that supports
               exporting to the given URL. Note that the type annotation here is not completely
               accurate. This is not an instance, it's a class, but there is no way to reflect
               that in :pep:`484` type hints.

        :param str jobStoreFileID: The id of the file that will be exported.

        :param ParseResult url: The parsed URL of the file to export to.
        """
        self._default_export_file(otherCls, jobStoreFileID, url)

    def _default_export_file(
        self, otherCls: "AbstractJobStore", jobStoreFileID: FileID, url: ParseResult
    ) -> None:
        """
        Refer to exportFile docstring for information about this method.

        :param AbstractJobStore otherCls: The concrete subclass of AbstractJobStore that supports
               exporting to the given URL. Note that the type annotation here is not completely
               accurate. This is not an instance, it's a class, but there is no way to reflect
               that in :pep:`484` type hints.

        :param str jobStoreFileID: The id of the file that will be exported.

        :param ParseResult url: The parsed URL of the file to export to.
        """
        executable = False
        with self.read_file_stream(jobStoreFileID) as readable:
            if getattr(jobStoreFileID, "executable", False):
                executable = jobStoreFileID.executable
            otherCls._write_to_url(readable, url, executable)

    @classmethod
    def url_exists(cls, src_uri: str) -> bool:
        """
        Return True if the file at the given URI exists, and False otherwise.

        May raise an error if file existence cannot be determined.

        :param src_uri: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.
        """
        parseResult = urlparse(src_uri)
        otherCls = cls._findJobStoreForUrl(parseResult)
        return otherCls._url_exists(parseResult)

    @classmethod
    def get_size(cls, src_uri: str) -> Optional[int]:
        """
        Get the size in bytes of the file at the given URL, or None if it cannot be obtained.

        :param src_uri: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.
        """
        parseResult = urlparse(src_uri)
        otherCls = cls._findJobStoreForUrl(parseResult)
        return otherCls._get_size(parseResult)

    @classmethod
    def get_is_directory(cls, src_uri: str) -> bool:
        """
        Return True if the thing at the given URL is a directory, and False if
        it is a file. The URL may or may not end in '/'.
        """
        parseResult = urlparse(src_uri)
        otherCls = cls._findJobStoreForUrl(parseResult)
        return otherCls._get_is_directory(parseResult)

    @classmethod
    def list_url(cls, src_uri: str) -> list[str]:
        """
        List the directory at the given URL. Returned path components can be
        joined with '/' onto the passed URL to form new URLs. Those that end in
        '/' correspond to directories. The provided URL may or may not end with
        '/'.

        Currently supported schemes are:

            - 's3' for objects in Amazon S3
                e.g. s3://bucket/prefix/

            - 'file' for local files
                e.g. file:///local/dir/path/

        :param str src_uri: URL that points to a directory or prefix in the storage mechanism of a
                supported URL scheme e.g. a prefix in an AWS s3 bucket.

        :return: A list of URL components in the given directory, already URL-encoded.
        """
        parseResult = urlparse(src_uri)
        otherCls = cls._findJobStoreForUrl(parseResult)
        return otherCls._list_url(parseResult)

    @classmethod
    def read_from_url(cls, src_uri: str, writable: IO[bytes]) -> tuple[int, bool]:
        """
        Read the given URL and write its content into the given writable stream.

        Raises FileNotFoundError if the URL doesn't exist.

        :return: The size of the file in bytes and whether the executable permission bit is set
        """
        parseResult = urlparse(src_uri)
        otherCls = cls._findJobStoreForUrl(parseResult)
        return otherCls._read_from_url(parseResult, writable)

    @classmethod
    def open_url(cls, src_uri: str) -> IO[bytes]:
        """
        Read from the given URI.

        Raises FileNotFoundError if the URL doesn't exist.

        Has a readable stream interface, unlike :meth:`read_from_url` which
        takes a writable stream.
        """
        parseResult = urlparse(src_uri)
        otherCls = cls._findJobStoreForUrl(parseResult)
        return otherCls._open_url(parseResult)

    @classmethod
    @abstractmethod
    def _url_exists(cls, url: ParseResult) -> bool:
        """
        Return True if the item at the given URL exists, and Flase otherwise.

        May raise an error if file existence cannot be determined.
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _get_size(cls, url: ParseResult) -> Optional[int]:
        """
        Get the size of the object at the given URL, or None if it cannot be obtained.
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _get_is_directory(cls, url: ParseResult) -> bool:
        """
        Return True if the thing at the given URL is a directory, and False if
        it is a file or it is known not to exist. The URL may or may not end in
        '/'.

        :param url: URL that points to a file or object, or directory or prefix,
               in the storage mechanism of a supported URL scheme e.g. a blob
               in an AWS s3 bucket.
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _read_from_url(cls, url: ParseResult, writable: IO[bytes]) -> tuple[int, bool]:
        """
        Reads the contents of the object at the specified location and writes it to the given
        writable stream.

        Refer to :func:`~AbstractJobStore.importFile` documentation for currently supported URL schemes.

        Raises FileNotFoundError if the thing at the URL is not found.

        :param ParseResult url: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.

        :param IO[bytes] writable: a writable stream

        :return: The size of the file in bytes and whether the executable permission bit is set
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _list_url(cls, url: ParseResult) -> list[str]:
        """
        List the contents of the given URL, which may or may not end in '/'

        Returns a list of URL components. Those that end in '/' are meant to be
        directories, while those that do not are meant to be files.

        Refer to :func:`~AbstractJobStore.importFile` documentation for currently supported URL schemes.

        :param ParseResult url: URL that points to a directory or prefix in the
        storage mechanism of a supported URL scheme e.g. a prefix in an AWS s3
        bucket.

        :return: The children of the given URL, already URL-encoded if
        appropriate. (If the URL is a bare path, no encoding is done.)
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _open_url(cls, url: ParseResult) -> IO[bytes]:
        """
        Get a stream of the object at the specified location.

        Refer to :func:`~AbstractJobStore.importFile` documentation for currently supported URL schemes.

        Raises FileNotFoundError if the thing at the URL is not found.
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _write_to_url(
        cls,
        readable: Union[IO[bytes], IO[str]],
        url: ParseResult,
        executable: bool = False,
    ) -> None:
        """
        Reads the contents of the given readable stream and writes it to the object at the
        specified location. Raises FileNotFoundError if the URL doesn't exist..

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param Union[IO[bytes], IO[str]] readable: a readable stream

        :param ParseResult url: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.

        :param bool executable: determines if the file has executable permissions
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _supports_url(cls, url: ParseResult, export: bool = False) -> bool:
        """
        Returns True if the job store supports the URL's scheme.

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param ParseResult url: a parsed URL that may be supported

        :param bool export: Determines if the url is supported for exported

        :return bool: returns true if the cls supports the URL
        """
        raise NotImplementedError(f"No implementation for {url}")

    @abstractmethod
    def destroy(self) -> None:
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

    @deprecated(new_function_name="get_env")
    def getEnv(self) -> dict[str, str]:
        return self.get_env()

    def get_env(self) -> dict[str, str]:
        """
        Returns a dictionary of environment variables that this job store requires to be set in
        order to function properly on a worker.

        :rtype: dict[str,str]
        """
        return {}

    # Cleanup functions
    def clean(
        self, jobCache: Optional[dict[Union[str, "TemporaryID"], JobDescription]] = None
    ) -> JobDescription:
        """
        Function to cleanup the state of a job store after a restart.

        Fixes jobs that might have been partially updated. Resets the try counts
        and removes jobs that are not successors of the current root job.

        :param jobCache: if a value it must be a dict
               from job ID keys to JobDescription object values. Jobs will be loaded
               from the cache (which can be downloaded from the job store in a batch)
               instead of piecemeal when recursed into.
        """
        if jobCache is None:
            logger.warning("Cleaning jobStore recursively. This may be slow.")

        # Functions to get and check the existence of jobs,
        # using the jobCache if present
        def getJobDescription(jobId: str) -> JobDescription:
            if jobCache is not None:
                try:
                    return jobCache[jobId]
                except KeyError:
                    return self.load_job(jobId)
            else:
                return self.load_job(jobId)

        def haveJob(jobId: str) -> bool:
            assert (
                len(jobId) > 1
            ), f"Job ID {jobId} too short; is a string being used as a list?"
            if jobCache is not None:
                if jobId in jobCache:
                    return True
                else:
                    return self.job_exists(jobId)
            else:
                return self.job_exists(jobId)

        def deleteJob(jobId: str) -> None:
            if jobCache is not None:
                if jobId in jobCache:
                    del jobCache[jobId]
            self.delete_job(jobId)

        def updateJobDescription(jobDescription: JobDescription) -> None:
            if jobCache is not None:
                jobCache[str(jobDescription.jobStoreID)] = jobDescription
                self.update_job(jobDescription)

        def getJobDescriptions() -> (
            Union[ValuesView[JobDescription], Iterator[JobDescription]]
        ):
            if jobCache is not None:
                return jobCache.values()
            else:
                return self.jobs()

        def get_jobs_reachable_from_root() -> set[str]:
            """
            Traverse the job graph from the root job and return a flattened set of all active jobstore IDs.

            Note: Jobs returned by self.jobs(), but not this function, are orphaned,
            and can be removed as dead jobs.
            """
            # Iterate from the root JobDescription and collate all jobs
            # that are reachable from it.
            root_job_description = self.load_root_job()
            reachable_from_root: set[str] = set()

            for merged_in in root_job_description.get_chain():
                # Add the job itself and any other jobs that chained with it.
                # Keep merged-in jobs around themselves, but don't bother
                # exploring them, since we took their successors.
                reachable_from_root.add(merged_in.job_store_id)
            # add all of root's linked service jobs as well
            for service_job_store_id in root_job_description.services:
                if haveJob(service_job_store_id):
                    reachable_from_root.add(service_job_store_id)

            # Unprocessed means it might have successor jobs we need to add.
            unprocessed_job_descriptions = [root_job_description]

            while unprocessed_job_descriptions:
                new_job_descriptions_to_process = []  # Reset.
                for job_description in unprocessed_job_descriptions:
                    for merged_in in job_description.get_chain():
                        # Add the job and anything chained with it.
                        # Keep merged-in jobs around themselves, but don't bother
                        # exploring them, since we took their successors.
                        reachable_from_root.add(merged_in.job_store_id)
                    for successor_job_store_id in job_description.allSuccessors():
                        if (
                            successor_job_store_id not in reachable_from_root
                            and haveJob(successor_job_store_id)
                        ):
                            successor_job_description = getJobDescription(
                                successor_job_store_id
                            )

                            # Add all of the successor's linked service jobs as well.
                            for (
                                service_job_store_id
                            ) in successor_job_description.services:
                                if haveJob(service_job_store_id):
                                    reachable_from_root.add(service_job_store_id)

                            new_job_descriptions_to_process.append(
                                successor_job_description
                            )
                unprocessed_job_descriptions = new_job_descriptions_to_process

            logger.debug(f"{len(reachable_from_root)} jobs reachable from root.")
            return reachable_from_root

        reachable_from_root = get_jobs_reachable_from_root()

        # Cleanup jobs that are not reachable from the root, and therefore orphaned
        # TODO: Avoid reiterating reachable_from_root (which may be very large)
        unreachable = [
            x for x in getJobDescriptions() if x.jobStoreID not in reachable_from_root
        ]
        for jobDescription in unreachable:
            # clean up any associated files before deletion
            for fileID in jobDescription.filesToDelete:
                # Delete any files that should already be deleted
                logger.warning(
                    f"Deleting file '{fileID}'. It is marked for deletion but has not yet been removed."
                )
                self.delete_file(fileID)
            # Delete the job from us and the cache
            deleteJob(str(jobDescription.jobStoreID))

        jobDescriptionsReachableFromRoot = {
            id: getJobDescription(id) for id in reachable_from_root
        }

        # Clean up any checkpoint jobs -- delete any successors it
        # may have launched, and restore the job to a pristine state
        jobsDeletedByCheckpoints = set()
        for jobDescription in [
            desc
            for desc in jobDescriptionsReachableFromRoot.values()
            if isinstance(desc, CheckpointJobDescription)
        ]:
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
                    logger.critical(
                        "Removing file in job store: %s that was "
                        "marked for deletion but not previously removed" % fileID
                    )
                    self.delete_file(fileID)
                jobDescription.filesToDelete = []
                changed[0] = True

            # For a job whose body has already executed, remove jobs from the
            # stack that are already deleted. This cleans up the case that the
            # jobDescription had successors to run, but had not been updated to
            # reflect this.
            if not jobDescription.has_body():

                def stackSizeFn() -> int:
                    return len(list(jobDescription.allSuccessors()))

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
            def subFlagFile(jobStoreID: str, jobStoreFileID: str, flag: int) -> str:
                if self.file_exists(jobStoreFileID):
                    return jobStoreFileID

                # Make a new flag
                newFlag = self.get_empty_file_store_id(jobStoreID, cleanup=False)

                # Load the jobDescription for the service and initialise the link
                serviceJobDescription = getJobDescription(jobStoreID)

                # Make sure it really is a service
                assert isinstance(serviceJobDescription, ServiceJobDescription)

                if flag == 1:
                    logger.debug(
                        "Recreating a start service flag for job: %s, flag: %s",
                        jobStoreID,
                        newFlag,
                    )
                    serviceJobDescription.startJobStoreID = newFlag
                elif flag == 2:
                    logger.debug(
                        "Recreating a terminate service flag for job: %s, flag: %s",
                        jobStoreID,
                        newFlag,
                    )
                    serviceJobDescription.terminateJobStoreID = newFlag
                else:
                    logger.debug(
                        "Recreating a error service flag for job: %s, flag: %s",
                        jobStoreID,
                        newFlag,
                    )
                    assert flag == 3
                    serviceJobDescription.errorJobStoreID = newFlag

                # Update the service job on disk
                updateJobDescription(serviceJobDescription)

                changed[0] = True

                return newFlag

            def servicesSizeFn() -> int:
                return len(jobDescription.services)

            startServicesSize = servicesSizeFn()

            def replaceFlagsIfNeeded(serviceJobDescription: JobDescription) -> None:
                # Make sure it really is a service
                if not isinstance(serviceJobDescription, ServiceJobDescription):
                    raise Exception(
                        "Must be a ServiceJobDescription, not "
                        f'"{type(serviceJobDescription)}": '
                        f'"{serviceJobDescription}".'
                    )
                if not serviceJobDescription.startJobStoreID:
                    raise Exception("Must be a registered ServiceJobDescription.")
                else:
                    serviceJobDescription.startJobStoreID = subFlagFile(
                        str(serviceJobDescription.jobStoreID),
                        serviceJobDescription.startJobStoreID,
                        1,
                    )
                if not serviceJobDescription.terminateJobStoreID:
                    raise Exception("Must be a registered ServiceJobDescription.")
                else:
                    serviceJobDescription.terminateJobStoreID = subFlagFile(
                        str(serviceJobDescription.jobStoreID),
                        serviceJobDescription.terminateJobStoreID,
                        2,
                    )
                if not serviceJobDescription.errorJobStoreID:
                    raise Exception("Must be a registered ServiceJobDescription.")
                else:
                    serviceJobDescription.errorJobStoreID = subFlagFile(
                        str(serviceJobDescription.jobStoreID),
                        serviceJobDescription.errorJobStoreID,
                        3,
                    )

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
            if jobDescription.logJobStoreFileID is not None:
                self.delete_file(jobDescription.logJobStoreFileID)
                jobDescription.logJobStoreFileID = None
                changed[0] = True

            if changed[0]:  # Update, but only if a change has occurred
                logger.critical("Repairing job: %s" % jobDescription.jobStoreID)
                updateJobDescription(jobDescription)

        # Remove any crufty stats/logging files from the previous run
        logger.debug("Discarding old statistics and logs...")

        # We have to manually discard the stream to avoid getting
        # stuck on a blocking write from the job store.
        def discardStream(stream: Union[IO[bytes], IO[str]]) -> None:
            """Read the stream 4K at a time until EOF, discarding all input."""
            while len(stream.read(4096)) != 0:
                pass

        self.read_logs(discardStream)

        logger.debug("Job store is clean")
        # TODO: reloading of the rootJob may be redundant here
        return self.load_root_job()

    ##########################################
    # The following methods deal with creating/loading/updating/writing/checking for the
    # existence of jobs
    ##########################################

    @deprecated(new_function_name="assign_job_id")
    def assignID(self, jobDescription: JobDescription) -> None:
        return self.assign_job_id(jobDescription)

    @abstractmethod
    def assign_job_id(self, job_description: JobDescription) -> None:
        """
        Get a new jobStoreID to be used by the described job, and assigns it to the JobDescription.

        Files associated with the assigned ID will be accepted even if the JobDescription has never been created or updated.

        :param toil.job.JobDescription job_description: The JobDescription to give an ID to
        """
        raise NotImplementedError()

    @contextmanager
    def batch(self) -> Iterator[None]:
        """
        If supported by the batch system, calls to create() with this context
        manager active will be performed in a batch after the context manager
        is released.
        """
        yield

    @deprecated(new_function_name="create_job")
    def create(self, jobDescription: JobDescription) -> JobDescription:
        return self.create_job(jobDescription)

    @abstractmethod
    def create_job(self, job_description: JobDescription) -> JobDescription:
        """
        Writes the given JobDescription to the job store. The job must have an ID assigned already.

        Must call jobDescription.pre_update_hook()

        :return: The JobDescription passed.
        :rtype: toil.job.JobDescription
        """
        raise NotImplementedError()

    @deprecated(new_function_name="job_exists")
    def exists(self, jobStoreID: str) -> bool:
        return self.job_exists(jobStoreID)

    @abstractmethod
    def job_exists(self, job_id: str) -> bool:
        """
        Indicates whether a description of the job with the specified jobStoreID exists in the job store

        :rtype: bool
        """
        raise NotImplementedError()

    # One year should be sufficient to finish any pipeline ;-)
    publicUrlExpiration = timedelta(days=365)

    @deprecated(new_function_name="get_public_url")
    def getPublicUrl(self, fileName: str) -> str:
        return self.get_public_url(fileName)

    @abstractmethod
    def get_public_url(self, file_name: str) -> str:
        """
        Returns a publicly accessible URL to the given file in the job store. The returned URL may
        expire as early as 1h after its been returned. Throw an exception if the file does not
        exist.

        :param str file_name: the jobStoreFileID of the file to generate a URL for

        :raise NoSuchFileException: if the specified file does not exist in this job store

        :rtype: str
        """
        raise NotImplementedError()

    @deprecated(new_function_name="get_shared_public_url")
    def getSharedPublicUrl(self, sharedFileName: str) -> str:
        return self.get_shared_public_url(sharedFileName)

    @abstractmethod
    def get_shared_public_url(self, shared_file_name: str) -> str:
        """
        Differs from :meth:`getPublicUrl` in that this method is for generating URLs for shared
        files written by :meth:`writeSharedFileStream`.

        Returns a publicly accessible URL to the given file in the job store. The returned URL
        starts with 'http:',  'https:' or 'file:'. The returned URL may expire as early as 1h
        after its been returned. Throw an exception if the file does not exist.

        :param str shared_file_name: The name of the shared file to generate a publically accessible url for.

        :raise NoSuchFileException: raised if the specified file does not exist in the store

        :rtype: str
        """
        raise NotImplementedError()

    @deprecated(new_function_name="load_job")
    def load(self, jobStoreID: str) -> JobDescription:
        return self.load_job(jobStoreID)

    @abstractmethod
    def load_job(self, job_id: str) -> JobDescription:
        """
        Loads the description of the job referenced by the given ID, assigns it
        the job store's config, and returns it.

        May declare the job to have failed (see
        :meth:`toil.job.JobDescription.setupJobAfterFailure`) if there is
        evidence of a failed update attempt.

        :param job_id: the ID of the job to load

        :raise NoSuchJobException: if there is no job with the given ID
        """
        raise NotImplementedError()

    @deprecated(new_function_name="update_job")
    def update(self, jobDescription: JobDescription) -> None:
        return self.update_job(jobDescription)

    @abstractmethod
    def update_job(self, job_description: JobDescription) -> None:
        """
        Persists changes to the state of the given JobDescription in this store atomically.

        Must call jobDescription.pre_update_hook()

        :param toil.job.JobDescription job: the job to write to this job store
        """
        raise NotImplementedError()

    @deprecated(new_function_name="delete_job")
    def delete(self, jobStoreID: str) -> None:
        return self.delete_job(jobStoreID)

    @abstractmethod
    def delete_job(self, job_id: str) -> None:
        """
        Removes the JobDescription from the store atomically. You may not then
        subsequently call load(), write(), update(), etc. with the same
        jobStoreID or any JobDescription bearing it.

        This operation is idempotent, i.e. deleting a job twice or deleting a non-existent job
        will succeed silently.

        :param str job_id: the ID of the job to delete from this job store
        """
        raise NotImplementedError()

    def jobs(self) -> Iterator[JobDescription]:
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

    @deprecated(new_function_name="write_file")
    def writeFile(
        self,
        localFilePath: str,
        jobStoreID: Optional[str] = None,
        cleanup: bool = False,
    ) -> str:
        return self.write_file(localFilePath, jobStoreID, cleanup)

    @abstractmethod
    def write_file(
        self, local_path: str, job_id: Optional[str] = None, cleanup: bool = False
    ) -> str:
        """
        Takes a file (as a path) and places it in this job store. Returns an ID that can be used
        to retrieve the file at a later time.  The file is written in a atomic manner.  It will
        not appear in the jobStore until the write has successfully completed.

        :param str local_path: the path to the local file that will be uploaded to the job store.
               The last path component (basename of the file) will remain
               associated with the file in the file store, if supported, so
               that the file can be searched for by name or name glob.

        :param str job_id: the id of a job, or None. If specified, the may be associated
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

    @deprecated(new_function_name="write_file_stream")
    def writeFileStream(
        self,
        jobStoreID: Optional[str] = None,
        cleanup: bool = False,
        basename: Optional[str] = None,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> ContextManager[tuple[IO[bytes], str]]:
        return self.write_file_stream(jobStoreID, cleanup, basename, encoding, errors)

    @abstractmethod
    @contextmanager
    def write_file_stream(
        self,
        job_id: Optional[str] = None,
        cleanup: bool = False,
        basename: Optional[str] = None,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Iterator[tuple[IO[bytes], str]]:
        """
        Similar to writeFile, but returns a context manager yielding a tuple of
        1) a file handle which can be written to and 2) the ID of the resulting
        file in the job store. The yielded file handle does not need to and
        should not be closed explicitly.  The file is written in a atomic manner.
        It will not appear in the jobStore until the write has successfully
        completed.

        :param str job_id: the id of a job, or None. If specified, the may be associated
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

        :return: a context manager yielding a file handle which can be written to and an ID that references
                 the newly created file and can be used to read the file in the future.
        :rtype: Iterator[Tuple[IO[bytes], str]]
        """
        raise NotImplementedError()

    @deprecated(new_function_name="get_empty_file_store_id")
    def getEmptyFileStoreID(
        self,
        jobStoreID: Optional[str] = None,
        cleanup: bool = False,
        basename: Optional[str] = None,
    ) -> str:
        return self.get_empty_file_store_id(jobStoreID, cleanup, basename)

    @abstractmethod
    def get_empty_file_store_id(
        self,
        job_id: Optional[str] = None,
        cleanup: bool = False,
        basename: Optional[str] = None,
    ) -> str:
        """
        Creates an empty file in the job store and returns its ID.
        Call to fileExists(getEmptyFileStoreID(jobStoreID)) will return True.

        :param str job_id: the id of a job, or None. If specified, the may be associated
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

    @deprecated(new_function_name="read_file")
    def readFile(
        self, jobStoreFileID: str, localFilePath: str, symlink: bool = False
    ) -> None:
        return self.read_file(jobStoreFileID, localFilePath, symlink)

    @abstractmethod
    def read_file(self, file_id: str, local_path: str, symlink: bool = False) -> None:
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

        :param str file_id: ID of the file to be copied

        :param str local_path: the local path indicating where to place the contents of the
               given file in the job store

        :param bool symlink: whether the reader can tolerate a symlink. If set to true, the job
               store may create a symlink instead of a full copy of the file or a hard link.
        """
        raise NotImplementedError()

    @deprecated(new_function_name="read_file_stream")
    def readFileStream(
        self,
        jobStoreFileID: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Union[ContextManager[IO[bytes]], ContextManager[IO[str]]]:
        return self.read_file_stream(jobStoreFileID, encoding, errors)

    @overload
    def read_file_stream(
        self,
        file_id: Union[FileID, str],
        encoding: Literal[None] = None,
        errors: Optional[str] = None,
    ) -> ContextManager[IO[bytes]]: ...

    @overload
    def read_file_stream(
        self, file_id: Union[FileID, str], encoding: str, errors: Optional[str] = None
    ) -> ContextManager[IO[str]]: ...

    @abstractmethod
    def read_file_stream(
        self,
        file_id: Union[FileID, str],
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Union[ContextManager[IO[bytes]], ContextManager[IO[str]]]:
        """
        Similar to readFile, but returns a context manager yielding a file handle which can be
        read from. The yielded file handle does not need to and should not be closed explicitly.

        :param str file_id: ID of the file to get a readable file handle for

        :param str encoding: the name of the encoding used to decode the file. Encodings are the same as
                for decode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.

        :return: a context manager yielding a file handle which can be read from
        :rtype: Iterator[Union[IO[bytes], IO[str]]]
        """
        raise NotImplementedError()

    @deprecated(new_function_name="delete_file")
    def deleteFile(self, jobStoreFileID: str) -> None:
        return self.delete_file(jobStoreFileID)

    @abstractmethod
    def delete_file(self, file_id: str) -> None:
        """
        Deletes the file with the given ID from this job store. This operation is idempotent, i.e.
        deleting a file twice or deleting a non-existent file will succeed silently.

        :param str file_id: ID of the file to delete
        """
        raise NotImplementedError()

    @deprecated(new_function_name="file_exists")
    def fileExists(self, jobStoreFileID: str) -> bool:
        """Determine whether a file exists in this job store."""
        return self.file_exists(jobStoreFileID)

    @abstractmethod
    def file_exists(self, file_id: str) -> bool:
        """
        Determine whether a file exists in this job store.

        :param file_id: an ID referencing the file to be checked
        """
        raise NotImplementedError()

    @deprecated(new_function_name="get_file_size")
    def getFileSize(self, jobStoreFileID: str) -> int:
        """Get the size of the given file in bytes."""
        return self.get_file_size(jobStoreFileID)

    @abstractmethod
    def get_file_size(self, file_id: str) -> int:
        """
        Get the size of the given file in bytes, or 0 if it does not exist when queried.

        Note that job stores which encrypt files might return overestimates of
        file sizes, since the encrypted file may have been padded to the
        nearest block, augmented with an initialization vector, etc.

        :param str file_id: an ID referencing the file to be checked

        :rtype: int
        """
        raise NotImplementedError()

    @deprecated(new_function_name="update_file")
    def updateFile(self, jobStoreFileID: str, localFilePath: str) -> None:
        """Replaces the existing version of a file in the job store."""
        return self.update_file(jobStoreFileID, localFilePath)

    @abstractmethod
    def update_file(self, file_id: str, local_path: str) -> None:
        """
        Replaces the existing version of a file in the job store.

        Throws an exception if the file does not exist.

        :param file_id: the ID of the file in the job store to be updated
        :param local_path: the local path to a file that will overwrite the current
                           version in the job store
        :raise ConcurrentFileModificationException: if the file was modified
               concurrently during an invocation of this method
        :raise NoSuchFileException: if the specified file does not exist
        """
        raise NotImplementedError()

    @deprecated(new_function_name="update_file_stream")
    def updateFileStream(
        self,
        jobStoreFileID: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> ContextManager[IO[Any]]:
        return self.update_file_stream(jobStoreFileID, encoding, errors)

    @abstractmethod
    @contextmanager
    def update_file_stream(
        self, file_id: str, encoding: Optional[str] = None, errors: Optional[str] = None
    ) -> Iterator[IO[Any]]:
        """
        Replaces the existing version of a file in the job store. Similar to writeFile, but
        returns a context manager yielding a file handle which can be written to. The
        yielded file handle does not need to and should not be closed explicitly.

        :param str file_id: the ID of the file in the job store to be updated

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

    sharedFileNameRegex = re.compile(r"^[a-zA-Z0-9._-]+$")

    @deprecated(new_function_name="write_shared_file_stream")
    def writeSharedFileStream(
        self,
        sharedFileName: str,
        isProtected: Optional[bool] = None,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> ContextManager[IO[bytes]]:
        return self.write_shared_file_stream(
            sharedFileName, isProtected, encoding, errors
        )

    @abstractmethod
    @contextmanager
    def write_shared_file_stream(
        self,
        shared_file_name: str,
        encrypted: Optional[bool] = None,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Iterator[IO[bytes]]:
        """
        Returns a context manager yielding a writable file handle to the global file referenced
        by the given name.  File will be created in an atomic manner.

        :param str shared_file_name: A file name matching AbstractJobStore.fileNameRegex, unique within
               this job store

        :param bool encrypted: True if the file must be encrypted, None if it may be encrypted or
               False if it must be stored in the clear.

        :param str encoding: the name of the encoding used to encode the file. Encodings are the same
                as for encode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method

        :return: a context manager yielding a writable file handle
        :rtype: Iterator[IO[bytes]]
        """
        raise NotImplementedError()

    @deprecated(new_function_name="read_shared_file_stream")
    def readSharedFileStream(
        self,
        sharedFileName: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Union[ContextManager[IO[str]], ContextManager[IO[bytes]]]:
        return self.read_shared_file_stream(sharedFileName, encoding, errors)

    @overload
    @abstractmethod
    @contextmanager
    def read_shared_file_stream(
        self,
        shared_file_name: str,
        encoding: str,
        errors: Optional[str] = None,
    ) -> Iterator[IO[str]]:
        """If encoding is specified, then a text file handle is provided."""

    @overload
    @abstractmethod
    @contextmanager
    def read_shared_file_stream(
        self,
        shared_file_name: str,
        encoding: Literal[None] = None,
        errors: Optional[str] = None,
    ) -> Iterator[IO[bytes]]:
        """If no encoding is provided, then a bytest file handle is provided."""

    @abstractmethod
    @contextmanager
    def read_shared_file_stream(
        self,
        shared_file_name: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Union[Iterator[IO[str]], Iterator[IO[bytes]]]:
        """
        Returns a context manager yielding a readable file handle to the global file referenced
        by the given name.

        :param str shared_file_name: A file name matching AbstractJobStore.fileNameRegex, unique within
               this job store

        :param str encoding: the name of the encoding used to decode the file. Encodings are the same
                as for decode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.

        :return: a context manager yielding a readable file handle
        """
        raise NotImplementedError()

    @deprecated(new_function_name="write_logs")
    def writeStatsAndLogging(self, statsAndLoggingString: str) -> None:
        return self.write_logs(statsAndLoggingString)

    @abstractmethod
    def write_logs(self, msg: str) -> None:
        """
        Stores a message as a log in the jobstore.

        :param str msg: the string to be written

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method
        """
        raise NotImplementedError()

    @deprecated(new_function_name="read_logs")
    def readStatsAndLogging(
        self, callback: Callable[..., Any], readAll: bool = False
    ) -> int:
        return self.read_logs(callback, readAll)

    @abstractmethod
    def read_logs(self, callback: Callable[..., Any], read_all: bool = False) -> int:
        """
        Reads logs accumulated by the write_logs() method. For each log this method calls the
        given callback function with the message as an argument (rather than returning logs directly,
        this method must be supplied with a callback which will process log messages).

        Only unread logs will be read unless the read_all parameter is set.

        :param Callable callback: a function to be applied to each of the stats file handles found

        :param bool read_all: a boolean indicating whether to read the already processed stats files
               in addition to the unread stats files

        :raise ConcurrentFileModificationException: if the file was modified concurrently during
               an invocation of this method

        :return: the number of stats files processed
        :rtype: int
        """
        raise NotImplementedError()

    # A few shared files useful to Toil, but probably less useful to the users.

    def write_leader_pid(self) -> None:
        """
        Write the pid of this process to a file in the job store.

        Overwriting the current contents of pid.log is a feature, not a bug of
        this method. Other methods will rely on always having the most current
        pid available. So far there is no reason to store any old pids.
        """
        with self.write_shared_file_stream("pid.log") as f:
            f.write(str(os.getpid()).encode("utf-8"))

    def read_leader_pid(self) -> int:
        """
        Read the pid of the leader process to a file in the job store.

        :raise NoSuchFileException: If the PID file doesn't exist.
        """
        with self.read_shared_file_stream("pid.log") as f:
            return int(f.read().strip())

    def write_leader_node_id(self) -> None:
        """
        Write the leader node id to the job store. This should only be called
        by the leader.
        """
        with self.write_shared_file_stream("leader_node_id.log") as f:
            f.write(getNodeID().encode("utf-8"))

    def read_leader_node_id(self) -> str:
        """
        Read the leader node id stored in the job store.

        :raise NoSuchFileException: If the node ID file doesn't exist.
        """
        with self.read_shared_file_stream("leader_node_id.log") as f:
            return f.read().decode("utf-8").strip()

    def write_kill_flag(self, kill: bool = False) -> None:
        """
        Write a file inside the job store that serves as a kill flag.

        The initialized file contains the characters "NO". This should only be
        changed when the user runs the "toil kill" command.

        Changing this file to a "YES" triggers a kill of the leader process. The
        workers are expected to be cleaned up by the leader.
        """
        with self.write_shared_file_stream("_toil_kill_flag") as f:
            f.write(("YES" if kill else "NO").encode("utf-8"))

    def read_kill_flag(self) -> bool:
        """
        Read the kill flag from the job store, and return True if the leader
        has been killed. False otherwise.
        """
        try:
            with self.read_shared_file_stream("_toil_kill_flag") as f:
                killed = f.read().decode() == "YES"
        except NoSuchFileException:
            # The kill flag file doesn't exist yet.
            killed = False
        return killed

    def default_caching(self) -> bool:
        """
        Jobstore's preference as to whether it likes caching or doesn't care about it.
        Some jobstores benefit from caching, however on some local configurations it can be flaky.

        see https://github.com/DataBiosphere/toil/issues/4218
        """

        return True

    # Helper methods for subclasses

    def _defaultTryCount(self) -> int:
        if not self.config:
            raise Exception("Must initialize first.")
        return int(self.config.retryCount + 1)

    @classmethod
    def _validateSharedFileName(cls, sharedFileName: str) -> bool:
        return bool(cls.sharedFileNameRegex.match(sharedFileName))

    @classmethod
    def _requireValidSharedFileName(cls, sharedFileName: str) -> None:
        if not cls._validateSharedFileName(sharedFileName):
            raise ValueError("Not a valid shared file name: '%s'." % sharedFileName)


class JobStoreSupport(AbstractJobStore, metaclass=ABCMeta):
    """
    A mostly fake JobStore to access URLs not really associated with real job
    stores.
    """

    @classmethod
    def _setup_ftp(cls) -> FtpFsAccess:
        # FTP connections are not reused. Ideally, a thread should watch any reused FTP connections
        # and close them when necessary
        return FtpFsAccess()

    @classmethod
    def _supports_url(cls, url: ParseResult, export: bool = False) -> bool:
        return url.scheme.lower() in ("http", "https", "ftp") and not export

    @classmethod
    def _url_exists(cls, url: ParseResult) -> bool:
        # Deal with FTP first to support user/password auth
        if url.scheme.lower() == "ftp":
            ftp = cls._setup_ftp()
            return ftp.exists(url.geturl())

        try:
            with closing(urlopen(Request(url.geturl(), method="HEAD"))):
                return True
        except HTTPError as e:
            if e.code in (404, 410):
                return False
            else:
                raise
        # Any other errors we should pass through because something really went
        # wrong (e.g. server is broken today but file may usually exist)

    @classmethod
    @retry(
        errors=[
            BadStatusLine,
            ErrorCondition(error=HTTPError, error_codes=[408, 500, 503]),
        ]
    )
    def _get_size(cls, url: ParseResult) -> Optional[int]:
        if url.scheme.lower() == "ftp":
            ftp = cls._setup_ftp()
            return ftp.size(url.geturl())

        # just read the header for content length
        resp = urlopen(Request(url.geturl(), method="HEAD"))
        size = resp.info().get("content-length")
        return int(size) if size is not None else None

    @classmethod
    def _read_from_url(
        cls, url: ParseResult, writable: Union[IO[bytes], IO[str]]
    ) -> tuple[int, bool]:
        # We can't actually retry after we start writing.
        # TODO: Implement retry with byte range requests
        with cls._open_url(url) as readable:
            # Make something to count the bytes we get
            # We need to put the actual count in a container so our
            # nested function can modify it without creating its own
            # local with the same name.
            size = [0]

            def count(l: int) -> None:
                size[0] += l

            counter = WriteWatchingStream(writable)
            counter.onWrite(count)

            # Do the download
            shutil.copyfileobj(readable, counter)
            return size[0], False

    @classmethod
    @retry(
        errors=[
            BadStatusLine,
            ErrorCondition(error=HTTPError, error_codes=[408, 429, 500, 502, 503]),
        ]
    )
    def _open_url(cls, url: ParseResult) -> IO[bytes]:
        # Deal with FTP first so we support user/password auth
        if url.scheme.lower() == "ftp":
            ftp = cls._setup_ftp()
            # we open in read mode as write mode is not supported
            return ftp.open(url.geturl(), mode="r")

        try:
            return cast(IO[bytes], closing(urlopen(url.geturl())))
        except HTTPError as e:
            if e.code in (404, 410):
                # Translate into a FileNotFoundError for detecting
                # known nonexistent files
                raise FileNotFoundError(str(url)) from e
            else:
                # Other codes indicate a real problem with the server; we don't
                # want to e.g. run a workflow without an optional input that
                # the user specified a path to just because the server was
                # busy.

                # Sometimes we expect to see this when polling existence for
                # inputs at guessed paths, so don't complain *too* loudly here.
                logger.debug("Unusual status %d for URL %s", e.code, str(url))
                raise

    @classmethod
    def _get_is_directory(cls, url: ParseResult) -> bool:
        # TODO: Implement HTTP index parsing and FTP directory listing
        return False

    @classmethod
    def _list_url(cls, url: ParseResult) -> list[str]:
        # TODO: Implement HTTP index parsing and FTP directory listing
        raise NotImplementedError("HTTP and FTP URLs cannot yet be listed")
