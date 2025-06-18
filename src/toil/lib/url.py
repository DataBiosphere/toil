# Copyright (C) 2015-2025 Regents of the University of California
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
from abc import ABC, ABCMeta, abstractmethod
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
    Type,
)
from urllib.parse import ParseResult, urlparse

from toil.lib.exceptions import UnimplementedURLException
from toil.lib.memoize import memoize
from toil.lib.plugins import register_plugin, get_plugin

try:
    from botocore.exceptions import ProxyConnectionError
except ImportError:

    class ProxyConnectionError(BaseException):  # type: ignore
        """Dummy class."""

logger = logging.getLogger(__name__)

class URLAccess:
    """
    Widget for accessing external storage (URLs).
    """

    @classmethod
    def url_exists(cls, src_uri: str) -> bool:
        """
        Return True if the file at the given URI exists, and False otherwise.

        May raise an error if file existence cannot be determined.

        :param src_uri: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.
        """
        parseResult = urlparse(src_uri)
        otherCls = cls._find_url_implementation(parseResult)
        return otherCls._url_exists(parseResult)

    @classmethod
    def get_size(cls, src_uri: str) -> Optional[int]:
        """
        Get the size in bytes of the file at the given URL, or None if it cannot be obtained.

        :param src_uri: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.
        """
        parseResult = urlparse(src_uri)
        otherCls = cls._find_url_implementation(parseResult)
        return otherCls._get_size(parseResult)

    @classmethod
    def get_is_directory(cls, src_uri: str) -> bool:
        """
        Return True if the thing at the given URL is a directory, and False if
        it is a file. The URL may or may not end in '/'.
        """
        parseResult = urlparse(src_uri)
        otherCls = cls._find_url_implementation(parseResult)
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
        otherCls = cls._find_url_implementation(parseResult)
        return otherCls._list_url(parseResult)

    @classmethod
    def read_from_url(cls, src_uri: str, writable: IO[bytes]) -> tuple[int, bool]:
        """
        Read the given URL and write its content into the given writable stream.

        Raises FileNotFoundError if the URL doesn't exist.

        :return: The size of the file in bytes and whether the executable permission bit is set
        """
        parseResult = urlparse(src_uri)
        otherCls = cls._find_url_implementation(parseResult)
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
        otherCls = cls._find_url_implementation(parseResult)
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
        specified location. Raises FileNotFoundError if the URL doesn't exist.

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
        Returns True if the url access implementation supports the URL's scheme.

        :param ParseResult url: a parsed URL that may be supported

        :param bool export: Determines if the url is supported for exported

        :return bool: returns true if the cls supports the URL
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    def _find_url_implementation(
        cls, url: ParseResult, export: bool = False
    ) -> type["URLAccess"]:
        """
        Returns the URLAccess subclass that supports the given URL.

        :param ParseResult url: The given URL

        :param bool export: Determines if the url is supported for exporting

        """
        try:
            implementation_factory = get_plugin("url_access", url.scheme.lower())
        except KeyError:
            raise UnimplementedURLException(url, "export" if export else "import")
        
        try:
            implementation = cast(Type[URLAccess], implementation_factory())
        except (ImportError, ProxyConnectionError):
            logger.debug(
                "Unable to import implementation for scheme '%s', as is expected if the corresponding extra was "
                "omitted at installation time.",
                url.scheme.lower(),
            )
            raise UnimplementedURLException(url, "export" if export else "import")

        if implementation._supports_url(url, export):
            return implementation
        raise UnimplementedURLException(url, "export" if export else "import")

#####
# Built-in url access
#####

def file_job_store_factory() -> type[URLAccess]:
    from toil.jobStores.fileJobStore import FileJobStore

    return FileJobStore


def google_job_store_factory() -> type[URLAccess]:
    from toil.jobStores.googleJobStore import GoogleJobStore

    return GoogleJobStore


def aws_job_store_factory() -> type[URLAccess]:
    from toil.jobStores.aws.jobStore import AWSJobStore

    return AWSJobStore


def job_store_support_factory() -> type[URLAccess]:
    from toil.jobStores.abstractJobStore import JobStoreSupport

    return JobStoreSupport

#make sure my py still works and the tests work
# can then get rid of _url_access_classes method

#####
# Registers all built-in urls
#####
register_plugin("url_access", "file", file_job_store_factory)
register_plugin("url_access", "gs", google_job_store_factory)
register_plugin("url_access", "s3", aws_job_store_factory)
register_plugin("url_access", "http", job_store_support_factory)
register_plugin("url_access", "https", job_store_support_factory)
register_plugin("url_access", "ftp", job_store_support_factory)
