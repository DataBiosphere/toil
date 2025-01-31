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
)
from urllib.parse import ParseResult, urlparse

from toil.lib.exceptions import UnimplementedURLException

if TYPE_CHECKING:
    from toil.common import Config

logger = logging.getLogger(__name__)

class URLAccess:
    """
    Widget for accessing external storage (URLs).

    Can be instantiated.
    """

    def __init__(self, config: "Config") -> None:
        """
        Make a new widget for accessing URLs.
        """
        self._config = config
        super().__init__()

    def url_exists(self, src_uri: str) -> bool:
        """
        Return True if the file at the given URI exists, and False otherwise.

        May raise an error if file existence cannot be determined.

        :param src_uri: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.

        """
        parseResult = urlparse(src_uri)
        otherCls = AbstractURLProtocolImplementation.find_url_implementation(parseResult)
        return otherCls._url_exists(parseResult, self._config)

    def get_size(self, src_uri: str) -> Optional[int]:
        """
        Get the size in bytes of the file at the given URL, or None if it cannot be obtained.

        :param src_uri: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.
        """
        parseResult = urlparse(src_uri)
        otherCls = AbstractURLProtocolImplementation.find_url_implementation(parseResult)
        return otherCls._get_size(parseResult, self._config)

    def get_is_directory(self, src_uri: str) -> bool:
        """
        Return True if the thing at the given URL is a directory, and False if
        it is a file. The URL may or may not end in '/'.
        """
        parseResult = urlparse(src_uri)
        otherCls = AbstractURLProtocolImplementation.find_url_implementation(parseResult)
        return otherCls._get_is_directory(parseResult, self._config)

    def list_url(self, src_uri: str) -> list[str]:
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
        otherCls = AbstractURLProtocolImplementation.find_url_implementation(parseResult)
        return otherCls._list_url(parseResult, self._config)

    def read_from_url(self, src_uri: str, writable: IO[bytes]) -> tuple[int, bool]:
        """
        Read the given URL and write its content into the given writable stream.

        Raises FileNotFoundError if the URL doesn't exist.

        :param config: The current Toil configuration object.

        :return: The size of the file in bytes and whether the executable permission bit is set
        """
        parseResult = urlparse(src_uri)
        otherCls = AbstractURLProtocolImplementation.find_url_implementation(parseResult)
        return otherCls._read_from_url(parseResult, writable, self._config)

    def open_url(self, src_uri: str) -> IO[bytes]:
        """
        Read from the given URI.

        Raises FileNotFoundError if the URL doesn't exist.

        Has a readable stream interface, unlike :meth:`read_from_url` which
        takes a writable stream.
        """
        parseResult = urlparse(src_uri)
        otherCls = AbstractURLProtocolImplementation.find_url_implementation(parseResult)
        return otherCls._open_url(parseResult, self._config)


class AbstractURLProtocolImplementation(ABC):
    """
    Base class for URL accessor implementations. Also manages finding the
    implementation for a URL.

    Many job stores implement this to allow access to URLs on the same kind of
    backing storage as that kind of job store.
    """

    @classmethod
    def find_url_implementation(
        cls, url: ParseResult, export: bool = False
    ) -> type["AbstractURLProtocolImplementation"]:
        """
        Returns the subclass that supports the given URL.

        :param ParseResult url: The given URL

        :param bool export: Determines if the url is supported for exporting
        """
        # TODO: Make pluggable.

        from toil.jobStores.abstractJobStore import AbstractJobStore, StandardURLProtocolImplementation

        if StandardURLProtocolImplementation._supports_url(url, export):
            # This is a standard URL scheme
            return StandardURLProtocolImplementation

        for implementation in AbstractJobStore._get_job_store_classes():
            if issubclass(implementation, AbstractURLProtocolImplementation) and implementation._supports_url(url, export):
                # This scheme belongs to one of the available job store implementations.
                return implementation
        raise UnimplementedURLException(url, "export" if export else "import")

    # TODO: De-private-ify these methods while allowing a class that provides
    # this to also be an instance of URLAccess 

    @classmethod
    @abstractmethod
    def _url_exists(cls, url: ParseResult, config: "Config") -> bool:
        """
        Return True if the item at the given URL exists, and Flase otherwise.

        May raise an error if file existence cannot be determined.

        :param config: The current Toil configuration object.
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _get_size(cls, url: ParseResult, config: "Config") -> Optional[int]:
        """
        Get the size of the object at the given URL, or None if it cannot be obtained.

        :param config: The current Toil configuration object.
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _get_is_directory(cls, url: ParseResult, config: "Config") -> bool:
        """
        Return True if the thing at the given URL is a directory, and False if
        it is a file or it is known not to exist. The URL may or may not end in
        '/'.

        :param url: URL that points to a file or object, or directory or prefix,
               in the storage mechanism of a supported URL scheme e.g. a blob
               in an AWS s3 bucket.

        :param config: The current Toil configuration object.
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _read_from_url(cls, url: ParseResult, writable: IO[bytes], config: "Config") -> tuple[int, bool]:
        """
        Reads the contents of the object at the specified location and writes it to the given
        writable stream.

        Refer to :func:`~AbstractJobStore.importFile` documentation for currently supported URL schemes.

        Raises FileNotFoundError if the thing at the URL is not found.

        :param ParseResult url: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.

        :param IO[bytes] writable: a writable stream

        :param config: The current Toil configuration object.

        :return: The size of the file in bytes and whether the executable permission bit is set
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _list_url(cls, url: ParseResult, config: "Config") -> list[str]:
        """
        List the contents of the given URL, which may or may not end in '/'

        Returns a list of URL components. Those that end in '/' are meant to be
        directories, while those that do not are meant to be files.

        Refer to :func:`~AbstractJobStore.importFile` documentation for currently supported URL schemes.

        :param ParseResult url: URL that points to a directory or prefix in the
        storage mechanism of a supported URL scheme e.g. a prefix in an AWS s3
        bucket.

        :param config: The current Toil configuration object.

        :return: The children of the given URL, already URL-encoded if
        appropriate. (If the URL is a bare path, no encoding is done.)
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _open_url(cls, url: ParseResult, config: "Config") -> IO[bytes]:
        """
        Get a stream of the object at the specified location.

        Refer to :func:`~AbstractJobStore.importFile` documentation for currently supported URL schemes.

        :param config: The current Toil configuration object.

        :raises: FileNotFoundError if the thing at the URL is not found.
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _write_to_url(
        cls,
        readable: Union[IO[bytes], IO[str]],
        url: ParseResult,
        executable: bool,
        config: "Config"
    ) -> None:
        """
        Reads the contents of the given readable stream and writes it to the object at the
        specified location. Raises FileNotFoundError if the URL doesn't exist..

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param Union[IO[bytes], IO[str]] readable: a readable stream

        :param ParseResult url: URL that points to a file or object in the storage
               mechanism of a supported URL scheme e.g. a blob in an AWS s3 bucket.

        :param bool executable: determines if the file has executable permissions

        :param config: The current Toil configuration object.
        """
        raise NotImplementedError(f"No implementation for {url}")

    @classmethod
    @abstractmethod
    def _supports_url(cls, url: ParseResult, export: bool = False) -> bool:
        """
        Returns True if the job store supports the URL's scheme.

        Refer to AbstractJobStore.importFile documentation for currently supported URL schemes.

        :param ParseResult url: a parsed URL that may be supported

        :param bool export: Determines if the url is supported for export

        :param config: The current Toil configuration object.

        :return bool: returns true if the cls supports the URL
        """
        raise NotImplementedError(f"No implementation for {url}")
