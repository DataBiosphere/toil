import errno
import logging
import os
import hashlib
import threading

from abc import ABC, abstractmethod
from typing import Optional, TextIO, BinaryIO, IO, Any

from toil.lib.checksum import ChecksumError
from toil.lib.threading import ExceptionalThread

log = logging.getLogger(__name__)


class WritablePipe(ABC):
    """
    An object-oriented wrapper for os.pipe. Clients should subclass it, implement
    :meth:`.readFrom` to consume the readable end of the pipe, then instantiate the class as a
    context manager to get the writable end. See the example below.

    >>> import sys, shutil, codecs
    >>> class MyPipe(WritablePipe):
    ...     def readFrom(self, readable):
    ...         shutil.copyfileobj(codecs.getreader('utf-8')(readable), sys.stdout)
    >>> with MyPipe() as writable:
    ...     _ = writable.write('Hello, world!\\n'.encode('utf-8'))
    Hello, world!

    Each instance of this class creates a thread and invokes the readFrom method in that thread.
    The thread will be join()ed upon normal exit from the context manager, i.e. the body of the
    `with` statement. If an exception occurs, the thread will not be joined but a well-behaved
    :meth:`.readFrom` implementation will terminate shortly thereafter due to the pipe having
    been closed.

    Now, exceptions in the reader thread will be reraised in the main thread:

    >>> class MyPipe(WritablePipe):
    ...     def readFrom(self, readable):
    ...         raise RuntimeError('Hello, world!')
    >>> with MyPipe() as writable:
    ...     pass
    Traceback (most recent call last):
    ...
    RuntimeError: Hello, world!

    More complicated, less illustrative tests:

    Same as above, but proving that handles are closed:

    >>> x = os.dup(0); os.close(x)
    >>> class MyPipe(WritablePipe):
    ...     def readFrom(self, readable):
    ...         raise RuntimeError('Hello, world!')
    >>> with MyPipe() as writable:
    ...     pass
    Traceback (most recent call last):
    ...
    RuntimeError: Hello, world!
    >>> y = os.dup(0); os.close(y); x == y
    True

    Exceptions in the body of the with statement aren't masked, and handles are closed:

    >>> x = os.dup(0); os.close(x)
    >>> class MyPipe(WritablePipe):
    ...     def readFrom(self, readable):
    ...         pass
    >>> with MyPipe() as writable:
    ...     raise RuntimeError('Hello, world!')
    Traceback (most recent call last):
    ...
    RuntimeError: Hello, world!
    >>> y = os.dup(0); os.close(y); x == y
    True
    """

    def __init__(self, encoding: Optional[str] = None, errors: Optional[str] = None) -> None:
        """
        The specified encoding and errors apply to the writable end of the pipe.

        :param str encoding: the name of the encoding used to encode the file. Encodings are the same
                as for encode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.
        """
        super().__init__()
        self.encoding: Optional[str] = encoding
        self.errors: Optional[str] = errors
        self.readable_fh: Optional[int] = None
        self.writable: Optional[IO[Any]] = None
        self.thread: Optional[ExceptionalThread] = None
        self.reader_done: bool = False

    def __enter__(self) -> IO[Any]:
        self.readable_fh, writable_fh = os.pipe()
        self.writable = os.fdopen(
            writable_fh,
            "wb" if self.encoding == None else "wt",
            encoding=self.encoding,
            errors=self.errors,
        )
        self.thread = ExceptionalThread(target=self._reader)
        self.thread.start()
        return self.writable

    def __exit__(self, exc_type: Optional[str], exc_val: Optional[str], exc_tb: Optional[str]) -> None:
        # Closing the writable end will send EOF to the readable and cause the reader thread
        # to finish.
        # TODO: Can close() fail? If so, would we try and clean up after the reader?
        self.writable.close()
        try:
            if self.thread is not None:
                # reraises any exception that was raised in the thread
                self.thread.join()
        except Exception as e:
            if exc_type is None:
                # Only raise the child exception if there wasn't
                # already an exception in the main thread
                raise
            else:
                log.error(
                    "Swallowing additional exception in reader thread: %s", str(e)
                )
        finally:
            # The responsibility for closing the readable end is generally that of the reader
            # thread. To cover the small window before the reader takes over we also close it here.
            # TODO: Does that make any sense?
            if self.readable_fh is not None:
                # Close the file handle. The reader thread must be dead now.
                try:
                    os.close(self.readable_fh)
                except OSError as e:
                    # OSError: [Errno 9] Bad file descriptor implies this file handle is already closed
                    if not e.errno == errno.EBADF:
                        raise e

    @abstractmethod
    def readFrom(self, readable: IO[Any]) -> None:
        """
        Implement this method to read data from the pipe. This method should support both
        binary and text mode output.

        :param file readable: the file object representing the readable end of the pipe. Do not
            explicitly invoke the close() method of the object; that will be done automatically.
        """
        raise NotImplementedError()

    def _reader(self) -> None:
        with os.fdopen(self.readable_fh, "rb") as readable:
            # TODO: If the reader somehow crashes here, both threads might try
            # to close readable_fh.  Fortunately we don't do anything that
            # should be able to fail here.
            # TODO: Use a real mutex; this None-flagging logic doesn't seem race-free.
            self.readable_fh = None  # signal to parent thread that we've taken over
            self.readFrom(readable)
            self.reader_done = True



class ReadablePipe(ABC):
    """
    An object-oriented wrapper for os.pipe. Clients should subclass it, implement
    :meth:`.writeTo` to place data into the writable end of the pipe, then instantiate the class
    as a context manager to get the writable end. See the example below.

    >>> import sys, shutil, codecs
    >>> class MyPipe(ReadablePipe):
    ...     def writeTo(self, writable: IO[Any]) -> None:
    ...         writable.write('Hello, world!\\n'.encode('utf-8'))
    >>> with MyPipe() as readable:
    ...     shutil.copyfileobj(codecs.getreader('utf-8')(readable), sys.stdout)
    Hello, world!

    Each instance of this class creates a thread and invokes the :meth:`.writeTo` method in that
    thread. The thread will be join()ed upon normal exit from the context manager, i.e. the body
    of the `with` statement. If an exception occurs, the thread will not be joined but a
    well-behaved :meth:`.writeTo` implementation will terminate shortly thereafter due to the
    pipe having been closed.

    Now, exceptions in the reader thread will be reraised in the main thread:

    >>> class MyPipe(ReadablePipe):
    ...     def writeTo(self, writable):
    ...         raise RuntimeError('Hello, world!')
    >>> with MyPipe() as readable:
    ...     pass
    Traceback (most recent call last):
    ...
    RuntimeError: Hello, world!

    More complicated, less illustrative tests:

    Same as above, but proving that handles are closed:

    >>> x = os.dup(0); os.close(x)
    >>> class MyPipe(ReadablePipe):
    ...     def writeTo(self, writable: IO[Any]) -> None:
    ...         raise RuntimeError('Hello, world!')
    >>> with MyPipe() as readable:
    ...     pass
    Traceback (most recent call last):
    ...
    RuntimeError: Hello, world!
    >>> y = os.dup(0); os.close(y); x == y
    True

    Exceptions in the body of the with statement aren't masked, and handles are closed:

    >>> x = os.dup(0); os.close(x)
    >>> class MyPipe(ReadablePipe):
    ...     def writeTo(self, writable):
    ...         pass
    >>> with MyPipe() as readable:
    ...     raise RuntimeError('Hello, world!')
    Traceback (most recent call last):
    ...
    RuntimeError: Hello, world!
    >>> y = os.dup(0); os.close(y); x == y
    True
    """

    @abstractmethod
    def writeTo(self, writable: IO[Any]) -> None:
        """
        Implement this method to write data from the pipe. This method should support both
        binary and text mode input.

        :param file writable: the file object representing the writable end of the pipe. Do not
            explicitly invoke the close() method of the object, that will be done automatically.
        """
        raise NotImplementedError()

    def _writer(self) -> None:
        try:
            with os.fdopen(self.writable_fh, "wb") as writable:
                self.writeTo(writable)
        except OSError as e:
            # The other side of the pipe may have been closed by the
            # reading thread, which is OK.
            if e.errno != errno.EPIPE:
                raise

    def __init__(self, encoding: Optional[str] = None, errors: Optional[str] = None) -> None:
        """
        The specified encoding and errors apply to the readable end of the pipe.

        :param str encoding: the name of the encoding used to encode the file. Encodings are the same
                as for encode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.
        """
        super().__init__()
        self.encoding: Optional[str] = encoding
        self.errors: Optional[str] = errors
        self.writable_fh: Optional[int] = None
        self.readable: Optional[IO[Any]] = None
        self.thread: Optional[ExceptionalThread] = None

    def __enter__(self) -> IO[Any]:
        readable_fh, self.writable_fh = os.pipe()
        self.readable = os.fdopen(
            readable_fh,
            "rb" if self.encoding == None else "rt",
            encoding=self.encoding,
            errors=self.errors,
        )
        self.thread = ExceptionalThread(target=self._writer)
        self.thread.start()
        return self.readable

    def __exit__(self, exc_type: Optional[str], exc_val: Optional[str], exc_tb: Optional[str]) -> None:
        # Close the read end of the pipe. The writing thread may
        # still be writing to the other end, but this will wake it up
        # if that's the case.
        self.readable.close()
        try:
            if self.thread is not None:
                # reraises any exception that was raised in the thread
                self.thread.join()
        except:
            if exc_type is None:
                # Only raise the child exception if there wasn't
                # already an exception in the main thread
                raise


class ReadableTransformingPipe(ReadablePipe):
    """
    A pipe which is constructed around a readable stream, and which provides a
    context manager that gives a readable stream.

    Useful as a base class for pipes which have to transform or otherwise visit
    bytes that flow through them, instead of just consuming or producing data.

    Clients should subclass it and implement :meth:`.transform`, like so:

    >>> import sys, shutil, codecs
    >>> class MyPipe(ReadableTransformingPipe):
    ...     def transform(self, readable, writable):
    ...         writable.write(readable.read().decode('utf-8').upper().encode('utf-8'))
    >>> class SourcePipe(ReadablePipe):
    ...     def writeTo(self, writable):
    ...         writable.write('Hello, world!\\n'.encode('utf-8'))
    >>> with SourcePipe() as source:
    ...     with MyPipe(source) as transformed:
    ...         shutil.copyfileobj(codecs.getreader('utf-8')(transformed), sys.stdout)
    HELLO, WORLD!

    The :meth:`.transform` method runs in its own thread, and should move data
    chunk by chunk instead of all at once. It should finish normally if it
    encounters either an EOF on the readable, or a :class:`BrokenPipeError` on
    the writable. This means that it should make sure to actually catch a
    :class:`BrokenPipeError` when writing.

    See also: :class:`toil.lib.misc.WriteWatchingStream`.

    """

    def __init__(self, source: IO[Any], encoding: Optional[str] = None, errors: Optional[str] = None) -> None:
        """
        :param str encoding: the name of the encoding used to encode the file. Encodings are the same
                as for encode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.
        """
        super().__init__(encoding=encoding, errors=errors)
        self.source = source

    @abstractmethod
    def transform(self, readable: IO[Any], writable: IO[Any]) -> None:
        """
        Implement this method to ship data through the pipe.

        :param file readable: the input stream file object to transform.

        :param file writable: the file object representing the writable end of the pipe. Do not
            explicitly invoke the close() method of the object, that will be done automatically.
        """
        raise NotImplementedError()

    def writeTo(self, writable: IO[Any]) -> None:
        self.transform(self.source, writable)


class HashingPipe(ReadableTransformingPipe):
    """
    Class which checksums all the data read through it. If it
    reaches EOF and the checksum isn't correct, raises ChecksumError.

    Assumes info actually has a checksum.
    """
    def __init__(self, source: IO[Any], encoding: Optional[str] = None, errors: Optional[str] = None, checksum_to_verify: Optional[str] = None) -> None:
        """
        :param str encoding: the name of the encoding used to encode the file. Encodings are the same
                as for encode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.
        """
        super(HashingPipe, self).__init__(source=source, encoding=encoding, errors=errors)
        self.checksum_to_verify = checksum_to_verify

    def transform(self, readable: IO[Any], writable: IO[Any]) -> None:
        hash_object = hashlib.sha1()
        contents = readable.read(1024 * 1024)
        while contents != b'':
            hash_object.update(contents)
            try:
                writable.write(contents)
            except BrokenPipeError:
                # Read was stopped early by user code.
                # Can't check the checksum.
                return
            contents = readable.read(1024 * 1024)
        final_computed_checksum = f'sha1${hash_object.hexdigest()}'
        if not self.checksum_to_verify == final_computed_checksum:
            raise ChecksumError(f'Checksum mismatch. Expected: {self.checksum_to_verify} Actual: {final_computed_checksum}')
