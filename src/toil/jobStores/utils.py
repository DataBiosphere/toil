from builtins import object
import codecs
import logging
import os
import errno
from abc import ABCMeta
from abc import abstractmethod

from toil.lib.threading import ExceptionalThread
from future.utils import with_metaclass

log = logging.getLogger(__name__)

class WritablePipe(with_metaclass(ABCMeta, object)):
    """
    An object-oriented wrapper for os.pipe. Clients should subclass it, implement
    :meth:`.readFrom` to consume the readable end of the pipe, then instantiate the class as a
    context manager to get the writable end. See the example below.

    >>> import sys, shutil
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

    Same as above, but provving that handles are closed:

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

    @abstractmethod
    def readFrom(self, readable):
        """
        Implement this method to read data from the pipe.

        :param file readable: the file object representing the readable end of the pipe. Do not
        explicitly invoke the close() method of the object, that will be done automatically.
        """
        raise NotImplementedError()

    def _reader(self):
        with os.fdopen(self.readable_fh, 'rb') as readable:
            # TODO: If the reader somehow crashes here, both threads might try
            # to close readable_fh.  Fortunately we don't do anything that
            # should be able to fail here.
            self.readable_fh = None  # signal to parent thread that we've taken over
            self.readFrom(readable)
            self.reader_done = True

    def __init__(self):
        super(WritablePipe, self).__init__()
        self.readable_fh = None
        self.writable = None
        self.thread = None
        self.reader_done = False

    def __enter__(self):
        self.readable_fh, writable_fh = os.pipe()
        self.writable = os.fdopen(writable_fh, 'wb')
        self.thread = ExceptionalThread(target=self._reader)
        self.thread.start()
        return self.writable

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Closeing the writable end will send EOF to the readable and cause the reader thread
        # to finish.
        # TODO: Can close() fail? If so, whould we try and clean up after the reader?
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
                log.error('Swallowing additional exception in reader thread: %s', str(e))
        finally:
            # The responsibility for closing the readable end is generally that of the reader
            # thread. To cover the small window before the reader takes over we also close it here.
            readable_fh = self.readable_fh
            if readable_fh is not None:
                # Close the file handle. The reader thread must be dead now. 
                os.close(readable_fh)


class ReadablePipe(with_metaclass(ABCMeta, object)):
    """
    An object-oriented wrapper for os.pipe. Clients should subclass it, implement
    :meth:`.writeTo` to place data into the writable end of the pipe, then instantiate the class
    as a context manager to get the writable end. See the example below.

    >>> import sys, shutil
    >>> class MyPipe(ReadablePipe):
    ...     def writeTo(self, writable):
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

    Same as above, but provving that handles are closed:

    >>> x = os.dup(0); os.close(x)
    >>> class MyPipe(ReadablePipe):
    ...     def writeTo(self, writable):
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
    def writeTo(self, writable):
        """
        Implement this method to read data from the pipe.

        :param file writable: the file object representing the writable end of the pipe. Do not
        explicitly invoke the close() method of the object, that will be done automatically.
        """
        raise NotImplementedError()

    def _writer(self):
        try:
            with os.fdopen(self.writable_fh, 'wb') as writable:
                self.writeTo(writable)
        except IOError as e:
            # The other side of the pipe may have been closed by the
            # reading thread, which is OK.
            if e.errno != errno.EPIPE:
                raise

    def __init__(self):
        super(ReadablePipe, self).__init__()
        self.writable_fh = None
        self.readable = None
        self.thread = None

    def __enter__(self):
        readable_fh, self.writable_fh = os.pipe()
        self.readable = os.fdopen(readable_fh, 'rb')
        self.thread = ExceptionalThread(target=self._writer)
        self.thread.start()
        return self.readable

    def __exit__(self, exc_type, exc_val, exc_tb):
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
