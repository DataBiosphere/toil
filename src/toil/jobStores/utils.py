import logging
import os
from abc import ABCMeta
from abc import abstractmethod

from bd2k.util.threading import ExceptionalThread

log = logging.getLogger(__name__)

class WritablePipe(object):
    """
    An object-oriented wrapper for os.pipe. Clients should subclass it, implement
    :meth:`.readFrom` to consume the readable end of the pipe, then instantiate the class as a
    context manager to get the writable end. See the example below.

    >>> import sys, shutil
    >>> class MyPipe(WritablePipe):
    ...     def readFrom(self, readable):
    ...         shutil.copyfileobj(readable, sys.stdout)
    >>> with MyPipe() as writable:
    ...     writable.write('Hello, world!\\n')
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

    __metaclass__ = ABCMeta

    @abstractmethod
    def readFrom(self, readable):
        """
        Implement this method to read data from the pipe.

        :param file readable: the file object representing the readable end of the pipe. Do not
        explicitly invoke the close() method of the object, that will be done automatically.
        """
        raise NotImplementedError()

    def _reader(self):
        with os.fdopen(self.readable_fh, 'r') as readable:
            # FIXME: another race here, causing a redundant attempt to close in the main thread
            self.readable_fh = None  # signal to parent thread that we've taken over
            self.readFrom(readable)

    def __init__(self):
        super(WritablePipe, self).__init__()
        self.readable_fh = None
        self.writable = None
        self.thread = None

    def __enter__(self):
        self.readable_fh, writable_fh = os.pipe()
        self.writable = os.fdopen(writable_fh, 'w')
        self.thread = ExceptionalThread(target=self._reader)
        self.thread.start()
        return self.writable

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.writable.close()
            # Closeing the writable end will send EOF to the readable and cause the reader thread
            # to finish.
            if exc_type is None:
                if self.thread is not None:
                    # reraises any exception that was raised in the thread
                    self.thread.join()
        finally:
            # The responsibility for closing the readable end is generally that of the reader
            # thread. To cover the small window before the reader takes over we also close it here.
            readable_fh = self.readable_fh
            if readable_fh is not None:
                # FIXME: This is still racy. The reader thread could close it now, and someone
                # else may immediately open a new file, reusing the file handle.
                os.close(readable_fh)


# FIXME: Unfortunately these two classes are almost an exact mirror image of each other.
# Basically, read and write are swapped. The only asymmetry lies in how shutdown is handled. I
# tried generalizing but the code becomes inscrutable. Until I (or someone else) has a better
# idea how to solve this, I think its better to have code that is readable at the expense of
# duplication.


class ReadablePipe(object):
    """
    An object-oriented wrapper for os.pipe. Clients should subclass it, implement
    :meth:`.writeTo` to place data into the writable end of the pipe, then instantiate the class
    as a context manager to get the writable end. See the example below.

    >>> import sys, shutil
    >>> class MyPipe(ReadablePipe):
    ...     def writeTo(self, writable):
    ...         writable.write('Hello, world!\\n')
    >>> with MyPipe() as readable:
    ...     shutil.copyfileobj(readable, sys.stdout)
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

    __metaclass__ = ABCMeta

    @abstractmethod
    def writeTo(self, writable):
        """
        Implement this method to read data from the pipe.

        :param file writable: the file object representing the writable end of the pipe. Do not
        explicitly invoke the close() method of the object, that will be done automatically.
        """
        raise NotImplementedError()

    def _writer(self):
        with os.fdopen(self.writable_fh, 'w') as writable:
            # FIXME: another race here, causing a redundant attempt to close in the main thread
            self.writable_fh = None  # signal to parent thread that we've taken over
            self.writeTo(writable)

    def __init__(self):
        super(ReadablePipe, self).__init__()
        self.writable_fh = None
        self.readable = None
        self.thread = None

    def __enter__(self):
        readable_fh, self.writable_fh = os.pipe()
        self.readable = os.fdopen(readable_fh, 'r')
        self.thread = ExceptionalThread(target=self._writer)
        self.thread.start()
        return self.readable

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                if self.thread is not None:
                    # reraises any exception that was raised in the thread
                    self.thread.join()
        finally:
            self.readable.close()
            # The responsibility for closing the writable end is generally that of the writer
            # thread. To cover the small window before the writer takes over we also close it here.
            writable_fh = self.writable_fh
            if writable_fh is not None:
                # FIXME: This is still racy. The writer thread could close it now, and someone
                # else may immediately open a new file, reusing the file handle.
                os.close(writable_fh)
