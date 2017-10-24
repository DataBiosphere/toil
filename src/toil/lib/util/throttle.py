from __future__ import absolute_import

from builtins import object
import time
import threading

class throttle( object ):
    """
    A context manager for ensuring that the execution of its body takes at least a given amount
    of time, sleeping if necessary. It is a simpler version of LocalThrottle if used as a
    decorator.

    Ensures that body takes at least the given amount of time.

    >>> start = time.time()
    >>> with throttle(1):
    ...     pass
    >>> 1 <= time.time() - start <= 1.1
    True

    Ditto when used as a decorator.

    >>> @throttle(1)
    ... def f():
    ...     pass
    >>> start = time.time()
    >>> f()
    >>> 1 <= time.time() - start <= 1.1
    True

    If the body takes longer by itself, don't throttle.

    >>> start = time.time()
    >>> with throttle(1):
    ...     time.sleep(2)
    >>> 2 <= time.time() - start <= 2.1
    True

    Ditto when used as a decorator.

    >>> @throttle(1)
    ... def f():
    ...     time.sleep(2)
    >>> start = time.time()
    >>> f()
    >>> 2 <= time.time() - start <= 2.1
    True

    If an exception occurs, don't throttle.

    >>> start = time.time()
    >>> try:
    ...     with throttle(1):
    ...         raise ValueError('foo')
    ... except ValueError:
    ...     end = time.time()
    ...     raise
    Traceback (most recent call last):
    ...
    ValueError: foo
    >>> 0 <= end - start <= 0.1
    True

    Ditto when used as a decorator.

    >>> @throttle(1)
    ... def f():
    ...     raise ValueError('foo')
    >>> start = time.time()
    >>> try:
    ...     f()
    ... except ValueError:
    ...     end = time.time()
    ...     raise
    Traceback (most recent call last):
    ...
    ValueError: foo
    >>> 0 <= end - start <= 0.1
    True
    """

    def __init__( self, min_interval ):
        self.min_interval = min_interval

    def __enter__( self ):
        self.start = time.time( )

    def __exit__( self, exc_type, exc_val, exc_tb ):
        if exc_type is None:
            duration = time.time( ) - self.start
            remainder = self.min_interval - duration
            if remainder > 0:
                time.sleep( remainder )

    def __call__( self, function ):
        def wrapper( *args, **kwargs ):
            with self:
                return function( *args, **kwargs )
        return wrapper
