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

# 5.14.2018: copied into Toil from https://github.com/BD2KGenomics/bd2k-python-lib
import threading
import time
from typing import Union


class LocalThrottle:
    """
    A thread-safe rate limiter that throttles each thread independently. Can be used as a
    function or method decorator or as a simple object, via its .throttle() method.

    The use as a decorator is deprecated in favor of throttle().
    """

    def __init__(self, min_interval: int) -> None:
        """
        Initialize this local throttle.

        :param min_interval: The minimum interval in seconds between invocations of the throttle
        method or, if this throttle is used as a decorator, invocations of the decorated method.
        """
        self.min_interval = min_interval
        self.per_thread = threading.local()
        self.per_thread.last_invocation = None

    def throttle(self, wait: bool = True) -> bool:
        """
        If the wait parameter is True, this method returns True after suspending the current
        thread as necessary to ensure that no less than the configured minimum interval has
        passed since the last invocation of this method in the current thread returned True.

        If the wait parameter is False, this method immediatly returns True (if at least the
        configured minimum interval has passed since the last time this method returned True in
        the current thread) or False otherwise.
        """
        now = time.time( )
        last_invocation = self.per_thread.last_invocation
        if last_invocation is not None:
            interval = now - last_invocation
            if interval < self.min_interval:
                if wait:
                    remainder = self.min_interval - interval
                    time.sleep( remainder )
                else:
                    return False
        self.per_thread.last_invocation = now
        return True

    def __call__( self, function ):
        def wrapper( *args, **kwargs ):
            self.throttle( )
            return function( *args, **kwargs )

        return wrapper


class throttle:
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

    def __init__(self, min_interval: Union[int, float]) -> None:
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
