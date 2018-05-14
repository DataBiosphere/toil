# Copyright (C) 2015-2018 Regents of the University of California
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

from __future__ import absolute_import

from future import standard_library
standard_library.install_aliases()
from builtins import next
from contextlib import contextmanager
import time
import urllib.request, urllib.error, urllib.parse
import logging

log = logging.getLogger( __name__ )


# noinspection PyUnusedLocal
def never( exception ):
    return False


def retry( delays=(0, 1, 1, 4, 16, 64), timeout=300, predicate=never ):
    """
    Retry an operation while the failure matches a given predicate and until a given timeout
    expires, waiting a given amount of time in between attempts. This function is a generator
    that yields contextmanagers. See doctests below for example usage.

    :param Iterable[float] delays: an interable yielding the time in seconds to wait before each
           retried attempt, the last element of the iterable will be repeated.

    :param float timeout: a overall timeout that should not be exceeded for all attempts together.
           This is a best-effort mechanism only and it won't abort an ongoing attempt, even if the
           timeout expires during that attempt.

    :param Callable[[Exception],bool] predicate: a unary callable returning True if another
           attempt should be made to recover from the given exception. The default value for this
           parameter will prevent any retries!

    :return: a generator yielding context managers, one per attempt
    :rtype: Iterator

    Retry for a limited amount of time:

    >>> true = lambda _:True
    >>> false = lambda _:False
    >>> i = 0
    >>> for attempt in retry( delays=[0], timeout=.1, predicate=true ):
    ...     with attempt:
    ...         i += 1
    ...         raise RuntimeError('foo')
    Traceback (most recent call last):
    ...
    RuntimeError: foo
    >>> i > 1
    True

    If timeout is 0, do exactly one attempt:

    >>> i = 0
    >>> for attempt in retry( timeout=0 ):
    ...     with attempt:
    ...         i += 1
    ...         raise RuntimeError( 'foo' )
    Traceback (most recent call last):
    ...
    RuntimeError: foo
    >>> i
    1

    Don't retry on success:

    >>> i = 0
    >>> for attempt in retry( delays=[0], timeout=.1, predicate=true ):
    ...     with attempt:
    ...         i += 1
    >>> i
    1

    Don't retry on unless predicate returns True:

    >>> i = 0
    >>> for attempt in retry( delays=[0], timeout=.1, predicate=false):
    ...     with attempt:
    ...         i += 1
    ...         raise RuntimeError( 'foo' )
    Traceback (most recent call last):
    ...
    RuntimeError: foo
    >>> i
    1
    """
    if timeout > 0:
        go = [ None ]

        @contextmanager
        def repeated_attempt( delay ):
            try:
                yield
            except Exception as e:
                if time.time( ) + delay < expiration and predicate( e ):
                    log.info( 'Got %s, trying again in %is.', e, delay )
                    time.sleep( delay )
                else:
                    raise
            else:
                go.pop( )

        delays = iter( delays )
        expiration = time.time( ) + timeout
        delay = next( delays )
        while go:
            yield repeated_attempt( delay )
            delay = next( delays, delay )
    else:
        @contextmanager
        def single_attempt( ):
            yield

        yield single_attempt( )


default_delays = (0, 1, 1, 4, 16, 64)
default_timeout = 300


def retryable_http_error( e ):
    return isinstance( e, urllib.error.HTTPError ) and e.code in ('503', '408', '500')


def retry_http( delays=default_delays, timeout=default_timeout, predicate=retryable_http_error ):
    """
    >>> i = 0
    >>> for attempt in retry_http(timeout=5):  # doctest: +IGNORE_EXCEPTION_DETAIL
    ...     with attempt:
    ...         i += 1
    ...         raise urllib.error.HTTPError('http://www.test.com', '408', 'some message', {}, None)
    Traceback (most recent call last):
    ...
    HTTPError: HTTP Error 408: some message
    >>> i > 1
    True
    """
    return retry( delays=delays, timeout=timeout, predicate=predicate )
