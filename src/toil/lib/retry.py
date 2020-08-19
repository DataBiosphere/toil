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
import time
import copy
import functools
import logging

from contextlib import contextmanager
from requests.exceptions import HTTPError
from typing import List, Set, Optional, Tuple, Callable

log = logging.getLogger(__name__)


def retry(delays=(0, 1, 1, 4, 16, 64), timeout=300, predicate=lambda e: False):
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


def retry_decorator(intervals: Optional[List] = None,
                    infinite_retries: Optional[bool] = None,
                    errors: Optional[Set] = None,
                    error_codes: Optional[Set] = None,
                    error_msg_must_include: Optional[dict] = None,
                    log_message: Optional[Tuple[Callable, str]] = None):
    """
    Retry a function if it fails with any Exception defined in the "errors" set, every x seconds,
    where x is defined by a list of floats in "intervals".  If "error_codes" are specified,
    retry on the HTTPError return codes defined in "error_codes".

    Cases to consider:
        error_codes ={} && errors={}
            Don't retry on anything.
        error_codes ={500} && errors={}
        error_codes ={500} && errors={HTTPError}
            Retry only on HTTPErrors that return status_code 500.
        error_codes ={} && errors={HTTPError}
            Retry on all HTTPErrors regardless of error code.
        error_codes ={} && errors={AssertionError}
            Only retry on AssertionErrors.

    :param Optional[List[float, int]] intervals: A list of times in seconds we keep retrying until
        returning failure.
        Defaults to retrying with the following exponential backoff before failing:
            1s, 1s, 2s, 4s, 8s
    :param infinite_retries: If this is True, reset the intervals when they run out.
    :param errors: Exceptions to catch and retry on.  Defaults to: {HTTPError} if error_codes else {}.
    :param error_codes: HTTPError return codes to retry on.  The default is an empty set.
    :param log_message: A tuple of ("log/print function()", "message string") that will run on
        each retry.
    :return: The result of the wrapped function or raise.
    """
    # set mutable defaults
    intervals = intervals if intervals else [1, 1, 2, 4, 8]
    errors = errors if errors else {HTTPError} if error_codes else {}
    error_codes = error_codes if error_codes else {}

    if log_message:
        post_message_function = log_message[0]
        message = log_message[1]

    if error_codes:
        errors.add(HTTPError)

    if error_msg_must_include:
        for error in error_msg_must_include:
            errors.add(error)

    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            intervals_remaining = copy.deepcopy(intervals)
            while True:
                try:
                    if log_message:
                        post_message_function(message)
                    return func(*args, **kwargs)
                except tuple(errors) as e:
                    if not intervals_remaining:
                        if infinite_retries:
                            intervals_remaining = copy.deepcopy(intervals)
                        else:
                            raise
                    if isinstance(e, HTTPError):
                        if error_codes and e.response.status_code not in error_codes:
                            raise
                    for error in error_msg_must_include:
                        if isinstance(e, error) and error_msg_must_include[error] not in str(e):
                            raise
                    interval = intervals_remaining.pop(0)
                    log.debug(f"Error in {func}: {e}. Retrying after {interval} s...")
                    time.sleep(interval)
        return call
    return decorate
