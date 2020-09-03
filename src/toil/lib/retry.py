# Copyright (C) 2015-2020 Regents of the University of California
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
import traceback

import requests.exceptions
import http.client
import urllib.error
import urllib3.exceptions
import sqlite3
import kubernetes.client.rest
import botocore.exceptions
from contextlib import contextmanager
from typing import List, Set, Optional, Tuple, Callable, Any, Union

log = logging.getLogger(__name__)

SUPPORTED_HTTP_ERRORS = {http.client.HTTPException,
                         urllib.error.HTTPError,
                         urllib3.exceptions.HTTPError,
                         requests.exceptions.HTTPError,
                         kubernetes.client.rest.ApiException,
                         botocore.exceptions.ClientError}


class ErrorCondition:
    """
    A wrapper describing an error condition.

    ErrorCondition events may be used to define errors in more detail to determine whether to retry.

    :param error: An Exception (required)
    :param error_codes: Error codes that must match to be retried (optional; defaults to not checking)
    :param error_message_must_include: A string that must be in the error message to be retried
        (optional; defaults to not checking)
    :param retry_on_this_condition: This can be set to False to always error on this condition.
    """
    def __init__(self,
                 error: Any,
                 error_codes: List[int] = None,
                 boto_error_codes: List[str] = None,
                 error_message_must_include: str = None,
                 retry_on_this_condition: bool = True):
        self.error = error
        self.error_codes = error_codes
        self.boto_error_codes = boto_error_codes
        self.error_message_must_include = error_message_must_include
        self.retry_on_this_condition = retry_on_this_condition

        if self.error_codes:
            if error not in SUPPORTED_HTTP_ERRORS:
                raise NotImplementedError(f'Unknown error type used with error_codes: {error}')

        if self.boto_error_codes:
            if not isinstance(error, botocore.exceptions.ClientError):
                raise NotImplementedError(f'Unknown error type used with boto_error_codes: {error}')


def retry(intervals: Optional[List] = None,
          infinite_retries: bool = False,
          errors: Optional[List[Union[ErrorCondition, Exception, Any]]] = None,
          log_message: Optional[Tuple[Callable, str]] = None):
    """
    Retry a function if it fails with any Exception defined in "errors", every x seconds,
    where x is defined by a list of numbers (ints or floats) in "intervals".  Also accepts ErrorCondition events
    for more detailed retry attempts.

    :param Optional[List] intervals: A list of times in seconds we keep retrying until returning failure.
        Defaults to retrying with the following exponential back-off before failing:
            1s, 1s, 2s, 4s, 8s, 16s
    :param infinite_retries: If this is True, reset the intervals when they run out.  Defaults to: False.
    :param errors: A list of exceptions OR ErrorCondition objects to catch and retry on.
        ErrorCondition objects describe more detailed error event conditions than a plain error.
        An ErrorCondition specifies:
            - Exception (required)
            - Error codes that must match to be retried (optional; defaults to not checking)
            - A string that must be in the error message to be retried (optional; defaults to not checking)
            - A bool that can be set to False to always error on this condition.
        If not specified, this will default to a generic Exception.
    :param log_message: Optional tuple of ("log/print function()", "message string") that will precede each attempt.
    :return: The result of the wrapped function or raise.
    """
    # set mutable defaults
    intervals = intervals if intervals else [1, 1, 2, 4, 8, 16]
    errors = errors if errors else [Exception]

    error_conditions = set([error for error in errors if isinstance(error, ErrorCondition)])
    retriable_errors = set([error for error in errors if not isinstance(error, ErrorCondition)])

    if log_message:
        post_message_function = log_message[0]
        message = log_message[1]

    # if a generic error exists (with no restrictions), delete more specific error_condition instances of it
    for error_condition in error_conditions:
        if error_condition.retry_on_this_condition and error_condition.error in retriable_errors:
            del error_conditions[error_condition]

    # if a more specific error exists that isn't in the general set,
    # add it to the total errors that will be try/except-ed upon
    for error_condition in error_conditions:
        if error_condition.retry_on_this_condition:
            retriable_errors.add(error_condition.error)

    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            intervals_remaining = copy.deepcopy(intervals)
            while True:
                try:
                    if log_message:
                        post_message_function(message)
                    return func(*args, **kwargs)

                except tuple(retriable_errors) as e:
                    if not intervals_remaining:
                        if infinite_retries:
                            intervals_remaining = copy.deepcopy(intervals)
                        else:
                            raise

                    if error_conditions:
                        if not error_meets_conditions(e, error_conditions):
                            raise

                    interval = intervals_remaining.pop(0)
                    log.debug(f"Error in {func}: {e}. Retrying after {interval} s...")
                    time.sleep(interval)
        return call
    return decorate


def return_status_code(e):
    if isinstance(e, requests.exceptions.HTTPError):
        return e.response.status_code
    elif isinstance(e, http.client.HTTPException) or \
            isinstance(e, urllib3.error.HTTPError) or \
            isinstance(e, kubernetes.client.rest.ApiException):
        return e.status
    elif isinstance(e, urllib.error.HTTPError):
        return e.code
    elif isinstance(e, botocore.exceptions.ClientError):
        return e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
    else:
        raise ValueError(f'Unsupported error type; cannot grok status code: {e}.')


def meets_error_message_condition(e: Exception, error_message: Optional[str]):
    if error_message:
        if isinstance(e, http.client.HTTPException) or isinstance(e, urllib3.error.HTTPError):
            return error_message in e.reason
        elif isinstance(e, kubernetes.client.rest.ApiException) or isinstance(sqlite3.OperationalError):
            return error_message in str(e)
        elif isinstance(e, urllib.error.HTTPError) or isinstance(e, botocore.exceptions.ClientError):
            return error_message in e.msg
        elif isinstance(e, requests.exceptions.HTTPError):
            return error_message in e.raw
        else:
            return error_message in traceback.format_exc()
    else:  # there is no error message, so the user is not expecting it to be present
        return True


def meets_error_code_condition(e: Exception, error_codes: Optional[List[int]]):
    """These are expected to be normal HTTP error codes, like 404 or 500."""
    if error_codes:
        status_code = return_status_code(e)
        return int(str(status_code).strip()) in error_codes
    else:  # there are no error codes, so the user is not expecting the status to match
        return True


def meets_boto_error_code_condition(e: Exception, boto_error_codes: Optional[List[str]]):
    """These are expected to be AWS's custom error aliases, like 'BucketNotFound' or 'AccessDenied'."""
    if boto_error_codes:
        status_code = e.response.get('Error', {}).get('Code')
        return status_code in boto_error_codes
    else:  # there are no boto error codes, so the user is not expecting the status to match
        return True


def error_meets_conditions(e, error_conditions):
    condition_met = False
    for error in error_conditions:
        if isinstance(e, error.error):
            if error.error_codes or error.boto_error_codes or error.error_message_must_include:
                error_message_condition_met = meets_error_message_condition(e, error.error_message_must_include)
                error_code_condition_met = meets_error_code_condition(e, error.error_codes)
                boto_error_code_condition_met = meets_boto_error_code_condition(e, error.boto_error_codes)
                if error_message_condition_met and error_code_condition_met and boto_error_code_condition_met:
                    if not error.retry_on_this_condition:
                        return False
                    condition_met = True
    return condition_met


# TODO: Replace the use of this with retry_decorator
#  The aws provisioner and jobstore need a large refactoring to be boto3 compliant, so this is
#  still used there to avoid the duplication of future work
def old_retry(delays=(0, 1, 1, 4, 16, 64), timeout=300, predicate=lambda e: False):
    """
    Deprecated.

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
    >>> for attempt in old_retry( delays=[0], timeout=.1, predicate=true ):
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
    >>> for attempt in old_retry( timeout=0 ):
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
    >>> for attempt in old_retry( delays=[0], timeout=.1, predicate=true ):
    ...     with attempt:
    ...         i += 1
    >>> i
    1

    Don't retry on unless predicate returns True:

    >>> i = 0
    >>> for attempt in old_retry( delays=[0], timeout=.1, predicate=false):
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
