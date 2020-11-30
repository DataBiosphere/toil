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
import datetime
import re
from functools import wraps
from threading import Lock


def memoize(f):
    """
    A decorator that memoizes a function result based on its parameters. For example, this can be
    used in place of lazy initialization. If the decorating function is invoked by multiple
    threads, the decorated function may be called more than once with the same arguments.
    """
    # TODO: Recommend that f's arguments be immutable
    memory = {}

    @wraps(f)
    def new_f(*args):
        try:
            return memory[args]
        except KeyError:
            r = f(*args)
            memory[args] = r
            return r

    return new_f


def sync_memoize(f):
    """
    Like memoize, but guarantees that decorated function is only called once, even when multiple
    threads are calling the decorating function with multiple parameters.
    """
    # TODO: Think about an f that is recursive
    memory = {}
    lock = Lock()

    @wraps(f)
    def new_f(*args):
        try:
            return memory[args]
        except KeyError:
            # on cache misses, retry with lock held
            with lock:
                try:
                    return memory[args]
                except KeyError:
                    r = f(*args)
                    memory[args] = r
                    return r
    return new_f


def properties(obj):
    """
    Returns a dictionary with one entry per attribute of the given object. The key being the
    attribute name and the value being the attribute value. Attributes starting in two
    underscores will be ignored. This function is an alternative to vars() which only returns
    instance variables, not properties. Note that methods are returned as well but the value in
    the dictionary is the method, not the return value of the method.

    >>> class Foo():
    ...     def __init__(self):
    ...         self.var = 1
    ...     @property
    ...     def prop(self):
    ...         return self.var + 1
    ...     def meth(self):
    ...         return self.var + 2
    >>> foo = Foo()
    >>> properties(foo) == {'var': 1, 'prop': 2, 'meth': foo.meth}
    True

    Note how the entry for prop is not a bound method (i.e. the getter) but a the return value of
    that getter.
    """
    return dict((attr, getattr(obj, attr)) for attr in dir(obj) if not attr.startswith('__'))


def rfc3339_datetime_re(anchor=True):
    """
    Returns a regular expression for syntactic validation of ISO date-times, RFC-3339 date-times
    to be precise.


    >>> bool( rfc3339_datetime_re().match('2013-11-06T15:56:39Z') )
    True

    >>> bool( rfc3339_datetime_re().match('2013-11-06T15:56:39.123Z') )
    True

    >>> bool( rfc3339_datetime_re().match('2013-11-06T15:56:39-08:00') )
    True

    >>> bool( rfc3339_datetime_re().match('2013-11-06T15:56:39.123+11:00') )
    True

    It anchors the matching to the beginning and end of a string by default ...

    >>> bool( rfc3339_datetime_re().search('bla 2013-11-06T15:56:39Z bla') )
    False

    ... but that can be changed:

    >>> bool( rfc3339_datetime_re( anchor=False ).search('bla 2013-11-06T15:56:39Z bla') )
    True

    >>> bool( rfc3339_datetime_re( anchor=False ).match('2013-11-06T15:56:39Z bla') )
    True

    Keep in mind that re.match() always anchors at the beginning:

    >>> bool( rfc3339_datetime_re( anchor=False ).match('bla 2013-11-06T15:56:39Z') )
    False

    It does not check whether the actual value is a semantically valid datetime:

    >>> bool( rfc3339_datetime_re().match('9999-99-99T99:99:99.9-99:99') )
    True

    If the regular expression matches, each component of the matching value will be exposed as a
    captured group in the match object.

    >>> rfc3339_datetime_re().match('2013-11-06T15:56:39Z').groups()
    ('2013', '11', '06', '15', '56', '39', None, 'Z')
    >>> rfc3339_datetime_re().match('2013-11-06T15:56:39.123Z').groups()
    ('2013', '11', '06', '15', '56', '39', '123', 'Z')
    >>> rfc3339_datetime_re().match('2013-11-06T15:56:39.123-08:30').groups()
    ('2013', '11', '06', '15', '56', '39', '123', '-08:30')
    """
    return re.compile(
        ('^' if anchor else '') +
        '(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})' +
        ('$' if anchor else ''))


def parse_iso_utc(s):
    """
    Parses an ISO time with a hard-coded Z for zulu-time (UTC) at the end. Other timezones are
    not supported.

    :param str s: the ISO-formatted time

    :rtype: datetime.datetime

    :return: an timezone-naive datetime object

    >>> parse_iso_utc('2016-04-27T00:28:04.000Z')
    datetime.datetime(2016, 4, 27, 0, 28, 4)
    >>> parse_iso_utc('2016-04-27T00:28:04Z')
    datetime.datetime(2016, 4, 27, 0, 28, 4)
    >>> parse_iso_utc('2016-04-27T00:28:04X')
    Traceback (most recent call last):
    ...
    ValueError: Not a valid ISO datetime in UTC: 2016-04-27T00:28:04X
    """
    m = rfc3339_datetime_re().match(s)
    if not m:
        raise ValueError( 'Not a valid ISO datetime in UTC: ' + s )
    else:
        fmt = '%Y-%m-%dT%H:%M:%S' + ('.%f' if m.group(7) else '') + 'Z'
        return datetime.datetime.strptime(s, fmt)


def strict_bool(s):
    """Variant of bool() that only accepts two possible string values."""
    if s == 'True':
        return True
    elif s == 'False':
        return False
    else:
        raise ValueError(s)


def less_strict_bool(x):
    """Idempotent and None-safe version of strict_bool."""
    if x is None:
        return False
    elif x is True or x is False:
        return x
    else:
        return strict_bool(x)
