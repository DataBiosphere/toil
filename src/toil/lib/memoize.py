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
import datetime
import re
from functools import lru_cache, wraps
from threading import Lock
from typing import Any, Callable, TypeVar

memoize = lru_cache(maxsize=None)
"""
Memoize a function result based on its parameters using this decorator.

For example, this can be used in place of lazy initialization. If the decorating
function is invoked by multiple threads, the decorated function may be called
more than once with the same arguments.
"""

MAT = TypeVar("MAT")
MRT = TypeVar("MRT")


def sync_memoize(f: Callable[[MAT], MRT]) -> Callable[[MAT], MRT]:
    """
    Like memoize, but guarantees that decorated function is only called once, even when multiple
    threads are calling the decorating function with multiple parameters.
    """
    # TODO: Think about an f that is recursive
    memory: dict[tuple[Any, ...], Any] = {}
    lock = Lock()

    @wraps(f)
    def new_f(*args: Any) -> Any:
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


def parse_iso_utc(s: str) -> datetime.datetime:
    """
    Parses an ISO time with a hard-coded Z for zulu-time (UTC) at the end. Other timezones are
    not supported. Returns a timezone-aware UTC datetime object.

    :param s: The ISO-formatted time

    :return: A timezone-aware UTC datetime object

    >>> parse_iso_utc('2016-04-27T00:28:04.000Z')
    datetime.datetime(2016, 4, 27, 0, 28, 4, tzinfo=datetime.timezone.utc)
    >>> parse_iso_utc('2016-04-27T00:28:04Z')
    datetime.datetime(2016, 4, 27, 0, 28, 4, tzinfo=datetime.timezone.utc)
    >>> parse_iso_utc('2016-04-27T00:28:04X')
    Traceback (most recent call last):
    ...
    ValueError: Not a valid ISO datetime in UTC: 2016-04-27T00:28:04X
    """
    rfc3339_datetime = re.compile(
        r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$"
    )
    m = rfc3339_datetime.match(s)
    if not m:
        raise ValueError(f"Not a valid ISO datetime in UTC: {s}")
    else:
        if m.group(8) != "Z" and not m.group(8).endswith("00:00"):
            raise ValueError(f"Not in the UTC time zone: {s}")
        if m.group(8) == "Z":
            # Convert to an offset for parsing
            s = s[:-1] + "-00:00"

        fmt = "%Y-%m-%dT%H:%M:%S" + (".%f" if m.group(7) else "") + "%z"
        parsed = datetime.datetime.strptime(s, fmt)
        # We should have guaranteed that this is in UTC
        assert parsed.tzinfo is not None
        return parsed


def strict_bool(s: str) -> bool:
    """Variant of bool() that only accepts two possible string values."""
    if s == "True":
        return True
    elif s == "False":
        return False
    else:
        raise ValueError(s)
