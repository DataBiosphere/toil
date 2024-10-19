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
import fnmatch
import math
import os
import resource
import sys


class ResourceMonitor:
    """
    Global resource monitoring widget.

    Presents class methods to get the resource usage of this process and child
    processes, and other class methods to adjust the statistics so they can
    account for e.g. resources used inside containers, or other resource usage
    that *should* be billable to the current process.
    """

    # Store some extra usage to tack onto the stats as module-level globals
    _extra_cpu_seconds: float = 0
    _extra_memory_ki: int = 0

    @classmethod
    def record_extra_memory(cls, peak_ki: int) -> None:
        """
        Become responsible for the given peak memory usage, in kibibytes.

        The memory will be treated as if it was used by a child process at the time
        our real child processes were also using their peak memory.
        """
        cls._extra_memory_ki = max(cls._extra_memory_ki, peak_ki)

    @classmethod
    def record_extra_cpu(cls, seconds: float) -> None:
        """
        Become responsible for the given CPU time.

        The CPU time will be treated as if it had been used by a child process.
        """
        cls._extra_cpu_seconds += seconds

    @classmethod
    def get_total_cpu_time_and_memory_usage(cls) -> tuple[float, int]:
        """
        Gives the total cpu time of itself and all its children, and the maximum RSS memory usage of
        itself and its single largest child (in kibibytes).
        """
        me = resource.getrusage(resource.RUSAGE_SELF)
        children = resource.getrusage(resource.RUSAGE_CHILDREN)
        total_cpu_time = (
            me.ru_utime
            + me.ru_stime
            + children.ru_utime
            + children.ru_stime
            + cls._extra_cpu_seconds
        )
        total_memory_usage = me.ru_maxrss + children.ru_maxrss
        if sys.platform == "darwin":
            # On Linux, getrusage works in "kilobytes" (really kibibytes), but on
            # Mac it works in bytes. See
            # <https://github.com/python/cpython/issues/74698>
            total_memory_usage = int(math.ceil(total_memory_usage / 1024))
        total_memory_usage += cls._extra_memory_ki
        return total_cpu_time, total_memory_usage

    @classmethod
    def get_total_cpu_time(cls) -> float:
        """Gives the total cpu time, including the children."""
        me = resource.getrusage(resource.RUSAGE_SELF)
        childs = resource.getrusage(resource.RUSAGE_CHILDREN)
        return (
            me.ru_utime
            + me.ru_stime
            + childs.ru_utime
            + childs.ru_stime
            + cls._extra_cpu_seconds
        )


def glob(glob_pattern: str, directoryname: str) -> list[str]:
    """
    Walks through a directory and its subdirectories looking for files matching
    the glob_pattern and returns a list=[].

    :param directoryname: Any accessible folder name on the filesystem.
    :param glob_pattern: A string like ``*.txt``, which would find all text files.
    :return: A list=[] of absolute filepaths matching the glob pattern.
    """
    matches = []
    for root, dirnames, filenames in os.walk(directoryname):
        for filename in fnmatch.filter(filenames, glob_pattern):
            absolute_filepath = os.path.join(root, filename)
            matches.append(absolute_filepath)
    return matches
