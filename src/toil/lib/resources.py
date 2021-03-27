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
import os
import resource
import subprocess

from typing import List, Tuple


def get_total_cpu_time_and_memory_usage():
    """
    Gives the total cpu time of itself and all its children, and the maximum RSS memory usage of
    itself and its single largest child.
    """
    me = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    total_cpu_time = me.ru_utime + me.ru_stime + children.ru_utime + children.ru_stime
    total_memory_usage = me.ru_maxrss + children.ru_maxrss
    return total_cpu_time, total_memory_usage


def get_total_cpu_time():
    """Gives the total cpu time, including the children."""
    me = resource.getrusage(resource.RUSAGE_SELF)
    childs = resource.getrusage(resource.RUSAGE_CHILDREN)
    return me.ru_utime + me.ru_stime + childs.ru_utime + childs.ru_stime


def get_dir_size_recursively(dir_path: str) -> int:
    """
    This method will return the cumulative number of bytes occupied by the files
    on disk in the directory and its subdirectories.

    If the method is unable to access a file or directory (due to insufficient
    permissions, or due to the file or directory having been removed while this
    function was attempting to traverse it), the error will be handled
    internally, and a (possibly 0) lower bound on the size of the directory
    will be returned.

    The environment variable 'BLOCKSIZE'='512' is set instead of the much cleaner
    --block-size=1 because Apple can't handle it.

    :param str dir_path: A valid path to a directory or file.
    :return: Total size, in bytes, of the file or directory at dirPath.
    """
    # du is often faster than using os.lstat(), sometimes significantly so.

    # The call: 'du -s /some/path' should give the number of 512-byte blocks
    # allocated with the environment variable: BLOCKSIZE='512' set, and we
    # multiply this by 512 to return the filesize in bytes.
    try:
        return int(subprocess.check_output(['du', '-s', dir_path],
                                           env=dict(os.environ, BLOCKSIZE='512')).decode('utf-8').split()[0]) * 512
    except subprocess.CalledProcessError:
        # Something was inaccessible or went away
        return 0


def get_file_system_size(dir_path: str) -> Tuple[int, int]:
    """
    Return the free space, and total size of the file system hosting `dirPath`.

    :param str dir_path: A valid path to a directory.
    :return: free space and total size of file system
    :rtype: tuple
    """
    assert os.path.exists(dir_path)
    disk_stats = os.statvfs(dir_path)
    available_disk_space = disk_stats.f_frsize * disk_stats.f_bavail
    total_disk_size = disk_stats.f_frsize * disk_stats.f_blocks
    return available_disk_space, total_disk_size


def glob(glob_pattern: str, directoryname: str) -> List[str]:
    """
    Walks through a directory and its subdirectories looking for files matching
    the glob_pattern and returns a list=[].

    :param directoryname: Any accessible folder name on the filesystem.
    :param glob_pattern: A string like "*.txt", which would find all text files.
    :return: A list=[] of absolute filepaths matching the glob pattern.
    """
    matches = []
    for root, dirnames, filenames in os.walk(directoryname):
        for filename in fnmatch.filter(filenames, glob_pattern):
            absolute_filepath = os.path.join(root, filename)
            matches.append(absolute_filepath)
    return matches
