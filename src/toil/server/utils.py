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
import fcntl
import os
from datetime import datetime
from typing import Optional

import requests

from toil.lib.retry import retry


def get_iso_time() -> str:
    """
    Return the current time in ISO 8601 format.
    """
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")


def link_file(src: str, dest: str) -> None:
    """
    Create a link to a file from src to dest.
    """
    if os.path.exists(dest):
        raise RuntimeError(f"Destination file '{dest}' already exists.")
    try:
        os.link(src, dest)
    except OSError:
        os.symlink(src, dest)


def download_file_from_internet(src: str, dest: str, content_type: Optional[str] = None) -> None:
    """
    Download a file from the Internet and write it to dest.
    """
    response = requests.get(src)

    if not response.ok:
        raise RuntimeError("Request failed with a client error or a server error.")

    if content_type and not response.headers.get("Content-Type", "").startswith(content_type):
        val = response.headers.get("Content-Type")
        raise RuntimeError(f"Expected content type to be '{content_type}'.  Not {val}.")

    with open(dest, "wb") as f:
        f.write(response.content)


def get_file_class(path: str) -> str:
    """
    Return the type of the file as a human readable string.
    """
    if os.path.islink(path):
        return "Link"
    elif os.path.isfile(path):
        return "File"
    elif os.path.isdir(path):
        return "Directory"
    return "Unknown"


@retry(errors=[OSError, BlockingIOError])
def safe_read_file(file: str) -> Optional[str]:
    """
    Safely read a file by acquiring a shared lock to prevent other processes
    from writing to it while reading.
    """
    try:
        file_obj = open(file, "r")
    except FileNotFoundError:
        return None

    try:
        # acquire a shared lock on the state file, which is blocking until we can lock it
        fcntl.lockf(file_obj.fileno(), fcntl.LOCK_SH)

        try:
            return file_obj.read()
        finally:
            fcntl.flock(file_obj.fileno(), fcntl.LOCK_UN)
    finally:
        file_obj.close()


@retry(errors=[OSError, BlockingIOError])
def safe_write_file(file: str, s: str) -> None:
    """
    Safely write to a file by acquiring an exclusive lock to prevent other
    processes from reading and writing to it while writing.
    """
    # Open in read and update mode, so we don't modify the file before we acquire a lock
    file_obj = open(file, "r+")

    try:
        # acquire an exclusive lock
        fcntl.flock(file_obj.fileno(), fcntl.LOCK_EX)

        try:
            file_obj.seek(0)
            file_obj.write(s)
            file_obj.truncate()
        finally:
            fcntl.flock(file_obj.fileno(), fcntl.LOCK_UN)
    finally:
        file_obj.close()
