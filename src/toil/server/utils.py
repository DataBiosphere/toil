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
from abc import abstractmethod
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import logging
import requests

from toil.lib.retry import retry
from toil.lib.io import AtomicFileCreate

logger = logging.getLogger(__name__)

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
    if not os.path.exists(src):
        raise RuntimeError(f"Source file '{src}' does not exist.")
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

def download_file_from_s3(src: str, dest: str, content_type: Optional[str] = None) -> None:
    """
    Download a file from Amazon S3 and write it to dest.
    """
    try:
        # Modules to talk to S3 might not always be available so we import things here.
        from toil.lib.aws.utils import get_object_for_url
    except ImportError:
        raise RuntimeError("Cannot access S3 as AWS modules are not available")

    with open(dest, 'wb') as out_stream:
        obj = get_object_for_url(urlparse(src), existing=True)
        obj.download_fileobj(out_stream)

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
    
    if os.path.exists(file):
        # Going to overwrite without anyone else being able to see ths
        # intermediate state.
    
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
    else:
        # Contend with everyone else to create the file. Last write will win
        # but it will be atomic because of the filesystem.
        with AtomicFileCreate(file) as temp_name:
            with open(temp_name, "w") as file_obj:
                file_obj.write(s)
        

class AbstractStateStore:
    """
    A place for the WES server to keep its state: the set of workflows that
    exist and whether they are done or not.

    This is a key-value store, with keys namespaced by workflow ID. Concurrent
    access from multiple threads or processes is safe and globally consistent.

    Key values are either a string, or None if the key is not set.

    Workflow existence isn't a thing; nonexistent workflows just have None for
    all keys.

    Note that we don't yet have a cleanup operation: things are stored
    permanently. Even clearing all the keys may leave data behind.

    TODO: Can we replace this with just using a JobStore eventually, when
    AWSJobStore no longer needs SimpleDB?
    """

    @abstractmethod
    def get(self, workflow_id: str, key: str) -> Optional[str]:
        """
        Get the value of the given key for the given workflow, or None if the
        key is not set for the workflow.
        """
        raise NotImplementedError

    @abstractmethod
    def set(self, workflow_id: str, key: str, value: Optional[str]) -> None:
        """
        Set the value of the given key for the given workflow. If the value is
        None, clear the key.
        """
        raise NotImplementedError

class FileStateStore(AbstractStateStore):
    """
    A place to store workflow state that uses a POSIX-compatible file system.
    """

    def __init__(self, url: str) -> None:
        """
        Connect to the state store in the given local directory.

        :param url: Local state store path. Must be a path, not a file:// URL.
        """
        if url.startswith('file:') or url.startswith('s3:'):
            # We want to catch if we get the wrong argument.
            raise RuntimeError(f"{url} doesn't look like a local path")
        if not os.path.exists(url):
            # We need this directory to exist.
            os.makedirs(url, exist_ok=True)
        logger.debug("Connected to FileStateStore at %s", url)
        self._base_dir = url

    def get(self, workflow_id: str, key: str) -> Optional[str]:
        """
        Get a key value from the filesystem.
        """
        return safe_read_file(os.path.join(self._base_dir, workflow_id, key))

    def set(self, workflow_id: str, key: str, value: Optional[str]) -> None:
        """
        Set or clear a key value on the filesystem.
        """
        # Make sure the directory we need exists.
        workflow_dir = os.path.join(self._base_dir, workflow_id)
        os.makedirs(workflow_dir, exist_ok=True)
        file_path = os.path.join(workflow_dir, key)
        if value is None:
            # Delete the file
            try:
                os.unlink(file_path)
            except FileNotFoundError:
                # It wasn't there to start with
                pass
        else:
            # Set the value in the file
            safe_write_file(file_path, value)

class WorkflowStateStore:
    """
    Slice of a state store for the state of a particular workflow.
    """

    def __init__(self, state_store: AbstractStateStore, workflow_id: str) -> None:
        """
        Wrap the given state store for access to the given workflow's state.
        """

        # TODO: We could just use functools.partial on the state store methods
        # to make ours dynamically but that might upset MyPy.
        self._state_store = state_store
        self._workflow_id = workflow_id

    def get(self, key: str) -> Optional[str]:
        """
        Get the given item of workflow state.
        """
        return self._state_store.get(self._workflow_id, key)

    def set(self, key: str, value: Optional[str]) -> Optional[str]:
        """
        Set the given item of workflow state.
        """
        self._state_store.set(self._workflow_id, key, value)

def connect_to_state_store(url: str) -> AbstractStateStore:
    """
    Connect to a place to store state for workflows, defined by a URL.

    URL may be a local file path or (currently) an S3 URL.
    """

    # If it's not anything else, treat it as a local path.
    return FileStateStore(url)

def connect_to_workflow_state_store(url: str, workflow_id: str) -> WorkflowStateStore:
    """
    Connect to a place to store state for the given workflow, in the state
    store defined by the given URL.

    :param url: A URL that can be used for connect_to_state_store()
    """

    return WorkflowStateStore(connect_to_state_store(url), workflow_id)

# Some states are just last write wins
BASE_STATES = ["UNKNOWN", "QUEUED", "INITIALIZING", "RUNNING", "PAUSED"]
# Some states are flags. These flags are in priority order.
# The first of these that is set will win over all others and over any base state.
# TODO: do we need to ban e.g. CANCELING -> COMPLETE transitions? If so revise this order.
FLAG_STATES_IN_ORDER = ["COMPLETE", "EXECUTOR_ERROR", "SYSTEM_ERROR", "CANCELED", "CANCELING"]

class WorkflowStateMachine:
    """
    Class for managing the WES workflow state machine and ensuring only allowed
    transitions happen.

    This is the authority on the WES "state" of a workflow. You need one to
    read or change the state.

    The state machine is read-your-own-writes consistent and never allows you
    to observe prohibited transitions. Talking to other writers outside of the
    state machine may cause you to disagree on the current state.

    State can be:

    "UNKNOWN"
    "QUEUED"
    "INITIALIZING"
    "RUNNING"
    "PAUSED"
    "COMPLETE"
    "EXECUTOR_ERROR"
    "SYSTEM_ERROR"
    "CANCELED"
    "CANCELING"
    """

    def __init__(self, store: WorkflowStateStore) -> None:
        """
        Make a new state machine over the given state store slice for the
        workflow.
        """
        self._store = store

    def _set_base_state(self, state: str) -> None:
        """
        Set the base state (lowest priority) to the given state.
        """

        if state not in BASE_STATES:
            raise ValueError(f"Base state {state} is not in {BASE_STATES}")

        self._store.set("state", state)

    def _get_base_state(self) -> str:
        """
        Get the base state (lowest priority), which would be overridden by any
        flag state.
        """

        return self._store.get("state") or "UNKNOWN"

    def _set_flag_state(self, state: str) -> None:
        """
        Set a flag state. Overrides base state but not higher-priority flag
        states.
        """

        if state not in FLAG_STATES_IN_ORDER:
            raise ValueError(f"Flag state {state} is not in {FLAG_STATES_IN_ORDER}")

        self._store.set("state_" + state, "")

    def _get_flag_state(self, state: str) -> bool:
        """
        Return True if the flag for the given flag state is set.
        """

        if state not in FLAG_STATES_IN_ORDER:
            raise ValueError(f"Flag state {state} is not in {FLAG_STATES_IN_ORDER}")

        return self._store.get("state_" + state) is not None

    def send_enqueue(self) -> None:
        """
        Send an enqueue message that would move from UNKNOWN to QUEUED.
        """
        self._set_base_state("QUEUED")

    def send_initialize(self) -> None:
        """
        Send an initialize message that would move from QUEUED to INITIALIZING.
        """
        self._set_base_state("INITIALIZING")

    def send_run(self) -> None:
        """
        Send a run message that would move from INITIALIZING to RUNNING.
        """
        self._set_base_state("RUNNING")

    def send_cancel(self) -> None:
        """
        Send a cancel message that would move to CANCELING from any non-terminal state.
        """
        self._set_flag_state("CANCELING")

    def send_canceled(self) -> None:
        """
        Send a canceled message that would move to CANCELED from CANCELLING.
        """
        self._set_flag_state("CANCELED")

    def send_complete(self) -> None:
        """
        Send a complete message that would move from RUNNING to COMPLETE.
        """
        self._set_flag_state("COMPLETE")

    def send_executor_error(self) -> None:
        """
        Send an executor_error message that would move from QUERUED, INITIALIZING, or RUNNING to EXECUTOR_ERROR.
        """
        self._set_flag_state("EXECUTOR_ERROR")

    def send_system_error(self) -> None:
        """
        Send a system_error message that would move from QUERUED, INITIALIZING, or RUNNING to SYSTEM_ERROR.
        """
        self._set_flag_state("SYSTEM_ERROR")

    def get_current_state(self) -> str:
        """
        Get the current state of the workflow.
        """

        # We need to ensure only certain state transitions are observable, even
        # though all our writing is last-write-wins with no campare-and-swap.

        # So we have a priority of states.

        # First we need to check the lowest-priority base state
        observed_state = self._get_base_state()

        # Then we need to check the flags for each flag state in reverse
        # priority order, so the highest-priority thing is checked last.
        # Otherwise, we could observe a lower-priority state that was set after
        # a higher-priority state.

        for flag_state in reversed(FLAG_STATES_IN_ORDER):
            if self._get_flag_state(flag_state):
                # This flag is set
                observed_state = flag_state
                # But higher priority flags might still be set.

        # Now we know the state to report
        return observed_state



