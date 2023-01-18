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
import logging
import os
from abc import abstractmethod
from datetime import datetime
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse

import requests

from toil.lib.io import AtomicFileCreate
from toil.lib.retry import retry

try:
    from toil.lib.aws import get_current_aws_region
    from toil.lib.aws.session import client
    from toil.lib.aws.utils import retry_s3
    HAVE_S3 = True
except ImportError:
    HAVE_S3 = False

logger = logging.getLogger(__name__)

def get_iso_time() -> str:
    """
    Return the current time in ISO 8601 format.
    """
    return datetime.now().isoformat()


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
        file_obj = open(file)
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
        # Going to overwrite without anyone else being able to see this
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

class MemoryStateCache:
    """
    An in-memory place to store workflow state.
    """

    def __init__(self) -> None:
        """
        Make a new in-memory state cache.
        """

        super().__init__()
        self._data: Dict[Tuple[str, str], Optional[str]] = {}

    def get(self, workflow_id: str, key: str) -> Optional[str]:
        """
        Get a key value from memory.
        """
        return self._data.get((workflow_id, key))

    def set(self, workflow_id: str, key: str, value: Optional[str]) -> None:
        """
        Set or clear a key value in memory.
        """

        if value is None:
            try:
                del self._data[(workflow_id, key)]
            except KeyError:
                pass
        else:
            self._data[(workflow_id, key)] = value

class AbstractStateStore:
    """
    A place for the WES server to keep its state: the set of workflows that
    exist and whether they are done or not.

    This is a key-value store, with keys namespaced by workflow ID. Concurrent
    access from multiple threads or processes is safe and globally consistent.

    Keys and workflow IDs are restricted to [-a-zA-Z0-9_], because backends may
    use them as path or URL components.

    Key values are either a string, or None if the key is not set.

    Workflow existence isn't a thing; nonexistent workflows just have None for
    all keys.

    Note that we don't yet have a cleanup operation: things are stored
    permanently. Even clearing all the keys may leave data behind.

    Also handles storage for a local cache, with a separate key namespace (not
    a read/write-through cache).

    TODO: Can we replace this with just using a JobStore eventually, when
    AWSJobStore no longer needs SimpleDB?
    """

    def __init__(self):
        """
        Set up the AbstractStateStore and its cache.
        """

        # We maintain a local cache here.
        # TODO: Upgrade to an LRU cache wehn we finally learn to paginate
        # workflow status
        self._cache = MemoryStateCache()

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

    def read_cache(self, workflow_id: str, key: str) -> Optional[str]:
        """
        Read a value from a local cache, without checking the actual backend.
        """

        return self._cache.get(workflow_id, key)

    def write_cache(self, workflow_id: str, key: str, value: Optional[str]) -> None:
        """
        Write a value to a local cache, without modifying the actual backend.
        """
        self._cache.set(workflow_id, key, value)

class MemoryStateStore(MemoryStateCache, AbstractStateStore):
    """
    An in-memory place to store workflow state, for testing.

    Inherits from MemoryStateCache first to provide implementations for
    AbstractStateStore.
    """

    def __init__(self):
        super().__init__()

class FileStateStore(AbstractStateStore):
    """
    A place to store workflow state that uses a POSIX-compatible file system.
    """

    def __init__(self, url: str) -> None:
        """
        Connect to the state store in the given local directory.

        :param url: Local state store path. Interpreted as a URL, so can't
                    contain ? or #.
        """
        super().__init__()
        parse = urlparse(url)
        if parse.scheme.lower() not in ['file', '']:
            # We want to catch if we get the wrong argument.
            raise RuntimeError(f"{url} doesn't look like a local path")
        if not os.path.exists(parse.path):
            # We need this directory to exist.
            os.makedirs(parse.path, exist_ok=True)
        logger.debug("Connected to FileStateStore at %s", url)
        self._base_dir = parse.path

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

if HAVE_S3:
    class S3StateStore(AbstractStateStore):
        """
        A place to store workflow state that uses an S3-compatible object store.
        """

        def __init__(self, url: str) -> None:
            """
            Connect to the state store in the given S3 URL.

            :param url: An S3 URL to a prefix. Interpreted as a URL, so can't
                        contain ? or #.
            """

            super().__init__()

            parse = urlparse(url)

            if parse.scheme.lower() != 's3':
                # We want to catch if we get the wrong argument.
                raise RuntimeError(f"{url} doesn't look like an S3 URL")

            self._bucket = parse.netloc
            # urlparse keeps the leading '/', but here we want a path in the
            # bucket without a leading '/'. We also need to support an empty
            # path.
            self._base_path = parse.path[1:] if parse.path.startswith('/') else parse.path
            self._client = client('s3', region_name=get_current_aws_region())

            logger.debug("Connected to S3StateStore at %s", url)

        def _get_bucket_and_path(self, workflow_id: str, key: str) -> Tuple[str, str]:
            """
            Get the bucket and path in the bucket at which a key value belongs.
            """
            path = os.path.join(self._base_path, workflow_id, key)
            return self._bucket, path

        def get(self, workflow_id: str, key: str) -> Optional[str]:
            """
            Get a key value from S3.
            """
            bucket, path = self._get_bucket_and_path(workflow_id, key)
            for attempt in retry_s3():
                try:
                    logger.debug('Fetch %s path %s', bucket, path)
                    response = self._client.get_object(Bucket=bucket, Key=path)
                    return response['Body'].read().decode('utf-8')
                except self._client.exceptions.NoSuchKey:
                    return None


        def set(self, workflow_id: str, key: str, value: Optional[str]) -> None:
            """
            Set or clear a key value on S3.
            """
            bucket, path = self._get_bucket_and_path(workflow_id, key)
            for attempt in retry_s3():
                if value is None:
                    # Get rid of it.
                    logger.debug('Clear %s path %s', bucket, path)
                    self._client.delete_object(Bucket=bucket, Key=path)
                    return
                else:
                    # Store it, clobbering anything there already.
                    logger.debug('Set %s path %s', bucket, path)
                    self._client.put_object(Bucket=bucket, Key=path,
                                            Body=value.encode('utf-8'))
                    return

# We want to memoize state stores so we can cache on them.
state_store_cache: Dict[str, AbstractStateStore] = {}

def connect_to_state_store(url: str) -> AbstractStateStore:
    """
    Connect to a place to store state for workflows, defined by a URL.

    URL may be a local file path or URL or an S3 URL.
    """

    if url not in state_store_cache:
        # We need to actually make the state store
        parse = urlparse(url)
        if parse.scheme.lower() == 's3':
            # It's an S3 URL
            if HAVE_S3:
                # And we can use S3, so make the right implementation for S3.
                state_store_cache[url] = S3StateStore(url)
            else:
                # We can't actually use S3, so complain.
                raise RuntimeError(f'Cannot connect to {url} because Toil AWS '
                                   f'dependencies are not available. Did you '
                                   f'install Toil with the [aws] extra?')
        elif parse.scheme.lower() in ['file', '']:
            # It's a file URL or path
            state_store_cache[url] = FileStateStore(url)
        else:
            raise RuntimeError(f'Cannot connect to {url} because we do not '
                               f'implement its URL scheme')

    return state_store_cache[url]

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

    def set(self, key: str, value: Optional[str]) -> None:
        """
        Set the given item of workflow state.
        """
        self._state_store.set(self._workflow_id, key, value)

    def read_cache(self, key: str) -> Optional[str]:
        """
        Read a value from a local cache, without checking the actual backend.
        """

        return self._state_store.read_cache(self._workflow_id, key)

    def write_cache(self, key: str, value: Optional[str]) -> None:
        """
        Write a value to a local cache, without modifying the actual backend.
        """

        self._state_store.write_cache(self._workflow_id, key, value)


def connect_to_workflow_state_store(url: str, workflow_id: str) -> WorkflowStateStore:
    """
    Connect to a place to store state for the given workflow, in the state
    store defined by the given URL.

    :param url: A URL that can be used for connect_to_state_store()
    """

    return WorkflowStateStore(connect_to_state_store(url), workflow_id)

# When we see one of these terminal states, we stay there forever.
TERMINAL_STATES = {"COMPLETE", "EXECUTOR_ERROR", "SYSTEM_ERROR", "CANCELED"}

# How long can a workflow be in CANCELING state before we conclude that the
# workflow running task is gone and move it to CANCELED?
MAX_CANCELING_SECONDS = 30

class WorkflowStateMachine:
    """
    Class for managing the WES workflow state machine.

    This is the authority on the WES "state" of a workflow. You need one to
    read or change the state.

    Guaranteeing that only certain transitions can be observed is possible but
    not worth it. Instead, we just let updates clobber each other and grab and
    cache the first terminal state we see forever. If it becomes important that
    clients never see e.g. CANCELED -> COMPLETE or COMPLETE -> SYSTEM_ERROR, we
    can implement a real distributed state machine here.

    We do handle making sure that tasks don't get stuck in CANCELING.

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

    Uses the state store's local cache to prevent needing to read things we've
    seen already.
    """

    def __init__(self, store: WorkflowStateStore) -> None:
        """
        Make a new state machine over the given state store slice for the
        workflow.
        """
        self._store = store

    def _set_state(self, state: str) -> None:
        """
        Set the state to the given value, if a read does not show a terminal
        state already.
        We still might miss and clobber transitions to terminal states between
        the read and the write.
        This is not really consistent but also not worth protecting against.
        """

        if self.get_current_state() not in TERMINAL_STATES:
            self._store.set("state", state)

    def send_enqueue(self) -> None:
        """
        Send an enqueue message that would move from UNKNOWN to QUEUED.
        """
        self._set_state("QUEUED")

    def send_initialize(self) -> None:
        """
        Send an initialize message that would move from QUEUED to INITIALIZING.
        """
        self._set_state("INITIALIZING")

    def send_run(self) -> None:
        """
        Send a run message that would move from INITIALIZING to RUNNING.
        """
        self._set_state("RUNNING")

    def send_cancel(self) -> None:
        """
        Send a cancel message that would move to CANCELING from any
        non-terminal state.
        """

        state = self.get_current_state()
        if state != "CANCELING" and state not in TERMINAL_STATES:
            # If it's not obvious we shouldn't cancel, cancel.

            # If we end up in CANCELING but the workflow runner task isn't around,
            # or we signal it at the wrong time, we will stay there forever,
            # because it's responsible for setting the state to anything else.
            # So, we save a timestamp, and if we see a CANCELING status and an old
            # timestamp, we move on.
            self._store.set("cancel_time", get_iso_time())
            # Set state after time, because having the state but no time is an error.
            self._store.set("state", "CANCELING")

    def send_canceled(self) -> None:
        """
        Send a canceled message that would move to CANCELED from CANCELLING.
        """
        self._set_state("CANCELED")

    def send_complete(self) -> None:
        """
        Send a complete message that would move from RUNNING to COMPLETE.
        """
        self._set_state("COMPLETE")

    def send_executor_error(self) -> None:
        """
        Send an executor_error message that would move from QUEUED,
        INITIALIZING, or RUNNING to EXECUTOR_ERROR.
        """
        self._set_state("EXECUTOR_ERROR")

    def send_system_error(self) -> None:
        """
        Send a system_error message that would move from QUEUED, INITIALIZING,
            or RUNNING to SYSTEM_ERROR.
        """
        self._set_state("SYSTEM_ERROR")

    def get_current_state(self) -> str:
        """
        Get the current state of the workflow.
        """

        state = self._store.read_cache("state")
        if state is not None:
            # We permanently cached a terminal state
            return state

        # Otherwise do an actual read from backing storage.
        state = self._store.get("state")

        if state == "CANCELING":
            # Make sure it hasn't been CANCELING for too long.
            # We can get stuck in CANCELING if the workflow-running task goes
            # away or is stopped while reporting back, because it is
            # repsonsible for posting back that it has been successfully
            # canceled.
            canceled_at = self._store.get("cancel_time")
            if canceled_at is None:
                # If there's no timestamp but it's supposedly canceling, put it
                # into SYSTEM_ERROR, because we didn;t move to CANCELING properly.
                state = "SYSTEM_ERROR"
                self._store.set("state", state)
            else:
                # See if it has been stuck canceling for too long
                canceled_at = datetime.fromisoformat(canceled_at)
                canceling_seconds = (datetime.now() - canceled_at).total_seconds()
                if canceling_seconds > MAX_CANCELING_SECONDS:
                    # If it has, go to CANCELED instead, because the task is
                    # nonresponsive and thus not running.
                    state = "CANCELED"
                    self._store.set("state", state)

        if state in TERMINAL_STATES:
            # We can cache this state forever
            self._store.write_cache("state", state)

        if state is None:
            # Make sure we fill in if we couldn't fetch a stored state.
            state = "UNKNOWN"

        return state


