# Copyright (C) 2015-2026 Regents of the University of California
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
"""
Tests for safe_read_file and safe_write_file that verify correct locking
behavior by testing specific interleavings.

Approach: Mock fcntl.flock and file I/O operations to inject synchronization
checkpoints, allowing deterministic control over thread execution order. This
tests that the locking protocol is correct - the code acquires the right
locks at the right times.

The tests use no sleeps - all synchronization is done via events/conditions.
"""

import builtins
import fcntl
import os
import threading
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

# Timeout for waiting on synchronization events. Should be long enough to
# never trigger in normal operation, but short enough to fail fast if
# something deadlocks.
SYNC_TIMEOUT = 10.0


class Checkpoint:
    """
    A synchronization point that allows a test to pause a thread and know
    when the thread has arrived.

    Usage:
        checkpoint = Checkpoint()

        # In worker thread:
        checkpoint.arrive_and_wait()  # signals arrival, then blocks

        # In test:
        checkpoint.wait_for_arrival()  # blocks until thread arrives
        # ... check state ...
        checkpoint.release()  # allows thread to proceed
    """

    def __init__(self) -> None:
        self._arrived = threading.Event()
        self._released = threading.Event()

    def arrive_and_wait(self, timeout: float = SYNC_TIMEOUT) -> bool:
        """
        Signal that we've arrived at the checkpoint, then wait for release.
        Returns True if released, False if timed out.
        """
        self._arrived.set()
        return self._released.wait(timeout=timeout)

    def wait_for_arrival(self, timeout: float = SYNC_TIMEOUT) -> bool:
        """
        Wait for a thread to arrive at this checkpoint.
        Returns True if arrived, False if timed out.
        """
        return self._arrived.wait(timeout=timeout)

    def release(self) -> None:
        """Release the thread waiting at this checkpoint."""
        self._released.set()

    def has_arrived(self) -> bool:
        """Check if a thread has arrived (non-blocking)."""
        return self._arrived.is_set()


# TODO: SimulatedLock doesn't track which threads hold shared locks.
# release() will decrement count even if called by a thread that doesn't hold
# a lock. Should use a set to track shared lock holders.
class SimulatedLock:
    """
    Simulates flock semantics using threading primitives.

    Supports shared (LOCK_SH) and exclusive (LOCK_EX) locks with proper
    blocking behavior:
    - Multiple shared locks can be held simultaneously
    - Exclusive lock blocks until all shared locks are released
    - Shared locks block while exclusive lock is held
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._shared_count = 0
        self._exclusive_held = False
        self._exclusive_holder: int | None = None

    def acquire_shared(self) -> None:
        """Acquire a shared lock (blocks if exclusive lock held)."""
        with self._condition:
            while self._exclusive_held:
                self._condition.wait()
            self._shared_count += 1

    def acquire_exclusive(self) -> None:
        """Acquire an exclusive lock (blocks if any lock held)."""
        thread_id = threading.current_thread().ident
        with self._condition:
            while self._exclusive_held or self._shared_count > 0:
                self._condition.wait()
            self._exclusive_held = True
            self._exclusive_holder = thread_id

    def release(self) -> None:
        """Release whatever lock this thread holds."""
        thread_id = threading.current_thread().ident
        with self._condition:
            if self._exclusive_held and self._exclusive_holder == thread_id:
                self._exclusive_held = False
                self._exclusive_holder = None
            elif self._shared_count > 0:
                self._shared_count -= 1
            self._condition.notify_all()

    @property
    def has_exclusive(self) -> bool:
        return self._exclusive_held

    @property
    def shared_count(self) -> int:
        return self._shared_count


class Checkpointer(ABC):
    """Base class for hooking checkpoints into operations."""

    def __init__(self) -> None:
        self._checkpoints: dict[str, Checkpoint] = {}

    def add(self, thread_name: str, checkpoint: Checkpoint) -> None:
        """Register a checkpoint for the given thread."""
        self._checkpoints[thread_name] = checkpoint

    def get(self, thread_name: str) -> Checkpoint | None:
        """Get checkpoint for thread, if any."""
        return self._checkpoints.get(thread_name)

    @abstractmethod
    @contextmanager
    def install(self) -> Generator[None, None, None]:
        """
        Install patches for this checkpointer.

        Each checkpointer provides its own context manager that patches the
        necessary functions. Multiple checkpointers compose by each capturing
        the current (possibly already-patched) functions.
        """
        ...


class LockCheckpointer(Checkpointer):
    """Checkpointer that pauses after flock acquisition."""

    def __init__(self) -> None:
        super().__init__()
        self._locks: dict[str, SimulatedLock] = {}
        self._fd_to_path: dict[int, str] = {}
        self._global_lock = threading.Lock()

    def _register_fd(self, fd: int, path: str) -> None:
        """Associate a file descriptor with a path."""
        with self._global_lock:
            real_path = os.path.realpath(path)
            self._fd_to_path[fd] = real_path
            if real_path not in self._locks:
                self._locks[real_path] = SimulatedLock()

    def _get_lock(self, fd: int) -> SimulatedLock | None:
        """Get the lock for a file descriptor."""
        with self._global_lock:
            path = self._fd_to_path.get(fd)
            if path:
                return self._locks.get(path)
        return None

    def get_lock_for_path(self, path: str) -> SimulatedLock | None:
        """Get the lock for a path (for test assertions)."""
        real_path = os.path.realpath(path)
        with self._global_lock:
            return self._locks.get(real_path)

    @contextmanager
    def install(self) -> Generator[None, None, None]:
        """Patch open to register fds, and patch flock with lock simulation."""
        original_open = builtins.open
        checkpointer = self

        def patched_open(
            path: Any, mode: str = "r", *args: Any, **kwargs: Any
        ) -> Any:
            f = original_open(path, mode, *args, **kwargs)
            try:
                checkpointer._register_fd(f.fileno(), str(path))
            except (OSError, ValueError):
                pass
            return f

        def patched_flock(fd: int, operation: int) -> None:
            lock = checkpointer._get_lock(fd)
            if lock is None:
                return

            thread_name = threading.current_thread().name

            if operation == fcntl.LOCK_UN:
                lock.release()
            elif operation == fcntl.LOCK_SH:
                lock.acquire_shared()
                checkpoint = checkpointer.get(thread_name)
                if checkpoint:
                    checkpoint.arrive_and_wait()
            elif operation == fcntl.LOCK_EX:
                lock.acquire_exclusive()
                checkpoint = checkpointer.get(thread_name)
                if checkpoint:
                    checkpoint.arrive_and_wait()

        with (
            patch("builtins.open", patched_open),
            patch("toil.server.utils.fcntl.flock", patched_flock),
        ):
            yield


class ReadCheckpointer(Checkpointer):
    """Checkpointer that pauses during file read."""

    @contextmanager
    def install(self) -> Generator[None, None, None]:
        """Patch open to wrap read operations with checkpoint hooks."""
        original_open = builtins.open
        checkpointer = self

        def patched_open(
            path: Any, mode: str = "r", *args: Any, **kwargs: Any
        ) -> Any:
            f = original_open(path, mode, *args, **kwargs)
            original_read = f.read

            def tracked_read(size: int = -1) -> str:
                thread_name = threading.current_thread().name
                checkpoint = checkpointer.get(thread_name)
                if checkpoint:
                    checkpoint.arrive_and_wait()
                return original_read(size)

            f.read = tracked_read
            return f

        with patch("builtins.open", patched_open):
            yield


class WriteCheckpointer(Checkpointer):
    """Checkpointer that pauses during file write."""

    @contextmanager
    def install(self) -> Generator[None, None, None]:
        """Patch open to wrap write operations with checkpoint hooks."""
        original_open = builtins.open
        checkpointer = self

        def patched_open(
            path: Any, mode: str = "r", *args: Any, **kwargs: Any
        ) -> Any:
            f = original_open(path, mode, *args, **kwargs)
            original_write = f.write

            def tracked_write(data: str) -> int:
                thread_name = threading.current_thread().name
                checkpoint = checkpointer.get(thread_name)
                if checkpoint:
                    checkpoint.arrive_and_wait()
                return original_write(data)

            f.write = tracked_write
            return f

        with patch("builtins.open", patched_open):
            yield


# TODO: Add tests for AtomicFileCreate path (concurrent new file creation).
# TODO: The 0.1s timeout waits to verify blocking are effectively sleeps;
# consider adding a checkpoint before lock acquisition to prove thread is
# actually blocked waiting for the lock.
@pytest.mark.timeout(30)
class TestSafeFileInterleaving:
    """
    Tests that verify locking correctness through deterministic interleavings.

    Each test explicitly controls thread execution order using checkpoints to
    verify that locks are held and respected at the right times. No sleeps
    are used - all synchronization is event-based.
    """

    @pytest.fixture(autouse=True)
    def setup_test_file(self, tmp_path: Path) -> Generator[None]:
        """Set up test file path for each test."""
        self.test_file = tmp_path / "test_file"
        yield

    @contextmanager
    def patched_io(
        self, *checkpointers: Checkpointer
    ) -> Generator[None, None, None]:
        """
        Context manager that installs all checkpointer patches.

        Uses ExitStack to compose the context managers from each checkpointer.
        Patches compose naturally since each captures the current open.
        """
        with ExitStack() as stack:
            for checkpointer in checkpointers:
                stack.enter_context(checkpointer.install())
            yield

    def test_reader_blocked_while_writer_holds_lock(self) -> None:
        """
        Verify that a reader cannot proceed while a writer holds the
        exclusive lock.

        Sequence:
        1. Writer acquires exclusive lock, arrives at checkpoint
        2. Test verifies writer has lock
        3. Reader tries to acquire shared lock (will block on simulated lock)
        4. Test verifies reader is blocked
        5. Test releases writer checkpoint
        6. Both complete, reader sees written content
        """
        from toil.server.utils import safe_read_file, safe_write_file

        self.test_file.write_text("original")

        writer_checkpoint = Checkpoint()
        locker = LockCheckpointer()
        locker.add("writer", writer_checkpoint)

        reader_completed = threading.Event()
        results: dict[str, Any] = {"writer_done": False, "reader_result": None}
        errors: list[Exception] = []

        def writer() -> None:
            try:
                safe_write_file(str(self.test_file), "updated")
                results["writer_done"] = True
            except Exception as e:
                errors.append(e)

        def reader() -> None:
            try:
                results["reader_result"] = safe_read_file(str(self.test_file))
                reader_completed.set()
            except Exception as e:
                errors.append(e)

        with self.patched_io(locker):
            t_writer = threading.Thread(target=writer, name="writer")
            t_writer.start()

            # Wait for writer to acquire lock and hit checkpoint
            assert writer_checkpoint.wait_for_arrival(), "Writer didn't reach checkpoint"

            # Verify writer has the lock
            lock = locker.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.has_exclusive, "Writer should hold exclusive lock"

            # Start reader - will block on simulated lock (not checkpoint)
            t_reader = threading.Thread(target=reader, name="reader")
            t_reader.start()

            # Reader should NOT complete while writer holds lock.
            # Give it a moment to try, then check it's still blocked.
            assert not reader_completed.wait(timeout=0.1), (
                "Reader should be blocked while writer holds exclusive lock"
            )
            assert results["reader_result"] is None

            # Release writer
            writer_checkpoint.release()

            t_writer.join(timeout=SYNC_TIMEOUT)
            t_reader.join(timeout=SYNC_TIMEOUT)

        assert errors == []
        assert results["writer_done"]
        assert results["reader_result"] == "updated"

    def test_writer_blocked_while_reader_holds_lock(self) -> None:
        """
        Verify that a writer cannot proceed while a reader holds a
        shared lock.
        """
        from toil.server.utils import safe_read_file, safe_write_file

        self.test_file.write_text("original")

        reader_checkpoint = Checkpoint()
        locker = LockCheckpointer()
        locker.add("reader", reader_checkpoint)

        writer_completed = threading.Event()
        results: dict[str, Any] = {"reader_result": None, "writer_done": False}
        errors: list[Exception] = []

        def reader() -> None:
            try:
                results["reader_result"] = safe_read_file(str(self.test_file))
            except Exception as e:
                errors.append(e)

        def writer() -> None:
            try:
                safe_write_file(str(self.test_file), "updated")
                results["writer_done"] = True
                writer_completed.set()
            except Exception as e:
                errors.append(e)

        with self.patched_io(locker):
            t_reader = threading.Thread(target=reader, name="reader")
            t_reader.start()

            assert reader_checkpoint.wait_for_arrival(), "Reader didn't reach checkpoint"

            lock = locker.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.shared_count == 1, "Reader should hold shared lock"

            t_writer = threading.Thread(target=writer, name="writer")
            t_writer.start()

            # Writer should be blocked
            assert not writer_completed.wait(timeout=0.1), (
                "Writer should be blocked while reader holds shared lock"
            )
            assert not results["writer_done"]

            reader_checkpoint.release()

            t_reader.join(timeout=SYNC_TIMEOUT)
            t_writer.join(timeout=SYNC_TIMEOUT)

        assert errors == []
        assert results["reader_result"] == "original"
        assert results["writer_done"]

    def test_multiple_readers_not_blocked(self) -> None:
        """
        Verify that multiple readers can hold shared locks simultaneously.
        """
        from toil.server.utils import safe_read_file

        self.test_file.write_text("content")

        reader1_checkpoint = Checkpoint()
        reader2_checkpoint = Checkpoint()
        locker = LockCheckpointer()
        locker.add("reader1", reader1_checkpoint)
        locker.add("reader2", reader2_checkpoint)

        results: dict[str, str | None] = {"reader1": None, "reader2": None}
        errors: list[Exception] = []

        def reader1() -> None:
            try:
                results["reader1"] = safe_read_file(str(self.test_file))
            except Exception as e:
                errors.append(e)

        def reader2() -> None:
            try:
                results["reader2"] = safe_read_file(str(self.test_file))
            except Exception as e:
                errors.append(e)

        with self.patched_io(locker):
            t_reader1 = threading.Thread(target=reader1, name="reader1")
            t_reader1.start()

            assert reader1_checkpoint.wait_for_arrival(), "Reader1 didn't reach checkpoint"

            lock = locker.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.shared_count == 1

            # Reader2 should also acquire shared lock (not blocked by reader1)
            t_reader2 = threading.Thread(target=reader2, name="reader2")
            t_reader2.start()

            # Reader2 should reach its checkpoint (proving it got the lock)
            assert reader2_checkpoint.wait_for_arrival(), (
                "Reader2 should acquire shared lock while reader1 holds one"
            )

            # Both should hold shared locks simultaneously
            assert lock.shared_count == 2, (
                "Both readers should hold shared locks simultaneously"
            )

            reader1_checkpoint.release()
            reader2_checkpoint.release()

            t_reader1.join(timeout=SYNC_TIMEOUT)
            t_reader2.join(timeout=SYNC_TIMEOUT)

        assert errors == []
        assert results["reader1"] == "content"
        assert results["reader2"] == "content"

    def test_writers_serialize(self) -> None:
        """
        Verify that two writers cannot hold exclusive locks simultaneously.
        """
        from toil.server.utils import safe_write_file

        self.test_file.write_text("original")

        writer1_checkpoint = Checkpoint()
        locker = LockCheckpointer()
        locker.add("writer1", writer1_checkpoint)

        writer2_completed = threading.Event()
        results: dict[str, bool] = {"writer1_done": False, "writer2_done": False}
        errors: list[Exception] = []

        def writer1() -> None:
            try:
                safe_write_file(str(self.test_file), "from_writer1")
                results["writer1_done"] = True
            except Exception as e:
                errors.append(e)

        def writer2() -> None:
            try:
                safe_write_file(str(self.test_file), "from_writer2")
                results["writer2_done"] = True
                writer2_completed.set()
            except Exception as e:
                errors.append(e)

        with self.patched_io(locker):
            t_writer1 = threading.Thread(target=writer1, name="writer1")
            t_writer1.start()

            assert writer1_checkpoint.wait_for_arrival(), "Writer1 didn't reach checkpoint"

            lock = locker.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.has_exclusive, "Writer1 should hold exclusive lock"

            t_writer2 = threading.Thread(target=writer2, name="writer2")
            t_writer2.start()

            # Writer2 should be blocked
            assert not writer2_completed.wait(timeout=0.1), (
                "Writer2 should be blocked while writer1 holds exclusive lock"
            )
            assert not results["writer2_done"]
            assert lock.has_exclusive  # Still held by writer1

            writer1_checkpoint.release()

            t_writer1.join(timeout=SYNC_TIMEOUT)
            t_writer2.join(timeout=SYNC_TIMEOUT)

        assert errors == []
        assert results["writer1_done"]
        assert results["writer2_done"]

    def test_reader_paused_mid_read_blocks_writer(self) -> None:
        """
        Verify that a writer is blocked even when reader is paused during
        the actual read operation (not just after lock acquisition).
        """
        from toil.server.utils import safe_read_file, safe_write_file

        self.test_file.write_text("original")

        # Pause reader during the read operation itself
        read_checkpoint = Checkpoint()
        read_hooker = ReadCheckpointer()
        read_hooker.add("reader", read_checkpoint)

        # Also need lock checkpointer for fd registration and lock simulation
        locker = LockCheckpointer()

        writer_completed = threading.Event()
        results: dict[str, Any] = {"reader_result": None}
        errors: list[Exception] = []

        def reader() -> None:
            try:
                results["reader_result"] = safe_read_file(str(self.test_file))
            except Exception as e:
                errors.append(e)

        def writer() -> None:
            try:
                safe_write_file(str(self.test_file), "updated")
                writer_completed.set()
            except Exception as e:
                errors.append(e)

        with self.patched_io(locker, read_hooker):
            t_reader = threading.Thread(target=reader, name="reader")
            t_reader.start()

            assert read_checkpoint.wait_for_arrival(), "Reader didn't reach read checkpoint"

            # Reader should hold shared lock while paused mid-read
            lock = locker.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.shared_count == 1, "Reader should hold lock during read"

            t_writer = threading.Thread(target=writer, name="writer")
            t_writer.start()

            # Writer should be blocked
            assert not writer_completed.wait(timeout=0.1), (
                "Writer should be blocked while reader is mid-read"
            )

            read_checkpoint.release()

            t_reader.join(timeout=SYNC_TIMEOUT)
            t_writer.join(timeout=SYNC_TIMEOUT)

        assert errors == []
        assert results["reader_result"] == "original"

    def test_writer_paused_mid_write_blocks_reader(self) -> None:
        """
        Verify that a reader is blocked even when writer is paused during
        the actual write operation (not just after lock acquisition).
        """
        from toil.server.utils import safe_read_file, safe_write_file

        self.test_file.write_text("original")

        # Pause writer during the write operation itself
        write_checkpoint = Checkpoint()
        write_hooker = WriteCheckpointer()
        write_hooker.add("writer", write_checkpoint)

        # Also need lock checkpointer for fd registration and lock simulation
        locker = LockCheckpointer()

        reader_completed = threading.Event()
        results: dict[str, Any] = {"reader_result": None}
        errors: list[Exception] = []

        def writer() -> None:
            try:
                safe_write_file(str(self.test_file), "updated")
            except Exception as e:
                errors.append(e)

        def reader() -> None:
            try:
                results["reader_result"] = safe_read_file(str(self.test_file))
                reader_completed.set()
            except Exception as e:
                errors.append(e)

        with self.patched_io(locker, write_hooker):
            t_writer = threading.Thread(target=writer, name="writer")
            t_writer.start()

            assert write_checkpoint.wait_for_arrival(), "Writer didn't reach write checkpoint"

            # Writer should hold exclusive lock while paused mid-write
            lock = locker.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.has_exclusive, "Writer should hold lock during write"

            t_reader = threading.Thread(target=reader, name="reader")
            t_reader.start()

            # Reader should be blocked
            assert not reader_completed.wait(timeout=0.1), (
                "Reader should be blocked while writer is mid-write"
            )

            write_checkpoint.release()

            t_writer.join(timeout=SYNC_TIMEOUT)
            t_reader.join(timeout=SYNC_TIMEOUT)

        assert errors == []
        assert results["reader_result"] == "updated"
