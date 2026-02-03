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
barriers, allowing deterministic control over thread execution order. This
tests that the locking protocol is correct - the code acquires the right
locks at the right times.
"""

import fcntl
import os
import threading
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

# Time to wait between operations in interleaving tests. Must be long enough
# for threads to reach their synchronization points even on busy CI systems.
TICK_SECONDS = 2.0


def _server_available() -> bool:
    """Check if the server extra is installed."""
    try:
        import connexion  # noqa: F401

        return True
    except ImportError:
        return False


needs_server = pytest.mark.skipif(
    not _server_available(),
    reason="Install Toil with the 'server' extra to include this test.",
)


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


class LockManager:
    """Manages simulated locks for multiple files."""

    def __init__(self) -> None:
        self._locks: dict[str, SimulatedLock] = {}
        self._fd_to_path: dict[int, str] = {}
        self._global_lock = threading.Lock()

        # Barriers that tests can use to pause at specific points
        self.before_acquire: dict[str, threading.Event] = {}
        self.after_acquire: dict[str, threading.Event] = {}
        self.before_release: dict[str, threading.Event] = {}

    def register_fd(self, fd: int, path: str) -> None:
        """Associate a file descriptor with a path."""
        with self._global_lock:
            real_path = os.path.realpath(path)
            self._fd_to_path[fd] = real_path
            if real_path not in self._locks:
                self._locks[real_path] = SimulatedLock()

    def get_lock(self, fd: int) -> SimulatedLock | None:
        """Get the lock for a file descriptor."""
        with self._global_lock:
            path = self._fd_to_path.get(fd)
            if path:
                return self._locks.get(path)
        return None

    def get_lock_for_path(self, path: str) -> SimulatedLock | None:
        """Get the lock for a path."""
        real_path = os.path.realpath(path)
        with self._global_lock:
            return self._locks.get(real_path)

    def flock(self, fd: int, operation: int) -> None:
        """Simulated flock that respects barriers."""
        lock = self.get_lock(fd)
        if lock is None:
            return

        thread_name = threading.current_thread().name

        if operation == fcntl.LOCK_UN:
            barrier = self.before_release.get(thread_name)
            if barrier:
                barrier.wait()
            lock.release()

        elif operation == fcntl.LOCK_SH:
            barrier = self.before_acquire.get(thread_name)
            if barrier:
                barrier.wait()
            lock.acquire_shared()
            barrier = self.after_acquire.get(thread_name)
            if barrier:
                barrier.wait()

        elif operation == fcntl.LOCK_EX:
            barrier = self.before_acquire.get(thread_name)
            if barrier:
                barrier.wait()
            lock.acquire_exclusive()
            barrier = self.after_acquire.get(thread_name)
            if barrier:
                barrier.wait()


class FileOperationTracker:
    """
    Tracks and controls file operations with barriers.

    Wraps file objects to inject synchronization points during read/write.
    """

    def __init__(self, lock_manager: LockManager) -> None:
        self.lock_manager = lock_manager
        self.during_write: dict[str, threading.Event] = {}
        self.during_read: dict[str, threading.Event] = {}

    def wrap_file(self, file_obj: Any, path: str) -> Any:
        """Wrap a file object to track operations."""
        try:
            self.lock_manager.register_fd(file_obj.fileno(), path)
        except (OSError, ValueError):
            pass

        original_write = file_obj.write
        original_read = file_obj.read
        tracker = self

        def tracked_write(data: str) -> int:
            thread_name = threading.current_thread().name
            barrier = tracker.during_write.get(thread_name)
            if barrier:
                barrier.wait()
            return original_write(data)

        def tracked_read(size: int = -1) -> str:
            thread_name = threading.current_thread().name
            barrier = tracker.during_read.get(thread_name)
            if barrier:
                barrier.wait()
            return original_read(size)

        file_obj.write = tracked_write
        file_obj.read = tracked_read
        return file_obj


@needs_server
@pytest.mark.timeout(30)
class TestSafeFileInterleaving:
    """
    Tests that verify locking correctness through deterministic interleavings.

    Each test explicitly controls thread execution order using barriers to
    verify that locks are held and respected at the right times.
    """

    @pytest.fixture(autouse=True)
    def setup_managers(self, tmp_path: Path) -> Generator[None]:
        """Set up lock manager and file tracker for each test."""
        self.test_file = tmp_path / "test_file"
        self.lock_manager = LockManager()
        self.file_tracker = FileOperationTracker(self.lock_manager)
        yield

    def _create_patches(self) -> tuple[Any, Any]:
        """Create patch contexts for flock and open."""
        original_open = open
        lock_manager = self.lock_manager
        file_tracker = self.file_tracker

        def patched_open(path: Any, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
            f = original_open(path, mode, *args, **kwargs)
            return file_tracker.wrap_file(f, str(path))

        def patched_flock(fd: int, operation: int) -> None:
            lock_manager.flock(fd, operation)

        return (
            patch("builtins.open", patched_open),
            patch("toil.server.utils.fcntl.flock", patched_flock),
        )

    def test_reader_blocked_while_writer_holds_lock(self) -> None:
        """
        Verify that a reader cannot proceed while a writer holds the
        exclusive lock.

        Sequence:
        1. Writer acquires exclusive lock
        2. Writer is paused (holding lock)
        3. Reader tries to acquire shared lock
        4. Verify reader is blocked (cannot proceed)
        5. Release writer
        6. Verify reader proceeds and sees written content
        """
        from toil.server.utils import safe_read_file, safe_write_file

        # Create file with initial content
        self.test_file.write_text("original")

        # Barrier to pause writer after acquiring lock
        writer_after_lock = threading.Event()
        self.lock_manager.after_acquire["writer"] = writer_after_lock

        # Barrier for write operation (in case it's needed)
        writer_write_barrier = threading.Event()
        self.file_tracker.during_write["writer"] = writer_write_barrier

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

        patches = self._create_patches()
        with patches[0], patches[1]:
            # Start writer - will block on after_acquire barrier
            t_writer = threading.Thread(target=writer, name="writer")
            t_writer.start()

            # Wait for writer to acquire lock and hit barrier
            time.sleep(TICK_SECONDS)

            # Verify writer has the lock
            lock = self.lock_manager.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.has_exclusive, "Writer should hold exclusive lock"

            # Start reader - should block on lock acquisition
            t_reader = threading.Thread(target=reader, name="reader")
            t_reader.start()

            # Give reader time to try to acquire lock
            time.sleep(TICK_SECONDS)

            # Reader should NOT have completed (blocked on lock)
            assert not reader_completed.is_set(), (
                "Reader should be blocked while writer holds exclusive lock"
            )
            assert results["reader_result"] is None

            # Release writer barriers
            writer_after_lock.set()
            writer_write_barrier.set()

            # Wait for both to complete
            t_writer.join(timeout=TICK_SECONDS * 5)
            t_reader.join(timeout=TICK_SECONDS * 5)

        assert errors == []
        assert results["writer_done"]
        # Reader should see updated content (writer finished first)
        assert results["reader_result"] == "updated"

    def test_writer_blocked_while_reader_holds_lock(self) -> None:
        """
        Verify that a writer cannot proceed while a reader holds a
        shared lock.

        Sequence:
        1. Reader acquires shared lock
        2. Reader is paused (holding lock)
        3. Writer tries to acquire exclusive lock
        4. Verify writer is blocked
        5. Release reader
        6. Verify writer proceeds
        """
        from toil.server.utils import safe_read_file, safe_write_file

        # Create file with initial content
        self.test_file.write_text("original")

        # Barrier to pause reader after acquiring lock
        reader_after_lock = threading.Event()
        self.lock_manager.after_acquire["reader"] = reader_after_lock

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

        patches = self._create_patches()
        with patches[0], patches[1]:
            # Start reader - will block on after_acquire barrier
            t_reader = threading.Thread(target=reader, name="reader")
            t_reader.start()

            # Wait for reader to acquire lock and hit barrier
            time.sleep(TICK_SECONDS)

            # Verify reader has the lock
            lock = self.lock_manager.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.shared_count == 1, "Reader should hold shared lock"

            # Start writer - should block on lock acquisition
            t_writer = threading.Thread(target=writer, name="writer")
            t_writer.start()

            # Give writer time to try to acquire lock
            time.sleep(TICK_SECONDS)

            # Writer should NOT have completed (blocked on lock)
            assert not writer_completed.is_set(), (
                "Writer should be blocked while reader holds shared lock"
            )
            assert not results["writer_done"]

            # Release reader barrier
            reader_after_lock.set()

            # Wait for both to complete
            t_reader.join(timeout=TICK_SECONDS * 5)
            t_writer.join(timeout=TICK_SECONDS * 5)

        assert errors == []
        assert results["reader_result"] == "original"
        assert results["writer_done"]

    def test_multiple_readers_not_blocked(self) -> None:
        """
        Verify that multiple readers can hold shared locks simultaneously.

        Sequence:
        1. Reader1 acquires shared lock, pauses
        2. Reader2 acquires shared lock (should succeed immediately)
        3. Both readers hold locks simultaneously
        4. Release both, verify both complete
        """
        from toil.server.utils import safe_read_file

        # Create file
        self.test_file.write_text("content")

        # Barriers to pause both readers after acquiring locks
        reader1_after_lock = threading.Event()
        reader2_after_lock = threading.Event()
        self.lock_manager.after_acquire["reader1"] = reader1_after_lock
        self.lock_manager.after_acquire["reader2"] = reader2_after_lock

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

        patches = self._create_patches()
        with patches[0], patches[1]:
            # Start reader1
            t_reader1 = threading.Thread(target=reader1, name="reader1")
            t_reader1.start()

            # Wait for reader1 to acquire lock
            time.sleep(TICK_SECONDS)

            lock = self.lock_manager.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.shared_count == 1

            # Start reader2 - should also acquire shared lock (not blocked)
            t_reader2 = threading.Thread(target=reader2, name="reader2")
            t_reader2.start()

            # Give reader2 time to acquire lock
            time.sleep(TICK_SECONDS)

            # Both should hold shared locks simultaneously
            assert lock.shared_count == 2, (
                "Both readers should hold shared locks simultaneously"
            )

            # Release both barriers
            reader1_after_lock.set()
            reader2_after_lock.set()

            t_reader1.join(timeout=TICK_SECONDS * 5)
            t_reader2.join(timeout=TICK_SECONDS * 5)

        assert errors == []
        assert results["reader1"] == "content"
        assert results["reader2"] == "content"

    def test_writers_serialize(self) -> None:
        """
        Verify that two writers cannot hold exclusive locks simultaneously.

        Sequence:
        1. Writer1 acquires exclusive lock, pauses
        2. Writer2 tries to acquire exclusive lock
        3. Verify writer2 is blocked
        4. Release writer1
        5. Verify writer2 proceeds
        """
        from toil.server.utils import safe_write_file

        # Create file
        self.test_file.write_text("original")

        # Barrier to pause writer1 after acquiring lock
        writer1_after_lock = threading.Event()
        self.lock_manager.after_acquire["writer1"] = writer1_after_lock

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

        patches = self._create_patches()
        with patches[0], patches[1]:
            # Start writer1
            t_writer1 = threading.Thread(target=writer1, name="writer1")
            t_writer1.start()

            # Wait for writer1 to acquire lock
            time.sleep(TICK_SECONDS)

            lock = self.lock_manager.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.has_exclusive, "Writer1 should hold exclusive lock"

            # Start writer2 - should block
            t_writer2 = threading.Thread(target=writer2, name="writer2")
            t_writer2.start()

            # Give writer2 time to try to acquire
            time.sleep(TICK_SECONDS)

            # Writer2 should be blocked
            assert not writer2_completed.is_set(), (
                "Writer2 should be blocked while writer1 holds exclusive lock"
            )
            assert not results["writer2_done"]

            # Writer1 still holds exclusive lock
            assert lock.has_exclusive

            # Release writer1
            writer1_after_lock.set()

            # Wait for both
            t_writer1.join(timeout=TICK_SECONDS * 5)
            t_writer2.join(timeout=TICK_SECONDS * 5)

        assert errors == []
        assert results["writer1_done"]
        assert results["writer2_done"]

    def test_reader_never_sees_partial_write(self) -> None:
        """
        Verify that a reader cannot see partial write content.

        The lock ensures the reader either sees the old content or the
        complete new content, never content mid-write.

        Sequence:
        1. File has "AAAA"
        2. Writer acquires lock, pauses before writing
        3. Reader tries to read - should block
        4. Writer completes writing "BBBB"
        5. Writer releases lock
        6. Reader acquires lock, reads complete "BBBB"
        """
        from toil.server.utils import safe_read_file, safe_write_file

        # Create file with initial content
        self.test_file.write_text("AAAA")

        # Pause writer after acquiring lock (before any write)
        writer_after_lock = threading.Event()
        self.lock_manager.after_acquire["writer"] = writer_after_lock

        results: dict[str, Any] = {"reader_result": None}
        errors: list[Exception] = []

        def writer() -> None:
            try:
                safe_write_file(str(self.test_file), "BBBB")
            except Exception as e:
                errors.append(e)

        def reader() -> None:
            try:
                results["reader_result"] = safe_read_file(str(self.test_file))
            except Exception as e:
                errors.append(e)

        patches = self._create_patches()
        with patches[0], patches[1]:
            # Start writer
            t_writer = threading.Thread(target=writer, name="writer")
            t_writer.start()

            # Wait for writer to hold lock
            time.sleep(TICK_SECONDS)

            # Start reader while writer holds lock
            t_reader = threading.Thread(target=reader, name="reader")
            t_reader.start()

            # Give reader time to block
            time.sleep(TICK_SECONDS)

            # Reader should still be waiting (not completed)
            assert results["reader_result"] is None

            # Release writer to complete
            writer_after_lock.set()

            t_writer.join(timeout=TICK_SECONDS * 5)
            t_reader.join(timeout=TICK_SECONDS * 5)

        assert errors == []
        # Reader must see complete new content, never partial or mixed
        assert results["reader_result"] == "BBBB", (
            "Reader should see complete write, not partial"
        )

    def test_writer_paused_mid_write_blocks_reader(self) -> None:
        """
        Verify that a reader is blocked even when writer is paused during
        the actual write operation (not just after lock acquisition).

        This tests that the lock is held throughout the entire write.
        """
        from toil.server.utils import safe_read_file, safe_write_file

        # Create file with initial content
        self.test_file.write_text("original")

        # Pause writer during the write operation itself
        writer_during_write = threading.Event()
        self.file_tracker.during_write["writer"] = writer_during_write

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

        patches = self._create_patches()
        with patches[0], patches[1]:
            # Start writer - will block during write
            t_writer = threading.Thread(target=writer, name="writer")
            t_writer.start()

            # Wait for writer to reach the write barrier
            time.sleep(TICK_SECONDS)

            # Writer should hold exclusive lock while paused mid-write
            lock = self.lock_manager.get_lock_for_path(str(self.test_file))
            assert lock is not None
            assert lock.has_exclusive, "Writer should hold lock during write"

            # Start reader - should block
            t_reader = threading.Thread(target=reader, name="reader")
            t_reader.start()

            time.sleep(TICK_SECONDS)

            # Reader should NOT have completed
            assert not reader_completed.is_set(), (
                "Reader should be blocked while writer is mid-write"
            )

            # Release writer to finish
            writer_during_write.set()

            t_writer.join(timeout=TICK_SECONDS * 5)
            t_reader.join(timeout=TICK_SECONDS * 5)

        assert errors == []
        assert results["reader_result"] == "updated"
