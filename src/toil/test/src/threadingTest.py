import logging
import multiprocessing
import os
import random
import time
import traceback
from functools import partial
from pathlib import Path
import errno

from toil.lib.threading import LastProcessStandingArena, cpu_count, global_mutex, safe_lock, safe_unlock_and_close
from unittest.mock import patch

log = logging.getLogger(__name__)


class TestThreading:
    """Test Toil threading/synchronization tools."""

    def testGlobalMutexOrdering(self, tmp_path: Path) -> None:
        for it in range(10):
            log.info("Iteration %d", it)

            scope = tmp_path / f"tempDir{it}"
            scope.mkdir()
            mutex = "mutex"
            # Use processes (as opposed to threads) to prevent GIL from ordering things artificially
            pool = multiprocessing.Pool(processes=cpu_count())
            try:
                numTasks = 100
                temp_results = pool.map_async(
                    func=partial(_testGlobalMutexOrderingTask, scope, mutex),
                    iterable=list(range(numTasks)),
                )
                results = temp_results.get()
            finally:
                pool.close()
                pool.join()

            assert len(results) == numTasks
            for item in results:
                # Make sure all workers say they succeeded
                assert item is True

    def testLastProcessStanding(self, tmp_path: Path) -> None:
        for it in range(10):
            log.info("Iteration %d", it)

            scope = tmp_path / f"tempDir{it}"
            scope.mkdir()
            arena_name = "thunderdome"
            # Use processes (as opposed to threads) to prevent GIL from ordering things artificially
            pool = multiprocessing.Pool(processes=cpu_count())
            try:
                numTasks = 100
                temp_results = pool.map_async(
                    func=partial(_testLastProcessStandingTask, scope, arena_name),
                    iterable=list(range(numTasks)),
                )
                results = temp_results.get()
            finally:
                pool.close()
                pool.join()

            assert len(results) == numTasks
            for item in results:
                # Make sure all workers say they succeeded
                assert item is True
            for filename in os.listdir(scope):
                assert not filename.startswith(
                    "precious"
                ), f"File {filename} still exists"
    
class BaseSafeLockingTest:
    """
    Base class for testing retry and error-swallowing behavior in safe_lock
    and safe_unlock_and_close. Subclasses provide the specific OSError to test
    by implementing get_error().
    """
    def get_error(self) -> OSError:
        """Return the OSError to simulate in tests. Must be implemented by subclasses."""
        raise NotImplementedError

    def test_safe_lock_retries(self) -> None:
        """safe_lock should retry on a transient error and succeed on the second attempt."""
        error = self.get_error()
        # First call raises error, second call succeeds
        with patch("fcntl.flock", side_effect=[error, None]) as mock_flock:
            safe_lock(0)
            assert mock_flock.call_count == 2
    
    def test_safe_lock_fails_after_max_retries(self) -> None:
        """safe_lock should raise OSError after exhausting all retries."""
        error = self.get_error()
        # First call raises error, second call succeeds
        with patch("fcntl.flock", side_effect=error):
            with patch("toil.lib.threading.time.sleep"):  # skip the backoff waits
                try:
                    safe_lock(0)
                    assert False, "Expected OSError to be raised"
                except OSError as e:
                    assert e.errno == error.errno
    
    def test_safe_unlock_and_close_swallows(self) -> None:
        """safe_unlock_and_close should swallow the error and still close the fd."""
        error = self.get_error()
        # First call raises error, second call succeeds
        with patch("fcntl.flock", side_effect=error):
            with patch("os.close") as mock_close:
                safe_unlock_and_close(0)
                mock_close.assert_called_once_with(0)

class TestENOLCKSafeLocking(BaseSafeLockingTest):
    """Tests safe_lock and safe_unlock_and_close behavior when fcntl raises ENOLCK (NFS lockd unavailable)."""
    def get_error(self) -> OSError:
        return OSError(errno.ENOLCK, "No locks available")

class TestEIOSafeLocking(BaseSafeLockingTest):
    """Tests safe_lock and safe_unlock_and_close behavior when fcntl raises EIO (Ceph IO error)."""
    def get_error(self) -> OSError:
        return OSError(errno.EIO, "Input/Output Error")

def _testGlobalMutexOrderingTask(scope: Path, mutex: str, number: int) -> bool:
    try:
        # We will all fight over the potato
        potato = scope / "potato"

        with global_mutex(scope, mutex):
            log.info("PID %d = num %d running", os.getpid(), number)
            assert not potato.exists(), "We see someone else holding the potato file"

            # Put our name there
            with potato.open("w") as out_stream:
                out_stream.write(str(number))

            # Wait
            time.sleep(random.random() * 0.01)

            # Make sure our name is still there
            with potato.open() as in_stream:
                seen = in_stream.read().rstrip()
                assert seen == str(
                    number
                ), f"We are {number} but {seen} stole our potato!"

            potato.unlink()
            assert not potato.exists(), "We left the potato behind"
            log.info("PID %d = num %d dropped potato", os.getpid(), number)
        return True
    except:
        traceback.print_exc()
        return False


def _testLastProcessStandingTask(scope: Path, arena_name: str, number: int) -> bool:
    try:
        arena = LastProcessStandingArena(scope, arena_name)

        arena.enter()
        log.info("PID %d = num %d entered arena", os.getpid(), number)
        try:
            # We all make files
            my_precious = scope / f"precious{number}"

            # Put our name there
            with my_precious.open("w") as out_stream:
                out_stream.write(str(number))

            # Wait
            time.sleep(random.random() * 0.01)

            # Make sure our file is still there unmodified
            assert my_precious.exists(), f"Precious file {my_precious} has been stolen!"
            with my_precious.open() as in_stream:
                seen = in_stream.read().rstrip()
                assert seen == str(
                    number
                ), f"We are {number} but saw {seen} in our precious file!"
        finally:
            was_last = False
            for _ in arena.leave():
                log.info("PID %d = num %d is last standing", os.getpid(), number)

                # Clean up all the files
                for filename in os.listdir(scope):
                    if filename.startswith("precious"):
                        log.info(
                            "PID %d = num %d cleaning up %s",
                            os.getpid(),
                            number,
                            filename,
                        )
                        (scope / filename).unlink()

            log.info("PID %d = num %d left arena", os.getpid(), number)

        return True
    except:
        traceback.print_exc()
        return False
