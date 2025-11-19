import logging
import multiprocessing
import os
import random
import time
import traceback
from functools import partial
from pathlib import Path

from toil.lib.threading import LastProcessStandingArena, cpu_count, global_mutex

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
