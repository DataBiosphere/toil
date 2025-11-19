import errno
import multiprocessing
import os
from functools import partial
from pathlib import Path
from typing import Any

from toil.lib.io import mkdtemp
from toil.lib.threading import cpu_count


class TestSystem:
    """Test various assumptions about the operating system's behavior."""

    def testAtomicityOfNonEmptyDirectoryRenames(self, tmp_path: Path) -> None:
        for _ in range(100):
            parent = tmp_path / f"parent{_}"
            parent.mkdir()
            child = parent / "child"
            # Use processes (as opposed to threads) to prevent GIL from ordering things artificially
            pool = multiprocessing.Pool(processes=cpu_count())
            try:
                numTasks = cpu_count() * 10
                temp_grandChildIds = pool.map_async(
                    func=partial(
                        _testAtomicityOfNonEmptyDirectoryRenamesTask, parent, child
                    ),
                    iterable=list(range(numTasks)),
                )
                grandChildIds = temp_grandChildIds.get()
            finally:
                pool.close()
                pool.join()
            assert len(grandChildIds) == numTasks
            # Assert that we only had one winner
            grandChildIds = [n for n in grandChildIds if n is not None]
            assert len(grandChildIds) == 1
            # Assert that the winner's grandChild wasn't silently overwritten by a looser
            expectedGrandChildId = grandChildIds[0]
            actualGrandChild = child / "grandChild"
            actualGrandChildId = actualGrandChild.stat().st_ino
            assert actualGrandChildId == expectedGrandChildId


def _testAtomicityOfNonEmptyDirectoryRenamesTask(
    parent: Path, child: Path, _: Any
) -> int | None:
    tmpChildDir = mkdtemp(dir=parent, prefix="child", suffix=".tmp")
    grandChild = os.path.join(tmpChildDir, "grandChild")
    open(grandChild, "w").close()
    grandChildId = os.stat(grandChild).st_ino
    try:
        os.rename(tmpChildDir, child)
    except OSError as e:
        if e.errno == errno.ENOTEMPTY or e.errno == errno.EEXIST:
            os.unlink(grandChild)
            os.rmdir(tmpChildDir)
            return None
        else:
            raise
    else:
        # We won the race
        return grandChildId
