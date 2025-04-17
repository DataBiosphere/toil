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
import inspect
import logging
import os
import re
from pathlib import Path
import random
import sys
from types import FrameType
from uuid import uuid4
from typing import cast, Union

from toil.common import getNodeID
from toil.lib.exceptions import panic, raise_
from toil.lib.io import AtomicFileCreate, atomic_install, atomic_tmp_file, mkdtemp
from toil.lib.misc import CalledProcessErrorStderr, StrPath, call_command
from toil.test import pslow as slow

import pytest

log = logging.getLogger(__name__)
logging.basicConfig()


class TestMisc:
    """
    This class contains miscellaneous tests that don't have enough content to be their own test
    file, and that don't logically fit in with any of the other test suites.
    """

    def testIDStability(self) -> None:
        prevNodeID = None
        for i in range(10, 1):
            nodeID = getNodeID()
            assert nodeID == prevNodeID
            prevNodeID = nodeID

    @slow
    @pytest.mark.slow
    def testGetSizeOfDirectoryWorks(self, tmp_path: Path) -> None:
        """A test to make sure toil.common.getDirSizeRecursively does not
        underestimate the amount of disk space needed.

        Disk space allocation varies from system to system.  The computed value
        should always be equal to or slightly greater than the creation value.
        This test generates a number of random directories and randomly sized
        files to test this using getDirSizeRecursively.
        """
        from toil.common import getDirSizeRecursively

        # a list of the directories used in the test
        directories: list[StrPath] = [tmp_path]
        # A dict of {FILENAME: FILESIZE or FILELINK} for all files used in the test
        files: dict[Path, Union[int, str]] = {}
        # Create a random directory structure
        for i in range(0, 10):
            directories.append(mkdtemp(dir=random.choice(directories), prefix="test"))
        # Create 50 random file entries in different locations in the directories. 75% of the time
        # these are fresh files of size [1, 10] MB and 25% of the time they are hard links to old
        # files.
        while len(files) <= 50:
            fileName = Path(random.choice(directories)) / self._getRandomName()
            if random.randint(0, 100) < 75:
                # Create a fresh file in the range of 1-10 MB
                fileSize = int(round(random.random(), 2) * 10 * 1024 * 1024)
                with fileName.open("wb") as fileHandle:
                    fileHandle.write(os.urandom(fileSize))
                files[fileName] = fileSize
            else:
                # Link to one of the previous files
                if len(files) == 0:
                    continue
                linkSrc = random.choice(list(files.keys()))
                os.link(linkSrc, fileName)
                files[fileName] = "Link to %s" % linkSrc

        computedDirectorySize = getDirSizeRecursively(tmp_path)
        totalExpectedSize = sum(x for x in list(files.values()) if isinstance(x, int))
        assert computedDirectorySize >= totalExpectedSize

    @staticmethod
    def _getRandomName() -> str:
        return str(uuid4().hex)

    def _get_test_out_file(self, testDir: Path, tail: str) -> Path:
        outf = testDir / f"test.{tail}"
        if outf.exists():
            outf.unlink()
        return outf

    def _write_test_file(self, outf_tmp: str) -> None:
        with open(outf_tmp, "w") as fh:
            fh.write(Path(outf_tmp).as_uri() + "\n")

    def test_atomic_install(self, tmp_path: Path) -> None:
        outf = self._get_test_out_file(tmp_path, ".foo.gz")
        outf_tmp = atomic_tmp_file(outf)
        self._write_test_file(outf_tmp)
        atomic_install(outf_tmp, outf)
        assert outf.exists()

    def test_atomic_install_dev(self) -> None:
        devn = "/dev/null"
        tmp = atomic_tmp_file(devn)
        assert tmp == devn
        atomic_install(tmp, devn)

    def test_atomic_context_ok(self, tmp_path: Path) -> None:
        outf = self._get_test_out_file(tmp_path, ".tar")
        with AtomicFileCreate(outf) as outf_tmp:
            self._write_test_file(outf_tmp)
        assert outf.exists()

    def test_atomic_context_error(self, tmp_path: Path) -> None:
        outf = self._get_test_out_file(tmp_path, ".tar")
        try:
            with AtomicFileCreate(outf) as outf_tmp:
                self._write_test_file(outf_tmp)
                raise Exception("stop!")
        except Exception as ex:
            assert str(ex) == "stop!"
        assert not os.path.exists(outf)

    def test_call_command_ok(self) -> None:
        o = call_command(["echo", "Fred"])
        assert "Fred\n" == o
        assert isinstance(o, str), str(type(o))

    def test_call_command_err(self) -> None:
        with pytest.raises(
            CalledProcessErrorStderr,
            match=re.escape(
                "Command '['cat', '/dev/Frankenheimer']' exit status 1: cat: /dev/Frankenheimer: No such file or directory"
            ),
        ):
            call_command(["cat", "/dev/Frankenheimer"])


class TestPanic:
    def test_panic_by_hand(self) -> None:
        try:
            self.try_and_panic_by_hand()
        except:
            self.__assert_raised_exception_is_primary()

    def test_panic(self) -> None:
        try:
            self.try_and_panic()
        except:
            self.__assert_raised_exception_is_primary()

    def test_panic_with_secondary(self) -> None:
        try:
            self.try_and_panic_with_secondary()
        except:
            self.__assert_raised_exception_is_primary()

    def test_nested_panic(self) -> None:
        try:
            self.try_and_nested_panic_with_secondary()
        except:
            self.__assert_raised_exception_is_primary()

    def try_and_panic_by_hand(self) -> None:
        try:
            self.line_of_primary_exc = (inspect.currentframe()).f_lineno + 1  # type: ignore[union-attr]
            raise ValueError("primary")
        except Exception:
            exc_type, exc_value, traceback = sys.exc_info()
            try:
                raise RuntimeError("secondary")
            except Exception:
                pass
            raise_(exc_type, exc_value, traceback)

    def try_and_panic(self) -> None:
        try:
            self.line_of_primary_exc = (inspect.currentframe()).f_lineno + 1  # type: ignore[union-attr]
            raise ValueError("primary")
        except:
            with panic(log):
                pass

    def try_and_panic_with_secondary(self) -> None:
        try:
            self.line_of_primary_exc = (inspect.currentframe()).f_lineno + 1  # type: ignore[union-attr]
            raise ValueError("primary")
        except:
            with panic(log):
                raise RuntimeError("secondary")

    def try_and_nested_panic_with_secondary(self) -> None:
        try:
            self.line_of_primary_exc = (inspect.currentframe()).f_lineno + 1  # type: ignore[union-attr]
            raise ValueError("primary")
        except:
            with panic(log):
                with panic(log):
                    raise RuntimeError("secondary")

    def __assert_raised_exception_is_primary(self) -> None:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        assert exc_type == ValueError
        assert str(exc_value) == "primary"
        assert exc_traceback is not None
        while exc_traceback.tb_next is not None:
            exc_traceback = exc_traceback.tb_next
        assert exc_traceback.tb_lineno == self.line_of_primary_exc
