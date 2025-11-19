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
import logging
import mimetypes
import os
import subprocess
import sys
from pathlib import Path

from toil.test import pslow as slow
from toil.test.mesos import helloWorld

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class RegularLogTest:

    def _getFiles(self, dirpath: Path) -> list[str]:
        return [
            os.path.join(dirpath, f)
            for f in os.listdir(dirpath)
            if os.path.isfile(os.path.join(dirpath, f))
        ]

    def _assertFileTypeExists(
        self, dirpath: Path, extension: str, encoding: str | None = None
    ) -> None:
        # an encoding of None implies no compression
        logger.info("Checking for %s file in %s", extension, dirpath)
        onlyFiles = self._getFiles(dirpath)
        logger.info("Found: %s", str(os.listdir(dirpath)))
        onlyLogs = [f for f in onlyFiles if f.endswith(extension)]
        logger.info("Found matching: %s", str(onlyLogs))
        assert onlyLogs

        if encoding is not None:
            for log in onlyLogs:
                with open(log, "rb") as f:
                    logger.info(
                        "Checking for encoding %s on file %s", str(encoding), log
                    )
                    if encoding == "gzip":
                        # Check for gzip magic header '\x1f\x8b'
                        assert f.read().startswith(b"\x1f\x8b")
                    else:
                        mime = mimetypes.guess_type(log)
                        assert mime[1] == encoding

    @slow
    def testLogToMaster(self) -> None:
        toilOutput = subprocess.check_output(
            [
                sys.executable,
                "-m",
                helloWorld.__name__,
                "./toilTest",
                "--clean=always",
                "--logLevel=info",
            ],
            stderr=subprocess.STDOUT,
        )
        assert helloWorld.childMessage in toilOutput.decode("utf-8")

    def testWriteLogs(self, tmp_path: Path) -> None:
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                helloWorld.__name__,
                "./toilTest",
                "--clean=always",
                "--logLevel=debug",
                f"--writeLogs={tmp_path}",
            ]
        )
        self._assertFileTypeExists(tmp_path, ".log")

    @slow
    def testWriteGzipLogs(self, tmp_path: Path) -> None:
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                helloWorld.__name__,
                "./toilTest",
                "--clean=always",
                "--logLevel=debug",
                f"--writeLogsGzip={tmp_path}",
            ]
        )
        self._assertFileTypeExists(tmp_path, ".log.gz", "gzip")

    @slow
    def testMultipleLogToMaster(self) -> None:
        toilOutput = subprocess.check_output(
            [
                sys.executable,
                "-m",
                helloWorld.__name__,
                "./toilTest",
                "--clean=always",
                "--logLevel=info",
            ],
            stderr=subprocess.STDOUT,
        )
        assert helloWorld.parentMessage in toilOutput.decode("utf-8")

    def testRegularLog(self) -> None:
        toilOutput = subprocess.check_output(
            [
                sys.executable,
                "-m",
                helloWorld.__name__,
                "./toilTest",
                "--clean=always",
                "--batchSystem=single_machine",
                "--logLevel=debug",
            ],
            stderr=subprocess.STDOUT,
        )
        assert "single machine batch system" in toilOutput.decode("utf-8")
