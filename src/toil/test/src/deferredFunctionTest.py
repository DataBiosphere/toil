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
from argparse import Namespace
from collections.abc import Callable, Generator, Sequence
import logging
import os
from pathlib import Path
import signal
import time
from typing import Optional
from uuid import uuid4

import psutil
import pytest

from toil.exceptions import FailedJobsException
from toil.job import Job
from toil.lib.threading import cpu_count
from toil.test import pslow as slow

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def options(tmp_path: Path) -> Generator[Namespace]:
    try:
        testDir = tmp_path / "testDir"
        testDir.mkdir()
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "INFO"
        options.workDir = str(testDir)
        options.clean = "always"
        options.logFile = str(testDir / "logFile")
        yield options
    finally:
        pass  # no cleanup


class TestDeferredFunction:
    """Test the deferred function system."""

    # Tests for the various defer possibilities
    def testDeferredFunctionRunsWithMethod(
        self, tmp_path: Path, options: Namespace
    ) -> None:
        """
        Refer docstring in _testDeferredFunctionRuns.
        Test with Method
        """
        self._testDeferredFunctionRuns(tmp_path, options, _writeNonLocalFilesMethod)

    def testDeferredFunctionRunsWithClassMethod(
        self, tmp_path: Path, options: Namespace
    ) -> None:
        """
        Refer docstring in _testDeferredFunctionRuns.
        Test with Class Method
        """
        self._testDeferredFunctionRuns(
            tmp_path, options, _writeNonLocalFilesClassMethod
        )

    def testDeferredFunctionRunsWithLambda(
        self, tmp_path: Path, options: Namespace
    ) -> None:
        """
        Refer docstring in _testDeferredFunctionRuns.
        Test with Lambda
        """
        self._testDeferredFunctionRuns(tmp_path, options, _writeNonLocalFilesLambda)

    def _testDeferredFunctionRuns(
        self,
        tmp_path: Path,
        options: Namespace,
        callableFn: Callable[[Job, tuple[Path, Path]], None],
    ) -> None:
        """
        Create 2 files. Make a job that writes data to them. Register a deferred function that
        deletes the two files (one passed as an arg, and one as a kwarg) and later assert that
        the files have been deleted.

        :param function callableFn: The function to use in the test.
        :return: None
        """
        workdir = tmp_path / "nonLocalDir"
        workdir.mkdir()
        nonLocalFile1 = workdir / str(uuid4())
        nonLocalFile2 = workdir / str(uuid4())
        nonLocalFile1.touch()
        nonLocalFile2.touch()
        assert nonLocalFile1.exists()
        assert nonLocalFile2.exists()
        A = Job.wrapJobFn(callableFn, files=(nonLocalFile1, nonLocalFile2))
        Job.Runner.startToil(A, options)
        assert not nonLocalFile1.exists()
        assert not nonLocalFile2.exists()

    @slow
    @pytest.mark.slow
    def testDeferredFunctionRunsWithFailures(
        self, options: Namespace, tmp_path: Path
    ) -> None:
        """
        Create 2 non local filesto use as flags.  Create a job that registers a function that
        deletes one non local file.  If that file exists, the job SIGKILLs itself. If it doesn't
        exist, the job registers a second deferred function to delete the second non local file
        and exits normally.

        Initially the first file exists, so the job should SIGKILL itself and neither deferred
        function will run (in fact, the second should not even be registered). On the restart,
        the first deferred function should run and the first file should not exist, but the
        second one should.  We assert the presence of the second, then register the second
        deferred function and exit normally.  At the end of the test, neither file should exist.

        Incidentally, this also tests for multiple registered deferred functions, and the case
        where a deferred function fails (since the first file doesn't exist on the retry).
        """
        options.retryCount = 1
        workdir = tmp_path / "nonLocalDir"
        workdir.mkdir()
        nonLocalFile1 = workdir / str(uuid4())
        nonLocalFile2 = workdir / str(uuid4())
        nonLocalFile1.touch()
        nonLocalFile2.touch()
        assert nonLocalFile1.exists()
        assert nonLocalFile2.exists()
        A = Job.wrapJobFn(
            _deferredFunctionRunsWithFailuresFn, files=(nonLocalFile1, nonLocalFile2)
        )
        Job.Runner.startToil(A, options)
        assert not nonLocalFile1.exists()
        assert not nonLocalFile2.exists()

    @slow
    @pytest.mark.slow
    @pytest.mark.skipif(
        cpu_count() < 2, reason="Not enough CPUs to run two tasks at once"
    )
    def testNewJobsCanHandleOtherJobDeaths(
        self, options: Namespace, tmp_path: Path
    ) -> None:
        """
        Create 2 non-local files and then create 2 jobs. The first job registers a deferred job
        to delete the second non-local file, deletes the first non-local file and then kills
        itself.  The second job waits for the first file to be deleted, then sleeps for a few
        seconds and then spawns a child. the child of the second does nothing. However starting
        it should handle the untimely demise of the first job and run the registered deferred
        function that deletes the first file.  We assert the absence of the two files at the
        end of the run.
        """

        # There can be no retries
        options.retryCount = 0
        workdir = tmp_path / "nonLocalDir"
        workdir.mkdir()
        nonLocalFile1 = workdir / str(uuid4())
        nonLocalFile2 = workdir / str(uuid4())
        nonLocalFile1.touch()
        nonLocalFile2.touch()
        assert nonLocalFile1.exists()
        assert nonLocalFile2.exists()
        files = [nonLocalFile1, nonLocalFile2]
        root = Job()
        # A and B here must run in parallel for this to work
        A = Job.wrapJobFn(_testNewJobsCanHandleOtherJobDeaths_A, files=files, cores=1)
        B = Job.wrapJobFn(_testNewJobsCanHandleOtherJobDeaths_B, files=files, cores=1)
        C = Job.wrapJobFn(
            _testNewJobsCanHandleOtherJobDeaths_C,
            files=files,
            expectedResult=False,
            cores=1,
        )
        root.addChild(A)
        root.addChild(B)
        B.addChild(C)
        try:
            Job.Runner.startToil(root, options)
        except FailedJobsException:
            pass

    def testBatchSystemCleanupCanHandleWorkerDeaths(
        self, options: Namespace, tmp_path: Path
    ) -> None:
        """
        Create some non-local files. Create a job that registers a deferred
        function to delete the file and then kills its worker.

        Assert that the file is missing after the pipeline fails, because we're
        using a single-machine batch system and the leader's batch system
        cleanup will find and run the deferred function.
        """

        # There can be no retries
        options.retryCount = 0
        workdir = tmp_path / "nonLocalDir"
        workdir.mkdir()
        nonLocalFile1 = workdir / str(uuid4())
        nonLocalFile2 = workdir / str(uuid4())
        # The first file has to be non zero or meseeks will go into an infinite sleep
        with nonLocalFile1.open("w") as file1:
            file1.write("test")
        nonLocalFile2.touch()
        assert nonLocalFile1.exists()
        assert nonLocalFile2.exists()
        # We only use the "A" job here, and we fill in the first file, so all
        # it will do is defer deleting the second file, delete the first file,
        # and die.
        A = Job.wrapJobFn(
            _testNewJobsCanHandleOtherJobDeaths_A, files=(nonLocalFile1, nonLocalFile2)
        )
        try:
            Job.Runner.startToil(A, options)
        except FailedJobsException:
            pass
        assert not nonLocalFile1.exists()
        assert not nonLocalFile2.exists()


def _writeNonLocalFilesMethod(job: Job, files: tuple[Path, Path]) -> None:
    """
    Write some data to 2 files.  Pass them to a registered deferred method.

    :param tuple files: the tuple of the two files to work with
    """
    for nlf in files:
        with nlf.open("wb") as nonLocalFileHandle:
            nonLocalFileHandle.write(os.urandom(1 * 1024 * 1024))
    job.defer(_deleteMethods._deleteFileMethod, files[0], nlf=files[1])


def _writeNonLocalFilesClassMethod(job: Job, files: tuple[Path, Path]) -> None:
    """
    Write some data to 2 files.  Pass them to a registered deferred class method.

    :param tuple files: the tuple of the two files to work with
    """
    for nlf in files:
        with nlf.open("wb") as nonLocalFileHandle:
            nonLocalFileHandle.write(os.urandom(1 * 1024 * 1024))
    job.defer(_deleteMethods._deleteFileClassMethod, files[0], nlf=files[1])


def _writeNonLocalFilesLambda(job: Job, files: tuple[Path, Path]) -> None:
    """
    Write some data to 2 files.  Pass them to a registered deferred Lambda.

    :param tuple files: the tuple of the two files to work with
    """

    def lmd(x: Path, nlf: Path) -> None:
        x.unlink()
        nlf.unlink()
    for nlf in files:
        with nlf.open("wb") as nonLocalFileHandle:
            nonLocalFileHandle.write(os.urandom(1 * 1024 * 1024))
    job.defer(lmd, files[0], nlf=files[1])


def _deferredFunctionRunsWithFailuresFn(job: Job, files: tuple[Path, Path]) -> None:
    """
    Refer testDeferredFunctionRunsWithFailures

    :param tuple files: the tuple of the two files to work with
    """
    job.defer(_deleteFile, files[0])
    if files[0].exists():
        os.kill(os.getpid(), signal.SIGKILL)
    else:
        assert files[1].exists()
        job.defer(_deleteFile, files[1])


def _deleteFile(nonLocalFile: Path, nlf: Optional[Path] = None) -> None:
    """
    Delete nonLocalFile and nlf
    :param nonLocalFile:
    :param nlf:
    """
    logger.debug("Removing file: %s", nonLocalFile)
    nonLocalFile.unlink()
    logger.debug("Successfully removed file: %s", nonLocalFile)
    if nlf is not None:
        logger.debug("Removing file: %s", nlf)
        nlf.unlink()
        logger.debug("Successfully removed file: %s", nlf)


def _testNewJobsCanHandleOtherJobDeaths_A(job: Job, files: tuple[Path, Path]) -> None:
    """
    Defer deletion of files[1], then wait for _testNewJobsCanHandleOtherJobDeaths_B to
    start up, and finally delete files[0] before sigkilling self.

    :param tuple files: the tuple of the two files to work with
    """

    # Write the pid to files[1] such that we can be sure that this process has died before
    # we spawn the next job that will do the cleanup.
    with files[1].open("w") as fileHandle:
        fileHandle.write(str(os.getpid()))
    job.defer(_deleteFile, files[1])
    logger.info("Deferred delete of %s", files[1])
    while files[0].stat().st_size == 0:
        time.sleep(0.5)
    files[0].unlink()
    os.kill(os.getpid(), signal.SIGKILL)


def _testNewJobsCanHandleOtherJobDeaths_B(job: Job, files: tuple[Path, Path]) -> None:
    # Write something to files[0] such that we can be sure that this process has started
    # before _testNewJobsCanHandleOtherJobDeaths_A kills itself.
    with files[0].open("w") as fileHandle:
        fileHandle.write(str(os.getpid()))
    while files[0].exists():
        time.sleep(0.5)
    # Get the pid of _testNewJobsCanHandleOtherJobDeaths_A and wait for it to truly be dead.
    with files[1].open() as fileHandle:
        pid = int(fileHandle.read())
    assert pid > 0
    while psutil.pid_exists(pid):
        time.sleep(0.5)
    # Now that we are convinced that_testNewJobsCanHandleOtherJobDeaths_A has died, we can
    # spawn the next job


def _testNewJobsCanHandleOtherJobDeaths_C(
    job: Job, files: Sequence[Path], expectedResult: bool
) -> None:
    """
    Asserts whether the files exist or not.

    :param job: Job
    :param files: list of files to test
    :param expectedResult: Are we expecting the files to exist or not?
    """
    for testFile in files:
        assert testFile.exists() is expectedResult


class _deleteMethods:
    @staticmethod
    def _deleteFileMethod(nonLocalFile: Path, nlf: Optional[Path] = None) -> None:
        """
        Delete nonLocalFile and nlf
        """
        nonLocalFile.unlink()
        if nlf is not None:
            nlf.unlink()

    @classmethod
    def _deleteFileClassMethod(
        cls, nonLocalFile: Path, nlf: Optional[Path] = None
    ) -> None:
        """
        Delete nonLocalFile and nlf
        """
        nonLocalFile.unlink()
        if nlf is not None:
            nlf.unlink()
