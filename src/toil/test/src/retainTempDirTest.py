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
import argparse
from collections.abc import Callable
import os
from pathlib import Path

from toil.exceptions import FailedJobsException
from toil.job import Job, JobFunctionWrappingJob

import pytest


class TestCleanWorkDir:
    """
    Tests testing :class:toil.fileStores.abstractFileStore.AbstractFileStore
    """

    def testNever(self, tmp_path: Path) -> None:
        retainedTempData = self._runAndReturnWorkDir(
            tmp_path, "never", job=tempFileTestJob
        )
        assert retainedTempData != [], (
            "The worker's temporary workspace was deleted despite "
            "cleanWorkDir being set to 'never'"
        )

    def testAlways(self, tmp_path: Path) -> None:
        retainedTempData = self._runAndReturnWorkDir(
            tmp_path, "always", job=tempFileTestJob
        )
        assert retainedTempData == [], (
            "The worker's temporary workspace was not deleted despite "
            "cleanWorkDir being set to 'always'"
        )

    def testOnErrorWithError(self, tmp_path: Path) -> None:
        retainedTempData = self._runAndReturnWorkDir(
            tmp_path, "onError", job=tempFileTestErrorJob, expectError=True
        )
        assert retainedTempData == [], (
            "The worker's temporary workspace was not deleted despite "
            "an error occurring and cleanWorkDir being set to 'onError'"
        )

    def testOnErrorWithNoError(self, tmp_path: Path) -> None:
        retainedTempData = self._runAndReturnWorkDir(
            tmp_path, "onError", job=tempFileTestJob
        )
        assert retainedTempData != [], (
            "The worker's temporary workspace was deleted despite "
            "no error occurring and cleanWorkDir being set to 'onError'"
        )

    def testOnSuccessWithError(self, tmp_path: Path) -> None:
        retainedTempData = self._runAndReturnWorkDir(
            tmp_path, "onSuccess", job=tempFileTestErrorJob, expectError=True
        )
        assert retainedTempData != [], (
            "The worker's temporary workspace was deleted despite "
            "an error occurring and cleanWorkDir being set to 'onSuccesss'"
        )

    def testOnSuccessWithSuccess(self, tmp_path: Path) -> None:
        retainedTempData = self._runAndReturnWorkDir(
            tmp_path, "onSuccess", job=tempFileTestJob
        )
        assert retainedTempData == [], (
            "The worker's temporary workspace was not deleted despite "
            "a successful job execution and cleanWorkDir being set to 'onSuccesss'"
        )

    def _runAndReturnWorkDir(
        self,
        tmp_path: Path,
        cleanWorkDir: str,
        job: Callable[[JobFunctionWrappingJob], None],
        expectError: bool = False,
    ) -> list[str]:
        """
        Runs toil with the specified job and cleanWorkDir setting. expectError determines whether the test's toil
        run is expected to succeed, and the test will fail if that expectation is not met. returns the contents of
        the workDir after completion of the run
        """
        workdir = tmp_path / "testDir"
        workdir.mkdir()
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.workDir = str(workdir)
        options.clean = "always"
        options.cleanWorkDir = cleanWorkDir
        A = Job.wrapJobFn(job)
        if expectError:
            self._launchError(A, options)
        else:
            self._launchRegular(A, options)
        return os.listdir(workdir)

    def _launchRegular(
        self, A: JobFunctionWrappingJob, options: argparse.Namespace
    ) -> None:
        Job.Runner.startToil(A, options)

    def _launchError(
        self, A: JobFunctionWrappingJob, options: argparse.Namespace
    ) -> None:
        try:
            Job.Runner.startToil(A, options)
        except FailedJobsException:
            pass  # we expect a job to fail here
        else:
            pytest.fail("Toil run succeeded unexpectedly")


def tempFileTestJob(job: JobFunctionWrappingJob) -> None:
    with open(job.fileStore.getLocalTempFile(), "w") as f:
        f.write("test file retention")


def tempFileTestErrorJob(job: JobFunctionWrappingJob) -> None:
    with open(job.fileStore.getLocalTempFile(), "w") as f:
        f.write("test file retention")
    raise RuntimeError()  # test failure
