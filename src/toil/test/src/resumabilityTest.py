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

from pathlib import Path

from toil.exceptions import FailedJobsException
from toil.job import Job, JobFunctionWrappingJob
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.test import pslow as slow

import pytest


class TestResumability:
    """
    https://github.com/BD2KGenomics/toil/issues/808
    """

    @slow
    @pytest.mark.slow
    def test(self, tmp_path: Path) -> None:
        """
        Tests that a toil workflow that fails once can be resumed without a NoSuchJobException.
        """
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "INFO"
        options.retryCount = 0
        root = Job.wrapJobFn(parent)
        with pytest.raises(FailedJobsException):
            # This one is intended to fail.
            Job.Runner.startToil(root, options)

        # Resume the workflow. Unfortunately, we have to check for
        # this bug using the logging output, since although the
        # NoSuchJobException causes the worker to fail, the batch
        # system code notices that the job has been deleted despite
        # the failure and avoids the failure.
        options.restart = True
        tempDir = tmp_path / "tempdir"
        tempDir.mkdir()
        options.logFile = str(tempDir / "log.txt")
        Job.Runner.startToil(root, options)
        with open(options.logFile) as f:
            logString = f.read()
            # We are looking for e.g. "Batch system is reporting that
            # the jobGraph with batch system ID: 1 and jobGraph
            # store ID: n/t/jobwbijqL failed with exit value 1"
            assert "failed with exit value" not in logString

    def test_chaining(self, tmp_path: Path) -> None:
        """
        Tests that a job which is chained to and fails can resume and succeed.
        """

        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "DEBUG"
        options.retryCount = 0
        tempDir = tmp_path / "tempdir"
        tempDir.mkdir()
        options.logFile = str(tempDir / "log.txt")

        root = Job.wrapJobFn(chaining_parent)

        with pytest.raises(FailedJobsException):
            # This one is intended to fail.
            Job.Runner.startToil(root, options)

        with open(options.logFile) as f:
            log_content = f.read()
            # Make sure we actually did do chaining
            assert "Chaining from" in log_content

        # Because of the chaining, the problem we are looking for is the job
        # with the root ID not being able to load the body of a job with a
        # different ID. That doesn't look like a job deleted despite failure.
        options.restart = True
        Job.Runner.startToil(root, options)


def parent(job: Job) -> None:
    """
    Set up a bunch of dummy child jobs, and a bad job that needs to be
    restarted as the follow on.
    """
    for _ in range(5):
        job.addChildJobFn(goodChild)
    job.addFollowOnJobFn(badChild)


def chaining_parent(job: Job) -> None:
    """
    Set up a failing job to chain to.
    """
    job.addFollowOnJobFn(badChild)


def goodChild(job: Job) -> None:
    """
    Does nothing.
    """
    return


def badChild(job: JobFunctionWrappingJob) -> None:
    """
    Fails the first time it's run, succeeds the second time.
    """
    try:
        with job.fileStore.jobStore.read_shared_file_stream("alreadyRun") as fileHandle:
            fileHandle.read()
    except NoSuchFileException as ex:
        with job.fileStore.jobStore.write_shared_file_stream(
            "alreadyRun", encrypted=False
        ) as fileHandle:
            fileHandle.write(b"failed once\n")
        raise RuntimeError(f"this is an expected error: {str(ex)}")
