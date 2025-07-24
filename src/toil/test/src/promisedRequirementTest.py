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

from collections.abc import Generator
import argparse
import logging
import os
from pathlib import Path
import time
from typing import Any

from toil.test.batchSystems import batchSystemTest
from toil.batchSystems.mesos.test import MesosTestSupport
from toil.fileStores import FileID
from toil.job import Job, PromisedRequirement, JobFunctionWrappingJob, Promise
from toil.lib.retry import retry_flaky_test
from toil.test import pneeds_mesos as needs_mesos, pslow as slow

import pytest

log = logging.getLogger(__name__)


class AbstractPromisedRequirementsTest(batchSystemTest.AbstractBatchSystemJobTest):
    """An abstract base class for testing Toil workflows with promised requirements."""

    @slow
    @pytest.mark.slow
    def testConcurrencyDynamic(self, tmp_path: Path) -> None:
        """
        Asserts that promised core resources are allocated properly using a dynamic Toil workflow
        """
        for coresPerJob in self.allocatedCores:
            log.debug(
                "Testing %d cores per job with CPU count %d",
                coresPerJob,
                self.cpuCount,
            )
            tempDir = tmp_path / f"testFiles_{coresPerJob}"
            tempDir.mkdir()
            counterPath = self.getCounterPath(tempDir)

            root = Job.wrapJobFn(
                maxConcurrency,
                self.cpuCount,
                counterPath,
                coresPerJob,
                cores=1,
                memory="1M",
                disk="1M",
            )
            values = Job.Runner.startToil(root, self.getOptions(tempDir))
            maxValue = max(values)
            assert maxValue <= (self.cpuCount // coresPerJob)

    @slow
    @pytest.mark.slow
    @retry_flaky_test(prepare=[])
    def testConcurrencyStatic(self, tmp_path: Path) -> None:
        """
        Asserts that promised core resources are allocated properly using a static DAG
        """
        for coresPerJob in self.allocatedCores:
            log.debug(
                "Testing %d cores per job with CPU count %d",
                coresPerJob,
                self.cpuCount,
            )
            tempDir = tmp_path / f"testFiles_{coresPerJob}"
            tempDir.mkdir()
            counterPath = self.getCounterPath(tempDir)

            root = Job()
            one = Job.wrapFn(getOne, cores=0.1, memory="32M", disk="1M")
            thirtyTwoMb = Job.wrapFn(getThirtyTwoMb, cores=0.1, memory="32M", disk="1M")
            root.addChild(one)
            root.addChild(thirtyTwoMb)
            for _ in range(self.cpuCount):
                root.addFollowOn(
                    Job.wrapFn(
                        batchSystemTest.measureConcurrency,
                        counterPath,
                        cores=PromisedRequirement(lambda x: x * coresPerJob, one.rv()),
                        memory=PromisedRequirement(thirtyTwoMb.rv()),
                        disk="1M",
                    )
                )
            Job.Runner.startToil(root, self.getOptions(tempDir))
            _, maxValue = batchSystemTest.getCounters(counterPath)
            assert maxValue <= (self.cpuCount // coresPerJob)

    def getOptions(self, tempDir: Path, caching: bool = True) -> argparse.Namespace:
        options = super().getOptions(tempDir)
        # defaultCores defaults to 1 - this is coincidentally the core requirement relied upon by this
        # test, so we change defaultCores to 2 to make the test more strict
        options.defaultCores = 2
        options.caching = caching
        return options

    def getCounterPath(self, tempDir: Path) -> Path:
        """
        Returns path to a counter file
        :param tempDir: path to test directory
        :return: path to counter file
        """
        counterPath = tempDir / "counter"
        batchSystemTest.resetCounters(counterPath)
        minValue, maxValue = batchSystemTest.getCounters(counterPath)
        assert (minValue, maxValue) == (0, 0)
        return counterPath

    def testPromisesWithJobStoreFileObjects(
        self, tmp_path: Path, caching: bool = True
    ) -> None:
        """
        Check whether FileID objects are being pickled properly when used as return
        values of functions.  Then ensure that lambdas of promised FileID objects can be
        used to describe the requirements of a subsequent job.  This type of operation will be
        used commonly in Toil scripts.
        :return: None
        """
        file1 = 1024
        file2 = 512
        F1 = Job.wrapJobFn(_writer, file1)
        F2 = Job.wrapJobFn(_writer, file2)
        G = Job.wrapJobFn(
            _follower,
            file1 + file2,
            disk=PromisedRequirement(lambda x, y: x.size + y.size, F1.rv(), F2.rv()),
        )
        F1.addChild(F2)
        F2.addChild(G)

        Job.Runner.startToil(F1, self.getOptions(tmp_path, caching=caching))

    def testPromisesWithNonCachingFileStore(self, tmp_path: Path) -> None:
        self.testPromisesWithJobStoreFileObjects(tmp_path, caching=False)

    @slow
    @pytest.mark.slow
    def testPromiseRequirementRaceStatic(self, tmp_path: Path) -> None:
        """
        Checks for a race condition when using promised requirements and child job functions.
        """
        A = Job.wrapJobFn(logDiskUsage, "A", sleep=5, disk=PromisedRequirement(1024))
        B = Job.wrapJobFn(
            logDiskUsage, "B", disk=PromisedRequirement(lambda x: x + 1024, A.rv())
        )
        A.addChild(B)
        Job.Runner.startToil(A, self.getOptions(tmp_path))


def _writer(job: JobFunctionWrappingJob, fileSize: int) -> FileID:
    """
    Write a local file and return the FileID obtained from running writeGlobalFile on
    it.

    :param job: job
    :param fileSize: Size of the file in bytes
    :returns: the result of writeGlobalFile on a locally created file
    :rtype: job.FileID
    """
    with open(job.fileStore.getLocalTempFileName(), "wb") as fH:
        fH.write(os.urandom(fileSize))
    return job.fileStore.writeGlobalFile(fH.name)


def _follower(job: Job, expectedDisk: Any) -> None:
    """
    This job follows the _writer jobs and ensures the expected disk is used.

    :param job: job
    :param expectedDisk: Expect disk to be used by this job
    :return: None
    """
    assert job.disk == expectedDisk


def maxConcurrency(
    job: Job, cpuCount: int, filename: Path, coresPerJob: int
) -> list[Promise]:
    """
    Returns the max number of concurrent tasks when using a PromisedRequirement instance
    to allocate the number of cores per job.

    :param cpuCount: number of available cpus
    :param filename: path to counter file
    :param coresPerJob: number of cores assigned to each job
    :return: max concurrency value per CPU
    """
    one = job.addChildFn(getOne, cores=0.1, memory="32M", disk="1M")
    thirtyTwoMb = job.addChildFn(getThirtyTwoMb, cores=0.1, memory="32M", disk="1M")

    values = []
    for _ in range(cpuCount):
        value = job.addFollowOnFn(
            batchSystemTest.measureConcurrency,
            filename,
            cores=PromisedRequirement(lambda x: x * coresPerJob, one.rv()),
            memory=PromisedRequirement(thirtyTwoMb.rv()),
            disk="1M",
        ).rv()
        values.append(value)
    return values


def getOne() -> int:
    return 1


def getThirtyTwoMb() -> str:
    return "32M"


def logDiskUsage(job: JobFunctionWrappingJob, funcName: str, sleep: int = 0) -> int:
    """
    Logs the job's disk usage to master and sleeps for specified amount of time.

    :return: job function's disk usage
    """
    diskUsage = job.disk
    job.fileStore.log_to_leader(f"{funcName}: {diskUsage}")
    time.sleep(sleep)
    return diskUsage


class TestSingleMachinePromisedRequirements(AbstractPromisedRequirementsTest):
    """
    Tests against the SingleMachine batch system
    """

    def getBatchSystemName(self) -> str:
        return "single_machine"

    def tearDown(self) -> None:
        pass


@needs_mesos
class TestMesosPromisedRequirements(AbstractPromisedRequirementsTest, MesosTestSupport):
    """
    Tests against the Mesos batch system
    """

    @pytest.fixture(autouse=True)
    def mesos_support(self) -> Generator[None]:
        try:
            self._startMesos(self.cpuCount)
            yield
        finally:
            self._stopMesos()

    def getOptions(self, tempDir: Path, caching: bool = True) -> argparse.Namespace:
        options = super().getOptions(tempDir, caching=caching)
        options.mesos_endpoint = "localhost:5050"
        return options

    def getBatchSystemName(self) -> str:
        return "mesos"
