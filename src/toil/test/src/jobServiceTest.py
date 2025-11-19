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
import codecs
import logging
import random
import sys
import time
import traceback
from pathlib import Path
from threading import Event, Thread
from typing import Any, Literal, cast

import pytest

from toil.batchSystems import DeadlockException
from toil.batchSystems.singleMachine import SingleMachineBatchSystem
from toil.exceptions import FailedJobsException
from toil.job import Job, ServiceHostJob
from toil.jobStores.abstractJobStore import AbstractJobStore
from toil.test import pslow as slow

logger = logging.getLogger(__name__)


class TestJobService:
    """
    Tests testing the Job.Service class
    """

    @slow
    @pytest.mark.slow
    def testServiceSerialization(self, tmp_path: Path) -> None:
        """
        Tests that a service can receive a promise without producing a serialization
        error.
        """
        job = Job()
        service = ToySerializableService("woot")
        startValue = job.addService(service)  # Add a first service to job
        subService = ToySerializableService(
            cast(str, startValue)
        )  # Now create a child of
        # that service that takes the start value promise from the parent service
        job.addService(subService, parentService=service)  # This should work if
        # serialization on services is working correctly.

        self.runToil(tmp_path, job)

    @slow
    @pytest.mark.slow
    def testService(self, tmp_path: Path, checkpoint: bool = False) -> None:
        """
        Tests the creation of a Job.Service with random failures of the worker.
        """
        for test in range(2):
            outFile = tmp_path / f"test{test}"
            messageInt = random.randint(1, sys.maxsize)
            # Wire up the services/jobs
            t = Job.wrapJobFn(serviceTest, outFile, messageInt, checkpoint=checkpoint)

            # Run the workflow repeatedly until success
            sub_tmp_path = tmp_path / str(test)
            sub_tmp_path.mkdir()
            self.runToil(sub_tmp_path, t)

            # Check output
            assert int(outFile.read_text()) == messageInt

    @slow
    @pytest.mark.slow
    @pytest.mark.skipif(
        SingleMachineBatchSystem.numCores < 4,
        reason="Need at least four cores to run this test",
    )
    def testServiceDeadlock(self, tmp_path: Path) -> None:
        """
        Creates a job with more services than maxServices, checks that deadlock is detected.
        """
        outFile = tmp_path / "out"

        def makeWorkflow() -> Job:
            job = Job()
            r1 = job.addService(ToySerializableService("woot1"))
            r2 = job.addService(ToySerializableService("woot2"))
            r3 = job.addService(ToySerializableService("woot3"))
            job.addChildFn(fnTest, [r1, r2, r3], outFile)
            return job

        # This should fail as too few services available
        sub_tmp_path1 = tmp_path / "1"
        sub_tmp_path1.mkdir()
        with pytest.raises(DeadlockException):
            self.runToil(
                sub_tmp_path1,
                makeWorkflow(),
                badWorker=0.0,
                maxServiceJobs=2,
                deadlockWait=5,
            )

        # This should pass, as adequate services available
        sub_tmp_path2 = tmp_path / "2"
        sub_tmp_path2.mkdir()
        self.runToil(sub_tmp_path2, makeWorkflow(), maxServiceJobs=3)
        # Check we get expected output
        assert outFile.read_text() == "woot1 woot2 woot3"

    def testServiceWithCheckpoints(self, tmp_path: Path) -> None:
        """
        Tests the creation of a Job.Service with random failures of the worker, making the root job use checkpointing to
        restart the subtree.
        """
        self.testService(tmp_path, checkpoint=True)

    @slow
    @pytest.mark.slow
    @pytest.mark.skipif(
        SingleMachineBatchSystem.numCores < 4,
        reason="Need at least four cores to run this test",
    )
    def testServiceRecursive(self, tmp_path: Path, checkpoint: bool = True) -> None:
        """
        Tests the creation of a Job.Service, creating a chain of services and accessing jobs.
        Randomly fails the worker.
        """
        for test in range(1):
            # Temporary file
            outFile = tmp_path / f"test{test}"
            messages = [random.randint(1, sys.maxsize) for i in range(3)]
            # Wire up the services/jobs
            t = Job.wrapJobFn(
                serviceTestRecursive, outFile, messages, checkpoint=checkpoint
            )

            # Run the workflow repeatedly until success
            sub_tmp_path = tmp_path / str(test)
            sub_tmp_path.mkdir()
            self.runToil(sub_tmp_path, t)

            # Check output
            assert list(map(int, outFile.open().readlines())) == messages

    @slow
    @pytest.mark.slow
    @pytest.mark.skipif(
        SingleMachineBatchSystem.numCores < 4,
        reason="Need at least four cores to run this test",
    )
    @pytest.mark.timeout(1200)
    def testServiceParallelRecursive(
        self, tmp_path: Path, checkpoint: bool = True
    ) -> None:
        """
        Tests the creation of a Job.Service, creating parallel chains of services and accessing jobs.
        Randomly fails the worker.
        """

        # This test needs to have something like 10 jobs succeed

        BUNDLE_SIZE = 3
        BUNDLE_COUNT = 2
        RETRY_COUNT = 4
        FAIL_FRACTION = 0.5
        MAX_ATTEMPTS = 10

        total_jobs = BUNDLE_SIZE * BUNDLE_COUNT * 2 + 1
        p_complete_job_failure = FAIL_FRACTION ** (RETRY_COUNT + 1)
        p_workflow_success = (1 - p_complete_job_failure) ** total_jobs
        logger.info(
            "Going to run %s total jobs, each of which completely fails %s of the time, so the workflow will succeed with probability %s",
            total_jobs,
            p_complete_job_failure,
            p_workflow_success,
        )
        p_test_failure = (1 - p_workflow_success) ** MAX_ATTEMPTS
        logger.info(
            "This test will fail spuriously with probability %s", p_test_failure
        )

        # We want to run the workflow through several times to test restarting, so we need it to often fail but reliably sometimes succeed, and almost always succeed when repeated.

        assert 0.8 > p_workflow_success
        assert p_workflow_success > 0.2
        assert 0.001 > p_test_failure

        for test in range(1):
            # Temporary file
            outFiles = [tmp_path / f"test{test}_{j}" for j in range(BUNDLE_COUNT)]
            # We send 3 messages each in 2 sets, each of which needs a service and a client
            messageBundles = [
                [random.randint(1, sys.maxsize) for i in range(BUNDLE_SIZE)]
                for j in range(BUNDLE_COUNT)
            ]
            # Wire up the services/jobs
            t = Job.wrapJobFn(
                serviceTestParallelRecursive,
                outFiles,
                messageBundles,
                checkpoint=True,
            )

            # Run the workflow repeatedly until success
            sub_tmp_path = tmp_path / str(test)
            sub_tmp_path.mkdir()
            self.runToil(
                sub_tmp_path,
                t,
                retryCount=RETRY_COUNT,
                badWorker=FAIL_FRACTION,
                max_attempts=MAX_ATTEMPTS,
            )

            # Check output
            for messages, outFile in zip(messageBundles, outFiles):
                assert list(map(int, outFile.open().readlines())) == messages

    def runToil(
        self,
        tmp_path: Path,
        rootJob: Job,
        retryCount: int = 1,
        badWorker: float = 0.5,
        badWorkedFailInterval: float = 0.1,
        maxServiceJobs: int = sys.maxsize,
        deadlockWait: int = 60,
        max_attempts: int = 50,
    ) -> None:
        # Create the runner for the workflow.
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "DEBUG"

        options.retryCount = retryCount
        options.badWorker = badWorker
        options.badWorkerFailInterval = badWorkedFailInterval
        options.servicePollingInterval = 1
        options.maxServiceJobs = maxServiceJobs
        options.deadlockWait = deadlockWait

        # Run the workflow
        total_tries = 1
        while True:
            try:
                Job.Runner.startToil(rootJob, options)
                break
            except FailedJobsException as e:
                i = e.numberOfFailedJobs
                logger.info(
                    "Workflow attempt %s/%s failed with %s failed jobs",
                    total_tries,
                    max_attempts,
                    i,
                )
                if total_tries == max_attempts:
                    pytest.fail(reason="Exceeded a reasonable number of restarts")
                total_tries += 1
                options.restart = True
        logger.info("Succeeded after %s/%s attempts", total_tries, max_attempts)


class TestPerfectServicet(TestJobService):
    def runToil(self, *args: Any, **kwargs: Any) -> None:
        """
        Let us run all the tests in the other service test class, but without worker failures.
        """
        kwargs["badWorker"] = 0
        super().runToil(*args, **kwargs)


def serviceTest(job: Job, outFile: Path, messageInt: int) -> None:
    """
    Creates one service and one accessing job, which communicate with two files to establish
    that both run concurrently.
    """
    # Clean out out-file
    outFile.open("w").close()
    # We create a random number that is added to messageInt and subtracted by
    # the serviceAccessor, to prove that when service test is checkpointed and
    # restarted there is never a connection made between an earlier service and
    # later serviceAccessor, or vice versa.
    to_subtract = random.randint(1, sys.maxsize)
    job.addChildJobFn(
        serviceAccessor,
        job.addService(ToyService(messageInt + to_subtract)),
        outFile,
        to_subtract,
    )


def serviceTestRecursive(job: Job, outFile: Path, messages: list[int]) -> None:
    """
    Creates a chain of services and accessing jobs, each paired together.
    """
    if len(messages) > 0:
        # Clean out out-file
        outFile.open("w").close()
        to_add = random.randint(1, sys.maxsize)
        service = ToyService(messages[0] + to_add)
        child = job.addChildJobFn(
            serviceAccessor, job.addService(service), outFile, to_add
        )

        for i in range(1, len(messages)):
            to_add = random.randint(1, sys.maxsize)
            service2 = ToyService(messages[i] + to_add, cores=0.1)
            child = child.addChildJobFn(
                serviceAccessor,
                job.addService(service2, parentService=service),
                outFile,
                to_add,
                cores=0.1,
            )
            service = service2


def serviceTestParallelRecursive(
    job: Job, outFiles: list[Path], messageBundles: list[list[int]]
) -> None:
    """
    Creates multiple chains of services and accessing jobs.
    """
    for messages, outFile in zip(messageBundles, outFiles):
        # Clean out out-file
        outFile.open("w").close()
        if len(messages) > 0:
            to_add = random.randint(1, sys.maxsize)
            service = ToyService(messages[0] + to_add)
            child = job.addChildJobFn(
                serviceAccessor, job.addService(service), outFile, to_add
            )

            for i in range(1, len(messages)):
                to_add = random.randint(1, sys.maxsize)
                service2 = ToyService(messages[i] + to_add, cores=0.1)
                child = child.addChildJobFn(
                    serviceAccessor,
                    job.addService(service2, parentService=service),
                    outFile,
                    to_add,
                    cores=0.1,
                )
                service = service2


class ToyService(Job.Service):
    def __init__(self, messageInt: int, *args: Any, **kwargs: Any) -> None:
        """
        While established the service repeatedly:
             - reads an integer i from the inJobStoreFileID file
             - writes i and messageInt to the outJobStoreFileID file
        """
        Job.Service.__init__(self, *args, **kwargs)
        self.messageInt = messageInt

    def start(self, job: ServiceHostJob) -> tuple[str, str]:
        assert self.disk is not None
        assert self.memory is not None
        assert self.cores is not None
        self.terminate = Event()
        self.error = Event()
        # Note that service jobs are special and do not necessarily have job.jobStoreID.
        # So we don't associate these files with this job.
        inJobStoreID = job.fileStore.jobStore.get_empty_file_store_id()
        outJobStoreID = job.fileStore.jobStore.get_empty_file_store_id()
        self.serviceThread = Thread(
            target=self.serviceWorker,
            args=(
                job.fileStore.jobStore,
                self.terminate,
                self.error,
                inJobStoreID,
                outJobStoreID,
                self.messageInt,
            ),
        )
        self.serviceThread.start()
        return (inJobStoreID, outJobStoreID)

    def stop(self, job: Job) -> None:
        self.terminate.set()
        self.serviceThread.join()

    def check(self) -> Literal[True]:
        if self.error.isSet():
            raise RuntimeError("Service worker failed")
        return True

    @staticmethod
    def serviceWorker(
        jobStore: AbstractJobStore,
        terminate: Event,
        error: Event,
        inJobStoreID: str,
        outJobStoreID: str,
        messageInt: int,
    ) -> None:
        try:
            while True:
                if terminate.isSet():  # Quit if we've got the terminate signal
                    logger.debug("Service worker being told to quit")
                    return

                time.sleep(0.2)  # Sleep to avoid thrashing

                # Try reading a line from the input file
                try:
                    with jobStore.read_file_stream(inJobStoreID) as f:
                        line = codecs.getreader("utf-8")(f).readline()
                except:
                    logger.debug(
                        "Something went wrong reading a line: %s",
                        traceback.format_exc(),
                    )
                    raise

                if len(line.strip()) == 0:
                    # Don't try and make an integer out of nothing
                    continue

                # Try converting the input line into an integer
                try:
                    inputInt = int(line)
                except ValueError:
                    logger.debug(
                        "Tried casting input line '%s' to integer but got error: %s",
                        line,
                        traceback.format_exc(),
                    )
                    continue

                # Write out the resulting read integer and the message
                with jobStore.update_file_stream(outJobStoreID) as f:
                    f.write((f"{inputInt} {messageInt}\n").encode())
        except:
            logger.debug("Error in service worker: %s", traceback.format_exc())
            error.set()
            raise


def serviceAccessor(
    job: ServiceHostJob,
    communicationFiles: tuple[str, str],
    outFile: Path,
    to_subtract: int,
) -> None:
    """
    Writes a random integer iinto the inJobStoreFileID file, then tries 10 times reading
    from outJobStoreFileID to get a pair of integers, the first equal to i the second written into the outputFile.
    """
    inJobStoreFileID, outJobStoreFileID = communicationFiles

    # Get a random integer to advertise ourselves
    key = random.randint(1, sys.maxsize)

    # Write the integer into the file
    logger.debug("Writing key to inJobStoreFileID")
    with job.fileStore.jobStore.update_file_stream(inJobStoreFileID) as fH:
        fH.write(("%s\n" % key).encode("utf-8"))

    logger.debug("Trying to read key and message from outJobStoreFileID")
    for i in range(10):  # Try 10 times over
        time.sleep(0.2)  # Avoid thrashing

        # Try reading an integer from the input file and writing out the message
        with job.fileStore.jobStore.read_file_stream(outJobStoreFileID) as fH:
            fH = codecs.getreader("utf-8")(fH)
            line = fH.readline()

        tokens = line.split()
        if len(tokens) != 2:
            continue

        key2, message = tokens

        if int(key2) == key:
            logger.debug(
                f"Matched key's: {key}, writing message: {int(message) - to_subtract} with to_subtract: {to_subtract}"
            )
            with outFile.open("a") as fH:
                fH.write("%s\n" % (int(message) - to_subtract))
            return

    assert 0  # Job failed to get info from the service


class ToySerializableService(Job.Service):
    def __init__(self, messageInt: str, *args: Any, **kwargs: Any) -> None:
        """
        Trivial service for testing serialization.
        """
        Job.Service.__init__(self, *args, **kwargs)
        self.messageInt = messageInt

    def start(self, job: Job) -> str:
        return self.messageInt

    def stop(self, job: Job) -> None:
        pass

    def check(self) -> Literal[True]:
        return True


def fnTest(strings: list[str], outputFile: Path) -> None:
    """
    Function concatenates the strings together and writes them to the output file
    """
    with outputFile.open("w") as fH:
        fH.write(" ".join(strings))
