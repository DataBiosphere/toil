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
import os
import random
import sys
import time
import traceback
from threading import Event, Thread
from unittest import skipIf

import pytest

from toil.batchSystems.singleMachine import SingleMachineBatchSystem
from toil.job import Job
from toil.leader import DeadlockException
from toil.exceptions import FailedJobsException
from toil.lib.retry import retry_flaky_test
from toil.test import ToilTest, get_temp_file, slow

logger = logging.getLogger(__name__)


class JobServiceTest(ToilTest):
    """
    Tests testing the Job.Service class
    """

    @slow
    def testServiceSerialization(self):
        """
        Tests that a service can receive a promise without producing a serialization
        error.
        """
        job = Job()
        service = ToySerializableService("woot")
        startValue = job.addService(service) # Add a first service to job
        subService = ToySerializableService(startValue) # Now create a child of
        # that service that takes the start value promise from the parent service
        job.addService(subService, parentService=service) # This should work if
        # serialization on services is working correctly.

        self.runToil(job)

    @slow
    def testService(self, checkpoint=False):
        """
        Tests the creation of a Job.Service with random failures of the worker.
        """
        for test in range(2):
            outFile = get_temp_file(rootDir=self._createTempDir()) # Temporary file
            messageInt = random.randint(1, sys.maxsize)
            try:
                # Wire up the services/jobs
                t = Job.wrapJobFn(serviceTest, outFile, messageInt, checkpoint=checkpoint)

                # Run the workflow repeatedly until success
                self.runToil(t)

                # Check output
                self.assertEqual(int(open(outFile).readline()), messageInt)
            finally:
                os.remove(outFile)

    @slow
    @skipIf(SingleMachineBatchSystem.numCores < 4, 'Need at least four cores to run this test')
    def testServiceDeadlock(self):
        """
        Creates a job with more services than maxServices, checks that deadlock is detected.
        """
        outFile = get_temp_file(rootDir=self._createTempDir())
        try:
            def makeWorkflow():
                job = Job()
                r1 = job.addService(ToySerializableService("woot1"))
                r2 = job.addService(ToySerializableService("woot2"))
                r3 = job.addService(ToySerializableService("woot3"))
                job.addChildFn(fnTest, [ r1, r2, r3 ], outFile)
                return job

            # This should fail as too few services available
            try:
                self.runToil(makeWorkflow(), badWorker=0.0, maxServiceJobs=2, deadlockWait=5)
            except DeadlockException:
                print("Got expected deadlock exception")
            else:
                assert 0

            # This should pass, as adequate services available
            self.runToil(makeWorkflow(), maxServiceJobs=3)
            # Check we get expected output
            assert open(outFile).read() == "woot1 woot2 woot3"
        finally:
            os.remove(outFile)

    def testServiceWithCheckpoints(self):
        """
        Tests the creation of a Job.Service with random failures of the worker, making the root job use checkpointing to
        restart the subtree.
        """
        self.testService(checkpoint=True)

    @slow
    @skipIf(SingleMachineBatchSystem.numCores < 4, 'Need at least four cores to run this test')
    def testServiceRecursive(self, checkpoint=True):
        """
        Tests the creation of a Job.Service, creating a chain of services and accessing jobs.
        Randomly fails the worker.
        """
        for test in range(1):
            # Temporary file
            outFile = get_temp_file(rootDir=self._createTempDir())
            messages = [ random.randint(1, sys.maxsize) for i in range(3) ]
            try:
                # Wire up the services/jobs
                t = Job.wrapJobFn(serviceTestRecursive, outFile, messages, checkpoint=checkpoint)

                # Run the workflow repeatedly until success
                self.runToil(t)

                # Check output
                self.assertEqual(list(map(int, open(outFile).readlines())), messages)
            finally:
                os.remove(outFile)

    @slow
    @skipIf(SingleMachineBatchSystem.numCores < 4, 'Need at least four cores to run this test')
    @retry_flaky_test(prepare=[ToilTest.tearDown, ToilTest.setUp])
    @pytest.mark.timeout(1200)
    def testServiceParallelRecursive(self, checkpoint=True):
        """
        Tests the creation of a Job.Service, creating parallel chains of services and accessing jobs.
        Randomly fails the worker.
        """
        for test in range(1):
            # Temporary file
            outFiles = [get_temp_file(rootDir=self._createTempDir()) for j in range(2)]
            messageBundles = [ [ random.randint(1, sys.maxsize) for i in range(3) ] for j in range(2) ]
            try:
                # Wire up the services/jobs
                t = Job.wrapJobFn(serviceTestParallelRecursive, outFiles, messageBundles, checkpoint=True)

                # Run the workflow repeatedly until success
                self.runToil(t, retryCount=2)

                # Check output
                for (messages, outFile) in zip(messageBundles, outFiles):
                    self.assertEqual(list(map(int, open(outFile).readlines())), messages)
            finally:
                list(map(os.remove, outFiles))

    def runToil(self, rootJob, retryCount=1, badWorker=0.5, badWorkedFailInterval=0.1, maxServiceJobs=sys.maxsize, deadlockWait=60):
        # Create the runner for the workflow.
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "DEBUG"

        options.retryCount = retryCount
        options.badWorker = badWorker
        options.badWorkerFailInterval = badWorkedFailInterval
        options.servicePollingInterval = 1
        options.maxServiceJobs = maxServiceJobs
        options.deadlockWait=deadlockWait

        # Run the workflow
        totalTrys = 0
        while True:
            try:
                Job.Runner.startToil(rootJob, options)
                break
            except FailedJobsException as e:
                i = e.numberOfFailedJobs
                if totalTrys > 50: #p(fail after this many restarts) = 0.5**32
                    self.fail() #Exceeded a reasonable number of restarts
                totalTrys += 1
                options.restart = True

class PerfectServiceTest(JobServiceTest):
    def runToil(self, rootJob, retryCount=1, badWorker=0, badWorkedFailInterval=1000, maxServiceJobs=sys.maxsize, deadlockWait=60):
        """
        Let us run all the tests in the other service test class, but without worker failures.
        """
        super().runToil(rootJob, retryCount, badWorker, badWorkedFailInterval, maxServiceJobs, deadlockWait)


def serviceTest(job, outFile, messageInt):
    """
    Creates one service and one accessing job, which communicate with two files to establish
    that both run concurrently.
    """
    #Clean out out-file
    open(outFile, 'w').close()
    randInt = random.randint(1, sys.maxsize) # We create a random number that is added to messageInt and subtracted by the serviceAccessor, to prove that
    # when service test is checkpointed and restarted there is never a connection made between an earlier service and later serviceAccessor, or vice versa.
    job.addChildJobFn(serviceAccessor, job.addService(ToyService(messageInt + randInt)), outFile, randInt)

def serviceTestRecursive(job, outFile, messages):
    """
    Creates a chain of services and accessing jobs, each paired together.
    """
    if len(messages) > 0:
        #Clean out out-file
        open(outFile, 'w').close()
        randInt = random.randint(1, sys.maxsize)
        service = ToyService(messages[0] + randInt)
        child = job.addChildJobFn(serviceAccessor, job.addService(service), outFile, randInt)

        for i in range(1, len(messages)):
            randInt = random.randint(1, sys.maxsize)
            service2 = ToyService(messages[i] + randInt, cores=0.1)
            child = child.addChildJobFn(serviceAccessor,
                                        job.addService(service2, parentService=service),
                                        outFile, randInt, cores=0.1)
            service = service2

def serviceTestParallelRecursive(job, outFiles, messageBundles):
    """
    Creates multiple chains of services and accessing jobs.
    """
    for messages, outFile in zip(messageBundles, outFiles):
        #Clean out out-file
        open(outFile, 'w').close()
        if len(messages) > 0:
            randInt = random.randint(1, sys.maxsize)
            service = ToyService(messages[0] + randInt)
            child = job.addChildJobFn(serviceAccessor, job.addService(service), outFile, randInt)

            for i in range(1, len(messages)):
                randInt = random.randint(1, sys.maxsize)
                service2 = ToyService(messages[i] + randInt, cores=0.1)
                child = child.addChildJobFn(serviceAccessor,
                                            job.addService(service2, parentService=service),
                                            outFile, randInt, cores=0.1)
                service = service2

class ToyService(Job.Service):
    def __init__(self, messageInt, *args, **kwargs):
        """
        While established the service repeatedly:
             - reads an integer i from the inJobStoreFileID file
             - writes i and messageInt to the outJobStoreFileID file
        """
        Job.Service.__init__(self, *args, **kwargs)
        self.messageInt = messageInt

    def start(self, job):
        assert self.disk is not None
        assert self.memory is not None
        assert self.cores is not None
        self.terminate = Event()
        self.error = Event()
        # Note that service jobs are special and do not necessarily have job.jobStoreID.
        # So we don't associate these files with this job.
        inJobStoreID = job.fileStore.jobStore.get_empty_file_store_id()
        outJobStoreID = job.fileStore.jobStore.get_empty_file_store_id()
        self.serviceThread = Thread(target=self.serviceWorker,
                                    args=(job.fileStore.jobStore, self.terminate, self.error,
                                          inJobStoreID, outJobStoreID,
                                          self.messageInt))
        self.serviceThread.start()
        return (inJobStoreID, outJobStoreID)

    def stop(self, job):
        self.terminate.set()
        self.serviceThread.join()

    def check(self):
        if self.error.isSet():
            raise RuntimeError("Service worker failed")
        return True

    @staticmethod
    def serviceWorker(jobStore, terminate, error, inJobStoreID, outJobStoreID, messageInt):
        try:
            while True:
                if terminate.isSet(): # Quit if we've got the terminate signal
                    logger.debug("Service worker being told to quit")
                    return

                time.sleep(0.2) # Sleep to avoid thrashing

                # Try reading a line from the input file
                try:
                    with jobStore.read_file_stream(inJobStoreID) as f:
                        f = codecs.getreader('utf-8')(f)
                        line = f.readline()
                except:
                    logger.debug("Something went wrong reading a line: %s", traceback.format_exc())
                    raise

                if len(line.strip()) == 0:
                    # Don't try and make an integer out of nothing
                    continue

                # Try converting the input line into an integer
                try:
                    inputInt = int(line)
                except ValueError:
                    logger.debug("Tried casting input line '%s' to integer but got error: %s", line, traceback.format_exc())
                    continue

                # Write out the resulting read integer and the message
                with jobStore.update_file_stream(outJobStoreID) as f:
                    f.write((f"{inputInt} {messageInt}\n").encode())
        except:
            logger.debug("Error in service worker: %s", traceback.format_exc())
            error.set()
            raise

def serviceAccessor(job, communicationFiles, outFile, randInt):
    """
    Writes a random integer iinto the inJobStoreFileID file, then tries 10 times reading
    from outJobStoreFileID to get a pair of integers, the first equal to i the second written into the outputFile.
    """
    inJobStoreFileID, outJobStoreFileID = communicationFiles

    # Get a random integer
    key = random.randint(1, sys.maxsize)

    # Write the integer into the file
    logger.debug("Writing key to inJobStoreFileID")
    with job.fileStore.jobStore.update_file_stream(inJobStoreFileID) as fH:
        fH.write(("%s\n" % key).encode('utf-8'))

    logger.debug("Trying to read key and message from outJobStoreFileID")
    for i in range(10): # Try 10 times over
        time.sleep(0.2) #Avoid thrashing

        # Try reading an integer from the input file and writing out the message
        with job.fileStore.jobStore.read_file_stream(outJobStoreFileID) as fH:
            fH = codecs.getreader('utf-8')(fH)
            line = fH.readline()

        tokens = line.split()
        if len(tokens) != 2:
            continue

        key2, message = tokens

        if int(key2) == key:
            logger.debug(f"Matched key's: {key}, writing message: {int(message) - randInt} with randInt: {randInt}")
            with open(outFile, 'a') as fH:
                fH.write("%s\n" % (int(message) - randInt))
            return

    assert 0 # Job failed to get info from the service

class ToySerializableService(Job.Service):
    def __init__(self, messageInt, *args, **kwargs):
        """
        Trivial service for testing serialization.
        """
        Job.Service.__init__(self, *args, **kwargs)
        self.messageInt = messageInt

    def start(self, job):
        return self.messageInt

    def stop(self, job):
        pass

    def check(self):
        return True

def fnTest(strings, outputFile):
    """
    Function concatenates the strings together and writes them to the output file
    """
    with open(outputFile, 'w') as fH:
        fH.write(" ".join(strings))
