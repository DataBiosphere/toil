# Copyright (C) 2015 UCSC Computational Genomics Lab
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
from __future__ import absolute_import
import os
import random
from toil.lib.bioio import getTempFile
from toil.job import Job
from toil.test import ToilTest
import time
import sys
from threading import Thread, Event
import logging
logger = logging.getLogger( __name__ )
from unittest import skipIf
from toil.batchSystems.singleMachine import SingleMachineBatchSystem
from toil.leader import FailedJobsException

class JobServiceTest(ToilTest):
    """
    Tests testing the Job.Service class
    """
    
    def testServiceSerialization(self):
        """
        Tests that a service can receive a promise without producing a serialization
        error.
        """
        job = Job()
        service = TestServiceSerialization("woot")
        startValue = job.addService(service) # Add a first service to job
        subService = TestServiceSerialization(startValue) # Now create a child of 
        # that service that takes the start value promise from the parent service
        job.addService(subService, parentService=service) # This should work if
        # serialization on services is working correctly.
        
        self.runToil(job)
        
    def testService(self, checkpoint=False):
        """
        Tests the creation of a Job.Service with random failures of the worker.
        """
        for test in xrange(2):
            outFile = getTempFile(rootDir=self._createTempDir()) # Temporary file
            messageInt = random.randint(1, sys.maxint)
            try:
                # Wire up the services/jobs
                t = Job.wrapJobFn(serviceTest, outFile, messageInt, checkpoint=checkpoint)

                # Run the workflow repeatedly until success
                self.runToil(t)

                # Check output
                self.assertEquals(int(open(outFile, 'r').readline()), messageInt)
            finally:
                os.remove(outFile)

    def testServiceWithCheckpoints(self):
        """
        Tests the creation of a Job.Service with random failures of the worker, making the root job use checkpointing to 
        restart the subtree.
        """
        self.testService(checkpoint=True)

    @skipIf(SingleMachineBatchSystem.numCores < 4, 'Need at least four cores to run this test')
    def testServiceRecursive(self, checkpoint=True):
        """
        Tests the creation of a Job.Service, creating a chain of services and accessing jobs.
        Randomly fails the worker.
        """
        for test in xrange(1):
            # Temporary file
            outFile = getTempFile(rootDir=self._createTempDir())
            messages = [ random.randint(1, sys.maxint) for i in xrange(3) ]
            try:
                # Wire up the services/jobs
                t = Job.wrapJobFn(serviceTestRecursive, outFile, messages, checkpoint=checkpoint)

                # Run the workflow repeatedly until success
                self.runToil(t)

                # Check output
                self.assertEquals(map(int, open(outFile, 'r').readlines()), messages)
            finally:
                os.remove(outFile)

    @skipIf(SingleMachineBatchSystem.numCores < 4, 'Need at least four cores to run this test')
    def testServiceParallelRecursive(self, checkpoint=True):
        """
        Tests the creation of a Job.Service, creating parallel chains of services and accessing jobs.
        Randomly fails the worker.
        """
        for test in xrange(1):
            # Temporary file
            outFiles = [ getTempFile(rootDir=self._createTempDir()) for j in xrange(2) ]
            messageBundles = [ [ random.randint(1, sys.maxint) for i in xrange(3) ] for j in xrange(2) ]
            try:
                # Wire up the services/jobs
                t = Job.wrapJobFn(serviceTestParallelRecursive, outFiles, messageBundles, checkpoint=True)

                # Run the workflow repeatedly until success
                self.runToil(t, retryCount=2)

                # Check output
                for (messages, outFile) in zip(messageBundles, outFiles):
                    self.assertEquals(map(int, open(outFile, 'r').readlines()), messages)
            finally:
                map(os.remove, outFiles)

    def runToil(self, rootJob, retryCount=1, badWorker=0.5, badWorkedFailInterval=0.05):
        # Create the runner for the workflow.
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"

        options.retryCount = retryCount
        options.badWorker = badWorker
        options.badWorkerFailInterval = badWorkedFailInterval
        options.servicePollingInterval = 1

        # Run the workflow
        totalTrys = 0
        while True:
            try:
                Job.Runner.startToil(rootJob, options)
                break
            except FailedJobsException as e:
                i = e.numberOfFailedJobs
                if totalTrys > 40: #p(fail after this many restarts) = 0.5**32
                    self.fail() #Exceeded a reasonable number of restarts
                totalTrys += 1
                options.restart = True

def serviceTest(job, outFile, messageInt):
    """
    Creates one service and one accessing job, which communicate with two files to establish
    that both run concurrently. 
    """
    #Clean out out-file
    open(outFile, 'w').close()
    randInt = random.randint(1, sys.maxint) # We create a random number that is added to messageInt and subtracted by the serviceAccessor, to prove that
    # when service test is checkpointed and restarted there is never a connection made between an earlier service and later serviceAccessor, or vice versa.
    job.addChildJobFn(serviceAccessor, job.addService(TestService(messageInt + randInt)), outFile, randInt)

def serviceTestRecursive(job, outFile, messages):
    """
    Creates a chain of services and accessing jobs, each paired together.
    """
    if len(messages) > 0:
        #Clean out out-file
        open(outFile, 'w').close()
        randInt = random.randint(1, sys.maxint)
        service = TestService(messages[0] + randInt)
        child = job.addChildJobFn(serviceAccessor, job.addService(service), outFile, randInt)

        for i in xrange(1, len(messages)):
            randInt = random.randint(1, sys.maxint)
            service2 = TestService(messages[i] + randInt, cores=0.1)
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
            randInt = random.randint(1, sys.maxint)
            service = TestService(messages[0] + randInt)
            child = job.addChildJobFn(serviceAccessor, job.addService(service), outFile, randInt)

            for i in xrange(1, len(messages)):
                randInt = random.randint(1, sys.maxint)
                service2 = TestService(messages[i] + randInt, cores=0.1)
                child = child.addChildJobFn(serviceAccessor,
                                            job.addService(service2, parentService=service),
                                            outFile, randInt, cores=0.1)
                service = service2

class TestService(Job.Service):
    def __init__(self, messageInt, *args, **kwargs):
        """
        While established the service repeatedly:
             - reads an integer i from the inJobStoreFileID file
             - writes i and messageInt to the outJobStoreFileID file
        """
        Job.Service.__init__(self, *args, **kwargs)
        self.messageInt = messageInt

    def start(self, fileStore):
        self.terminate = Event()
        self.error = Event()
        inJobStoreID = fileStore.jobStore.getEmptyFileStoreID()
        outJobStoreID = fileStore.jobStore.getEmptyFileStoreID()
        self.serviceThread = Thread(target=self.serviceWorker,
                                    args=(fileStore.jobStore, self.terminate, self.error,
                                          inJobStoreID, outJobStoreID,
                                          self.messageInt))
        self.serviceThread.start()
        return (inJobStoreID, outJobStoreID)

    def stop(self, fileStore):
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
                    logger.debug("Demo service worker being told to quit")
                    return

                time.sleep(0.2) # Sleep to avoid thrashing

                # Try reading a line from the input file
                try:
                    with jobStore.readFileStream(inJobStoreID) as fH:
                        line = fH.readline()
                except:
                    logger.debug("Something went wrong reading a line")
                    raise

                # Try converting the input line into an integer
                try:
                    inputInt = int(line)
                except ValueError:
                    logger.debug("Tried casting input line to integer but got error: %s" % line)
                    continue

                # Write out the resulting read integer and the message              
                with jobStore.updateFileStream(outJobStoreID) as fH:
                    fH.write("%s %s\n" % (inputInt, messageInt))
        except:
            error.set()
            raise

def serviceAccessor(job, communicationFiles, outFile, randInt):
    """
    Writes a random integer iinto the inJobStoreFileID file, then tries 10 times reading
    from outJobStoreFileID to get a pair of integers, the first equal to i the second written into the outputFile.
    """
    inJobStoreFileID, outJobStoreFileID = communicationFiles

    # Get a random integer
    key = random.randint(1, sys.maxint)

    # Write the integer into the file
    logger.debug("Writing key to inJobStoreFileID")
    with job.fileStore.jobStore.updateFileStream(inJobStoreFileID) as fH:
        fH.write("%s\n" % key)

    logger.debug("Trying to read key and message from outJobStoreFileID")
    for i in xrange(10): # Try 10 times over
        time.sleep(0.2) #Avoid thrashing

        # Try reading an integer from the input file and writing out the message
        with job.fileStore.jobStore.readFileStream(outJobStoreFileID) as fH:
            line = fH.readline()

        tokens = line.split()
        if len(tokens) != 2:
            continue

        key2, message = tokens

        if int(key2) == key:
            logger.debug("Matched key's: %s, writing message: %s with randInt: %s" % (key, int(message) - randInt, randInt))
            with open(outFile, 'a') as fH:
                fH.write("%s\n" % (int(message) - randInt))
            return

    assert 0 # Job failed to get info from the service
    
class TestServiceSerialization(Job.Service):
    def __init__(self, messageInt, *args, **kwargs):
        """
        Trivial service for testing serialization.
        """
        Job.Service.__init__(self, *args, **kwargs)
        self.messageInt = messageInt

    def start(self, fileStore):
        return self.messageInt

    def stop(self, fileStore):
        pass

    def check(self):
        return True
