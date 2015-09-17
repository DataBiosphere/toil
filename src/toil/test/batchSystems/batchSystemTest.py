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
from abc import ABCMeta, abstractmethod
from fractions import Fraction
import logging
import os
import tempfile
from textwrap import dedent
import time
import multiprocessing
import sys

from toil.common import Config
from toil.batchSystems.mesos.test import MesosTestSupport
from toil.batchSystems.parasolTestSupport import ParasolTestSupport
from toil.batchSystems.parasol import ParasolBatchSystem
from toil.batchSystems.singleMachine import SingleMachineBatchSystem
from toil.batchSystems.abstractBatchSystem import InsufficientSystemResources
from toil.test import ToilTest, needs_mesos, needs_parasol, needs_gridengine

log = logging.getLogger(__name__)

# How many cores should be utilized by this test. The test will fail if the running system
# doesn't have at least that many cores.
#
numCores = 2

memoryForJobs = 100e6

diskForJobs = 1000

class hidden:
    """
    Hide abstract base class from unittest's test case loader

    http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class#answer-25695512
    """

    class AbstractBatchSystemTest(ToilTest):
        """
        A base test case with generic tests that every batch system should pass
        """
        __metaclass__ = ABCMeta

        @abstractmethod
        def createBatchSystem(self):
            """
            :rtype: AbstractBatchSystem
            """
            raise NotImplementedError

        def _createDummyConfig(self):
            return Config()

        def setUp(self):
            super(hidden.AbstractBatchSystemTest, self).setUp()
            self.config = self._createDummyConfig()
            self.batchSystem = self.createBatchSystem()
            self.tempDir = self._createTempDir('testFiles')

        def tearDown(self):
            self.batchSystem.shutdown()
            super(hidden.AbstractBatchSystemTest, self).tearDown()

        def testAvailableCores(self):
            self.assertTrue(multiprocessing.cpu_count() >= numCores)

        def testRunJobs(self):
            testPath = os.path.join(self.tempDir, "test.txt")

            job1 = self.batchSystem.issueBatchJob("sleep 1000",
                                                  memory=memoryForJobs, cores=1, disk=diskForJobs)
            job2 = self.batchSystem.issueBatchJob("sleep 1000",
                                                  memory=memoryForJobs, cores=1, disk=diskForJobs)

            issuedIDs = self._waitForJobsToIssue(2)
            self.assertEqual(set(issuedIDs), {job1, job2})

            runningJobIDs = self._waitForJobsToStart(2)
            self.assertEqual(set(runningJobIDs), {job1, job2})

            # Killing the jobs instead of allowing them to complete means this
            # test can run very quickly if the batch system issues and starts
            # the jobs quickly.
            self.batchSystem.killBatchJobs([job1, job2])
            self.assertEqual({}, self.batchSystem.getRunningBatchJobIDs())

            # Issue a job and then allow it to finish by itself, causing
            # it to be added to the updated jobs queue.
            self.assertFalse(os.path.exists(testPath))
            job3 = self.batchSystem.issueBatchJob("touch %s" % testPath,
                                                  memory=memoryForJobs, cores=1, disk=diskForJobs)

            updatedID, exitStatus = self.batchSystem.getUpdatedBatchJob(maxWait=1000)

            # Since the first two jobs were killed, the only job in the updated jobs
            # queue should be job 3. If the first two jobs were (incorrectly) added
            # to the queue, this will fail with updatedID being equal to job1 or job2.
            self.assertEqual(exitStatus, 0)
            self.assertEqual(updatedID, job3)
            self.assertTrue(os.path.exists(testPath))
            self.assertFalse(self.batchSystem.getUpdatedBatchJob(0))

            #Make sure killBatchJobs can handle jobs that don't exist
            self.batchSystem.killBatchJobs([10])

        def testCheckResourceRequest(self):
            self.assertRaises(InsufficientSystemResources,
                              self.batchSystem.checkResourceRequest,
                              memory=1000, cores=200, disk=1e9)
            self.assertRaises(InsufficientSystemResources,
                              self.batchSystem.checkResourceRequest,
                              memory=5, cores=200, disk=1e9)
            self.assertRaises(InsufficientSystemResources,
                              self.batchSystem.checkResourceRequest,
                              memory=1001e9, cores=1, disk=1e9)
            self.assertRaises(InsufficientSystemResources,
                              self.batchSystem.checkResourceRequest,
                              memory=5, cores=1, disk=2e9)
            self.assertRaises(AssertionError,
                              self.batchSystem.checkResourceRequest,
                              memory=None, cores=1, disk=1000)
            self.assertRaises(AssertionError,
                              self.batchSystem.checkResourceRequest,
                              memory=10, cores=None, disk=1000)
            self.batchSystem.checkResourceRequest(memory=10, cores=1, disk=100)

        def testGetRescueJobFrequency(self):
            self.assertTrue(self.batchSystem.getRescueBatchJobFrequency() > 0)

        def _waitForJobsToIssue(self, numJobs):
            issuedIDs = []
            while not len(issuedIDs) == numJobs:
                issuedIDs = self.batchSystem.getIssuedBatchJobIDs()
            return issuedIDs

        def _waitForJobsToStart(self, numJobs):
            runningIDs = []
            while not len(runningIDs) == numJobs:
                time.sleep(0.1)
                runningIDs = self.batchSystem.getRunningBatchJobIDs().keys()
            return runningIDs

@needs_mesos
class MesosBatchSystemTest(hidden.AbstractBatchSystemTest, MesosTestSupport):
    """
    Tests against the Mesos batch system
    """

    def createBatchSystem(self):
        from toil.batchSystems.mesos.batchSystem import MesosBatchSystem
        self._startMesos(numCores)
        return MesosBatchSystem(config=self.config, maxCores=numCores, maxMemory=1e9, maxDisk=1001,
                                masterIP='127.0.0.1:5050')

    def tearDown(self):
        self._stopMesos()
        super(MesosBatchSystemTest, self).tearDown()


class SingleMachineBatchSystemTest(hidden.AbstractBatchSystemTest):
    def createBatchSystem(self):
        return SingleMachineBatchSystem(config=self.config, maxCores=numCores, maxMemory=1e9,
                                        maxDisk=1001)

class MaxCoresSingleMachineBatchSystemTest(ToilTest):
    """
    This test ensures that single machine batch system doesn't exceed more than maxCores.
    """

    @classmethod
    def setUpClass(cls):
        super(MaxCoresSingleMachineBatchSystemTest, cls).setUpClass()
        logging.basicConfig(level=logging.DEBUG)

    def setUp(self):
        super(MaxCoresSingleMachineBatchSystemTest, self).setUp()

        def writeTempFile(s):
            fd, path = tempfile.mkstemp()
            try:
                assert os.write(fd, s) == len(s)
            except:
                os.unlink(path)
                raise
            else:
                return path
            finally:
                os.close(fd)

        # Write initial value of counter file containing a tuple of two integers (i, n) where i
        # is the number of currently executing tasks and n the maximum observed value of i
        self.counterPath = writeTempFile('0,0')

        # Write test script that increments i, adjusts n, sleeps 1s and then decrements i again
        self.scriptPath = writeTempFile(dedent("""
            import os, sys, fcntl, time
            def count(delta):
                fd = os.open(sys.argv[1], os.O_RDWR)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX)
                    try:
                        s = os.read(fd, 10)
                        value, maxValue = map(int, s.split(','))
                        value += delta
                        if value > maxValue: maxValue = value
                        os.lseek(fd,0,0)
                        os.ftruncate(fd, 0)
                        os.write(fd, ','.join(map(str, (value, maxValue))))
                    finally:
                        fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)
            count(1)
            try:
                time.sleep(1)
            finally:
                count(-1)
        """))

    def tearDown(self):
        os.unlink(self.scriptPath)
        os.unlink(self.counterPath)

    def test(self):
        # We'll use fractions to avoid rounding errors. Remember that only certain, discrete
        # fractions can be represented as a floating point number.
        F = Fraction
        # This test isn't general enought to cover every possible value of minCores in
        # SingleMachineBatchSystem. Instead we hard-code a value and assert it.
        minCores = F(1, 10)
        self.assertEquals(float(minCores), SingleMachineBatchSystem.minCores)
        for maxCores in {F(minCores), minCores * 10, F(1), F(numCores, 2), F(numCores)}:
            for coresPerJob in {F(minCores), F(minCores * 10), F(1), F(maxCores, 2), F(maxCores)}:
                for load in (F(1, 10), F(1), F(10)):
                    jobs = int(maxCores / coresPerJob * load)
                    if jobs >= 1 and minCores <= coresPerJob < maxCores:
                        self.assertEquals(maxCores, float(maxCores))
                        bs = SingleMachineBatchSystem(config=Config(),
                                                      maxCores=float(maxCores),
                                                      # Ensure that memory or disk requirements
                                                      # don't get in the way.
                                                      maxMemory=jobs * 10,
                                                      maxDisk=jobs * 10)
                        try:
                            jobIds = set()
                            for i in range(0, int(jobs)):
                                cmd = ' '.join([sys.executable, self.scriptPath, self.counterPath])
                                jobIds.add(bs.issueBatchJob(command=cmd,
                                                            cores=float(coresPerJob),
                                                            memory=1,
                                                            disk=1))
                            self.assertEquals(len(jobIds), jobs)
                            while jobIds:
                                job = bs.getUpdatedBatchJob(maxWait=10)
                                self.assertIsNotNone(job)
                                jobId, status = job
                                self.assertEquals(status, 0)
                                # would raise KeyError on absence
                                jobIds.remove(jobId)
                        finally:
                            bs.shutdown()
                        with open(self.counterPath, 'r+') as f:
                            s = f.read()
                            log.info('Counter is %s', s)
                            concurrentTasks, maxConcurrentTasks = map(int, s.split(','))
                            self.assertEquals(concurrentTasks, 0)
                            log.info("maxCores: {maxCores}, "
                                     "coresPerJob: {coresPerJob}, "
                                     "load: {load}".format(**locals()))
                            # This is the key assertion
                            expectedMaxConcurrentTasks = min(maxCores / coresPerJob, jobs)
                            self.assertEquals(maxConcurrentTasks, expectedMaxConcurrentTasks)
                            # Reset the counter
                            f.seek(0)
                            f.truncate(0)
                            f.write('0,0')

@needs_parasol
class ParasolBatchSystemTest(hidden.AbstractBatchSystemTest, ParasolTestSupport):
    """
    Tests the Parasol batch system
    """

    def _createDummyConfig(self):
        config = super(ParasolBatchSystemTest, self)._createDummyConfig()
        # can't use _getTestJobStorePath since that method removes the directory
        config.jobStore = self._createTempDir('jobStore')
        return config

    def createBatchSystem(self):
        self._startParasol(numCores)
        return ParasolBatchSystem(config=self.config, maxCores=numCores, maxMemory=3e9,
                                  maxDisk=1001)

    def tearDown(self):
        super(ParasolBatchSystemTest, self).tearDown()
        self._stopParasol()

    def testBatchResourceLimits(self):
        #self.batchSystem.issueBatchJob("sleep 100", memory=1e9, cores=1, disk=1000)
        job1 = self.batchSystem.issueBatchJob("sleep 1000", memory=1e9, cores=1, disk=1000)
        job2 = self.batchSystem.issueBatchJob("sleep 1000", memory=2e9, cores=1, disk=1000)

        batches = self._getBatchList()
        self.assertEqual(len(batches), 2)
        #It would be better to directly check that the batches
        #have the correct memory and cpu values, but parasol seems
        #to slightly change the values sometimes.
        self.assertTrue(batches[0]["ram"] != batches[1]["ram"])

        #need to kill one of the jobs because there are only two cores available
        self.batchSystem.killBatchJobs([job2])
        job3 = self.batchSystem.issueBatchJob("sleep 1000", memory=1e9, cores=1, disk=1000)
        batches = self._getBatchList()
        self.assertEqual(len(batches), 1)

    def _parseBatchString(self, batchString):
        import re
        batchInfo = dict()
        memPattern = re.compile("(\d+\.\d+)([kgmbt])")
        items = batchString.split()
        batchInfo["cores"] = int(items[7])
        name = str(items[11])
        memMatch = memPattern.match(items[8])
        ramValue = float(memMatch.group(1))
        ramUnits = memMatch.group(2)
        ramConversion = {'b':1e0, 'k':1e3, 'm':1e6, 'g':1e9, 't':1e12}
        batchInfo["ram"] = ramValue * ramConversion[ramUnits]
        return batchInfo

    def _getBatchList(self):
        from toil.batchSystems.parasol import popenParasolCommand
        exitStatus, batchLines = popenParasolCommand("parasol list batches")
        return [self._parseBatchString(line) for line in batchLines[1:] if not line == ""]

@needs_gridengine
class GridEngineTest(hidden.AbstractBatchSystemTest):
    """
    Tests against the GridEngine batch system
    """

    def _createDummyConfig(self):
        config = super(GridEngineTest, self)._createDummyConfig()
        # can't use _getTestJobStorePath since that method removes the directory
        config.jobStore = self._createTempDir('jobStore')
        return config

    def createBatchSystem(self):
        from toil.batchSystems.gridengine import GridengineBatchSystem
        return GridengineBatchSystem(config=self.config, maxCores=numCores, maxMemory=1000e9,
                                     maxDisk=1e9)

    @classmethod
    def setUpClass(cls):
        super(GridEngineTest, cls).setUpClass()
        logging.basicConfig(level=logging.DEBUG)
