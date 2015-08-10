from abc import ABCMeta, abstractmethod
import logging
import os
import shutil
import tempfile
from xml.etree import ElementTree
import time
import multiprocessing

from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem
from toil.batchSystems.mesos.batchSystem import MesosBatchSystem
from toil.batchSystems.mesos.test import MesosTestSupport
from toil.batchSystems.singleMachine import SingleMachineBatchSystem
from toil.batchSystems.abstractBatchSystem import InsufficientSystemResources
from toil.test import ToilTest

log = logging.getLogger(__name__)

# How many cores should be utilized by this test. The test will fail if the running system doesn't have at least that
# many cores.
#
numCores = 2

# How many jobs to run. This is only read by tests that run multiple jobs.
#
numJobs = 2

# How much CPU to allocate for a particular batchjob
#
numCoresPerJob = (numCores) / numJobs


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

        def setUp(self):
            super(hidden.AbstractBatchSystemTest, self).setUp()
            self.batchSystem = self.createBatchSystem()
            self.tempDir = tempfile.mkdtemp()

        config = None
        tempDir = None

        @classmethod
        def setUpClass(cls):
            super(hidden.AbstractBatchSystemTest, cls).setUpClass()
            cls.config = cls._createDummyConfig()
            cls.tempDir = tempfile.mkdtemp()

        def testAvailableCores(self):
            self.assertTrue(multiprocessing.cpu_count() >= numCores)

        def testIssueJob(self):
            test_path = os.path.join(self.tempDir, 'test.txt')
            # sleep 1 coupled to command as 'touch' was too fast for wait_for_jobs to catch
            jobCommand = 'touch {}; sleep 1'.format(test_path)
            self.batchSystem.issueBatchJob(jobCommand, memory=10, cpu=.1, disk=1000)
            self.wait_for_jobs(wait_for_completion=True)
            self.assertTrue(os.path.exists(test_path))

        def testCheckResourceRequest(self):
            self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest, memory=1000, cpu=200, disk=1000)
            self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest, memory=5, cpu=200,disk=1000)
            self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest, memory=100, cpu=1,disk=1000)
            self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest, memory=5, cpu=1,disk=1002)
            self.assertRaises(AssertionError, self.batchSystem.checkResourceRequest, memory=None, cpu=1,disk=1000)
            self.assertRaises(AssertionError, self.batchSystem.checkResourceRequest, memory=10, cpu=None,disk=1000)
            self.batchSystem.checkResourceRequest(memory=10, cpu=1, disk=100)

        def testGetIssuedJobIDs(self):
            self.batchSystem.issueBatchJob('sleep 1', memory=10, cpu=numCoresPerJob, disk=1000)
            self.batchSystem.issueBatchJob('sleep 1', memory=10, cpu=numCoresPerJob, disk=1000)
            self.assertEqual({0, 1}, set(self.batchSystem.getIssuedBatchJobIDs()))

        def testGetRunningJobIDs(self):
            self.batchSystem.issueBatchJob('sleep 100', memory=10, cpu=.1, disk=1000)
            self.batchSystem.issueBatchJob('sleep 100', memory=10, cpu=.1, disk=1000)
            self.wait_for_jobs(numJobs=2)
            # Assert that jobs were correctly labeled by JobID
            self.assertEqual({0, 1}, set(self.batchSystem.getRunningBatchJobIDs().keys()))
            # Assert that the length of the batchjob was recorded
            self.assertTrue(len([t for t in self.batchSystem.getRunningBatchJobIDs().values() if t > 0]) == 2)
            self.batchSystem.killBatchJobs([0, 1])

        def testKillJobs(self):
            jobCommand = 'sleep 100'
            jobID = self.batchSystem.issueBatchJob(jobCommand, memory=10, cpu=.1, disk=1000)
            self.wait_for_jobs()
            # self.assertEqual([0], self.batchSystem.getRunningJobIDs().keys())
            self.batchSystem.killBatchJobs([jobID])
            self.assertEqual({}, self.batchSystem.getRunningBatchJobIDs())
            # Make sure that killJob doesn't hang / raise KeyError on unknown batchjob IDs
            self.batchSystem.killBatchJobs([0])

        def testGetUpdatedJob(self):
            delay = 1
            jobCommand = 'sleep %i' % delay
            for i in range(numJobs):
                self.batchSystem.issueBatchJob(jobCommand, memory=10, cpu=numCoresPerJob, disk=1000)
            jobs = set((i, 0) for i in range(numJobs))
            self.wait_for_jobs(numJobs=numJobs, wait_for_completion=True)
            for i in range(numJobs):
                jobs.remove(self.batchSystem.getUpdatedBatchJob(delay * 2))
            self.assertFalse(jobs)

        def testGetRescueJobFrequency(self):
            self.assertTrue(self.batchSystem.getRescueBatchJobFrequency() > 0)

        @staticmethod
        def _createDummyConfig():
            config = ElementTree.Element("config")
            config.attrib["log_level"] = 'DEBUG'
            config.attrib["job_store"] = '.'
            config.attrib["parasol_command"] = None
            config.attrib["try_count"] = str(2)
            config.attrib["max_job_duration"] = str(1)
            config.attrib["batch_system"] = None
            config.attrib["max_log_file_size"] = str(1)
            config.attrib["default_memory"] = str(1)
            config.attrib["default_cpu"] = str(1)
            config.attrib["max_cpus"] = str(1)
            config.attrib["max_memory"] = str(1)
            config.attrib["scale"] = str(1)
            return config

        def wait_for_jobs(self, numJobs=1, wait_for_completion=False):
            while not self.batchSystem.getIssuedBatchJobIDs():
                pass
            while not len(self.batchSystem.getRunningBatchJobIDs().keys()) == numJobs:
                time.sleep(0.1)
            if wait_for_completion:
                while self.batchSystem.getRunningBatchJobIDs():
                    time.sleep(0.1)
                    # pass updates too quickly (~24e6 iter/sec), which is why I'm using time.sleep(0.1):

        def tearDown(self):
            self.batchSystem.shutdown()

        @classmethod
        def tearDownClass(cls):
            shutil.rmtree(cls.tempDir)


class MesosBatchSystemTest(hidden.AbstractBatchSystemTest, MesosTestSupport):
    """
    Tests against the Mesos batch system
    """

    def createBatchSystem(self):
        self._startMesos(numCores)
        return MesosBatchSystem(config=self.config, maxCpus=numCores, maxMemory=20, maxDisk=1001,masterIP='127.0.0.1:5050')

    def tearDown(self):
        super(MesosBatchSystemTest, self).tearDown()
        self._stopMesos()
        self.batchSystem.shutdown()


class SingleMachineBatchSystemTest(hidden.AbstractBatchSystemTest):
    def createBatchSystem(self):
        return SingleMachineBatchSystem(config=self.config, maxCpus=numCores, maxMemory=50, maxDisk=1001)
