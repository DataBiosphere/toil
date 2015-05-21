from abc import ABCMeta, abstractmethod
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from xml.etree import ElementTree
import time

from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.batchSystems.mesos import MesosBatchSystem
from jobTree.batchSystems.singleMachine import SingleMachineBatchSystem
from jobTree.batchSystems.abstractBatchSystem import InsufficientSystemResources


log = logging.getLogger(__name__)


class AbstractBatchSystemTest(unittest.TestCase):
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
        super(AbstractBatchSystemTest, self).setUp()
        self.batchSystem = self.createBatchSystem()
        self.tempDir = tempfile.mkdtemp( )

    config = None
    tempDir = None

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.DEBUG)
        cls.config = cls._createDummyConfig()
        cls.tempDir = tempfile.mkdtemp()

    def testIssueJob(self):
        test_path = os.path.join(self.tempDir, 'test.txt')
        jobCommand = 'touch {}'.format(test_path)
        self.batchSystem.issueJob(jobCommand, memory=10, cpu=1)
        self.wait_for_jobs(wait_for_completion=True)
        self.assertTrue(os.path.exists(test_path))

    def testCheckResourceRequest(self):
        self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest, memory=1000, cpu=200)
        self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest, memory=5, cpu=200)
        self.assertRaises(InsufficientSystemResources, self.batchSystem.checkResourceRequest, memory=100, cpu=1)
        self.batchSystem.checkResourceRequest(memory=10, cpu=1)

    def testGetIssuedJobIDs(self):
        # TODO: Fix SingleMachineBatchSystem to support this call
        self.batchSystem.issueJob('sleep 1', memory=10, cpu=1)
        self.batchSystem.issueJob('sleep 1', memory=10, cpu=1)
        self.assertEqual([0, 1], self.batchSystem.getIssuedJobIDs())

    def testGetRunningJobsIDs(self):
        # TODO: Fix SingleMachineBatchSystem to support this call
        self.batchSystem.issueJob('sleep 1', memory=10, cpu=1)
        self.batchSystem.issueJob('sleep 1', memory=10, cpu=1)
        self.wait_for_jobs()
        self.assertEqual([0, 1], self.batchSystem.getRunningJobIDs().keys())

    def testKillJobs(self):
        jobCommand = 'sleep 100'
        self.batchSystem.issueJob(jobCommand, memory=10, cpu=1)
        self.wait_for_jobs()
        self.assertEqual([0], self.batchSystem.getRunningJobIDs().keys())
        self.batchSystem.killJobs([0])
        self.assertEqual({}, self.batchSystem.getRunningJobIDs())
        # TODO: Test killing a non-existent job

    def testGetUpdatedJob(self):
        jobCommand = 'sleep 1'
        self.batchSystem.issueJob(jobCommand, memory=10, cpu=1)
        self.batchSystem.issueJob(jobCommand, memory=10, cpu=1)
        self.wait_for_jobs(wait_for_completion=True)
        updated_job = self.batchSystem.getUpdatedJob(1)
        try:
            self.assertEqual((0, 0), updated_job)
        except:  # FIXME: catch more specific exception and document the purpose of this unorthodox idiom
            self.assertEqual((1, 0), updated_job)

    def testGetRescueJobFrequency(self):
        self.assertTrue(self.batchSystem.getRescueJobFrequency() > 0)

    @staticmethod
    def _createDummyConfig():
        config = ElementTree.Element("config")
        config.attrib["log_level"] = 'DEBUG'
        config.attrib["job_store"] = '.'
        config.attrib["parasol_command"] = None
        config.attrib["try_count"] = str(2)
        config.attrib["max_job_duration"] = str(1)
        config.attrib["batch_system"] = None
        config.attrib["job_time"] = str(1)
        config.attrib["max_log_file_size"] = str(1)
        config.attrib["default_memory"] = str(1)
        config.attrib["default_cpu"] = str(1)
        config.attrib["max_cpus"] = str(1)
        config.attrib["max_memory"] = str(1)
        config.attrib["max_threads"] = str(1)
        return config

    def wait_for_jobs(self, wait_for_completion=False):
        while not self.batchSystem.getIssuedJobIDs():
            pass
        while not self.batchSystem.getRunningJobIDs():
            time.sleep(0.1)
        if wait_for_completion:
            while self.batchSystem.getRunningJobIDs():
                time.sleep(0.1)
                # pass does not work here, which is why I'm using time.sleep(0.1):
                # for jobID,data in self.runningJobMap.iteritems():
                # RuntimeError: dictionary changed size during iteration

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempDir)

class MesosBatchSystemTest(AbstractBatchSystemTest):
    """
    Tests against the Mesos batch system
    """

    def createBatchSystem(self):
        shutil.rmtree('/tmp/mesos/', ignore_errors=True)
        # Launch Mesos Master and Slave if running MesosBatchSystemTest
        self.master = self.MesosMasterThread()
        self.master.start()
        self.slave = self.MesosSlaveThread()
        self.slave.start()
        while self.master.popen is None or self.slave.popen is None:
            log.info("Waiting for master and slave processes")
            time.sleep(1)
        return MesosBatchSystem(config=self.config, maxCpus=2, maxMemory=20, badExecutor=False)

    def setUp(self):
        super(MesosBatchSystemTest, self).setUp()

    def tearDown(self):
        super(MesosBatchSystemTest, self).tearDown()
        self.slave.popen.kill()
        self.slave.join()
        self.master.popen.kill()
        self.master.join()

    class MesosThread(threading.Thread):
        __metaclass__ = ABCMeta

        # Lock is used because subprocess is NOT thread safe: http://tinyurl.com/pkp5pgq
        lock = threading.Lock()

        def __init__(self):
            threading.Thread.__init__(self)
            self.popen = None

        @abstractmethod
        def mesosCommand(self):
            raise NotImplementedError

        def run(self):
            with self.lock:
                self.popen = subprocess.Popen(self.mesosCommand())
            self.popen.wait()
            log.info('Exiting %s', self.__class__.__name__)

    class MesosMasterThread(MesosThread):
        def mesosCommand(self):
            return ['/usr/local/sbin/mesos-master', '--registry=in_memory', '--ip=127.0.0.1']

    class MesosSlaveThread(MesosThread):
        def mesosCommand(self):
            return ['/usr/local/sbin/mesos-slave', '--ip=127.0.0.1', '--master=127.0.0.1:5050']

# FIXME: the single machine backend does not support crucial methods necessary for this test

if False:
    class SingleMachineBatchSystemTest(AbstractBatchSystemTest):
        def createBatchSystem(self):
            return SingleMachineBatchSystem(config=self.config, maxCpus=2, maxMemory=20)

