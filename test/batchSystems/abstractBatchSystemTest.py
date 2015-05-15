from abc import ABCMeta, abstractmethod
import os
import shutil
import subprocess
import threading
import unittest
from xml.etree import ElementTree
import time
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.batchSystems.mesos import MesosBatchSystem
from jobTree.batchSystems.singleMachine import SingleMachineBatchSystem


class AbstractBatchSystemTest(unittest.TestCase):

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

    config = None

    @classmethod
    def setUpClass(cls):
        cls.config = cls._createDummyConfig()


    def testIssueJob(self):
        test_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test.txt')
        self.robust_remove(test_path)

        jobCommand = 'touch {}'.format(test_path)
        self.batchSystem.issueJob(jobCommand, memory=10, cpu=1)

        time.sleep(1)

        self.assertTrue(os.path.exists(test_path))
        os.remove(test_path)

    def testCheckResourceRequest(self):
        self.assertRaises(RuntimeError, self.batchSystem.checkResourceRequest, memory=1000, cpu=200)
        self.assertRaises(RuntimeError, self.batchSystem.checkResourceRequest, memory=5, cpu=200)
        self.assertRaises(RuntimeError, self.batchSystem.checkResourceRequest, memory=100, cpu=1)
        try:
            self.batchSystem.checkResourceRequest(memory=1, cpu=1)
        except RuntimeError:
            self.fail('checkResourceRequest raised exception unexpectedly.')

    def testGetIssuedJobIDs(self):
        self.batchSystem.issueJob('sleep 1', memory=10, cpu=1)
        self.batchSystem.issueJob('sleep 1', memory=10, cpu=1)

        self.assertEqual([0,1], self.batchSystem.getIssuedJobIDs())

    def testGetRunningJobsIDs(self):
        self.batchSystem.issueJob('sleep 5', memory=10, cpu=1)
        self.batchSystem.issueJob('sleep 5', memory=10, cpu=1)

        time.sleep(4)

        self.assertEqual([0,1], self.batchSystem.getRunningJobIDs().keys())

    if False:
        def testKillJobs(self):

            jobCommand = 'sleep 100'
            self.batchSystem.issueJob(jobCommand, memory=10, cpu=1)

            print self.batchSystem.getIssuedJobIDs()
            print 'test: {}'.format(self.batchSystem.getUpdatedJob(1))

            print self.batchSystem.getIssuedJobIDs()
            self.batchSystem.killJobs([0])

            self.assertEqual({}, self.batchSystem.getRunningJobIDs())

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

    @classmethod
    def robust_remove(cls, name):
        try:
            os.remove(name)
        except OSError:
            pass


class MesosBatchSystemTest(AbstractBatchSystemTest):

    def createBatchSystem(self):
        shutil.rmtree('/tmp/mesos/', ignore_errors=True)

        # Launch Mesos Master and Slave if running MesosBatchSystemTest
        self.master = MesosMasterThread()
        self.master.start()

        self.slave = MesosSlaveThread()
        self.slave.start()


        while self.master.popen is None or self.slave.popen is None:
            time.sleep(2)
            print self.master.popen, self.slave.popen

        return MesosBatchSystem(config=self.config, maxCpus=2, maxMemory=20, badExecutor=False)

    def setUp(self):
        super(MesosBatchSystemTest, self).setUp()

    def tearDown(self):
        super(MesosBatchSystemTest, self).tearDown()
        self.slave.popen.kill()
        self.slave.join()

        self.master.popen.kill()
        self.master.join()


class SingleMachineBatchSystemTest(AbstractBatchSystemTest):

    def createBatchSystem(self):
        return SingleMachineBatchSystem(config=self.config, maxCpus=2, maxMemory=20)

# Lock is used because subprocess is NOT thread safe: http://tinyurl.com/pkp5pgq
lock = threading.Lock()
class MesosMasterThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.popen = None

    def run(self):
        with lock:
            self.popen = subprocess.Popen(['/usr/local/sbin/mesos-master', '--registry=in_memory', '--ip=127.0.0.1'])
        self.popen.wait()
        print 'exiting master thread'


class MesosSlaveThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.popen = None

    def run(self):
        with lock:
            self.popen = subprocess.Popen(['/usr/local/sbin/mesos-slave', '--ip=127.0.0.1', '--master=127.0.0.1:5050'])
        self.popen.wait()
        print 'exiting slave thread'
