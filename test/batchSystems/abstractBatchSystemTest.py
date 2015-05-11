from abc import ABCMeta, abstractmethod
import os
import unittest
from xml.etree import ElementTree
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.batchSystems.gridengine import GridengineBatchSystem
from jobTree.batchSystems.mesos import MesosBatchSystem
from jobTree.batchSystems.singleMachine import SingleMachineBatchSystem


class AbstractBatchSystemTest(unittest.TestCase):

    __metaclass__ = ABCMeta

    def setUp(self):
        self.config = self._createDummyConfig()
        self.batchSystem = self.createBatchSystem()
        self.test_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test.txt')

    @abstractmethod
    def createBatchSystem(self):
        """
        :rtype: AbstractBatchSystem
        """
        raise NotImplementedError

    def testCheckResourceRequest(self):
        self.assertRaises(RuntimeError, self.batchSystem.checkResourceRequest, memory=1000, cpu=200)
        self.assertRaises(RuntimeError, self.batchSystem.checkResourceRequest, memory=5, cpu=200)
        self.assertRaises(RuntimeError, self.batchSystem.checkResourceRequest, memory=100, cpu=1)
        try:
            self.batchSystem.checkResourceRequest(memory=1, cpu=1)
        except RuntimeError:
            self.fail('checkResourceRequest raised exception unexpectedly.')
    '''
    def testIssueJob(self):

        jobCommand = 'touch {}'.format(self.test_path)
        self.batchSystem.issueJob(jobCommand, memory=10, cpu=1)

        # Wait for batchSystem to issue job, otherwise assertTrue happens too quickly
        while not self.batchSystem.getIssuedJobIDs():
            pass

        self.assertTrue(os.path.exists(self.test_path))
        # TODO: Ask Hannes how to move this step to teardown -- currently fails otherwise
        #os.remove(self.test_path)


    def testKillJobs(self):

        jobCommand = 'ls'
        self.batchSystem.issueJob(jobCommand, memory=10, cpu=1)

        print self.batchSystem.getIssuedJobIDs()
        print 'test: {}'.format(self.batchSystem.getUpdatedJob(1))

        print self.batchSystem.getIssuedJobIDs()

        self.assertEqual({}, self.batchSystem.getRunningJobIDs())

    '''
    def _createDummyConfig(self):
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

    def tearDown(self):
        pass
        #os.remove(self.test_path)


class MesosBatchSystemTest(AbstractBatchSystemTest):

    def createBatchSystem(self):
        return MesosBatchSystem(config=self.config, maxCpus=2, maxMemory=10, badExecutor=False)


class SingleMachineBatchSystemTest(AbstractBatchSystemTest):

    def createBatchSystem(self):
        return SingleMachineBatchSystem(config=self.config, maxCpus=2, maxMemory=10)


class GridEngineTest(AbstractBatchSystemTest):

    def createBatchSystem(self):
        return GridengineBatchSystem(config=self.config, maxCpus=2, maxMemory=10)