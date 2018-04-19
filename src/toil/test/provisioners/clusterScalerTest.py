# Copyright (C) 2015-2016 Regents of the University of California
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
from __future__ import division
from builtins import map
from builtins import range
from builtins import object
from past.utils import old_div
import time
import datetime
from contextlib import contextmanager
from threading import Thread, Event
import logging
import random
import uuid

# Python 3 compatibility imports
from six.moves.queue import Empty, Queue
from six.moves import xrange
from six import iteritems

from bd2k.util.objects import InnerClass

from toil.job import JobNode, Job

from toil.test import ToilTest, slow
from toil.batchSystems.abstractBatchSystem import (AbstractScalableBatchSystem,
                                                   NodeInfo,
                                                   AbstractBatchSystem)
from toil.provisioners.node import Node
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.clusterScaler import ClusterScaler, binPacking
from toil.common import Config


logger = logging.getLogger(__name__)

# FIXME: Temporary log stuff - needs to be move to test setup

if False:
    logger.setLevel(logging.DEBUG)
    import sys

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)


class ClusterScalerTest(ToilTest):

    @slow
    def testBinPacking(self):
        """
        Tests the bin-packing method used by the cluster scaler.
        """
        for test in range(50):
            nodeShapes = [Shape(wallTime=random.choice(list(range(1, 100))),
                              memory=random.choice(list(range(1, 10))),
                              cores=random.choice(list(range(1, 10))),
                              disk=random.choice(list(range(1, 10))),
                              preemptable=False) for i in range(5)]
            randomJobShape = lambda x: Shape(wallTime=random.choice(list(range(1, (3 * x.wallTime) + 1))),
                                             memory=random.choice(list(range(1, x.memory + 1))),
                                             cores=random.choice(list(range(1, x.cores + 1))),
                                             disk=random.choice(list(range(1, x.disk + 1))),
                                             preemptable=False)
            randomJobShapes = []
            for nodeShape in nodeShapes:
                numberOfJobs = random.choice(list(range(1, 1000)))
                randomJobShapes.extend([randomJobShape(nodeShape) for i in range(numberOfJobs)])
            startTime = time.time()
            numberOfBins = binPacking(jobShapes=randomJobShapes, nodeShapes=nodeShapes)
            logger.info("Made the following node reservations: %s" % numberOfBins)


    def _testClusterScaling(self, config, numJobs, numPreemptableJobs, jobShape):
        """
        Test the ClusterScaler class with different patterns of job creation. Tests ascertain
        that autoscaling occurs and that all the jobs are run.
        """
        # First do simple test of creating 100 preemptable and non-premptable jobs and check the
        # jobs are completed okay, then print the amount of worker time expended and the total
        # number of worker nodes used.

        logger.info("Creating dummy batch system and scalar")

        mock = MockBatchSystemAndProvisioner(config, secondsPerJob=2.0)
        mock.start()
        clusterScaler = ClusterScaler(mock, mock, config)
        clusterScaler.start()
        try:
            # Add 100 jobs to complete 
            logger.info("Creating test jobs")
            list(map(lambda x: mock.addJob(jobShape=jobShape), list(range(numJobs))))
            list(map(lambda x: mock.addJob(jobShape=jobShape, preemptable=True), list(range(numPreemptableJobs))))

            # Add some completed jobs
            for preemptable in (True, False):
                if preemptable and numPreemptableJobs > 0 or not preemptable and numJobs > 0:
                    # Add a 1000 random jobs
                    for i in range(1000):
                        x = mock.getNodeShape(nodeType=jobShape)
                        iJ = JobNode(jobStoreID=1,
                                     requirements=dict(memory=random.choice(list(range(1, x.memory))),
                                                       cores=random.choice(list(range(1, x.cores))),
                                                       disk=random.choice(list(range(1, x.disk))),
                                                       preemptable=preemptable),
                                     command=None,
                                     jobName='testClusterScaling', unitName='')
                        clusterScaler.addCompletedJob(iJ, random.choice(list(range(1, x.wallTime))))

            logger.info("Waiting for jobs to be processed")
            startTime = time.time()
            # Wait while the cluster the process chunks through the jobs
            while (mock.getNumberOfJobsIssued(preemptable=False) > 0
                   or mock.getNumberOfJobsIssued(preemptable=True) > 0
                   or mock.getNumberOfNodes() > 0 or mock.getNumberOfNodes(preemptable=True) > 0):
                logger.info("Running, non-preemptable queue size: %s, non-preemptable workers: %s, "
                            "preemptable queue size: %s, preemptable workers: %s" %
                            (mock.getNumberOfJobsIssued(preemptable=False),
                             mock.getNumberOfNodes(preemptable=False),
                             mock.getNumberOfJobsIssued(preemptable=True),
                             mock.getNumberOfNodes(preemptable=True)))
                clusterScaler.check()
                time.sleep(0.5)
            logger.info("We waited %s for cluster to finish" % (time.time() - startTime))
        finally:
            clusterScaler.shutdown()
            mock.shutDown()

        # Print some info about the autoscaling
        logger.info("Total-jobs: %s: Max-workers: %s,"
                    " Total-worker-time: %s, Worker-time-per-job: %s" %
                    (mock.totalJobs, sum(mock.maxWorkers.values()),
                     mock.totalWorkerTime,
                     old_div(mock.totalWorkerTime, mock.totalJobs) if mock.totalJobs > 0 else 0.0))

    @slow
    def testClusterScaling(self):
        """
        Test scaling for a batch of non-preemptable jobs and no preemptable jobs (makes debugging
        easier).
        """
        config = Config()

        # Make defaults dummy values
        config.defaultMemory = 1
        config.defaultCores = 1
        config.defaultDisk = 1

        # No preemptable nodes/jobs
        config.maxPreemptableNodes = []  # No preemptable nodes

        # Non-preemptable parameters
        config.nodeTypes = [Shape(20, 10, 10, 10, False)]
        config.minNodes = [0]
        config.maxNodes = [10]

        # Algorithm parameters
        config.alphaPacking = 0.0
        config.betaInertia = 1.2
        config.scaleInterval = 3

        self._testClusterScaling(config, numJobs=100, numPreemptableJobs=0, jobShape=config.nodeTypes[0])

    @slow
    def testClusterScalingMultipleNodeTypes(self):

        smallNode = Shape(20, 5, 10, 10, False)
        mediumNode = Shape(20, 10, 10, 10, False)
        largeNode = Shape(20, 20, 10, 10, False)

        numJobs = 100

        config = Config()

        # Make defaults dummy values
        config.defaultMemory = 1
        config.defaultCores = 1
        config.defaultDisk = 1

        # No preemptable nodes/jobs
        config.preemptableNodeTypes = []
        config.minPreemptableNodes = []
        config.maxPreemptableNodes = []  # No preemptable nodes

        #Make sure the node types don't have to be ordered
        config.nodeTypes = [largeNode, smallNode, mediumNode]
        config.minNodes = [0, 0, 0]
        config.maxNodes = [10, 10] # test expansion of this list

        # Algorithm parameters
        config.alphaPacking = 0.0
        config.betaInertia = 1.2
        config.scaleInterval = 3

        mock = MockBatchSystemAndProvisioner(config, secondsPerJob=2.0)
        clusterScaler = ClusterScaler(mock, mock, config)
        clusterScaler.start()
        mock.start()

        try:
            #Add small jobs
            list(map(lambda x: mock.addJob(jobShape=smallNode), list(range(numJobs))))
            list(map(lambda x: mock.addJob(jobShape=mediumNode), list(range(numJobs))))

            #Add medium completed jobs
            for i in range(1000):
                iJ = JobNode(jobStoreID=1,
                             requirements=dict(memory=random.choice(range(smallNode.memory, mediumNode.memory)),
                                               cores=mediumNode.cores,
                                               disk=largeNode.cores,
                                               preemptable=False),
                             command=None,
                             jobName='testClusterScaling', unitName='')
                clusterScaler.addCompletedJob(iJ, random.choice(range(1, 10)))

            while mock.getNumberOfJobsIssued() > 0 or mock.getNumberOfNodes() > 0:
                logger.info("%i nodes currently provisioned" % mock.getNumberOfNodes())
                #Make sure there are no large nodes
                self.assertEqual(mock.getNumberOfNodes(nodeType=largeNode), 0)
                clusterScaler.check()
                time.sleep(0.5)
        finally:
            clusterScaler.shutdown()
            mock.shutDown()

        #Make sure jobs ran on both the small and medium node types
        self.assertTrue(mock.totalJobs > 0)
        self.assertTrue(mock.maxWorkers[smallNode] > 0)
        self.assertTrue(mock.maxWorkers[mediumNode] > 0)

        self.assertEqual(mock.maxWorkers[largeNode], 0)

    @slow
    def testClusterScalingWithPreemptableJobs(self):
        """
        Test scaling simultaneously for a batch of preemptable and non-preemptable jobs.
        """
        config = Config()

        jobShape = Shape(20, 10, 10, 10, False)
        preemptableJobShape = Shape(20, 10, 10, 10, True)

        # Make defaults dummy values
        config.defaultMemory = 1
        config.defaultCores = 1
        config.defaultDisk = 1

        # non-preemptable node parameters
        config.nodeTypes = [jobShape, preemptableJobShape]
        config.minNodes = [0,0]
        config.maxNodes = [10,10]

        # Algorithm parameters
        config.alphaPacking = 0.0
        config.betaInertia = 1.2
        config.scaleInterval = 3

        self._testClusterScaling(config, numJobs=100, numPreemptableJobs=100, jobShape=jobShape)


# noinspection PyAbstractClass
class MockBatchSystemAndProvisioner(AbstractScalableBatchSystem, AbstractProvisioner):
    """
    Mimics a job batcher, provisioner and scalable batch system
    """

    def __init__(self, config, secondsPerJob):
        super(MockBatchSystemAndProvisioner, self).__init__('clusterName')
        # To mimic parallel preemptable and non-preemptable queues
        # for jobs we create two parallel instances of the following class
        self.config = config
        self.secondsPerJob = secondsPerJob
        self.provisioner = self
        self.batchSystem = self

        self.nodeTypes = config.nodeTypes
        self.nodeShapes = self.nodeTypes
        self.nodeShapes.sort()

        self.jobQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.jobBatchSystemIDToIssuedJob = {}
        self.totalJobs = 0 # Count of total jobs processed
        self.totalWorkerTime = 0.0  # Total time spent in worker threads

        self.toilMetrics = None

        self.nodesToWorker = {}  # Map from Node to instances of the Worker class

        self.workers = {nodeShape:[] for nodeShape in self.nodeShapes} # Instances of the Worker class
        self.maxWorkers = {nodeShape:0 for nodeShape in self.nodeShapes} # Maximum number of workers
        self.running = False
        self.leaderThread = Thread(target=self._leaderFn)

    def start(self):
        self.running = True
        self.leaderThread.start()

    def shutDown(self):
        self.running = False
        self.leaderThread.join()


    # Stub out all AbstractBatchSystem methods since they are never called

    for name, value in iteritems(AbstractBatchSystem.__dict__):
        if getattr(value, '__isabstractmethod__', False):
            exec('def %s(): pass' % name)
        # Without this, the class would end up with .name and .value attributes
        del name, value

    # AbstractScalableBatchSystem methods

    def nodeInUse(self, nodeIP):
        return False

    def ignoreNode(self, nodeAddress):
        pass

    def unignoreNode(self, nodeAddress):
        pass

    @contextmanager
    def nodeFiltering(self, filter):
        nodes = self.getProvisionedWorkers(preemptable=True, nodeType=None) + self.getProvisionedWorkers(preemptable=False, nodeType=None)
        yield nodes

    # AbstractProvisioner methods

    def getProvisionedWorkers(self, nodeType=None, preemptable=None):
        """
        Returns a list of Node objects, each representing a worker node in the cluster
        
        :param preemptable: If True only return preemptable nodes else return non-preemptable nodes
        :return: list of Node
        """
        nodesToWorker = self.nodesToWorker
        if nodeType:
            return [node for node in nodesToWorker if node.nodeType == nodeType]
        else:
            return list(nodesToWorker.keys())


    def terminateNodes(self, nodes):
        self._removeNodes(nodes)

    def remainingBillingInterval(self, node):
        pass



    def addJob(self, jobShape, preemptable=False):
        """
        Add a job to the job queue
        """
        self.totalJobs += 1
        jobID = uuid.uuid4()
        self.jobBatchSystemIDToIssuedJob[jobID] = Job(memory=jobShape.memory,
                                                          cores=jobShape.cores, disk=jobShape.disk, preemptable=preemptable)
        self.jobQueue.put(jobID)

    # JobBatcher functionality

    def getNumberOfJobsIssued(self, preemptable=None):
        if preemptable is not None:
            jobList = [job for job in list(self.jobQueue.queue) if self.jobBatchSystemIDToIssuedJob[job].preemptable == preemptable]
            return len(jobList)
        else:
            return self.jobQueue.qsize()

    def getJobs(self):
        return self.jobBatchSystemIDToIssuedJob.values()

    # AbstractScalableBatchSystem functionality

    def getNodes(self, preemptable=False, timeout=None):
        nodes = dict()
        for node in self.nodesToWorker:
            if node.preemptable == preemptable:
                worker = self.nodesToWorker[node]
                nodes[node.privateIP] = NodeInfo(coresTotal=0, coresUsed=0, requestedCores=1,
                                                 memoryTotal=0, memoryUsed=0, requestedMemory=1,
                                                 workers=1 if worker.busyEvent.is_set() else 0)
        return nodes

    # AbstractProvisioner functionality

    def addNodes(self, nodeType, numNodes, preemptable):
        self._addNodes(numNodes=numNodes, nodeType=nodeType, preemptable=preemptable)
        return self.getNumberOfNodes(nodeType=nodeType, preemptable=preemptable)

    def getNodeShape(self, nodeType, preemptable=False):
        #Assume node shapes and node types are the same thing for testing
        return nodeType

    def getWorkersInCluster(self, nodeShape):
        return self.workers[nodeShape]

    def launchCluster(self, leaderNodeType, keyName, userTags=None,
                      vpcSubnet=None, leaderStorage=50, nodeStorage=50, botoPath=None, **kwargs):
        pass
    def destroyCluster(self):
        pass

    def getLeader(self):
        pass


    def _leaderFn(self):
        while self.running:
            updatedJobID = None
            try:
                updatedJobID = self.updatedJobsQueue.get(timeout=1.0)
            except Empty:
                continue
            if updatedJobID:
                del self.jobBatchSystemIDToIssuedJob[updatedJobID]
            time.sleep(0.1)


    def _addNodes(self, numNodes, nodeType, preemptable=False):
        nodeShape = self.getNodeShape(nodeType=nodeType, preemptable=preemptable)
        class Worker(object):
            def __init__(self, jobQueue, updatedJobsQueue, secondsPerJob):
                self.busyEvent = Event()
                self.stopEvent = Event()

                def workerFn():
                    while True:
                        if self.stopEvent.is_set():
                            return
                        try:
                            jobID = jobQueue.get(timeout=1.0)
                        except Empty:
                            continue
                        updatedJobsQueue.put(jobID)
                        self.busyEvent.set()
                        time.sleep(secondsPerJob)
                        self.busyEvent.clear()

                self.startTime = time.time()
                self.worker = Thread(target=workerFn)
                self.worker.start()

            def stop(self):
                self.stopEvent.set()
                self.worker.join()
                return time.time() - self.startTime
        for i in range(numNodes):
            node = Node('127.0.0.1', uuid.uuid4(), 'testNode', datetime.datetime.now().isoformat()+'Z', nodeType=nodeType,
                    preemptable=preemptable)
            self.nodesToWorker[node] = Worker(self.jobQueue, self.updatedJobsQueue, self.secondsPerJob)
            self.workers[nodeShape].append(self.nodesToWorker[node])
        self.maxWorkers[nodeShape] = max(self.maxWorkers[nodeShape], len(self.workers[nodeShape]))

    def _removeNodes(self, nodes):
        logger.info("removing nodes. %s workers and %s to terminate", len(self.nodesToWorker), len(nodes))
        for node in nodes:
            logger.info("removed node")
            try:
                nodeShape = self.getNodeShape(node.nodeType, node.preemptable)
                worker = self.nodesToWorker.pop(node)
                self.workers[nodeShape].pop()
                self.totalWorkerTime += worker.stop()
            except KeyError:
                # Node isn't our responsibility
                pass

    def getNumberOfNodes(self, nodeType=None, preemptable=None):
        if nodeType:
            nodeShape = self.getNodeShape(nodeType=nodeType, preemptable=preemptable)
            return len(self.workers[nodeShape])
        else:
            return len(self.nodesToWorker)

