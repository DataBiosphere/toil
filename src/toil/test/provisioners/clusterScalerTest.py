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
import time
from threading import Thread, Event
from Queue import Queue, Empty
import logging
import random

from bd2k.util.objects import InnerClass

from toil.test import ToilTest
from toil.batchSystems.abstractBatchSystem import (AbstractScalableBatchSystem,
                                                   NodeInfo,
                                                   AbstractBatchSystem)
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.clusterScaler import ClusterScaler, RunningJobShapes
from toil.common import Config
from toil.batchSystems.jobDispatcher import IssuedJob

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
    def testBinPacking(self):
        """
        Tests the bin-packing method used by the cluster scaler.
        """
        for test in xrange(50):
            nodeShape = Shape(wallTime=random.choice(range(1, 100)),
                              memory=random.choice(range(1, 10)),
                              cores=random.choice(range(1, 10)),
                              disk=random.choice(range(1, 10)))
            randomJobShape = lambda x: Shape(wallTime=random.choice(range(1, (3 * x.wallTime) + 1)),
                                             memory=random.choice(range(1, x.memory + 1)),
                                             cores=random.choice(range(1, x.cores + 1)),
                                             disk=random.choice(range(1, x.disk + 1)))
            numberOfJobs = random.choice(range(1, 1000))
            randomJobShapes = map(lambda i: randomJobShape(nodeShape), xrange(numberOfJobs))
            startTime = time.time()
            numberOfBins = RunningJobShapes.binPacking(randomJobShapes, nodeShape)
            logger.info("For node shape %s and %s job-shapes got %s bins in %s seconds",
                        nodeShape, numberOfJobs, numberOfBins, time.time() - startTime)

    def _testClusterScaling(self, config, numJobs, numPreemptableJobs):
        """
        Test the ClusterScaler class with different patterns of job creation. Tests ascertain
        that autoscaling occurs and that all the jobs are run.
        """
        # First do simple test of creating 100 preemptable and non-premptable jobs and check the
        # jobs are completed okay, then print the amount of worker time expended and the total
        # number of worker nodes used.

        logger.info("Creating dummy batch system and scalar")

        mock = MockBatchSystemAndProvisioner(config, secondsPerJob=2.0)
        clusterScaler = ClusterScaler(mock, mock, config)

        # Add 100 jobs to complete 
        logger.info("Creating test jobs")
        map(lambda x: mock.addJob(), range(numJobs))
        map(lambda x: mock.addJob(preemptable=True), range(numPreemptableJobs))

        # Add some completed jobs
        for preemptable in (True, False):
            if preemptable and numPreemptableJobs > 0 or not preemptable and numJobs > 0:
                # Add a 1000 random jobs
                for i in xrange(1000):
                    x = mock.getNodeShape(preemptable)
                    iJ = IssuedJob(1, memory=random.choice(range(1, x.memory)),
                                   cores=random.choice(range(1, x.cores)),
                                   disk=random.choice(range(1, x.disk)),
                                   preemptable=preemptable)
                    clusterScaler.addCompletedJob(iJ, random.choice(range(1, x.wallTime)))

        logger.info("Waiting for jobs to be processed")
        startTime = time.time()
        # Wait while the cluster the process chunks through the jobs
        while (mock.getNumberOfJobsIssued(preemptable=False) > 0 or
                       mock.getNumberOfJobsIssued(preemptable=True) > 0 or
                       mock.getNumberOfNodes() > 0 or mock.getNumberOfNodes(
            preemptable=True) > 0):
            logger.info("Running, non-preemptable queue size: %s, non-preemptable workers: %s, "
                        "preemptable queue size: %s, preemptable workers: %s",
                        mock.getNumberOfJobsIssued(preemptable=False),
                        mock.getNumberOfNodes(preemptable=False),
                        mock.getNumberOfJobsIssued(preemptable=True),
                        mock.getNumberOfNodes(preemptable=True))
            time.sleep(0.5)
        logger.info("We waited %s for cluster to finish" % (time.time() - startTime))
        clusterScaler.shutdown()

        # Print some info about the autoscaling
        for i, bs in enumerate(mock.delegates):
            preemptable = bool(i)
            logger.info("Preemptable: %s, Total-jobs: %s: Max-workers: %s,"
                        " Total-worker-time: %s, Worker-time-per-job: %s" %
                        (preemptable, bs.totalJobs, bs.maxWorkers,
                         bs.totalWorkerTime,
                         bs.totalWorkerTime / bs.totalJobs if bs.totalJobs > 0 else 0.0))

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
        config.maxPreemptableNodes = 0  # No preemptable nodes

        # Non-preemptable parameters
        config.nodeType = Shape(20, 10, 10, 10)
        config.minNodes = 0
        config.maxNodes = 10

        # Algorithm parameters
        config.alphaPacking = 0.8
        config.betaInertia = 1.2
        config.scaleInterval = 3

        self._testClusterScaling(config, numJobs=100, numPreemptableJobs=0)

    def testClusterScalingWithPreemptableJobs(self):
        """
        Test scaling simultaneously for a batch of preemptable and non-preemptable jobs.
        """
        config = Config()

        # Make defaults dummy values
        config.defaultMemory = 1
        config.defaultCores = 1
        config.defaultDisk = 1

        # Preemptable node parameters
        config.nodeType = Shape(20, 10, 10, 10)
        config.minNodes = 0
        config.maxNodes = 10

        # Preemptable node parameters
        config.preemptableNodeType = Shape(20, 10, 10, 10)
        config.minPreemptableNodes = 0
        config.maxPreemptableNodes = 10

        # Algorithm parameters
        config.alphaPacking = 0.8
        config.betaInertia = 1.2
        config.scaleInterval = 3

        self._testClusterScaling(config, numJobs=100, numPreemptableJobs=100)


class MockBatchSystemAndProvisioner(AbstractScalableBatchSystem, AbstractProvisioner):
    """
    Mimics a job batcher, provisioner and scalable batch system
    """

    def __init__(self, config, secondsPerJob):
        super(MockBatchSystemAndProvisioner, self).__init__()
        # To mimic parallel preemptable and non-preemptable queues
        # for jobs we create two parallel instances of the following class
        self.config = config
        self.secondsPerJob = secondsPerJob
        self.delegates = [self.Delegate(), self.Delegate()]

    def _pick(self, preemptable=False):
        """
        Select the preemptable or non-premptable delegate
        """
        return self.delegates[int(preemptable)]

    def addJob(self, preemptable=False):
        self._pick(preemptable).addJob()

    # JobBatcher methods

    def getNumberOfJobsIssued(self, preemptable=False):
        return self._pick(preemptable).getNumberOfJobsIssued()

    # Stub out all AbstractBatchSystem methods since they are never called

    for name, value in AbstractBatchSystem.__dict__.iteritems():
        if getattr(value, '__isabstractmethod__', False):
            exec 'def %s(): pass' % name
        # Without this, the class would end up with .name and .value attributes
        del name, value

    # AbstractScalableBatchSystem methods

    def getNodes(self, preemptable=False):
        return self._pick(preemptable).getNodes()

    # AbstractProvisioner methods

    def getNodeShape(self, preemptable=False):
        return self.config.preemptableNodeType if preemptable else self.config.nodeType

    def addNodes(self, numNodes=1, preemptable=False):
        self._pick(preemptable).addNodes(numNodes=numNodes)

    def removeNodes(self, numNodes=1, preemptable=False):
        self._pick(preemptable).removeNodes(numNodes=numNodes)

    def getNumberOfNodes(self, preemptable=False):
        return self._pick(preemptable).getNumberOfNodes()

    @InnerClass
    class Delegate(object):
        """
        Implements the methods of the Provisioner and JobBatcher class needed for the
        ClusterScaler class, but omits the preemptable argument.
        """

        def __init__(self):
            super(MockBatchSystemAndProvisioner.Delegate, self).__init__()
            self.jobQueue = Queue()
            self.totalJobs = 0  # Count of total jobs processed
            self.totalWorkerTime = 0.0  # Total time spent in worker threads
            self.workers = []  # Instances of the Worker class
            self.maxWorkers = 0  # Maximum number of workers

        def addJob(self):
            """
            Add a job to the job queue
            """
            self.totalJobs += 1
            self.jobQueue.put(None)

        # JobBatcher functionality

        def getNumberOfJobsIssued(self):
            return self.jobQueue.qsize()

        # AbstractScalableBatchSystem functionality

        def getNodes(self):
            return {address: NodeInfo(cores=0,
                                      memory=0,
                                      workers=1 if w.busyEvent.is_set() else 0)
                    for address, w in enumerate(self.workers)}

        # AbstractProvisioner functionality

        def addNodes(self, numNodes=1):
            class Worker(object):
                def __init__(self, jobQueue, secondsPerJob):
                    self.busyEvent = Event()
                    self.stopEvent = Event()

                    def workerFn():
                        while True:
                            if self.stopEvent.is_set():
                                return
                            try:
                                jobQueue.get(timeout=1.0)
                            except Empty:
                                continue
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

            for i in xrange(numNodes):
                self.workers.append(Worker(self.jobQueue, self.outer.secondsPerJob))
            if len(self.workers) > self.maxWorkers:
                self.maxWorkers = len(self.workers)

        def removeNodes(self, numNodes=1):
            while len(self.workers) > 0 and numNodes > 0:
                worker = self.workers.pop()
                self.totalWorkerTime += worker.stop()
                numNodes -= 1

        def getNumberOfNodes(self):
            return len(self.workers)
