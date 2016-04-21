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

from toil.test import ToilTest
from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem, NodeInfo
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
        Tests the bin packing method used by the cluster scaler. 
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

    def clusterScalerTests(self, config, preemptableJobs, nonPreemptableJobs):
        """
        Creates a simple, dummy scalable jobDispatcher interface / provisioner class and 
        uses this to test the ClusterScaler class through a series of 
        tests with different patterns of job creation. Tests ascertain that
        autoscaling occurs and that all the 'jobs' are run.
        """

        class Dummy(AbstractScalableBatchSystem, AbstractProvisioner):
            """
            Class that mimics a job dispatcher / provisioner
            """

            def __init__(self, secondsPerJob):
                super(Dummy, self).__init__()
                # To mimic parallel preemptable and non-preemptable queues
                # for jobs we create two parallel instances of the following class

                class DummyScalingBatchSystem(object):
                    """
                    This class implements the methods Provisioner/JobDispatcher class
                    needed for the ClusterScaler class, but omits the preemptable flag.
                    """

                    def __init__(self):
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

                    # Methods implementing the JobDispatcher/AbstractScalableBatchSystemInterface class

                    def getNumberOfJobsIssued(self):
                        return self.jobQueue.qsize()

                    def getNodeShape(self):
                        raise NotImplementedError()  # TODO

                    def getNodes(self):
                        return {address: NodeInfo(cores=0,
                                                  memory=0,
                                                  workers=1 if w.busyEvent.is_set() else 0)
                                for address, w in enumerate(self.workers)}

                    # Methods implementing the AbstractProvisioner class

                    def addNodes(self, numNodes=1):
                        class Worker(object):
                            def __init__(self, jobQueue):
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
                            self.workers.append(Worker(self.jobQueue))
                        if len(self.workers) > self.maxWorkers:
                            self.maxWorkers = len(self.workers)

                    def removeNodes(self, numNodes=1):
                        while len(self.workers) > 0 and numNodes > 0:
                            worker = self.workers.pop()
                            self.totalWorkerTime += worker.stop()
                            numNodes -= 1

                    def getNumberOfNodes(self):
                        return len(self.workers)

                self.dummyBatchSystems = (DummyScalingBatchSystem(),) * 2

            def _pick(self, preemptable=False):
                """
                Select the preemptable or nonpremptable instance of the the
                DummyScalingBatchSystem instance
                """
                return self.dummyBatchSystems[int(preemptable)]

            def addJob(self, preemptable=False):
                self._pick(preemptable).addJob()

            # AbstractScalableBatchSystem methods

            # FIXME: this isn't a method defined by the AbstractScalableBatchSystem yet the tests
            # depend on it. It needs to be declared in AbstractScalableBatchSystem or removed here.

            def getNumberOfJobsIssued(self, preemptable=False):
                return self._pick(preemptable).getNumberOfJobsIssued()

            def getNodeShape(self, preemptable=False):
                return self._pick(preemptable).getNodeShape()

            def getNodes(self, preemptable=False):
                return self._pick(preemptable).getNodes()

            def getNumberOfIdleNodes(self, preemptable=False):
                return self._pick(preemptable).getNumberOfIdleNodes()

            # AbstractProvisioner methods

            def addNodes(self, numNodes=1, preemptable=False):
                self._pick(preemptable).addNodes(numNodes=numNodes)

            def removeNodes(self, numNodes=1, preemptable=False):
                self._pick(preemptable).removeNodes(numNodes=numNodes)

            def getNumberOfNodes(self, preemptable=False):
                return self._pick(preemptable).getNumberOfNodes()

            # AbstractBatchSystem methods

            def shutdown(self):
                pass

            def killBatchJobs(self, jobIDs):
                pass

            def issueBatchJob(self, command, memory, cores, disk, preemptable):
                pass

            def getUpdatedBatchJob(self, maxWait):
                pass

            def getRunningBatchJobIDs(self):
                pass

            @classmethod
            def getRescueBatchJobFrequency(cls):
                pass

            def getIssuedBatchJobIDs(self):
                pass

            def setEnv(self, name, value=None):
                pass

            @classmethod
            def supportsWorkerCleanup(cls):
                pass

            @classmethod
            def supportsHotDeployment(cls):
                pass

        # First do simple test of creating 100 preemptable and non-premptable jobs
        # and check the jobs are completed okay, then print the amount of worker
        # time expended and the total number of worker nodes used.

        logger.info("Creating dummy batch system and scalar")

        dummy = Dummy(secondsPerJob=2.0)
        clusterScaler = ClusterScaler(dummy, dummy, config)

        # Add 100 jobs to complete 
        logger.info("Creating test jobs")
        map(lambda x: dummy.addJob(), range(nonPreemptableJobs))
        map(lambda x: dummy.addJob(preemptable=True), range(preemptableJobs))

        # Add some completed jobs
        for preemptable in (True, False):
            if (preemptable and preemptableJobs > 0) or (
                        not preemptable and nonPreemptableJobs > 0):
                for i in xrange(1000):  # Add a 1000 random jobs
                    x = config.preemptableNodeShape if preemptable else config.nonPreemptableNodeShape
                    iJ = IssuedJob(1, memory=random.choice(range(1, x.memory)),
                                   cores=random.choice(range(1, x.cores)),
                                   disk=random.choice(range(1, x.disk)),
                                   preemptable=preemptable)
                    clusterScaler.addCompletedJob(iJ, random.choice(range(1, x.wallTime)))

        logger.info("Waiting for jobs to be processed")
        startTime = time.time()
        # Wait while the cluster the process chunks through the jobs
        while (dummy.getNumberOfJobsIssued(preemptable=False) > 0 or
                       dummy.getNumberOfJobsIssued(preemptable=True) > 0 or
                       dummy.getNumberOfNodes() > 0 or dummy.getNumberOfNodes(
            preemptable=True) > 0):
            logger.info("Running, non-preemptable queue size: %s, non-preemptable workers: %s, "
                        "preemptable queue size: %s, preemptable workers: %s",
                        dummy.getNumberOfJobsIssued(preemptable=False),
                        dummy.getNumberOfNodes(preemptable=False),
                        dummy.getNumberOfJobsIssued(preemptable=True),
                        dummy.getNumberOfNodes(preemptable=True))
            time.sleep(0.5)
        logger.info("We waited %s for cluster to finish" % (time.time() - startTime))
        clusterScaler.shutdown()

        # Print some info about the autoscaling
        for i, bs in enumerate(dummy.dummyBatchSystems):
            preemptable = bool(i)
            logger.info("Preemptable: %s, Total-jobs: %s: Max-workers: %s,"
                        " Total-worker-time: %s, Worker-time-per-job: %s" %
                        (preemptable, bs.totalJobs, bs.maxWorkers,
                         bs.totalWorkerTime,
                         bs.totalWorkerTime / bs.totalJobs if bs.totalJobs > 0 else 0.0))

    def testClusterScaler_NoPreemptableJobs(self):
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
        config.nonPreemptableNodeShape = Shape(20, 10, 10, 10)
        config.minNonPreemptableNodes = 0
        config.maxNonPreemptableNodes = 10

        # Algorithm parameters
        config.alphaPacking = 0.8
        config.betaInertia = 1.2
        config.scaleInterval = 3

        self.clusterScalerTests(config, preemptableJobs=0, nonPreemptableJobs=100)

    def testClusterScaler_PreemptableAndNonPreemptableJobs(self):
        """
        Test scaling simultaneously for a batch of preemptable and non-preemptable jobs.
        """
        config = Config()

        # Make defaults dummy values
        config.defaultMemory = 1
        config.defaultCores = 1
        config.defaultDisk = 1

        # Preemptable node parameters
        config.nonPreemptableNodeShape = Shape(20, 10, 10, 10)
        config.minNonPreemptableNodes = 0
        config.maxNonPreemptableNodes = 10

        # Preemptable node parameters
        config.preemptableNodeShape = Shape(20, 10, 10, 10)
        config.minPreemptableNodes = 0
        config.maxPreemptableNodes = 10

        # Algorithm parameters
        config.alphaPacking = 0.8
        config.betaInertia = 1.2
        config.scaleInterval = 3

        self.clusterScalerTests(config, preemptableJobs=100, nonPreemptableJobs=100)
