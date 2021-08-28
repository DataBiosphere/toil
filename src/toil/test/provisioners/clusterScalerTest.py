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

import datetime
import logging
import random
import time
import types
import uuid
from collections import defaultdict
from contextlib import contextmanager
from queue import Empty, Queue
from threading import Event, Thread
from typing import Set, List, Optional, Tuple

from mock import MagicMock

from toil.batchSystems.abstractBatchSystem import (AbstractBatchSystem,
                                                   AbstractScalableBatchSystem,
                                                   NodeInfo)
from toil.common import Config, defaultTargetTime
from toil.job import JobDescription
from toil.lib.conversions import human2bytes as h2b
from toil.provisioners import parse_node_types
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.clusterScaler import (BinPackedFit,
                                             ClusterScaler,
                                             NodeReservation,
                                             ScalerThread)
from toil.provisioners.node import Node
from toil.test import ToilTest, slow, travis_test

logger = logging.getLogger(__name__)

# simplified c4.8xlarge (preemptable)
c4_8xlarge_preemptable = Shape(wallTime=3600,
                               memory=h2b('60G'),
                               cores=36,
                               disk=h2b('100G'),
                               preemptable=True)
# simplified c4.8xlarge (non-preemptable)
c4_8xlarge = Shape(wallTime=3600,
                   memory=h2b('60G'),
                   cores=36,
                   disk=h2b('100G'),
                   preemptable=False)
# simplified r3.8xlarge (non-preemptable)
r3_8xlarge = Shape(wallTime=3600,
                   memory=h2b('260G'),
                   cores=32,
                   disk=h2b('600G'),
                   preemptable=False)
# simplified t2.micro (non-preemptable)
t2_micro = Shape(wallTime=3600,
                 memory=h2b('1G'),
                 cores=1,
                 disk=h2b('8G'),
                 preemptable=False)

class BinPackingTest(ToilTest):
    def setUp(self):
        self.node_shapes_for_testing = [c4_8xlarge_preemptable, r3_8xlarge]
        self.bpf = BinPackedFit(self.node_shapes_for_testing)

    @travis_test
    def testPackingOneShape(self):
        """Pack one shape and check that the resulting reservations look sane."""
        self.bpf.nodeReservations[c4_8xlarge_preemptable] = [NodeReservation(c4_8xlarge_preemptable)]
        self.bpf.addJobShape(Shape(wallTime=1000,
                                   cores=2,
                                   memory=h2b('1G'),
                                   disk=h2b('2G'),
                                   preemptable=True))
        self.assertEqual(self.bpf.nodeReservations[r3_8xlarge], [])
        self.assertEqual([x.shapes() for x in self.bpf.nodeReservations[c4_8xlarge_preemptable]],
                         [[Shape(wallTime=1000,
                                 memory=h2b('59G'),
                                 cores=34,
                                 disk=h2b('98G'),
                                 preemptable=True),
                           Shape(wallTime=2600,
                                 memory=h2b('60G'),
                                 cores=36,
                                 disk=h2b('100G'),
                                 preemptable=True)]])

    @travis_test
    def testSorting(self):
        """
        Test that sorting is correct: preemptable, then memory, then cores, then disk,
        then wallTime.
        """
        shapeList = [c4_8xlarge_preemptable, r3_8xlarge, c4_8xlarge, c4_8xlarge,
                     t2_micro, t2_micro, c4_8xlarge, r3_8xlarge, r3_8xlarge, t2_micro]
        shapeList.sort()
        assert shapeList == [c4_8xlarge_preemptable,
                             t2_micro, t2_micro, t2_micro,
                             c4_8xlarge, c4_8xlarge, c4_8xlarge,
                             r3_8xlarge, r3_8xlarge, r3_8xlarge]

    @travis_test
    def testAddingInitialNode(self):
        """Pack one shape when no nodes are available and confirm that we fit one node properly."""
        self.bpf.addJobShape(Shape(wallTime=1000,
                                   cores=2,
                                   memory=h2b('1G'),
                                   disk=h2b('2G'),
                                   preemptable=True))
        self.assertEqual([x.shapes() for x in self.bpf.nodeReservations[c4_8xlarge_preemptable]],
                         [[Shape(wallTime=1000,
                                 memory=h2b('59G'),
                                 cores=34,
                                 disk=h2b('98G'),
                                 preemptable=True),
                           Shape(wallTime=2600,
                                 memory=h2b('60G'),
                                 cores=36,
                                 disk=h2b('100G'),
                                 preemptable=True)]])

    @travis_test
    def testLowTargetTime(self):
        """
        Test that a low targetTime (0) parallelizes jobs aggressively (1000 queued jobs require
        1000 nodes).

        Ideally, low targetTime means: Start quickly and maximize parallelization after the
        cpu/disk/mem have been packed.

        Disk/cpu/mem packing is prioritized first, so we set job resource reqs so that each
        t2.micro (1 cpu/8G disk/1G RAM) can only run one job at a time with its resources.

        Each job is parametrized to take 300 seconds, so (the minimum of) 1 of them should fit into
        each node's 0 second window, so we expect 1000 nodes.
        """
        allocation = self.run1000JobsOnMicros(jobCores=1,
                                              jobMem=h2b('1G'),
                                              jobDisk=h2b('1G'),
                                              jobTime=300,
                                              globalTargetTime=0)
        self.assertEqual(allocation, {t2_micro: 1000})

    @travis_test
    def testHighTargetTime(self):
        """
        Test that a high targetTime (3600 seconds) maximizes packing within the targetTime.

        Ideally, high targetTime means: Maximize packing within the targetTime after the
        cpu/disk/mem have been packed.

        Disk/cpu/mem packing is prioritized first, so we set job resource reqs so that each
        t2.micro (1 cpu/8G disk/1G RAM) can only run one job at a time with its resources.

        Each job is parametrized to take 300 seconds, so 12 of them should fit into each node's
        3600 second window.  1000/12 = 83.33, so we expect 84 nodes.
        """
        allocation = self.run1000JobsOnMicros(jobCores=1,
                                              jobMem=h2b('1G'),
                                              jobDisk=h2b('1G'),
                                              jobTime=300,
                                              globalTargetTime=3600)
        self.assertEqual(allocation, {t2_micro: 84})

    @travis_test
    def testZeroResourceJobs(self):
        """
        Test that jobs requiring zero cpu/disk/mem pack first, regardless of targetTime.

        Disk/cpu/mem packing is prioritized first, so we set job resource reqs so that each
        t2.micro (1 cpu/8G disk/1G RAM) can run a seemingly infinite number of jobs with its
        resources.

        Since all jobs should pack cpu/disk/mem-wise on a t2.micro, we expect only one t2.micro to
        be provisioned.  If we raise this, as in testLowTargetTime, it will launch 1000 t2.micros.
        """
        allocation = self.run1000JobsOnMicros(jobCores=0,
                                              jobMem=0,
                                              jobDisk=0,
                                              jobTime=300,
                                              globalTargetTime=0)
        self.assertEqual(allocation, {t2_micro: 1})

    @travis_test
    def testLongRunningJobs(self):
        """
        Test that jobs with long run times (especially service jobs) are aggressively parallelized.

        This is important, because services are one case where the degree of parallelization
        really, really matters. If you have multiple services, they may all need to be running
        simultaneously before any real work can be done.

        Despite setting globalTargetTime=3600, this should launch 1000 t2.micros because each job's
        estimated runtime (30000 seconds) extends well beyond 3600 seconds.
        """
        allocation = self.run1000JobsOnMicros(jobCores=1,
                                              jobMem=h2b('1G'),
                                              jobDisk=h2b('1G'),
                                              jobTime=30000,
                                              globalTargetTime=3600)
        self.assertEqual(allocation, {t2_micro: 1000})

    def run1000JobsOnMicros(self, jobCores, jobMem, jobDisk, jobTime, globalTargetTime):
        """Test packing 1000 jobs on t2.micros.  Depending on the targetTime and resources,
        these should pack differently.
        """
        node_shapes_for_testing = [t2_micro]
        bpf = BinPackedFit(node_shapes_for_testing, targetTime=globalTargetTime)

        for _ in range(1000):
            bpf.addJobShape(Shape(wallTime=jobTime,
                                   memory=jobMem,
                                   cores=jobCores,
                                   disk=jobDisk,
                                   preemptable=False))
        return bpf.getRequiredNodes()

    @travis_test
    def testPathologicalCase(self):
        """Test a pathological case where only one node can be requested to fit months' worth of jobs.

        If the reservation is extended to fit a long job, and the
        bin-packer naively searches through all the reservation slices
        to find the first slice that fits, it will happily assign the
        first slot that fits the job, even if that slot occurs days in
        the future.
        """
        # Add one job that partially fills an r3.8xlarge for 1000 hours
        self.bpf.addJobShape(Shape(wallTime=3600000,
                                   memory=h2b('10G'),
                                   cores=0,
                                   disk=h2b('10G'),
                                   preemptable=False))
        for _ in range(500):
            # Add 500 CPU-hours worth of jobs that fill an r3.8xlarge
            self.bpf.addJobShape(Shape(wallTime=3600,
                                       memory=h2b('26G'),
                                       cores=32,
                                       disk=h2b('60G'),
                                       preemptable=False))
        # Hopefully we didn't assign just one node to cover all those jobs.
        self.assertNotEqual(self.bpf.getRequiredNodes(), {r3_8xlarge: 1, c4_8xlarge_preemptable: 0})

    @travis_test
    def testJobTooLargeForAllNodes(self):
        """
        If a job is too large for all node types, the scaler should print a
        warning, but definitely not crash.
        """
        # Takes more RAM than an r3.8xlarge
        largerThanR3 = Shape(wallTime=3600,
                             memory=h2b('360G'),
                             cores=32,
                             disk=h2b('600G'),
                             preemptable=False)
        self.bpf.addJobShape(largerThanR3)
        # If we got here we didn't crash.

class ClusterScalerTest(ToilTest):
    def setUp(self):
        super(ClusterScalerTest, self).setUp()
        self.config = Config()
        self.config.targetTime = 1800
        self.config.nodeTypes = [r3_8xlarge, c4_8xlarge_preemptable]

        # Set up the mock leader
        self.leader = MockBatchSystemAndProvisioner(self.config, 1)
        # It is also a full mock provisioner, so configure it to be that as well
        self.provisioner = self.leader
        # Pretend that Shapes are actually strings we can use for instance type names.
        self.provisioner.setAutoscaledNodeTypes([({t}, None) for t in self.config.nodeTypes])

    @travis_test
    def testRounding(self):
        """
        Test to make sure the ClusterScaler's rounding rounds properly.
        """

        # Get a ClusterScaler
        self.config.targetTime = 1
        self.config.betaInertia = 0.0
        self.config.maxNodes = [2, 3]
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)

        # Exact integers round to themselves
        self.assertEqual(scaler._round(0.0), 0)
        self.assertEqual(scaler._round(1.0), 1)
        self.assertEqual(scaler._round(-1.0), -1)
        self.assertEqual(scaler._round(123456789101112.13), 123456789101112)

        # Decimals other than X.5 round to the side they are closer to
        self.assertEqual(scaler._round(1E-10), 0)
        self.assertEqual(scaler._round(0.5 + 1E-15), 1)
        self.assertEqual(scaler._round(-0.9), -1)
        self.assertEqual(scaler._round(-0.4), 0)

        # Decimals at exactly X.5 round away from 0
        self.assertEqual(scaler._round(0.5), 1)
        self.assertEqual(scaler._round(-0.5), -1)
        self.assertEqual(scaler._round(2.5), 3)
        self.assertEqual(scaler._round(-2.5), -3)
        self.assertEqual(scaler._round(15.5), 16)
        self.assertEqual(scaler._round(-15.5), -16)
        self.assertEqual(scaler._round(123456789101112.5), 123456789101113)

    @travis_test
    def testMaxNodes(self):
        """
        Set the scaler to be very aggressive, give it a ton of jobs, and
        make sure it doesn't go over maxNodes.
        """
        self.config.targetTime = 1
        self.config.betaInertia = 0.0
        self.config.maxNodes = [2, 3]
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)
        jobShapes = [Shape(wallTime=3600,
                           cores=2,
                           memory=h2b('1G'),
                           disk=h2b('2G'),
                           preemptable=True)] * 1000
        jobShapes.extend([Shape(wallTime=3600,
                                cores=2,
                                memory=h2b('1G'),
                                disk=h2b('2G'),
                                preemptable=False)] * 1000)
        estimatedNodeCounts = scaler.getEstimatedNodeCounts(jobShapes, defaultdict(int))
        self.assertEqual(estimatedNodeCounts[r3_8xlarge], 2)
        self.assertEqual(estimatedNodeCounts[c4_8xlarge_preemptable], 3)

    @travis_test
    def testMinNodes(self):
        """
        Without any jobs queued, the scaler should still estimate "minNodes" nodes.
        """
        self.config.betaInertia = 0.0
        self.config.minNodes = [2, 3]
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)
        jobShapes = []
        estimatedNodeCounts = scaler.getEstimatedNodeCounts(jobShapes, defaultdict(int))
        self.assertEqual(estimatedNodeCounts[r3_8xlarge], 2)
        self.assertEqual(estimatedNodeCounts[c4_8xlarge_preemptable], 3)

    @travis_test
    def testPreemptableDeficitResponse(self):
        """
        When a preemptable deficit was detected by a previous run of the
        loop, the scaler should add non-preemptable nodes to
        compensate in proportion to preemptableCompensation.
        """
        self.config.targetTime = 1
        self.config.betaInertia = 0.0
        self.config.maxNodes = [10, 10]
        # This should mean that one non-preemptable node is launched
        # for every two preemptable nodes "missing".
        self.config.preemptableCompensation = 0.5
        # In this case, we want to explicitly set up the config so
        # that we can have preemptable and non-preemptable nodes of
        # the same type. That is the only situation where
        # preemptableCompensation applies.
        self.config.nodeTypes = [c4_8xlarge_preemptable, c4_8xlarge]
        self.provisioner.setAutoscaledNodeTypes([({t}, None) for t in self.config.nodeTypes])
        
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)
        # Simulate a situation where a previous run caused a
        # "deficit" of 5 preemptable nodes (e.g. a spot bid was lost)
        scaler.preemptableNodeDeficit[c4_8xlarge] = 5
        # Add a bunch of preemptable jobs (so the bin-packing
        # estimate for the non-preemptable node should still be 0)
        jobShapes = [Shape(wallTime=3600,
                           cores=2,
                           memory=h2b('1G'),
                           disk=h2b('2G'),
                           preemptable=True)] * 1000
        estimatedNodeCounts = scaler.getEstimatedNodeCounts(jobShapes, defaultdict(int))
        # We don't care about the estimated size of the preemptable
        # nodes. All we want to know is if we responded to the deficit
        # properly: 0.5 * 5 (preemptableCompensation * the deficit) = 3 (rounded up).
        self.assertEqual(estimatedNodeCounts[self.provisioner.node_shapes_for_testing[1]], 3)

    @travis_test
    def testPreemptableDeficitIsSet(self):
        """
        Make sure that updateClusterSize sets the preemptable deficit if
        it can't launch preemptable nodes properly. That way, the
        deficit can be communicated to the next run of
        estimateNodeCount.
        """
        # Mock out addNodes. We want to pretend it had trouble
        # launching all 5 nodes, and could only launch 3.
        self.provisioner.addNodes = MagicMock(return_value=3)
        # Pretend there are no nodes in the cluster right now
        self.provisioner.getProvisionedWorkers = MagicMock(return_value=[])
        # In this case, we want to explicitly set up the config so
        # that we can have preemptable and non-preemptable nodes of
        # the same type. That is the only situation where
        # preemptableCompensation applies.
        self.config.nodeTypes = [c4_8xlarge_preemptable, c4_8xlarge]
        self.provisioner.setAutoscaledNodeTypes([({t}, None) for t in self.config.nodeTypes])
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)
        estimatedNodeCounts = {c4_8xlarge_preemptable: 5, c4_8xlarge: 0}
        scaler.updateClusterSize(estimatedNodeCounts)
        self.assertEqual(scaler.preemptableNodeDeficit[c4_8xlarge_preemptable], 2)
        self.provisioner.addNodes.assert_called_once()

        # OK, now pretend this is a while later, and actually launched
        # the nodes properly. The deficit should disappear
        self.provisioner.addNodes = MagicMock(return_value=5)
        scaler.updateClusterSize(estimatedNodeCounts)
        self.assertEqual(scaler.preemptableNodeDeficit[c4_8xlarge], 0)

    @travis_test
    def testNoLaunchingIfDeltaAlreadyMet(self):
        """
        Check that the scaler doesn't try to launch "0" more instances if
        the delta was able to be met by unignoring nodes.
        """
        # We have only one node type for simplicity
        self.provisioner.setAutoscaledNodeTypes([({c4_8xlarge}, None)])
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)
        # Pretend there is one ignored worker in the cluster
        self.provisioner.getProvisionedWorkers = MagicMock(
            return_value=[Node('127.0.0.1', '127.0.0.1', 'testNode',
                               datetime.datetime.now().isoformat(),
                               nodeType=c4_8xlarge, preemptable=True)])
        scaler.ignoredNodes.add('127.0.0.1')
        # Exercise the updateClusterSize logic
        self.provisioner.addNodes = MagicMock()
        scaler.updateClusterSize({c4_8xlarge: 1})
        self.assertFalse(self.provisioner.addNodes.called,
                         "addNodes was called when no new nodes were needed")
        self.assertEqual(len(scaler.ignoredNodes), 0,
                         "The scaler didn't unignore an ignored node when "
                         "scaling up")

    @travis_test
    def testBetaInertia(self):
        # This is really high, but makes things easy to calculate.
        self.config.betaInertia = 0.5
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)
        # OK, smoothing things this much should get us 50% of the way to 100.
        self.assertEqual(scaler.smoothEstimate(c4_8xlarge_preemptable, 100), 50)
        # Now we should be at 75%.
        self.assertEqual(scaler.smoothEstimate(c4_8xlarge_preemptable, 100), 75)
        # We should eventually converge on our estimate as long as betaInertia is below 1.
        for _ in range(1000):
            scaler.smoothEstimate(c4_8xlarge_preemptable, 100)
        self.assertEqual(scaler.smoothEstimate(c4_8xlarge_preemptable, 100), 100)


class ScalerThreadTest(ToilTest):
    def _testClusterScaling(self, config, numJobs, numPreemptableJobs, jobShape):
        """
        Test the ClusterScaler class with different patterns of job creation. Tests ascertain that
        autoscaling occurs and that all the jobs are run.
        """
        # First do simple test of creating 100 preemptable and non-premptable jobs and check the
        # jobs are completed okay, then print the amount of worker time expended and the total
        # number of worker nodes used.

        mock = MockBatchSystemAndProvisioner(config, secondsPerJob=2.0)
        mock.setAutoscaledNodeTypes([({t}, None) for t in config.nodeTypes])
        mock.start()
        clusterScaler = ScalerThread(mock, mock, config)
        clusterScaler.start()
        try:
            # Add 100 jobs to complete
            list(map(lambda x: mock.addJob(jobShape=jobShape),
                     list(range(numJobs))))
            list(map(lambda x: mock.addJob(jobShape=jobShape, preemptable=True),
                     list(range(numPreemptableJobs))))

            # Add some completed jobs
            for preemptable in (True, False):
                if preemptable and numPreemptableJobs > 0 or not preemptable and numJobs > 0:
                    # Add 1000 random jobs
                    for _ in range(1000):
                        x = mock.getNodeShape(nodeType=jobShape)
                        iJ = JobDescription(requirements=dict(
                                                memory=random.choice(list(range(1, x.memory))),
                                                cores=random.choice(list(range(1, x.cores))),
                                                disk=random.choice(list(range(1, x.disk))),
                                                preemptable=preemptable),
                                            jobName='testClusterScaling', unitName='')
                        clusterScaler.addCompletedJob(iJ, random.choice(list(range(1, x.wallTime))))

            startTime = time.time()
            # Wait while the cluster processes the jobs
            while (mock.getNumberOfJobsIssued(preemptable=False) > 0
                   or mock.getNumberOfJobsIssued(preemptable=True) > 0
                   or mock.getNumberOfNodes() > 0 or mock.getNumberOfNodes(preemptable=True) > 0):
                logger.debug("Running, non-preemptable queue size: %s, non-preemptable workers: %s, "
                            "preemptable queue size: %s, preemptable workers: %s" %
                            (mock.getNumberOfJobsIssued(preemptable=False),
                             mock.getNumberOfNodes(preemptable=False),
                             mock.getNumberOfJobsIssued(preemptable=True),
                             mock.getNumberOfNodes(preemptable=True)))
                clusterScaler.check()
                time.sleep(0.5)
            logger.debug("We waited %s for cluster to finish" % (time.time() - startTime))
        finally:
            clusterScaler.shutdown()
            mock.shutDown()

        # Print some info about the autoscaling
        logger.debug("Total-jobs: %s: Max-workers: %s, "
                     "Total-worker-time: %s, Worker-time-per-job: %s" %
                    (mock.totalJobs, sum(mock.maxWorkers.values()),
                     mock.totalWorkerTime,
                     mock.totalWorkerTime // mock.totalJobs if mock.totalJobs > 0 else 0.0))

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
        config.targetTime = defaultTargetTime
        config.betaInertia = 0.1
        config.scaleInterval = 3

        self._testClusterScaling(config, numJobs=100, numPreemptableJobs=0,
                                 jobShape=config.nodeTypes[0])

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

        # Make sure the node types don't have to be ordered
        config.nodeTypes = [largeNode, smallNode, mediumNode]
        config.minNodes = [0, 0, 0]
        config.maxNodes = [10, 10]  # test expansion of this list

        # Algorithm parameters
        config.targetTime = defaultTargetTime
        config.betaInertia = 0.1
        config.scaleInterval = 3

        mock = MockBatchSystemAndProvisioner(config, secondsPerJob=2.0)
        mock.setAutoscaledNodeTypes([({t}, None) for t in config.nodeTypes])
        clusterScaler = ScalerThread(mock, mock, config)
        clusterScaler.start()
        mock.start()

        try:
            # Add small jobs
            list(map(lambda x: mock.addJob(jobShape=smallNode), list(range(numJobs))))
            list(map(lambda x: mock.addJob(jobShape=mediumNode), list(range(numJobs))))

            # Add medium completed jobs
            for i in range(1000):
                iJ = JobDescription(requirements=dict(
                                        memory=random.choice(range(smallNode.memory, mediumNode.memory)),
                                        cores=mediumNode.cores,
                                        disk=largeNode.cores,
                                        preemptable=False),
                                    jobName='testClusterScaling', unitName='')
                clusterScaler.addCompletedJob(iJ, random.choice(range(1, 10)))

            while mock.getNumberOfJobsIssued() > 0 or mock.getNumberOfNodes() > 0:
                logger.debug("%i nodes currently provisioned" % mock.getNumberOfNodes())
                # Make sure there are no large nodes
                self.assertEqual(mock.getNumberOfNodes(nodeType=largeNode), 0)
                clusterScaler.check()
                time.sleep(0.5)
        finally:
            clusterScaler.shutdown()
            mock.shutDown()

        # Make sure jobs ran on both the small and medium node types
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
        config.minNodes = [0, 0]
        config.maxNodes = [10, 10]

        # Algorithm parameters
        config.targetTime = defaultTargetTime
        config.betaInertia = 0.9
        config.scaleInterval = 3

        self._testClusterScaling(config, numJobs=100, numPreemptableJobs=100, jobShape=jobShape)


# noinspection PyAbstractClass
class MockBatchSystemAndProvisioner(AbstractScalableBatchSystem, AbstractProvisioner):
    """
    Mimics a job batcher, provisioner and scalable batch system
    """
    def __init__(self, config, secondsPerJob):
        super(MockBatchSystemAndProvisioner, self).__init__(clusterName='clusterName', clusterType='mesos')
        # To mimic parallel preemptable and non-preemptable queues
        # for jobs we create two parallel instances of the following class
        self.config = config
        self.secondsPerJob = secondsPerJob
        self.provisioner = self
        self.batchSystem = self
        self.jobQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.jobBatchSystemIDToIssuedJob = {}
        self.totalJobs = 0  # Count of total jobs processed
        self.totalWorkerTime = 0.0  # Total time spent in worker threads
        self.toilMetrics = None
        self.nodesToWorker = {}  # Map from Node to instances of the Worker class
        self.workers = defaultdict(list)  # Instances of the Worker class, by node shape
        self.maxWorkers = defaultdict(int)  # Maximum number of workers, by node shape
        self.running = False
        self.leaderThread = Thread(target=self._leaderFn)

    def start(self):
        self.running = True
        self.leaderThread.start()

    def shutDown(self):
        self.running = False
        self.leaderThread.join()

    # Stub out all AbstractBatchSystem methods since they are never called
    for name, value in AbstractBatchSystem.__dict__.items():
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

    def supportedClusterTypes(self):
        return {'mesos'}

    def createClusterSettings(self):
        pass

    def readClusterSettings(self):
        pass

    @contextmanager
    def nodeFiltering(self, filter):
        nodes = self.getProvisionedWorkers()
        yield nodes

    # AbstractProvisioner methods
    def setAutoscaledNodeTypes(self, node_types: List[Tuple[Set[Shape], Optional[float]]]):
        self.node_shapes_for_testing = sorted([it for t in node_types for it in t[0]])
        super().setAutoscaledNodeTypes(node_types)

    def getProvisionedWorkers(self, instance_type=None, preemptable=None):
        """
        Returns a list of Node objects, each representing a worker node in the cluster

        :param preemptable: If True only return preemptable nodes else return non-preemptable nodes
        :return: list of Node
        """
        nodesToWorker = self.nodesToWorker
        results = []
        if instance_type:
            results = [node for node in nodesToWorker if node.nodeType == instance_type]
        else:
            results = list(nodesToWorker.keys())
        if preemptable is not None:
            results = [node for node in results if node.preemptable == preemptable]
        return results

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
        self.jobBatchSystemIDToIssuedJob[jobID] = JobDescription(requirements={"memory": jobShape.memory,
                                                                               "cores": jobShape.cores,
                                                                               "disk": jobShape.disk,
                                                                               "preemptable": preemptable},
                                                                 jobName='job{}'.format(self.totalJobs))
        self.jobQueue.put(jobID)

    # JobBatcher functionality
    def getNumberOfJobsIssued(self, preemptable=None):
        if preemptable is not None:
            jobList = [job for job in list(self.jobQueue.queue) if
                       self.jobBatchSystemIDToIssuedJob[job].preemptable == preemptable]
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
    def addNodes(self, nodeTypes: Set[str], numNodes, preemptable):
        nodeType = next(iter(nodeTypes))
        self._addNodes(numNodes=numNodes, nodeType=nodeType, preemptable=preemptable)
        return self.getNumberOfNodes(nodeType=nodeType, preemptable=preemptable)

    def getNodeShape(self, nodeType, preemptable=False):
        # Assume node shapes and node types are the same thing for testing
        # TODO: this isn't really allowed by the type requirements on AbstractProvisioner
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

    def _addNodes(self, numNodes, nodeType: str, preemptable=False):
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

        for _ in range(numNodes):
            node = Node('127.0.0.1', uuid.uuid4(), 'testNode', datetime.datetime.now().isoformat()+'Z', nodeType=nodeType,
                    preemptable=preemptable)
            self.nodesToWorker[node] = Worker(self.jobQueue, self.updatedJobsQueue, self.secondsPerJob)
            self.workers[nodeShape].append(self.nodesToWorker[node])
        self.maxWorkers[nodeShape] = max(self.maxWorkers[nodeShape], len(self.workers[nodeShape]))

    def _removeNodes(self, nodes):
        logger.debug("Removing nodes. %s workers and %s to terminate.", len(self.nodesToWorker),
                    len(nodes))
        for node in nodes:
            logger.debug("removed node")
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
