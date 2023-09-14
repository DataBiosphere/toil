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
import uuid
from argparse import Namespace
from collections import defaultdict
from contextlib import contextmanager
from queue import Empty, Queue
from threading import Event, Thread
from typing import Dict, List, Optional, Set, Tuple
from unittest.mock import MagicMock

from toil.batchSystems.abstractBatchSystem import (AbstractBatchSystem,
                                                   AbstractScalableBatchSystem,
                                                   NodeInfo)
from toil.common import Config, defaultTargetTime
from toil.job import JobDescription
from toil.lib.conversions import human2bytes as h2b
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.clusterScaler import (BinPackedFit,
                                             ClusterScaler,
                                             NodeReservation,
                                             ScalerThread)
from toil.provisioners.node import Node
from toil.test import ToilTest, slow

logger = logging.getLogger(__name__)

# simplified c4.8xlarge (preemptible)
c4_8xlarge_preemptible = Shape(wallTime=3600,
                               memory=h2b('60G'),
                               cores=36,
                               disk=h2b('100G'),
                               preemptible=True)
# simplified c4.8xlarge (non-preemptible)
c4_8xlarge = Shape(wallTime=3600,
                   memory=h2b('60G'),
                   cores=36,
                   disk=h2b('100G'),
                   preemptible=False)
# simplified r3.8xlarge (non-preemptible)
r3_8xlarge = Shape(wallTime=3600,
                   memory=h2b('260G'),
                   cores=32,
                   disk=h2b('600G'),
                   preemptible=False)
# simplified r5.2xlarge (non-preemptible)
r5_2xlarge = Shape(wallTime=3600,
                   memory=h2b('64Gi'),
                   cores=8,
                   disk=h2b('50G'),
                   preemptible=False)
# simplified r5.4xlarge (non-preemptible)
r5_4xlarge = Shape(wallTime=3600,
                   memory=h2b('128Gi'),
                   cores=16,
                   disk=h2b('50G'),
                   preemptible=False)
# simplified t2.micro (non-preemptible)
t2_micro = Shape(wallTime=3600,
                 memory=h2b('1G'),
                 cores=1,
                 disk=h2b('8G'),
                 preemptible=False)

class BinPackingTest(ToilTest):
    def setUp(self):
        self.node_shapes_for_testing = [c4_8xlarge_preemptible, r3_8xlarge]
        self.bpf = BinPackedFit(self.node_shapes_for_testing)

    def testPackingOneShape(self):
        """Pack one shape and check that the resulting reservations look sane."""
        self.bpf.nodeReservations[c4_8xlarge_preemptible] = [NodeReservation(c4_8xlarge_preemptible)]
        self.bpf.addJobShape(Shape(wallTime=1000,
                                   cores=2,
                                   memory=h2b('1G'),
                                   disk=h2b('2G'),
                                   preemptible=True))
        self.assertEqual(self.bpf.nodeReservations[r3_8xlarge], [])
        self.assertEqual([x.shapes() for x in self.bpf.nodeReservations[c4_8xlarge_preemptible]],
                         [[Shape(wallTime=1000,
                                 memory=h2b('59G'),
                                 cores=34,
                                 disk=h2b('98G'),
                                 preemptible=True),
                           Shape(wallTime=2600,
                                 memory=h2b('60G'),
                                 cores=36,
                                 disk=h2b('100G'),
                                 preemptible=True)]])

    def testSorting(self):
        """
        Test that sorting is correct: preemptible, then memory, then cores, then disk,
        then wallTime.
        """
        shapeList = [c4_8xlarge_preemptible, r3_8xlarge, c4_8xlarge, c4_8xlarge,
                     t2_micro, t2_micro, c4_8xlarge, r3_8xlarge, r3_8xlarge, t2_micro]
        shapeList.sort()
        assert shapeList == [c4_8xlarge_preemptible,
                             t2_micro, t2_micro, t2_micro,
                             c4_8xlarge, c4_8xlarge, c4_8xlarge,
                             r3_8xlarge, r3_8xlarge, r3_8xlarge]

    def testAddingInitialNode(self):
        """Pack one shape when no nodes are available and confirm that we fit one node properly."""
        self.bpf.addJobShape(Shape(wallTime=1000,
                                   cores=2,
                                   memory=h2b('1G'),
                                   disk=h2b('2G'),
                                   preemptible=True))
        self.assertEqual([x.shapes() for x in self.bpf.nodeReservations[c4_8xlarge_preemptible]],
                         [[Shape(wallTime=1000,
                                 memory=h2b('59G'),
                                 cores=34,
                                 disk=h2b('98G'),
                                 preemptible=True),
                           Shape(wallTime=2600,
                                 memory=h2b('60G'),
                                 cores=36,
                                 disk=h2b('100G'),
                                 preemptible=True)]])

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
                                   preemptible=False))
        return bpf.getRequiredNodes()

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
                                   preemptible=False))
        for _ in range(500):
            # Add 500 CPU-hours worth of jobs that fill an r3.8xlarge
            self.bpf.addJobShape(Shape(wallTime=3600,
                                       memory=h2b('26G'),
                                       cores=32,
                                       disk=h2b('60G'),
                                       preemptible=False))
        # Hopefully we didn't assign just one node to cover all those jobs.
        self.assertNotEqual(self.bpf.getRequiredNodes(), {r3_8xlarge: 1, c4_8xlarge_preemptible: 0})

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
                             preemptible=False)
        self.bpf.addJobShape(largerThanR3)
        # If we got here we didn't crash.

class ClusterScalerTest(ToilTest):
    def setUp(self):
        super().setUp()
        self.config = Config()
        self.config.targetTime = 1800
        self.config.nodeTypes = [r3_8xlarge, c4_8xlarge_preemptible]

        # Set up the mock leader
        self.leader = MockBatchSystemAndProvisioner(config=self.config, secondsPerJob=1)
        # It is also a full mock provisioner, so configure it to be that as well
        self.provisioner = self.leader
        # Pretend that Shapes are actually strings we can use for instance type names.
        self.provisioner.setAutoscaledNodeTypes([({t}, None) for t in self.config.nodeTypes])

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
                           preemptible=True)] * 1000
        jobShapes.extend([Shape(wallTime=3600,
                                cores=2,
                                memory=h2b('1G'),
                                disk=h2b('2G'),
                                preemptible=False)] * 1000)
        estimatedNodeCounts, could_not_fit = scaler.getEstimatedNodeCounts(jobShapes, defaultdict(int))
        self.assertEqual(estimatedNodeCounts[r3_8xlarge], 2)
        self.assertEqual(estimatedNodeCounts[c4_8xlarge_preemptible], 3)
        self.assertEqual(len(could_not_fit), 0)

    def testMinNodes(self):
        """
        Without any jobs queued, the scaler should still estimate "minNodes" nodes.
        """
        self.config.betaInertia = 0.0
        self.config.minNodes = [2, 3]
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)
        jobShapes = []
        estimatedNodeCounts, could_not_fit = scaler.getEstimatedNodeCounts(jobShapes, defaultdict(int))
        self.assertEqual(estimatedNodeCounts[r3_8xlarge], 2)
        self.assertEqual(estimatedNodeCounts[c4_8xlarge_preemptible], 3)
        self.assertEqual(len(could_not_fit), 0)

    def testPreemptibleDeficitResponse(self):
        """
        When a preemptible deficit was detected by a previous run of the
        loop, the scaler should add non-preemptible nodes to
        compensate in proportion to preemptibleCompensation.
        """
        self.config.targetTime = 1
        self.config.betaInertia = 0.0
        self.config.maxNodes = [10, 10]
        # This should mean that one non-preemptible node is launched
        # for every two preemptible nodes "missing".
        self.config.preemptibleCompensation = 0.5
        # In this case, we want to explicitly set up the config so
        # that we can have preemptible and non-preemptible nodes of
        # the same type. That is the only situation where
        # preemptibleCompensation applies.
        self.config.nodeTypes = [c4_8xlarge_preemptible, c4_8xlarge]
        self.provisioner.setAutoscaledNodeTypes([({t}, None) for t in self.config.nodeTypes])

        scaler = ClusterScaler(self.provisioner, self.leader, self.config)
        # Simulate a situation where a previous run caused a
        # "deficit" of 5 preemptible nodes (e.g. a spot bid was lost)
        scaler.preemptibleNodeDeficit[c4_8xlarge] = 5
        # Add a bunch of preemptible jobs (so the bin-packing
        # estimate for the non-preemptible node should still be 0)
        jobShapes = [Shape(wallTime=3600,
                           cores=2,
                           memory=h2b('1G'),
                           disk=h2b('2G'),
                           preemptible=True)] * 1000
        estimatedNodeCounts, could_not_fit = scaler.getEstimatedNodeCounts(jobShapes, defaultdict(int))
        # We don't care about the estimated size of the preemptible
        # nodes. All we want to know is if we responded to the deficit
        # properly: 0.5 * 5 (preemptibleCompensation * the deficit) = 3 (rounded up).
        self.assertEqual(estimatedNodeCounts[self.provisioner.node_shapes_for_testing[1]], 3)
        self.assertEqual(len(could_not_fit), 0)

    def testPreemptibleDeficitIsSet(self):
        """
        Make sure that updateClusterSize sets the preemptible deficit if
        it can't launch preemptible nodes properly. That way, the
        deficit can be communicated to the next run of
        estimateNodeCount.
        """
        # Mock out addNodes. We want to pretend it had trouble
        # launching all 5 nodes, and could only launch 3.
        self.provisioner.addNodes = MagicMock(return_value=3)
        # Pretend there are no nodes in the cluster right now
        self.provisioner.getProvisionedWorkers = MagicMock(return_value=[])
        # In this case, we want to explicitly set up the config so
        # that we can have preemptible and non-preemptible nodes of
        # the same type. That is the only situation where
        # preemptibleCompensation applies.
        self.config.nodeTypes = [c4_8xlarge_preemptible, c4_8xlarge]
        self.provisioner.setAutoscaledNodeTypes([({t}, None) for t in self.config.nodeTypes])
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)
        estimatedNodeCounts = {c4_8xlarge_preemptible: 5, c4_8xlarge: 0}
        scaler.updateClusterSize(estimatedNodeCounts)
        self.assertEqual(scaler.preemptibleNodeDeficit[c4_8xlarge_preemptible], 2)
        self.provisioner.addNodes.assert_called_once()

        # OK, now pretend this is a while later, and actually launched
        # the nodes properly. The deficit should disappear
        self.provisioner.addNodes = MagicMock(return_value=5)
        scaler.updateClusterSize(estimatedNodeCounts)
        self.assertEqual(scaler.preemptibleNodeDeficit[c4_8xlarge], 0)

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
                               nodeType=c4_8xlarge, preemptible=True)])
        scaler.ignoredNodes.add('127.0.0.1')
        # Exercise the updateClusterSize logic
        self.provisioner.addNodes = MagicMock()
        scaler.updateClusterSize({c4_8xlarge: 1})
        self.assertFalse(self.provisioner.addNodes.called,
                         "addNodes was called when no new nodes were needed")
        self.assertEqual(len(scaler.ignoredNodes), 0,
                         "The scaler didn't unignore an ignored node when "
                         "scaling up")

    def testBetaInertia(self):
        # This is really high, but makes things easy to calculate.
        self.config.betaInertia = 0.5
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)
        # OK, smoothing things this much should get us 50% of the way to 100.
        self.assertEqual(scaler.smoothEstimate(c4_8xlarge_preemptible, 100), 50)
        # Now we should be at 75%.
        self.assertEqual(scaler.smoothEstimate(c4_8xlarge_preemptible, 100), 75)
        # We should eventually converge on our estimate as long as betaInertia is below 1.
        for _ in range(1000):
            scaler.smoothEstimate(c4_8xlarge_preemptible, 100)
        self.assertEqual(scaler.smoothEstimate(c4_8xlarge_preemptible, 100), 100)

    def test_overhead_accounting_large(self):
        """
        If a node has a certain raw memory or disk capacity, that won't all be
        available when it actually comes up; some disk and memory will be used
        by the OS, and the backing scheduler (Mesos, Kubernetes, etc.).

        Make sure this overhead is accounted for for large nodes.
        """

        # Use a small node (60G) and a big node (260G)

        # If the job needs 100% of the memory of the instance type, it won't
        # fit and will need a bigger node.
        self._check_job_estimate([(c4_8xlarge, 0), (r3_8xlarge, 1)], memory=h2b('60G'))

        # If the job needs 98% of the memory of the instance type, it won't
        # fit and will need a bigger node.
        self._check_job_estimate([(c4_8xlarge, 0), (r3_8xlarge, 1)], memory=int(h2b('60G') * 0.98))

        # If the job needs 90% of the memory of the instance type, it will fit.
        self._check_job_estimate([(c4_8xlarge, 1), (r3_8xlarge, 0)], memory=int(h2b('60G') * 0.90))

        # If the job needs 100% of the disk of the instance type, it won't
        # fit and will need a bigger node.
        self._check_job_estimate([(c4_8xlarge, 0), (r3_8xlarge, 1)], disk=h2b('100G'))

        # If the job needs all but 7G of the disk of the instance type, it won't
        # fit and will need a bigger node.
        self._check_job_estimate([(c4_8xlarge, 0), (r3_8xlarge, 1)], disk=h2b('93G'))

        # If the job leaves 10% and 10G of the disk free, it fits
        self._check_job_estimate([(c4_8xlarge, 1), (r3_8xlarge, 0)], disk=h2b('90G'))

    def test_overhead_accounting_small(self):
        """
        If a node has a certain raw memory or disk capacity, that won't all be
        available when it actually comes up; some disk and memory will be used
        by the OS, and the backing scheduler (Mesos, Kubernetes, etc.).

        Make sure this overhead is accounted for for small nodes.
        """

        # Use a small node (1G) and a big node (260G)

        # If the job needs 100% of the memory of the instance type, it won't
        # fit and will need a bigger node.
        self._check_job_estimate([(t2_micro, 0), (r3_8xlarge, 1)], memory=h2b('1G'))

        # If the job needs all but 100M of the memory of the instance type, it
        # won't fit and will need a bigger node.
        self._check_job_estimate([(t2_micro, 0), (r3_8xlarge, 1)], memory=h2b('1G') - h2b('100M'))

        # If the job needs no more than 90% of the memory on the node *and*
        # leaves at least 384M free for overhead, we can rely on it fitting on a 1G
        # memory node.
        jobs = [
            Shape(
                wallTime=3600,
                cores=1,
                memory=h2b('1G') - h2b('384M'),
                disk=h2b('2G'),
                preemptible=True
            )
        ]
        self._check_job_estimate([(t2_micro, 1), (r3_8xlarge, 0)], memory=h2b('1G') - h2b('384M'))

    def test_overhead_accounting_observed(self):
        """
        If a node has a certain raw memory or disk capacity, that won't all be
        available when it actually comes up; some disk and memory will be used
        by the OS, and the backing scheduler (Mesos, Kubernetes, etc.).

        Make sure this overhead is accounted for so that real-world observed
        failures cannot happen again.
        """
        # In
        # https://github.com/DataBiosphere/toil/issues/4147#issuecomment-1179587561
        # a user observed what seems to be a nominally 64 GiB node
        # (r5.2xlarge?) with Mesos reporting "61.0 GB" available, and a
        # nominally 32 GiB node with Mesos reporting "29.9 GB" available. It's
        # not clear if Mesos is thinking in actual GB or GiB here.

        # A 62.5Gi job is sent to the larger node
        self._check_job_estimate([(r5_2xlarge, 0), (r5_4xlarge, 1)], memory=h2b('62.5 Gi'))

    def _check_job_estimate(self, nodes: List[Tuple[Shape, int]], cores=1, memory=1, disk=1) -> None:
        """
        Make sure that a job with the given requirements, when run on the given
        nodes, produces the given numbers of them.
        """

        self.provisioner.setAutoscaledNodeTypes([({node}, None) for node, _ in nodes])
        self.config.targetTime = 1
        self.config.betaInertia = 0.0
        # We only need up to one node
        self.config.maxNodes = [1] * len(nodes)
        scaler = ClusterScaler(self.provisioner, self.leader, self.config)

        jobs = [
            Shape(
                wallTime=3600,
                cores=cores,
                memory=memory,
                disk=disk,
                preemptible=True
            )
        ]

        logger.debug('Try and fit jobs: %s', jobs)
        counts, could_not_fit = scaler.getEstimatedNodeCounts(jobs, defaultdict(int))
        for node, count in nodes:
            seen_count = counts.get(node, 0)
            if seen_count != count:
                logger.error('Saw %s/%s instances of node %s', seen_count, count, node)
            self.assertEqual(seen_count, count)
        self.assertEqual(len(could_not_fit), 0)

class ScalerThreadTest(ToilTest):
    def _testClusterScaling(self, config, numJobs, numPreemptibleJobs, jobShape):
        """
        Test the ClusterScaler class with different patterns of job creation. Tests ascertain that
        autoscaling occurs and that all the jobs are run.
        """
        # First do simple test of creating 100 preemptible and non-premptable jobs and check the
        # jobs are completed okay, then print the amount of worker time expended and the total
        # number of worker nodes used.

        mock = MockBatchSystemAndProvisioner(config=config, secondsPerJob=2.0)
        mock.setAutoscaledNodeTypes([({t}, None) for t in config.nodeTypes])
        mock.start()
        clusterScaler = ScalerThread(mock, mock, config, stop_on_exception=True)
        clusterScaler.start()
        try:
            # Add 100 jobs to complete
            list(map(lambda x: mock.addJob(jobShape=jobShape),
                     list(range(numJobs))))
            list(map(lambda x: mock.addJob(jobShape=jobShape, preemptible=True),
                     list(range(numPreemptibleJobs))))

            # Add some completed jobs
            for preemptible in (True, False):
                if preemptible and numPreemptibleJobs > 0 or not preemptible and numJobs > 0:
                    # Add 1000 random jobs
                    for _ in range(1000):
                        x = mock.getNodeShape(nodeType=jobShape)
                        iJ = JobDescription(requirements=dict(
                                                memory=random.randrange(1, x.memory),
                                                cores=random.randrange(1, x.cores),
                                                disk=random.randrange(1, x.disk),
                                                preemptible=preemptible),
                                            jobName='testClusterScaling', unitName='')
                        clusterScaler.addCompletedJob(iJ, random.choice(list(range(1, x.wallTime))))

            startTime = time.time()
            # Wait while the cluster processes the jobs
            while (mock.getNumberOfJobsIssued(preemptible=False) > 0
                   or mock.getNumberOfJobsIssued(preemptible=True) > 0
                   or mock.getNumberOfNodes() > 0 or mock.getNumberOfNodes(preemptible=True) > 0):
                logger.debug("Running, non-preemptible queue size: %s, non-preemptible workers: %s, "
                            "preemptible queue size: %s, preemptible workers: %s" %
                            (mock.getNumberOfJobsIssued(preemptible=False),
                             mock.getNumberOfNodes(preemptible=False),
                             mock.getNumberOfJobsIssued(preemptible=True),
                             mock.getNumberOfNodes(preemptible=True)))
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
        Test scaling for a batch of non-preemptible jobs and no preemptible jobs (makes debugging
        easier).
        """
        config = Config()

        # Make defaults dummy values
        config.defaultMemory = h2b('1Gi')
        config.defaultCores = 1
        config.defaultDisk = h2b('1Gi')

        # No preemptible nodes/jobs
        config.maxPreemptibleNodes = []  # No preemptible nodes

        # Non-preemptible parameters
        config.nodeTypes = [Shape(20, h2b('10Gi'), 10, h2b('100Gi'), False)]
        config.minNodes = [0]
        config.maxNodes = [10]

        # Algorithm parameters
        config.targetTime = defaultTargetTime
        config.betaInertia = 0.1
        config.scaleInterval = 3

        self._testClusterScaling(config, numJobs=100, numPreemptibleJobs=0,
                                 jobShape=Shape(20, h2b('7Gi'), 10, h2b('80Gi'), False))

    @slow
    def testClusterScalingMultipleNodeTypes(self):

        small_node = Shape(20, h2b('5Gi'), 10, h2b('20Gi'), False)
        small_job = Shape(20, h2b('3Gi'), 10, h2b('4Gi'), False)
        medium_node = Shape(20, h2b('10Gi'), 10, h2b('20Gi'), False)
        medium_job = Shape(20, h2b('7Gi'), 10, h2b('4Gi'), False)
        large_node = Shape(20, h2b('20Gi'), 10, h2b('20Gi'), False)
        large_job = Shape(20, h2b('16Gi'), 10, h2b('4Gi'), False)

        numJobs = 100

        config = Config()

        # Make defaults dummy values
        config.defaultMemory = h2b('1Gi')
        config.defaultCores = 1
        config.defaultDisk = h2b('1Gi')

        # No preemptible nodes/jobs
        config.preemptibleNodeTypes = []
        config.minPreemptibleNodes = []
        config.maxPreemptibleNodes = []  # No preemptible nodes

        # Make sure the node types don't have to be ordered
        config.nodeTypes = [large_node, small_node, medium_node]
        config.minNodes = [0, 0, 0]
        config.maxNodes = [10, 10]  # test expansion of this list

        # Algorithm parameters
        config.targetTime = defaultTargetTime
        config.betaInertia = 0.1
        config.scaleInterval = 3

        mock = MockBatchSystemAndProvisioner(config=config, secondsPerJob=2.0)
        mock.setAutoscaledNodeTypes([({t}, None) for t in config.nodeTypes])
        clusterScaler = ScalerThread(mock, mock, config, stop_on_exception=True)
        clusterScaler.start()
        mock.start()

        try:
            # Add small jobs
            list(map(lambda x: mock.addJob(jobShape=small_job), list(range(numJobs))))
            list(map(lambda x: mock.addJob(jobShape=medium_job), list(range(numJobs))))

            # Add medium completed jobs
            for i in range(1000):
                iJ = JobDescription(requirements=dict(
                                        memory=random.choice(range(small_job.memory, medium_job.memory)),
                                        cores=medium_job.cores,
                                        disk=large_job.disk,
                                        preemptible=False),
                                    jobName='testClusterScaling', unitName='')
                clusterScaler.addCompletedJob(iJ, random.choice(range(1, 10)))

            while mock.getNumberOfJobsIssued() > 0 or mock.getNumberOfNodes() > 0:
                logger.debug("%i nodes currently provisioned" % mock.getNumberOfNodes())
                # Make sure there are no large nodes
                self.assertEqual(mock.getNumberOfNodes(nodeType=large_node), 0)
                clusterScaler.check()
                time.sleep(0.5)
        finally:
            clusterScaler.shutdown()
            mock.shutDown()

        # Make sure jobs ran on both the small and medium node types
        self.assertTrue(mock.totalJobs > 0)
        self.assertTrue(mock.maxWorkers[small_node] > 0)
        self.assertTrue(mock.maxWorkers[medium_node] > 0)

        self.assertEqual(mock.maxWorkers[large_node], 0)

    @slow
    def testClusterScalingWithPreemptibleJobs(self):
        """
        Test scaling simultaneously for a batch of preemptible and non-preemptible jobs.
        """
        config = Config()

        node_shape = Shape(20, h2b('10Gi'), 10, h2b('20Gi'), False)
        preemptible_node_shape = Shape(20, h2b('10Gi'), 10, h2b('20Gi'), True)
        job_shape = Shape(20, h2b('7Gi'), 10, h2b('2Gi'), False)
        preemptible_job_shape = Shape(20, h2b('7Gi'), 10, h2b('2Gi'), True)

        # Make defaults dummy values
        config.defaultMemory = h2b('1Gi')
        config.defaultCores = 1
        config.defaultDisk = h2b('1Gi')

        # non-preemptible node parameters
        config.nodeTypes = [node_shape, preemptible_node_shape]
        config.minNodes = [0, 0]
        config.maxNodes = [10, 10]

        # Algorithm parameters
        config.targetTime = defaultTargetTime
        config.betaInertia = 0.9
        config.scaleInterval = 3

        self._testClusterScaling(config, numJobs=100, numPreemptibleJobs=100, jobShape=job_shape)


class MockBatchSystemAndProvisioner(AbstractScalableBatchSystem, AbstractProvisioner):
    """Mimics a leader, job batcher, provisioner and scalable batch system."""
    def __init__(self, config, secondsPerJob):
        super().__init__(clusterName='clusterName', clusterType='mesos')
        # To mimic parallel preemptible and non-preemptible queues
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
        self.toilState = Namespace()
        self.toilState.bus = Namespace()
        self.toilState.bus.publish = MagicMock()

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

    # AbstractProvisioner methods
    def setAutoscaledNodeTypes(self, node_types: List[Tuple[Set[Shape], Optional[float]]]):
        self.node_shapes_for_testing = sorted(it for t in node_types for it in t[0])
        super().setAutoscaledNodeTypes(node_types)

    def getProvisionedWorkers(self, instance_type=None, preemptible=None):
        """
        Returns a list of Node objects, each representing a worker node in the cluster

        :param preemptible: If True only return preemptible nodes else return non-preemptible nodes
        :return: list of Node
        """
        nodesToWorker = self.nodesToWorker
        results = []
        if instance_type:
            results = [node for node in nodesToWorker if node.nodeType == instance_type]
        else:
            results = list(nodesToWorker.keys())
        if preemptible is not None:
            results = [node for node in results if node.preemptible == preemptible]
        return results

    def terminateNodes(self, nodes):
        if nodes:
            self._removeNodes(nodes)

    def remainingBillingInterval(self, node):
        pass

    def addJob(self, jobShape, preemptible=False):
        """
        Add a job to the job queue
        """
        self.totalJobs += 1
        jobID = uuid.uuid4()
        self.jobBatchSystemIDToIssuedJob[jobID] = JobDescription(requirements={"memory": jobShape.memory,
                                                                               "cores": jobShape.cores,
                                                                               "disk": jobShape.disk,
                                                                               "preemptible": preemptible},
                                                                 jobName=f'job{self.totalJobs}')
        self.jobQueue.put(jobID)

    # JobBatcher functionality
    def getNumberOfJobsIssued(self, preemptible=None):
        if preemptible is not None:
            jobList = [job for job in list(self.jobQueue.queue) if
                       self.jobBatchSystemIDToIssuedJob[job].preemptible == preemptible]
            return len(jobList)
        else:
            return self.jobQueue.qsize()

    def getJobs(self):
        return self.jobBatchSystemIDToIssuedJob.values()

    # AbstractScalableBatchSystem functionality
    def getNodes(self, preemptible: Optional[bool] = False, timeout: int = 600):
        nodes = dict()
        for node in self.nodesToWorker:
            if node.preemptible == preemptible:
                worker = self.nodesToWorker[node]
                nodes[node.privateIP] = NodeInfo(coresTotal=0, coresUsed=0, requestedCores=1,
                                                 memoryTotal=0, memoryUsed=0, requestedMemory=1,
                                                 workers=1 if worker.busyEvent.is_set() else 0)
        return nodes

    # AbstractProvisioner functionality
    def addNodes(self, nodeTypes: Set[str], numNodes, preemptible) -> int:
        nodeType = next(iter(nodeTypes))
        self._addNodes(numNodes=numNodes, nodeType=nodeType, preemptible=preemptible)
        return self.getNumberOfNodes(nodeType=nodeType, preemptible=preemptible)

    def getNodeShape(self, nodeType, preemptible=False):
        # Assume node shapes and node types are the same thing for testing
        # TODO: this isn't really allowed by the type requirements on AbstractProvisioner
        return nodeType

    def getWorkersInCluster(self, nodeShape):
        return self.workers[nodeShape]

    def launchCluster(self, leaderNodeType, keyName, userTags=None,
                      vpcSubnet=None, leaderStorage=50, nodeStorage=50, botoPath=None, **kwargs):
        pass

    def destroyCluster(self) -> None:
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

    def _addNodes(self, numNodes, nodeType: str, preemptible=False):
        nodeShape = self.getNodeShape(nodeType=nodeType, preemptible=preemptible)

        class Worker:
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
                    preemptible=preemptible)
            self.nodesToWorker[node] = Worker(self.jobQueue, self.updatedJobsQueue, self.secondsPerJob)
            self.workers[nodeShape].append(self.nodesToWorker[node])
        self.maxWorkers[nodeShape] = max(self.maxWorkers[nodeShape], len(self.workers[nodeShape]))

    def _removeNodes(self, nodes):
        logger.debug("Removing nodes. %s workers and %s to terminate.", len(self.nodesToWorker), len(nodes))
        for node in nodes:
            try:
                nodeShape = self.getNodeShape(node.nodeType, node.preemptible)
                worker = self.nodesToWorker.pop(node)
                self.workers[nodeShape].pop()
                self.totalWorkerTime += worker.stop()
            except KeyError:
                # Node isn't our responsibility
                pass

    def getNumberOfNodes(self, nodeType=None, preemptible=None):
        if nodeType:
            nodeShape = self.getNodeShape(nodeType=nodeType, preemptible=preemptible)
            return len(self.workers[nodeShape])
        else:
            return len(self.nodesToWorker)
