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

import logging
from collections import deque
from threading import Lock

from bd2k.util.exceptions import require
from bd2k.util.threading import ExceptionalThread
from bd2k.util.throttle import throttle

from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem
from toil.common import Config
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape

logger = logging.getLogger(__name__)

# A *deficit* exists when we have more jobs that can run on preemptable nodes than we have
# preemptable nodes. In order to not block these jobs, we want to increase the number of non-
# preemptable nodes that we have and need for just non-preemptable jobs. However, we may still
# prefer waiting for preemptable instances to come available.
#
# To accomodate this, we set the delta to the difference between the number of provisioned
# preemptable nodes and the number of nodes that were requested. when the non-preemptable thread
# wants to provision nodes, it will multiply this delta times a preference for preemptable vs.
# non-preemptable nodes.

_preemptableNodeDeficit = 0


class RecentJobShapes(object):
    """
    Used to track the 'shapes' of the last N jobs run (see Shape).
    """

    def __init__(self, config, nodeShape, N=1000):
        # As a prior we start of with 10 jobs each with the default memory, cores, and disk. To
        # estimate the running time we use the the default wall time of each node allocation,
        # so that one job will fill the time per node.
        self.jobShapes = deque(maxlen=N,
                               iterable=10 * [Shape(wallTime=nodeShape.wallTime,
                                                    memory=config.defaultMemory,
                                                    cores=config.defaultCores,
                                                    disk=config.defaultDisk)])
        # Calls to add and getLastNJobShapes may be concurrent
        self.lock = Lock()
        # Number of jobs to average over
        self.N = N

    def add(self, jobShape):
        """
        Adds a job shape as the last completed job.

        :param Shape jobShape: The memory, core and disk requirements of the completed job
        """
        with self.lock:
            self.jobShapes.append(jobShape)

    def get(self):
        """
        Gets the last N job shapes added.
        """
        with self.lock:
            return list(self.jobShapes)


def binPacking(jobShapes, nodeShape):
    """
    Use a first fit decreasing (FFD) bin packing like algorithm to calculate an approximate
    minimum number of nodes that will fit the given list of jobs.

    :param Shape nodeShape: The properties of an atomic node allocation, in terms of wall-time,
           memory, cores and local disk.

    :param list[Shape] jobShapes: A list of shapes, each representing a job.

    Let a *node reservation* be an interval of time that a node is reserved for, it is defined by
    an integer number of node-allocations.

    For a node reservation its *jobs* are the set of jobs that will be run within the node
    reservation.

    A minimal node reservation has time equal to one atomic node allocation, or the minimum
    number node allocations to run the longest running job in its jobs.

    :rtype: int

    :returns: The minimum number of minimal node allocations estimated to be required to run all
              the jobs in jobShapes.
    """
    logger.debug('Running bin packing for node shape %s and %s job(s).', nodeShape, len(jobShapes))
    # Sort in ascending order. The FFD like-strategy will schedule the jobs in order from longest
    # to shortest.
    jobShapes.sort()

    class NodeReservation(object):
        """
        Represents a node reservation. To represent the resources available in a reservation a
        node reservation is represented as a sequence of Shapes, each giving the resources free
        within the given interval of time
        """

        def __init__(self, shape):
            # The wall-time and resource available
            self.shape = shape
            # The next portion of the reservation
            self.nReservation = None

    nodeReservations = []  # The list of node reservations

    for jS in jobShapes:
        def addToReservation():
            """
            Function adds the job, jS, to the first node reservation in which it will fit (this
            is the bin-packing aspect)
            """

            def fits(x, y):
                """
                Check if a job shape's resource requirements will fit within a given node allocation
                """
                return y.memory <= x.memory and y.cores <= x.cores and y.disk <= x.disk

            def subtract(x, y):
                """
                Adjust available the resources of a node allocation as a job is scheduled within it.
                """
                return Shape(x.wallTime, x.memory - y.memory, x.cores - y.cores, x.disk - y.disk)

            def split(x, y, t):
                """
                Partition a node allocation into two
                """
                return (Shape(t, x.memory - y.memory, x.cores - y.cores, x.disk - y.disk),
                        NodeReservation(Shape(x.wallTime - t, x.memory, x.cores, x.disk)))

            i = 0
            while True:
                # Case a new node reservation is required
                if i == len(nodeReservations):
                    x = NodeReservation(subtract(nodeShape, jS))
                    nodeReservations.append(x)
                    t = nodeShape.wallTime
                    while t < jS.wallTime:
                        y = NodeReservation(x.shape)
                        t += nodeShape.wallTime
                        x.nReservation = y
                        x = y
                    return

                # Attempt to add the job to node reservation i
                x = nodeReservations[i]
                y = x
                t = 0
                while True:
                    if fits(y.shape, jS):
                        t += y.shape.wallTime
                        if t >= jS.wallTime:
                            # Insert into reservation between x and y and return
                            t = 0
                            while x != y:
                                x.shape = subtract(x.shape, jS)
                                t += x.shape.wallTime
                                x = x.nReservation
                            assert x == y
                            assert jS.wallTime - t <= x.shape.wallTime
                            if jS.wallTime - t < x.shape.wallTime:
                                x.shape, nS = split(x.shape, jS, jS.wallTime - t)
                                nS.nReservation = x.nReservation
                                x.nReservation = nS
                            else:
                                assert jS.wallTime - t == x.shape.wallTime
                                x.shape = subtract(x.shape, jS)
                            return
                    else:
                        x = y.nReservation
                        t = 0
                    y = y.nReservation
                    if y is None:
                        # Reached the end of the reservation without success so stop trying to
                        # add to reservation i
                        break
                i += 1

        addToReservation()
    logger.debug("Done running bin packing for node shape %s and %s job(s) resulting in %s node "
                 "reservations.", nodeShape, len(jobShapes), len(nodeReservations))
    return len(nodeReservations)


class ClusterScaler(object):
    def __init__(self, provisioner, jobBatcher, config):
        """
        Class manages automatically scaling the number of worker nodes.

        :param AbstractProvisioner provisioner: Provisioner instance to scale.

        :param JobBatcher jobBatcher: The class issuing jobs to the batch system. This is
               monitored to make scaling decisions.

        :param Config config: Config object from which to draw parameters.
        """
        self.provisioner = provisioner
        self.jobBatcher = jobBatcher
        self.config = config
        # Indicates that the scaling threads should shutdown
        self.stop = False

        assert config.maxPreemptableNodes >= 0 and config.maxNodes >= 0
        require(config.maxPreemptableNodes + config.maxNodes > 0,
                'Either --maxNodes or --maxPreemptableNodes must be non-zero.')

        if config.maxPreemptableNodes > 0:
            self.preemptableScaler = ScalerThread(self, preemptable=True)
            self.preemptableScaler.start()
        else:
            self.preemptableScaler = None

        if config.maxNodes > 0:
            self.scaler = ScalerThread(self, preemptable=False)
            self.scaler.start()
        else:
            self.scaler = None

    def shutdown(self):
        """
        Shutdown the cluster.
        """
        self.stop = True
        for scaler in self.preemptableScaler, self.scaler:
            if scaler is not None:
                self.scaler.join()

    def addCompletedJob(self, job, wallTime):
        """
        Adds the shape of a completed job to the queue, allowing the scalar to use the last N
        completed jobs in factoring how many nodes are required in the cluster.

        :param IssuedJob job: The memory, core and disk requirements of the completed job

        :param int wallTime: The wall-time taken to complete the job in seconds.
        """
        s = Shape(wallTime=wallTime, memory=job.memory, cores=job.cores, disk=job.disk)
        if job.preemptable and self.preemptableScaler is not None:
            self.preemptableScaler.jobShapes.add(s)
        else:
            self.scaler.jobShapes.add(s)


class ScalerThread(ExceptionalThread):
    """
    A thread that automatically scales the number of either preemptable or non-preemptable worker
    nodes according to the number of jobs queued and the resource requirements of the last N
    completed jobs.

    The scaling calculation is essentially as follows: Use the RecentJobShapes instance to
    calculate how many nodes, n, can be used to productively compute the last N completed
    jobs. Let M be the number of jobs issued to the batch system. The number of nodes
    required is then estimated to be alpha * n * M/N, where alpha is a scaling factor used to
    adjust the balance between under- and over- provisioning the cluster.

    At each scaling decision point a comparison between the current, C, and newly estimated
    number of nodes is made. If the absolute difference is less than beta * C then no change
    is made, else the size of the cluster is adapted. The beta factor is an inertia parameter
    that prevents continual fluctuations in the number of nodes.
    """

    def __init__(self, scaler, preemptable):
        """
        :param ClusterScaler scaler: the parent class
        """
        super(ScalerThread, self).__init__(name='preemptable-scaler' if preemptable else 'scaler')
        self.scaler = scaler
        self.preemptable = preemptable
        # Resource requirements and wall-time of an atomic node allocation
        self.nodeShape = scaler.provisioner.getNodeShape(preemptable=preemptable)
        # Monitors the requirements of the N most recently completed jobs
        self.jobShapes = RecentJobShapes(scaler.config, self.nodeShape)
        # Minimum/maximum number of either preemptable or non-preemptable nodes in the cluster
        self.minNodes = scaler.config.minPreemptableNodes if preemptable else scaler.config.minNodes
        self.maxNodes = scaler.config.maxPreemptableNodes if preemptable else scaler.config.maxNodes

    def tryRun(self):
        global _preemptableNodeDeficit

        if isinstance(self.scaler.jobBatcher.batchSystem, AbstractScalableBatchSystem):
            totalNodes = len(self.scaler.jobBatcher.batchSystem.getNodes(self.preemptable))
        else:
            totalNodes = 0
        logger.info('Starting with %s node(s) in the cluster.', totalNodes)
        while not self.scaler.stop:
            with throttle(self.scaler.config.scaleInterval):
                # Calculate the approx. number nodes needed
                # TODO: Correct for jobs already running which can be considered fractions of a job
                queueSize = self.scaler.jobBatcher.getNumberOfJobsIssued(preemptable=self.preemptable)

                recentJobShapes = self.jobShapes.get()
                assert len(recentJobShapes) > 0
                nodesToRunRecentJobs = binPacking(recentJobShapes, self.nodeShape)
                estimatedNodes = 0 if queueSize == 0 else max(1, int(round(
                    self.scaler.config.alphaPacking
                    * nodesToRunRecentJobs
                    * float(queueSize)
                    / len(recentJobShapes))))

                # If we're the non-preemptable scaler, we need to see if we have a deficit of
                # preemptable nodes that we should compensate for.
                if not self.preemptable:
                    compensation = self.scaler.config.preemptableCompensation
                    assert 0.0 <= compensation <= 1.0
                    # The number of nodes we provision as compensation for missing preemptable
                    # nodes is the product of the deficit (the number of preemptable nodes we did
                    # _not_ allocate) and configuration preference.
                    compensationNodes = int(round(_preemptableNodeDeficit * compensation))
                    logger.info('Adding %d preemptable nodes to compensate for a deficit of %d '
                                'non-preemptable ones.', compensationNodes, _preemptableNodeDeficit)
                    estimatedNodes += compensationNodes

                fix_my_name = (0 if nodesToRunRecentJobs <= 0
                               else len(recentJobShapes) / float(nodesToRunRecentJobs))
                logger.debug('Estimating that cluster needs %s nodes of shape %s, from current '
                             'size of %s, given a queue size of %s, the number of jobs per node '
                             'estimated to be %s and an alpha parameter of %s.',
                             estimatedNodes, self.nodeShape, totalNodes, queueSize, fix_my_name,
                             self.scaler.config.alphaPacking)

                # Use inertia parameter to stop small fluctuations
                if estimatedNodes <= totalNodes * self.scaler.config.betaInertia <= estimatedNodes:
                    logger.debug('Difference in new (%s) and previous estimates in number of '
                                 'nodes (%s) required is within beta (%s), making no change.',
                                 estimatedNodes, totalNodes, self.scaler.config.betaInertia)
                    estimatedNodes = totalNodes

                # Bound number using the max and min node parameters
                if estimatedNodes > self.maxNodes:
                    logger.info('Limiting the estimated number of necessary nodes (%s) to the '
                                'configured maximum (%s).', estimatedNodes, self.maxNodes)
                    estimatedNodes = self.maxNodes
                elif estimatedNodes < self.minNodes:
                    logger.info('Raising the estimated number of necessary nodes (%s) to the '
                                'configured mininimum (%s).', estimatedNodes, self.minNodes)
                    estimatedNodes = self.minNodes

                if estimatedNodes != totalNodes:
                    logger.info('Changing the number of worker nodes from %s to %s.', totalNodes,
                                estimatedNodes)
                    totalNodes = self.scaler.provisioner.setNodeCount(numNodes=estimatedNodes,
                                                                      preemptable=self.preemptable)
                    
                    # If we were scaling up the number of preemptable nodes and failed to meet
                    # our target, we need to update the slack so that non-preemptable nodes will
                    # be allocated instead and we won't block. If we _did_ meet our target,
                    # we need to reset the slack to 0.
                    if self.preemptable:
                        if totalNodes < estimatedNodes:
                            deficit = estimatedNodes - totalNodes
                            logger.info('Preemptable scaler detected deficit of %d nodes.', deficit)
                            _preemptableNodeDeficit = deficit
                        else:
                            _preemptableNodeDeficit = 0
                    
        logger.info('Forcing provisioner to reduce cluster size to zero.')
        totalNodes = self.scaler.provisioner.setNodeCount(numNodes=0,
                                                          preemptable=self.preemptable,
                                                          force=True)
        if totalNodes != 0:
            raise RuntimeError('Provisioner was not able to reduce cluster size to zero.')
        else:
            logger.info('Scaler exited normally.')
