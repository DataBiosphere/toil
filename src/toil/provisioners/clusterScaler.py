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
import json
import logging
import os
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from toil.batchSystems.abstractBatchSystem import (
    AbstractBatchSystem,
    AbstractScalableBatchSystem,
    NodeInfo,
)
from toil.common import Config, defaultTargetTime
from toil.job import JobDescription, ServiceJobDescription
from toil.lib.retry import old_retry
from toil.lib.threading import ExceptionalThread
from toil.lib.throttle import throttle
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape

if TYPE_CHECKING:
    from toil.leader import Leader
    from toil.provisioners.node import Node

logger = logging.getLogger(__name__)


class BinPackedFit:
    """
    If jobShapes is a set of tasks with run requirements (mem/disk/cpu), and nodeShapes is a sorted
    list of available computers to run these jobs on, this function attempts to return a dictionary
    representing the minimum set of computerNode computers needed to run the tasks in jobShapes.

    Uses a first fit decreasing (FFD) bin packing like algorithm to calculate an approximate minimum
    number of nodes that will fit the given list of jobs.  BinPackingFit assumes the ordered list,
    nodeShapes, is ordered for "node preference" outside of BinPackingFit beforehand. So when
    virtually "creating" nodes, the first node within nodeShapes that fits the job is the one
    that's added.

    :param list nodeShapes: The properties of an atomic node allocation, in terms of wall-time,
                            memory, cores, disk, and whether it is preemptable or not.
    :param targetTime: The time before which all jobs should at least be started.

    :returns: The minimum number of minimal node allocations estimated to be required to run all
              the jobs in jobShapes.
    """
    nodeReservations: Dict[Shape, List['NodeReservation']]

    def __init__(self, nodeShapes: List[Shape], targetTime: float = defaultTargetTime) -> None:
        self.nodeShapes = sorted(nodeShapes)
        self.targetTime = targetTime
        self.nodeReservations = {nodeShape: [] for nodeShape in nodeShapes}

    def binPack(self, jobShapes: List[Shape]) -> None:
        """Pack a list of jobShapes into the fewest nodes reasonable. Can be run multiple times."""
        # TODO: Check for redundancy with batchsystems.mesos.JobQueue() sorting
        logger.debug('Running bin packing for node shapes %s and %s job(s).',
                     self.nodeShapes, len(jobShapes))
        # Sort in descending order from largest to smallest. The FFD like-strategy will pack the
        # jobs in order from longest to shortest.
        jobShapes.sort()
        jobShapes.reverse()
        assert len(jobShapes) == 0 or jobShapes[0] >= jobShapes[-1]
        for jS in jobShapes:
            self.addJobShape(jS)

    def addJobShape(self, jobShape: Shape) -> None:
        """
        Add the job to the first node reservation in which it will fit.

        (this is the bin-packing aspect).
        """
        chosenNodeShape = None
        for nodeShape in self.nodeShapes:
            if NodeReservation(nodeShape).fits(jobShape):
                # This node shape is the first that fits this jobShape
                chosenNodeShape = nodeShape
                break

        if chosenNodeShape is None:
            logger.warning("Couldn't fit job with requirements %r into any nodes in the nodeTypes "
                           "list." % jobShape)
            return

        # grab current list of job objects appended to this instance type
        nodeReservations = self.nodeReservations[chosenNodeShape]
        for nodeReservation in nodeReservations:
            if nodeReservation.attemptToAddJob(jobShape, chosenNodeShape, self.targetTime):
                # We succeeded adding the job to this node reservation. Now we're done.
                return

        reservation = NodeReservation(chosenNodeShape)
        currentTimeAllocated = chosenNodeShape.wallTime
        adjustEndingReservationForJob(reservation, jobShape, 0)
        self.nodeReservations[chosenNodeShape].append(reservation)

        # Extend the reservation if necessary to cover the job's entire runtime.
        while currentTimeAllocated < jobShape.wallTime:
            extendThisReservation = NodeReservation(reservation.shape)
            currentTimeAllocated += chosenNodeShape.wallTime
            reservation.nReservation = extendThisReservation
            reservation = extendThisReservation

    def getRequiredNodes(self) -> Dict[Shape, int]:
        """Return a dict from node shape to number of nodes required to run the packed jobs."""
        return {
            nodeShape: len(self.nodeReservations[nodeShape])
            for nodeShape in self.nodeShapes
        }


class NodeReservation:
    """
    The amount of resources that we expect to be available on a given node at each point in time.

    To represent the resources available in a reservation, we represent a
    reservation as a linked list of NodeReservations, each giving the
    resources free within a single timeslice.
    """

    def __init__(self, shape: Shape) -> None:
        # The wall-time of this slice and resources available in this timeslice
        self.shape = shape
        # The next portion of the reservation (None if this is the end)
        self.nReservation: Optional[NodeReservation] = None

    def __str__(self) -> str:
        return "-------------------\n" \
               "Current Reservation\n" \
               "-------------------\n" \
               "Shape wallTime: %s\n" \
               "Shape memory: %s\n" \
               "Shape cores: %s\n" \
               "Shape disk: %s\n" \
               "Shape preempt: %s\n" \
               "\n" \
               "nReserv wallTime: %s\n" \
               "nReserv memory: %s\n" \
               "nReserv cores: %s\n" \
               "nReserv disk: %s\n" \
               "nReserv preempt: %s\n" \
               "\n" \
               "Time slices: %s\n" \
               "\n" % \
               (self.shape.wallTime,
                self.shape.memory,
                self.shape.cores,
                self.shape.disk,
                self.shape.preemptable,
                self.nReservation.shape.wallTime if self.nReservation is not None else str(None),
                self.nReservation.shape.memory if self.nReservation is not None else str(None),
                self.nReservation.shape.cores if self.nReservation is not None else str(None),
                self.nReservation.shape.disk if self.nReservation is not None else str(None),
                self.nReservation.shape.preemptable if self.nReservation is not None else str(None),
                str(len(self.shapes())))

    def fits(self, jobShape: Shape) -> bool:
        """Check if a job shape's resource requirements will fit within this allocation."""
        return jobShape.memory <= self.shape.memory and \
               jobShape.cores <= self.shape.cores and \
               jobShape.disk <= self.shape.disk and \
               (jobShape.preemptable or not self.shape.preemptable)

    def shapes(self) -> List[Shape]:
        """Get all time-slice shapes, in order, from this reservation on."""
        shapes = []
        curRes: Optional[NodeReservation] = self
        while curRes is not None:
            shapes.append(curRes.shape)
            curRes = curRes.nReservation
        return shapes

    def subtract(self, jobShape: Shape) -> None:
        """Subtract the resources necessary to run a jobShape from the reservation."""
        self.shape = Shape(self.shape.wallTime,
                           self.shape.memory - jobShape.memory,
                           self.shape.cores - jobShape.cores,
                           self.shape.disk - jobShape.disk,
                           self.shape.preemptable)

    def attemptToAddJob(
        self, jobShape: Shape, nodeShape: Shape, targetTime: float
    ) -> bool:
        """
        Attempt to pack a job into this reservation timeslice and/or the reservations after it.

        jobShape is the Shape of the job requirements, nodeShape is the Shape of the node this
        is a reservation for, and targetTime is the maximum time to wait before starting this job.
        """
        # starting slice of time that we can fit in so far
        startingReservation: Optional[NodeReservation] = self
        # current end of the slices we can fit in so far
        endingReservation = self
        # the amount of runtime of the job currently covered by slices
        availableTime: float = 0
        # total time from when the instance started up to startingReservation
        startingReservationTime: float = 0

        while True:
            # True == can run the job (resources & preemptable only; NO time)
            if endingReservation.fits(jobShape):
                # add the time left available on the reservation
                availableTime += endingReservation.shape.wallTime
                # does the job time fit in the reservation's remaining time?
                if availableTime >= jobShape.wallTime:
                    timeSlice: float = 0
                    while (startingReservation != endingReservation):
                        # removes resources only (NO time) from startingReservation
                        startingReservation.subtract(jobShape)  # type: ignore
                        # set aside the timeSlice
                        timeSlice += startingReservation.shape.wallTime  # type: ignore
                        startingReservation = startingReservation.nReservation  # type: ignore
                    assert jobShape.wallTime - timeSlice <= startingReservation.shape.wallTime
                    adjustEndingReservationForJob(endingReservation, jobShape, timeSlice)
                    # Packed the job.
                    return True

                # If the job would fit, but is longer than the total node allocation
                # extend the node allocation
                elif endingReservation.nReservation == None and startingReservation == self:
                    # Extend the node reservation to accommodate jobShape
                    endingReservation.nReservation = NodeReservation(nodeShape)
            # can't run the job with the current resources
            else:
                if startingReservationTime + availableTime + endingReservation.shape.wallTime <= targetTime:
                    startingReservation = endingReservation.nReservation
                    startingReservationTime += availableTime + endingReservation.shape.wallTime
                    availableTime = 0
                else:
                    break
            nextEndingReservation = endingReservation.nReservation
            if nextEndingReservation is None:
                # Reached the end of the reservation without success so stop trying to
                # add to reservation
                return False
            else:
                endingReservation = nextEndingReservation

        # Couldn't pack the job.
        return False


def adjustEndingReservationForJob(
    reservation: NodeReservation, jobShape: Shape, wallTime: float
) -> None:
    """
    Add a job to an ending reservation that ends at wallTime.

    (splitting the reservation if the job doesn't fill the entire timeslice)
    """
    if jobShape.wallTime - wallTime < reservation.shape.wallTime:
        # This job only partially fills one of the slices. Create a new slice.
        reservation.shape, nS = split(reservation.shape, jobShape, jobShape.wallTime - wallTime)
        nS.nReservation = reservation.nReservation
        reservation.nReservation = nS
    else:
        # This job perfectly fits within the boundaries of the slices.
        reservation.subtract(jobShape)


def split(
    nodeShape: Shape, jobShape: Shape, wallTime: float
) -> Tuple[Shape, NodeReservation]:
    """
    Partition a node allocation into two to fit the job.

    Returning the modified shape of the node and a new node reservation for
    the extra time that the job didn't fill.
    """
    return (Shape(wallTime,
                  nodeShape.memory - jobShape.memory,
                  nodeShape.cores - jobShape.cores,
                  nodeShape.disk - jobShape.disk,
                  nodeShape.preemptable),
            NodeReservation(Shape(nodeShape.wallTime - wallTime,
                                  nodeShape.memory,
                                  nodeShape.cores,
                                  nodeShape.disk,
                                  nodeShape.preemptable)))


def binPacking(nodeShapes: List[Shape], jobShapes: List[Shape], goalTime: float) -> Dict[Shape, int]:
    bpf = BinPackedFit(nodeShapes, goalTime)
    bpf.binPack(jobShapes)
    return bpf.getRequiredNodes()


class ClusterScaler:
    def __init__(
        self, provisioner: AbstractProvisioner, leader: "Leader", config: Config
    ) -> None:
        """
        Class manages automatically scaling the number of worker nodes.

        :param provisioner: Provisioner class with functions to create/remove resources (i.e. AWS).
        :param leader: Leader class which runs the main workflow loop until complete.
        :param config: Config object from which to draw parameters.
        """
        self.provisioner = provisioner
        self.leader = leader
        self.config = config
        self.static: Dict[bool, Dict[str, "Node"]] = {}

        # Dictionary of job names to their average runtime, used to estimate wall time of queued
        # jobs for bin-packing
        self.jobNameToAvgRuntime: Dict[str, float] = {}
        self.jobNameToNumCompleted: Dict[str, int] = {}
        self.totalAvgRuntime = 0.0
        self.totalJobsCompleted = 0

        self.targetTime: float = config.targetTime
        if self.targetTime <= 0:
            raise RuntimeError('targetTime (%s) must be a positive integer!' % self.targetTime)
        self.betaInertia = config.betaInertia
        if not 0.0 <= self.betaInertia <= 0.9:
            raise RuntimeError('betaInertia (%f) must be between 0.0 and 0.9!' % self.betaInertia)


        # Pull scaling information from the provisioner.
        self.nodeShapeToType = provisioner.getAutoscaledInstanceShapes()
        self.instance_types = list(self.nodeShapeToType.values())
        self.nodeShapes = list(self.nodeShapeToType.keys())

        self.ignoredNodes: Set[str] = set()

        # A *deficit* exists when we have more jobs that can run on preemptable
        # nodes than we have preemptable nodes. In order to not block these jobs,
        # we want to increase the number of non-preemptable nodes that we have and
        # need for just non-preemptable jobs. However, we may still
        # prefer waiting for preemptable instances to come available.
        # To accommodate this, we set the delta to the difference between the number
        # of provisioned preemptable nodes and the number of nodes that were requested.
        # Then, when provisioning non-preemptable nodes of the same type, we attempt to
        # make up the deficit.
        self.preemptableNodeDeficit = {instance_type: 0 for instance_type in self.instance_types}

        # Keeps track of the last raw (i.e. float, not limited by
        # max/min nodes) estimates of the number of nodes needed for
        # each node shape. NB: we start with an estimate of 0, so
        # scaling up is smoothed as well.
        self.previousWeightedEstimate = {nodeShape: 0.0 for nodeShape in self.nodeShapes}

        assert len(self.nodeShapes) > 0

        # Minimum/maximum number of either preemptable or non-preemptable nodes in the cluster
        minNodes = config.minNodes
        if minNodes is None:
            minNodes = [0 for node in self.instance_types]
        maxNodes = config.maxNodes
        while len(maxNodes) < len(self.instance_types):
            # Pad out the max node counts if we didn't get one per type.
            maxNodes.append(maxNodes[0])
        while len(minNodes) < len(self.instance_types):
            # Pad out the min node counts with 0s, so we can have fewer than
            # the node types without crashing.
            minNodes.append(0)
        self.minNodes = dict(zip(self.nodeShapes, minNodes))
        self.maxNodes = dict(zip(self.nodeShapes, maxNodes))

        self.nodeShapes.sort()

        #Node shape to number of currently provisioned nodes
        totalNodes: Dict[Shape, int] = defaultdict(int)
        if isinstance(leader.batchSystem, AbstractScalableBatchSystem) and leader.provisioner:
            for preemptable in (True, False):
                nodes: List[Node] = []
                for nodeShape, instance_type in self.nodeShapeToType.items():
                    nodes_thisType = leader.provisioner.getProvisionedWorkers(instance_type=instance_type,
                                                                              preemptable=preemptable)
                    totalNodes[nodeShape] += len(nodes_thisType)
                    nodes.extend(nodes_thisType)

                self.setStaticNodes(nodes, preemptable)

        logger.debug('Starting with the following nodes in the cluster: %s' % totalNodes)

        if not sum(config.maxNodes) > 0:
            raise RuntimeError('Not configured to create nodes of any type.')

    def _round(self, number: float) -> int:
        """
        Helper function for rounding-as-taught-in-school (X.5 rounds to X+1 if positive).
        Python 3 now rounds 0.5 to whichever side is even (i.e. 2.5 rounds to 2).

        :param int number: a float to round.
        :return: closest integer to number, rounding ties away from 0.
        """

        sign = 1 if number >= 0 else -1

        rounded = int(round(number))
        nextRounded = int(round(number + 1 * sign))

        if nextRounded == rounded:
            # We rounded X.5 to even, and it was also away from 0.
            return rounded
        elif nextRounded == rounded + 1 * sign:
            # We rounded normally (we are in Python 2)
            return rounded
        elif nextRounded == rounded + 2 * sign:
            # We rounded X.5 to even, but it was towards 0.
            # Go away from 0 instead.
            return rounded + 1 * sign
        else:
            # If we get here, something has gone wrong.
            raise RuntimeError(f"Could not round {number}")

    def getAverageRuntime(self, jobName: str, service: bool = False) -> float:
        if service:
            # We short-circuit service jobs and assume that they will
            # take a very long time, because if they are assumed to
            # take a short time, we may try to pack multiple services
            # into the same core/memory/disk "reservation", one after
            # the other. That could easily lead to underprovisioning
            # and a deadlock, because often multiple services need to
            # be running at once for any actual work to get done.
            return self.targetTime * 24 + 3600
        if jobName in self.jobNameToAvgRuntime:
            #Have seen jobs of this type before, so estimate
            #the runtime based on average of previous jobs of this type
            return self.jobNameToAvgRuntime[jobName]
        elif self.totalAvgRuntime > 0:
            #Haven't seen this job yet, so estimate its runtime as
            #the average runtime of all completed jobs
            return self.totalAvgRuntime
        else:
            #Have no information whatsoever
            return 1.0

    def addCompletedJob(self, job: JobDescription, wallTime: int) -> None:
        """
        Adds the shape of a completed job to the queue, allowing the scalar to use the last N
        completed jobs in factoring how many nodes are required in the cluster.
        :param toil.job.JobDescription job: The description of the completed job
        :param int wallTime: The wall-time taken to complete the job in seconds.
        """

        #Adjust average runtimes to include this job.
        if job.jobName in self.jobNameToAvgRuntime:
            prevAvg = self.jobNameToAvgRuntime[job.jobName]
            prevNum = self.jobNameToNumCompleted[job.jobName]
            self.jobNameToAvgRuntime[job.jobName] = float(prevAvg*prevNum + wallTime)/(prevNum + 1)
            self.jobNameToNumCompleted[job.jobName] += 1
        else:
            self.jobNameToAvgRuntime[job.jobName] = wallTime
            self.jobNameToNumCompleted[job.jobName] = 1

        self.totalJobsCompleted += 1
        self.totalAvgRuntime = float(self.totalAvgRuntime * (self.totalJobsCompleted - 1) + \
                                     wallTime)/self.totalJobsCompleted

    def setStaticNodes(self, nodes: List["Node"], preemptable: bool) -> bool:
        """
        Used to track statically provisioned nodes. This method must be called
        before any auto-scaled nodes are provisioned.

        These nodes are treated differently than auto-scaled nodes in that they should
        not be automatically terminated.

        :param nodes: list of Node objects
        """
        prefix = 'non-' if not preemptable else ''
        logger.debug("Adding %s to %spreemptable static nodes", nodes, prefix)
        if nodes is not None:
            self.static[preemptable] = {node.privateIP : node for node in nodes}

    def getStaticNodes(self, preemptable: bool) -> Dict[str, "Node"]:
        """
        Returns nodes set in setStaticNodes().

        :param preemptable:
        :return: Statically provisioned nodes.
        """
        return self.static[preemptable]

    def smoothEstimate(self, nodeShape: Shape, estimatedNodeCount: int) -> int:
        """
        Smooth out fluctuations in the estimate for this node compared to previous runs.

        Returns an integer.
        """
        weightedEstimate = (1 - self.betaInertia) * estimatedNodeCount + \
                           self.betaInertia * self.previousWeightedEstimate[nodeShape]
        self.previousWeightedEstimate[nodeShape] = weightedEstimate
        return self._round(weightedEstimate)

    def getEstimatedNodeCounts(
        self, queuedJobShapes: List[Shape], currentNodeCounts: Dict[Shape, int]
    ) -> Dict[Shape, int]:
        """
        Given the resource requirements of queued jobs and the current size of the cluster.

        Returns a dict mapping from nodeShape to the number of nodes we want in the cluster right now.
        """
        nodesToRunQueuedJobs = binPacking(jobShapes=queuedJobShapes,
                                          nodeShapes=self.nodeShapes,
                                          goalTime=self.targetTime)
        estimatedNodeCounts = {}
        for nodeShape in self.nodeShapes:
            instance_type = self.nodeShapeToType[nodeShape]

            logger.debug("Nodes of type %s to run queued jobs: %s" % (instance_type, nodesToRunQueuedJobs[nodeShape]))
            # Actual calculation of the estimated number of nodes required
            estimatedNodeCount = 0 if nodesToRunQueuedJobs[nodeShape] == 0 \
                else max(1, self._round(nodesToRunQueuedJobs[nodeShape]))
            logger.debug("Estimating %i nodes of shape %s" % (estimatedNodeCount, nodeShape))

            # Use inertia parameter to smooth out fluctuations according to an exponentially
            # weighted moving average.
            estimatedNodeCount = self.smoothEstimate(nodeShape, estimatedNodeCount)

            # If we're scaling a non-preemptable node type, we need to see if we have a
            # deficit of preemptable nodes of this type that we should compensate for.
            if not nodeShape.preemptable:
                compensation = self.config.preemptableCompensation
                assert 0.0 <= compensation <= 1.0
                # The number of nodes we provision as compensation for missing preemptable
                # nodes is the product of the deficit (the number of preemptable nodes we did
                # _not_ allocate) and configuration preference.
                compensationNodes = self._round(self.preemptableNodeDeficit[instance_type] * compensation)
                if compensationNodes > 0:
                    logger.debug('Adding %d non-preemptable nodes of type %s to compensate for a '
                                'deficit of %d preemptable ones.', compensationNodes,
                                instance_type,
                                self.preemptableNodeDeficit[instance_type])
                estimatedNodeCount += compensationNodes

            logger.debug("Currently %i nodes of type %s in cluster" % (currentNodeCounts[nodeShape],
                                                                      instance_type))
            if self.leader.toilMetrics:
                self.leader.toilMetrics.logClusterSize(instance_type=instance_type,
                                                       currentSize=currentNodeCounts[nodeShape],
                                                       desiredSize=estimatedNodeCount)

            # Bound number using the max and min node parameters
            if estimatedNodeCount > self.maxNodes[nodeShape]:
                logger.debug('Limiting the estimated number of necessary %s (%s) to the '
                             'configured maximum (%s).', instance_type,
                             estimatedNodeCount,
                             self.maxNodes[nodeShape])
                estimatedNodeCount = self.maxNodes[nodeShape]
            elif estimatedNodeCount < self.minNodes[nodeShape]:
                logger.debug('Raising the estimated number of necessary %s (%s) to the '
                            'configured minimum (%s).', instance_type,
                            estimatedNodeCount,
                            self.minNodes[nodeShape])
                estimatedNodeCount = self.minNodes[nodeShape]
            estimatedNodeCounts[nodeShape] = estimatedNodeCount
        return estimatedNodeCounts

    def updateClusterSize(self, estimatedNodeCounts: Dict[Shape, int]) -> Dict[Shape, int]:
        """
        Given the desired and current size of the cluster, attempts to launch/remove instances to get to the desired size.

        Also attempts to remove ignored nodes that were marked for graceful removal.

        Returns the new size of the cluster.
        """
        newNodeCounts = defaultdict(int)
        for nodeShape, estimatedNodeCount in estimatedNodeCounts.items():
            instance_type = self.nodeShapeToType[nodeShape]

            newNodeCount = self.setNodeCount(instance_type, estimatedNodeCount, preemptable=nodeShape.preemptable)
            # If we were scaling up a preemptable node type and failed to meet
            # our target, we will attempt to compensate for the deficit while scaling
            # non-preemptable nodes of this type.
            if nodeShape.preemptable:
                if newNodeCount < estimatedNodeCount:
                    deficit = estimatedNodeCount - newNodeCount
                    logger.debug('Preemptable scaler detected deficit of %d nodes of type %s.' % (deficit, instance_type))
                    self.preemptableNodeDeficit[instance_type] = deficit
                else:
                    self.preemptableNodeDeficit[instance_type] = 0
            newNodeCounts[nodeShape] = newNodeCount

        #Attempt to terminate any nodes that we previously designated for
        #termination, but which still had workers running.
        self._terminateIgnoredNodes()
        return newNodeCounts

    def setNodeCount(
        self,
        instance_type: str,
        numNodes: int,
        preemptable: bool = False,
        force: bool = False,
    ) -> int:
        """
        Attempt to grow or shrink the number of preemptable or non-preemptable worker nodes in
        the cluster to the given value, or as close a value as possible, and, after performing
        the necessary additions or removals of worker nodes, return the resulting number of
        preemptable or non-preemptable nodes currently in the cluster.

        :param instance_type: The instance type to add or remove.

        :param numNodes: Desired size of the cluster

        :param preemptable: whether the added nodes will be preemptable, i.e. whether they
               may be removed spontaneously by the underlying platform at any time.

        :param force: If False, the provisioner is allowed to deviate from the given number
               of nodes. For example, when downsizing a cluster, a provisioner might leave nodes
               running if they have active jobs running on them.

        :return: the number of worker nodes in the cluster after making the necessary
                adjustments. This value should be, but is not guaranteed to be, close or equal to
                the `numNodes` argument. It represents the closest possible approximation of the
                actual cluster size at the time this method returns.
        """
        if not isinstance(self.leader.batchSystem, AbstractScalableBatchSystem):
            raise RuntimeError('Non-scalable batch system abusing a scalable-only function.')
        for attempt in old_retry(predicate=self.provisioner.retryPredicate):
            with attempt:
                nodes = self.getNodes(preemptable)
                logger.debug("Cluster contains %i instances" % len(nodes))

                nodes = {node: nodes[node] for node in nodes if node.nodeType == instance_type}
                ignoredNodes = [node for node in nodes if node.privateIP in self.ignoredNodes]
                numIgnoredNodes = len(ignoredNodes)
                numCurrentNodes = len(nodes)
                logger.debug("Cluster contains %i instances of type %s (%i ignored and draining jobs until "
                             "they can be safely terminated)" % (numCurrentNodes, instance_type, numIgnoredNodes))
                if not force:
                    delta = numNodes - (numCurrentNodes - numIgnoredNodes)
                else:
                    delta = numNodes - numCurrentNodes
                if delta > 0 and numIgnoredNodes > 0:
                    # We can un-ignore a few nodes to compensate for the additional nodes we want.
                    numNodesToUnignore = min(delta, numIgnoredNodes)
                    logger.debug('Unignoring %i nodes because we want to scale back up again.' % numNodesToUnignore)
                    delta -= numNodesToUnignore

                    for node in ignoredNodes[:numNodesToUnignore]:
                        self.ignoredNodes.remove(node.privateIP)
                        self.leader.batchSystem.unignoreNode(node.privateIP)
                if delta > 0:
                    logger.info('Adding %i %s nodes to get to desired cluster size of %i.',
                                delta,
                                'preemptable' if preemptable else 'non-preemptable',
                                numNodes)
                    numNodes = numCurrentNodes + self._addNodes(instance_type, numNodes=delta,
                                                                preemptable=preemptable)
                elif delta < 0:
                    logger.info('Removing %i %s nodes to get to desired cluster size of %i.', -delta, 'preemptable' if preemptable else 'non-preemptable', numNodes)
                    numNodes = numCurrentNodes - self._removeNodes(nodes,
                                                                   instance_type=instance_type,
                                                                   num_nodes=-delta,
                                                                   preemptable=preemptable,
                                                                   force=force)
                elif force:
                    logger.debug('Cluster already at desired size of %i. Nothing to do.', numNodes)
                else:
                    logger.debug('Cluster (minus ignored nodes) already at desired size of %i. Nothing to do.', numNodes)
        return numNodes

    def _addNodes(self, instance_type: str, numNodes: int, preemptable: bool) -> int:
        return self.provisioner.addNodes(nodeTypes={instance_type}, numNodes=numNodes, preemptable=preemptable)

    def _removeNodes(
        self,
        nodes: Dict["Node", NodeInfo],
        instance_type: str,
        num_nodes: int,
        preemptable: bool = False,
        force: bool = False,
    ) -> int:
        # If the batch system is scalable, we can use the number of currently running workers on
        # each node as the primary criterion to select which nodes to terminate.
        if isinstance(self.leader.batchSystem, AbstractScalableBatchSystem):
            # Unless forced, exclude nodes with running workers. Note that it is possible for
            # the batch system to report stale nodes for which the corresponding instance was
            # terminated already. There can also be instances that the batch system doesn't have
            # nodes for yet. We'll ignore those, too, unless forced.
            # TODO: This ignores/overrides the input arg, do we really want to do that???
            nodes = self.getNodes(preemptable)
            # Filter down to nodes of the correct node type

            nodes = {node: nodes[node] for node in nodes if
                     node.nodeType == instance_type}

            filtered_nodes = self.filter_out_static_nodes(nodes, preemptable)
            filtered_nodes = filtered_nodes[:num_nodes]

            # Join nodes and instances on private IP address.
            logger.debug('Nodes considered to terminate: %s', ' '.join(map(str, nodes)))

            # Tell the batch system to stop sending jobs to these nodes
            for (node, nodeInfo) in filtered_nodes:
                self.ignoredNodes.add(node.privateIP)
                self.leader.batchSystem.ignoreNode(node.privateIP)

            if not force:
                # Filter out nodes with jobs still running. These
                # will be terminated in _removeIgnoredNodes later on
                # once all jobs have finished, but they will be ignored by
                # the batch system and cluster scaler from now on
                filtered_nodes = [(node, nodeInfo) for (node, nodeInfo) in filtered_nodes if
                                  nodeInfo and nodeInfo.workers < 1]
            nodes_to_terminate = [node for (node, nodeInfo) in filtered_nodes]
            for node in nodes_to_terminate:
                if node.privateIP in self.ignoredNodes:
                    # TODO: Why are we undoing what was just done above???
                    self.leader.batchSystem.unignoreNode(node.privateIP)
        else:
            # Without load info all we can do is sort instances by time left in billing cycle.
            nodes_to_terminate = sorted(nodes.keys(), key=lambda x: x.remainingBillingInterval())
            nodes_to_terminate = nodes_to_terminate[:num_nodes]
        number_terminated = len(nodes_to_terminate)
        logger.debug('Terminating %i instance(s).', number_terminated)
        for node in nodes_to_terminate:
            if node.privateIP in self.ignoredNodes:
                # TODO: Why are we undoing what was just done above???
                self.ignoredNodes.remove(node.privateIP)
        self.provisioner.terminateNodes(nodes_to_terminate)
        return number_terminated

    def _terminateIgnoredNodes(self) -> None:
        """
        Terminate straggling nodes designated for termination,
        but which still have workers running.
        """
        if not isinstance(self.leader.batchSystem, AbstractScalableBatchSystem):
            raise RuntimeError('Non-scalable batch system abusing a scalable-only function.')

        # start with a dictionary of all nodes and filter down
        nodes = self.getNodes()

        # remove any nodes that have already been terminated from ignored nodes
        all_node_ips = [node.privateIP for node in nodes]
        terminated_node_ips = {ip for ip in self.ignoredNodes if ip not in all_node_ips}

        # unignore terminated nodes
        for ip in terminated_node_ips:
            self.ignoredNodes.remove(ip)
            self.leader.batchSystem.unignoreNode(ip)

        logger.debug("There are %i nodes being ignored by the batch system, "
                     "checking if they can be terminated" % len(self.ignoredNodes))
        nodes = {node: info for node, info in nodes.items() if node.privateIP in self.ignoredNodes}
        nodes = {node: info for node, info in nodes.items() if info and info.workers < 1}
        nodes_to_terminate = list(nodes.keys())

        for node in nodes_to_terminate:
            self.ignoredNodes.remove(node.privateIP)
            self.leader.batchSystem.unignoreNode(node.privateIP)
        self.provisioner.terminateNodes(nodes_to_terminate)

    def filter_out_static_nodes(
            self,
            nodes: Dict["Node", NodeInfo],
            preemptable: bool = False) -> List[Tuple["Node", NodeInfo]]:
        filtered_nodes = []
        for node, nodeInfo in nodes.items():
            if node:
                non = 'non-' if not preemptable else ''
                if node.privateIP in self.getStaticNodes(preemptable):
                    # we don't want to automatically terminate any statically provisioned nodes
                    logger.debug(f'Found {node.privateIP} in {non}preemptable static nodes')
                else:
                    logger.debug(f'Did not find {node.privateIP} in {non}preemptable static nodes')
                    filtered_nodes.append((node, nodeInfo))
        # Sort nodes by number of workers and time left in billing cycle
        filtered_nodes.sort(key=lambda node_nodeInfo: (
            node_nodeInfo[1].workers if node_nodeInfo[1] else 1, node_nodeInfo[0].remainingBillingInterval()))
        return filtered_nodes

    def getNodes(self, preemptable: Optional[bool] = None) -> Dict["Node", NodeInfo]:
        """
        Returns a dictionary mapping node identifiers of preemptable or non-preemptable nodes to
        NodeInfo objects, one for each node.

        This method is the definitive source on nodes in cluster, & is responsible for consolidating
        cluster state between the provisioner & batch system.

        :param bool preemptable: If True (False) only (non-)preemptable nodes will be returned.
               If None, all nodes will be returned.
        """
        if not isinstance(self.leader.batchSystem, AbstractScalableBatchSystem):
            raise RuntimeError('Non-scalable batch system abusing a scalable-only function.')
        # nodes seen within the last 600 seconds (10 minutes)
        recent_nodes = self.leader.batchSystem.getNodes(preemptable, timeout=600)
        # all available nodes
        all_nodes = self.leader.batchSystem.getNodes(preemptable)
        # nodes that are supposedly doing something
        provisioned_nodes = self.provisioner.getProvisionedWorkers(preemptable=preemptable)

        if len(recent_nodes) != len(provisioned_nodes):
            logger.debug("Consolidating state between mesos and provisioner")

        nodeToInfo: Dict["Node", NodeInfo] = {}
        # fixme: what happens if awsFilterImpairedNodes is used?
        # if this assertion is false it means that user-managed nodes are being
        # used that are outside the provisioner's control
        # this would violate many basic assumptions in autoscaling so it currently not allowed
        for node, ip in ((node, node.privateIP) for node in provisioned_nodes):
            if ip not in recent_nodes:
                logger.debug("Worker node at %s is not reporting executor information", ip)

                # get up-to-date information about the node, if available
                info = all_nodes.get(ip)

                # info was found, but the node is not in use
                if not self.leader.batchSystem.nodeInUse(ip) and info:
                    # Node hasn't reported in the last 10 minutes & last we knew there weren't
                    # any tasks running.  We will fake executorInfo with no worker to reflect
                    # this, since otherwise this node will never be considered for termination.
                    info.workers = 0

                # info was not found about the node
                if not info:
                    # Never seen by mesos - 1 of 3 possibilities:
                    # 1) Node is still launching mesos & will come online soon.
                    # 2) No jobs have been assigned to this worker. This means the executor
                    #    was never launched, so we don't even get an executorInfo back indicating
                    #    0 workers running.
                    # 3) Mesos crashed before launching, worker will never come online.
                    #
                    # In all 3 situations it's safe to fake executor info with 0 workers,
                    # since in all cases there are no workers running.
                    info = NodeInfo(coresTotal=1, coresUsed=0, requestedCores=0,
                                    memoryTotal=1, memoryUsed=0, requestedMemory=0,
                                    workers=0)
            else:
                # mesos knows about the ip & we have up-to-date information - easy!
                info = recent_nodes[ip]
            # add info to dict to return
            nodeToInfo[node] = info
        return nodeToInfo

    def shutDown(self) -> None:
        logger.debug('Forcing provisioner to reduce cluster size to zero.')
        for nodeShape in self.nodeShapes:
            preemptable = nodeShape.preemptable
            instance_type = self.nodeShapeToType[nodeShape]
            self.setNodeCount(instance_type=instance_type, numNodes=0, preemptable=preemptable, force=True)


class ScalerThread(ExceptionalThread):
    """
    A thread that automatically scales the number of either preemptable or non-preemptable worker
    nodes according to the resource requirements of the queued jobs.

    The scaling calculation is essentially as follows: start with 0 estimated worker nodes. For
    each queued job, check if we expect it can be scheduled into a worker node before a certain time
    (currently one hour). Otherwise, attempt to add a single new node of the smallest type that
    can fit that job.

    At each scaling decision point a comparison between the current, C, and newly estimated
    number of nodes is made. If the absolute difference is less than beta * C then no change
    is made, else the size of the cluster is adapted. The beta factor is an inertia parameter
    that prevents continual fluctuations in the number of nodes.
    """
    def __init__(self, provisioner: AbstractProvisioner, leader: "Leader", config: Config) -> None:
        super().__init__(name='scaler')
        self.scaler = ClusterScaler(provisioner, leader, config)

        # Indicates that the scaling thread should shutdown
        self.stop = False

        self.stats = None
        if config.clusterStats:
            logger.debug("Starting up cluster statistics...")
            self.stats = ClusterStats(leader.config.clusterStats,
                                      leader.batchSystem,
                                      provisioner.clusterName)
            for preemptable in [True, False]:
                self.stats.startStats(preemptable=preemptable)
            logger.debug("...Cluster stats started.")

    def check(self) -> None:
        """
        Attempt to join any existing scaler threads that may have died or finished.

        This insures any exceptions raised in the threads are propagated in a timely fashion.
        """
        try:
            self.join(timeout=0)
        except Exception as e:
            logger.exception(e)
            raise

    def shutdown(self) -> None:
        """Shutdown the cluster."""
        self.stop = True
        if self.stats:
            self.stats.shutDownStats()
        self.join()

    def addCompletedJob(self, job: JobDescription, wallTime: int) -> None:
        self.scaler.addCompletedJob(job, wallTime)

    def tryRun(self) -> None:
        if self.scaler.leader.provisioner is None:
            raise RuntimeError('No provisioner found for a scaling cluster '
                               '(cannot access "getProvisionedWorkers").')
        while not self.stop:
            with throttle(self.scaler.config.scaleInterval):
                try:
                    queuedJobs = self.scaler.leader.getJobs()
                    queuedJobShapes = [
                        Shape(wallTime=self.scaler.getAverageRuntime(
                            jobName=job.jobName,
                            service=isinstance(job, ServiceJobDescription)),
                            memory=job.memory,
                            cores=job.cores,
                            disk=job.disk,
                            preemptable=job.preemptable) for job in queuedJobs]
                    currentNodeCounts = {}
                    for nodeShape in self.scaler.nodeShapes:
                        instance_type = self.scaler.nodeShapeToType[nodeShape]
                        currentNodeCounts[nodeShape] = len(
                            self.scaler.leader.provisioner.getProvisionedWorkers(
                                instance_type=instance_type,
                                preemptable=nodeShape.preemptable,
                            )
                        )
                    estimatedNodeCounts = self.scaler.getEstimatedNodeCounts(
                        queuedJobShapes, currentNodeCounts
                    )
                    self.scaler.updateClusterSize(estimatedNodeCounts)
                    if self.stats:
                        self.stats.checkStats()
                except:
                    logger.exception("Exception encountered in scaler thread. Making a best-effort "
                                     "attempt to keep going, but things may go wrong from now on.")
        self.scaler.shutDown()

class ClusterStats:
    def __init__(
        self, path: str, batchSystem: AbstractBatchSystem, clusterName: Optional[str]
    ) -> None:
        logger.debug("Initializing cluster statistics")
        self.stats: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        self.statsThreads: List[ExceptionalThread] = []
        self.statsPath = path
        self.stop = False
        self.clusterName = clusterName
        self.batchSystem = batchSystem
        self.scaleable = isinstance(self.batchSystem, AbstractScalableBatchSystem) \
            if batchSystem else False

    def shutDownStats(self) -> None:
        if self.stop:
            return

        def getFileName() -> str:
            extension = '.json'
            file = '%s-stats' % self.clusterName
            counter = 0
            while True:
                suffix = str(counter).zfill(3) + extension
                fullName = os.path.join(self.statsPath, file + suffix)
                if not os.path.exists(fullName):
                    return fullName
                counter += 1
        if self.statsPath and self.scaleable:
            self.stop = True
            for thread in self.statsThreads:
                thread.join()
            fileName = getFileName()
            with open(fileName, 'w') as f:
                json.dump(self.stats, f)

    def startStats(self, preemptable: bool) -> None:
        thread = ExceptionalThread(target=self._gatherStats, args=[preemptable])
        thread.start()
        self.statsThreads.append(thread)

    def checkStats(self) -> None:
        for thread in self.statsThreads:
            # propagate any errors raised in the threads execution
            thread.join(timeout=0)

    def _gatherStats(self, preemptable: bool) -> None:
        def toDict(nodeInfo: NodeInfo) -> Dict[str, Any]:
            # convert NodeInfo object to dict to improve JSON output
            return dict(memory=nodeInfo.memoryUsed,
                        cores=nodeInfo.coresUsed,
                        memoryTotal=nodeInfo.memoryTotal,
                        coresTotal=nodeInfo.coresTotal,
                        requestedCores=nodeInfo.requestedCores,
                        requestedMemory=nodeInfo.requestedMemory,
                        workers=nodeInfo.workers,
                        time=time.time()  # add time stamp
                        )
        if self.scaleable:
            logger.debug("Starting to gather statistics")
            stats: Dict[str, List[Dict[str, Any]]] = {}
            if not isinstance(self.batchSystem, AbstractScalableBatchSystem):
                raise RuntimeError('Non-scalable batch system abusing a scalable-only function.')
            try:
                while not self.stop:
                    nodeInfo = self.batchSystem.getNodes(preemptable)
                    for nodeIP in list(nodeInfo.keys()):
                        nodeStats = nodeInfo[nodeIP]
                        if nodeStats is not None:
                            nodeStatsDict = toDict(nodeStats)
                            try:
                                # if the node is already registered update the
                                # dictionary with the newly reported stats
                                stats[nodeIP].append(nodeStatsDict)
                            except KeyError:
                                # create a new entry for the node
                                stats[nodeIP] = [nodeStatsDict]
                    time.sleep(60)
            finally:
                threadName = 'Preemptable' if preemptable else 'Non-preemptable'
                logger.debug('%s provisioner stats thread shut down successfully.', threadName)
                self.stats[threadName] = stats
