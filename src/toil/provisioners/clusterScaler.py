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
import copy
import json
import logging
import math
import os
import time
from collections import defaultdict
from typing import (TYPE_CHECKING,
                    Any,
                    Callable,
                    Dict,
                    List,
                    Optional,
                    Set,
                    Tuple,
                    Union)

from toil.batchSystems.abstractBatchSystem import (AbstractBatchSystem,
                                                   AbstractScalableBatchSystem,
                                                   NodeInfo)
from toil.bus import ClusterDesiredSizeMessage, ClusterSizeMessage
from toil.common import Config, defaultTargetTime
from toil.job import JobDescription, ServiceJobDescription
from toil.lib.conversions import bytes2human, human2bytes
from toil.lib.retry import old_retry
from toil.lib.threading import ExceptionalThread
from toil.lib.throttle import LocalThrottle, throttle
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape

if TYPE_CHECKING:
    from toil.leader import Leader
    from toil.provisioners.node import Node

logger = logging.getLogger(__name__)

# Properties of GKE's memory overhead algorithm
EVICTION_THRESHOLD = human2bytes('100MiB')
RESERVE_SMALL_LIMIT = human2bytes('1GiB')
RESERVE_SMALL_AMOUNT = human2bytes('255MiB')
RESERVE_BREAKPOINTS: List[Union[int, float]] = [human2bytes('4GiB'), human2bytes('8GiB'), human2bytes('16GiB'), human2bytes('128GiB'), math.inf]
RESERVE_FRACTIONS = [0.25, 0.2, 0.1, 0.06, 0.02]

# Guess of how much disk space on the root volume is used for the OS and essential container images
OS_SIZE = human2bytes('5G')

# Define a type for an explanation of why a job can't fit on a node.
# Consists of a resource name and a constraining value for that resource.
FailedConstraint = Tuple[str, Union[int, float, bool]]

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
                            memory, cores, disk, and whether it is preemptible or not.
    :param targetTime: The time before which all jobs should at least be started.

    :returns: The minimum number of minimal node allocations estimated to be required to run all
              the jobs in jobShapes.
    """
    nodeReservations: Dict[Shape, List['NodeReservation']]

    def __init__(self, nodeShapes: List[Shape], targetTime: float = defaultTargetTime) -> None:
        self.nodeShapes = sorted(nodeShapes)
        self.targetTime = targetTime
        self.nodeReservations = {nodeShape: [] for nodeShape in nodeShapes}

    def binPack(self, jobShapes: List[Shape]) -> Dict[Shape, List[FailedConstraint]]:
        """
        Pack a list of jobShapes into the fewest nodes reasonable.
        
        Can be run multiple times.
        
        Returns any distinct Shapes that did not fit, mapping to reasons they did not fit.
        """
        # TODO: Check for redundancy with batchsystems.mesos.JobQueue() sorting
        logger.debug('Running bin packing for node shapes %s and %s job(s).',
                     self.nodeShapes, len(jobShapes))
        # Sort in descending order from largest to smallest. The FFD like-strategy will pack the
        # jobs in order from longest to shortest.
        jobShapes.sort()
        jobShapes.reverse()
        assert len(jobShapes) == 0 or jobShapes[0] >= jobShapes[-1]
        could_not_fit = {}
        for jS in jobShapes:
            rejection = self.addJobShape(jS)
            if rejection is not None:
                # The job bounced because it didn't fit in any bin.
                could_not_fit[rejection[0]] = rejection[1]
        return could_not_fit

    def addJobShape(self, jobShape: Shape) -> Optional[Tuple[Shape, List[FailedConstraint]]]:
        """
        Add the job to the first node reservation in which it will fit. (This
        is the bin-packing aspect).
        
        Returns the job shape again, and a list of failed constraints, if it did not fit.
        """
        chosenNodeShape = None
        for nodeShape in self.nodeShapes:
            if NodeReservation(nodeShape).fits(jobShape):
                # This node shape is the first that fits this jobShape
                chosenNodeShape = nodeShape
                break

        if chosenNodeShape is None:
            logger.debug("Couldn't fit job with requirements %s into any nodes in the nodeTypes "
                         "list.", jobShape)
            # Go back and debug why this happened.
            fewest_constraints: Optional[List[FailedConstraint]] = None
            for shape in self.nodeShapes:
                failures = NodeReservation(nodeShape).get_failed_constraints(jobShape)
                if fewest_constraints is None or len(failures) < len(fewest_constraints):
                    # This was closer to fitting.
                    # TODO: Check the actual constraint values so we don't tell
                    # the user to raise the memory on the smallest machine?
                    fewest_constraints = failures
            
            return jobShape, fewest_constraints if fewest_constraints is not None else []

        # grab current list of job objects appended to this instance type
        nodeReservations = self.nodeReservations[chosenNodeShape]
        for nodeReservation in nodeReservations:
            if nodeReservation.attemptToAddJob(jobShape, chosenNodeShape, self.targetTime):
                # We succeeded adding the job to this node reservation. Now we're done.
                return None

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
        return None

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
                self.shape.preemptible,
                self.nReservation.shape.wallTime if self.nReservation is not None else str(None),
                self.nReservation.shape.memory if self.nReservation is not None else str(None),
                self.nReservation.shape.cores if self.nReservation is not None else str(None),
                self.nReservation.shape.disk if self.nReservation is not None else str(None),
                self.nReservation.shape.preemptible if self.nReservation is not None else str(None),
                str(len(self.shapes())))

    def get_failed_constraints(self, job_shape: Shape) -> List[FailedConstraint]:
        """
        Check if a job shape's resource requirements will fit within this allocation.
        
        If the job does *not* fit, returns the failing constraints: the resources
        that can't be accomodated, and the limits that were hit.
        
        If the job *does* fit, returns an empty list.
        
        Must always agree with fits()! This codepath is slower and used for diagnosis.
        """
        
        failures: List[FailedConstraint] = []
        if job_shape.memory > self.shape.memory:
            failures.append(("memory", self.shape.memory))
        if job_shape.cores > self.shape.cores:
            failures.append(("cores", self.shape.cores))
        if job_shape.disk > self.shape.disk:
            failures.append(("disk", self.shape.disk))
        if not job_shape.preemptible and self.shape.preemptible:
            failures.append(("preemptible", self.shape.preemptible))
        return failures
    
    def fits(self, jobShape: Shape) -> bool:
        """Check if a job shape's resource requirements will fit within this allocation."""
        return jobShape.memory <= self.shape.memory and \
               jobShape.cores <= self.shape.cores and \
               jobShape.disk <= self.shape.disk and \
               (jobShape.preemptible or not self.shape.preemptible)

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
                           self.shape.preemptible)

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
            # True == can run the job (resources & preemptible only; NO time)
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
                  nodeShape.preemptible),
            NodeReservation(Shape(nodeShape.wallTime - wallTime,
                                  nodeShape.memory,
                                  nodeShape.cores,
                                  nodeShape.disk,
                                  nodeShape.preemptible)))


def binPacking(nodeShapes: List[Shape], jobShapes: List[Shape], goalTime: float) -> Tuple[Dict[Shape, int], Dict[Shape, List[FailedConstraint]]]:
    """
    Using the given node shape bins, pack the given job shapes into nodes to
    get them done in the given amount of time.
    
    Returns a dict saying how many of each node will be needed, a dict from job
    shapes that could not fit to reasons why.
    """
    bpf = BinPackedFit(nodeShapes, goalTime)
    could_not_fit = bpf.binPack(jobShapes)
    return bpf.getRequiredNodes(), could_not_fit


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
        
        # If we encounter a Shape of job that we don't think we can run, call
        # these callbacks with the Shape that didn't fit and the Shapes that
        # were available.
        self.on_too_big: List[Callable[[Shape, List[Shape]], Any]] = []

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

        # A *deficit* exists when we have more jobs that can run on preemptible
        # nodes than we have preemptible nodes. In order to not block these jobs,
        # we want to increase the number of non-preemptible nodes that we have and
        # need for just non-preemptible jobs. However, we may still
        # prefer waiting for preemptible instances to come available.
        # To accommodate this, we set the delta to the difference between the number
        # of provisioned preemptible nodes and the number of nodes that were requested.
        # Then, when provisioning non-preemptible nodes of the same type, we attempt to
        # make up the deficit.
        self.preemptibleNodeDeficit = {instance_type: 0 for instance_type in self.instance_types}

        # Keeps track of the last raw (i.e. float, not limited by
        # max/min nodes) estimates of the number of nodes needed for
        # each node shape. NB: we start with an estimate of 0, so
        # scaling up is smoothed as well.
        self.previousWeightedEstimate = {nodeShape: 0.0 for nodeShape in self.nodeShapes}

        assert len(self.nodeShapes) > 0

        # Minimum/maximum number of either preemptible or non-preemptible nodes in the cluster
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

        # Nodes might not actually provide all the resources of their nominal shapes
        self.node_shapes_after_overhead = self.nodeShapes if config.assume_zero_overhead else [self._reserve_overhead(s) for s in self.nodeShapes]
        self.without_overhead = {k: v for k, v in zip(self.node_shapes_after_overhead, self.nodeShapes)}

        #Node shape to number of currently provisioned nodes
        totalNodes: Dict[Shape, int] = defaultdict(int)
        if isinstance(leader.batchSystem, AbstractScalableBatchSystem) and leader.provisioner:
            for preemptible in (True, False):
                nodes: List["Node"] = []
                for nodeShape, instance_type in self.nodeShapeToType.items():
                    nodes_thisType = leader.provisioner.getProvisionedWorkers(instance_type=instance_type,
                                                                              preemptible=preemptible)
                    totalNodes[nodeShape] += len(nodes_thisType)
                    nodes.extend(nodes_thisType)

                self.setStaticNodes(nodes, preemptible)

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

    def _reserve_overhead(self, full_node: Shape) -> Shape:
        """
        Given the shape of an entire virtual machine, return the Shape of the
        resources we expect to actually be available to the backing scheduler
        (i.e. Mesos or Kubernetes) to run jobs on. Some disk may need to be
        used to install the OS, and some memory may be needed to run the
        kernel, Mesos, a Kubelet, Kubernetes system pods, and so on.

        Because shapes are used as dictionary keys, this function must be 1 to
        1: any two distinct inputs (that are actual node shapes and not
        pathological inputs off by a single byte) must produce distinct
        outputs.
        """

        # See
        # https://cloud.google.com/kubernetes-engine/docs/concepts/cluster-architecture#memory_cpu
        # for how EKS works. We are going to just implement their scheme and
        # hope it is a good enough overestimate of actual overhead.
        smaller = copy.copy(full_node)

        # Take away some memory to account for how much the scheduler is going to use.
        smaller.memory -= self._gke_overhead(smaller.memory)
        # And some disk to account for the OS and possibly superuser reservation
        # TODO: Figure out if the disk is an OS disk of a scratch disk
        smaller.disk -= self._disk_overhead(smaller.disk)

        logger.debug('Node shape %s can hold jobs of shape %s', full_node, smaller)

        return smaller

    def _gke_overhead(self, memory_bytes: int) -> int:
        """
        Get the number of bytes of memory that Google Kubernetes Engine would
        reserve or lose to the eviction threshold on a machine with the given
        number of bytes of memory, according to
        <https://cloud.google.com/kubernetes-engine/docs/concepts/cluster-architecture#memory_cpu>.

        We assume that this is a good estimate of how much memory will be lost
        to overhead in a Toil cluster, even though we aren't actually running a
        GKE cluster.
        """

        if memory_bytes < RESERVE_SMALL_LIMIT:
            # On machines with just a little memory, use a flat amount of it.
            return RESERVE_SMALL_AMOUNT

        # How many bytes are reserved so far?
        reserved = 0.0
        # How many bytes of memory have we accounted for so far?
        accounted: Union[float, int] = 0
        for breakpoint, fraction in zip(RESERVE_BREAKPOINTS, RESERVE_FRACTIONS):
            # Below each breakpoint, reserve the matching portion of the memory
            # since the previous breakpoint, like a progressive income tax.
            limit = min(breakpoint, memory_bytes)
            reservation = fraction * (limit - accounted)
            logger.debug('Reserve %s of memory between %s and %s', bytes2human(reservation), bytes2human(accounted), bytes2human(limit))
            reserved += reservation
            accounted = limit
            if accounted >= memory_bytes:
                break
        logger.debug('Reserved %s/%s memory for overhead', bytes2human(reserved), bytes2human(memory_bytes))

        return int(reserved) + EVICTION_THRESHOLD

    def _disk_overhead(self, disk_bytes: int) -> int:
        """
        Get the number of bytes of disk that are probably not usable on a
        machine with a disk of the given size.
        """

        # How much do we think we need for OS and possible super-user
        # reservation?
        disk_needed = OS_SIZE + int(0.05 * disk_bytes)

        if disk_bytes <= disk_needed:
            # We don't think we can actually use any of this disk
            logger.warning('All %sB of disk on a node type are likely to be needed by the OS! The node probably cannot do any useful work!', bytes2human(disk_bytes))
            return disk_bytes

        if disk_needed * 2 > disk_bytes:
            logger.warning('A node type has only %sB disk, of which more than half are expected to be used by the OS. Consider using a larger --nodeStorage', bytes2human(disk_bytes))

        return disk_needed


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

    def setStaticNodes(self, nodes: List["Node"], preemptible: bool) -> None:
        """
        Used to track statically provisioned nodes. This method must be called
        before any auto-scaled nodes are provisioned.

        These nodes are treated differently than auto-scaled nodes in that they should
        not be automatically terminated.

        :param nodes: list of Node objects
        """
        prefix = 'non-' if not preemptible else ''
        logger.debug("Adding %s to %spreemptible static nodes", nodes, prefix)
        if nodes is not None:
            self.static[preemptible] = {node.privateIP : node for node in nodes}

    def getStaticNodes(self, preemptible: bool) -> Dict[str, "Node"]:
        """
        Returns nodes set in setStaticNodes().

        :param preemptible:
        :return: Statically provisioned nodes.
        """
        return self.static[preemptible]

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
    ) -> Tuple[Dict[Shape, int], Dict[Shape, List[FailedConstraint]]]:
        """
        Given the resource requirements of queued jobs and the current size of the cluster.

        Returns a dict mapping from nodeShape to the number of nodes we want in
        the cluster right now, and a dict from job shapes that are too big to run
        on any node to reasons why.
        """

        # Do the bin packing with overhead reserved
        nodesToRunQueuedJobs, could_not_fit = binPacking(
            jobShapes=queuedJobShapes,
            nodeShapes=self.node_shapes_after_overhead,
            goalTime=self.targetTime
        )
        
        # Then translate back to get results in terms of full nodes without overhead.
        nodesToRunQueuedJobs = {self.without_overhead[k]: v for k, v in nodesToRunQueuedJobs.items()}

        estimatedNodeCounts = {}
        for nodeShape in self.nodeShapes:
            instance_type = self.nodeShapeToType[nodeShape]

            logger.debug(f"Nodes of type {instance_type} to run queued jobs: {nodesToRunQueuedJobs[nodeShape]}")
            # Actual calculation of the estimated number of nodes required
            estimatedNodeCount = 0 if nodesToRunQueuedJobs[nodeShape] == 0 \
                else max(1, self._round(nodesToRunQueuedJobs[nodeShape]))
            logger.debug("Estimating %i nodes of shape %s" % (estimatedNodeCount, nodeShape))

            # Use inertia parameter to smooth out fluctuations according to an exponentially
            # weighted moving average.
            estimatedNodeCount = self.smoothEstimate(nodeShape, estimatedNodeCount)

            # If we're scaling a non-preemptible node type, we need to see if we have a
            # deficit of preemptible nodes of this type that we should compensate for.
            if not nodeShape.preemptible:
                compensation = self.config.preemptibleCompensation
                assert 0.0 <= compensation <= 1.0
                # The number of nodes we provision as compensation for missing preemptible
                # nodes is the product of the deficit (the number of preemptible nodes we did
                # _not_ allocate) and configuration preference.
                compensationNodes = self._round(self.preemptibleNodeDeficit[instance_type] * compensation)
                if compensationNodes > 0:
                    logger.debug('Adding %d non-preemptible nodes of type %s to compensate for a '
                                'deficit of %d preemptible ones.', compensationNodes,
                                instance_type,
                                self.preemptibleNodeDeficit[instance_type])
                estimatedNodeCount += compensationNodes

            # Tell everyone how big the cluster is
            logger.debug("Currently %i nodes of type %s in cluster" % (currentNodeCounts[nodeShape],
                                                                      instance_type))
            self.leader.toilState.bus.publish(ClusterSizeMessage(instance_type, currentNodeCounts[nodeShape]))
            self.leader.toilState.bus.publish(ClusterDesiredSizeMessage(instance_type, estimatedNodeCount))

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
        return estimatedNodeCounts, could_not_fit

    def updateClusterSize(self, estimatedNodeCounts: Dict[Shape, int]) -> Dict[Shape, int]:
        """
        Given the desired and current size of the cluster, attempts to launch/remove instances to get to the desired size.

        Also attempts to remove ignored nodes that were marked for graceful removal.

        Returns the new size of the cluster.
        """
        newNodeCounts = defaultdict(int)
        for nodeShape, estimatedNodeCount in estimatedNodeCounts.items():
            instance_type = self.nodeShapeToType[nodeShape]

            newNodeCount = self.setNodeCount(instance_type, estimatedNodeCount, preemptible=nodeShape.preemptible)
            # If we were scaling up a preemptible node type and failed to meet
            # our target, we will attempt to compensate for the deficit while scaling
            # non-preemptible nodes of this type.
            if nodeShape.preemptible:
                if newNodeCount < estimatedNodeCount:
                    deficit = estimatedNodeCount - newNodeCount
                    logger.debug('Preemptible scaler detected deficit of %d nodes of type %s.' % (deficit, instance_type))
                    self.preemptibleNodeDeficit[instance_type] = deficit
                else:
                    self.preemptibleNodeDeficit[instance_type] = 0
            newNodeCounts[nodeShape] = newNodeCount

        #Attempt to terminate any nodes that we previously designated for
        #termination, but which still had workers running.
        self._terminateIgnoredNodes()
        return newNodeCounts

    def setNodeCount(
        self,
        instance_type: str,
        numNodes: int,
        preemptible: bool = False,
        force: bool = False,
    ) -> int:
        """
        Attempt to grow or shrink the number of preemptible or non-preemptible worker nodes in
        the cluster to the given value, or as close a value as possible, and, after performing
        the necessary additions or removals of worker nodes, return the resulting number of
        preemptible or non-preemptible nodes currently in the cluster.

        :param instance_type: The instance type to add or remove.

        :param numNodes: Desired size of the cluster

        :param preemptible: whether the added nodes will be preemptible, i.e. whether they
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
                nodes = self.getNodes(preemptible)
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
                                'preemptible' if preemptible else 'non-preemptible',
                                numNodes)
                    numNodes = numCurrentNodes + self._addNodes(instance_type, numNodes=delta,
                                                                preemptible=preemptible)
                elif delta < 0:
                    logger.info('Removing %i %s nodes to get to desired cluster size of %i.', -delta, 'preemptible' if preemptible else 'non-preemptible', numNodes)
                    numNodes = numCurrentNodes - self._removeNodes(nodes,
                                                                   instance_type=instance_type,
                                                                   num_nodes=-delta,
                                                                   preemptible=preemptible,
                                                                   force=force)
                elif force:
                    logger.debug('Cluster already at desired size of %i. Nothing to do.', numNodes)
                else:
                    logger.debug('Cluster (minus ignored nodes) already at desired size of %i. Nothing to do.', numNodes)
        return numNodes

    def _addNodes(self, instance_type: str, numNodes: int, preemptible: bool) -> int:
        return self.provisioner.addNodes(nodeTypes={instance_type}, numNodes=numNodes, preemptible=preemptible)

    def _removeNodes(
        self,
        nodes: Dict["Node", NodeInfo],
        instance_type: str,
        num_nodes: int,
        preemptible: bool = False,
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
            nodes = self.getNodes(preemptible)
            # Filter down to nodes of the correct node type

            nodes = {node: nodes[node] for node in nodes if
                     node.nodeType == instance_type}

            filtered_nodes = self.filter_out_static_nodes(nodes, preemptible)
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
            preemptible: bool = False) -> List[Tuple["Node", NodeInfo]]:
        filtered_nodes = []
        for node, nodeInfo in nodes.items():
            if node:
                non = 'non-' if not preemptible else ''
                if node.privateIP in self.getStaticNodes(preemptible):
                    # we don't want to automatically terminate any statically provisioned nodes
                    logger.debug(f'Found {node.privateIP} in {non}preemptible static nodes')
                else:
                    logger.debug(f'Did not find {node.privateIP} in {non}preemptible static nodes')
                    filtered_nodes.append((node, nodeInfo))
        # Sort nodes by number of workers and time left in billing cycle
        filtered_nodes.sort(key=lambda node_nodeInfo: (
            node_nodeInfo[1].workers if node_nodeInfo[1] else 1, node_nodeInfo[0].remainingBillingInterval()))
        return filtered_nodes

    def getNodes(self, preemptible: Optional[bool] = None) -> Dict["Node", NodeInfo]:
        """
        Returns a dictionary mapping node identifiers of preemptible or non-preemptible nodes to
        NodeInfo objects, one for each node.

        This method is the definitive source on nodes in cluster, & is responsible for consolidating
        cluster state between the provisioner & batch system.

        :param bool preemptible: If True (False) only (non-)preemptible nodes will be returned.
               If None, all nodes will be returned.
        """
        if not isinstance(self.leader.batchSystem, AbstractScalableBatchSystem):
            raise RuntimeError('Non-scalable batch system abusing a scalable-only function.')
        # nodes seen within the last 600 seconds (10 minutes)
        recent_nodes = self.leader.batchSystem.getNodes(preemptible, timeout=600)
        # all available nodes
        all_nodes = self.leader.batchSystem.getNodes(preemptible)
        # nodes that are supposedly doing something
        provisioned_nodes = self.provisioner.getProvisionedWorkers(preemptible=preemptible)

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
            preemptible = nodeShape.preemptible
            instance_type = self.nodeShapeToType[nodeShape]
            self.setNodeCount(instance_type=instance_type, numNodes=0, preemptible=preemptible, force=True)

class JobTooBigError(Exception):
    """
    Raised in the scaler thread when a job cannot fit in any available node
    type and is likely to lock up the workflow.
    """
    
    def __init__(self, job: Optional[JobDescription] = None, shape: Optional[Shape] = None, constraints: Optional[List[FailedConstraint]] = None):
        """
        Make a JobTooBigError.
        
        Can have a job, the job's shape, and the limiting resources and amounts. All are optional.
        """
        self.job = job
        self.shape = shape
        self.constraints = constraints if constraints is not None else []
        
        parts = [
            f"The job {self.job}" if self.job else "A job",
            f" with shape {self.shape}" if self.shape else "",
            " is too big for any available node type."
        ]
        
        if self.constraints:
            parts.append(" It could have fit if it only needed ")
            parts.append(", ".join([f"{limit} {resource}" for resource, limit in self.constraints]))
            parts.append(".") 
        
        self.msg = ''.join(parts)
        super().__init__()

    def __str__(self) -> str:
        """
        Stringify the exception, including the message.
        """
        return self.msg

class ScalerThread(ExceptionalThread):
    """
    A thread that automatically scales the number of either preemptible or non-preemptible worker
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
    def __init__(self, provisioner: AbstractProvisioner, leader: "Leader", config: Config, stop_on_exception: bool = False) -> None:
        super().__init__(name='scaler')
        self.scaler = ClusterScaler(provisioner, leader, config)
        
        # Indicates that the scaling thread should shutdown
        self.stop = False
        # Indicates that we should stop the thread if we encounter an error.
        # Mostly for testing.
        self.stop_on_exception = stop_on_exception

        self.stats = None
        if config.clusterStats:
            logger.debug("Starting up cluster statistics...")
            self.stats = ClusterStats(leader.config.clusterStats,
                                      leader.batchSystem,
                                      provisioner.clusterName)
            for preemptible in [True, False]:
                self.stats.startStats(preemptible=preemptible)
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
                            preemptible=job.preemptible) for job in queuedJobs]
                    currentNodeCounts = {}
                    for nodeShape in self.scaler.nodeShapes:
                        instance_type = self.scaler.nodeShapeToType[nodeShape]
                        currentNodeCounts[nodeShape] = len(
                            self.scaler.leader.provisioner.getProvisionedWorkers(
                                instance_type=instance_type,
                                preemptible=nodeShape.preemptible,
                            )
                        )
                    estimatedNodeCounts, could_not_fit = self.scaler.getEstimatedNodeCounts(
                        queuedJobShapes, currentNodeCounts
                    )
                    self.scaler.updateClusterSize(estimatedNodeCounts)
                    if self.stats:
                        self.stats.checkStats()
                    
                    if len(could_not_fit) != 0: 
                        # If we have any jobs left over that we couldn't fit, complain.
                        bad_job: Optional[JobDescription] = None
                        bad_shape: Optional[Shape] = None
                        for job, shape in zip(queuedJobs, queuedJobShapes):
                            # Try and find an example job with an offending shape
                            if shape in could_not_fit:
                                bad_job = job
                                bad_shape = shape
                                break
                        if bad_shape is None:
                            # If we can't find an offending job, grab an arbitrary offending shape.
                            bad_shape = next(iter(could_not_fit))
                        
                        raise JobTooBigError(job=bad_job, shape=bad_shape, constraints=could_not_fit[bad_shape])
                        
                except:
                    if self.stop_on_exception:
                        logger.critical("Stopping ScalerThread due to an error.")
                        raise
                    else:
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

    def startStats(self, preemptible: bool) -> None:
        thread = ExceptionalThread(target=self._gatherStats, args=[preemptible])
        thread.start()
        self.statsThreads.append(thread)

    def checkStats(self) -> None:
        for thread in self.statsThreads:
            # propagate any errors raised in the threads execution
            thread.join(timeout=0)

    def _gatherStats(self, preemptible: bool) -> None:
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
                    nodeInfo = self.batchSystem.getNodes(preemptible)
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
                threadName = 'Preemptible' if preemptible else 'Non-preemptible'
                logger.debug('%s provisioner stats thread shut down successfully.', threadName)
                self.stats[threadName] = stats
