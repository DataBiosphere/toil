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

import logging
import time
from threading import Thread, Event, Lock

from toil.common import Config
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.abstractProvisioner import ProvisioningException

logger = logging.getLogger(__name__)


class RunningJobShapes(object):
    """
    Used to track the 'shapes' of the last N jobs run (see Shape).
    """

    def __init__(self, config, nodeShape, N=1000):
        # As a prior we start of with 10 jobs each with the default memory, cores, and disk. To
        # estimate the running time we use the the default wall time of each node allocation,
        # so that one job will fill the time per node.
        self.jobShapes = [Shape(wallTime=nodeShape.wallTime,
                                memory=config.defaultMemory,
                                cores=config.defaultCores,
                                disk=config.defaultDisk)] * 10
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
            # Remove old jobs from the list, doing so infrequently to avoid too many list resizes
            if len(self.jobShapes) > 10 * self.N:
                self.jobShapes = self.jobShapes[-self.N:]

    def getLastNJobShapes(self):
        """
        Gets the last N job shapes added.
        """
        with self.lock:
            self.jobShapes = self.jobShapes[-self.N:]
            return self.jobShapes[:]

    @staticmethod
    def binPacking(jobShapes, nodeShape):
        """
        Use a first fit decreasing (FFD) bin packing like algorithm to calculate an approximate
        minimum number of nodes that will fit the given list of jobs.
        
        :param Shape nodeShape: The properties of an atomic node allocation, in terms of
               wall-time, memory, cores and local disk.

        :param list[Shape] jobShapes: A list of shapes, each representing a job.
        
        Let a *node reservation* be an interval of time that a node is reserved for, it is
        defined by an integer number of node-allocations.
        
        For a node reservation its *jobs* are the set of jobs that will be run within the 
        node reservation.
        
        A minimal node reservation has time equal to one atomic node allocation, or the minimum
        number node allocations to run the longest running job in its jobs.
        
        :rtype: int
        :returns: The minimum number of minimal node allocations estimated to be required to run
                  all the jobs in jobShapes.
        """
        # Sort in ascending order. The FFD like-strategy will schedule the jobs in order from
        # longest to shortest.
        jobShapes.sort()

        # Represents a node reservation. To represent the resources available in a reservation a
        # node reservation is represented as a sequence of Shapes, each giving the resources free
        # within the given interval of time
        class NodeReservation(object):
            def __init__(self, shape):
                self.shape = shape  # The wall-time and resource available
                self.nReservation = None  # The next portion of the reservation

        nodeReservations = []  # The list of node reservations

        for jS in jobShapes:
            def addToReservation():
                # Function adds the job, jS, to the first node reservation in which it will fit
                # (this is the bin-packing aspect)

                # Used to check if a job shape's resource requirements will fit within a given
                # node allocation
                def fits(x, y):
                    return y.memory <= x.memory and y.cores <= x.cores and y.disk <= x.disk

                # Used to adjust available the resources of a node allocation as a job is
                # scheduled within it.
                def subtract(x, y):
                    return Shape(x.wallTime,
                                 x.memory - y.memory,
                                 x.cores - y.cores,
                                 x.disk - y.disk)

                # Used to partition a node allocation into two
                def split(x, y, t):
                    return (
                        Shape(t, x.memory - y.memory, x.cores - y.cores, x.disk - y.disk),
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
                            # Reached the end of the reservation without success so stop trying
                            # to add to reservation i
                            break
                    i += 1

            addToReservation()

        logger.debug("Ran bin packing algorithm, for node shape: "
                     "%s needed %s nodes for %s jobs" % (nodeShape,
                                                         len(nodeReservations), len(jobShapes)))

        return len(nodeReservations)


class ClusterScaler(object):
    def __init__(self, provisioner, jobBatcher, config):
        """
        Class manages automatically scaling the number of worker nodes. 

        :param AbstractProvisioner provisioner: The provisioner instance to scale.

        :param JobBatcher jobBatcher: The class issuing jobs to the batch system.

        :param Config config: Config object from which to draw parameters.
        """
        self.stop = Event()  # Event used to indicate that the scaling processes should shutdown
        self.error = Event()  # Event used by scaling processes to indicate failure

        if config.maxPreemptableNodes + config.maxNodes == 0:
            raise RuntimeError("Trying to create a cluster that can have no workers!")

        # Create scaling process for preemptable nodes
        if config.maxPreemptableNodes > 0:
            nodeShape = provisioner.getNodeShape(preemptable=True)
            self.preemptableRunningJobShape = RunningJobShapes(config, nodeShape)
            args = (provisioner, jobBatcher,
                    config.minPreemptableNodes, config.maxPreemptableNodes,
                    self.preemptableRunningJobShape, config, nodeShape,
                    self.stop, self.error, True)
            self.preemptableScaler = Thread(target=self.scaler, args=args)
            self.preemptableScaler.start()
        else:
            self.preemptableScaler = None

        # Create scaling process for non-preemptable nodes
        if config.maxNodes > 0:
            nodeShape = provisioner.getNodeShape(preemptable=False)
            self.runningJobShape = RunningJobShapes(config, nodeShape)
            args = (provisioner, jobBatcher,
                    config.minNodes, config.maxNodes,
                    self.runningJobShape, config, config.nodeType,
                    self.stop, self.error, False)
            self.scaler = Thread(target=self.scaler, args=args)
            self.scaler.start()
        else:
            self.scaler = None

    def shutdown(self):
        """
        Shutdown the cluster.
        """
        self.stop.set()
        if self.preemptableScaler is not None:
            self.preemptableScaler.join()
        if self.scaler is not None:
            self.scaler.join()

    def addCompletedJob(self, issuedJob, wallTime):
        """
        Adds the shape of a completed job to the queue, allowing the scalar to use the last N
        completed jobs in factoring how many nodes are required in the cluster.
        
        :param IssuedJob issuedJob: The memory, core and disk requirements of the completed job

        :param int wallTime: The wall-time taken to complete the job in seconds.
        """
        s = Shape(wallTime=wallTime, memory=issuedJob.memory,
                  cores=issuedJob.cores, disk=issuedJob.disk)
        if issuedJob.preemptable:
            self.preemptableRunningJobShape.add(s)
        else:
            self.runningJobShape.add(s)

    @staticmethod
    def scaler(provisioner, jobBatcher,
               minNodes, maxNodes, runningJobShapes,
               config, nodeShape,
               stop, error, preemptable):
        """
        Automatically scales the number of worker nodes according to the number of jobs queued
        and the resource requirements of the last N completed jobs.
        
        The scaling calculation is essentially as follows: Use the RunningJobShapes instance to
        calculate how many nodes, n, can be used to productively compute the last N completed
        jobs. Let M be the number of jobs issued to the batch system. The number of nodes
        required is then estimated to be alpha * n * M/N, where alpha is a scaling factor used to
        adjust the balance between under- and over- provisioning the cluster.
        
        At each scaling decision point a comparison between the current, C, and newly estimated
        number of nodes is made. If the absolute difference is less than beta * C then no change
        is made, else the size of the cluster is adapted. The beta factor is an inertia parameter
        that prevents continual fluctuations in the number of nodes.

        :param AbstractProvisioner provisioner: Provisioner instance to scale.

        :param JobBatcher jobBatcher: Class used to schedule jobs. This is monitored to
               make scaling decisions.

        :param int minNodes: the minimum nodes in the cluster

        :param int maxNodes: the maximum nodes in the cluster

        :param RunningJobShapes runningJobShapes: the class used to monitor the requirements of
               the last N completed jobs.

        :param Config config: Config object from which to draw parameters.

        :param Shape nodeShape: The resource requirements and wall-time of an atomic node
               allocation.

        :param Event stop: The event instance used to signify that the function should shut down
               the cluster and terminate.

        :param Event error: Event used to signify that the function has
               terminated unexpectedly.

        :param bool preemptable: If True create/cleanup preemptable nodes, else create/cleanup
               non-preemptable nodes.
        """
        try:
            totalNodes = minNodes
            # Setup a minimal cluster
            if minNodes > 0:
                try:
                    provisioner.addNodes(numNodes=minNodes, preemptable=preemptable)
                except ProvisioningException as e:
                    logger.debug("We tried to create a minimal cluster of %s nodes,"
                                 " but got a provisioning exception: %s" % (minNodes, e))
                    raise

            # The scaling loop iterated until we get the stop signal
            while True:

                # Check if we've got the cleanup signal       
                if stop.is_set():  # Cleanup logic
                    try:
                        provisioner.removeNodes(totalNodes, preemptable=preemptable)
                    except ProvisioningException as e:
                        logger.debug("We tried to stop the worker nodes (%s total) but got a "
                                     "provisioning exception: %s" % (totalNodes, e))
                        raise
                    logger.debug("Scalar (preemptable=%s) exiting normally" % preemptable)
                    break

                # Calculate the approx. number nodes needed
                # TODO: Correct for jobs already running which can be considered fractions of a job
                queueSize = jobBatcher.getNumberOfJobsIssued()
                recentJobShapes = runningJobShapes.getLastNJobShapes()
                assert len(recentJobShapes) > 0
                nodesToRunRecentJobs = runningJobShapes.binPacking(recentJobShapes, nodeShape)
                estimatedNodesRequired = 0 if queueSize == 0 else max(round(
                    config.alphaPacking * nodesToRunRecentJobs * float(queueSize) / len(
                        recentJobShapes)), 1)
                nodesDelta = estimatedNodesRequired - totalNodes

                fix_my_name = len(recentJobShapes) / float(
                    nodesToRunRecentJobs) if nodesToRunRecentJobs > 0 else 0
                logger.debug("Estimating cluster needs %s, node shape: %s, current worker nodes: "
                             "%s, queue size: %s, jobs/node estimated required: %s, "
                             "alpha: %s", estimatedNodesRequired, nodeShape, totalNodes, queueSize,
                             fix_my_name, config.alphaPacking)

                # Use inertia parameter to stop small fluctuations
                if estimatedNodesRequired <= totalNodes * config.betaInertia <= estimatedNodesRequired:
                    logger.debug("Difference in new (%s) and previous estimates of number of "
                                 "nodes (%s) required is within beta (%s), making no change",
                                 estimatedNodesRequired, totalNodes, config.betaInertia)
                    nodesDelta = 0

                # Bound number using the max and min node parameters
                if nodesDelta + totalNodes > maxNodes:
                    logger.debug("The number of nodes estimated we need (%s) is larger than the "
                                 "max allowed (%s).", nodesDelta + totalNodes, maxNodes)
                    assert nodesDelta > 0
                    nodesDelta -= totalNodes + nodesDelta - maxNodes
                    assert nodesDelta >= 0
                elif nodesDelta + totalNodes < minNodes:
                    logger.debug("The number of nodes estimated we need (%s) is smaller than the "
                                 "min allowed (%s).", nodesDelta + totalNodes, minNodes)
                    assert nodesDelta < 0
                    nodesDelta += minNodes - totalNodes - nodesDelta
                    assert nodesDelta <= 0

                if nodesDelta != 0:
                    # Adjust the number of nodes in the cluster
                    try:
                        if nodesDelta > 0:
                            provisioner.addNodes(numNodes=int(nodesDelta),
                                                 preemptable=preemptable)
                        else:
                            provisioner.removeNodes(numNodes=abs(int(nodesDelta)),
                                                    preemptable=preemptable)
                        totalNodes += nodesDelta
                        logger.debug("%s %s worker nodes %s the (preemptable: %s) cluster",
                                     ("Added" if nodesDelta > 0 else "Removed"), abs(nodesDelta),
                                     preemptable, ("from" if nodesDelta > 0 else "to"))
                    except ProvisioningException as e:
                        logger.debug("We tried to %s %s worker nodes but got a provisioning "
                                     "exception: %s", "add" if nodesDelta > 0 else "remove",
                                     abs(nodesDelta), e)
                # FIXME: addNodes() and removeNodes() block, so only wait the difference
                # Sleep to avoid thrashing
                time.sleep(config.scaleInterval)
        except:
            error.set()  # Set the error event
            raise
