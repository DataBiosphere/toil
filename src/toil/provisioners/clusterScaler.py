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

import json
import logging
import os
from collections import deque
from threading import Lock

import time
from bd2k.util.exceptions import require
from bd2k.util.retry import retry
from bd2k.util.threading import ExceptionalThread
from bd2k.util.throttle import throttle
from itertools import islice

from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem, NodeInfo
from toil.common import Config
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape

logger = logging.getLogger(__name__)

# A *deficit* exists when we have more jobs that can run on preemptable nodes than we have
# preemptable nodes. In order to not block these jobs, we want to increase the number of non-
# preemptable nodes that we have and need for just non-preemptable jobs. However, we may still
# prefer waiting for preemptable instances to come available.
#
# To accommodate this, we set the delta to the difference between the number of provisioned
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
    # Sort in descending order from largest to smallest. The FFD like-strategy will pack the jobs in order from longest
    # to shortest.
    jobShapes.sort()
    jobShapes.reverse()
    assert len(jobShapes) == 0 or jobShapes[0] >= jobShapes[-1]

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
                Adjust available resources of a node allocation as a job is scheduled within it.
                """
                return Shape(x.wallTime, x.memory - y.memory, x.cores - y.cores, x.disk - y.disk)

            def split(x, y, t):
                """
                Partition a node allocation into two
                """
                return (Shape(t, x.memory - y.memory, x.cores - y.cores, x.disk - y.disk),
                        NodeReservation(Shape(x.wallTime - t, x.memory, x.cores, x.disk)))

            i = 0 # Index of node reservation
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
                        
                        # If the jS fits in the node allocation from x to y
                        if t >= jS.wallTime:
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
                        
                        # If the job would fit, but is longer than the total node allocation
                        # extend the node allocation
                        elif y.nReservation == None and x == nodeReservations[i]:
                            # Extend the node reservation to accommodate jS
                            y.nReservation = NodeReservation(nodeShape)
                        
                    else: # Does not fit, reset
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
    def __init__(self, provisioner, leader, config):
        """
        Class manages automatically scaling the number of worker nodes.
        :param AbstractProvisioner provisioner: Provisioner instance to scale.
        :param toil.leader.Leader leader: 
        :param Config config: Config object from which to draw parameters.
        """
        self.provisioner = provisioner
        self.leader = leader
        self.config = config
        # Indicates that the scaling threads should shutdown
        self.stop = False

        assert config.maxPreemptableNodes >= 0 and config.maxNodes >= 0
        require(config.maxPreemptableNodes + config.maxNodes > 0,
                'Either --maxNodes or --maxPreemptableNodes must be non-zero.')
        
        self.preemptableScaler = ScalerThread(self, preemptable=True) if self.config.maxPreemptableNodes > 0 else None

        self.scaler = ScalerThread(self, preemptable=False) if self.config.maxNodes > 0 else None

    def start(self):
        """ 
        Start the cluster scaler thread(s).
        """
        if self.preemptableScaler != None:
            self.preemptableScaler.start()

        if self.scaler != None:
            self.scaler.start()

    def check(self):
        """
        Attempt to join any existing scaler threads that may have died or finished. This insures
        any exceptions raised in the threads are propagated in a timely fashion.
        """
        exception = False
        for scalerThread in [self.preemptableScaler, self.scaler]:
            if scalerThread is not None:
                try:
                    scalerThread.join(timeout=0)
                except Exception as e:
                    logger.exception(e)
                    exception = True
        if exception:
            raise RuntimeError('The cluster scaler has exited due to an exception')

    def shutdown(self):
        """
        Shutdown the cluster.
        """
        self.stop = True
        for scaler in self.preemptableScaler, self.scaler:
            if scaler is not None:
                scaler.join()

    def addCompletedJob(self, job, wallTime):
        """
        Adds the shape of a completed job to the queue, allowing the scalar to use the last N
        completed jobs in factoring how many nodes are required in the cluster.
        :param toil.job.JobNode job: The memory, core and disk requirements of the completed job
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
        self.nodeTypeString = ("preemptable" if self.preemptable else "non-preemptable") + " nodes" # Used for logging
        # Resource requirements and wall-time of an atomic node allocation
        self.nodeShape = scaler.provisioner.getNodeShape(preemptable=preemptable)
        # Monitors the requirements of the N most recently completed jobs
        self.jobShapes = RecentJobShapes(scaler.config, self.nodeShape)
        # Minimum/maximum number of either preemptable or non-preemptable nodes in the cluster
        self.minNodes = scaler.config.minPreemptableNodes if preemptable else scaler.config.minNodes
        self.maxNodes = scaler.config.maxPreemptableNodes if preemptable else scaler.config.maxNodes
        if isinstance(self.scaler.leader.batchSystem, AbstractScalableBatchSystem):
            for preemptable in (True, False):
                # although this thread only deals with either preemptable or non-preemptable nodes,
                # the presence of any statically provisioned nodes effects both scaler threads so we
                # will check for both preemptable and non preemptable static nodes
                nodes = self.scaler.leader.provisioner.getProvisionedWorkers(preemptable)
                self.scaler.provisioner.setStaticNodes(nodes, preemptable)
                if preemptable == self.preemptable:
                    self.totalNodes = len(nodes) if nodes else 0
        else:
            self.totalNodes = 0
        logger.info('Starting with %s %s(s) in the cluster.', self.totalNodes, self.nodeTypeString)

        self.stats = None
        if scaler.config.clusterStats:
            logger.debug("Starting up cluster statistics...")
            self.stats = ClusterStats(self.scaler.leader.config.clusterStats,
                                      self.scaler.leader.batchSystem,
                                      self.scaler.provisioner.clusterName)
            self.stats.startStats(preemptable=preemptable)
            logger.debug("...Cluster stats started.")

    def tryRun(self):
        global _preemptableNodeDeficit

        while not self.scaler.stop:
            with throttle(self.scaler.config.scaleInterval):
                self.totalNodes = len(self.scaler.leader.provisioner.getProvisionedWorkers(self.preemptable))
                # Estimate the number of nodes to run the issued jobs.
                # Number of jobs issued
                queueSize = self.scaler.leader.getNumberOfJobsIssued(preemptable=self.preemptable)
                
                # Job shapes of completed jobs
                recentJobShapes = self.jobShapes.get()
                assert len(recentJobShapes) > 0
                
                # Estimate of number of nodes needed to run recent jobs
                nodesToRunRecentJobs = binPacking(recentJobShapes, self.nodeShape)
                
                # Actual calculation of the estimated number of nodes required
                estimatedNodes = 0 if queueSize == 0 else max(1, int(round(
                    self.scaler.config.alphaPacking
                    * nodesToRunRecentJobs
                    * float(queueSize) / len(recentJobShapes))))
                
                # Account for case where the average historical runtime of completed jobs is less
                # than the runtime of currently running jobs. This is important
                # to avoid a deadlock where the estimated number of nodes to run the jobs
                # is too small to schedule a set service jobs and their dependent jobs, leading
                # to service jobs running indefinitely.
                
                # How many jobs are currently running and their average runtime.
                numberOfRunningJobs, currentAvgRuntime  = self.scaler.leader.getNumberAndAvgRuntimeOfCurrentlyRunningJobs()
                
                # Average runtime of recently completed jobs
                historicalAvgRuntime = sum(map(lambda jS : jS.wallTime, recentJobShapes))/len(recentJobShapes)

                # Ratio of avg. runtime of currently running and completed jobs
                runtimeCorrection = float(currentAvgRuntime)/historicalAvgRuntime if currentAvgRuntime > historicalAvgRuntime and numberOfRunningJobs >= estimatedNodes else 1.0
                
                # Make correction, if necessary (only do so if cluster is busy and average runtime is higher than historical
                # average)
                if runtimeCorrection != 1.0:
                    estimatedNodes = int(round(estimatedNodes * runtimeCorrection))
                    if self.totalNodes < self.maxNodes:
                        logger.warn("Historical avg. runtime (%s) is less than current avg. runtime (%s) and cluster"
                                    " is being well utilised (%s running jobs), increasing cluster requirement by: %s" % 
                                    (historicalAvgRuntime, currentAvgRuntime, numberOfRunningJobs, runtimeCorrection))

                # If we're the non-preemptable scaler, we need to see if we have a deficit of
                # preemptable nodes that we should compensate for.
                if not self.preemptable:
                    compensation = self.scaler.config.preemptableCompensation
                    assert 0.0 <= compensation <= 1.0
                    # The number of nodes we provision as compensation for missing preemptable
                    # nodes is the product of the deficit (the number of preemptable nodes we did
                    # _not_ allocate) and configuration preference.
                    compensationNodes = int(round(_preemptableNodeDeficit * compensation))
                    if compensationNodes > 0:
                        logger.info('Adding %d preemptable nodes to compensate for a deficit of %d '
                                    'non-preemptable ones.', compensationNodes, _preemptableNodeDeficit)
                    estimatedNodes += compensationNodes

                jobsPerNode = (0 if nodesToRunRecentJobs <= 0
                               else len(recentJobShapes) / float(nodesToRunRecentJobs))
                if estimatedNodes > 0 and self.totalNodes < self.maxNodes:
                    logger.info('Estimating that cluster needs %s %s of shape %s, from current '
                                'size of %s, given a queue size of %s, the number of jobs per node '
                                'estimated to be %s, an alpha parameter of %s and a run-time length correction of %s.',
                                estimatedNodes, self.nodeTypeString, self.nodeShape,
                                self.totalNodes, queueSize, jobsPerNode,
                                self.scaler.config.alphaPacking, runtimeCorrection)

                # Use inertia parameter to stop small fluctuations
                delta = self.totalNodes * max(0.0, self.scaler.config.betaInertia - 1.0)
                if self.totalNodes - delta <= estimatedNodes <= self.totalNodes + delta:
                    logger.debug('Difference in new (%s) and previous estimates in number of '
                                 '%s (%s) required is within beta (%s), making no change.',
                                 estimatedNodes, self.nodeTypeString, self.totalNodes, self.scaler.config.betaInertia)
                    estimatedNodes = self.totalNodes

                # Bound number using the max and min node parameters
                if estimatedNodes > self.maxNodes:
                    logger.debug('Limiting the estimated number of necessary %s (%s) to the '
                                 'configured maximum (%s).', self.nodeTypeString, estimatedNodes, self.maxNodes)
                    estimatedNodes = self.maxNodes
                elif estimatedNodes < self.minNodes:
                    logger.info('Raising the estimated number of necessary %s (%s) to the '
                                'configured mininimum (%s).', self.nodeTypeString, estimatedNodes, self.minNodes)
                    estimatedNodes = self.minNodes

                if estimatedNodes != self.totalNodes:
                    logger.info('Changing the number of %s from %s to %s.', self.nodeTypeString, self.totalNodes,
                                estimatedNodes)
                    self.totalNodes = self.setNodeCount(numNodes=estimatedNodes, preemptable=self.preemptable)
                    
                    # If we were scaling up the number of preemptable nodes and failed to meet
                    # our target, we need to update the slack so that non-preemptable nodes will
                    # be allocated instead and we won't block. If we _did_ meet our target,
                    # we need to reset the slack to 0.
                    if self.preemptable:
                        if self.totalNodes < estimatedNodes:
                            deficit = estimatedNodes - self.totalNodes
                            logger.info('Preemptable scaler detected deficit of %d nodes.', deficit)
                            _preemptableNodeDeficit = deficit
                        else:
                            _preemptableNodeDeficit = 0

                if self.stats:
                    self.stats.checkStats()
                    
        self.shutDown(preemptable=self.preemptable)
        logger.info('Scaler exited normally.')

    def setNodeCount(self, numNodes, preemptable=False, force=False):
        """
        Attempt to grow or shrink the number of prepemptable or non-preemptable worker nodes in
        the cluster to the given value, or as close a value as possible, and, after performing
        the necessary additions or removals of worker nodes, return the resulting number of
        preemptable or non-preemptable nodes currently in the cluster.

        :param int numNodes: Desired size of the cluster

        :param bool preemptable: whether the added nodes will be preemptable, i.e. whether they
               may be removed spontaneously by the underlying platform at any time.

        :param bool force: If False, the provisioner is allowed to deviate from the given number
               of nodes. For example, when downsizing a cluster, a provisioner might leave nodes
               running if they have active jobs running on them.

        :rtype: int :return: the number of worker nodes in the cluster after making the necessary
                adjustments. This value should be, but is not guaranteed to be, close or equal to
                the `numNodes` argument. It represents the closest possible approximation of the
                actual cluster size at the time this method returns.
        """
        for attempt in retry(predicate=self.scaler.provisioner.retryPredicate):
            with attempt:
                workerInstances = self.getNodes(preemptable=preemptable)
                numCurrentNodes = len(workerInstances)
                delta = numNodes - numCurrentNodes
                if delta > 0:
                    logger.info('Adding %i %s nodes to get to desired cluster size of %i.', delta, 'preemptable' if preemptable else 'non-preemptable', numNodes)
                    numNodes = numCurrentNodes + self._addNodes(numNodes=delta,
                                                                preemptable=preemptable)
                elif delta < 0:
                    logger.info('Removing %i %s nodes to get to desired cluster size of %i.', -delta, 'preemptable' if preemptable else 'non-preemptable', numNodes)
                    numNodes = numCurrentNodes - self._removeNodes(workerInstances,
                                                                   numNodes=-delta,
                                                                   preemptable=preemptable,
                                                                   force=force)
                else:
                    logger.info('Cluster already at desired size of %i. Nothing to do.', numNodes)
        return numNodes

    def _addNodes(self, numNodes, preemptable):
        return self.scaler.provisioner.addNodes(numNodes, preemptable)

    def _removeNodes(self, nodeToNodeInfo, numNodes, preemptable=False, force=False):
        # If the batch system is scalable, we can use the number of currently running workers on
        # each node as the primary criterion to select which nodes to terminate.
        if isinstance(self.scaler.leader.batchSystem, AbstractScalableBatchSystem):
            # iMap = ip : instance
            ipMap = {node.privateIP: node for node in nodeToNodeInfo.keys()}
            def filterRemovableNodes(executorInfo):
                return not bool(self.chooseNodes({ipMap.get(executorInfo.nodeAddress): executorInfo.nodeInfo},
                                                 preemptable=preemptable))
            with self.scaler.leader.batchSystem.nodeFiltering(filterRemovableNodes):
                # while this context manager is active, the batch system will not launch any
                # news tasks on nodes that are being considered for termination (as determined by the
                # filterRemovableNodes method)
                nodeToNodeInfo = self.getNodes(preemptable)
                # Join nodes and instances on private IP address.
                logger.debug('Nodes considered to terminate: %s', ' '.join(map(str, nodeToNodeInfo)))
                nodesToTerminate = self.chooseNodes(nodeToNodeInfo, force, preemptable=preemptable)
                nodesToTerminate = nodesToTerminate[:numNodes]
                if logger.isEnabledFor(logging.DEBUG):
                    for instance in nodesToTerminate:
                        logger.debug("Instance %s is about to be terminated. It "
                                     "would be billed again in %s minutes.",
                                     instance, 60 * self.scaler.provisioner.remainingBillingInterval(instance))
                nodeToNodeInfo = nodesToTerminate
        else:
            # Without load info all we can do is sort instances by time left in billing cycle.
            nodeToNodeInfo = sorted(nodeToNodeInfo, key=self.scaler.provisioner.remainingBillingInterval)
            nodeToNodeInfo = [instance for instance in islice(nodeToNodeInfo, numNodes)]
        logger.info('Terminating %i instance(s).', len(nodeToNodeInfo))
        if nodeToNodeInfo:
            self.scaler.provisioner.terminateNodes(nodeToNodeInfo)
        return len(nodeToNodeInfo)

    def chooseNodes(self, nodeToNodeInfo, force=False, preemptable=False):
        # Unless forced, exclude nodes with runnning workers. Note that it is possible for
        # the batch system to report stale nodes for which the corresponding instance was
        # terminated already. There can also be instances that the batch system doesn't have
        # nodes for yet. We'll ignore those, too, unless forced.
        nodesToTerminate = []
        for node, nodeInfo in nodeToNodeInfo.items():
            if node is None:
                logger.info("Node with info %s was not found in our node list", nodeInfo)
                continue
            staticNodes = self.scaler.provisioner.getStaticNodes(preemptable)
            prefix = 'non-' if not preemptable else ''
            if node.privateIP in staticNodes:
                # we don't want to automatically terminate any statically
                # provisioned nodes
                logger.debug("Found %s in %spreemptable static nodes", node.privateIP, prefix)
                continue
            else:
                logger.debug("Did not find %s in %spreemptable static nodes", node.privateIP, prefix)
                pass
            if force:
                nodesToTerminate.append((node, nodeInfo))
            elif nodeInfo is not None and nodeInfo.workers < 1:
                nodesToTerminate.append((node, nodeInfo))
            else:
                logger.debug('Not terminating instances %s. Node info: %s', node, nodeInfo)
        # Sort nodes by number of workers and time left in billing cycle
        nodesToTerminate.sort(key=lambda ((node, nodeInfo)): (
            nodeInfo.workers if nodeInfo else 1,
            self.scaler.provisioner.remainingBillingInterval(node))
                              )
        if not force:
            # don't terminate nodes that still have > 15% left in their allocated (prepaid) time
            nodesToTerminate = [node for node in nodesToTerminate if
                                self.scaler.provisioner.remainingBillingInterval(node) <= 0.15]
        return [node for node,_ in nodesToTerminate]

    def getNodes(self, preemptable):
        """
        Returns a dictionary mapping node identifiers of preemptable or non-preemptable nodes to
        NodeInfo objects, one for each node.

        This method is the definitive source on nodes in cluster, & is responsible for consolidating
        cluster state between the provisioner & batch system.

        :param bool preemptable: If True (False) only (non-)preemptable nodes will be returned.
               If None, all nodes will be returned.

        :rtype: dict[Node, NodeInfo]
        """
        def _getInfo(allMesosNodes, ip):
            info = None
            try:
                info = allMesosNodes[ip]
            except KeyError:
                # never seen by mesos - 1 of 3 possibilities:
                # 1) node is still launching mesos & will come online soon
                # 2) no jobs have been assigned to this worker. This means the executor was never
                #    launched, so we don't even get an executorInfo back indicating 0 workers running
                # 3) mesos crashed before launching, worker will never come online
                # In all 3 situations it's safe to fake executor info with 0 workers, since in all
                # cases there are no workers running. We also won't waste any money in cases 1/2 since
                # we will still wait for the end of the node's billing cycle for the actual
                # termination.
                info = NodeInfo(coresTotal=1, coresUsed=0, requestedCores=0,
                                memoryTotal=1, memoryUsed=0, requestedMemory=0,
                                workers=0)
            else:
                # Node was tracked but we haven't seen this in the last 10 minutes
                inUse = self.scaler.leader.batchSystem.nodeInUse(ip)
                if not inUse:
                    # The node hasn't reported in the last 10 minutes & last we know
                    # there weren't any tasks running. We will fake executorInfo with no
                    # worker to reflect this, since otherwise this node will never
                    # be considered for termination
                    info.workers = 0
                else:
                    pass
                    # despite the node not reporting to mesos jobs may still be running
                    # so we can't terminate the node
            return info

        allMesosNodes = self.scaler.leader.batchSystem.getNodes(preemptable, timeout=None)
        recentMesosNodes = self.scaler.leader.batchSystem.getNodes(preemptable)
        provisionerNodes = self.scaler.provisioner.getProvisionedWorkers(preemptable)

        if len(recentMesosNodes) != len(provisionerNodes):
            logger.debug("Consolidating state between mesos and provisioner")
        nodeToInfo = {}
        # fixme: what happens if awsFilterImpairedNodes is used?
        # if this assertion is false it means that user-managed nodes are being
        # used that are outside the provisioners control
        # this would violate many basic assumptions in autoscaling so it currently not allowed
        for node, ip in ((node, node.privateIP) for node in provisionerNodes):
            info = None
            if ip not in recentMesosNodes:
                logger.debug("Worker node at %s is not reporting executor information")
                # we don't have up to date information about the node
                info = _getInfo(allMesosNodes, ip)
            else:
                # mesos knows about the ip & we have up to date information - easy!
                info = recentMesosNodes[ip]
            # add info to dict to return
            nodeToInfo[node] = info
        return nodeToInfo

    def shutDown(self, preemptable):
        if self.stats:
            self.stats.shutDownStats()
        logger.debug('Forcing provisioner to reduce cluster size to zero.')
        totalNodes = self.setNodeCount(numNodes=0, preemptable=preemptable, force=True)
        if totalNodes > len(self.scaler.provisioner.getStaticNodes(preemptable)):  # ignore static nodes
            raise RuntimeError('Provisioner could not terminate all autoscaled nodes. There are '
                               '%s nodes left in the cluster, %s of which were statically provisioned' % (totalNodes, len(self.getStaticNodes(preemptable)))
                               )
        elif totalNodes < len(self.scaler.provisioner.getStaticNodes(preemptable)):  # ignore static nodes
            raise RuntimeError('Provisioner incorrectly terminated statically provisioned nodes.')


class ClusterStats(object):

    def __init__(self, path, batchSystem, clusterName):
        logger.debug("Initializing cluster statistics")
        self.stats = {}
        self.statsThreads = []
        self.statsPath = path
        self.stop = False
        self.clusterName = clusterName
        self.batchSystem = batchSystem
        self.scaleable = isinstance(self.batchSystem, AbstractScalableBatchSystem) if batchSystem else False

    def shutDownStats(self):
        if self.stop:
            return
        def getFileName():
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

    def startStats(self, preemptable):
        thread = ExceptionalThread(target=self._gatherStats, args=[preemptable])
        thread.start()
        self.statsThreads.append(thread)

    def checkStats(self):
        for thread in self.statsThreads:
            # propagate any errors raised in the threads execution
            thread.join(timeout=0)

    def _gatherStats(self, preemptable):
        def toDict(nodeInfo):
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
            logger.debug("Staring to gather statistics")
            stats = {}
            try:
                while not self.stop:
                    nodeInfo = self.batchSystem.getNodes(preemptable)
                    for nodeIP in nodeInfo.keys():
                        nodeStats = nodeInfo[nodeIP]
                        if nodeStats is not None:
                            nodeStats = toDict(nodeStats)
                            try:
                                # if the node is already registered update the dictionary with
                                # the newly reported stats
                                stats[nodeIP].append(nodeStats)
                            except KeyError:
                                # create a new entry for the node
                                stats[nodeIP] = [nodeStats]
                    time.sleep(60)
            finally:
                threadName = 'Preemptable' if preemptable else 'Non-preemptable'
                logger.debug('%s provisioner stats thread shut down successfully.', threadName)
                self.stats[threadName] = stats
        else:
            pass
