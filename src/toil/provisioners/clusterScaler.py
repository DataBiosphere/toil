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
            self.totalNodes = len(self.scaler.leader.batchSystem.getNodes(self.preemptable))
        else:
            self.totalNodes = 0
        logger.info('Starting with %s %s(s) in the cluster.', self.totalNodes, self.nodeTypeString)
        
        if scaler.config.clusterStats:
            self.scaler.provisioner.startStats(preemptable=preemptable)

    def tryRun(self):
        global _preemptableNodeDeficit

        while not self.scaler.stop:
            with throttle(self.scaler.config.scaleInterval):
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
                historicalAvgRuntime = sum(map(lambda jS : jS.wallTime, recentJobShapes))
            
                # Ratio of avg. runtime of currently running and completed jobs
                runtimeCorrection = float(currentAvgRuntime)/historicalAvgRuntime if currentAvgRuntime > historicalAvgRuntime and numberOfRunningJobs >= estimatedNodes else 1.0
                
                # Make correction, if necessary (only do so if cluster is busy and average runtime is higher than historical
                # average)
                if runtimeCorrection != 1.0:
                    logger.warn("Historical avg. runtime (%s) is less than current avg. runtime (%s) and cluster"
                                " is being well utilised (%s running jobs), increasing cluster requirement by: %s" % 
                                (historicalAvgRuntime, currentAvgRuntime, numberOfRunningJobs, runtimeCorrection))
                    estimatedNodes *= runtimeCorrection

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
                logger.info('Estimating that cluster needs %s %s of shape %s, from current '
                             'size of %s, given a queue size of %s, the number of jobs per node '
                             'estimated to be %s, an alpha parameter of %s and a run-time length correction of %s.',
                             estimatedNodes, self.nodeTypeString, self.nodeShape, 
                             self.totalNodes, queueSize, fix_my_name,
                             self.scaler.config.alphaPacking, runtimeCorrection)

                # Use inertia parameter to stop small fluctuations
                if estimatedNodes <= self.totalNodes * self.scaler.config.betaInertia <= estimatedNodes:
                    logger.debug('Difference in new (%s) and previous estimates in number of '
                                 '%s (%s) required is within beta (%s), making no change.',
                                 estimatedNodes, self.nodeTypeString, self.totalNodes, self.scaler.config.betaInertia)
                    estimatedNodes = self.totalNodes

                # Bound number using the max and min node parameters
                if estimatedNodes > self.maxNodes:
                    logger.info('Limiting the estimated number of necessary %s (%s) to the '
                                'configured maximum (%s).', self.nodeTypeString, estimatedNodes, self.maxNodes)
                    estimatedNodes = self.maxNodes
                elif estimatedNodes < self.minNodes:
                    logger.info('Raising the estimated number of necessary %s (%s) to the '
                                'configured mininimum (%s).', self.nodeTypeString, estimatedNodes, self.minNodes)
                    estimatedNodes = self.minNodes

                if estimatedNodes != self.totalNodes:
                    logger.info('Changing the number of %s from %s to %s.', self.nodeTypeString, self.totalNodes,
                                estimatedNodes)
                    self.totalNodes = self.scaler.provisioner.setNodeCount(numNodes=estimatedNodes,
                                                                           preemptable=self.preemptable)
                    
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

                self.scaler.provisioner.checkStats()
                    
        self.scaler.provisioner.shutDown(preemptable=self.preemptable)
        logger.info('Scaler exited normally.')
