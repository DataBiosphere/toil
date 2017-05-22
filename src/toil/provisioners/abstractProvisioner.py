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
import json
import logging
import os
from abc import ABCMeta, abstractmethod

from collections import namedtuple


import time

from bd2k.util.retry import never
from bd2k.util.threading import ExceptionalThread

from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem, NodeInfo

log = logging.getLogger(__name__)


Shape = namedtuple("_Shape", "wallTime memory cores disk")
"""
Represents a job or a node's "shape", in terms of the dimensions of memory, cores, disk and
wall-time allocation. All attributes are integers.

The wallTime attribute stores the number of seconds of a node allocation, e.g. 3600 for AWS,
or 60 for Azure. FIXME: and for jobs?

The memory and disk attributes store the number of bytes required by a job (or provided by a
node) in RAM or on disk (SSD or HDD), respectively.
"""


class AbstractProvisioner(object):
    """
    An abstract base class to represent the interface for provisioning worker nodes to use in a
    Toil cluster.
    """

    __metaclass__ = ABCMeta


    def __init__(self, config=None, batchSystem=None):
        """
        Initialize provisioner. If config and batchSystem are not specified, the
        provisioner is being used to manage nodes without a workflow

        :param config: Config from common.py
        :param batchSystem: The batchSystem used during run
        """
        self.config = config
        self.batchSystem = batchSystem
        self.stop = False
        self.stats = {}
        self.statsThreads = []
        self.statsPath = config.clusterStats if config else None
        self.scaleable = isinstance(self.batchSystem, AbstractScalableBatchSystem) if batchSystem else False
        self.staticNodesDict = {}  # dict with keys of nodes private IPs, val is nodeInfo
        self.static = {}

    def getStaticNodes(self, preemptable):
        return self.static[preemptable]

    @staticmethod
    def retryPredicate(e):
        """
        Return true if the exception e should be retried by the cluster scaler

        :param e: exception raised during execution of setNodeCount
        :return: boolean indicating whether the exception e should be retried
        """
        return never(e)

    def _shutDownStats(self):
        def getFileName():
            extension = '.json'
            file = '%s-stats' % self.config.jobStore
            counter = 0
            while True:
                suffix = str(counter).zfill(3) + extension
                fullName = os.path.join(self.statsPath, file + suffix)
                if not os.path.exists(fullName):
                    return fullName
                counter += 1
        if self.config.clusterStats and self.scaleable and not self.stop:
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
                log.debug('%s provisioner stats thread shut down successfully.', threadName)
                self.stats[threadName] = stats
        else:
            pass

    def setStaticNodes(self, nodes, preemptable):
        """
        Allows tracking of statically provisioned nodes. These are
        treated differently than autoscaled nodes in that they should not
        be automatically terminated.

        :param nodes: list of Node objects
        """
        prefix = 'non-' if not preemptable else ''
        log.debug("Adding %s to %spreemptable static nodes", nodes, preemptable)
        self.static[preemptable] = nodes

    @abstractmethod
    def addNodes(self, numNodes, preemptable):
        """

        :param numNodes:
        :param preemptable:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def logAndTerminate(self, nodes):
        """
        Terminate the nodes represented by given Node objects

        :param nodes: list of Node objects
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def getProvisionedWorkers(self, preemptable):
        """
        Gets all nodes known about in the provisioner. Includes both static and autoscaled
        nodes.

        :param preemptable:
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def remainingBillingInterval(self, node):
        """
        Calculate how much of a node's allocated billing interval is
        left in this cycle.

        :param node: Node object
        :return: float from 0 -> 1.0 representing percentage of pre-paid time left in cycle
        """
        raise NotImplementedError

    @abstractmethod
    def getNodeShape(self, preemptable=False):
        """
        The shape of a preemptable or non-preemptable node managed by this provisioner. The node
        shape defines key properties of a machine, such as its number of cores or the time
        between billing intervals.

        :param preemptable: Whether to return the shape of preemptable nodes or that of
               non-preemptable ones.

        :rtype: Shape
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def rsyncLeader(cls, clusterName, src, dst):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def launchCluster(cls, instanceType, keyName, clusterName, spotBid=None):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def sshLeader(cls, clusterName, args):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def destroyCluster(cls, clusterName):
        raise NotImplementedError
