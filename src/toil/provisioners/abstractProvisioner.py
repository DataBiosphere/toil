# Copyright (C) 2015-2018 Regents of the University of California
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
from abc import ABCMeta, abstractmethod
from builtins import object
from collections import namedtuple
from itertools import count
import logging
import pipes
import socket
from toil import subprocess

from future.utils import with_metaclass

from bd2k.util.retry import never
a_short_time = 5

log = logging.getLogger(__name__)


Shape = namedtuple("_Shape", "wallTime memory cores disk preemptable")
"""
Represents a job or a node's "shape", in terms of the dimensions of memory, cores, disk and
wall-time allocation.

The wallTime attribute stores the number of seconds of a node allocation, e.g. 3600 for AWS,
or 60 for Azure. FIXME: and for jobs?

The memory and disk attributes store the number of bytes required by a job (or provided by a
node) in RAM or on disk (SSD or HDD), respectively.
"""


class AbstractProvisioner(with_metaclass(ABCMeta, object)):
    """
    An abstract base class to represent the interface for provisioning worker nodes to use in a
    Toil cluster.
    """

    def __init__(self, clusterName, zone=None, config=None):
        """
        Initialize provisioner. If config and batchSystem are not specified, the
        provisioner is being used to manage nodes without a workflow

        :param config: Config from common.py
        :param batchSystem: The batchSystem used during run
        """
        self.config = config
        self.clusterName = clusterName
        self.zone = zone
        self.static = {}

    def getStaticNodes(self, preemptable):
        return self.static[preemptable]

    @staticmethod
    def retryPredicate(e):
        """
        Return true if the exception e should be retried by the cluster scaler.
        For example, should return true if the exception was due to exceeding an API rate limit.
        The error will be retried with exponential backoff.

        :param e: exception raised during execution of setNodeCount
        :return: boolean indicating whether the exception e should be retried
        """
        return never(e)

    def setStaticNodes(self, nodes, preemptable):
        """
        Used to track statically provisioned nodes. These nodes are
        treated differently than autoscaled nodes in that they should not
        be automatically terminated.

        :param nodes: list of Node objects
        """
        prefix = 'non-' if not preemptable else ''
        log.debug("Adding %s to %spreemptable static nodes", nodes, prefix)
        if nodes is not None:
            self.static[preemptable] = {node.privateIP : node for node in nodes}

    @abstractmethod
    def launchCluster(self, leaderNodeType, keyName, userTags=None,
            vpcSubnet=None, leaderStorage=50, nodeStorage=50, botoPath=None):
        """
        Initialize a cluster and create a leader node.

        :param leaderNodeType: The leader instance.
        :param preemptable: whether or not the nodes will be preemptable
        :param spotBid: The bid for preemptable nodes if applicable (this can be set in config, also).
        :return: number of nodes successfully added
        """
        raise NotImplementedError

    @abstractmethod
    def addNodes(self, nodeType, numNodes, preemptable, spotBid=None):
        """
        Used to add worker nodes to the cluster

        :param numNodes: The number of nodes to add
        :param preemptable: whether or not the nodes will be preemptable
        :param spotBid: The bid for preemptable nodes if applicable (this can be set in config, also).
        :return: number of nodes successfully added
        """
        raise NotImplementedError

    @abstractmethod
    def terminateNodes(self, nodes):
        """
        Terminate the nodes represented by given Node objects

        :param nodes: list of Node objects
        """
        raise NotImplementedError

    @abstractmethod
    def getProvisionedWorkers(self, nodeType, preemptable):
        """
        Gets all nodes of the given preemptability from the provisioner.
        Includes both static and autoscaled nodes.

        :param preemptable: Boolean value indicating whether to return preemptable nodes or
           non-preemptable nodes
        :return: list of Node objects
        """
        raise NotImplementedError

    @abstractmethod
    def getNodeShape(self, nodeType=None, preemptable=False):
        """
        The shape of a preemptable or non-preemptable node managed by this provisioner. The node
        shape defines key properties of a machine, such as its number of cores or the time
        between billing intervals.

        :param str nodeType: Node type name to return the shape of.

        :rtype: Shape
        """
        raise NotImplementedError

    @abstractmethod
    def destroyCluster(self):
        """
        Terminates all nodes in the specified cluster and cleans up all resources associated with the
        cluser.
        :param clusterName: identifier of the cluster to terminate.
        """
        raise NotImplementedError


