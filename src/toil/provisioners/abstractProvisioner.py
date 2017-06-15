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
import logging
from abc import ABCMeta, abstractmethod

from collections import namedtuple



from bd2k.util.retry import never


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


    def __init__(self, config=None):
        """
        Initialize provisioner. If config and batchSystem are not specified, the
        provisioner is being used to manage nodes without a workflow

        :param config: Config from common.py
        :param batchSystem: The batchSystem used during run
        """
        self.config = config
        self.stop = False
        self.staticNodesDict = {}  # dict with keys of nodes private IPs, val is nodeInfo
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
    def addNodes(self, numNodes, preemptable):
        """
        Used to add worker nodes to the cluster

        :param numNodes: The number of nodes to add
        :param preemptable: whether or not the nodes will be preemptable
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
    def getProvisionedWorkers(self, preemptable):
        """
        Gets all nodes of the given preemptability from the provisioner.
        Includes both static and autoscaled nodes.

        :param preemptable: Boolean value indicating whether to return preemptable nodes or
           non-preemptable nodes
        :return: list of Node objects
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
    def rsyncLeader(cls, clusterName, args, **kwargs):
        """
        Rsyncs to the leader of the cluster with the specified name. The arguments are passed directly to
        Rsync.

        :param clusterName: name of the cluster to target
        :param args: list of string arguments to rsync. Identical to the normal arguments to rsync, but the
           host name of the remote host can be omitted. ex) ['/localfile', ':/remotedest']
        :param \**kwargs:
           See below

        :Keyword Arguments:
            * *strict*: if False, strict host key checking is disabled. (Enabled by default.)
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def launchCluster(cls, instanceType, keyName, clusterName, spotBid=None):
        """
        Launches a cluster with the specified instance type for the leader with the specified name.

        :param instanceType: desired type of the leader instance
        :param keyName: name of the ssh key pair to launch the instance with
        :param clusterName: desired identifier of the cluster
        :param spotBid: how much to bid for the leader instance. If none, use on demand pricing.
        :return:
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def sshLeader(cls, clusterName, args, **kwargs):
        """
        SSH into the leader instance of the specified cluster with the specified arguments to SSH.
        :param clusterName: name of the cluster to target
        :param args: list of string arguments to ssh.
        :param strict: If False, strict host key checking is disabled. (Enabled by default.)
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def destroyCluster(cls, clusterName):
        """
        Terminates all nodes in the specified cluster and cleans up all resources associated with the
        cluser.
        :param clusterName: identifier of the cluster to terminate.
        """
        raise NotImplementedError
