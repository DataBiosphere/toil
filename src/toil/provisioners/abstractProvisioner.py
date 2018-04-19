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
import logging
import os.path
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

    LEADER_HOME_DIR = '/root/' # home directory in the Toil appliance on an instance

    def __init__(self, clusterName=None, zone=None, nodeStorage=50):
        """
        Initialize provisioner.

        :param clusterName: The cluster identifier.
        :param zone: The zone the cluster runs in.
        :param nodeStorage: The amount of storage on the worker instances, in gigabytes.
        """
        self.clusterName = clusterName
        self._zone = zone
        self._nodeStorage = nodeStorage

    def readClusterSettings(self):
        """
        Initialize class from an existing cluster. This method assumes that
        the instance we are running on is the leader.
        """
        raise NotImplementedError

    def setAutoscaledNodeTypes(self, nodeTypes):
        """
        Set node types, shapes and spot bids. Preemptable nodes will have the form "type:spotBid".
        :param nodeTypes: A list of node types
        """
        spotBids = []
        nonPreemptableNodeTypes = []
        preemptableNodeTypes = []
        for nodeTypeStr in nodeTypes:
            nodeBidTuple = nodeTypeStr.split(":")
            if len(nodeBidTuple) == 2:
                #This is a preemptable node type, with a spot bid
                preemptableNodeTypes.append(nodeBidTuple[0])
                spotBids.append(nodeBidTuple[1])
            else:
                nonPreemptableNodeTypes.append(nodeTypeStr)
        preemptableNodeShapes = [self.getNodeShape(nodeType=nodeType, preemptable=True)
                                      for nodeType in preemptableNodeTypes]
        nonPreemptableNodeShapes = [self.getNodeShape(nodeType=nodeType, preemptable=False)
                                         for nodeType in nonPreemptableNodeTypes]
        self.nodeShapes = nonPreemptableNodeShapes + preemptableNodeShapes
        self.nodeTypes = nonPreemptableNodeTypes + preemptableNodeTypes
        self._spotBidsMap = dict(zip(preemptableNodeTypes, spotBids))

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

    @abstractmethod
    def launchCluster(self, leaderNodeType, leaderStorage, owner, **kwargs):
        """
        Initialize a cluster and create a leader node.

        :param leaderNodeType: The leader instance.
        :param leaderStorage: The amount of disk to allocate to the leader in gigabytes.
        :param owner: Tag identifying the owner of the instances.

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
    def getLeader(self):
        """
        :return: The leader node.
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

    def _setSSH(self):
        if not os.path.exists('/root/.sshSuccess'):
            subprocess.check_call(['ssh-keygen', '-f', '/root/.ssh/id_rsa', '-t', 'rsa', '-N', ''])
            with open('/root/.sshSuccess', 'w') as f:
                f.write('written here because of restrictive permissions on .ssh dir')
        os.chmod('/root/.ssh', 0o700)
        subprocess.check_call(['bash', '-c', 'eval $(ssh-agent) && ssh-add -k'])
        with open('/root/.ssh/id_rsa.pub') as f:
            masterPublicKey = f.read()
        masterPublicKey = masterPublicKey.split(' ')[1]  # take 'body' of key
        # confirm it really is an RSA public key
        assert masterPublicKey.startswith('AAAAB3NzaC1yc2E'), masterPublicKey
        return masterPublicKey
