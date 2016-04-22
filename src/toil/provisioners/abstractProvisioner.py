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

from abc import ABCMeta, abstractmethod

from collections import namedtuple


Shape = namedtuple("_Shape", "wallTime memory cores disk")
"""
Represents a job or a node's "shape", in terms of the dimensions of memory, cores, disk and
wall-time allocation. All attributes are integers.

The wallTime attribute stores the number of seconds of a node allocation, e.g. 3600 for AWS,
or 60 for Azure. FIXME: and for jobs?

The memory and disk attributes store the number of bytes required by a job (or provided by a
node) in RAM or on disk (SSD or HDD), respectively.
"""


class ProvisioningException(Exception):
    """
    A general provisioning exception.
    """

    def __init__(self, message):
        super(ProvisioningException, self).__init__(message)


class AbstractProvisioner(object):
    """
    An abstract base class to represent the interface for provisioning worker nodes to use in a
    Toil cluster.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def addNodes(self, numNodes=1, preemptable=False):
        """
        Add worker nodes to the set of worker nodes. The function must block while the nodes are
        being provisioned. It should to fail atomically, i.e. it should either add the requested
        number of nodes or none at all.

        :param int numNodes: Number of nodes to add.

        :param bool preemptable: whether the added nodes will be preemptable, i.e. if they may be
               removed spontaneously by the underlying platform at any time.
        
        :raise ProvisioningException: If worker nodes can not be added.
        """
        raise NotImplementedError()

    @abstractmethod
    def removeNodes(self, numNodes=1, preemptable=False):
        """
        Removes worker nodes from the set of worker nodes. The function must block while the
        nodes are being removed. It should to fail atomically, i.e. it should either add the
        requested number of nodes or none at all.

        :param int numNodes: Number of nodes to remove.

        :param bool preemptable: if True, preemptable nodes will be removed,
               otherwise non-preemptable nodes will be removed.

        :raise ProvisioningException: If worker nodes can not be removed.
        """
        raise NotImplementedError()

    @abstractmethod
    def getNumberOfNodes(self, preemptable=False):
        """
        The total number of worker nodes in the cluster.

        :param boolean preemptable: If True the return value is the number of preemptable workers
               in the cluster, else is number of non-preemptable workers.

        :rtype: int
        """
        raise NotImplementedError()

    @abstractmethod
    def getNodeShape(self, preemptable=False):
        """
        The shape of a node managed by this provisioner. The node shape defines key properties of
        a machine, such as its number of cores or the time between billing intervals.

        :param preemptable: Whether to return the shape of preemptable nodes or that of
               non-preemptable ones.

        :rtype: Shape
        """
        raise NotImplementedError
