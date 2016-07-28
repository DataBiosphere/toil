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


class AbstractProvisioner(object):
    """
    An abstract base class to represent the interface for provisioning worker nodes to use in a
    Toil cluster.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def setNodeCount(self, numNodes, preemptable=False, force=False):
        """
        Attempt to grow or shrink the number of prepemptable or non-preemptable worker nodes in
        the cluster to the given value, or as close a value as possible, and, after performing
        the necessary additions or removals of worker nodes, return the resulting number of
        preemptable or non-preemptable nodes currently in the cluster.

        :param int numNodes: Number of nodes to add.

        :param bool preemptable: whether the added nodes will be preemptable, i.e. whether they
               may be removed spontaneously by the underlying platform at any time.

        :param bool force: If False, the provisioner is allowed to deviate from the given number
               of nodes. For example, when downsizing a cluster, a provisioner might leave nodes
               running if they have active jobs running on them.

        :rtype: int :return: the number of nodes in the cluster after making the necessary
                adjustments. This value should be, but is not guaranteed to be, close or equal to
                the `numNodes` argument. It represents the closest possible approximation of the
                actual cluster size at the time this method returns.
        """
        raise NotImplementedError()

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
