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


class ProvisioningException( Exception ):
    """
    General provisioning exception. 
    """
    def __init__( self, message ):
        super( ProvisioningException, self ).__init__( message )

class AbstractProvisioner(object):
    """
    An abstract base class to represent the interface for provisioning
    worker nodes to use in a toil cluster.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def addNodes(self, nodes=1, preemptable=False):
        """
        Adds worker node to the set of worker nodes. The function should block
        while the node is being provisioned. 
        
        :param int nodes: Number of nodes to added.
        
        :raise ProvisioningException: If worker nodes can not be added.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def removeNodes(self, nodes=1, preemptable=False):
        """
        Removes worker nodes from the set of worker nodes.
        
        :param int nodes: Number of nodes to remove.
        :raise ProvisioningException: If worker nodes can not be removed.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def numberOfWorkers(self, preemptable=False):
        """
        The total number of worker nodes in the cluster.

        :param boolean preemptable: If True the return value is the number of preemptable workers
               in the cluster, else is number of non-preemptable workers.

        :rtype: int
        """
        raise NotImplementedError()
