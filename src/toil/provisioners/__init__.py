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
logger = logging.getLogger(__name__)


def clusterFactory(provisioner, clusterName=None, clusterType='mesos', zone=None, nodeStorage=50, nodeStorageOverrides=None, sseKey=None):
    """
    :param provisioner: The cloud type of the cluster.
    :param clusterName: The name of the cluster.
    :param clusterType: The type of cluster: 'mesos' or 'kubernetes'.
    :param zone: The cloud zone
    :return: A cluster object for the the cloud type.
    
    Raises ClusterTypeNotSupportedException if the given provisioner does not
    implement clusters of the given type.
    
    """
    if provisioner == 'aws':
        try:
            from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        except ImportError:
            logger.error('The aws extra must be installed to use this provisioner')
            raise
        return AWSProvisioner(clusterName, clusterType, zone, nodeStorage, nodeStorageOverrides, sseKey)
    elif provisioner == 'gce':
        try:
            from toil.provisioners.gceProvisioner import GCEProvisioner
        except ImportError:
            logger.error('The google extra must be installed to use this provisioner')
            raise
        return GCEProvisioner(clusterName, clusterType, zone, nodeStorage, nodeStorageOverrides, sseKey)
    else:
        raise RuntimeError("Invalid provisioner '%s'" % provisioner)

class NoSuchClusterException(Exception):
    """Indicates that the specified cluster does not exist."""
    def __init__(self, clusterName):
        super(NoSuchClusterException, self).__init__("The cluster '%s' could not be found" % clusterName)
        
class ClusterTypeNotSupportedException(Exception):
    """Indicates that a provisioner does not support a given cluster type."""
    def __init__(self, provisionerClass, clusterType):
        super().__init__("The %s provisioner does not support making %s clusters" % (provisionerClass, clusterType))
