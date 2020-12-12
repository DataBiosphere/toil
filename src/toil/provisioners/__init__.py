# Copyright (C) 2015-2020 Regents of the University of California
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

logger = logging.getLogger(__name__)


def clusterFactory(provisioner, clusterName=None, zone=None, nodeStorage=50, nodeStorageOverrides=None, sseKey=None):
    """
    :param clusterName: The name of the cluster.
    :param provisioner: The cloud type of the cluster.
    :param zone: The cloud zone
    :return: A cluster object for the the cloud type.
    """
    if provisioner == 'aws':
        try:
            from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        except ImportError:
            logger.error('The aws extra must be installed to use this provisioner')
            raise
        return AWSProvisioner(clusterName, zone, nodeStorage, nodeStorageOverrides, sseKey)
    elif provisioner == 'gce':
        try:
            from toil.provisioners.gceProvisioner import GCEProvisioner
        except ImportError:
            logger.error('The google extra must be installed to use this provisioner')
            raise
        return GCEProvisioner(clusterName, zone, nodeStorage, nodeStorageOverrides, sseKey)
    else:
        raise RuntimeError("Invalid provisioner '%s'" % provisioner)


def add_provisioner_options(parser):
    group = parser.add_argument_group("Provisioner Options.")
    group.add_argument('-p', "--provisioner", dest='provisioner', choices=['aws', 'gce'], required=False,
                       default="aws", help="The provisioner for cluster auto-scaling.  "
                                           "AWS and Google are currently supported.")
    group.add_argument('-z', '--zone', dest='zone', required=False, default=None,
                       help="The availability zone of the master. This parameter can also be set via the 'TOIL_X_ZONE' "
                            "environment variable, where X is AWS or GCE, or by the ec2_region_name parameter "
                            "in your .boto file, or derived from the instance metadata if using this utility on an "
                            "existing EC2 instance.")
    group.add_argument("clusterName", help="The name that the cluster will be identifiable by.  "
                                           "Must be lowercase and may not contain the '_' character.")


class NoSuchClusterException(Exception):
    """Indicates that the specified cluster does not exist."""
    def __init__(self, clusterName):
        super(NoSuchClusterException, self).__init__("The cluster '%s' could not be found" % clusterName)
