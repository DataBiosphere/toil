# Copyright (C) 2015-2021 Regents of the University of California
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
from difflib import get_close_matches
from typing import List, Tuple, Set, Optional

logger = logging.getLogger(__name__)


def cluster_factory(provisioner, clusterName=None, clusterType='mesos', zone=None, nodeStorage=50, nodeStorageOverrides=None, sseKey=None):
    """
    Find and instantiate the appropriate provisioner instance to make clusters
    in the given cloud.

    Raises ClusterTypeNotSupportedException if the given provisioner does not
    implement clusters of the given type.

    :param provisioner: The cloud type of the cluster.
    :param clusterName: The name of the cluster.
    :param clusterType: The type of cluster: 'mesos' or 'kubernetes'.
    :param zone: The cloud zone
    :return: A cluster object for the the cloud type.
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


def add_provisioner_options(parser):
    group = parser.add_argument_group("Provisioner Options.")

    provisioner_choices = ['aws', 'gce']
    # TODO: Better consolidate this provisioner arg and the one in common.py?
    group.add_argument('--provisioner', '-p', dest="provisioner", choices=provisioner_choices, default='aws',
                       help=f"The provisioner for cluster auto-scaling.  This is the '--provisioner' option set for "
                            f"Toil utils like launch-cluster and destroy-cluster, which always require a provisioner, "
                            f"and so this defaults to: %(default)s.  Choices: {provisioner_choices}.")
    group.add_argument('-z', '--zone', dest='zone', required=False, default=None,
                       help="The availability zone of the master. This parameter can also be set via the 'TOIL_X_ZONE' "
                            "environment variable, where X is AWS or GCE, or by the ec2_region_name parameter "
                            "in your .boto file, or derived from the instance metadata if using this utility on an "
                            "existing EC2 instance.")
    group.add_argument("clusterName", help="The name that the cluster will be identifiable by.  "
                                           "Must be lowercase and may not contain the '_' character.")


def parse_node_types(node_type_specs: Optional[str]) -> List[Tuple[Set[str], Optional[float]]]:
    """
    Parse a specification for zero or more node types.

    Takes a comma-separated list of node types. Each node type is a
    slash-separated list of at least one instance type name (like 'm5a.large'
    for AWS), and an optional bid in dollars after a colon.
    
    Raises ValueError if a node type cannot be parsed.

    Inputs should look something like this:

    >>> parse_node_types('c5.4xlarge/c5a.4xlarge:0.42,t2.large')
    [({'c5.4xlarge', 'c5a.4xlarge'}, 0.42), ({'t2.large'}, None)]

    :param node_type_specs: A string defining node types

    :returns: a list of node types, where each type is the set of
              instance types, and the float bid, or None.
    """
    
    # Collect together all the node types
    parsed = []
    
    if node_type_specs:
        # Some node types were actually specified
        for node_type_spec in node_type_specs.split(','):
            try:
                # Types are comma-separated
                # Then we have the colon and the bid
                parts = node_type_spec.split(':')

                if len(parts) > 2:
                    # Only one bid allowed
                    raise ValueError(f'Cound not parse node type "{node_type_spec}": multiple bids')

                # Instance types are slash-separated within an equivalence
                # class
                instance_types = set(parts[0].split('/'))

                for instance_type in instance_types:
                    if instance_type == '':
                        # No empty instance types allowed
                        raise ValueError(f'Cound not parse node type "{node_type_spec}": empty instance type')

                # Build the node type tuple
                parsed.append((instance_types, float(parts[1]) if len(parts) > 1 else None))
            except Exception as e:
                if isinstance(e, ValueError):
                    raise
                else:
                    raise ValueError(f'Cound not parse node type "{node_type_spec}"')

    return parsed


def check_valid_node_types(provisioner, node_types: List[Tuple[Set[str], Optional[float]]]):
    """
    Raises if an invalid nodeType is specified for aws or gce.

    :param str provisioner: 'aws' or 'gce' to specify which cloud provisioner used.
    :param node_types: A list of node types.  Example: [({'t2.micro'}, None), ({'t2.medium'}, 0.5)]
    :return: Nothing.  Raises if any instance type in the node type isn't real.
    """
    
    # check if a valid node type for aws
    from toil.lib.generatedEC2Lists import E2Instances, regionDict
    if provisioner == 'aws':
        from toil.provisioners.aws import get_current_aws_region
        current_region = get_current_aws_region() or 'us-west-2'
        # check if instance type exists in this region
        for node_type in node_types:
            for instance_type_name in node_type[0]:
                if instance_type_name not in regionDict[current_region]:
                    # They probably misspelled it and can't tell.
                    close = get_close_matches(instance_type_name, regionDict[current_region], 1)
                    if len(close) > 0:
                        helpText = ' Did you mean ' + close[0] + '?'
                    else:
                        helpText = ''
                    raise RuntimeError(f'Invalid instance type ({instance_type_name}) specified for AWS in '
                                       f'region: {current_region}.{helpText}')
    elif provisioner == 'gce':
        for node_type in node_types:
            for instance_type_name in node_type[0]:
                if instance_type_name in E2Instances:
                    raise RuntimeError(f"It looks like you've specified an AWS nodeType with the {provisioner} "
                                       f"provisioner. Please specify a nodeType for {provisioner}.")
    else:
        raise RuntimeError(f"Invalid provisioner: {provisioner}")


class NoSuchClusterException(Exception):
    """Indicates that the specified cluster does not exist."""
    def __init__(self, cluster_name):
        super(NoSuchClusterException, self).__init__(f"The cluster '{cluster_name}' could not be found")


class ClusterTypeNotSupportedException(Exception):
    """Indicates that a provisioner does not support a given cluster type."""
    def __init__(self, provisioner_class, cluster_type):
        super().__init__(f"The {provisioner_class} provisioner does not support making {cluster_type} clusters")
