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
"""Launches a toil leader instance with the specified provisioner."""
import logging
import os

from toil import applianceSelf
from toil.common import parser_with_common_options
from toil.provisioners import parse_node_types, check_valid_node_types, cluster_factory
from toil.statsAndLogging import set_logging_from_options

logger = logging.getLogger(__name__)


def create_tags_dict(tags: list) -> dict:
    tags_dict = dict()
    for tag in tags:
        key, value = tag.split('=')
        tags_dict[key] = value
    return tags_dict


def main():
    parser = parser_with_common_options(provisioner_options=True, jobstore_option=False)
    parser.add_argument("-T", "--clusterType", dest="clusterType",
                        choices=['mesos', 'kubernetes'], default='mesos',
                        help="Cluster scheduler to use.")
    parser.add_argument("--leaderNodeType", dest="leaderNodeType", required=True,
                        help="Non-preemptable node type to use for the cluster leader.")
    parser.add_argument("--keyPairName", dest='keyPairName',
                        help="On AWS, the name of the AWS key pair to include on the instance."
                        " On Google/GCE, this is the ssh key pair.")
    parser.add_argument("--owner", dest='owner',
                        help="The owner tag for all instances. If not given, the value in"
                        " --keyPairName will be used if given.")
    parser.add_argument("--boto", dest='botoPath',
                        help="The path to the boto credentials directory. This is transferred "
                        "to all nodes in order to access the AWS jobStore from non-AWS instances.")
    parser.add_argument("-t", "--tag", metavar='NAME=VALUE', dest='tags',
                        default=[], action='append',
                        help="Tags are added to the AWS cluster for this node and all of its "
                             "children. Tags are of the form:\n"
                             " -t key1=value1 --tag key2=value2\n"
                             "Multiple tags are allowed and each tag needs its own flag. By "
                             "default the cluster is tagged with "
                             " {\n"
                             "      \"Name\": clusterName,\n"
                             "      \"Owner\": IAM username\n"
                             " }. ")
    parser.add_argument("--vpcSubnet",
                        help="VPC subnet ID to launch cluster in. Uses default subnet if not "
                        "specified. This subnet needs to have auto assign IPs turned on.")
    parser.add_argument("--nodeTypes", dest='nodeTypes', default=None, type=str,
                        help="Specifies a list of comma-separated node types, each of which is "
                             "composed of slash-separated instance types, and an optional spot "
                             "bid set off by a colon, making the node type preemptable. Instance "
                             "types may appear in multiple node types, and the same node type "
                             "may appear as both preemptable and non-preemptable.\n"
                             "Valid argument specifying two node types:\n"
                             "\tc5.4xlarge/c5a.4xlarge:0.42,t2.large\n"
                             "Node types:\n"
                             "\tc5.4xlarge/c5a.4xlarge:0.42 and t2.large\n"
                             "Instance types:\n"
                             "\tc5.4xlarge, c5a.4xlarge, and t2.large\n"
                             "Semantics:\n"
                             "\tBid $0.42/hour for either c5.4xlarge or c5a.4xlarge instances,\n"
                             "\ttreated interchangeably, while they are available at that price,\n"
                             "\tand buy t2.large instances at full price\n"
                             "Must also provide the --workers argument to specify how many "
                             "workers of each node type to create.")
    parser.add_argument("-w", "--workers", dest='workers', default=None, type=str,
                        help="Comma-separated list of the ranges of numbers of workers of each "
                             "node type to launch, such as '0-2,5,1-3'. If a range is given, "
                             "workers will automatically be launched and terminated by the cluster "
                             "to auto-scale to the workload.")
    parser.add_argument("--leaderStorage", dest='leaderStorage', type=int, default=50,
                        help="Specify the size (in gigabytes) of the root volume for the leader "
                             "instance. This is an EBS volume.")
    parser.add_argument("--nodeStorage", dest='nodeStorage', type=int, default=50,
                        help="Specify the size (in gigabytes) of the root volume for any worker "
                             "instances created when using the -w flag. This is an EBS volume.")
    parser.add_argument('--forceDockerAppliance', dest='forceDockerAppliance', action='store_true',
                        default=False,
                        help="Disables sanity checking the existence of the docker image specified "
                             "by TOIL_APPLIANCE_SELF, which Toil uses to provision mesos for "
                             "autoscaling.")
    parser.add_argument('--awsEc2ProfileArn', dest='awsEc2ProfileArn', default=None, type=str,
                        help="If provided, the specified ARN is used as the instance profile for EC2 instances."
                             "Useful for setting custom IAM profiles. If not specified, a new IAM role is created "
                             "by default with sufficient access to perform basic cluster operations.")
    parser.add_argument('--awsEc2ExtraSecurityGroupId', dest='awsEc2ExtraSecurityGroupIds', default=[], action='append',
                        help="Any additional security groups to attach to EC2 instances. Note that a security group "
                             "with its name equal to the cluster name will always be created, thus ensure that "
                             "the extra security groups do not have the same name as the cluster name.")
    options = parser.parse_args()
    set_logging_from_options(options)
    tags = create_tags_dict(options.tags) if options.tags else dict()

    # Get worker node types
    worker_node_types = parse_node_types(options.nodeTypes)
    check_valid_node_types(options.provisioner, worker_node_types + [({options.leaderNodeType}, None)])

    # Holds string ranges, like "5", or "3-10"
    worker_node_ranges = options.workers.split(',') if options.workers else []

    # checks the validity of TOIL_APPLIANCE_SELF before proceeding
    applianceSelf(forceDockerAppliance=options.forceDockerAppliance)

    # This holds either ints to launch static nodes, or tuples of ints
    # specifying ranges to launch managed auto-scaling nodes, for each type.
    nodeCounts = []

    if ((worker_node_types != [] or worker_node_ranges != []) and not
        (worker_node_types != [] and worker_node_ranges != [])):
        raise RuntimeError("The --nodeTypes option requires --workers, and visa versa.")
    if worker_node_types and worker_node_ranges:
            if not len(worker_node_types) == len(worker_node_ranges):
                raise RuntimeError("List of worker count ranges must be the same length as the list of node types.")

            for spec in worker_node_ranges:
                if '-' in spec:
                    # Provision via autoscaling
                    parts = spec.split('-')
                    if len(parts) != 2:
                        raise RuntimeError("Unacceptable range: " + spec)
                    nodeCounts.append((int(parts[0]), int(parts[1])))
                else:
                    # Provision fixed nodes
                    nodeCounts.append(int(spec))

    owner = options.owner or options.keyPairName or 'toil'

    # Check to see if the user specified a zone. If not, see if one is stored in an environment variable.
    options.zone = options.zone or os.environ.get(f'TOIL_{options.provisioner.upper()}_ZONE')

    if not options.zone:
        raise RuntimeError(f'Please provide a value for --zone or set a default in the '
                           f'TOIL_{options.provisioner.upper()}_ZONE environment variable.')

    logger.info('Creating cluster %s...', options.clusterName)

    cluster = cluster_factory(provisioner=options.provisioner,
                              clusterName=options.clusterName,
                              clusterType=options.clusterType,
                              zone=options.zone,
                              nodeStorage=options.nodeStorage)

    cluster.launchCluster(leaderNodeType=options.leaderNodeType,
                          leaderStorage=options.leaderStorage,
                          owner=owner,
                          keyName=options.keyPairName,
                          botoPath=options.botoPath,
                          userTags=tags,
                          vpcSubnet=options.vpcSubnet,
                          awsEc2ProfileArn=options.awsEc2ProfileArn,
                          awsEc2ExtraSecurityGroupIds=options.awsEc2ExtraSecurityGroupIds)

    for typeNum, spec in enumerate(nodeCounts):
        # For each batch of workers to make
        wanted = worker_node_types[typeNum]

        if isinstance(spec, int):
            # Make static nodes

            if spec == 0:
                # Don't make anything
                continue

            if wanted[1] is None:
                # Make non-spot instances
                cluster.addNodes(nodeTypes=wanted[0], numNodes=spec, preemptable=False)
            else:
                # We have a spot bid
                cluster.addNodes(nodeTypes=wanted[0], numNodes=spec, preemptable=True,
                                 spotBid=wanted[1])

        elif isinstance(spec, tuple):
            # Make a range of auto-scaling nodes

            max_count, min_count = spec

            if max_count < min_count:
                # Flip them around
                min_count, max_count = max_count, min_count

            if max_count == 0:
                # Don't want any
                continue

            if wanted[1] is None:
                # Make non-spot instances
                cluster.addManagedNodes(nodeTypes=wanted[0], minNodes=min_count, maxNodes=max_count,
                                        preemptable=False)
            else:
                # Bid at the given price.
                cluster.addManagedNodes(nodeTypes=wanted[0], minNodes=min_count, maxNodes=max_count,
                                        preemptable=True, spotBid=wanted[1])

    logger.info('Cluster created successfully.')



