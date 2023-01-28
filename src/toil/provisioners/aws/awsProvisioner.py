# Copyright (C) 2015-2021 UCSC Computational Genomics Lab
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
import json
import logging
import os
import platform
import socket
import string
import textwrap
import time
import uuid
from functools import wraps
from shlex import quote
from typing import (Any,
                    Callable,
                    Collection,
                    Dict,
                    Iterable,
                    List,
                    Optional,
                    Set)
from urllib.parse import unquote

# We need these to exist as attributes we can get off of the boto object
import boto.ec2
import boto.iam
import boto.vpc
from boto.ec2.blockdevicemapping import \
    BlockDeviceMapping as Boto2BlockDeviceMapping
from boto.ec2.blockdevicemapping import BlockDeviceType as Boto2BlockDeviceType
from boto.ec2.instance import Instance as Boto2Instance
from boto.exception import BotoServerError, EC2ResponseError
from boto.utils import get_instance_metadata
from botocore.exceptions import ClientError

from toil.lib.aws import zone_to_region
from toil.lib.aws.ami import get_flatcar_ami
from toil.lib.aws.iam import (CLUSTER_LAUNCHING_PERMISSIONS,
                              get_policy_permissions,
                              policy_permissions_allow)
from toil.lib.aws.session import AWSConnectionManager
from toil.lib.aws.utils import create_s3_bucket
from toil.lib.conversions import human2bytes
from toil.lib.ec2 import (a_short_time,
                          create_auto_scaling_group,
                          create_instances,
                          create_launch_template,
                          create_ondemand_instances,
                          create_spot_instances,
                          wait_instances_running,
                          wait_transition,
                          wait_until_instance_profile_arn_exists)
from toil.lib.ec2nodes import InstanceType
from toil.lib.generatedEC2Lists import E2Instances
from toil.lib.memoize import memoize
from toil.lib.misc import truncExpBackoff
from toil.lib.retry import (ErrorCondition,
                            get_error_body,
                            get_error_code,
                            get_error_message,
                            get_error_status,
                            old_retry,
                            retry)
from toil.provisioners import (ClusterCombinationNotSupportedException,
                               NoSuchClusterException)
from toil.provisioners.abstractProvisioner import (AbstractProvisioner,
                                                   ManagedNodesNotSupportedException,
                                                   Shape)
from toil.provisioners.aws import get_best_aws_zone
from toil.provisioners.node import Node

logger = logging.getLogger(__name__)
logging.getLogger("boto").setLevel(logging.CRITICAL)
# Role name (used as the suffix) for EC2 instance profiles that are automatically created by Toil.
_INSTANCE_PROFILE_ROLE_NAME = 'toil'
# The tag key that specifies the Toil node type ("leader" or "worker") so that
# leader vs. worker nodes can be robustly identified.
_TAG_KEY_TOIL_NODE_TYPE = 'ToilNodeType'
# The tag that specifies the cluster name on all nodes
_TAG_KEY_TOIL_CLUSTER_NAME = 'clusterName'
# How much storage on the root volume is expected to go to overhead and be
# unavailable to jobs when the node comes up?
# TODO: measure
_STORAGE_ROOT_OVERHEAD_GIGS = 4
# The maximum length of a S3 bucket
_S3_BUCKET_MAX_NAME_LEN = 63
# The suffix of the S3 bucket associated with the cluster
_S3_BUCKET_INTERNAL_SUFFIX = '--internal'


def awsRetryPredicate(e):
    if isinstance(e, socket.gaierror):
        # Could be a DNS outage:
        # socket.gaierror: [Errno -2] Name or service not known
        return True
    # boto/AWS gives multiple messages for the same error...
    if get_error_status(e) == 503 and 'Request limit exceeded' in get_error_body(e):
        return True
    elif get_error_status(e) == 400 and 'Rate exceeded' in get_error_body(e):
        return True
    elif get_error_status(e) == 400 and 'NotFound' in get_error_body(e):
        # EC2 can take a while to propagate instance IDs to all servers.
        return True
    elif get_error_status(e) == 400 and get_error_code(e) == 'Throttling':
        return True
    return False


def expectedShutdownErrors(e: Exception) -> bool:
    """
    Matches errors that we expect to occur during shutdown, and which indicate
    that we need to wait or try again.

    Should *not* match any errors which indicate that an operation is
    impossible or unnecessary (such as errors resulting from a thing not
    existing to be deleted).
    """
    return get_error_status(e) == 400 and 'dependent object' in get_error_body(e)


def awsRetry(f):
    """
    This decorator retries the wrapped function if aws throws unexpected errors
    errors.
    It should wrap any function that makes use of boto
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        for attempt in old_retry(delays=truncExpBackoff(),
                                 timeout=300,
                                 predicate=awsRetryPredicate):
            with attempt:
                return f(*args, **kwargs)
    return wrapper


def awsFilterImpairedNodes(nodes, ec2):
    # if TOIL_AWS_NODE_DEBUG is set don't terminate nodes with
    # failing status checks so they can be debugged
    nodeDebug = os.environ.get('TOIL_AWS_NODE_DEBUG') in ('True', 'TRUE', 'true', True)
    if not nodeDebug:
        return nodes
    nodeIDs = [node.id for node in nodes]
    statuses = ec2.get_all_instance_status(instance_ids=nodeIDs)
    statusMap = {status.id: status.instance_status for status in statuses}
    healthyNodes = [node for node in nodes if statusMap.get(node.id, None) != 'impaired']
    impairedNodes = [node.id for node in nodes if statusMap.get(node.id, None) == 'impaired']
    logger.warning('TOIL_AWS_NODE_DEBUG is set and nodes %s have failed EC2 status checks so '
                   'will not be terminated.', ' '.join(impairedNodes))
    return healthyNodes


class InvalidClusterStateException(Exception):
    pass

class AWSProvisioner(AbstractProvisioner):
    def __init__(self, clusterName, clusterType, zone, nodeStorage, nodeStorageOverrides, sseKey):
        self.cloud = 'aws'
        self._sseKey = sseKey
        # self._zone will be filled in by base class constructor
        # We will use it as the leader zone.
        zone = zone if zone else get_best_aws_zone()

        if zone is None:
            # Can't proceed without a real zone
            raise RuntimeError('No AWS availability zone specified. Configure in Boto '
                               'configuration file, TOIL_AWS_ZONE environment variable, or '
                               'on the command line.')

        # Determine our region to work in, before readClusterSettings() which
        # might need it. TODO: support multiple regions in one cluster
        self._region = zone_to_region(zone)

        # Set up our connections to AWS
        self.aws = AWSConnectionManager()

        # Set our architecture to the current machine architecture
        # Assume the same architecture unless specified differently in launchCluster()
        self._architecture = 'amd64' if platform.machine() in ['x86_64', 'amd64'] else 'arm64'

        # Call base class constructor, which will call createClusterSettings()
        # or readClusterSettings()
        super().__init__(clusterName, clusterType, zone, nodeStorage, nodeStorageOverrides)

        # After self.clusterName is set, generate a valid name for the S3 bucket associated with this cluster
        suffix = _S3_BUCKET_INTERNAL_SUFFIX
        self.s3_bucket_name = self.clusterName[:_S3_BUCKET_MAX_NAME_LEN - len(suffix)] + suffix

    def supportedClusterTypes(self):
        return {'mesos', 'kubernetes'}

    def createClusterSettings(self):
        """
        Create a new set of cluster settings for a cluster to be deployed into
        AWS.
        """

        # Nothing needs to happen here; self._zone is always filled by the
        # constructor.
        assert self._zone is not None

    def readClusterSettings(self):
        """
        Reads the cluster settings from the instance metadata, which assumes
        the instance is the leader.
        """
        instanceMetaData = get_instance_metadata()
        ec2 = self.aws.boto2(self._region, 'ec2')
        instance = ec2.get_all_instances(instance_ids=[instanceMetaData["instance-id"]])[0].instances[0]
        # The cluster name is the same as the name of the leader.
        self.clusterName = str(instance.tags["Name"])
        # Determine what subnet we, the leader, are in
        self._leader_subnet = instance.subnet_id
        # Determine where to deploy workers.
        self._worker_subnets_by_zone = self._get_good_subnets_like(self._leader_subnet)

        self._leaderPrivateIP = instanceMetaData['local-ipv4']  # this is PRIVATE IP
        self._keyName = list(instanceMetaData['public-keys'].keys())[0]
        self._tags = {k: v for k, v in self.getLeader().tags.items() if k != _TAG_KEY_TOIL_NODE_TYPE}
        # Grab the ARN name of the instance profile (a str) to apply to workers
        self._leaderProfileArn = instanceMetaData['iam']['info']['InstanceProfileArn']
        # The existing metadata API returns a single string if there is one security group, but
        # a list when there are multiple: change the format to always be a list.
        rawSecurityGroups = instanceMetaData['security-groups']
        self._leaderSecurityGroupNames = {rawSecurityGroups} if not isinstance(rawSecurityGroups, list) else set(rawSecurityGroups)
        # Since we have access to the names, we don't also need to use any IDs
        self._leaderSecurityGroupIDs = set()

        # Let the base provisioner work out how to deploy duly authorized
        # workers for this leader.
        self._setLeaderWorkerAuthentication()

    @retry(errors=[ErrorCondition(
        error=ClientError,
        error_codes=[404, 500, 502, 503, 504]
    )])
    def _write_file_to_cloud(self, key: str, contents: bytes) -> str:
        bucket_name = self.s3_bucket_name

        # Connect to S3
        s3 = self.aws.resource(self._region, 's3')
        s3_client = self.aws.client(self._region, 's3')

        # create bucket if needed, then write file to S3
        try:
            # the head_bucket() call makes sure that the bucket exists and the user can access it
            s3_client.head_bucket(Bucket=bucket_name)
            bucket = s3.Bucket(bucket_name)
        except ClientError as err:
            if get_error_status(err) == 404:
                bucket = create_s3_bucket(s3, bucket_name=bucket_name, region=self._region)
                bucket.wait_until_exists()
                bucket.Versioning().enable()

                owner_tag = os.environ.get('TOIL_OWNER_TAG')
                if owner_tag:
                    bucket_tagging = s3.BucketTagging(bucket_name)
                    bucket_tagging.put(Tagging={'TagSet': [{'Key': 'Owner', 'Value': owner_tag}]})
            else:
                raise

        # write file to bucket
        logger.debug(f'Writing "{key}" to bucket "{bucket_name}"...')
        obj = bucket.Object(key=key)
        obj.put(Body=contents)

        obj.wait_until_exists()
        return f's3://{bucket_name}/{key}'

    def _read_file_from_cloud(self, key: str) -> bytes:
        bucket_name = self.s3_bucket_name
        obj = self.aws.resource(self._region, 's3').Object(bucket_name, key)

        try:
            return obj.get().get('Body').read()
        except ClientError as e:
            if get_error_status(e) == 404:
                logger.warning(f'Trying to read non-existent file "{key}" from {bucket_name}.')
            raise

    def _get_user_data_limit(self) -> int:
        # See: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-add-user-data.html
        return human2bytes('16KB')

    def launchCluster(self,
                      leaderNodeType: str,
                      leaderStorage: int,
                      owner: str,
                      keyName: str,
                      botoPath: str,
                      userTags: Optional[dict],
                      vpcSubnet: Optional[str],
                      awsEc2ProfileArn: Optional[str],
                      awsEc2ExtraSecurityGroupIds: Optional[list],
                      **kwargs):
        """
        Starts a single leader node and populates this class with the leader's metadata.

        :param leaderNodeType: An AWS instance type, like "t2.medium", for example.
        :param leaderStorage: An integer number of gigabytes to provide the leader instance with.
        :param owner: Resources will be tagged with this owner string.
        :param keyName: The ssh key to use to access the leader node.
        :param botoPath: The path to the boto credentials directory.
        :param userTags: Optionally provided user tags to put on the cluster.
        :param vpcSubnet: Optionally specify the VPC subnet for the leader.
        :param awsEc2ProfileArn: Optionally provide the profile ARN.
        :param awsEc2ExtraSecurityGroupIds: Optionally provide additional security group IDs.
        :return: None
        """

        if 'network' in kwargs:
            logger.warning('AWS provisioner does not support a network parameter. Ignoring %s!', kwargs["network"])

        # First, pre-flight-check our permissions before making anything.
        if not policy_permissions_allow(get_policy_permissions(region=self._region), CLUSTER_LAUNCHING_PERMISSIONS):
            # Function prints a more specific warning to the log, but give some context.
            logger.warning('Toil may not be able to properly launch (or destroy!) your cluster.')

        leader_type = E2Instances[leaderNodeType]

        if self.clusterType == 'kubernetes':
            if leader_type.cores < 2:
                # Kubernetes won't run here.
                raise RuntimeError('Kubernetes requires 2 or more cores, and %s is too small' %
                                   leaderNodeType)
        self._keyName = keyName
        self._architecture = leader_type.architecture

        if self.clusterType == 'mesos' and self._architecture != 'amd64':
            # Mesos images aren't currently available for this architecture, so we can't start a Mesos cluster.
            # Complain about this before we create anything.
            raise ClusterCombinationNotSupportedException(type(self), self.clusterType, self._architecture,
                                                          reason="Mesos is only available for amd64.")

        if vpcSubnet:
            # This is where we put the leader
            self._leader_subnet = vpcSubnet
        else:
            # Find the default subnet for the zone
            self._leader_subnet = self._get_default_subnet(self._zone)

        profileArn = awsEc2ProfileArn or self._createProfileArn()

        # the security group name is used as the cluster identifier
        createdSGs = self._createSecurityGroups()
        bdms = self._getBoto3BlockDeviceMappings(leader_type, rootVolSize=leaderStorage)

        # Make up the tags
        self._tags = {'Name': self.clusterName,
                      'Owner': owner,
                      _TAG_KEY_TOIL_CLUSTER_NAME: self.clusterName}

        if userTags is not None:
            self._tags.update(userTags)

        #All user specified tags have been set
        userData = self._getIgnitionUserData('leader', architecture=self._architecture)

        if self.clusterType == 'kubernetes':
            # All nodes need a tag putting them in the cluster.
            # This tag needs to be on there before the a leader can finish its startup.
            self._tags['kubernetes.io/cluster/' + self.clusterName] = ''

        # Make tags for the leader specifically
        leader_tags = dict(self._tags)
        leader_tags[_TAG_KEY_TOIL_NODE_TYPE] = 'leader'
        logger.debug('Launching leader with tags: %s', leader_tags)

        instances = create_instances(self.aws.resource(self._region, 'ec2'),
                                     image_id=self._discoverAMI(),
                                     num_instances=1,
                                     key_name=self._keyName,
                                     security_group_ids=createdSGs + (awsEc2ExtraSecurityGroupIds or []),
                                     instance_type=leader_type.name,
                                     user_data=userData,
                                     block_device_map=bdms,
                                     instance_profile_arn=profileArn,
                                     placement_az=self._zone,
                                     subnet_id=self._leader_subnet,
                                     tags=leader_tags)

        # wait for the leader to exist at all
        leader = instances[0]
        leader.wait_until_exists()

        # Don't go on until the leader is started
        logger.info('Waiting for leader instance %s to be running', leader)
        leader.wait_until_running()

        # Now reload it to make sure all the IPs are set.
        leader.reload()

        if leader.public_ip_address is None:
            # Sometimes AWS just fails to assign a public IP when we really need one.
            # But sometimes people intend to use private IPs only in Toil-managed clusters.
            # TODO: detect if we have a route to the private IP and fail fast if not.
            logger.warning("AWS did not assign a public IP to the cluster leader. If you aren't "
                           "connected to the private subnet, cluster setup will fail!")

        # Remember enough about the leader to let us launch workers in its
        # cluster.
        self._leaderPrivateIP = leader.private_ip_address
        self._worker_subnets_by_zone = self._get_good_subnets_like(self._leader_subnet)
        self._leaderSecurityGroupNames = set()
        self._leaderSecurityGroupIDs = set(createdSGs + (awsEc2ExtraSecurityGroupIds or []))
        self._leaderProfileArn = profileArn

        leaderNode = Node(publicIP=leader.public_ip_address, privateIP=leader.private_ip_address,
                          name=leader.id, launchTime=leader.launch_time,
                          nodeType=leader_type.name, preemptible=False,
                          tags=leader.tags)
        leaderNode.waitForNode('toil_leader')

        # Download credentials
        self._setLeaderWorkerAuthentication(leaderNode)

    def toil_service_env_options(self) -> str:
        """
        Set AWS tags in user docker container
        """
        config = super().toil_service_env_options()
        instance_base_tags = json.dumps(self._tags)
        return config + " -e TOIL_AWS_TAGS=" + quote(instance_base_tags)

    def _get_worker_subnets(self) -> List[str]:
        """
        Get all worker subnets we should balance across, as a flat list.
        """
        # TODO: When we get multi-region clusters, scope this by region

        # This will hold the collected list of subnet IDs.
        collected = []
        for subnets in self._worker_subnets_by_zone.values():
            # We assume all zones are in the same region here.
            for subnet in subnets:
                # We don't need to deduplicate because each subnet is in only one region
                collected.append(subnet)
        return collected

    @awsRetry
    def _get_good_subnets_like(self, base_subnet_id: str) -> Dict[str, List[str]]:
        """
        Given a subnet ID, get all the similar subnets (including it),
        organized by availability zone.

        The input subnet must be in the available state.

        Similar subnets are ones with the same default-ness and ACLs.
        """

        # Grab the ec2 resource we need to make queries
        ec2 = self.aws.resource(self._region, 'ec2')
        # And the client
        ec2_client = self.aws.client(self._region, 'ec2')

        # What subnet are we basing this on?
        base_subnet = ec2.Subnet(base_subnet_id)

        # What VPC is it in?
        vpc_id = base_subnet.vpc_id

        # Is it default for its VPC?
        is_default = base_subnet.default_for_az

        # What ACLs does it have?
        acls = set(self._get_subnet_acls(base_subnet_id))

        # Compose a filter that selects the subnets we might want
        filters = [{
            'Name': 'vpc-id',
            'Values': [vpc_id]
        }, {
            'Name': 'default-for-az',
            'Values': ['true' if is_default else 'false']
        }, {
            'Name': 'state',
            'Values': ['available']
        }]

        # Fill in this collection
        by_az = {}

        # Go get all the subnets. There's no way to page manually here so it
        # must page automatically.
        for subnet in self.aws.resource(self._region, 'ec2').subnets.filter(Filters=filters):
            # For each subnet in the VPC

            # See if it has the right ACLs
            subnet_acls = set(self._get_subnet_acls(subnet.subnet_id))
            if subnet_acls != acls:
                # Reject this subnet because it has different ACLs
                logger.debug('Subnet %s is a lot like subnet %s but has ACLs of %s instead of %s; skipping',
                             subnet.subnet_id, base_subnet_id, subnet_acls, acls)
                continue

            if subnet.availability_zone not in by_az:
                # Make sure we have a bucket of subnets for this AZ
                by_az[subnet.availability_zone] = []
            # Bucket the IDs by availability zone
            by_az[subnet.availability_zone].append(subnet.subnet_id)

        return by_az

    @awsRetry
    def _get_subnet_acls(self, subnet: str) -> List[str]:
        """
        Get all Network ACL IDs associated with a given subnet ID.
        """

        # Grab the connection we need to use for this operation.
        ec2 = self.aws.client(self._region, 'ec2')

        # Compose a filter that selects the default subnet in the AZ
        filters = [{
            'Name': 'association.subnet-id',
            'Values': [subnet]
        }]

        # TODO: Can't we use the resource's network_acls.filter(Filters=)?
        return [item['NetworkAclId'] for item in self._pager(ec2.describe_network_acls,
                                                             'NetworkAcls',
                                                             Filters=filters)]

    @awsRetry
    def _get_default_subnet(self, zone: str) -> str:
        """
        Given an availability zone, get the default subnet for the default VPC
        in that zone.
        """

        # Compose a filter that selects the default subnet in the AZ
        filters = [{
            'Name': 'default-for-az',
            'Values': ['true']
        }, {
            'Name': 'availability-zone',
            'Values': [zone]
        }]

        for subnet in self.aws.resource(zone_to_region(zone), 'ec2').subnets.filter(Filters=filters):
            # There should only be one result, so when we see it, return it
            return subnet.subnet_id
        # If we don't find a subnet, something is wrong. Maybe this zone was
        # added after your account?
        raise RuntimeError(f"No default subnet found in availability zone {zone}. "
                           f"Note that Amazon does not add default subnets for new "
                           f"zones to old accounts. Specify a VPC subnet ID to use, "
                           f"or create a default subnet in the zone.")

    def getKubernetesAutoscalerSetupCommands(self, values: Dict[str, str]) -> str:
        """
        Get the Bash commands necessary to configure the Kubernetes Cluster Autoscaler for AWS.
        """

        return textwrap.dedent('''\
            curl -sSL https://raw.githubusercontent.com/kubernetes/autoscaler/cluster-autoscaler-{AUTOSCALER_VERSION}/cluster-autoscaler/cloudprovider/aws/examples/cluster-autoscaler-run-on-master.yaml | \\
                sed "s|--nodes={{{{ node_asg_min }}}}:{{{{ node_asg_max }}}}:{{{{ name }}}}|--node-group-auto-discovery=asg:tag=k8s.io/cluster-autoscaler/enabled,k8s.io/cluster-autoscaler/{CLUSTER_NAME}|" | \\
                sed 's|kubernetes.io/role: master|node-role.kubernetes.io/master: ""|' | \\
                sed 's|operator: "Equal"|operator: "Exists"|' | \\
                sed '/value: "true"/d' | \\
                sed 's|path: "/etc/ssl/certs/ca-bundle.crt"|path: "/usr/share/ca-certificates/ca-certificates.crt"|' | \\
                kubectl apply -f -
            ''').format(**values)

    def getKubernetesCloudProvider(self) -> Optional[str]:
        """
        Use the "aws" Kubernetes cloud provider when setting up Kubernetes.
        """

        return 'aws'

    def getNodeShape(self, instance_type: str, preemptible=False) -> Shape:
        """
        Get the Shape for the given instance type (e.g. 't2.medium').
        """
        type_info = E2Instances[instance_type]

        disk = type_info.disks * type_info.disk_capacity * 2 ** 30
        if disk == 0:
            # This is an EBS-backed instance. We will use the root
            # volume, so add the amount of EBS storage requested for
            # the root volume
            disk = self._nodeStorageOverrides.get(instance_type, self._nodeStorage) * 2 ** 30

        # Underestimate memory by 100M to prevent autoscaler from disagreeing with
        # mesos about whether a job can run on a particular node type
        memory = (type_info.memory - 0.1) * 2 ** 30
        return Shape(wallTime=60 * 60,
                     memory=memory,
                     cores=type_info.cores,
                     disk=disk,
                     preemptible=preemptible)

    @staticmethod
    def retryPredicate(e):
        return awsRetryPredicate(e)

    def destroyCluster(self) -> None:
        """Terminate instances and delete the profile and security group."""

        # We should terminate the leader first in case a workflow is still running in the cluster.
        # The leader may create more instances while we're terminating the workers.
        vpcId = None
        try:
            leader = self._getLeaderInstance()
            vpcId = leader.vpc_id
            logger.info('Terminating the leader first ...')
            self._terminateInstances([leader])
        except (NoSuchClusterException, InvalidClusterStateException):
            # It's ok if the leader is not found. We'll terminate any remaining
            # instances below anyway.
            pass

        logger.debug('Deleting autoscaling groups ...')
        removed = False

        for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
            with attempt:
                for asgName in self._getAutoScalingGroupNames():
                    try:
                        # We delete the group and all the instances via ForceDelete.
                        self.aws.client(self._region, 'autoscaling').delete_auto_scaling_group(AutoScalingGroupName=asgName, ForceDelete=True)
                        removed = True
                    except ClientError as e:
                        if get_error_code(e) == 'ValidationError' and 'AutoScalingGroup name not found' in get_error_message(e):
                            # This ASG does not need to be removed (or a
                            # previous delete returned an error but also
                            # succeeded).
                            pass

        if removed:
            logger.debug('... Successfully deleted autoscaling groups')

        # Do the workers after the ASGs because some may belong to ASGs
        logger.info('Terminating any remaining workers ...')
        removed = False
        instances = self._get_nodes_in_cluster(include_stopped_nodes=True)
        spotIDs = self._getSpotRequestIDs()
        if spotIDs:
            self.aws.boto2(self._region, 'ec2').cancel_spot_instance_requests(request_ids=spotIDs)
            removed = True
        instancesToTerminate = awsFilterImpairedNodes(instances, self.aws.boto2(self._region, 'ec2'))
        if instancesToTerminate:
            vpcId = vpcId or instancesToTerminate[0].vpc_id
            self._terminateInstances(instancesToTerminate)
            removed = True
        if removed:
            logger.debug('... Successfully terminated workers')

        logger.info('Deleting launch templates ...')
        removed = False
        for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
            with attempt:
                # We'll set this to True if we don't get a proper response
                # for some LuanchTemplate.
                mistake = False
                for ltID in self._get_launch_template_ids():
                    response = self.aws.client(self._region, 'ec2').delete_launch_template(LaunchTemplateId=ltID)
                    if 'LaunchTemplate' not in response:
                        mistake = True
                    else:
                        removed = True
        if mistake:
            # We missed something
            removed = False
        if removed:
            logger.debug('... Successfully deleted launch templates')

        if len(instances) == len(instancesToTerminate):
            # All nodes are gone now.

            logger.info('Deleting IAM roles ...')
            self._deleteRoles(self._getRoleNames())
            self._deleteInstanceProfiles(self._getInstanceProfileNames())

            logger.info('Deleting security group ...')
            removed = False
            for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                with attempt:
                    for sg in self.aws.boto2(self._region, 'ec2').get_all_security_groups():
                        # TODO: If we terminate the leader and the workers but
                        # miss the security group, we won't find it now because
                        # we won't have vpcId set.
                        if sg.name == self.clusterName and vpcId and sg.vpc_id == vpcId:
                            try:
                                self.aws.boto2(self._region, 'ec2').delete_security_group(group_id=sg.id)
                                removed = True
                            except BotoServerError as e:
                                if e.error_code == 'InvalidGroup.NotFound':
                                    pass
                                else:
                                    raise
            if removed:
                logger.debug('... Successfully deleted security group')
        else:
            assert len(instances) > len(instancesToTerminate)
            # the security group can't be deleted until all nodes are terminated
            logger.warning('The TOIL_AWS_NODE_DEBUG environment variable is set and some nodes '
                           'have failed health checks. As a result, the security group & IAM '
                           'roles will not be deleted.')

        # delete S3 buckets that might have been created by `self._write_file_to_cloud()`
        logger.info('Deleting S3 buckets ...')
        removed = False
        for attempt in old_retry(timeout=300, predicate=awsRetryPredicate):
            with attempt:
                # Grab the S3 resource to use
                s3 = self.aws.resource(self._region, 's3')
                try:
                    bucket = s3.Bucket(self.s3_bucket_name)

                    bucket.objects.all().delete()
                    bucket.object_versions.delete()
                    bucket.delete()
                    removed = True
                except s3.meta.client.exceptions.NoSuchBucket:
                    pass
                except ClientError as e:
                    if get_error_status(e) == 404:
                        pass
                    else:
                        raise  # retry this
        if removed:
            print('... Successfully deleted S3 buckets')

    def terminateNodes(self, nodes: List[Node]) -> None:
        if nodes:
            self._terminateIDs([x.name for x in nodes])

    def _recover_node_type_bid(self, node_type: Set[str], spot_bid: Optional[float]) -> Optional[float]:
        """
        The old Toil-managed autoscaler will tell us to make some nodes of
        particular instance types, and to just work out a bid, but it doesn't
        know anything about instance type equivalence classes within a node
        type. So we need to do some work to infer how much to bid by guessing
        some node type an instance type could belong to.

        If we get a set of instance types corresponding to a known node type
        with a bid, we use that bid instead.

        :return: the guessed spot bid
        """
        if spot_bid is None:
            if self._spotBidsMap and frozenset(node_type) in self._spotBidsMap:
                spot_bid = self._spotBidsMap[frozenset(node_type)]
            elif len(node_type) == 1:
                # The Toil autoscaler forgets the equivalence classes. Find
                # some plausible equivalence class.
                instance_type = next(iter(node_type))
                for types, bid in self._spotBidsMap.items():
                    if instance_type in types:
                        # We bid on a class that includes this type
                        spot_bid = bid
                        break
                if spot_bid is None:
                    # We didn't bid on any class including this type either
                    raise RuntimeError("No spot bid given for a preemptible node request.")
            else:
                raise RuntimeError("No spot bid given for a preemptible node request.")

        return spot_bid

    def addNodes(self, nodeTypes: Set[str], numNodes, preemptible, spotBid=None) -> int:
        # Grab the AWS connection we need
        ec2 = self.aws.boto2(self._region, 'ec2')

        assert self._leaderPrivateIP

        if preemptible:
            # May need to provide a spot bid
            spotBid = self._recover_node_type_bid(nodeTypes, spotBid)

        # We don't support any balancing here so just pick one of the
        # equivalent node types
        node_type = next(iter(nodeTypes))
        type_info = E2Instances[node_type]
        root_vol_size = self._nodeStorageOverrides.get(node_type, self._nodeStorage)
        bdm = self._getBoto2BlockDeviceMapping(type_info,
                                               rootVolSize=root_vol_size)

        # Pick a zone and subnet_id to launch into
        if preemptible:
            # We may need to balance preemptible instances across zones, which
            # then affects the subnets they can use.

            # We're allowed to pick from any of these zones.
            zone_options = list(self._worker_subnets_by_zone.keys())

            zone = get_best_aws_zone(spotBid, type_info.name, ec2, zone_options)
        else:
            # We don't need to ever do any balancing across zones for on-demand
            # instances. Just pick a zone.
            if self._zone in self._worker_subnets_by_zone:
                # We can launch into the same zone as the leader
                zone = self._zone
            else:
                # The workers aren't allowed in the leader's zone.
                # Pick an arbitrary zone we can use.
                zone = next(iter(self._worker_subnets_by_zone.keys()))
        if self._leader_subnet in self._worker_subnets_by_zone.get(zone, []):
            # The leader's subnet is an option for this zone, so use it.
            subnet_id = self._leader_subnet
        else:
            # Use an arbitrary subnet from the zone
            subnet_id = next(iter(self._worker_subnets_by_zone[zone]))

        keyPath = self._sseKey if self._sseKey else None
        userData = self._getIgnitionUserData('worker', keyPath, preemptible, self._architecture)
        if isinstance(userData, str):
            # Spot-market provisioning requires bytes for user data.
            userData = userData.encode('utf-8')

        kwargs = {'key_name': self._keyName,
                  'security_group_ids': self._getSecurityGroupIDs(),
                  'instance_type': type_info.name,
                  'user_data': userData,
                  'block_device_map': bdm,
                  'instance_profile_arn': self._leaderProfileArn,
                  'placement': zone,
                  'subnet_id': subnet_id}

        instancesLaunched = []

        for attempt in old_retry(predicate=awsRetryPredicate):
            with attempt:
                # after we start launching instances we want to ensure the full setup is done
                # the biggest obstacle is AWS request throttling, so we retry on these errors at
                # every request in this method
                if not preemptible:
                    logger.debug('Launching %s non-preemptible nodes', numNodes)
                    instancesLaunched = create_ondemand_instances(ec2,
                                                                  image_id=self._discoverAMI(),
                                                                  spec=kwargs, num_instances=numNodes)
                else:
                    logger.debug('Launching %s preemptible nodes', numNodes)
                    # force generator to evaluate
                    instancesLaunched = list(create_spot_instances(ec2=ec2,
                                                                   price=spotBid,
                                                                   image_id=self._discoverAMI(),
                                                                   tags={_TAG_KEY_TOIL_CLUSTER_NAME: self.clusterName},
                                                                   spec=kwargs,
                                                                   num_instances=numNodes,
                                                                   tentative=True)
                                             )
                    # flatten the list
                    instancesLaunched = [item for sublist in instancesLaunched for item in sublist]

        for attempt in old_retry(predicate=awsRetryPredicate):
            with attempt:
                wait_instances_running(ec2, instancesLaunched)

        self._tags[_TAG_KEY_TOIL_NODE_TYPE] = 'worker'
        AWSProvisioner._addTags(instancesLaunched, self._tags)
        if self._sseKey:
            for i in instancesLaunched:
                self._waitForIP(i)
                node = Node(publicIP=i.ip_address, privateIP=i.private_ip_address, name=i.id,
                            launchTime=i.launch_time, nodeType=i.instance_type, preemptible=preemptible,
                            tags=i.tags)
                node.waitForNode('toil_worker')
                node.coreRsync([self._sseKey, ':' + self._sseKey], applianceName='toil_worker')
        logger.debug('Launched %s new instance(s)', numNodes)
        return len(instancesLaunched)

    def addManagedNodes(self, nodeTypes: Set[str], minNodes, maxNodes, preemptible, spotBid=None) -> None:

        if self.clusterType != 'kubernetes':
            raise ManagedNodesNotSupportedException("Managed nodes only supported for Kubernetes clusters")

        assert self._leaderPrivateIP

        if preemptible:
            # May need to provide a spot bid
            spotBid = self._recover_node_type_bid(nodeTypes, spotBid)

        # TODO: We assume we only ever do this once per node type...

        # Make one template per node type, so we can apply storage overrides correctly
        # TODO: deduplicate these if the same instance type appears in multiple sets?
        launch_template_ids = {n: self._get_worker_launch_template(n, preemptible=preemptible) for n in nodeTypes}
        # Make the ASG across all of them
        self._createWorkerAutoScalingGroup(launch_template_ids, nodeTypes, minNodes, maxNodes,
                                           spot_bid=spotBid)

    def getProvisionedWorkers(self, instance_type: Optional[str] = None, preemptible: Optional[bool] = None) -> List[Node]:
        assert self._leaderPrivateIP
        entireCluster = self._get_nodes_in_cluster(instance_type=instance_type)
        logger.debug('All nodes in cluster: %s', entireCluster)
        workerInstances = [i for i in entireCluster if i.private_ip_address != self._leaderPrivateIP]
        logger.debug('All workers found in cluster: %s', workerInstances)
        if preemptible is not None:
            workerInstances = [i for i in workerInstances if preemptible == (i.spot_instance_request_id is not None)]
            logger.debug('%spreemptible workers found in cluster: %s', 'non-' if not preemptible else '', workerInstances)
        workerInstances = awsFilterImpairedNodes(workerInstances, self.aws.boto2(self._region, 'ec2'))
        return [Node(publicIP=i.ip_address, privateIP=i.private_ip_address,
                     name=i.id, launchTime=i.launch_time, nodeType=i.instance_type,
                     preemptible=i.spot_instance_request_id is not None, tags=i.tags)
                for i in workerInstances]

    @memoize
    def _discoverAMI(self) -> str:
        """
        :return: The AMI ID (a string like 'ami-0a9a5d2b65cce04eb') for Flatcar.
        :rtype: str
        """
        return get_flatcar_ami(self.aws.client(self._region, 'ec2'), self._architecture)

    def _toNameSpace(self) -> str:
        assert isinstance(self.clusterName, (str, bytes))
        if any(char.isupper() for char in self.clusterName) or '_' in self.clusterName:
            raise RuntimeError("The cluster name must be lowercase and cannot contain the '_' "
                               "character.")
        namespace = self.clusterName
        if not namespace.startswith('/'):
            namespace = '/' + namespace + '/'
        return namespace.replace('-', '/')

    def _namespace_name(self, name: str) -> str:
        """
        Given a name for a thing, add our cluster name to it in a way that
        results in an acceptable name for something on AWS.
        """

        # This logic is a bit weird, but it's what Boto2Context used to use.
        # Drop the leading / from the absolute-path-style "namespace" name and
        # then encode underscores and slashes.
        return (self._toNameSpace() + name)[1:].replace('_', '__').replace('/', '_')

    def _is_our_namespaced_name(self, namespaced_name: str) -> bool:
        """
        Return True if the given AWS object name looks like it belongs to us
        and was generated by _namespace_name().
        """

        denamespaced = '/' + '_'.join(s.replace('_', '/') for s in namespaced_name.split('__'))
        return denamespaced.startswith(self._toNameSpace())


    def _getLeaderInstance(self) -> Boto2Instance:
        """
        Get the Boto 2 instance for the cluster's leader.
        """
        instances = self._get_nodes_in_cluster(include_stopped_nodes=True)
        instances.sort(key=lambda x: x.launch_time)
        try:
            leader = instances[0]  # assume leader was launched first
        except IndexError:
            raise NoSuchClusterException(self.clusterName)
        if (leader.tags.get(_TAG_KEY_TOIL_NODE_TYPE) or 'leader') != 'leader':
            raise InvalidClusterStateException(
                'Invalid cluster state! The first launched instance appears not to be the leader '
                'as it is missing the "leader" tag. The safest recovery is to destroy the cluster '
                'and restart the job. Incorrect Leader ID: %s' % leader.id
            )
        return leader

    def getLeader(self, wait=False) -> Node:
        """
        Get the leader for the cluster as a Toil Node object.
        """
        leader = self._getLeaderInstance()

        leaderNode = Node(publicIP=leader.ip_address, privateIP=leader.private_ip_address,
                          name=leader.id, launchTime=leader.launch_time, nodeType=None,
                          preemptible=False, tags=leader.tags)
        if wait:
            logger.debug("Waiting for toil_leader to enter 'running' state...")
            wait_instances_running(self.aws.boto2(self._region, 'ec2'), [leader])
            logger.debug('... toil_leader is running')
            self._waitForIP(leader)
            leaderNode.waitForNode('toil_leader')

        return leaderNode

    @classmethod
    @awsRetry
    def _addTag(cls, instance: Boto2Instance, key: str, value: str):
        instance.add_tag(key, value)

    @classmethod
    def _addTags(cls, instances: List[Boto2Instance], tags: Dict[str, str]):
        for instance in instances:
            for key, value in tags.items():
                cls._addTag(instance, key, value)

    @classmethod
    def _waitForIP(cls, instance: Boto2Instance):
        """
        Wait until the instances has a public IP address assigned to it.

        :type instance: boto.ec2.instance.Instance
        """
        logger.debug('Waiting for ip...')
        while True:
            time.sleep(a_short_time)
            instance.update()
            if instance.ip_address or instance.public_dns_name or instance.private_ip_address:
                logger.debug('...got ip')
                break

    def _terminateInstances(self, instances: List[Boto2Instance]):
        instanceIDs = [x.id for x in instances]
        self._terminateIDs(instanceIDs)
        logger.info('... Waiting for instance(s) to shut down...')
        for instance in instances:
            wait_transition(instance, {'pending', 'running', 'shutting-down', 'stopping', 'stopped'}, 'terminated')
        logger.info('Instance(s) terminated.')

    @awsRetry
    def _terminateIDs(self, instanceIDs: List[str]):
        logger.info('Terminating instance(s): %s', instanceIDs)
        self.aws.boto2(self._region, 'ec2').terminate_instances(instance_ids=instanceIDs)
        logger.info('Instance(s) terminated.')

    @awsRetry
    def _deleteRoles(self, names: List[str]):
        """
        Delete all the given named IAM roles.
        Detatches but does not delete associated instance profiles.
        """

        for role_name in names:
            for profile_name in self._getRoleInstanceProfileNames(role_name):
                # We can't delete either the role or the profile while they
                # are attached.

                for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                    with attempt:
                        self.aws.client(self._region, 'iam').remove_role_from_instance_profile(InstanceProfileName=profile_name,
                                                                                               RoleName=role_name)
            # We also need to drop all inline policies
            for policy_name in self._getRoleInlinePolicyNames(role_name):
                for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                    with attempt:
                        self.aws.client(self._region, 'iam').delete_role_policy(PolicyName=policy_name,
                                                                                RoleName=role_name)

            for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                with attempt:
                    self.aws.client(self._region, 'iam').delete_role(RoleName=role_name)
                    logger.debug('... Successfully deleted IAM role %s', role_name)


    @awsRetry
    def _deleteInstanceProfiles(self, names: List[str]):
        """
        Delete all the given named IAM instance profiles.
        All roles must already be detached.
        """

        for profile_name in names:
            for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                with attempt:
                    self.aws.client(self._region, 'iam').delete_instance_profile(InstanceProfileName=profile_name)
                    logger.debug('... Succesfully deleted instance profile %s', profile_name)

    @classmethod
    def _getBoto2BlockDeviceMapping(cls, type_info: InstanceType, rootVolSize: int = 50) -> Boto2BlockDeviceMapping:
        # determine number of ephemeral drives via cgcloud-lib (actually this is moved into toil's lib
        bdtKeys = [''] + [f'/dev/xvd{c}' for c in string.ascii_lowercase[1:]]
        bdm = Boto2BlockDeviceMapping()
        # Change root volume size to allow for bigger Docker instances
        root_vol = Boto2BlockDeviceType(delete_on_termination=True)
        root_vol.size = rootVolSize
        bdm["/dev/xvda"] = root_vol
        # The first disk is already attached for us so start with 2nd.
        # Disk count is weirdly a float in our instance database, so make it an int here.
        for disk in range(1, int(type_info.disks) + 1):
            bdm[bdtKeys[disk]] = Boto2BlockDeviceType(
                ephemeral_name=f'ephemeral{disk - 1}')  # ephemeral counts start at 0

        logger.debug('Device mapping: %s', bdm)
        return bdm

    @classmethod
    def _getBoto3BlockDeviceMappings(cls, type_info: InstanceType, rootVolSize: int = 50) -> List[dict]:
        """
        Get block device mappings for the root volume for a worker.
        """

        # Start with the root
        bdms = [{
            'DeviceName': '/dev/xvda',
            'Ebs': {
                'DeleteOnTermination': True,
                'VolumeSize': rootVolSize,
                'VolumeType': 'gp2'
            }
        }]

        # Get all the virtual drives we might have
        bdtKeys = [f'/dev/xvd{c}' for c in string.ascii_lowercase]

        # The first disk is already attached for us so start with 2nd.
        # Disk count is weirdly a float in our instance database, so make it an int here.
        for disk in range(1, int(type_info.disks) + 1):
            # Make a block device mapping to attach the ephemeral disk to a
            # virtual block device in the VM
            bdms.append({
                'DeviceName': bdtKeys[disk],
                'VirtualName': f'ephemeral{disk - 1}'  # ephemeral counts start at 0
            })
        logger.debug('Device mapping: %s', bdms)
        return bdms

    @awsRetry
    def _get_nodes_in_cluster(self, instance_type: Optional[str] = None, include_stopped_nodes=False) -> List[Boto2Instance]:
        """
        Get Boto2 instance objects for all nodes in the cluster.
        """

        all_instances = self.aws.boto2(self._region, 'ec2').get_only_instances(filters={'instance.group-name': self.clusterName})

        def instanceFilter(i):
            # filter by type only if nodeType is true
            rightType = not instance_type or i.instance_type == instance_type
            rightState = i.state == 'running' or i.state == 'pending'
            if include_stopped_nodes:
                rightState = rightState or i.state == 'stopping' or i.state == 'stopped'
            return rightType and rightState

        return [i for i in all_instances if instanceFilter(i)]

    def _filter_nodes_in_cluster(self, instance_type: Optional[str] = None, preemptible: bool = False) -> List[Boto2Instance]:
        """
        Get Boto2 instance objects for the nodes in the cluster filtered by preemptability.
        """

        instances = self._get_nodes_in_cluster(instance_type, include_stopped_nodes=False)

        if preemptible:
            return [i for i in instances if i.spot_instance_request_id is not None]

        return [i for i in instances if i.spot_instance_request_id is None]

    def _getSpotRequestIDs(self) -> List[str]:
        """
        Get the IDs of all spot requests associated with the cluster.
        """

        # Grab the connection we need to use for this operation.
        ec2 = self.aws.boto2(self._region, 'ec2')

        requests = ec2.get_all_spot_instance_requests()
        tags = ec2.get_all_tags({'tag:': {_TAG_KEY_TOIL_CLUSTER_NAME: self.clusterName}})
        idsToCancel = [tag.id for tag in tags]
        return [request for request in requests if request.id in idsToCancel]

    def _createSecurityGroups(self) -> List[str]:
        """
        Create security groups for the cluster. Returns a list of their IDs.
        """

        # Grab the connection we need to use for this operation.
        # The VPC connection can do anything the EC2 one can do, but also look at subnets.
        vpc = self.aws.boto2(self._region, 'vpc')

        def groupNotFound(e):
            retry = (e.status == 400 and 'does not exist in default VPC' in e.body)
            return retry
        # Security groups need to belong to the same VPC as the leader. If we
        # put the leader in a particular non-default subnet, it may be in a
        # particular non-default VPC, which we need to know about.
        vpcId = None
        if self._leader_subnet:
            subnets = vpc.get_all_subnets(subnet_ids=[self._leader_subnet])
            if len(subnets) > 0:
                vpcId = subnets[0].vpc_id
        # security group create/get. ssh + all ports open within the group
        try:
            web = vpc.create_security_group(self.clusterName,
                                            'Toil appliance security group', vpc_id=vpcId)
        except EC2ResponseError as e:
            if e.status == 400 and 'already exists' in e.body:
                pass  # group exists- nothing to do
            else:
                raise
        else:
            for attempt in old_retry(predicate=groupNotFound, timeout=300):
                with attempt:
                    # open port 22 for ssh-ing
                    web.authorize(ip_protocol='tcp', from_port=22, to_port=22, cidr_ip='0.0.0.0/0')
                    # TODO: boto2 doesn't support IPv6 here but we need to.
            for attempt in old_retry(predicate=groupNotFound, timeout=300):
                with attempt:
                    # the following authorizes all TCP access within the web security group
                    web.authorize(ip_protocol='tcp', from_port=0, to_port=65535, src_group=web)
            for attempt in old_retry(predicate=groupNotFound, timeout=300):
                with attempt:
                    # We also want to open up UDP, both for user code and for the RealtimeLogger
                    web.authorize(ip_protocol='udp', from_port=0, to_port=65535, src_group=web)
        out = []
        for sg in vpc.get_all_security_groups():
            if sg.name == self.clusterName and (vpcId is None or sg.vpc_id == vpcId):
                out.append(sg)
        return [sg.id for sg in out]

    @awsRetry
    def _getSecurityGroupIDs(self) -> List[str]:
        """
        Get all the security group IDs to apply to leaders and workers.
        """

        # TODO: memoize to save requests.

        # Depending on if we enumerated them on the leader or locally, we might
        # know the required security groups by name, ID, or both.
        sgs = [sg for sg in self.aws.boto2(self._region, 'ec2').get_all_security_groups()
               if (sg.name in self._leaderSecurityGroupNames or
                   sg.id in self._leaderSecurityGroupIDs)]
        return [sg.id for sg in sgs]

    @awsRetry
    def _get_launch_template_ids(self, filters: Optional[List[Dict[str, List[str]]]] = None) -> List[str]:
        """
        Find all launch templates associated with the cluster.

        Returns a list of launch template IDs.
        """

        # Grab the connection we need to use for this operation.
        ec2 = self.aws.client(self._region, 'ec2')

        # How do we match the right templates?
        combined_filters = [{'Name': 'tag:' + _TAG_KEY_TOIL_CLUSTER_NAME, 'Values': [self.clusterName]}]

        if filters:
            # Add any user-specified filters
            combined_filters += filters

        allTemplateIDs = []
        # Get the first page with no NextToken
        response = ec2.describe_launch_templates(Filters=combined_filters,
                                                 MaxResults=200)
        while True:
            # Process the current page
            allTemplateIDs += [item['LaunchTemplateId'] for item in response.get('LaunchTemplates', [])]
            if 'NextToken' in response:
                # There are more pages. Get the next one, supplying the token.
                response = ec2.describe_launch_templates(Filters=filters,
                                                         NextToken=response['NextToken'],
                                                         MaxResults=200)
            else:
                # No more pages
                break

        return allTemplateIDs

    @awsRetry
    def _get_worker_launch_template(self, instance_type: str, preemptible: bool = False, backoff: float = 1.0) -> str:
        """
        Get a launch template for instances with the given parameters. Only one
        such launch template will be created, no matter how many times the
        function is called.

        Not thread safe.

        :param instance_type: Type of node to use in the template. May be overridden
                              by an ASG that uses the template.

        :param preemptible: When the node comes up, does it think it is a spot instance?

        :param backoff: How long to wait if it seems like we aren't reading our
                        own writes before trying again.

        :return: The ID of the template.
        """

        lt_name = self._name_worker_launch_template(instance_type, preemptible=preemptible)

        # How do we match the right templates?
        filters = [{'Name': 'launch-template-name', 'Values': [lt_name]}]

        # Get the templates
        templates = self._get_launch_template_ids(filters=filters)

        if len(templates) > 1:
            # There shouldn't ever be multiple templates with our reserved name
            raise RuntimeError(f"Multiple launch templates already exist named {lt_name}; "
                               "something else is operating in our cluster namespace.")
        elif len(templates) == 0:
            # Template doesn't exist so we can create it.
            try:
                return self._create_worker_launch_template(instance_type, preemptible=preemptible)
            except ClientError as e:
                if get_error_code(e) == 'InvalidLaunchTemplateName.AlreadyExistsException':
                    # Someone got to it before us (or we couldn't read our own
                    # writes). Recurse to try again, because now it exists.
                    logger.info('Waiting %f seconds for template %s to be available', backoff, lt_name)
                    time.sleep(backoff)
                    return self._get_worker_launch_template(instance_type, preemptible=preemptible, backoff=backoff*2)
                else:
                    raise
        else:
            # There must be exactly one template
            return templates[0]

    def _name_worker_launch_template(self, instance_type: str, preemptible: bool = False) -> str:
        """
        Get the name we should use for the launch template with the given parameters.

        :param instance_type: Type of node to use in the template. May be overridden
                              by an ASG that uses the template.

        :param preemptible: When the node comes up, does it think it is a spot instance?
        """

        # The name has the cluster name in it
        lt_name = f'{self.clusterName}-lt-{instance_type}'
        if preemptible:
            lt_name += '-spot'

        return lt_name

    def _create_worker_launch_template(self, instance_type: str, preemptible: bool = False) -> str:
        """
        Create the launch template for launching worker instances for the cluster.

        :param instance_type: Type of node to use in the template. May be overridden
                              by an ASG that uses the template.

        :param preemptible: When the node comes up, does it think it is a spot instance?

        :return: The ID of the template created.
        """

        # TODO: If we already have one like this, set its storage and/or remake it.

        assert self._leaderPrivateIP
        type_info = E2Instances[instance_type]
        rootVolSize=self._nodeStorageOverrides.get(instance_type, self._nodeStorage)
        bdms = self._getBoto3BlockDeviceMappings(type_info, rootVolSize=rootVolSize)

        keyPath = self._sseKey if self._sseKey else None
        userData = self._getIgnitionUserData('worker', keyPath, preemptible, self._architecture)

        lt_name = self._name_worker_launch_template(instance_type, preemptible=preemptible)

        # But really we find it by tag
        tags = dict(self._tags)
        tags[_TAG_KEY_TOIL_NODE_TYPE] = 'worker'

        return create_launch_template(self.aws.client(self._region, 'ec2'),
                                      template_name=lt_name,
                                      image_id=self._discoverAMI(),
                                      key_name=self._keyName,
                                      security_group_ids=self._getSecurityGroupIDs(),
                                      instance_type=instance_type,
                                      user_data=userData,
                                      block_device_map=bdms,
                                      instance_profile_arn=self._leaderProfileArn,
                                      tags=tags)

    @awsRetry
    def _getAutoScalingGroupNames(self) -> List[str]:
        """
        Find all auto-scaling groups associated with the cluster.

        Returns a list of ASG IDs. ASG IDs and ASG names are the same things.
        """

        # Grab the connection we need to use for this operation.
        autoscaling = self.aws.client(self._region, 'autoscaling')

        # AWS won't filter ASGs server-side for us in describe_auto_scaling_groups.
        # So we search instances of applied tags for the ASGs they are on.
        # The ASGs tagged with our cluster are our ASGs.
        # The filtering is on different fields of the tag object itself.
        filters = [{'Name': 'key',
                    'Values': [_TAG_KEY_TOIL_CLUSTER_NAME]},
                   {'Name': 'value',
                    'Values': [self.clusterName]}]

        matchedASGs = []
        # Get the first page with no NextToken
        response = autoscaling.describe_tags(Filters=filters)
        while True:
            # Process the current page
            matchedASGs += [item['ResourceId'] for item in response.get('Tags', [])
                            if item['Key'] == _TAG_KEY_TOIL_CLUSTER_NAME and
                            item['Value'] == self.clusterName]
            if 'NextToken' in response:
                # There are more pages. Get the next one, supplying the token.
                response = autoscaling.describe_tags(Filters=filters,
                                                     NextToken=response['NextToken'])
            else:
                # No more pages
                break

        for name in matchedASGs:
            # Double check to make sure we definitely aren't finding non-Toil
            # things
            assert name.startswith('toil-')

        return matchedASGs

    def _createWorkerAutoScalingGroup(self,
                                      launch_template_ids: Dict[str, str],
                                      instance_types: Collection[str],
                                      min_size: int,
                                      max_size: int,
                                      spot_bid: Optional[float] = None) -> str:
        """
        Create an autoscaling group.

        :param launch_template_ids: ID of the launch template to use for
               each instance type name.
        :param instance_types: Names of instance types to use. Must have
               at least one. Needed here to calculate the ephemeral storage
               provided. The instance type used to create the launch template
               must be present, for correct storage space calculation.
        :param min_size: Minimum number of instances to scale to.
        :param max_size: Maximum number of instances to scale to.
        :param spot_bid: Make this a spot ASG with the given bid.

        :return: the unique autoscaling group name.

        TODO: allow overriding launch template and pooling.
        """

        assert self._leaderPrivateIP

        assert len(instance_types) >= 1

        # Find the minimum storage any instance in the group will provide.
        # For each, we look at the root volume size we would assign it if it were the type used to make the template.
        # TODO: Work out how to apply each instance type's root volume size override independently when they're all in a pool.
        storage_gigs = []
        for instance_type in instance_types:
            spec = E2Instances[instance_type]
            spec_gigs = spec.disks * spec.disk_capacity
            rootVolSize = self._nodeStorageOverrides.get(instance_type, self._nodeStorage)
            storage_gigs.append(max(rootVolSize - _STORAGE_ROOT_OVERHEAD_GIGS, spec_gigs))
        # Get the min storage we expect to see, but not less than 0.
        min_gigs = max(min(storage_gigs), 0)

        # Make tags. These are just for the ASG, not for the node.
        # If are a Kubernetes cluster, this includes the tag for membership.
        tags = dict(self._tags)

        # We tag the ASG with the Toil type, although nothing cares.
        tags[_TAG_KEY_TOIL_NODE_TYPE] = 'worker'

        if self.clusterType == 'kubernetes':
            # We also need to tag it with Kubernetes autoscaler info (empty tags)
            tags['k8s.io/cluster-autoscaler/' + self.clusterName] = ''
            assert(self.clusterName != 'enabled')
            tags['k8s.io/cluster-autoscaler/enabled'] = ''
            tags['k8s.io/cluster-autoscaler/node-template/resources/ephemeral-storage'] = f'{min_gigs}G'

        # Now we need to make up a unique name
        # TODO: can we make this more semantic without risking collisions? Maybe count up in memory?
        asg_name = 'toil-' + str(uuid.uuid4())

        create_auto_scaling_group(self.aws.client(self._region, 'autoscaling'),
                                  asg_name=asg_name,
                                  launch_template_ids=launch_template_ids,
                                  vpc_subnets=self._get_worker_subnets(),
                                  min_size=min_size,
                                  max_size=max_size,
                                  instance_types=instance_types,
                                  spot_bid=spot_bid,
                                  tags=tags)

        return asg_name

    def _boto2_pager(self, requestor_callable: Callable, result_attribute_name: str) -> Iterable[Dict[str, Any]]:
        """
        Yield all the results from calling the given Boto 2 method and paging
        through all the results using the "marker" field. Results are to be
        found in the field with the given name in the AWS responses.
        """
        marker = None
        while True:
            result = requestor_callable(marker=marker)
            yield from getattr(result, result_attribute_name)
            if result.is_truncated == 'true':
                marker = result.marker
            else:
                break

    def _pager(self, requestor_callable: Callable, result_attribute_name: str, **kwargs) -> Iterable[Dict[str, Any]]:
        """
        Yield all the results from calling the given Boto 3 method with the
        given keyword arguments, paging through the results using the Marker or
        NextToken, and fetching out and looping over the list in the response
        with the given attribute name.
        """

        # Recover the Boto3 client, and the name of the operation
        client = requestor_callable.__self__
        op_name = requestor_callable.__name__

        # grab a Boto 3 built-in paginator. See
        # <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html>
        paginator = client.get_paginator(op_name)

        for page in paginator.paginate(**kwargs):
            # Invoke it and go through the pages, yielding from them
            yield from page.get(result_attribute_name, [])

    @awsRetry
    def _getRoleNames(self) -> List[str]:
        """
        Get all the IAM roles belonging to the cluster, as names.
        """

        results = []
        for result in self._boto2_pager(self.aws.boto2(self._region, 'iam').list_roles, 'roles'):
            # For each Boto2 role object
            # Grab out the name
            name = result['role_name']
            if self._is_our_namespaced_name(name):
                # If it looks like ours, it is ours.
                results.append(name)
        return results

    @awsRetry
    def _getInstanceProfileNames(self) -> List[str]:
        """
        Get all the instance profiles belonging to the cluster, as names.
        """

        results = []
        for result in self._boto2_pager(self.aws.boto2(self._region, 'iam').list_instance_profiles,
                                        'instance_profiles'):
            # For each Boto2 role object
            # Grab out the name
            name = result['instance_profile_name']
            if self._is_our_namespaced_name(name):
                # If it looks like ours, it is ours.
                results.append(name)
        return results

    @awsRetry
    def _getRoleInstanceProfileNames(self, role_name: str) -> List[str]:
        """
        Get all the instance profiles with the IAM role with the given name.

        Returns instance profile names.
        """

        # Grab the connection we need to use for this operation.
        iam = self.aws.client(self._region, 'iam')

        return [item['InstanceProfileName'] for item in self._pager(iam.list_instance_profiles_for_role,
                                                                    'InstanceProfiles',
                                                                    RoleName=role_name)]

    @awsRetry
    def _getRolePolicyArns(self, role_name: str) -> List[str]:
        """
        Get all the policies attached to the IAM role with the given name.

        These do not include inline policies on the role.

        Returns policy ARNs.
        """

        # Grab the connection we need to use for this operation.
        iam = self.aws.client(self._region, 'iam')

        # TODO: we don't currently use attached policies.

        return [item['PolicyArn'] for item in self._pager(iam.list_attached_role_policies,
                                                          'AttachedPolicies',
                                                          RoleName=role_name)]

    @awsRetry
    def _getRoleInlinePolicyNames(self, role_name: str) -> List[str]:
        """
        Get all the policies inline in the given IAM role.
        Returns policy names.
        """

        # Grab the connection we need to use for this operation.
        iam = self.aws.client(self._region, 'iam')

        return list(self._pager(iam.list_role_policies,
                                'PolicyNames',
                                RoleName=role_name))

    def full_policy(self, resource: str) -> dict:
        """
        Produce a dict describing the JSON form of a full-access-granting AWS
        IAM policy for the service with the given name (e.g. 's3').
        """
        return dict(Version="2012-10-17", Statement=[dict(Effect="Allow", Resource="*", Action=f"{resource}:*")])

    def kubernetes_policy(self) -> dict:
        """
        Get the Kubernetes policy grants not provided by the full grants on EC2
        and IAM. See
        <https://github.com/DataBiosphere/toil/wiki/Manual-Autoscaling-Kubernetes-Setup#leader-policy>
        and
        <https://github.com/DataBiosphere/toil/wiki/Manual-Autoscaling-Kubernetes-Setup#worker-policy>.

        These are mostly needed to support Kubernetes' AWS CloudProvider, and
        some are for the Kubernetes Cluster Autoscaler's AWS integration.

        Some of these are really only needed on the leader.
        """

        return dict(Version="2012-10-17", Statement=[dict(Effect="Allow", Resource="*", Action=[
            "ecr:GetAuthorizationToken",
            "ecr:BatchCheckLayerAvailability",
            "ecr:GetDownloadUrlForLayer",
            "ecr:GetRepositoryPolicy",
            "ecr:DescribeRepositories",
            "ecr:ListImages",
            "ecr:BatchGetImage",
            "autoscaling:DescribeAutoScalingGroups",
            "autoscaling:DescribeAutoScalingInstances",
            "autoscaling:DescribeLaunchConfigurations",
            "autoscaling:DescribeTags",
            "autoscaling:SetDesiredCapacity",
            "autoscaling:TerminateInstanceInAutoScalingGroup",
            "elasticloadbalancing:AddTags",
            "elasticloadbalancing:ApplySecurityGroupsToLoadBalancer",
            "elasticloadbalancing:AttachLoadBalancerToSubnets",
            "elasticloadbalancing:ConfigureHealthCheck",
            "elasticloadbalancing:CreateListener",
            "elasticloadbalancing:CreateLoadBalancer",
            "elasticloadbalancing:CreateLoadBalancerListeners",
            "elasticloadbalancing:CreateLoadBalancerPolicy",
            "elasticloadbalancing:CreateTargetGroup",
            "elasticloadbalancing:DeleteListener",
            "elasticloadbalancing:DeleteLoadBalancer",
            "elasticloadbalancing:DeleteLoadBalancerListeners",
            "elasticloadbalancing:DeleteTargetGroup",
            "elasticloadbalancing:DeregisterInstancesFromLoadBalancer",
            "elasticloadbalancing:DeregisterTargets",
            "elasticloadbalancing:DescribeListeners",
            "elasticloadbalancing:DescribeLoadBalancerAttributes",
            "elasticloadbalancing:DescribeLoadBalancerPolicies",
            "elasticloadbalancing:DescribeLoadBalancers",
            "elasticloadbalancing:DescribeTargetGroups",
            "elasticloadbalancing:DescribeTargetHealth",
            "elasticloadbalancing:DetachLoadBalancerFromSubnets",
            "elasticloadbalancing:ModifyListener",
            "elasticloadbalancing:ModifyLoadBalancerAttributes",
            "elasticloadbalancing:ModifyTargetGroup",
            "elasticloadbalancing:RegisterInstancesWithLoadBalancer",
            "elasticloadbalancing:RegisterTargets",
            "elasticloadbalancing:SetLoadBalancerPoliciesForBackendServer",
            "elasticloadbalancing:SetLoadBalancerPoliciesOfListener",
            "kms:DescribeKey"
        ])])

    def _setup_iam_ec2_role(self, local_role_name: str, policies: Dict[str, Any]) -> str:
        """
        Create an IAM role with the given policies, using the given name in
        addition to the cluster name, and return its full name.
        """

        # Grab the connection we need to use for this operation.
        iam = self.aws.boto2(self._region, 'iam')

        # Make sure we can tell our roles apart from roles for other clusters
        aws_role_name = self._namespace_name(local_role_name)
        try:
            # Make the role
            logger.debug('Creating IAM role %s...', aws_role_name)
            iam.create_role(aws_role_name, assume_role_policy_document=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": ["ec2.amazonaws.com"]},
                    "Action": ["sts:AssumeRole"]}
                ]}))
            logger.debug('Created new IAM role')
        except BotoServerError as e:
            if e.status == 409 and e.error_code == 'EntityAlreadyExists':
                logger.debug('IAM role already exists. Reusing.')
            else:
                raise

        # Delete superfluous policies
        policy_names = set(iam.list_role_policies(aws_role_name).policy_names)
        for policy_name in policy_names.difference(set(list(policies.keys()))):
            iam.delete_role_policy(aws_role_name, policy_name)

        # Create expected policies
        for policy_name, policy in policies.items():
            current_policy = None
            try:
                current_policy = json.loads(unquote(
                    iam.get_role_policy(aws_role_name, policy_name).policy_document))
            except BotoServerError as e:
                if e.status == 404 and e.error_code == 'NoSuchEntity':
                    pass
                else:
                    raise
            if current_policy != policy:
                iam.put_role_policy(aws_role_name, policy_name, json.dumps(policy))

        # Now the role has the right policies so it is ready.
        return aws_role_name

    @awsRetry
    def _createProfileArn(self) -> str:
        """
        Create an IAM role and instance profile that grants needed permissions
        for cluster leaders and workers. Naming is specific to the cluster.

        Returns its ARN.
        """

        # Grab the connection we need to use for this operation.
        iam = self.aws.boto2(self._region, 'iam')

        policy = dict(iam_full=self.full_policy('iam'), ec2_full=self.full_policy('ec2'),
                      s3_full=self.full_policy('s3'), sbd_full=self.full_policy('sdb'))
        if self.clusterType == 'kubernetes':
            # We also need autoscaling groups and some other stuff for AWS-Kubernetes integrations.
            # TODO: We use one merged policy for leader and worker, but we could be more specific.
            policy['kubernetes_merged'] = self.kubernetes_policy()
        iamRoleName = self._setup_iam_ec2_role(_INSTANCE_PROFILE_ROLE_NAME, policy)

        try:
            profile = iam.get_instance_profile(iamRoleName)
            logger.debug("Have preexisting instance profile: %s", profile.get_instance_profile_response.get_instance_profile_result.instance_profile)
        except BotoServerError as e:
            if e.status == 404:
                profile = iam.create_instance_profile(iamRoleName)
                profile = profile.create_instance_profile_response.create_instance_profile_result
                logger.debug("Created new instance profile: %s", profile.instance_profile)
            else:
                raise
        else:
            profile = profile.get_instance_profile_response.get_instance_profile_result
        profile = profile.instance_profile

        profile_arn = profile.arn

        # Now we have the profile ARN, but we want to make sure it really is
        # visible by name in a different session.
        wait_until_instance_profile_arn_exists(profile_arn)

        if len(profile.roles) > 1:
            # This is too many roles. We probably grabbed something we should
            # not have by mistake, and this is some important profile for
            # something else.
            raise RuntimeError(f'Did not expect instance profile {profile_arn} to contain '
                               f'more than one role; is it really a Toil-managed profile?')
        elif len(profile.roles) == 1:
            # this should be profile.roles[0].role_name
            if profile.roles.member.role_name == iamRoleName:
                return profile_arn
            else:
                # Drop this wrong role and use the fallback code for 0 roles
                iam.remove_role_from_instance_profile(iamRoleName,
                                                      profile.roles.member.role_name)

        # If we get here, we had 0 roles on the profile, or we had 1 but we removed it.
        for attempt in old_retry(predicate=lambda err: err.status == 404):
            with attempt:
                # Put the IAM role on the profile
                iam.add_role_to_instance_profile(profile.instance_profile_name, iamRoleName)
                logger.debug("Associated role %s with profile", iamRoleName)

        return profile_arn
