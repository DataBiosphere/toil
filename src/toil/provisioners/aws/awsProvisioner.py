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
import logging
import os
import socket
import string
import textwrap
import time
import uuid

from botocore.exceptions import ClientError
import boto.ec2

from typing import List, Dict, Optional, Set, Collection
from functools import wraps
from boto.ec2.blockdevicemapping import BlockDeviceMapping as Boto2BlockDeviceMapping, BlockDeviceType as Boto2BlockDeviceType
from boto.exception import BotoServerError, EC2ResponseError
from boto.utils import get_instance_metadata
from boto.ec2.instance import Instance as Boto2Instance

from toil.lib.aws.utils import create_s3_bucket
from toil.lib.conversions import human2bytes
from toil.lib.ec2 import (a_short_time,
                          create_auto_scaling_group,
                          create_instances,
                          create_launch_template,
                          create_ondemand_instances,
                          create_spot_instances,
                          establish_boto3_session,
                          wait_instances_running,
                          wait_transition)
from toil.lib.aws import zone_to_region
from toil.lib.ec2nodes import InstanceType
from toil.lib.generatedEC2Lists import E2Instances
from toil.lib.ec2 import get_flatcar_ami
from toil.lib.memoize import memoize
from toil.lib.misc import truncExpBackoff
from toil.lib.retry import (get_error_body,
                            get_error_code,
                            get_error_message,
                            get_error_status,
                            old_retry,
                            retry,
                            ErrorCondition)
from toil.provisioners import NoSuchClusterException
from toil.provisioners.abstractProvisioner import (AbstractProvisioner,
                                                   Shape,
                                                   ManagedNodesNotSupportedException)
from toil.provisioners.aws import get_current_aws_zone
from toil.provisioners.aws.boto2Context import Boto2Context
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


def expectedShutdownErrors(e):
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
        zone = zone if zone else get_current_aws_zone()

        if zone is None:
            # Can't proceed without a real zone
            raise RuntimeError('No AWS availability zone specified. Configure in Boto '
                               'configuration file, TOIL_AWS_ZONE environment variable, or '
                               'on the command line.')

        # establish boto3 clients
        self.session = establish_boto3_session(region_name=zone_to_region(zone))
        # Boto3 splits functionality between a "resource" and a "client" for the same AWS aspect.
        self.ec2_resource = self.session.resource('ec2')
        self.ec2_client = self.session.client('ec2')
        self.autoscaling_client = self.session.client('autoscaling')
        self.iam_client = self.session.client('iam')
        self.s3_resource = self.session.resource('s3')
        self.s3_client = self.session.client('s3')

        # Call base class constructor, which will call createClusterSettings()
        # or readClusterSettings()
        super(AWSProvisioner, self).__init__(clusterName, clusterType, zone, nodeStorage, nodeStorageOverrides)

        # After self.clusterName is set, generate a valid name for the S3 bucket associated with this cluster
        suffix = _S3_BUCKET_INTERNAL_SUFFIX
        self.s3_bucket_name = self.clusterName[:_S3_BUCKET_MAX_NAME_LEN - len(suffix)] + suffix

    def supportedClusterTypes(self):
        return {'mesos', 'kubernetes'}

    def createClusterSettings(self):
        # All we need to do for a new cluster is build the context and fill in
        # self._boto2
        self._buildContext()

    def readClusterSettings(self):
        """
        Reads the cluster settings from the instance metadata, which assumes the instance
        is the leader.
        """
        instanceMetaData = get_instance_metadata()
        region = zone_to_region(self._zone)
        conn = boto.ec2.connect_to_region(region)
        instance = conn.get_all_instances(instance_ids=[instanceMetaData["instance-id"]])[0].instances[0]
        # The cluster name is the same as the name of the leader.
        self.clusterName = str(instance.tags["Name"])
        self._buildContext()
        # This is where we will put the workers.
        # See also: self._vpcSubnet
        self._subnetID = instance.subnet_id
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
        region = zone_to_region(self._zone)

        # create bucket if needed, then write file to S3
        try:
            # the head_bucket() call makes sure that the bucket exists and the user can access it
            self.s3_client.head_bucket(Bucket=bucket_name)
            bucket = self.s3_resource.Bucket(bucket_name)
        except ClientError as err:
            if err.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                bucket = create_s3_bucket(self.s3_resource, bucket_name=bucket_name, region=region)
                bucket.wait_until_exists()
                bucket.Versioning().enable()

                owner_tag = os.environ.get('TOIL_OWNER_TAG')
                if owner_tag:
                    bucket_tagging = self.s3_resource.BucketTagging(bucket_name)
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
        obj = self.s3_resource.Object(bucket_name, key)

        try:
            return obj.get().get('Body').read()
        except ClientError as e:
            if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
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
                      userTags: dict,
                      vpcSubnet: str,
                      awsEc2ProfileArn: str,
                      awsEc2ExtraSecurityGroupIds: list):
        """
        Starts a single leader node and populates this class with the leader's metadata.

        :param leaderNodeType: An AWS instance type, like "t2.medium", for example.
        :param leaderStorage: An integer number of gigabytes to provide the leader instance with.
        :param owner: Resources will be tagged with this owner string.
        :param keyName: The ssh key to use to access the leader node.
        :param botoPath: The path to the boto credentials directory.
        :param userTags: Optionally provided user tags to put on the cluster.
        :param vpcSubnet: Optionally specify the VPC subnet.
        :param awsEc2ProfileArn: Optionally provide the profile ARN.
        :param awsEc2ExtraSecurityGroupIds: Optionally provide additional security group IDs.
        :return: None
        """

        leader_type = E2Instances[leaderNodeType]

        if self.clusterType == 'kubernetes':
            if leader_type.cores < 2:
                # Kubernetes won't run here.
                raise RuntimeError('Kubernetes requires 2 or more cores, and %s is too small' %
                                   leaderNodeType)

        self._keyName = keyName
        # This is where we put the leader
        self._vpcSubnet = vpcSubnet

        profileArn = awsEc2ProfileArn or self._createProfileArn()
        # the security group name is used as the cluster identifier
        createdSGs = self._createSecurityGroups()
        bdms = self._getBoto3BlockDeviceMappings(leader_type, rootVolSize=leaderStorage)

        userData = self._getCloudConfigUserData('leader')

        # Make up the tags
        self._tags = {'Name': self.clusterName,
                      'Owner': owner,
                      _TAG_KEY_TOIL_CLUSTER_NAME: self.clusterName}

        if self.clusterType == 'kubernetes':
            # All nodes need a tag putting them in the cluster.
            # This tag needs to be on there before the a leader can finish its startup.
            self._tags['kubernetes.io/cluster/' + self.clusterName] = ''

        self._tags.update(userTags)

        # Make tags for the leader specifically
        leader_tags = dict(self._tags)
        leader_tags[_TAG_KEY_TOIL_NODE_TYPE] = 'leader'

        instances = create_instances(self.ec2_resource,
                                     image_id=self._discoverAMI(),
                                     num_instances=1,
                                     key_name=self._keyName,
                                     security_group_ids=createdSGs + awsEc2ExtraSecurityGroupIds,
                                     instance_type=leader_type.name,
                                     user_data=userData,
                                     block_device_map=bdms,
                                     instance_profile_arn=profileArn,
                                     placement_az=self._zone,
                                     subnet_id=self._vpcSubnet,
                                     tags=leader_tags)

        # wait for the leader to exist at all
        leader = instances[0]
        leader.wait_until_exists()

        # Don't go on until the leader is started
        leader.wait_until_running()

        # Now reload it to make sure all the IPs are set.
        leader.reload()

        if leader.public_ip_address is None:
            raise RuntimeError("AWS did not assign a public IP to the cluster leader! Leader is lost!")

        # Remember enough about the leader to let us launch workers in its
        # cluster.
        self._leaderPrivateIP = leader.private_ip_address
        # This is where we will put the workers.
        # See also: self._vpcSubnet
        self._subnetID = leader.subnet_id
        self._leaderSecurityGroupNames = set()
        self._leaderSecurityGroupIDs = set(createdSGs + awsEc2ExtraSecurityGroupIds)
        self._leaderProfileArn = profileArn

        leaderNode = Node(publicIP=leader.public_ip_address, privateIP=leader.private_ip_address,
                          name=leader.id, launchTime=leader.launch_time,
                          nodeType=leader_type.name, preemptable=False,
                          tags=leader.tags)
        leaderNode.waitForNode('toil_leader')

        # Download credentials
        self._setLeaderWorkerAuthentication(leaderNode)

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

    def getNodeShape(self, instance_type: str, preemptable=False) -> Shape:
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
                     preemptable=preemptable)

    @staticmethod
    def retryPredicate(e):
        return awsRetryPredicate(e)

    def destroyCluster(self):
        """
        Terminate instances and delete the profile and security group.
        """
        assert self._boto2

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
                    # We delete the group and all the instances via ForceDelete.
                    self.autoscaling_client.delete_auto_scaling_group(AutoScalingGroupName=asgName, ForceDelete=True)
                    removed = True
        if removed:
            logger.debug('... Successfully deleted autoscaling groups')

        # Do the workers after the ASGs because some may belong to ASGs
        logger.info('Terminating any remaining workers ...')
        removed = False
        instances = self._getNodesInCluster(both=True)
        spotIDs = self._getSpotRequestIDs()
        if spotIDs:
            self._boto2.ec2.cancel_spot_instance_requests(request_ids=spotIDs)
            removed = True
        instancesToTerminate = awsFilterImpairedNodes(instances, self._boto2.ec2)
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
                    response = self.ec2_client.delete_launch_template(LaunchTemplateId=ltID)
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
                    for sg in self._boto2.ec2.get_all_security_groups():
                        if sg.name == self.clusterName and vpcId and sg.vpc_id == vpcId:
                            try:
                                self._boto2.ec2.delete_security_group(group_id=sg.id)
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
                try:
                    bucket = self.s3_resource.Bucket(self.s3_bucket_name)

                    bucket.objects.all().delete()
                    bucket.object_versions.delete()
                    bucket.delete()
                    removed = True
                except self.s3_client.exceptions.NoSuchBucket:
                    pass
                except ClientError as e:
                    if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                        pass
                    else:
                        raise  # retry this
        if removed:
            print('... Successfully deleted S3 buckets')

    def terminateNodes(self, nodes : List[Node]):
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
                    raise RuntimeError("No spot bid given for a preemptable node request.")
            else:
                raise RuntimeError("No spot bid given for a preemptable node request.")

        return spot_bid

    def addNodes(self, nodeTypes: Set[str], numNodes, preemptable, spotBid=None) -> int:
        assert self._leaderPrivateIP

        if preemptable:
            # May need to provide a spot bid
            spotBid = self._recover_node_type_bid(nodeTypes, spotBid)

        # We don't support any balancing here so just pick one of the
        # equivalent node types
        node_type = next(iter(nodeTypes))

        type_info = E2Instances[node_type]
        root_vol_size = self._nodeStorageOverrides.get(node_type, self._nodeStorage)
        bdm = self._getBoto2BlockDeviceMapping(type_info,
                                               rootVolSize=root_vol_size)

        keyPath = self._sseKey if self._sseKey else None
        userData = self._getCloudConfigUserData('worker', keyPath, preemptable)
        if isinstance(userData, str):
            # Spot-market provisioning requires bytes for user data.
            userData = userData.encode('utf-8')

        kwargs = {'key_name': self._keyName,
                  'security_group_ids': self._getSecurityGroupIDs(),
                  'instance_type': type_info.name,
                  'user_data': userData,
                  'block_device_map': bdm,
                  'instance_profile_arn': self._leaderProfileArn,
                  'placement': self._zone,
                  'subnet_id': self._subnetID}

        instancesLaunched = []

        for attempt in old_retry(predicate=awsRetryPredicate):
            with attempt:
                # after we start launching instances we want to ensure the full setup is done
                # the biggest obstacle is AWS request throttling, so we retry on these errors at
                # every request in this method
                if not preemptable:
                    logger.debug('Launching %s non-preemptable nodes', numNodes)
                    instancesLaunched = create_ondemand_instances(self._boto2.ec2, image_id=self._discoverAMI(),
                                                                  spec=kwargs, num_instances=numNodes)
                else:
                    logger.debug('Launching %s preemptable nodes', numNodes)
                    kwargs['placement'] = get_current_aws_zone(spotBid, type_info.name, self._boto2)
                    # force generator to evaluate
                    instancesLaunched = list(create_spot_instances(ec2=self._boto2.ec2,
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
                wait_instances_running(self._boto2.ec2, instancesLaunched)

        self._tags[_TAG_KEY_TOIL_NODE_TYPE] = 'worker'
        AWSProvisioner._addTags(instancesLaunched, self._tags)
        if self._sseKey:
            for i in instancesLaunched:
                self._waitForIP(i)
                node = Node(publicIP=i.ip_address, privateIP=i.private_ip_address, name=i.id,
                            launchTime=i.launch_time, nodeType=i.instance_type, preemptable=preemptable,
                            tags=i.tags)
                node.waitForNode('toil_worker')
                node.coreRsync([self._sseKey, ':' + self._sseKey], applianceName='toil_worker')
        logger.debug('Launched %s new instance(s)', numNodes)
        return len(instancesLaunched)

    def addManagedNodes(self, nodeTypes: Set[str], minNodes, maxNodes, preemptable, spotBid=None) -> None:

        if self.clusterType != 'kubernetes':
            raise ManagedNodesNotSupportedException("Managed nodes only supported for Kubernetes clusters")

        assert self._leaderPrivateIP

        if preemptable:
            # May need to provide a spot bid
            spotBid = self._recover_node_type_bid(nodeTypes, spotBid)

        # TODO: We assume we only ever do this once per node type...

        # Make one template per node type, so we can apply storage overrides correctly
        # TODO: deduplicate these if the same instance type appears in multiple sets?
        launch_template_ids = {n: self._get_worker_launch_template(n, preemptable=preemptable) for n in nodeTypes}
        # Make the ASG across all of them
        self._createWorkerAutoScalingGroup(launch_template_ids, nodeTypes, minNodes, maxNodes,
                                           spot_bid=spotBid)

    def getProvisionedWorkers(self, instance_type: Optional[str] = None, preemptable: Optional[bool] = None) -> List[Node]:
        assert self._leaderPrivateIP
        entireCluster = self._getNodesInCluster(instance_type=instance_type, both=True)
        logger.debug('All nodes in cluster: %s', entireCluster)
        workerInstances = [i for i in entireCluster if i.private_ip_address != self._leaderPrivateIP]
        logger.debug('All workers found in cluster: %s', workerInstances)
        if preemptable is not None:
            workerInstances = [i for i in workerInstances if preemptable == (i.spot_instance_request_id is not None)]
            logger.debug('%spreemptable workers found in cluster: %s', 'non-' if not preemptable else '', workerInstances)
        workerInstances = awsFilterImpairedNodes(workerInstances, self._boto2.ec2)
        return [Node(publicIP=i.ip_address, privateIP=i.private_ip_address,
                     name=i.id, launchTime=i.launch_time, nodeType=i.instance_type,
                     preemptable=i.spot_instance_request_id is not None, tags=i.tags)
                for i in workerInstances]

    def _buildContext(self):
        if self._zone is None:
            self._zone = get_current_aws_zone()
            if self._zone is None:
                raise RuntimeError(
                    'Could not determine availability zone. Ensure that one of the following '
                    'is true: the --zone flag is set, the TOIL_AWS_ZONE environment variable '
                    'is set, ec2_region_name is set in the .boto file, or that '
                    'you are running on EC2.')
        logger.debug("Building AWS context in zone %s for cluster %s" % (self._zone, self.clusterName))
        self._boto2 = Boto2Context(availability_zone=self._zone, namespace=self._toNameSpace())

    @memoize
    def _discoverAMI(self) -> str:
        """
        :return: The AMI ID (a string like 'ami-0a9a5d2b65cce04eb') for Flatcar.
        :rtype: str
        """
        return get_flatcar_ami(self.ec2_client)

    def _toNameSpace(self) -> str:
        assert isinstance(self.clusterName, (str, bytes))
        if any((char.isupper() for char in self.clusterName)) or '_' in self.clusterName:
            raise RuntimeError("The cluster name must be lowercase and cannot contain the '_' "
                               "character.")
        namespace = self.clusterName
        if not namespace.startswith('/'):
            namespace = '/' + namespace + '/'
        return namespace.replace('-', '/')

    def _getLeaderInstance(self) -> Boto2Instance:
        """
        Get the Boto 2 instance for the cluster's leader.
        """
        assert self._boto2
        instances = self._getNodesInCluster(both=True)
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
        assert self._boto2
        leader = self._getLeaderInstance()

        leaderNode = Node(publicIP=leader.ip_address, privateIP=leader.private_ip_address,
                          name=leader.id, launchTime=leader.launch_time, nodeType=None,
                          preemptable=False, tags=leader.tags)
        if wait:
            logger.debug("Waiting for toil_leader to enter 'running' state...")
            wait_instances_running(self._boto2.ec2, [leader])
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
            wait_transition(instance, {'pending', 'running', 'shutting-down'}, 'terminated')
        logger.info('Instance(s) terminated.')

    @awsRetry
    def _terminateIDs(self, instanceIDs: List[str]):
        assert self._boto2
        logger.info('Terminating instance(s): %s', instanceIDs)
        self._boto2.ec2.terminate_instances(instance_ids=instanceIDs)
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
                        self.iam_client.remove_role_from_instance_profile(InstanceProfileName=profile_name,
                                                                          RoleName=role_name)
            # We also need to drop all inline policies
            for policy_name in self._getRoleInlinePolicyNames(role_name):
                for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                    with attempt:
                        self.iam_client.delete_role_policy(PolicyName=policy_name,
                                                           RoleName=role_name)

            for attempt in old_retry(timeout=300, predicate=expectedShutdownErrors):
                with attempt:
                    self.iam_client.delete_role(RoleName=role_name)
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
                    self.iam_client.delete_instance_profile(InstanceProfileName=profile_name)
                    logger.debug('... Succesfully deleted instance profile %s', profile_name)

    @classmethod
    def _getBoto2BlockDeviceMapping(cls, type_info: InstanceType, rootVolSize: int = 50) -> Boto2BlockDeviceMapping:
        # determine number of ephemeral drives via cgcloud-lib (actually this is moved into toil's lib
        bdtKeys = [''] + ['/dev/xvd{}'.format(c) for c in string.ascii_lowercase[1:]]
        bdm = Boto2BlockDeviceMapping()
        # Change root volume size to allow for bigger Docker instances
        root_vol = Boto2BlockDeviceType(delete_on_termination=True)
        root_vol.size = rootVolSize
        bdm["/dev/xvda"] = root_vol
        # The first disk is already attached for us so start with 2nd.
        # Disk count is weirdly a float in our instance database, so make it an int here.
        for disk in range(1, int(type_info.disks) + 1):
            bdm[bdtKeys[disk]] = Boto2BlockDeviceType(
                ephemeral_name='ephemeral{}'.format(disk - 1))  # ephemeral counts start at 0

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
        bdtKeys = ['/dev/xvd{}'.format(c) for c in string.ascii_lowercase]

        # The first disk is already attached for us so start with 2nd.
        # Disk count is weirdly a float in our instance database, so make it an int here.
        for disk in range(1, int(type_info.disks) + 1):
            # Make a block device mapping to attach the ephemeral disk to a
            # virtual block device in the VM
            bdms.append({
                'DeviceName': bdtKeys[disk],
                'VirtualName': 'ephemeral{}'.format(disk - 1)  # ephemeral counts start at 0
            })
        logger.debug('Device mapping: %s', bdms)
        return bdms

    @awsRetry
    def _getNodesInCluster(self, instance_type: Optional[str] = None, preemptable=False, both=False) -> List[Boto2Instance]:
        """
        Get Boto2 instance objects for all nodes in the cluster.
        """
        assert self._boto2
        allInstances = self._boto2.ec2.get_only_instances(filters={'instance.group-name': self.clusterName})
        def instanceFilter(i):
            # filter by type only if nodeType is true
            rightType = not instance_type or i.instance_type == instance_type
            rightState = i.state == 'running' or i.state == 'pending'
            return rightType and rightState
        filteredInstances = [i for i in allInstances if instanceFilter(i)]
        if not preemptable and not both:
            return [i for i in filteredInstances if i.spot_instance_request_id is None]
        elif preemptable and not both:
            return [i for i in filteredInstances if i.spot_instance_request_id is not None]
        elif both:
            return filteredInstances

    def _getSpotRequestIDs(self) -> List[str]:
        """
        Get the IDs of all spot requests associated with the cluster.
        """
        assert self._boto2
        requests = self._boto2.ec2.get_all_spot_instance_requests()
        tags = self._boto2.ec2.get_all_tags({'tag:': {_TAG_KEY_TOIL_CLUSTER_NAME: self.clusterName}})
        idsToCancel = [tag.id for tag in tags]
        return [request for request in requests if request.id in idsToCancel]

    def _createSecurityGroups(self) -> List[str]:
        """
        Create security groups for the cluster. Returns a list of their IDs.
        """
        assert self._boto2
        def groupNotFound(e):
            retry = (e.status == 400 and 'does not exist in default VPC' in e.body)
            return retry
        vpcId = None
        if self._vpcSubnet:
            conn = boto.connect_vpc(region=self._boto2.ec2.region)
            subnets = conn.get_all_subnets(subnet_ids=[self._vpcSubnet])
            if len(subnets) > 0:
                vpcId = subnets[0].vpc_id
        # security group create/get. ssh + all ports open within the group
        try:
            web = self._boto2.ec2.create_security_group(self.clusterName,
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
        for sg in self._boto2.ec2.get_all_security_groups():
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
        sgs = [sg for sg in self._boto2.ec2.get_all_security_groups()
               if (sg.name in self._leaderSecurityGroupNames or
                   sg.id in self._leaderSecurityGroupIDs)]
        return [sg.id for sg in sgs]

    @awsRetry
    def _get_launch_template_ids(self, filters: Optional[List[Dict[str, List[str]]]] = None) -> List[str]:
        """
        Find all launch templates associated with the cluster.

        Returns a list of launch template IDs.
        """

        # How do we match the right templates?
        combined_filters = [{'Name': 'tag:' + _TAG_KEY_TOIL_CLUSTER_NAME, 'Values': [self.clusterName]}]

        if filters:
            # Add any user-specified filters
            combined_filters += filters

        allTemplateIDs = []
        # Get the first page with no NextToken
        response = self.ec2_client.describe_launch_templates(Filters=combined_filters,
                                                             MaxResults=200)
        while True:
            # Process the current page
            allTemplateIDs += [item['LaunchTemplateId'] for item in response.get('LaunchTemplates', [])]
            if 'NextToken' in response:
                # There are more pages. Get the next one, supplying the token.
                response = self.ec2_client.describe_launch_templates(Filters=filters,
                                                                     NextToken=response['NextToken'],
                                                                     MaxResults=200)
            else:
                # No more pages
                break

        return allTemplateIDs

    @awsRetry
    def _get_worker_launch_template(self, instance_type: str, preemptable: bool = False, backoff: float = 1.0) -> str:
        """
        Get a launch template for instances with the given parameters. Only one
        such launch template will be created, no matter how many times the
        function is called.

        Not thread safe.

        :param instance_type: Type of node to use in the template. May be overridden
                              by an ASG that uses the template.

        :param preemptable: When the node comes up, does it think it is a spot instance?

        :param backoff: How long to wait if it seems like we aren't reading our
                        own writes before trying again.

        :return: The ID of the template.
        """

        lt_name = self._name_worker_launch_template(instance_type, preemptable=preemptable)

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
                return self._create_worker_launch_template(instance_type, preemptable=preemptable)
            except ClientError as e:
                if get_error_code(e) == 'InvalidLaunchTemplateName.AlreadyExistsException':
                    # Someone got to it before us (or we couldn't read our own
                    # writes). Recurse to try again, because now it exists.
                    logger.info('Waiting %f seconds for template %s to be available', backoff, lt_name)
                    time.sleep(backoff)
                    return self._get_worker_launch_template(instance_type, preemptable=preemptable, backoff=backoff*2)
                else:
                    raise
        else:
            # There must be exactly one template
            return templates[0]

    def _name_worker_launch_template(self, instance_type: str, preemptable: bool = False) -> str:
        """
        Get the name we should use for the launch template with the given parameters.

        :param instance_type: Type of node to use in the template. May be overridden
                              by an ASG that uses the template.

        :param preemptable: When the node comes up, does it think it is a spot instance?
        """

        # The name has the cluster name in it
        lt_name = f'{self.clusterName}-lt-{instance_type}'
        if preemptable:
            lt_name += '-spot'

        return lt_name

    def _create_worker_launch_template(self, instance_type: str, preemptable: bool = False) -> str:
        """
        Create the launch template for launching worker instances for the cluster.

        :param instance_type: Type of node to use in the template. May be overridden
                              by an ASG that uses the template.

        :param preemptable: When the node comes up, does it think it is a spot instance?

        :return: The ID of the template created.
        """

        # TODO: If we already have one like this, set its storage and/or remake it.

        assert self._leaderPrivateIP
        type_info = E2Instances[instance_type]
        rootVolSize=self._nodeStorageOverrides.get(instance_type, self._nodeStorage)
        bdms = self._getBoto3BlockDeviceMappings(type_info, rootVolSize=rootVolSize)

        keyPath = self._sseKey if self._sseKey else None
        userData = self._getCloudConfigUserData('worker', keyPath, preemptable)

        lt_name = self._name_worker_launch_template(instance_type, preemptable=preemptable)

        # But really we find it by tag
        tags = dict(self._tags)
        tags[_TAG_KEY_TOIL_NODE_TYPE] = 'worker'

        return create_launch_template(self.ec2_client,
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
        response = self.autoscaling_client.describe_tags(Filters=filters)
        while True:
            # Process the current page
            matchedASGs += [item['ResourceId'] for item in response.get('Tags', [])
                            if item['Key'] == _TAG_KEY_TOIL_CLUSTER_NAME and
                            item['Value'] == self.clusterName]
            if 'NextToken' in response:
                # There are more pages. Get the next one, supplying the token.
                response = self.autoscaling_client.describe_tags(Filters=filters,
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

        create_auto_scaling_group(self.autoscaling_client,
                                  asg_name=asg_name,
                                  launch_template_ids=launch_template_ids,
                                  vpc_subnets=[self._subnetID],
                                  min_size=min_size,
                                  max_size=max_size,
                                  instance_types=instance_types,
                                  spot_bid=spot_bid,
                                  tags=tags)

        return asg_name

    @awsRetry
    def _getRoleNames(self) -> List[str]:
        """
        Get all the roles belonging to the cluster, as names.
        """

        # TODO: When we drop boto2 and the Boto2Context, keep track of which
        # roles are ours ourselves.
        return [role['role_name'] for role in self._boto2.local_roles()]

    @awsRetry
    def _getInstanceProfileNames(self) -> List[str]:
        """
        Get all the instance profiles belonging to the cluster, as names.
        """

        # TODO: When we drop boto2 and the Boto2Context, keep track of which
        # instance profiles are ours ourselves.
        return [profile['instance_profile_name'] for profile in self._boto2.local_instance_profiles()]

    @awsRetry
    def _getRoleInstanceProfileNames(self, role_name: str) -> List[str]:
        """
        Get all the instance profiles with the IAM role with the given name.

        Returns instance profile names.
        """

        allProfiles = []

        response = self.iam_client.list_instance_profiles_for_role(RoleName=role_name,
                                                                   MaxItems=200)
        while True:
            # Process the current page
            allProfiles += [item['InstanceProfileName'] for item in response.get('InstanceProfiles', [])]
            if 'IsTruncated' in response and response['IsTruncated']:
                # There are more pages. Get the next one, supplying the marker.
                response = self.iam_client.list_instance_profiles_for_role(RoleName=role_name,
                                                                           MaxItems=200,
                                                                           Marker=response['Marker'])
            else:
                # No more pages
                break

        return allProfiles

    @awsRetry
    def _getRolePolicyArns(self, role_name: str) -> List[str]:
        """
        Get all the policies attached to the IAM role with the given name.

        These do not include inline policies on the role.

        Returns policy ARNs.
        """

        # TODO: we don't currently use attached policies.

        allPolicies = []

        response = self.iam_client.list_attached_role_policies(RoleName=role_name,
                                                               MaxItems=200)
        while True:
            # Process the current page
            allPolicies += [item['PolicyArn'] for item in response.get('AttachedPolicies', [])]
            if 'IsTruncated' in response and response['IsTruncated']:
                # There are more pages. Get the next one, supplying the marker.
                response = self.iam_client.list_attached_role_policies(RoleName=role_name,
                                                                       MaxItems=200,
                                                                       Marker=response['Marker'])
            else:
                # No more pages
                break

        return allPolicies

    @awsRetry
    def _getRoleInlinePolicyNames(self, role_name: str) -> List[str]:
        """
        Get all the policies inline in the given IAM role.
        Returns policy names.
        """

        allPolicies = []

        response = self.iam_client.list_role_policies(RoleName=role_name,
                                                      MaxItems=200)
        while True:
            # Process the current page
            allPolicies += response.get('PolicyNames', [])
            if 'IsTruncated' in response and response['IsTruncated']:
                # There are more pages. Get the next one, supplying the marker.
                response = self.iam_client.list_role_policies(RoleName=role_name,
                                                              MaxItems=200,
                                                              Marker=response['Marker'])
            else:
                # No more pages
                break

        return allPolicies

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

    @awsRetry
    def _createProfileArn(self) -> str:
        """
        Create an IAM role and instance profile that grants needed permissions
        for cluster leaders and workers. Naming is handled by the Boto2Context
        and is specific to the cluster.

        Returns its ARN.
        """
        assert self._boto2
        policy = dict(iam_full=self.full_policy('iam'), ec2_full=self.full_policy('ec2'),
                      s3_full=self.full_policy('s3'), sbd_full=self.full_policy('sdb'))
        if self.clusterType == 'kubernetes':
            # We also need autoscaling groups and some other stuff for AWS-Kubernetes integrations.
            # TODO: We use one merged policy for leader and worker, but we could be more specific.
            policy['kubernetes_merged'] = self.kubernetes_policy()
        iamRoleName = self._boto2.setup_iam_ec2_role(role_name=_INSTANCE_PROFILE_ROLE_NAME, policies=policy)

        try:
            profile = self._boto2.iam.get_instance_profile(iamRoleName)
        except BotoServerError as e:
            if e.status == 404:
                profile = self._boto2.iam.create_instance_profile(iamRoleName)
                profile = profile.create_instance_profile_response.create_instance_profile_result
            else:
                raise
        else:
            profile = profile.get_instance_profile_response.get_instance_profile_result
        profile = profile.instance_profile
        profile_arn = profile.arn

        if len(profile.roles) > 1:
            raise RuntimeError('Did not expect profile to contain more than one role')
        elif len(profile.roles) == 1:
            # this should be profile.roles[0].role_name
            if profile.roles.member.role_name == iamRoleName:
                return profile_arn
            else:
                self._boto2.iam.remove_role_from_instance_profile(iamRoleName,
                                                                  profile.roles.member.role_name)
        for attempt in old_retry(predicate=lambda err: err.status == 404):
            with attempt:
                self._boto2.iam.add_role_to_instance_profile(iamRoleName, iamRoleName)
        return profile_arn


