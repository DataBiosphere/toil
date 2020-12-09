# Copyright (C) 2015-2020 UCSC Computational Genomics Lab
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
import os
import time
import string
import json
import boto3
import logging
import urllib.request
import boto.ec2

from six import iteritems
from typing import List, Dict, Any, Optional
from functools import wraps
from boto.ec2.blockdevicemapping import BlockDeviceMapping as Boto2BlockDeviceMapping, BlockDeviceType as Boto2BlockDeviceType
from boto.exception import BotoServerError, EC2ResponseError
from boto.utils import get_instance_metadata
from boto.ec2.instance import Instance as Boto2Instance

from toil.lib.memoize import memoize
from toil.lib.ec2 import (a_short_time, create_ondemand_instances, create_instances,
                          create_spot_instances, wait_instances_running, wait_transition)
from toil.lib.ec2nodes import InstanceType
from toil.lib.misc import truncExpBackoff
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.aws import zoneToRegion, getCurrentAWSZone, getSpotZone
from toil.provisioners.aws.boto2Context import Boto2Context
from toil.lib.retry import old_retry
from toil.lib.memoize import less_strict_bool
from toil.provisioners import NoSuchClusterException
from toil.provisioners.node import Node
from toil.lib.generatedEC2Lists import E2Instances

logger = logging.getLogger(__name__)
logging.getLogger("boto").setLevel(logging.CRITICAL)
# Role name (used as the suffix) for EC2 instance profiles that are automatically created by Toil.
_INSTANCE_PROFILE_ROLE_NAME = 'toil'
# The tag key that specifies the Toil node type ("leader" or "worker") so that
# leader vs. worker nodes can be robustly identified.
_TAG_KEY_TOIL_NODE_TYPE = 'ToilNodeType'
# The tag that specifies the cluster name on all nodes
_TAG_KEY_TOIL_CLUSTER_NAME = 'clusterName'

def awsRetryPredicate(e):
    if not isinstance(e, BotoServerError):
        return False
    # boto/AWS gives multiple messages for the same error...
    if e.status == 503 and 'Request limit exceeded' in e.body:
        return True
    elif e.status == 400 and 'Rate exceeded' in e.body:
        return True
    elif e.status == 400 and 'NotFound' in e.body:
        # EC2 can take a while to propagate instance IDs to all servers.
        return True
    elif e.status == 400 and e.error_code == 'Throttling':
        return True
    return False


def awsFilterImpairedNodes(nodes, ec2):
    # if TOIL_AWS_NODE_DEBUG is set don't terminate nodes with
    # failing status checks so they can be debugged
    nodeDebug = less_strict_bool(os.environ.get('TOIL_AWS_NODE_DEBUG'))
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


class InvalidClusterStateException(Exception):
    pass


class AWSProvisioner(AbstractProvisioner):
    def __init__(self, clusterName, clusterType, zone, nodeStorage, nodeStorageOverrides, sseKey):
        if clusterType not in ['mesos', 'kubernetes']:
            raise ClusterTypeNotSupportedException(AWSProvisioner, clusterType)
        super(AWSProvisioner, self).__init__(clusterName, clusterType, zone, nodeStorage, nodeStorageOverrides)
        self.cloud = 'aws'
        self._sseKey = sseKey
        self._zone = zone if zone else getCurrentAWSZone()
        
        if zone is None:
            # Can't proceed without a real zone
            raise RuntimeError('No AWS availability zone specified. Configure in Boto '
                               'configuration file, TOIL_AWS_ZONE environment variable, or '
                               'on the command line.')
    

        # establish boto3 clients
        self.session = boto3.Session(region_name=zoneToRegion(self._zone))
        self.ec2 = self.session.resource('ec2')

        if clusterName:
            self._buildContext()  # create boto2 context (self._boto2)
        else:
            self._readClusterSettings() # Also fills in self._boto2

    def _readClusterSettings(self):
        """
        Reads the cluster settings from the instance metadata, which assumes the instance
        is the leader.
        """
        instanceMetaData = get_instance_metadata()
        region = zoneToRegion(self._zone)
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
        self._tags = self.getLeader().tags
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
        :param userTags: Optionally provided user tags to put on the leader.
        :param vpcSubnet: Optionally specify the VPC subnet.
        :param awsEc2ProfileArn: Optionally provide the profile ARN.
        :param awsEc2ExtraSecurityGroupIds: Optionally provide additional security group IDs.
        :return: None
        """
        
        instanceType = E2Instances[leaderNodeType]
        
        if self.clusterType == 'kubernetes':
            if instanceType.cores < 2:
                # Kubernetes won't run here.
                raise RuntimeError('Kubernetes requires 2 or more cores, and %s is too small' %
                                   leaderNodeType)
        
        
        self._keyName = keyName
        # This is where we put the leader
        self._vpcSubnet = vpcSubnet

        profileArn = awsEc2ProfileArn or self._getProfileArn()
        # the security group name is used as the cluster identifier
        createdSGs = self._createSecurityGroups()
        bdms = self._getBoto3BlockDeviceMappings(instanceType, rootVolSize=leaderStorage)

        userData = self._getCloudConfigUserData('leader')
        if isinstance(userData, str):
            # Spot-market provisioning requires bytes for user data.
            # We probably won't have a spot-market leader, but who knows!
            userData = userData.encode('utf-8')
        instances = create_instances(self.ec2,
                                     image_id=self._discoverAMI(),
                                     num_instances=1,
                                     key_name=self._keyName,
                                     security_group_ids=createdSGs + awsEc2ExtraSecurityGroupIds,
                                     instance_type=instanceType.name,
                                     user_data=userData,
                                     block_device_map=bdms,
                                     instance_profile_arn=profileArn,
                                     placement_az=self._zone,
                                     subnet_id=self._vpcSubnet)

        # wait for the leader to exist at all
        leader = instances[0]
        leader.wait_until_exists()

        default_tags = {'Name': self.clusterName,
                        'Owner': owner,
                        _TAG_KEY_TOIL_CLUSTER_NAME: self.clusterName,
                        _TAG_KEY_TOIL_NODE_TYPE: 'leader'}
        default_tags.update(userTags)
        if self.clusterType == 'kubernetes':
            # All nodes need a tag putting them in the cluster.
            # This tag needs to be on there before the a leader can finish its startup.
            default_tags['kubernetes.io/cluster/' + self.clusterName] = ''

        tags = []
        for user_key, user_value in default_tags.items():
            tags.append({'Key': user_key, 'Value': user_value})
        leader.create_tags(Tags=tags)
        
        # Don't go on until the leader is started
        leader.wait_until_running()

        # Remember enough about the leader to let us launch workers in its
        # cluster.
        self._tags = {t['Key']: t['Value'] for t in tags}
        self._leaderPrivateIP = leader.private_ip_address
        # This is where we will put the workers.
        # See also: self._vpcSubnet
        self._subnetID = leader.subnet_id
        self._leaderSecurityGroupNames = set()
        self._leaderSecurityGroupIDs = set(createdSGs + awsEc2ExtraSecurityGroupIds)
        self._leaderProfileArn = profileArn
        
        leaderNode = Node(publicIP=leader.public_ip_address, privateIP=leader.private_ip_address,
                          name=leader.id, launchTime=leader.launch_time,
                          nodeType=instanceType.name, preemptable=False,
                          tags=leader.tags)
        leaderNode.waitForNode('toil_leader')
        
        # Download credentials
        self._setLeaderWorkerAuthentication(leaderNode)

    def getNodeShape(self, nodeType: str, preemptable=False) -> Shape:
        """
        Get the Shape for the given instance type (e.g. 't2.medium').
        """
        instanceType = E2Instances[nodeType]

        disk = instanceType.disks * instanceType.disk_capacity * 2 ** 30
        if disk == 0:
            # This is an EBS-backed instance. We will use the root
            # volume, so add the amount of EBS storage requested for
            # the root volume
            disk = self._nodeStorageOverrides.get(nodeType, self._nodeStorage) * 2 ** 30

        #Underestimate memory by 100M to prevent autoscaler from disagreeing with
        #mesos about whether a job can run on a particular node type
        memory = (instanceType.memory - 0.1) * 2** 30
        return Shape(wallTime=60 * 60,
                     memory=memory,
                     cores=instanceType.cores,
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
        def expectedShutdownErrors(e):
            return e.status == 400 and 'dependent object' in e.body

        def destroyInstances(instances: List[Boto2Instance]):
            """
            Similar to _terminateInstances, except that it also cleans up any
            resources associated with the instances (e.g. IAM profiles).
            """
            instanceProfiles = [x.instance_profile['arn'] for x in instances]
            self._deleteIAMProfiles(instanceProfiles)
            self._terminateInstances(instances)

        # We should terminate the leader first in case a workflow is still running in the cluster.
        # The leader may create more instances while we're terminating the workers.
        vpcId = None
        try:
            leader = self._getLeaderInstance()
            vpcId = leader.vpc_id
            logger.info('Terminating the leader first ...')
            destroyInstances([leader])
            logger.info('Now terminating any remaining workers ...')
        except (NoSuchClusterException, InvalidClusterStateException):
            # It's ok if the leader is not found. We'll terminate any remaining
            # instances below anyway.
            pass

        instances = self._getNodesInCluster(nodeType=None, both=True)
        spotIDs = self._getSpotRequestIDs()
        if spotIDs:
            self._boto2.ec2.cancel_spot_instance_requests(request_ids=spotIDs)
        instancesToTerminate = awsFilterImpairedNodes(instances, self._boto2.ec2)
        if instancesToTerminate:
            vpcId = vpcId or instancesToTerminate[0].vpc_id
            destroyInstances(instancesToTerminate)
        if len(instances) == len(instancesToTerminate):
            logger.debug('Deleting security group...')
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
                logger.debug('... Succesfully deleted security group')
        else:
            assert len(instances) > len(instancesToTerminate)
            # the security group can't be deleted until all nodes are terminated
            logger.warning('The TOIL_AWS_NODE_DEBUG environment variable is set and some nodes '
                           'have failed health checks. As a result, the security group & IAM '
                           'roles will not be deleted.')

    def terminateNodes(self, nodes : List[Node]):
        self._terminateIDs([x.name for x in nodes])

    def addNodes(self, nodeType, numNodes, preemptable, spotBid=None) -> int:
        assert self._leaderPrivateIP
        if preemptable and not spotBid:
            if self._spotBidsMap and nodeType in self._spotBidsMap:
                spotBid = self._spotBidsMap[nodeType]
            else:
                raise RuntimeError("No spot bid given for a preemptable node request.")
        instanceType = E2Instances[nodeType]
        bdm = self._getBoto2BlockDeviceMapping(instanceType, rootVolSize=self._nodeStorageOverrides.get(nodeType, self._nodeStorage))

        keyPath = self._sseKey if self._sseKey else None
        userData = self._getCloudConfigUserData('worker', keyPath, preemptable)
        if isinstance(userData, str):
            # Spot-market provisioning requires bytes for user data.
            userData = userData.encode('utf-8')
        
        kwargs = {'key_name': self._keyName,
                  'security_group_ids': self._getSecurityGroupIDs(),
                  'instance_type': instanceType.name,
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
                    kwargs['placement'] = getSpotZone(spotBid, instanceType.name, self._boto2)
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

    def getProvisionedWorkers(self, nodeType, preemptable) -> List[Node]:
        assert self._leaderPrivateIP
        entireCluster = self._getNodesInCluster(both=True, nodeType=nodeType)
        logger.debug('All nodes in cluster: %s', entireCluster)
        workerInstances = [i for i in entireCluster if i.private_ip_address != self._leaderPrivateIP]
        logger.debug('All workers found in cluster: %s', workerInstances)
        workerInstances = [i for i in workerInstances if preemptable != (i.spot_instance_request_id is None)]
        logger.debug('%spreemptable workers found in cluster: %s', 'non-' if not preemptable else '', workerInstances)
        workerInstances = awsFilterImpairedNodes(workerInstances, self._boto2.ec2)
        return [Node(publicIP=i.ip_address, privateIP=i.private_ip_address,
                     name=i.id, launchTime=i.launch_time, nodeType=i.instance_type,
                     preemptable=preemptable, tags=i.tags)
                for i in workerInstances]

    def _buildContext(self):
        if self._zone is None:
            self._zone = getCurrentAWSZone()
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
        :return: The AMI ID (a string like 'ami-0a9a5d2b65cce04eb') for CoreOS
                 or a compatible replacement like Flatcar.
        :rtype: str
        """
        
        # Take a user override
        ami = os.environ.get('TOIL_AWS_AMI')
        if ami is not None:
            return ami
        
        # CoreOS is dead, long live Flatcar
        
        # Flatcar images, however, only live for 9 months.
        # Rather than hardcode a list of AMIs by region that will die, we use
        # their JSON feed of the current ones. 
        JSON_FEED_URL = 'https://stable.release.flatcar-linux.net/amd64-usr/current/flatcar_production_ami_all.json'
        
        # What region do we care about?
        region = zoneToRegion(self._zone)
        
        for attempt in old_retry(predicate=lambda e: True):
            # Until we get parseable JSON
            # TODO: What errors do we get for timeout, JSON parse failure, etc?
            with attempt:
                # Try to get the JSON and parse it.
                feed = json.loads(urllib.request.urlopen(JSON_FEED_URL).read())
                
        try:
            for ami_record in feed['amis']:
                # Scan the klist of regions
                if ami_record['name'] == region:
                    # When we find ours
                    # Save the AMI ID
                    ami = ami_record['hvm']
                    # And stop scanning
                    break
        except KeyError:
            # We didn't see a field we need
            raise RuntimeError('Flatcar image feed at {} does not have expected format'.format(JSON_FEED_URL))
        
        if ami is None:
            # We didn't find it
            raise RuntimeError('Flatcar image feed at {} does not have an image for region {}'.format(JSON_FEED_URL, region))

        return ami

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
        instances = self._getNodesInCluster(nodeType=None, both=True)
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

        return leader if returnRawInstance else leaderNode

    @classmethod
    @awsRetry
    def _addTag(cls, instance: Boto2Instance, key: str, value: str):
        instance.add_tag(key, value)

    @classmethod
    def _addTags(cls, instances: List[Boto2Instance], tags: Dict[str, str]):
        for instance in instances:
            for key, value in iteritems(tags):
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

    def _deleteIAMProfiles(self, profileARNs: List[str]):
        """
        Delete the Toil-creared IAM instance profiles named by the given list of ARNs.
        """
        assert self._boto2
        for profile in profileARNs:
            # boto won't look things up by the ARN so we have to parse it to get
            # the profile name
            profileName = profile.rsplit('/')[-1]

            # Only delete profiles that were automatically created by Toil.
            if profileName != self._boto2.to_aws_name(_INSTANCE_PROFILE_ROLE_NAME):
                continue

            try:
                profileResult = self._boto2.iam.get_instance_profile(profileName)
            except BotoServerError as e:
                if e.status == 404:
                    return
                else:
                    raise
            # wade through EC2 response object to get what we want
            profileResult = profileResult['get_instance_profile_response']
            profileResult = profileResult['get_instance_profile_result']
            profile = profileResult['instance_profile']
            # this is based off of our 1:1 mapping of profiles to roles
            role = profile['roles']['member']['role_name']
            try:
                self._boto2.iam.remove_role_from_instance_profile(profileName, role)
            except BotoServerError as e:
                if e.status == 404:
                    pass
                else:
                    raise
            policyResults = self._boto2.iam.list_role_policies(role)
            policyResults = policyResults['list_role_policies_response']
            policyResults = policyResults['list_role_policies_result']
            policies = policyResults['policy_names']
            for policyName in policies:
                try:
                    self._boto2.iam.delete_role_policy(role, policyName)
                except BotoServerError as e:
                    if e.status == 404:
                        pass
                    else:
                        raise
            try:
                self._boto2.iam.delete_role(role)
            except BotoServerError as e:
                if e.status == 404:
                    pass
                else:
                    raise
            try:
                self._boto2.iam.delete_instance_profile(profileName)
            except BotoServerError as e:
                if e.status == 404:
                    pass
                else:
                    raise

    @classmethod
    def _getBoto2BlockDeviceMapping(cls, instanceType: InstanceType, rootVolSize: int = 50) -> Boto2BlockDeviceMapping:
        # determine number of ephemeral drives via cgcloud-lib (actually this is moved into toil's lib
        bdtKeys = [''] + ['/dev/xvd{}'.format(c) for c in string.ascii_lowercase[1:]]
        bdm = Boto2BlockDeviceMapping()
        # Change root volume size to allow for bigger Docker instances
        root_vol = Boto2BlockDeviceType(delete_on_termination=True)
        root_vol.size = rootVolSize
        bdm["/dev/xvda"] = root_vol
        # The first disk is already attached for us so start with 2nd.
        # Disk count is weirdly a float in our instance database, so make it an int here.
        for disk in range(1, int(instanceType.disks) + 1):
            bdm[bdtKeys[disk]] = Boto2BlockDeviceType(
                ephemeral_name='ephemeral{}'.format(disk - 1))  # ephemeral counts start at 0

        logger.debug('Device mapping: %s', bdm)
        return bdm
        
    @classmethod
    def _getBoto3BlockDeviceMappings(cls, instanceType: InstanceType, rootVolSize: int = 50) -> List[dict]:
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
        for disk in range(1, int(instanceType.disks) + 1):
            # Make a block device mapping to attach the ephemeral disk to a
            # virtual block device in the VM
            bdms.append({
                'DeviceName': bdtKeys[disk],
                'VirtualName': 'ephemeral{}'.format(disk - 1) # ephemeral counts start at 0
            })
        logger.debug('Device mapping: %s', bdms)
        return bdms

    @awsRetry
    def _getNodesInCluster(self, nodeType=None, preemptable=False, both=False) -> List[Boto2Instance]:
        """
        Get Boto2 instance objects for all nodes in the cluster.
        """
        assert self._boto2
        allInstances = self._boto2.ec2.get_only_instances(filters={'instance.group-name': self.clusterName})
        def instanceFilter(i):
            # filter by type only if nodeType is true
            rightType = not nodeType or i.instance_type == nodeType
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
    def _getLaunchTemplateIDs(self, nodeType=None, preemptable=False, both=False) -> List[str]:
        """
        Find all launch templates associated with the cluster.
        
        Returns a list of launch template IDs.
        """
        
        # How do we match the right templates?
        filters = [{'tag:' + _TAG_KEY_TOIL_CLUSTER_NAME: self.clusterName}]
        
        allTemplateIDs = []
        # Get the first page with no NextToken
        response = self.ec2.describe_launch_templates(Filters=filters,
                                                      MaxResults=200)
        while True:
            # Process the current page
            allTemplateIDs += [item['LaunchTemplateId'] for item in response.get('LaunchTemplates', [])]
            if 'NextToken' in response:
                # There are more pages. Get the next one, supplying the token.
                response = self.ec2.describe_launch_templates(Filters=filters,
                                                              NextToken=response['NextToken'], MaxResults=200)
            else:
                # No more pages
                break
                
        return allTemplateIDs
        
    def _createWorkerLaunchTemplate(self, nodeType: str, preemptable: bool = False) -> str:
        """
        Create the launch template for launching worker instances for the cluster.
        
        :param nodeType: Type of node to use in the template. May be overridden
                         by an ASG that uses the template.
                         
        :param preemptable: When the node comes up, does it think it is a spot instance?
        
        :return: The ID of the template created.
        """
        
        assert self._leaderPrivateIP
        instanceType = E2Instances[nodeType]
        rootVolSize=self._nodeStorageOverrides.get(nodeType, self._nodeStorage)
        bdms = self._getBoto3BlockDeviceMappings(instanceType, rootVolSize=rootVolSize)

        keyPath = self._sseKey if self._sseKey else None
        userData = self._getCloudConfigUserData('worker', keyPath, preemptable)
        
        # The name has the cluster name in it
        lt_name = f'{self.clusterName}-lt-{nodeType}'
        if preemptable:
            lt_name += '-spot'
            
        # But really we find it by tag
        tags = {_TAG_KEY_TOIL_CLUSTER_NAME: self.clusterName}
        

        return create_launch_template(self.ec2,
                                      template_name=lt_name,
                                      image_id=self._discoverAMI(),
                                      key_name=self._keyName,
                                      security_group_ids=self._getSecurityGroupIDs(),
                                      instance_type=instanceType.name,
                                      user_data=userData,
                                      block_device_map=bdms,
                                      instance_profile_arn=self._leaderProfileArn,
                                      placement_az=self._zone,
                                      subnet_id=self._subnetID,
                                      tags=tags)

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
    def _getProfileArn(self) -> str:
        """
        Create an IAM role and instance profile that grants needed permissions
        for cluster leaders and workers.
        
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
