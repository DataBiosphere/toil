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
from builtins import str
from builtins import range
import logging
import time
import subprocess
import sys
import string

# Python 3 compatibility imports
from _ssl import SSLError

from six import iteritems

from bd2k.util import memoize
import boto.ec2
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from boto.exception import BotoServerError, EC2ResponseError
from toil.lib.ec2 import (ec2_instance_types, a_short_time, create_ondemand_instances,
                          create_spot_instances, wait_instances_running, wait_transition)

from toil import applianceSelf
from toil.lib.misc import truncExpBackoff
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.aws import *
from toil.lib.context import Context
from boto.utils import get_instance_metadata
from bd2k.util.retry import retry
from toil.provisioners import (awsRemainingBillingInterval, awsFilterImpairedNodes,
                               Node, NoSuchClusterException)

logger = logging.getLogger(__name__)


class AWSProvisioner(AbstractProvisioner):

    def __init__(self, config=None):
        """
        Initialize the AWS Provisioner object. The object is created in two distinct
        ways:

        1.  The first is by the `toil launch-cluster` utility which does not pass a config
            and creates a provisioner. Fields are initialized to None and
            are set later when the leader is created via `self.launchCluster`. This
            round-about initialization is necessary because launch-cluster is a
            classmethod.

        2.  The second is used when doing regular autoscaling and the provisioner is
            initialized with a config file. This happens in `Toil._setProvisioner()`

        This is due to the fact that the provisioner is used both in Toil runs to manage
        autoscaling as well as outside of Toil runs to launch clusters and manage statically
        provisioned nodes. Static provisioned nodes are those that the user explicitly adds into
        the cluster via `launch-cluster -workers n`, which will launch a cluster with n statically
        provisioned nodes.

        :param config: Optional config object from common.py
        :param batchSystem:
        """
        super(AWSProvisioner, self).__init__(config)
        if config:
            self.instanceMetaData = get_instance_metadata()
            self.clusterName = self._getClusterNameFromTags(self.instanceMetaData)
            self.ctx = self._buildContext(clusterName=self.clusterName)
            self.leaderIP = self.instanceMetaData['local-ipv4']  # this is PRIVATE IP
            self.keyName = list(self.instanceMetaData['public-keys'].keys())[0]
            self.tags = self._getLeader(self.clusterName).tags
            self.masterPublicKey = self._setSSH()
            self.nodeStorage = config.nodeStorage
            spotBids = []
            self.nonPreemptableNodeTypes = []
            self.preemptableNodeTypes = []
            for nodeTypeStr in config.nodeTypes:
                nodeBidTuple = nodeTypeStr.split(":")
                if len(nodeBidTuple) == 2:
                    #This is a preemptable node type, with a spot bid
                    self.preemptableNodeTypes.append(nodeBidTuple[0])
                    spotBids.append(nodeBidTuple[1])
                else:
                    self.nonPreemptableNodeTypes.append(nodeTypeStr)
            self.preemptableNodeShapes = [self.getNodeShape(nodeType=nodeType, preemptable=True) for nodeType in self.preemptableNodeTypes]
            self.nonPreemptableNodeShapes = [self.getNodeShape(nodeType=nodeType, preemptable=False) for nodeType in self.nonPreemptableNodeTypes]

            self.nodeShapes = self.nonPreemptableNodeShapes + self.preemptableNodeShapes
            self.nodeTypes = self.nonPreemptableNodeTypes + self.preemptableNodeTypes
            self.spotBids = dict(zip(self.preemptableNodeTypes, spotBids))

        else:
            self.ctx = None
            self.clusterName = None
            self.instanceMetaData = None
            self.leaderIP = None
            self.keyName = None
            self.tags = None
            self.masterPublicKey = None
            self.nodeStorage = None
        self.subnetID = None

    def launchCluster(self, leaderNodeType, leaderSpotBid, nodeTypes, preemptableNodeTypes, keyName,
            clusterName, numWorkers=0, numPreemptableWorkers=0, spotBids=None, userTags=None, zone=None, vpcSubnet=None, leaderStorage=50, nodeStorage=50):
        if self.config is None:
            self.nodeStorage = nodeStorage
        if userTags is None:
            userTags = {}
        ctx = self._buildContext(clusterName=clusterName, zone=zone)
        profileARN = self._getProfileARN(ctx)
        leaderInstanceType = ec2_instance_types[leaderNodeType]
        # the security group name is used as the cluster identifier
        sgs = self._createSecurityGroup(ctx, clusterName, vpcSubnet)
        bdm = self._getBlockDeviceMapping(leaderInstanceType, rootVolSize=leaderStorage)
        self.masterPublicKey = 'AAAAB3NzaC1yc2Enoauthorizedkeyneeded'
        leaderData = dict(role='leader',
                          image=applianceSelf(),
                          entrypoint='mesos-master',
                          sshKey=self.masterPublicKey,
                          args=leaderArgs.format(name=clusterName))
        userData = awsUserData.format(**leaderData)
        kwargs = {'key_name': keyName, 'security_group_ids': [sg.id for sg in sgs],
                  'instance_type': leaderNodeType,
                  'user_data': userData, 'block_device_map': bdm,
                  'instance_profile_arn': profileARN,
                  'placement': zone}
        if vpcSubnet:
            kwargs["subnet_id"] = vpcSubnet
        if not leaderSpotBid:
            logger.info('Launching non-preemptable leader')
            create_ondemand_instances(ctx.ec2, image_id=self._discoverAMI(ctx),
                                      spec=kwargs, num_instances=1)
        else:
            logger.info('Launching preemptable leader')
            # force generator to evaluate
            list(create_spot_instances(ec2=ctx.ec2,
                                       price=leaderSpotBid,
                                       image_id=self._discoverAMI(ctx),
                                       tags={'clusterName': clusterName},
                                       spec=kwargs,
                                       num_instances=1))
        leader = self._getLeader(clusterName=clusterName, wait=True, zone=zone)

        defaultTags = {'Name': clusterName, 'Owner': keyName}
        defaultTags.update(userTags)

        # if we running launch cluster we need to save this data as it won't be generated
        # from the metadata. This data is needed to launch worker nodes.
        self.leaderIP = leader.private_ip_address
        self._addTags([leader], defaultTags)
        self.ctx = ctx
        if spotBids:
            self.spotBids = dict(zip(preemptableNodeTypes, spotBids))
        self.clusterName = clusterName
        self.keyName = keyName
        self.tags = leader.tags
        self.subnetID = leader.subnet_id
        # assuming that if the leader was launched without a spotbid then all workers
        # will be non-preemptable
        workersCreated = 0
        for nodeType, workers in zip(nodeTypes, numWorkers):
            workersCreated += self.addNodes(nodeType=nodeType, numNodes=workers, preemptable=False)
        for nodeType, workers in zip(preemptableNodeTypes, numPreemptableWorkers):
            workersCreated += self.addNodes(nodeType=nodeType, numNodes=workers, preemptable=True)
        logger.info('Added %d workers', workersCreated)

        return leader

    def getNodeShape(self, nodeType, preemptable=False):
        instanceType = ec2_instance_types[nodeType]

        disk = instanceType.disks * instanceType.disk_capacity * 2 ** 30
        if disk == 0:
            # This is an EBS-backed instance. We will use the root
            # volume, so add the amount of EBS storage requested for
            # the root volume
            disk = self.nodeStorage * 2 ** 30

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
        return AWSProvisioner._throttlePredicate(e)

    @classmethod
    def destroyCluster(cls, clusterName, zone=None):
        def expectedShutdownErrors(e):
            return e.status == 400 and 'dependent object' in e.body

        ctx = cls._buildContext(clusterName=clusterName, zone=zone)
        instances = cls._getNodesInCluster(ctx, clusterName, nodeType=None, both=True)
        spotIDs = cls._getSpotRequestIDs(ctx, clusterName)
        if spotIDs:
            ctx.ec2.cancel_spot_instance_requests(request_ids=spotIDs)
        instancesToTerminate = awsFilterImpairedNodes(instances, ctx.ec2)
        vpcId = None
        if instancesToTerminate:
            vpcId = instancesToTerminate[0].vpc_id
            cls._deleteIAMProfiles(instances=instancesToTerminate, ctx=ctx)
            cls._terminateInstances(instances=instancesToTerminate, ctx=ctx)
        if len(instances) == len(instancesToTerminate):
            logger.info('Deleting security group...')
            removed = False
            for attempt in retry(timeout=300, predicate=expectedShutdownErrors):
                with attempt:
                    for sg in ctx.ec2.get_all_security_groups():
                        if sg.name == clusterName and vpcId and sg.vpc_id == vpcId:
                            try:
                                ctx.ec2.delete_security_group(group_id=sg.id)
                                removed = True
                            except BotoServerError as e:
                                if e.error_code == 'InvalidGroup.NotFound':
                                    pass
                                else:
                                    raise
            if removed:
                logger.info('... Succesfully deleted security group')
        else:
            assert len(instances) > len(instancesToTerminate)
            # the security group can't be deleted until all nodes are terminated
            logger.warning('The TOIL_AWS_NODE_DEBUG environment variable is set and some nodes '
                           'have failed health checks. As a result, the security group & IAM '
                           'roles will not be deleted.')

    @classmethod
    def sshLeader(cls, clusterName, args=None, zone=None, **kwargs):
        leader = cls._getLeader(clusterName, zone=zone)
        logger.info('SSH ready')
        kwargs['tty'] = sys.stdin.isatty()
        command = args if args else ['bash']
        cls._sshAppliance(leader.public_dns_name, *command, **kwargs)

    @classmethod
    def rsyncLeader(cls, clusterName, args, zone=None, **kwargs):
        leader = cls._getLeader(clusterName, zone=zone)
        cls._coreRsync(leader.public_dns_name, args, **kwargs)

    def remainingBillingInterval(self, node):
        return awsRemainingBillingInterval(node)

    def terminateNodes(self, nodes):
        self._terminateNodes(nodes, self.ctx)

    def addNodes(self, nodeType, numNodes, preemptable):
        instanceType = ec2_instance_types[nodeType]
        bdm = self._getBlockDeviceMapping(instanceType, rootVolSize=self.nodeStorage)
        arn = self._getProfileARN(self.ctx)
        keyPath = '' if not self.config or not self.config.sseKey else self.config.sseKey
        entryPoint = 'mesos-slave' if not self.config or not self.config.sseKey else "waitForKey.sh"
        workerData = dict(role='worker',
                          image=applianceSelf(),
                          entrypoint=entryPoint,
                          sshKey=self.masterPublicKey,
                          args=workerArgs.format(ip=self.leaderIP, preemptable=preemptable, keyPath=keyPath))
        userData = awsUserData.format(**workerData)
        sgs = [sg for sg in self.ctx.ec2.get_all_security_groups() if sg.name == self.clusterName]
        kwargs = {'key_name': self.keyName,
                  'security_group_ids': [sg.id for sg in sgs],
                  'instance_type': instanceType.name,
                  'user_data': userData,
                  'block_device_map': bdm,
                  'instance_profile_arn': arn,
                  'placement': getCurrentAWSZone()}
        kwargs["subnet_id"] = self.subnetID if self.subnetID else self._getClusterInstance(self.instanceMetaData).subnet_id

        instancesLaunched = []

        for attempt in retry(predicate=AWSProvisioner._throttlePredicate):
            with attempt:
                # after we start launching instances we want to insure the full setup is done
                # the biggest obstacle is AWS request throttling, so we retry on these errors at
                # every request in this method
                if not preemptable:
                    logger.info('Launching %s non-preemptable nodes', numNodes)
                    instancesLaunched = create_ondemand_instances(self.ctx.ec2, image_id=self._discoverAMI(self.ctx),
                                                                  spec=kwargs, num_instances=numNodes)
                else:
                    logger.info('Launching %s preemptable nodes', numNodes)
                    kwargs['placement'] = getSpotZone(self.spotBids[nodeType], instanceType.name, self.ctx)
                    # force generator to evaluate
                    instancesLaunched = list(create_spot_instances(ec2=self.ctx.ec2,
                                                                   price=self.spotBids[nodeType],
                                                                   image_id=self._discoverAMI(self.ctx),
                                                                   tags={'clusterName': self.clusterName},
                                                                   spec=kwargs,
                                                                   num_instances=numNodes,
                                                                   tentative=True)
                                             )
                    # flatten the list
                    instancesLaunched = [item for sublist in instancesLaunched for item in sublist]

        for attempt in retry(predicate=AWSProvisioner._throttlePredicate):
            with attempt:
                wait_instances_running(self.ctx.ec2, instancesLaunched)

        # request throttling retry happens internally to these two methods to insure proper granularity
        AWSProvisioner._addTags(instancesLaunched, self.tags)
        self._propagateKey(instancesLaunched)

        logger.info('Launched %s new instance(s)', numNodes)
        return len(instancesLaunched)

    def getProvisionedWorkers(self, nodeType, preemptable):
        entireCluster = self._getNodesInCluster(ctx=self.ctx, clusterName=self.clusterName, both=True, nodeType=nodeType)
        logger.debug('All nodes in cluster: %s', entireCluster)
        workerInstances = [i for i in entireCluster if i.private_ip_address != self.leaderIP]
        logger.debug('All workers found in cluster: %s', workerInstances)
        workerInstances = [i for i in workerInstances if preemptable != (i.spot_instance_request_id is None)]
        logger.debug('%spreemptable workers found in cluster: %s', 'non-' if not preemptable else '', workerInstances)
        workerInstances = awsFilterImpairedNodes(workerInstances, self.ctx.ec2)
        return [Node(publicIP=i.ip_address, privateIP=i.private_ip_address,
                     name=i.id, launchTime=i.launch_time, nodeType=i.instance_type,
                     preemptable=preemptable)
                for i in workerInstances]

    def _getClusterNameFromTags(self, md):
        """Retrieve cluster name from current instance tags
        """
        instance = self._getClusterInstance(md)
        return str(instance.tags["Name"])

    def _getClusterInstance(self, md):
        zone = getCurrentAWSZone()
        region = Context.availability_zone_re.match(zone).group(1)
        conn = boto.ec2.connect_to_region(region)
        for attempt in retry(predicate=AWSProvisioner._throttlePredicate):
            with attempt:
                return conn.get_all_instances(instance_ids=[md["instance-id"]])[0].instances[0]

    @staticmethod
    def _throttlePredicate(e):
        if not isinstance(e, BotoServerError):
            return False
        # boto/AWS gives multiple messages for the same error...
        if e.status == 503 and 'Request limit exceeded' in e.body:
            return True
        elif e.status == 400 and 'Rate exceeded' in e.body:
            return True
        return False

    def _setSSH(self):
        if not os.path.exists('/root/.sshSuccess'):
            subprocess.check_call(['ssh-keygen', '-f', '/root/.ssh/id_rsa', '-t', 'rsa', '-N', ''])
            with open('/root/.sshSuccess', 'w') as f:
                f.write('written here because of restrictive permissions on .ssh dir')
        os.chmod('/root/.ssh', 0o700)
        subprocess.check_call(['bash', '-c', 'eval $(ssh-agent) && ssh-add -k'])
        with open('/root/.ssh/id_rsa.pub') as f:
            masterPublicKey = f.read()
        masterPublicKey = masterPublicKey.split(' ')[1]  # take 'body' of key
        # confirm it really is an RSA public key
        assert masterPublicKey.startswith('AAAAB3NzaC1yc2E'), masterPublicKey
        return masterPublicKey

    @classmethod
    def _buildContext(cls, clusterName, zone=None):
        if zone is None:
            zone = getCurrentAWSZone()
            if zone is None:
                raise RuntimeError(
                    'Could not determine availability zone. Insure that one of the following '
                    'is true: the --zone flag is set, the TOIL_AWS_ZONE environment variable '
                    'is set, ec2_region_name is set in the .boto file, or that '
                    'you are running on EC2.')
        return Context(availability_zone=zone, namespace=cls._toNameSpace(clusterName))

    @classmethod
    @memoize
    def _discoverAMI(cls, ctx):
        def descriptionMatches(ami):
            return ami.description is not None and 'stable 1235.4.0' in ami.description
        coreOSAMI = os.environ.get('TOIL_AWS_AMI')
        if coreOSAMI is not None:
            return coreOSAMI
        # that ownerID corresponds to coreOS

        for attempt in retry(predicate= lambda e : isinstance(e, SSLError)):
            # SSLError is thrown when get_all_images times out
            with attempt:
                amis = ctx.ec2.get_all_images(owners=['679593333241'])

        coreOSAMI = [ami for ami in amis if descriptionMatches(ami)]
        logger.debug('Found the following matching AMIs: %s', coreOSAMI)
        assert len(coreOSAMI) == 1
        return coreOSAMI.pop().id

    @classmethod
    def dockerInfo(cls):
        try:
            return os.environ['TOIL_APPLIANCE_SELF']
        except KeyError:
            raise RuntimeError('Please set TOIL_APPLIANCE_SELF environment variable to the '
                               'image of the Toil Appliance you wish to use. For example: '
                               "'quay.io/ucsc_cgl/toil:3.5.0a1--80c340c5204bde016440e78e84350e3c13bd1801'. "
                               'See https://quay.io/repository/ucsc_cgl/toil-leader?tab=tags '
                               'for a full list of available versions.')

    @classmethod
    def _toNameSpace(cls, clusterName):
        assert isinstance(clusterName, (str, bytes))
        if any((char.isupper() for char in clusterName)) or '_' in clusterName:
            raise RuntimeError("The cluster name must be lowercase and cannot contain the '_' "
                               "character.")
        namespace = clusterName
        if not namespace.startswith('/'):
            namespace = '/' + namespace + '/'
        return namespace.replace('-', '/')

    @classmethod
    def _getLeader(cls, clusterName, wait=False, zone=None):
        ctx = cls._buildContext(clusterName=clusterName, zone=zone)
        instances = cls._getNodesInCluster(ctx, clusterName, nodeType=None, both=True)
        instances.sort(key=lambda x: x.launch_time)
        try:
            leader = instances[0]  # assume leader was launched first
        except IndexError:
            raise NoSuchClusterException(clusterName)
        if wait:
            logger.info("Waiting for toil_leader to enter 'running' state...")
            wait_instances_running(ctx.ec2, [leader])
            logger.info('... toil_leader is running')
            cls._waitForNode(leader, 'toil_leader')
        return leader

    @classmethod
    def _addTags(cls, instances, tags):
        for instance in instances:
            for key, value in iteritems(tags):
                for attempt in retry(predicate=AWSProvisioner._throttlePredicate):
                    with attempt:
                        instance.add_tag(key, value)

    @classmethod
    def _waitForNode(cls, instance, role):
        # returns the node's IP
        cls._waitForIP(instance)
        instanceIP = instance.public_dns_name
        cls._waitForSSHPort(instanceIP)
        cls._waitForSSHKeys(instanceIP)
        # wait here so docker commands can be used reliably afterwards
        cls._waitForDockerDaemon(instanceIP)
        cls._waitForAppliance(instanceIP, role=role)
        return instanceIP

    @classmethod
    def _waitForSSHKeys(cls, instanceIP):
        # the propagation of public ssh keys vs. opening the SSH port is racey, so this method blocks until
        # the keys are propagated and the instance can be SSH into
        while True:
            try:
                logger.info('Attempting to establish SSH connection...')
                cls._sshInstance(instanceIP, 'ps', sshOptions=['-oBatchMode=yes'])
            except RuntimeError:
                logger.info('Connection rejected, waiting for public SSH key to be propagated. Trying again in 10s.')
                time.sleep(10)
            else:
                logger.info('...SSH connection established.')
                # ssh succeeded
                return

    @classmethod
    def _waitForDockerDaemon(cls, ip_address):
        logger.info('Waiting for docker on %s to start...', ip_address)
        while True:
            output = cls._sshInstance(ip_address, '/usr/bin/ps', 'auxww')
            time.sleep(5)
            if 'dockerd' in output:
                # docker daemon has started
                break
            else:
                logger.info('... Still waiting...')
        logger.info('Docker daemon running')

    @classmethod
    def _waitForAppliance(cls, ip_address, role):
        logger.info('Waiting for %s Toil appliance to start...', role)
        while True:
            output = cls._sshInstance(ip_address, '/usr/bin/docker', 'ps')
            if role in output:
                logger.info('...Toil appliance started')
                break
            else:
                logger.info('...Still waiting, trying again in 10sec...')
                time.sleep(10)

    @classmethod
    def _waitForIP(cls, instance):
        """
        Wait until the instances has a public IP address assigned to it.

        :type instance: boto.ec2.instance.Instance
        """
        logger.info('Waiting for ip...')
        while True:
            time.sleep(a_short_time)
            instance.update()
            if instance.ip_address or instance.public_dns_name:
                logger.info('...got ip')
                break

    @classmethod
    def _terminateNodes(cls, nodes, ctx):
        instanceIDs = [x.name for x in nodes]
        cls._terminateIDs(instanceIDs, ctx)

    @classmethod
    def _terminateInstances(cls, instances, ctx):
        instanceIDs = [x.id for x in instances]
        cls._terminateIDs(instanceIDs, ctx)
        logger.info('... Waiting for instance(s) to shut down...')
        for instance in instances:
            wait_transition(instance, {'pending', 'running', 'shutting-down'}, 'terminated')
        logger.info('Instance(s) terminated.')

    @classmethod
    def _terminateIDs(cls, instanceIDs, ctx):
        logger.info('Terminating instance(s): %s', instanceIDs)
        for attempt in retry(predicate=AWSProvisioner._throttlePredicate):
            with attempt:
                ctx.ec2.terminate_instances(instance_ids=instanceIDs)
        logger.info('Instance(s) terminated.')

    @classmethod
    def _deleteIAMProfiles(cls, instances, ctx):
        instanceProfiles = [x.instance_profile['arn'] for x in instances]
        for profile in instanceProfiles:
            # boto won't look things up by the ARN so we have to parse it to get
            # the profile name
            profileName = profile.rsplit('/')[-1]
            try:
                profileResult = ctx.iam.get_instance_profile(profileName)
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
                ctx.iam.remove_role_from_instance_profile(profileName, role)
            except BotoServerError as e:
                if e.status == 404:
                    pass
                else:
                    raise
            policyResults = ctx.iam.list_role_policies(role)
            policyResults = policyResults['list_role_policies_response']
            policyResults = policyResults['list_role_policies_result']
            policies = policyResults['policy_names']
            for policyName in policies:
                try:
                    ctx.iam.delete_role_policy(role, policyName)
                except BotoServerError as e:
                    if e.status == 404:
                        pass
                    else:
                        raise
            try:
                ctx.iam.delete_role(role)
            except BotoServerError as e:
                if e.status == 404:
                    pass
                else:
                    raise
            try:
                ctx.iam.delete_instance_profile(profileName)
            except BotoServerError as e:
                if e.status == 404:
                    pass
                else:
                    raise

    def _propagateKey(self, instances):
        if not self.config or not self.config.sseKey:
            return
        for node in instances:
            for attempt in retry(predicate=AWSProvisioner._throttlePredicate):
                with attempt:
                    # since we're going to be rsyncing into the appliance we need the appliance to be running first
                    ipAddress = self._waitForNode(node, 'toil_worker')
                    self._coreRsync(ipAddress, [self.config.sseKey, ':' + self.config.sseKey], applianceName='toil_worker')

    @classmethod
    def _getBlockDeviceMapping(cls, instanceType, rootVolSize=50):
        # determine number of ephemeral drives via cgcloud-lib (actually this is moved into toil's lib
        bdtKeys = [''] + ['/dev/xvd{}'.format(c) for c in string.lowercase[1:]]
        bdm = BlockDeviceMapping()
        # Change root volume size to allow for bigger Docker instances
        root_vol = BlockDeviceType(delete_on_termination=True)
        root_vol.size = rootVolSize
        bdm["/dev/xvda"] = root_vol
        # the first disk is already attached for us so start with 2nd.
        for disk in range(1, instanceType.disks + 1):
            bdm[bdtKeys[disk]] = BlockDeviceType(
                ephemeral_name='ephemeral{}'.format(disk - 1))  # ephemeral counts start at 0

        logger.debug('Device mapping: %s', bdm)
        return bdm

    @classmethod
    def _getNodesInCluster(cls, ctx, clusterName, nodeType=None, preemptable=False, both=False):
        for attempt in retry(predicate=AWSProvisioner._throttlePredicate):
            with attempt:
                pendingInstances = ctx.ec2.get_only_instances(filters={'instance.group-name': clusterName,
                                                                       'instance-state-name': 'pending'})
        for attempt in retry(predicate=AWSProvisioner._throttlePredicate):
            with attempt:
                runningInstances = ctx.ec2.get_only_instances(filters={'instance.group-name': clusterName,
                                                                       'instance-state-name': 'running'})
        if nodeType:
            pendingInstances = [instance for instance in pendingInstances if instance.instance_type == nodeType]
            runningInstances = [instance for instance in runningInstances if instance.instance_type == nodeType]
        instances = set(pendingInstances)
        if not preemptable and not both:
            return [x for x in instances.union(set(runningInstances)) if x.spot_instance_request_id is None]
        elif preemptable and not both:
            return [x for x in instances.union(set(runningInstances)) if x.spot_instance_request_id is not None]
        elif both:
            return [x for x in instances.union(set(runningInstances))]

    @classmethod
    def _getSpotRequestIDs(cls, ctx, clusterName):
        requests = ctx.ec2.get_all_spot_instance_requests()
        tags = ctx.ec2.get_all_tags({'tag:': {'clusterName': clusterName}})
        idsToCancel = [tag.id for tag in tags]
        return [request for request in requests if request.id in idsToCancel]

    @classmethod
    def _createSecurityGroup(cls, ctx, name, vpcSubnet=None):
        def groupNotFound(e):
            retry = (e.status == 400 and 'does not exist in default VPC' in e.body)
            return retry
        vpcId = None
        if vpcSubnet:
            conn = boto.connect_vpc(region=ctx.ec2.region)
            subnets = conn.get_all_subnets(subnet_ids=[vpcSubnet])
            if len(subnets) > 0:
                vpcId = subnets[0].vpc_id
        # security group create/get. ssh + all ports open within the group
        try:
            web = ctx.ec2.create_security_group(name, 'Toil appliance security group', vpc_id=vpcId)
        except EC2ResponseError as e:
            if e.status == 400 and 'already exists' in e.body:
                pass  # group exists- nothing to do
            else:
                raise
        else:
            for attempt in retry(predicate=groupNotFound, timeout=300):
                with attempt:
                    # open port 22 for ssh-ing
                    web.authorize(ip_protocol='tcp', from_port=22, to_port=22, cidr_ip='0.0.0.0/0')
            for attempt in retry(predicate=groupNotFound, timeout=300):
                with attempt:
                    # the following authorizes all TCP access within the web security group
                    web.authorize(ip_protocol='tcp', from_port=0, to_port=65535, src_group=web)
            for attempt in retry(predicate=groupNotFound, timeout=300):
                with attempt:
                    # We also want to open up UDP, both for user code and for the RealtimeLogger
                    web.authorize(ip_protocol='udp', from_port=0, to_port=65535, src_group=web)
        out = []
        for sg in ctx.ec2.get_all_security_groups():
            if sg.name == name and vpcId is None or sg.vpc_id == vpcId:
                out.append(sg)
        return out

    @classmethod
    def _getProfileARN(cls, ctx):
        def addRoleErrors(e):
            return e.status == 404

        def throttleError(e):
            return isinstance(e, BotoServerError) and e.status == 400 and e.error_code == 'Throttling'

        for attempt in retry(delays=truncExpBackoff(), predicate=throttleError):
            with attempt:
                roleName = 'toil'
                policy = dict(iam_full=iamFullPolicy, ec2_full=ec2FullPolicy,
                              s3_full=s3FullPolicy, sbd_full=sdbFullPolicy)
                iamRoleName = ctx.setup_iam_ec2_role(role_name=roleName, policies=policy)

                try:
                    profile = ctx.iam.get_instance_profile(iamRoleName)
                except BotoServerError as e:
                    if e.status == 404:
                        profile = ctx.iam.create_instance_profile(iamRoleName)
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
                        ctx.iam.remove_role_from_instance_profile(iamRoleName,
                                                                  profile.roles.member.role_name)
                for attempt in retry(predicate=addRoleErrors):
                    with attempt:
                        ctx.iam.add_role_to_instance_profile(iamRoleName, iamRoleName)
        return profile_arn
