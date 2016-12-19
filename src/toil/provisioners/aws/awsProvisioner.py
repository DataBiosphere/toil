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
from StringIO import StringIO
import pipes
import socket
import subprocess
import logging

import time

import sys
from bd2k.util import memoize
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from boto.exception import BotoServerError, EC2ResponseError
from cgcloud.lib.ec2 import (ec2_instance_types, a_short_time,
                             wait_transition, create_ondemand_instances,
                             create_spot_instances, wait_instances_running)
from itertools import count

from toil import applianceSelf
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.aws import *
from cgcloud.lib.context import Context
from boto.utils import get_instance_metadata
from bd2k.util.retry import retry
from toil.provisioners import awsRemainingBillingInterval, awsFilterImpairedNodes

logger = logging.getLogger(__name__)


class AWSProvisioner(AbstractProvisioner):

    def __init__(self, config, batchSystem):
        super(AWSProvisioner, self).__init__(config, batchSystem)
        self.instanceMetaData = get_instance_metadata()
        self.clusterName = self.instanceMetaData['security-groups']
        self.ctx = self._buildContext(clusterName=self.clusterName)
        self.spotBid = None
        assert config.preemptableNodeType or config.nodeType
        if config.preemptableNodeType is not None:
            nodeBidTuple = config.preemptableNodeType.split(':', 1)
            self.spotBid = nodeBidTuple[1]
            self.instanceType = ec2_instance_types[nodeBidTuple[0]]
        else:
            self.instanceType = ec2_instance_types[config.nodeType]
        self.leaderIP = self.instanceMetaData['local-ipv4']
        self.keyName = self.instanceMetaData['public-keys'].keys()[0]
        self.masterPublicKey = self.setSSH()

    def setSSH(self):
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

    def getNodeShape(self, preemptable=False):
        instanceType = self.instanceType
        return Shape(wallTime=60 * 60,
                     memory=instanceType.memory * 2 ** 30,
                     cores=instanceType.cores,
                     disk=(instanceType.disks * instanceType.disk_capacity * 2 ** 30))

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
    def sshLeader(cls, clusterName, args=None, zone=None, **kwargs):
        leader = cls._getLeader(clusterName)
        logger.info('SSH ready')
        kwargs['tty'] = sys.stdin.isatty()
        command = args if args else ['bash']
        cls._sshAppliance(leader.ip_address, *command, **kwargs)

    def _remainingBillingInterval(self, instance):
        return awsRemainingBillingInterval(instance)

    @classmethod
    @memoize
    def _discoverAMI(cls, ctx):
        def descriptionMatches(ami):
            return ami.description is not None and 'stable 1068.9.0' in ami.description
        coreOSAMI = os.environ.get('TOIL_AWS_AMI')
        if coreOSAMI is not None:
            return coreOSAMI
        # that ownerID corresponds to coreOS
        coreOSAMI = [ami for ami in ctx.ec2.get_all_images(owners=['679593333241']) if
                     descriptionMatches(ami)]
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
    def _sshAppliance(cls, leaderIP, *args, **kwargs):
        """
        :param str leaderIP: IP of the master
        :param args: arguments to execute in the appliance
        :param kwargs: tty=bool tells docker whether or not to create a TTY shell for
            interactive SSHing. The default value is False. Input=string is passed as
            input to the Popen call.
        """
        kwargs['appliance'] = True
        return cls._coreSSH(leaderIP, *args, **kwargs)


    @classmethod
    def _sshInstance(cls, nodeIP, *args, **kwargs):
        kwargs['collectStdout'] = True
        return cls._coreSSH(nodeIP, *args, **kwargs)

    @classmethod
    def _coreSSH(cls, nodeIP, *args, **kwargs):
        """
        kwargs: input, tty, appliance, collectStdout
        """
        commandTokens = ['ssh', '-o', "StrictHostKeyChecking=no", '-t', 'core@%s' % nodeIP]
        appliance = kwargs.pop('appliance', None)
        if appliance:
            # run the args in the appliance
            tty = kwargs.pop('tty', None)
            ttyFlag = '-t' if tty else ''
            commandTokens += ['docker', 'exec', '-i', ttyFlag, 'toil_leader']
        inputString = kwargs.pop('input', None)
        if inputString is not None:
            kwargs['stdin'] = subprocess.PIPE
        collectStdout = kwargs.pop('collectStdout', None)
        if collectStdout:
            kwargs['stdout'] = subprocess.PIPE
        logger.debug('Node %s: %s', nodeIP, ' '.join(args))
        args = map(pipes.quote, args)
        commandTokens += args
        logger.debug('Full command %s', ' '.join(commandTokens))
        popen = subprocess.Popen(commandTokens, **kwargs)
        stdout, stderr = popen.communicate(input=inputString)
        # at this point the process has already exited, no need for a timeout
        resultValue = popen.wait()
        if resultValue != 0:
            raise RuntimeError('Executing the command "%s" on the appliance returned a non-zero '
                               'exit code %s with stdout %s and stderr %s' % (' '.join(args), resultValue, stdout, stderr))
        assert stderr is None
        return stdout

    @classmethod
    def rsyncLeader(cls, clusterName, args):
        leader = cls._getLeader(clusterName)
        cls._rsyncNode(leader.ip_address, args)

    @classmethod
    def _rsyncNode(cls, ip, args, applianceName='toil_leader'):
        sshCommand = 'ssh -o "StrictHostKeyChecking=no"'  # Skip host key checking
        remoteRsync = "docker exec -i %s rsync" % applianceName  # Access rsync inside appliance
        parsedArgs = []
        hostInserted = False
        # Insert remote host address
        for i in args:
            if i.startswith(":") and not hostInserted:
                i = ("core@%s" % ip) + i
                hostInserted = True
            elif i.startswith(":") and hostInserted:
                raise ValueError("Cannot rsync between two remote hosts")
            parsedArgs.append(i)
        if not hostInserted:
            raise ValueError("No remote host found in argument list")
        command = ['rsync', '-e', sshCommand, '--rsync-path', remoteRsync]
        logger.debug("Running %r.", command + parsedArgs)

        return subprocess.check_call(command + parsedArgs)

    @classmethod
    def _toNameSpace(cls, clusterName):
        assert isinstance(clusterName, str)
        if any((char.isupper() for char in clusterName)) or '_' in clusterName:
            raise RuntimeError("The cluster name must be lowercase and cannot contain the '_' "
                               "character.")
        namespace = clusterName
        if not namespace.startswith('/'):
            namespace = '/'+namespace+'/'
        return namespace.replace('-','/')

    @classmethod
    def _getLeader(cls, clusterName, wait=False, zone=None):
        ctx = cls._buildContext(clusterName=clusterName, zone=zone)
        instances = cls.__getNodesInCluster(ctx, clusterName, both=True)
        instances.sort(key=lambda x: x.launch_time)
        leader = instances[0]  # assume leader was launched first
        if wait:
            logger.info("Waiting for toil_leader to enter 'running' state...")
            wait_transition(leader, {'pending'}, 'running')
            logger.info('... toil_leader is running')
            cls._waitForNode(leader, 'toil_leader')
        return leader

    @classmethod
    def _waitForNode(cls, instance, role):
        # returns the node's IP
        cls._waitForIP(instance)
        instanceIP = instance.ip_address
        cls._waitForSSHPort(instanceIP)
        # wait here so docker commands can be used reliably afterwards
        cls._waitForDockerDaemon(instanceIP)
        cls._waitForAppliance(instanceIP, role=role)
        return instanceIP

    @classmethod
    def _waitForDockerDaemon(cls, ip_address):
        logger.info('Waiting for docker on %s to start...', ip_address)
        while True:
            output = cls._sshInstance(ip_address, '/usr/bin/ps', 'aux')
            time.sleep(5)
            if 'docker daemon' in output:
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
    def _waitForSSHPort(cls, ip_address):
        """
        Wait until the instance represented by this box is accessible via SSH.

        :return: the number of unsuccessful attempts to connect to the port before a the first
        success
        """
        logger.info('Waiting for ssh port to open...')
        for i in count():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.settimeout(a_short_time)
                s.connect((ip_address, 22))
                logger.info('...ssh port open')
                return i
            except socket.error:
                pass
            finally:
                s.close()

    @classmethod
    def launchCluster(cls, instanceType, keyName, clusterName, spotBid=None, zone=None):
        ctx = cls._buildContext(clusterName=clusterName, zone=zone)
        profileARN = cls._getProfileARN(ctx)
        # the security group name is used as the cluster identifier
        cls._createSecurityGroup(ctx, clusterName)
        bdm = cls._getBlockDeviceMapping(ec2_instance_types[instanceType])
        leaderData = dict(role='leader',
                          image=applianceSelf(),
                          entrypoint='mesos-master',
                          sshKey='AAAAB3NzaC1yc2Enoauthorizedkeyneeded',
                          args=leaderArgs.format(name=clusterName))
        userData = awsUserData.format(**leaderData)
        kwargs = {'key_name': keyName, 'security_groups': [clusterName],
                  'instance_type': instanceType,
                  'user_data': userData, 'block_device_map': bdm,
                  'instance_profile_arn': profileARN}
        if not spotBid:
            logger.info('Launching non-preemptable leader')
            create_ondemand_instances(ctx.ec2, image_id=cls._discoverAMI(ctx),
                                      spec=kwargs, num_instances=1)
        else:
            logger.info('Launching preemptable leader')
            # force generator to evaluate
            list(create_spot_instances(ec2=ctx.ec2,
                                       price=spotBid,
                                       image_id=cls._discoverAMI(ctx),
                                       tags={'clusterName': clusterName},
                                       spec=kwargs,
                                       num_instances=1))
        return cls._getLeader(clusterName=clusterName, wait=True)

    @classmethod
    def destroyCluster(cls, clusterName, zone=None):
        def expectedShutdownErrors(e):
            return e.status == 400 and 'dependent object' in e.body

        ctx = cls._buildContext(clusterName=clusterName, zone=zone)
        instances = cls.__getNodesInCluster(ctx, clusterName, both=True)
        spotIDs = cls._getSpotRequestIDs(ctx, clusterName)
        if spotIDs:
            ctx.ec2.cancel_spot_instance_requests(request_ids=spotIDs)
        instancesToTerminate = awsFilterImpairedNodes(instances, ctx.ec2)
        if instancesToTerminate:
            cls._deleteIAMProfiles(instances=instancesToTerminate, ctx=ctx)
            cls._terminateInstances(instances=instancesToTerminate, ctx=ctx)
        if len(instances) == len(instancesToTerminate):
            logger.info('Deleting security group...')
            for attempt in retry(timeout=300, predicate=expectedShutdownErrors):
                with attempt:
                    try:
                        ctx.ec2.delete_security_group(name=clusterName)
                    except BotoServerError as e:
                        if e.error_code == 'InvalidGroup.NotFound':
                            pass
                        else:
                            raise
            logger.info('... Succesfully deleted security group')
        else:
            assert len(instances) > len(instancesToTerminate)
            # the security group can't be deleted until all nodes are terminated
            logger.warning('The TOIL_AWS_NODE_DEBUG environment variable is set and some nodes '
                           'have failed health checks. As a result, the security group & IAM '
                           'roles will not be deleted.')

    @classmethod
    def _terminateInstances(cls, instances, ctx):
        instanceIDs = [x.id for x in instances]
        cls._terminateIDs(instanceIDs, ctx)

    @classmethod
    def _terminateIDs(cls, instanceIDs, ctx):
        logger.info('Terminating instance(s): %s', instanceIDs)
        ctx.ec2.terminate_instances(instance_ids=instanceIDs)
        logger.info('Instance(s) terminated.')

    def _logAndTerminate(self, instanceIDs):
        self._terminateIDs(instanceIDs, self.ctx)

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

    def _addNodes(self, instances, numNodes, preemptable=False):
        bdm = self._getBlockDeviceMapping(self.instanceType)
        arn = self._getProfileARN(self.ctx)
        keyPath = '' if not self.config.sseKey else self.config.sseKey
        entryPoint = 'mesos-slave' if not self.config.sseKey else "waitForKey.sh"
        workerData = dict(role='worker',
                          image=applianceSelf(),
                          entrypoint=entryPoint,
                          sshKey=self.masterPublicKey,
                          args=workerArgs.format(ip=self.leaderIP, preemptable=preemptable, keyPath=keyPath))
        userData = awsUserData.format(**workerData)
        kwargs = {'key_name': self.keyName,
                  'security_groups': [self.clusterName],
                  'instance_type': self.instanceType.name,
                  'user_data': userData,
                  'block_device_map': bdm,
                  'instance_profile_arn': arn}

        instancesLaunched = []

        if not preemptable:
            logger.info('Launching %s non-preemptable nodes', numNodes)
            instancesLaunched = create_ondemand_instances(self.ctx.ec2, image_id=self._discoverAMI(self.ctx),
                                      spec=kwargs, num_instances=1)
        else:
            logger.info('Launching %s preemptable nodes', numNodes)
            kwargs['placement'] = getSpotZone(self.spotBid, self.instanceType.name, self.ctx)
            # force generator to evaluate
            instancesLaunched = list(create_spot_instances(ec2=self.ctx.ec2,
                                                           price=self.spotBid,
                                                           image_id=self._discoverAMI(self.ctx),
                                                           tags={'clusterName': self.clusterName},
                                                           spec=kwargs,
                                                           num_instances=numNodes,
                                                           tentative=True)
                                     )
            # flatten the list 
            instancesLaunched = [item for sublist in instancesLaunched for item in sublist]
        wait_instances_running(self.ctx.ec2, instancesLaunched)
        self._propagateKey(instancesLaunched)
        logger.info('Launched %s new instance(s)', numNodes)
        return len(instancesLaunched)

    def _propagateKey(self, instances):
        if not self.config.sseKey:
            return
        # wait 5 minutes so coreos has time to modify the ssh auth key file properly.
        logger.debug('Waiting for 5 minutes')
        time.sleep(5 * 60)
        for node in instances:
            # since we're going to be rsyncing into the appliance we need the appliance to be running first
            ipAddress = self._waitForNode(node, 'toil_worker')
            self._rsyncNode(ipAddress, [self.config.sseKey, ':' + self.config.sseKey], applianceName='toil_worker')

    @classmethod
    def _getBlockDeviceMapping(cls, instanceType):
        # determine number of ephemeral drives via cgcloud-lib
        bdtKeys = ['', '/dev/xvdb', '/dev/xvdc', '/dev/xvdd']
        bdm = BlockDeviceMapping()
        # the first disk is already attached for us so start with 2nd.
        for disk in xrange(1, instanceType.disks + 1):
            bdm[bdtKeys[disk]] = BlockDeviceType(
                ephemeral_name='ephemeral{}'.format(disk - 1))  # ephemeral counts start at 0

        logger.debug('Device mapping: %s', bdm)
        return bdm

    @classmethod
    def __getNodesInCluster(cls, ctx, clusterName, preemptable=False, both=False):
        pendingInstances = ctx.ec2.get_only_instances(filters={'instance.group-name': clusterName,
                                                               'instance-state-name': 'pending'})
        runningInstances = ctx.ec2.get_only_instances(filters={'instance.group-name': clusterName,
                                                               'instance-state-name': 'running'})
        instances = set(pendingInstances)
        if not preemptable and not both:
            return [x for x in instances.union(set(runningInstances)) if x.spot_instance_request_id is None]
        elif preemptable and not both:
            return [x for x in instances.union(set(runningInstances)) if x.spot_instance_request_id is not None]
        elif both:
            return [x for x in instances.union(set(runningInstances))]

    def _getNodesInCluster(self, preeptable=False, both=False):
        if not both:
            return self.__getNodesInCluster(self.ctx, self.clusterName, preemptable=preeptable)
        else:
            return self.__getNodesInCluster(self.ctx, self.clusterName, both=both)

    def _getWorkersInCluster(self, preemptable):
        entireCluster = self._getNodesInCluster(both=True)
        logger.debug('All nodes in cluster %s', entireCluster)
        workerInstances = [i for i in entireCluster if i.private_ip_address != self.leaderIP and
                           preemptable != (i.spot_instance_request_id is None)]
        logger.debug('Workers found in cluster %s', workerInstances)
        workerInstances = awsFilterImpairedNodes(workerInstances, self.ctx.ec2)
        return workerInstances

    @classmethod
    def _getSpotRequestIDs(cls, ctx, clusterName):
        requests = ctx.ec2.get_all_spot_instance_requests()
        tags = ctx.ec2.get_all_tags({'tag:': {'clusterName': clusterName}})
        idsToCancel = [tag.id for tag in tags]
        return [request for request in requests if request.id in idsToCancel]

    @classmethod
    def _createSecurityGroup(cls, ctx, name):
        def groupNotFound(e):
            retry = (e.status == 400 and 'does not exist in default VPC' in e.body)
            return retry

        # security group create/get. ssh + all ports open within the group
        try:
            web = ctx.ec2.create_security_group(name, 'Toil appliance security group')
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
                    # the following authorizes all port access within the web security group
                    web.authorize(ip_protocol='tcp', from_port=0, to_port=65535, src_group=web)

    @classmethod
    def _getProfileARN(cls, ctx):
        def addRoleErrors(e):
            return e.status == 404
        roleName = '-toil'
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
