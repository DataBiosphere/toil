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
import os
import socket
import subprocess
import logging

import time
from contextlib import contextmanager

from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from boto.exception import BotoServerError, EC2ResponseError
from cgcloud.lib.ec2 import (ec2_instance_types, retry_ec2, wait_spot_requests_active, a_short_time,
                             wait_transition, inconsistencies_detected, create_ondemand_instances,
                             a_long_time)
from itertools import islice, count

from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.aws import *
from cgcloud.lib.context import Context
from boto.utils import get_instance_metadata
from bd2k.util.retry import retry
from toil.provisioners import BaseAWSProvisioner

logger = logging.getLogger(__name__)


class AWSProvisioner(AbstractProvisioner, BaseAWSProvisioner):

    dockerInfo = os.environ.get('TOIL_APPLIANCE_SELF')

    def __init__(self, config, batchSystem):
        self.instanceMetaData = get_instance_metadata()
        self.clusterName = self.instanceMetaData['security-groups']
        self.ctx = Context(availability_zone='us-west-2a', namespace=self._toNameSpace(self.clusterName))
        self.spotBid = None
        assert config.preemptableNodeType or config.nodeType
        if config.preemptableNodeType is not None:
            nodeBidTuple = config.preemptableNodeType.split(':', 1)
            self.spotBid = nodeBidTuple[1]
            self.instanceType = ec2_instance_types[nodeBidTuple[0]]
        else:
            self.instanceType = ec2_instance_types[config.nodeType]
        self.batchSystem = batchSystem
        self.leaderIP = self.instanceMetaData['local-ipv4']
        self.keyName = self.instanceMetaData['public-keys'].keys()[0]

    def setNodeCount(self, numNodes, preemptable=False, force=False):
        # get all nodes in cluster
        workerInstances = self._getWorkersInCluster(preemptable)
        instancesToLaunch = numNodes - len(workerInstances)
        logger.info('Adjusting cluster size by %s', instancesToLaunch)
        if instancesToLaunch > 0:
            self._addNodes(instancesToLaunch, preemptable=preemptable)
        elif instancesToLaunch < 0:
            self._removeNodes(instances=workerInstances, numNodes=numNodes, preemptable=preemptable, force=force)
        else:
            pass
        workerInstances = self._getWorkersInCluster(preemptable)
        return len(workerInstances)

    def getNodeShape(self, preemptable=False):
        instanceType = self.instanceType
        return Shape(wallTime=60 * 60,
                     memory=instanceType.memory * 2 ** 30,
                     cores=instanceType.cores,
                     disk=(instanceType.disks * instanceType.disk_capacity * 2 ** 30))

    @classmethod
    def sshLeader(cls, clusterName):
        leader = cls._getLeader(clusterName)
        logger.info('SSH ready')
        cls._sshAppliance(leader.ip_address, 'bash')

    @classmethod
    def _sshAppliance(cls, leaderIP, command):
        command = "ssh -o \"StrictHostKeyChecking=no\" -t core@%s \"docker exec -i leader %s\"" % (leaderIP, command)
        return subprocess.check_call(command, shell=True)

    @classmethod
    def _sshInstance(cls, leaderIP, command):
        command = "ssh -o \"StrictHostKeyChecking=no\" -t core@%s \"%s\"" % (leaderIP, command)
        ouput = subprocess.check_output(command, shell=True)
        return ouput

    @classmethod
    def _toNameSpace(cls, clusterName):
        if not clusterName.startswith('/'):
            clusterName = '/'+clusterName+'/'
        return clusterName.replace('-','/')

    @classmethod
    def _getLeader(cls, clusterName, wait=False):
        ctx = Context(availability_zone='us-west-2a', namespace=cls._toNameSpace(clusterName))
        instances = cls.__getNodesInCluster(ctx, clusterName, both=True)
        instances.sort(key=lambda x: x.launch_time)
        leader = instances[0]  # assume leader was launched first
        if wait:
            logger.info("Waiting for leader to enter 'running' state...")
            wait_transition(leader, {'pending'}, 'running')
            logger.info('... leader is running')
            cls._waitForIP(leader)
            leaderIP = leader.ip_address
            cls._waitForSSHPort(leaderIP)
            # wait here so docker commands can be used reliably afterwards
            cls._waitForDockerDaemon(leaderIP)
            cls._waitForAppliance(leaderIP)
        return leader

    @classmethod
    def _waitForAppliance(cls, ip_address):
        logger.info('Waiting for leader Toil appliance to start...')
        while True:
            output = cls._sshInstance(leaderIP=ip_address, command='docker ps')
            if 'leader' in output:
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
        logger.info('Waiting for leader ip...')
        while True:
            time.sleep(a_short_time)
            instance.update()
            if instance.ip_address or instance.public_dns_name:
                logger.info('...got leader ip')
                break

    @classmethod
    def _waitForDockerDaemon(cls, ip_address):
        logger.info('Waiting for docker to start...')
        command = 'ps aux | grep \\"docker daemon\\"'
        while True:
            output = cls._sshInstance(ip_address, command)
            time.sleep(5)
            if 'root' in output:
                # ps aux | grep x will always list itself. The actual docker daemon process will
                # be started by root whereas the ssh-ed command will be executed by the user 'core'
                break
            else:
                logger.info('... Still waiting...')
        logger.info('Docker daemon running')

    @classmethod
    def _waitForSSHPort(cls, ip_address):
        """
        Wait until the instance represented by this box is accessible via SSH.

        :return: the number of unsuccessful attempts to connect to the port before a the first
        success
        """
        logger.info('Waiting for leader ssh port to open...')
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
    def launchCluster(cls, instanceType, keyName, clusterName, spotBid=None):
        ctx = Context(availability_zone='us-west-2a', namespace=cls._toNameSpace(clusterName))
        profileARN = cls._getProfileARN(ctx)
        # the security group name is used as the cluster identifier
        cls._createSecurityGroup(ctx, clusterName)
        bdm = cls._getBlockDeviceMapping(ec2_instance_types[instanceType])
        dockerLeaderData = cls.dockerInfo.rsplit(':', 1)
        leaderRepo = dockerLeaderData[0]
        leaderTag = dockerLeaderData[1]
        leaderData = {'role': 'leader', 'tag': leaderTag,
                      'args': leaderArgs.format(name=clusterName), 'repo': leaderRepo}
        userData = awsUserData.format(**leaderData)
        kwargs = {'key_name': keyName, 'security_groups': [clusterName],
                  'instance_type': instanceType,
                  'user_data': userData, 'block_device_map': bdm,
                  'instance_profile_arn': profileARN}
        if not spotBid:
            logger.info('Launching non-preemptable leader')
            create_ondemand_instances(ctx.ec2, image_id=coreOSAMI,
                                      spec=kwargs, num_instances=1)
        else:
            logger.info('Launching preemptable leader')
            # force generator to evaluate
            list(create_spot_instances(ec2=ctx.ec2, price=spotBid, image_id=coreOSAMI,
                                       clusterName=clusterName, spec=kwargs, num_instances=1))
        return cls._getLeader(clusterName=clusterName, wait=True)

    @classmethod
    def destroyCluster(cls, clusterName):
        def expectedShutdownErrors(e):
            return e.status == 400 and 'dependent object' in e.body

        ctx = Context(availability_zone='us-west-2a', namespace=cls._toNameSpace(clusterName))
        instances = cls.__getNodesInCluster(ctx, clusterName, both=True)
        spotIDs = cls._getSpotRequestIDs(ctx, clusterName)
        if spotIDs:
            ctx.ec2.cancel_spot_instance_requests(request_ids=spotIDs)
        if instances:
            cls._deleteIAMProfiles(instances=instances, ctx=ctx)
            cls._terminateInstance(instances=instances, ctx=ctx)
        logger.info('Deleting security group...')
        for attempt in retry_ec2(retry_after=30, retry_for=300, retry_while=expectedShutdownErrors):
            with attempt:
                ctx.ec2.delete_security_group(name=clusterName)
        logger.info('... Succesfully deleted security group')

    @classmethod
    def _terminateInstance(cls, instances, ctx):
        instanceIDs = [x.id for x in instances]
        logger.info('Terminating instance(s): %s', instanceIDs)
        ctx.ec2.terminate_instances(instance_ids=instanceIDs)
        logger.info('Instance(s) terminated.')

    @classmethod
    def _deleteIAMProfiles(cls, instances, ctx):
        instanceProfiles = [x.instance_profile for x in instances]
        for profile in instanceProfiles:
            profile_name = profile['arn'].split('/', 1)[1]
            try:
                ctx.iam.remove_role_from_instance_profile(profile_name, profile_name)
            except BotoServerError as e:
                if e.status == 404:
                    pass
                else:
                    raise
            try:
                ctx.iam.delete_instance_profile(profile_name)
            except BotoServerError as e:
                if e.status == 404:
                    pass
                else:
                    raise

    def _addNodes(self, instancesToLaunch, preemptable=False):
        bdm = self._getBlockDeviceMapping(self.instanceType)
        arn = self._getProfileARN(self.ctx)
        # quay.io/toil-leader:tag
        workerData = self.dockerInfo.rsplit(':', 1)
        workerRepo = workerData[0].rsplit('-', 1)[0] + '-worker'
        workerTag = workerData[1]
        workerData = {'role': 'worker', 'tag': workerTag,
                      'args': workerArgs.format(ip=self.leaderIP, preemptable=preemptable),
                      'repo': workerRepo}
        userData = awsUserData.format(**workerData)
        kwargs = {'key_name': self.keyName, 'security_groups': [self.clusterName],
                  'instance_type': self.instanceType.name,
                  'user_data': userData, 'block_device_map': bdm,
                  'instance_profile_arn': arn}

        if not preemptable:
            logger.info('Launching %s non-preemptable nodes', instancesToLaunch)
            create_ondemand_instances(self.ctx.ec2, image_id=coreOSAMI,
                                      spec=kwargs, num_instances=1)
        else:
            logger.info('Launching %s preemptable nodes', instancesToLaunch)
            # force generator to evaluate
            list(create_spot_instances(ec2=self.ctx.ec2, price=self.spotBid, image_id=coreOSAMI,
                                       clusterName=self.clusterName, spec=kwargs, num_instances=instancesToLaunch))
        logger.info('Launched %s new instance(s)', instancesToLaunch)

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

    def _removeNodes(self, instances, numNodes, preemptable=False, force=False):
        # based off toil.provisioners.cgcloud.provisioner.CGCloudProvisioner._removeNodes()
        logger.debug('Attempting to delete nodes - force = %s', force)
        # If the batch system is scalable, we can use the number of currently running workers on
        # each node as the primary criterion to select which nodes to terminate.
        if isinstance(self.batchSystem, AbstractScalableBatchSystem):
            logger.debug('Using a scalable batch system')
            nodes = self.batchSystem.getNodes(preemptable)
            # Join nodes and instances on private IP address.
            nodes = [(instance, nodes.get(instance.private_ip_address)) for instance in instances]
            # Unless forced, exclude nodes with runnning workers. Note that it is possible for
            # the batch system to report stale nodes for which the corresponding instance was
            # terminated already. There can also be instances that the batch system doesn't have
            # nodes for yet. We'll ignore those, too, unless forced.
            nodes = [(instance, nodeInfo)
                     for instance, nodeInfo in nodes
                     if force or nodeInfo is not None and nodeInfo.workers < 1]
            # Sort nodes by number of workers and time left in billing cycle
            nodes.sort(key=lambda (instance, nodeInfo): (
                nodeInfo.workers if nodeInfo else 1,
                self._remainingBillingInterval(instance)))
            nodes = nodes[numNodes:]
            instancesTerminate = [instance for instance, nodeInfo in nodes]
        else:
            # Without load info all we can do is sort instances by time left in billing cycle.
            instances = sorted(instances,
                               key=lambda instance: (self._remainingBillingInterval(instance)))
            instancesTerminate = [instance for instance in islice(instances, numNodes)]
        if instancesTerminate:
            self._terminateInstance(instances=instancesTerminate, ctx=self.ctx)
        else:
            logger.debug('No nodes to delete')
        return len(instancesTerminate)

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
        logger.debug('Workers found in cluster after filtering %s', workerInstances)
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
            for attempt in retry(predicate=groupNotFound, timeout=300):
                with attempt:
                    # open port 5050-5051 for mesos web interface
                    web.authorize(ip_protocol='tcp', from_port=5050, to_port=5051, cidr_ip='0.0.0.0/0')

    @classmethod
    def _getProfileARN(cls, ctx):
        def addRoleErrors(e):
            return e.status == 404
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


def create_spot_instances(ec2, price, image_id, spec, clusterName,
                          num_instances=1, timeout=None, tentative=False):
    """
    Adapted from cgcloud.lib.ec2.create_spot_instances to tag spot requests with the cluster name
    so they can be discovered and cleaned up at a later time

    :rtype: Iterator[list[Instance]]
    """
    for attempt in retry_ec2(retry_for=a_long_time,
                             retry_while=inconsistencies_detected):
        with attempt:
            requests = ec2.request_spot_instances(price, image_id, count=num_instances, **spec)

    ec2.create_tags([x.id for x in requests], {'clusterName': clusterName})
    num_active, num_other = 0, 0
    # noinspection PyUnboundLocalVariable,PyTypeChecker
    # request_spot_instances's type annotation is wrong
    for batch in wait_spot_requests_active(ec2,
                                           requests,
                                           timeout=timeout,
                                           tentative=tentative):
        instance_ids = []
        for request in batch:
            if request.state == 'active':
                instance_ids.append(request.instance_id)
                num_active += 1
            else:
                logger.info('Request %s in unexpected state %s.', request.id, request.state)
                num_other += 1
        if instance_ids:
            # This next line is the reason we batch. It's so we can get multiple instances in
            # a single request.
            yield ec2.get_only_instances(instance_ids)
    if not num_active:
        message = 'None of the spot requests entered the active state'
        if tentative:
            logger.warn(message + '.')
        else:
            raise RuntimeError(message)
    if num_other:
        logger.warn('%i request(s) entered a state other than active.', num_other)
