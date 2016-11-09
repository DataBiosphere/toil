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

import sys
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from boto.exception import BotoServerError, EC2ResponseError
from cgcloud.lib.ec2 import (ec2_instance_types, retry_ec2, wait_spot_requests_active, a_short_time,
                             wait_transition, inconsistencies_detected, create_ondemand_instances,
                             a_long_time, create_spot_instances)
from itertools import islice, count

from toil import applianceSelf
from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.aws import *
from cgcloud.lib.context import Context
from boto.utils import get_instance_metadata
from bd2k.util.retry import retry
from toil.provisioners import BaseAWSProvisioner

logger = logging.getLogger(__name__)


class AWSProvisioner(AbstractProvisioner, BaseAWSProvisioner):

    def __init__(self, config, batchSystem):
        super(AWSProvisioner, self).__init__(config, batchSystem)
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
        self.leaderIP = self.instanceMetaData['local-ipv4']
        self.keyName = self.instanceMetaData['public-keys'].keys()[0]

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
        tty = sys.stdin.isatty()
        cls._sshAppliance(leader.ip_address, 'bash', tty=tty)

    @classmethod
    def _sshAppliance(cls, leaderIP, command, tty=False):
        ttyFlag = 't' if tty else ''
        command = 'ssh -o "StrictHostKeyChecking=no" -t core@%s "docker exec -i%s leader %s"' % (leaderIP, ttyFlag, command)
        return subprocess.check_call(command, shell=True)

    @classmethod
    def _sshInstance(cls, leaderIP, command):
        command = 'ssh -o "StrictHostKeyChecking=no" -t core@%s "%s"' % (leaderIP, command)
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
        leaderData = dict(role='leader',
                          image=applianceSelf(),
                          entrypoint='mesos-master',
                          args=leaderArgs.format(name=clusterName))
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
            list(create_spot_instances(ec2=ctx.ec2,
                                       price=spotBid,
                                       image_id=coreOSAMI,
                                       tags={'clusterName': clusterName},
                                       spec=kwargs,
                                       num_instances=1))
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
            cls._terminateInstances(instances=instances, ctx=ctx)
        logger.info('Deleting security group...')
        for attempt in retry_ec2(retry_after=30, retry_for=300, retry_while=expectedShutdownErrors):
            with attempt:
                ctx.ec2.delete_security_group(name=clusterName)
        logger.info('... Succesfully deleted security group')

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

    def _addNodes(self, instances, numNodes, preemptable=False):
        bdm = self._getBlockDeviceMapping(self.instanceType)
        arn = self._getProfileARN(self.ctx)
        workerData = dict(role='worker',
                          image=applianceSelf(),
                          entrypoint='mesos-slave',
                          args=workerArgs.format(ip=self.leaderIP, preemptable=preemptable))
        userData = awsUserData.format(**workerData)
        kwargs = {'key_name': self.keyName, 'security_groups': [self.clusterName],
                  'instance_type': self.instanceType.name,
                  'user_data': userData, 'block_device_map': bdm,
                  'instance_profile_arn': arn}

        if not preemptable:
            logger.info('Launching %s non-preemptable nodes', numNodes)
            create_ondemand_instances(self.ctx.ec2, image_id=coreOSAMI,
                                      spec=kwargs, num_instances=1)
        else:
            logger.info('Launching %s preemptable nodes', numNodes)
            # force generator to evaluate
            list(create_spot_instances(ec2=self.ctx.ec2,
                                       price=self.spotBid,
                                       image_id=coreOSAMI,
                                       tags={'clusterName': self.clusterName},
                                       spec=kwargs,
                                       num_instances=numNodes))
        logger.info('Launched %s new instance(s)', numNodes)

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
