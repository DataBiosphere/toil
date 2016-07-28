# Copyright (C) 2015-2016 Regents of the University of California
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

import datetime
import logging
import re
import time
from collections import Iterable
from urllib2 import urlopen

import boto.ec2
from bd2k.util import memoize, parse_iso_utc
from bd2k.util.exceptions import require
from bd2k.util.throttle import throttle
from boto.ec2.instance import Instance
from cgcloud.lib.ec2 import (ec2_instance_types,
                             create_spot_instances,
                             create_ondemand_instances,
                             tag_object_persistently)
from cgcloud.lib.util import (allocate_cluster_ordinals,
                              thread_pool)
from itertools import islice

from toil.batchSystems.abstractBatchSystem import (AbstractScalableBatchSystem,
                                                   AbstractBatchSystem)
from toil.common import Config
from toil.provisioners.abstractProvisioner import (AbstractProvisioner,
                                                   Shape)

log = logging.getLogger(__name__)

# The maximum time to allow for instance creation and nodes joining the cluster. Note that it may
# take twice that time to provision preemptable instances since the timeout is reset after spot
# instance creation.
#
provisioning_timeout = 10 * 60


class CGCloudProvisioner(AbstractProvisioner):
    """
    A provisioner that uses CGCloud's toil-box role to boot up worker nodes in EC2. It uses the
    spot market to provision preemptable instances, but defaults to on-demand instances.

    This provisioner assumes that

    * It is running the leader node, i.e. the EC2 instance that's running the Mesos master
      process and the Toil leader process.

    * The leader node was recreated from a toil-box image using CGCloud.

    * The version of cgcloud used to create the toil-box image is compatible with the one this
      provisioner, and therefore Toil depend on.

    * The SSH keypair, security group and user data applied to the leader also apply to the workers

    * An instance type with ephemeral volumes is being used (it asserts that assumption)
    """

    def __init__(self, config, batchSystem):
        """
        :type config: Config
        :type batchSystem: AbstractBatchSystem
        """
        super(CGCloudProvisioner, self).__init__()
        self.batchSystem = batchSystem
        self.imageId = self._instance.image_id
        require(config.nodeType, 'Must pass --nodeType when using the cgcloud provisioner')
        instanceType = self._resolveInstanceType(config.nodeType)
        self._requireEphemeralDrives(instanceType)
        if config.preemptableNodeType:
            try:
                preemptableInstanceType, spotBid = config.preemptableNodeType.split(':')
            except ValueError:
                raise ValueError("Preemptible node type '%s' is not valid for this provisioner. "
                                 "Use format INSTANCE_TYPE:SPOT_BID, e.g. m3.large:0.10 instead"
                                 % config.preemptableNodeType)
            preemptableInstanceType = self._resolveInstanceType(preemptableInstanceType)
            self._requireEphemeralDrives(preemptableInstanceType)
            try:
                self.spotBid = float(spotBid)
            except ValueError:
                raise ValueError("The spot bid '%s' is not valid. Use a floating point dollar "
                                 "amount such as '0.42' instead." % spotBid)
        else:
            preemptableInstanceType, self.spotBid = None, None
        self.instanceType = {False: instanceType, True: preemptableInstanceType}

    def _requireEphemeralDrives(self, workerType):
        require(workerType.disks > 0,
                "This provisioner only supports instance types with one or more ephemeral "
                "volumes. The requested type '%s' does not have any.", workerType.name)
        leaderType = self._resolveInstanceType(self._instance.instance_type)
        require(workerType.disks == leaderType.disks,
                'The instance type selected for worker nodes (%s) offers %i ephemeral volumes but '
                'this type of leader (%s) has %i. The number of drives must match between leader '
                'and worker nodes. Please specify a different worker node type or use a different '
                'leader.', workerType.name, workerType.disks, leaderType.name, leaderType.disks)

    def _resolveInstanceType(self, instanceType):
        """
        :param str instanceType: the instance type as a string, e.g. 'm3.large'
        :rtype: cgcloud.lib.ec2.InstanceType
        """
        try:
            return ec2_instance_types[instanceType]
        except KeyError:
            raise RuntimeError("Invalid or unknown instance type '%s'" % instanceType)

    def setNodeCount(self, numNodes, preemptable=False, force=False):
        instances = list(self._getAllRunningInstances())
        workerInstances = [i for i in instances
                           if i.id != self._instanceId  # exclude leader
                           and preemptable != (i.spot_instance_request_id is None)]
        numCurrentNodes = len(workerInstances)
        delta = numNodes - numCurrentNodes
        if delta > 0:
            log.info('Adding %i nodes to get to desired cluster size of %i.', delta, numNodes)
            numNodes = numCurrentNodes + self._addNodes(workerInstances,
                                                        numNodes=delta,
                                                        preemptable=preemptable)
        elif delta < 0:
            log.info('Removing %i nodes to get to desired cluster size of %i.', -delta, numNodes)
            numNodes = numCurrentNodes - self._removeNodes(workerInstances,
                                                           numNodes=-delta,
                                                           preemptable=preemptable,
                                                           force=force)
        else:
            log.info('Cluster already at desired size of %i. Nothing to do.', numNodes)
        return numNodes

    def _addNodes(self, instances, numNodes, preemptable=False):
        deadline = time.time() + provisioning_timeout
        spec = dict(key_name=self._keyName,
                    user_data=self._userData(),
                    instance_type=self.instanceType[preemptable].name,
                    instance_profile_arn=self._instanceProfileArn,
                    security_group_ids=self._securityGroupIds,
                    ebs_optimized=self.ebsOptimized,
                    dry_run=False)
        # Offset the ordinals of the preemptable nodes to be disjunct from the non-preemptable
        # ones. Without this, the two scaler threads would inevitably allocate colliding ordinals.
        offset = 1000 if preemptable else 0
        used_ordinals = {int(i.tags['cluster_ordinal']) - offset for i in instances}
        # Since leader is absent from the instances iterable, we need to explicitly reserve its
        # ordinal unless we're allocating offset ordinals reserved for preemptable instances:
        assert len(used_ordinals) == len(instances)  # check for collisions
        if not preemptable:
            used_ordinals.add(0)
        ordinals = (ordinal + offset for ordinal in allocate_cluster_ordinals(num=numNodes,
                                                                              used=used_ordinals))

        def createInstances():
            """
            :rtype: Iterable[list[Instance]]
            """
            if preemptable:
                for batch in create_spot_instances(self._ec2, self.spotBid, self.imageId, spec,
                                                   # Don't insist on spot requests and don't raise
                                                   # if no requests were fulfilled:
                                                   tentative=True,
                                                   num_instances=numNodes,
                                                   timeout=deadline - time.time()):
                    yield batch
            else:
                yield create_ondemand_instances(self._ec2, self.imageId, spec,
                                                num_instances=numNodes)

        instancesByAddress = {}

        def handleInstance(instance):
            log.debug('Tagging instance %s.', instance.id)
            leader_tags = self._instance.tags
            name = leader_tags['Name'].replace('toil-leader', 'toil-worker')
            tag_object_persistently(instance, dict(leader_tags,
                                                   Name=name,
                                                   cluster_ordinal=next(ordinals)))
            assert instance.private_ip_address
            instancesByAddress[instance.private_ip_address] = instance

        # Each instance gets a different ordinal so we can't tag an entire batch at once but have
        # to tag each instance individually. It needs to be done quickly because the tags are
        # crucial for the boot code running inside the instance to join the cluster. Hence we do
        # it in a thread pool. If the pool is too large, we'll hit the EC2 limit on the number of
        # of concurrent requests. If it is too small, we won't be able to tag all instances in
        # time.
        with thread_pool(min(numNodes, 32)) as pool:
            for batch in createInstances():
                log.debug('Got a batch of %i instance(s).', len(batch))
                for instance in batch:
                    log.debug('Submitting instance %s to thread pool for tagging.', instance.id)
                    pool.apply_async(handleInstance, (instance,))
        numInstancesAdded = len(instancesByAddress)
        log.info('Created and tagged %i instance(s).', numInstancesAdded)

        if preemptable:
            # Reset deadline such that slow spot creation does not take away from instance boot-up
            deadline = time.time() + provisioning_timeout
        if isinstance(self.batchSystem, AbstractScalableBatchSystem):
            while instancesByAddress and time.time() < deadline:
                with throttle(10):
                    log.debug('Waiting for batch system to report back %i node(s).',
                              len(instancesByAddress))
                    # Get all nodes to be safe, not just the ones whose preemptability matches,
                    # in case there's a problem with a node determining its own preemptability.
                    nodes = self.batchSystem.getNodes()
                    for nodeAddress in nodes.iterkeys():
                        instancesByAddress.pop(nodeAddress, None)
            if instancesByAddress:
                log.warn('%i instance(s) out of %i did not join the cluster as worker nodes. They '
                         'will be terminated.', len(instancesByAddress), numInstancesAdded)
                instanceIds = [i.id for i in instancesByAddress.itervalues()]
                self._logAndTerminate(instanceIds)
                numInstancesAdded -= len(instanceIds)
            else:
                log.info('All %i node(s) joined the cluster.', numInstancesAdded)
        else:
            log.warn('Batch system is not scalable. Assuming all instances joined the cluster.')
        return numInstancesAdded

    def _partialBillingInterval(self, instance):
        """
        Returns a floating point value between 0 and 1.0 representing how far we are into the
        current billing cycle for the given instance. If the return value is .25, we are one
        quarter into the billing cycle, with three quarters remaining before we will be charged
        again for that instance.
        """
        launch_time = parse_iso_utc(instance.launch_time)
        now = datetime.datetime.utcnow()
        delta = now - launch_time
        return delta.total_seconds() / 3600.0 % 1.0

    def _removeNodes(self, instances, numNodes, preemptable=False, force=False):
        # If the batch system is scalable, we can use the number of currently running workers on
        # each node as the primary criterion to select which nodes to terminate.
        if isinstance(self.batchSystem, AbstractScalableBatchSystem):
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
            nodes = nodes[:numNodes]
            if log.isEnabledFor(logging.DEBUG):
                for instance, nodeInfo in nodes:
                    log.debug("Instance %s is about to be terminated. Its node info is %r. It "
                              "would be billed again in %s minutes.", instance.id, nodeInfo,
                              60 * self._remainingBillingInterval(instance))
            instanceIds = [instance.id for instance, nodeInfo in nodes]
        else:
            # Without load info all we can do is sort instances by time left in billing cycle.
            instances = sorted(instances,
                               key=lambda instance: (self._remainingBillingInterval(instance)))
            instanceIds = [instance.id for instance in islice(instances, numNodes)]
        log.info('Terminating %i instance(s).', len(instanceIds))
        if instanceIds:
            self._logAndTerminate(instanceIds)
        return len(instanceIds)

    def _remainingBillingInterval(self, instance):
        return 1.0 - self._partialBillingInterval(instance)

    def _logAndTerminate(self, instanceIds):
        log.debug('IDs of terminated instances: %r', instanceIds)
        self._ec2.terminate_instances(instance_ids=instanceIds)

    def getNodeShape(self, preemptable=False):
        instanceType = self.instanceType[preemptable]
        return Shape(wallTime=60 * 60,
                     memory=instanceType.memory * 2 ** 30,
                     cores=instanceType.cores,
                     disk=(instanceType.disks * instanceType.disk_capacity * 2 ** 30))

    def _getAllRunningInstances(self):
        """
        ... including the leader.

        :rtype: Iterable[Instance]
        """
        return self._ec2.get_only_instances(filters={
            'tag:leader_instance_id': self._instanceId,
            'instance-state-name': 'running'})

    @classmethod
    def _instanceData(cls, path):
        return urlopen('http://169.254.169.254/latest/' + path).read()

    @classmethod
    def _metaData(cls, path):
        return cls._instanceData('meta-data/' + path)

    @classmethod
    def _userData(cls):
        user_data = cls._instanceData('user-data')
        log.info("User data is '%s'", user_data)
        return user_data

    @property
    @memoize
    def _nodeIP(self):
        ip = self._metaData('local-ipv4')
        log.info("Local IP is '%s'", ip)
        return ip

    @property
    @memoize
    def _instanceId(self):
        instance_id = self._metaData('instance-id')
        log.info("Instance ID is '%s'", instance_id)
        return instance_id

    @property
    @memoize
    def _availabilityZone(self):
        zone = self._metaData('placement/availability-zone')
        log.info("Availability zone is '%s'", zone)
        return zone

    @property
    @memoize
    def _region(self):
        m = re.match(r'^([a-z]{2}-[a-z]+-[1-9][0-9]*)([a-z])$', self._availabilityZone)
        assert m
        region = m.group(1)
        log.info("Region is '%s'", region)
        return region

    @property
    @memoize
    def _ec2(self):
        return boto.ec2.connect_to_region(self._region)

    @property
    @memoize
    def _keyName(self):
        return self._instance.key_name

    @property
    @memoize
    def _instance(self):
        return self._getInstance(self._instanceId)

    @property
    @memoize
    def _securityGroupIds(self):
        return [sg.id for sg in self._instance.groups]

    @property
    @memoize
    def _instanceProfileArn(self):
        return self._instance.instance_profile['arn']

    def _getInstance(self, instance_id):
        """
        :rtype: Instance
        """
        reservations = self._ec2.get_all_reservations(instance_ids=[instance_id])
        instances = (i for r in reservations for i in r.instances if i.id == instance_id)
        instance = next(instances)
        assert next(instances, None) is None
        return instance

    @property
    @memoize
    def ebsOptimized(self):
        return self._instance.ebs_optimized
