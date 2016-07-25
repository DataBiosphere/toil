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


def switch(false, true):
    return {False: false, True: true}


class CGCloudProvisioner(AbstractProvisioner):
    """
    A provisioner that uses CGCloud's toil-box role to boot up worker nodes in EC2. It uses the
    spot market to provision preemptable instances, but defaults to on-demand instances.

    This provisioner assumes that

    * It is running the leader node, i.e. the EC2 instance that's running the Mesos master
      process and the Toil leader process.

    * The leader node was recreated from a toil-box image using cgcloud.

    * The version of cgcloud used to create the toil-box image is compatible with the one this
      provisioner, and therefore Toil depend on.

    * The SSH keypair, security group and userData applied to the leader also apply to the workers

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
        if config.nodeType:
            instanceType = self._resolveInstanceType(config.nodeType)
            if config.preemptableNodeType:
                preemptableInstanceType, spotBid = ':'.split(config.preemptableNodeType)
                preemptableInstanceType = self._resolveInstanceType(preemptableInstanceType)
                self.spotBid = float(spotBid)
            else:
                preemptableInstanceType, self.spotBid = None, None
        else:
            raise RuntimeError('Must pass --nodeType when using the cgcloud provisioner')
        self.instanceType = switch(instanceType, preemptableInstanceType)
        # TODO: assert that leader has same number of ephemeral drives as workers or user_data won't match!!!

    @staticmethod
    def _resolveInstanceType(instanceType):
        """
        :param str instanceType: the instance type as a string, e.g. 'm3.large'
        :rtype: cgcloud.lib.ec2.InstanceType
        """
        try:
            instanceType = ec2_instance_types[instanceType]
        except KeyError:
            raise RuntimeError("Invalid or unknown instance type '%s'" % instanceType)
        else:
            if instanceType.disks == 0:
                raise RuntimeError("This provisioner only supports instance types with one or "
                                   "more ephemeral volumes. The requested type '%s' does not "
                                   "have any." % instanceType.name)
            else:
                return instanceType

    def addNodes(self, numNodes=1, preemptable=False):
        spec = dict(key_name=self._keyName,
                    user_data=self._userData(),
                    instance_type=self.instanceType[preemptable].name,
                    instance_profile_arn=self._instanceProfileArn,
                    security_group_ids=self._securityGroupIds,
                    ebs_optimized=self.ebsOptimized,
                    dry_run=False)
        instances = list(self._getAllInstances())  # includes leader
        used_cluster_ordinals = {int(i.tags['cluster_ordinal']) for i in instances}
        assert len(used_cluster_ordinals) == len(instances)  # check for collisions
        cluster_ordinal = allocate_cluster_ordinals(num=numNodes, used=used_cluster_ordinals)

        def createInstances():
            """
            :rtype: Iterable[list[Instance]]
            """
            if preemptable:
                for batch in create_spot_instances(self._ec2, self.spotBid, self.imageId, spec,
                                                   num_instances=numNodes):
                    yield batch
            else:
                yield create_ondemand_instances(self._ec2, self.imageId, spec,
                                                num_instances=numNodes)

        nodeAddresses = set()

        def handleInstance(instance):
            log.debug('Tagging instance %s.', instance.id)
            leader_tags = self._instance.tags
            name = leader_tags['Name'].replace('toil-leader', 'toil-worker')
            tag_object_persistently(instance, dict(leader_tags,
                                                   Name=name,
                                                   cluster_ordinal=next(cluster_ordinal)))
            nodeAddresses.add(instance.private_ip_address)

        # Each instance gets a different ordinal so we can't tag an entire batch at once but
        # instance have to tag each instance individually. It needs to be done quickly because
        # the tags are crucial for the boot code running inside the instance to join the cluster.
        # Hence we do it in a thread pool. If the pool is too large, we'll hit the EC2 limit on
        # the number of of concurrent requests. If it is too small, we won't be able to tag all
        # instances in time.
        with thread_pool(min(numNodes, 32)) as pool:
            for batch in createInstances():
                log.debug('Got a batch of %i instance(s).', len(batch))
                for instance in batch:
                    pool.apply_async(handleInstance, (instance,))

        numNodesAdded = len(nodeAddresses)
        log.info('Created and tagged %i instances.', numNodesAdded)

        if isinstance(self.batchSystem, AbstractScalableBatchSystem):
            while nodeAddresses:
                log.debug('Waiting for batch system to report back %i node(s).', len(nodeAddresses))
                # Get all nodes to be safe, not just the ones whose preemptability matches,
                # in case there's a problem with a node determining its own preemptability.
                nodes = self.batchSystem.getNodes()
                nodeAddresses.difference_update(nodes.iterkeys())
                time.sleep(10)
            log.info('All %i nodes have joined the cluster.', numNodesAdded)
        else:
            log.warn("Can't wait for nodes to join the cluster since batch system isn't scalable.")

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

    def removeNodes(self, numNodes=1, preemptable=False):
        instances = (i for i in self._getAllInstances()
                     if i.id != self._instanceId  # disregard leader
                     and preemptable != i.spot_instance_request_id is None)
        # If the batch system is scalable, we can use the number of currently running workers on
        # each node as the primary criterion to select which nodes to terminate.
        if isinstance(self.batchSystem, AbstractScalableBatchSystem):
            nodes = self.batchSystem.getNodes(preemptable)
            # Join nodes and instances on private IP address. It is possible for the batch system
            # to report stale nodes for which the corresponding instance was terminated already.
            # There can also be instances that the batch system doesn't have nodes for yet.
            nodes = [(instance, nodes.get(instance.private_ip_address))
                     for instance in instances]
            # Sort instances by # of workers and time left in billing cycle. Assume zero workers
            # if the node info is missing. In the case of the Mesos batch system this can happen
            # if 1) the node hasn't been discovered yet or 2) a node was first discovered through
            #  an offer rather than a framework message. This is an acceptable approximation
            # since the node is likely to be new.
            nodes.sort(key=lambda (instance, nodeInfo): (
                nodeInfo.workers if nodeInfo else 0,
                1.0 - self._partialBillingInterval(instance)))
            instanceIds = [instance.id for instance, nodeInfo in islice(nodes, numNodes)]
        else:
            # Without load info all we can do is sort instances by time left in billing cycle.
            instances = sorted(instances,
                               key=lambda instance: 1.0 - self._partialBillingInterval(instance))
            instanceIds = [instance.id for instance in islice(instances, numNodes)]
        log.info('Terminating %i instances.', len(instanceIds))
        log.debug('IDs of terminated instances: %r', instanceIds)
        self._ec2.terminate_instances(instance_ids=instanceIds)

    def getNumberOfNodes(self, preemptable=False):
        instanceIds = {instance.id for instance in self._getAllInstances()}
        assert self._instanceId in instanceIds
        return len(instanceIds) - 1

    def getNodeShape(self, preemptable=False):
        instanceType = self.instanceType[preemptable]
        return Shape(wallTime=60 * 60,
                     memory=instanceType.memory * 2 ** 30,
                     cores=instanceType.cores,
                     disk=(instanceType.disks * instanceType.disk_capacity * 2 ** 30))

    def _getAllInstances(self):
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


""" Demo:
cd ~/workspace/bd2k/toil
va
make develop
made sdist
----------------------------------------------------------------------------------------------------
cd ~/workspace/bd2k/cgcloud
va
make develop
made sdist
cgcloud create -IT toil-latest-box \
    --option toil_sdists='lib/dist/cgcloud-lib-1.4a1.dev0.tar.gz ../toil/dist/toil-3.2.0a2.tar.gz[aws,mesos,cgcloud]'
cgcloud create-cluster toil --leader-instance-type m3.large --num-workers 0
cgcloud rsync toil-leader -av ~/.aws :
cgcloud rsync -a toil-leader ../toil/dist/toil-3.2.0a2.tar.gz :
----------------------------------------------------------------------------------------------------
cgcloud ssh -a toil-leader
sudo pip install --upgrade toil-3.2.0a2.tar.gz[mesos,aws,cgcloud]
----------------------------------------------------------------------------------------------------
cgcloud ssh toil-leader
export AWS_PROFILE=bd2k
cat >helloWorld.py <<END
from toil.job import Job

def hello(who, memory="10M", cores=1, disk="10M"):
    return "Hello, %s!" % who

if __name__=="__main__":
    parser = Job.Runner.getDefaultArgumentParser()
    options = parser.parse_args()
    helloWorld = Job.wrapFn(hello, "world")
    print Job.Runner.startToil(helloWorld, options)
END
python helloWorld.py \
    /tmp/sort-jobstore \
    --provisioner=cgcloud --nodeType=m3.large \
    --batchSystem mesos --mesosMaster mesos-master:5050 \
    --clean always --logDebug
"""
