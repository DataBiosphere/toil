import logging
import re
import time
from collections import Iterable
from urllib2 import urlopen

import boto.ec2
from bd2k.util import memoize
from boto.ec2.instance import Instance
from cgcloud.lib.ec2 import (ec2_instance_types,
                             create_spot_instances,
                             create_ondemand_instances, tag_object_persistently)
from cgcloud.lib.util import allocate_cluster_ordinals, thread_pool

from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem, AbstractBatchSystem
from toil.common import Config
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape

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
        instances = list(self._getAllInstances())  # includes master
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

        def handleInstance(instance):
            log.debug('Tagging instance %s.', instance.id)
            leader_tags = self._instance.tags
            name = leader_tags['Name'].replace('toil-leader', 'toil-worker')
            tag_object_persistently(instance, dict(leader_tags,
                                                   Name=name,
                                                   cluster_ordinal=next(cluster_ordinal)))
            instances.add(instance.private_ip_address)

        instances = set()
        with thread_pool(10) as pool:  # 10 concurrent requests
            for batch in createInstances():
                log.debug('Got a batch of %i instance(s).', len(batch))
                for instance in batch:
                    pool.apply_async(handleInstance, (instance,))

        log.info('Created and tagged %i instances.', len(instances))
        # If the batch system is scalable, we can use it to wait for the nodes to join the cluster
        if isinstance(self.batchSystem, AbstractScalableBatchSystem):
            while instances:
                log.info('Waiting for batch system to report back %i node(s).', len(instances))
                numNodes = self.batchSystem.getNodes()
                for address in numNodes.keys():
                    instances.remove(address)
                time.sleep(10)

    def removeNodes(self, numNodes=1, preemptable=False):
        # If the batch system is scalable, we can use the number of currently running workers on
        # each node as the primary criterion to select which nodes to terminate. Otherwise,
        # we terminate the oldest nodes.
        instances = self._getAllInstances()
        if isinstance(self.batchSystem, AbstractScalableBatchSystem):
            # Index instances by private IP address
            instances = {instance.private_ip_address: instance for instance in instances}
            nodeLoad = self.batchSystem.getNodes(preemptable)
            # Join nodes and instances on private IP address
            nodes = [(nodeAddress, instances.get(nodeAddress), nodeInfo)
                     for nodeAddress, nodeInfo in nodeLoad.iteritems()]

            # Sort by # of workers, then CPU load and inverse instance age
            def by_load_and_youth((nodeAddress, instance, nodeInfo )):
                return nodeInfo.workers, nodeInfo.cores, instance.launchTime if instance else 0

            nodes.sort(key=by_load_and_youth)
        else:
            nodes = [(instance.private_ip_address, instance, None) for instance in instances]

            def by_youth((nodeAddress, instance, nodeInfo )):
                return instance.launch_time, nodeAddress

            nodes.sort(key=by_youth)
        assert numNodes <= len(nodes)
        nodes = nodes[:numNodes]
        instanceIds = [instance.id for nodeAddress, instance, nodeInfo in nodes]
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
