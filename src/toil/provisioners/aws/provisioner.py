import logging
import re
import time
from collections import Iterable
from urllib2 import urlopen

import boto.ec2
from bd2k.util import memoize
from bd2k.util.iterables import concat
from boto.ec2.instance import Instance
from cgcloud.lib.ec2 import wait_for_spot_instances, ec2_instance_types, wait_transition
from cgcloud.lib.util import papply

from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem, AbstractBatchSystem
from toil.common import Config
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape

log = logging.getLogger(__name__)


def switch(false, true):
    return {False: false, True: true}


class AWSProvisioner(AbstractProvisioner):
    """
    A provisioner that uses a single, self-contained AMI to boot up worker nodes in EC2. It
    assumes that worker instances booted from the AMI autonomously join the cluster. To aid that
    process, this provisioner tags the worker instance with the instance ID of the leader such
    that boot code running on the worker instance can use the EC2 API to determine the IP address
    of the master and pass that on to the Mesos slave daemon. This provisioner uses the spot
    market to provision preemptable instances, and create on-demand instances otherwise.

    This provisioner assumes that

    * It is running on the leader instance in EC2, i.e. the same EC2 instance that's running the
      Mesos master process *and* the Toil leader process

    * The SSH keypair, security group and userData applied to the leader also apply to the workers

    * An instance type with ephemeral volumes is being used (it assert that assumption)

    * The ephemeral volumes on an instance are mirrored or striped into a single volume
    """

    def __init__(self, config, batchSystem):
        """
        :type config: Config
        :type batchSystem: AbstractBatchSystem
        """
        super(AWSProvisioner, self).__init__()
        self.batchSystem = batchSystem
        ami, instanceType = ':'.split(config.nodeOptions)
        preemptableAmi, preemptableInstanceType = ':'.split(config.preemptableNodeOptions)
        self.ami = switch(ami, preemptableAmi)
        self.instanceType = switch(instanceType, preemptableInstanceType)
        for instanceType in self.instanceType.values():
            try:
                instanceType = ec2_instance_types[instanceType]
            except KeyError:
                raise RuntimeError("Invalid or unknown instance type '%s'" % instanceType)
            else:
                # FIXME: add support for EBS volumes
                if instanceType.disks == 0:
                    raise RuntimeError("This provisioner only supports instance types with one or "
                                       "more ephemeral volumes. The requested type '%s' does not "
                                       "have any." % instanceType.name)
        self.spotBid = config.preemptableBidPrice

    def addNodes(self, numNodes=1, preemptable=False):
        instanceSpec = dict(image_id=self.ami[preemptable],
                            key_name=self._keyName,
                            user_data=self._userData(),
                            instance_type=self.instanceType[preemptable],
                            instance_profile_arn=self._instanceProfileArn(),
                            security_group_ids=self._securityGroupIds,
                            ebs_optimized=self.ebsOptimized,
                            dry_run=False)
        if preemptable:
            requests = self._ec2.request_spot_instances(price=self.spotBid,
                                                        count=numNodes,
                                                        # TODO: spread nodes over availability zones
                                                        placement=self._availabilityZone(),
                                                        placement_group=None,
                                                        launch_group=None,
                                                        availability_zone_group=None,
                                                        **instanceSpec)
            instances = wait_for_spot_instances(self._ec2, requests)
        else:
            reservation = self._ec2.run_instances(min_count=numNodes,
                                                  max_count=numNodes,
                                                  **instanceSpec)
            instances = reservation.instances

        # Wait for all nodes concurrently using a thread pool. The pool size is capped, though.
        # FIXME: It may be more efficient (and AWS-friendly) to request the state of all
        # instances in a single request.
        #
        def wait_running(instance):
            wait_transition(instance, from_states={'pending'}, to_state='running')

        assert len(instances) == numNodes
        papply(wait_running, instances)

        # If the batch system is scalable, we can use it to wait for the nodes to join the cluster
        if isinstance(self.batchSystem, AbstractScalableBatchSystem):
            while instances:
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
        instanceType = ec2_instance_types[self.instanceType[preemptable]]
        return Shape(wallTime=60 * 60,
                     memory=instanceType.memory,
                     cores=instanceType.cores,
                     disk=instanceType.disks * instanceType.disk_capacity)

    def _getAllInstances(self):
        """
        :rtype: Iterable[Instance]
        """
        reservations = self._ec2.get_all_reservations(filters={
            'Tag:leader_instance_id': self._instanceId,
            'instance-state-name': 'running'})
        return concat(r.instances for r in reservations)

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
        return self._instance.instance_profile.arn

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
