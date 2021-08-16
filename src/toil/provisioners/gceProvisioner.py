# Copyright (C) 2015-2021 Regents of the University of California
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
import json
import logging
import os
import threading
import time
import uuid
from typing import Set, Optional

import requests
from libcloud.compute.drivers.gce import GCEFailedNode
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from toil.jobStores.googleJobStore import GoogleJobStore
from toil.lib.conversions import human2bytes
from toil.provisioners import NoSuchClusterException
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.node import Node

logger = logging.getLogger(__name__)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


class GCEProvisioner(AbstractProvisioner):
    """
    Implements a Google Compute Engine Provisioner using libcloud.
    """

    NODE_BOTO_PATH = "/root/.boto"  # boto file path on instances
    SOURCE_IMAGE = b'projects/flatcar-cloud/global/images/family/flatcar-stable'

    def __init__(self, clusterName, clusterType, zone, nodeStorage, nodeStorageOverrides, sseKey):
        self.cloud = 'gce'
        self._sseKey = sseKey

        # Call base class constructor, which will call createClusterSettings()
        # or readClusterSettings()
        super(GCEProvisioner, self).__init__(clusterName, clusterType, zone, nodeStorage, nodeStorageOverrides)

    def supportedClusterTypes(self):
        return {'mesos'}

    def createClusterSettings(self):
        # All we need to do is read the Google credentials we need to provision
        # things
        self._readCredentials()

    def readClusterSettings(self):
        """
        Read the cluster settings from the instance, which should be the leader.
        See https://cloud.google.com/compute/docs/storing-retrieving-metadata for details about
        reading the metadata.
        """
        metadata_server = "http://metadata/computeMetadata/v1/instance/"
        metadata_flavor = {'Metadata-Flavor': 'Google'}
        zone = requests.get(metadata_server + 'zone', headers = metadata_flavor).text
        self._zone = zone.split('/')[-1]

        project_metadata_server = "http://metadata/computeMetadata/v1/project/"
        self._projectId = requests.get(project_metadata_server + 'project-id', headers = metadata_flavor).text

        # From a GCE instance, these values can be blank. Only the projectId is needed
        self._googleJson = ''
        self._clientEmail = ''

        self._tags = requests.get(metadata_server + 'description', headers = metadata_flavor).text
        tags = json.loads(self._tags)
        self.clusterName = tags['clusterName']
        self._gceDriver = self._getDriver()
        self._instanceGroup = self._gceDriver.ex_get_instancegroup(self.clusterName, zone=self._zone)

        leader = self.getLeader()
        self._leaderPrivateIP = leader.privateIP

        # The location of the Google credentials file on instances.
        self._credentialsPath = GoogleJobStore.nodeServiceAccountJson
        self._keyName = 'core' # key name leader users to communicate with works
        self._botoPath = self.NODE_BOTO_PATH # boto credentials (used if reading an AWS bucket)

        # Let the base provisioner work out how to deploy duly authorized
        # workers for this leader.
        self._setLeaderWorkerAuthentication()

    def _readCredentials(self):
        """
        Get the credentials from the file specified by GOOGLE_APPLICATION_CREDENTIALS.
        """
        self._googleJson = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not self._googleJson:
            raise RuntimeError('GOOGLE_APPLICATION_CREDENTIALS not set.')
        try:
            with open(self._googleJson) as jsonFile:
                self.googleConnectionParams = json.loads(jsonFile.read())
        except:
             raise RuntimeError('GCEProvisioner: Could not parse the Google service account json file %s'
                                % self._googleJson)

        self._projectId = self.googleConnectionParams['project_id']
        self._clientEmail = self.googleConnectionParams['client_email']
        self._credentialsPath = self._googleJson
        self._clearLeaderWorkerAuthentication()  # TODO: Why are we doing this?
        self._gceDriver = self._getDriver()

    def _write_file_to_cloud(self, key: str, contents: bytes) -> str:
        raise NotImplementedError("The gceProvisioner doesn't support _write_file_to_cloud().")

    def _get_user_data_limit(self) -> int:
        # See: https://cloud.google.com/compute/docs/metadata/setting-custom-metadata#limitations
        return human2bytes('256KB')

    def launchCluster(self, leaderNodeType, leaderStorage, owner, **kwargs):
        """
        In addition to the parameters inherited from the abstractProvisioner,
        the Google launchCluster takes the following parameters:
        keyName: The key used to communicate with instances
        botoPath: Boto credentials for reading an AWS jobStore (optional).
        vpcSubnet: A subnet (optional).
        """
        if 'keyName' not in kwargs:
            raise RuntimeError("A keyPairName is required for the GCE provisioner.")
        self._keyName = kwargs['keyName']
        if 'botoPath' in kwargs:
            self._botoPath = kwargs['botoPath']
        self._vpcSubnet = kwargs['vpcSubnet'] if 'vpcSubnet' in kwargs else None

        # Throws an error if cluster exists
        self._instanceGroup = self._gceDriver.ex_create_instancegroup(self.clusterName, self._zone)
        logger.debug('Launching leader')

        # GCE doesn't have a dictionary tags field. The tags field is just a string list.
        # Therefore, dumping tags into the description.
        tags = {'Owner': self._keyName, 'clusterName': self.clusterName}
        if 'userTags' in kwargs:
            tags.update(kwargs['userTags'])
        self._tags = json.dumps(tags)

        metadata = {'items': [{'key': 'user-data', 'value': self._getCloudConfigUserData('leader')}]}
        imageType = 'flatcar-stable'
        sa_scopes = [{'scopes': ['compute', 'storage-full']}]
        disk = {}
        disk['initializeParams'] = {
            'sourceImage': self.SOURCE_IMAGE,
            'diskSizeGb': leaderStorage
        }
        disk.update({
            'boot': True,
            'autoDelete': True
        })
        name = 'l' + str(uuid.uuid4())

        leader = self._gceDriver.create_node(
            name,
            leaderNodeType,
            imageType,
            location=self._zone,
            ex_service_accounts=sa_scopes,
            ex_metadata=metadata,
            ex_subnetwork=self._vpcSubnet,
            ex_disks_gce_struct = [disk],
            description=self._tags,
            ex_preemptible=False
        )

        self._instanceGroup.add_instances([leader])
        self._leaderPrivateIP = leader.private_ips[0]  # needed if adding workers
        # self.subnetID = leader.subnet_id  # TODO: get subnetID

        # Wait for the appliance to start and inject credentials.
        leaderNode = Node(publicIP=leader.public_ips[0], privateIP=leader.private_ips[0],
                          name=leader.name, launchTime=leader.created_at, nodeType=leader.size,
                          preemptable=False, tags=self._tags)
        leaderNode.waitForNode('toil_leader', keyName=self._keyName)
        leaderNode.copySshKeys(self._keyName)
        leaderNode.injectFile(self._credentialsPath, GoogleJobStore.nodeServiceAccountJson, 'toil_leader')
        if self._botoPath:
            leaderNode.injectFile(self._botoPath, self.NODE_BOTO_PATH, 'toil_leader')
        # Download credentials
        self._setLeaderWorkerAuthentication(leaderNode)

        logger.debug('Launched leader')

    def getNodeShape(self, instance_type: str, preemptable=False):
        # TODO: read this value only once
        sizes = self._gceDriver.list_sizes(location=self._zone)
        sizes = [x for x in sizes if x.name == instance_type]
        assert len(sizes) == 1
        instanceType = sizes[0]

        disk = 0  # instanceType.disks * instanceType.disk_capacity * 2 ** 30
        if disk == 0:
            # This is an EBS-backed instance. We will use the root
            # volume, so add the amount of EBS storage requested forhe root volume
            disk = self._nodeStorageOverrides.get(instance_type, self._nodeStorage) * 2 ** 30

        # Ram is in M.
        # Underestimate memory by 100M to prevent autoscaler from disagreeing with
        # mesos about whether a job can run on a particular node type
        memory = (instanceType.ram/1000 - 0.1) * 2 ** 30
        return Shape(wallTime=60 * 60,
                     memory=memory,
                     cores=instanceType.extra['guestCpus'],
                     disk=disk,
                     preemptable=preemptable)

    @staticmethod
    def retryPredicate(e):
        """ Not used by GCE """
        return False

    def destroyCluster(self):
        """
        Try a few times to terminate all of the instances in the group.
        """
        logger.debug("Destroying cluster %s" % self.clusterName)
        instancesToTerminate = self._getNodesInCluster()
        attempts = 0
        while instancesToTerminate and attempts < 3:
            self._terminateInstances(instances=instancesToTerminate)
            instancesToTerminate = self._getNodesInCluster()
            attempts += 1

        # remove group
        instanceGroup = self._gceDriver.ex_get_instancegroup(self.clusterName, zone=self._zone)
        instanceGroup.destroy()

    def terminateNodes(self, nodes):
        nodeNames = [n.name for n in nodes]
        instances = self._getNodesInCluster()
        instancesToKill = [i for i in instances if i.name in nodeNames]
        self._terminateInstances(instancesToKill)

    def addNodes(self, nodeTypes: Set[str], numNodes, preemptable, spotBid=None):
        assert self._leaderPrivateIP

        # We don't support any balancing here so just pick one of the
        # equivalent node types
        node_type = next(iter(nodeTypes))

        # If keys are rsynced, then the mesos-slave needs to be started after the keys have been
        # transferred. The waitForKey.sh script loops on the new VM until it finds the keyPath file, then it starts the
        # mesos-slave. If there are multiple keys to be transferred, then the last one to be transferred must be
        # set to keyPath.
        keyPath = None
        botoExists = False
        if self._botoPath is not None and os.path.exists(self._botoPath):
            keyPath = self.NODE_BOTO_PATH
            botoExists = True
        elif self._sseKey:
            keyPath = self._sseKey

        if not preemptable:
            logger.debug('Launching %s non-preemptable nodes', numNodes)
        else:
            logger.debug('Launching %s preemptable nodes', numNodes)

        # kwargs["subnet_id"] = self.subnetID if self.subnetID else self._getClusterInstance(self.instanceMetaData).subnet_id
        userData = self._getCloudConfigUserData('worker', keyPath, preemptable)
        metadata = {'items': [{'key': 'user-data', 'value': userData}]}
        imageType = 'flatcar-stable'
        sa_scopes = [{'scopes': ['compute', 'storage-full']}]
        disk = {}
        disk['initializeParams'] = {
            'sourceImage': self.SOURCE_IMAGE,
            'diskSizeGb': self._nodeStorageOverrides.get(node_type, self._nodeStorage) }
        disk.update({
            'boot': True,
            'autoDelete': True
        })

        # TODO:
        #  - bug in gce.py for ex_create_multiple_nodes (erroneously, doesn't allow image and disk to specified)
        #  - ex_create_multiple_nodes is limited to 1000 nodes
        #    - use a different function
        #    - or write a loop over the rest of this function, with 1K nodes max on each iteration
        retries = 0
        workersCreated = 0
        # Try a few times to create the requested number of workers
        while numNodes-workersCreated > 0 and retries < 3:
            instancesLaunched = self.ex_create_multiple_nodes(
                                    '', node_type, imageType, numNodes-workersCreated,
                                    location=self._zone,
                                    ex_service_accounts=sa_scopes,
                                    ex_metadata=metadata,
                                    ex_disks_gce_struct=[disk],
                                    description=self._tags,
                                    ex_preemptible=preemptable
                                    )
            failedWorkers = []
            for instance in instancesLaunched:
                if isinstance(instance, GCEFailedNode):
                    logger.error("Worker failed to launch with code %s. Error message: %s"
                                 % (instance.code, instance.error))
                    continue

                node = Node(publicIP=instance.public_ips[0], privateIP=instance.private_ips[0],
                            name=instance.name, launchTime=instance.created_at, nodeType=instance.size,
                            preemptable=False, tags=self._tags)  # FIXME: what should tags be set to?
                try:
                    self._injectWorkerFiles(node, botoExists)
                    logger.debug("Created worker %s" % node.publicIP)
                    self._instanceGroup.add_instances([instance])
                    workersCreated += 1
                except Exception as e:
                    logger.error("Failed to configure worker %s. Error message: %s" % (node.name, e))
                    failedWorkers.append(instance)
            if failedWorkers:
                logger.error("Terminating %d failed workers" % len(failedWorkers))
                self._terminateInstances(failedWorkers)
            retries += 1

        logger.debug('Launched %d new instance(s)', numNodes)
        if numNodes != workersCreated:
            logger.error("Failed to launch %d worker(s)", numNodes-workersCreated)
        return workersCreated

    def getProvisionedWorkers(self, instance_type: Optional[str] = None, preemptable: Optional[bool] = None):
        assert self._leaderPrivateIP
        entireCluster = self._getNodesInCluster(instance_type=instance_type)
        logger.debug('All nodes in cluster: %s', entireCluster)
        workerInstances = []
        for instance in entireCluster:
            if preemptable is not None:
                scheduling = instance.extra.get('scheduling')
                # If this field is not found in the extra meta-data, assume the node is not preemptable.
                if scheduling and scheduling.get('preemptible', False) != preemptable:
                    continue
            isWorker = True
            for ip in instance.private_ips:
                if ip == self._leaderPrivateIP:
                    isWorker = False
                    break  # don't include the leader
            if isWorker and instance.state == 'running':
                workerInstances.append(instance)

        logger.debug('All workers found in cluster: %s', workerInstances)
        return [Node(publicIP=i.public_ips[0], privateIP=i.private_ips[0],
                     name=i.name, launchTime=i.created_at, nodeType=i.size,
                     preemptable=i.extra.get('scheduling', {}).get('preemptible', False), tags=None)
                for i in workerInstances]

    def getLeader(self):
        instances = self._getNodesInCluster()
        instances.sort(key=lambda x: x.created_at)
        try:
            leader = instances[0]  # assume leader was launched first
        except IndexError:
            raise NoSuchClusterException(self.clusterName)
        return Node(publicIP=leader.public_ips[0], privateIP=leader.private_ips[0],
                    name=leader.name, launchTime=leader.created_at, nodeType=leader.size,
                    preemptable=False, tags=None)

    def _injectWorkerFiles(self, node, botoExists):
        """
        Set up the credentials on the worker.
        """
        node.waitForNode('toil_worker', keyName=self._keyName)
        node.copySshKeys(self._keyName)
        node.injectFile(self._credentialsPath, GoogleJobStore.nodeServiceAccountJson, 'toil_worker')
        if self._sseKey:
            node.injectFile(self._sseKey, self._sseKey, 'toil_worker')
        if botoExists:
            node.injectFile(self._botoPath, self.NODE_BOTO_PATH, 'toil_worker')

    def _getNodesInCluster(self, instance_type: Optional[str] = None):
        instanceGroup = self._gceDriver.ex_get_instancegroup(self.clusterName, zone=self._zone)
        instances = instanceGroup.list_instances()
        if instance_type:
            instances = [instance for instance in instances if instance.size == instance_type]
        return instances

    def _getDriver(self):
        """  Connect to GCE """
        driverCls = get_driver(Provider.GCE)
        return driverCls(self._clientEmail,
                         self._googleJson,
                         project=self._projectId,
                         datacenter=self._zone)

    def _terminateInstances(self, instances):
        def worker(driver, instance):
            logger.debug('Terminating instance: %s', instance.name)
            driver.destroy_node(instance)

        threads = []
        for instance in instances:
            t = threading.Thread(target=worker, args=(self._gceDriver,instance))
            threads.append(t)
            t.start()

        logger.debug('... Waiting for instance(s) to shut down...')
        for t in threads:
            t.join()

    # MONKEY PATCH - This function was copied form libcloud to fix a bug.
    DEFAULT_TASK_COMPLETION_TIMEOUT = 180

    def ex_create_multiple_nodes(
            self, base_name, size, image, number, location=None,
            ex_network='default', ex_subnetwork=None, ex_tags=None,
            ex_metadata=None, ignore_errors=True, use_existing_disk=True,
            poll_interval=2, external_ip='ephemeral',
            ex_disk_type='pd-standard', ex_disk_auto_delete=True,
            ex_service_accounts=None, timeout=DEFAULT_TASK_COMPLETION_TIMEOUT,
            description=None, ex_can_ip_forward=None, ex_disks_gce_struct=None,
            ex_nic_gce_struct=None, ex_on_host_maintenance=None,
            ex_automatic_restart=None, ex_image_family=None,
            ex_preemptible=None):
        """
         Monkey patch to gce.py in libcloud to allow disk and images to be specified.
         Also changed name to a uuid below.
         The prefix 'wp' identifies preemptable nodes and 'wn' non-preemptable nodes.
        """
        # if image and ex_disks_gce_struct:
        #    raise ValueError("Cannot specify both 'image' and "
        #                     "'ex_disks_gce_struct'.")

        driver = self._getDriver()
        if image and ex_image_family:
            raise ValueError("Cannot specify both 'image' and "
                             "'ex_image_family'")

        location = location or driver.zone
        if not hasattr(location, 'name'):
            location = driver.ex_get_zone(location)
        if not hasattr(size, 'name'):
            size = driver.ex_get_size(size, location)
        if not hasattr(ex_network, 'name'):
            ex_network = driver.ex_get_network(ex_network)
        if ex_subnetwork and not hasattr(ex_subnetwork, 'name'):
            ex_subnetwork = \
                driver.ex_get_subnetwork(ex_subnetwork,
                                         region=driver._get_region_from_zone(location))
        if ex_image_family:
            image = driver.ex_get_image_from_family(ex_image_family)
        if image and not hasattr(image, 'name'):
            image = driver.ex_get_image(image)
        if not hasattr(ex_disk_type, 'name'):
            ex_disk_type = driver.ex_get_disktype(ex_disk_type, zone=location)

        node_attrs = {'size': size,
                      'image': image,
                      'location': location,
                      'network': ex_network,
                      'subnetwork': ex_subnetwork,
                      'tags': ex_tags,
                      'metadata': ex_metadata,
                      'ignore_errors': ignore_errors,
                      'use_existing_disk': use_existing_disk,
                      'external_ip': external_ip,
                      'ex_disk_type': ex_disk_type,
                      'ex_disk_auto_delete': ex_disk_auto_delete,
                      'ex_service_accounts': ex_service_accounts,
                      'description': description,
                      'ex_can_ip_forward': ex_can_ip_forward,
                      'ex_disks_gce_struct': ex_disks_gce_struct,
                      'ex_nic_gce_struct': ex_nic_gce_struct,
                      'ex_on_host_maintenance': ex_on_host_maintenance,
                      'ex_automatic_restart': ex_automatic_restart,
                      'ex_preemptible': ex_preemptible}
        # List for holding the status information for disk/node creation.
        status_list = []

        for i in range(number):
            name = 'wp' if ex_preemptible else 'wn'
            name += str(uuid.uuid4())  # '%s-%03d' % (base_name, i)
            status = {'name': name, 'node_response': None, 'node': None}
            status_list.append(status)

        start_time = time.time()
        complete = False
        while not complete:
            if time.time() - start_time >= timeout:
                raise Exception("Timeout (%s sec) while waiting for multiple "
                                "instances")
            complete = True
            time.sleep(poll_interval)
            for status in status_list:
                # Create the node or check status if already in progress.
                if not status['node']:
                    if not status['node_response']:
                        driver._multi_create_node(status, node_attrs)
                    else:
                        driver._multi_check_node(status, node_attrs)
                # If any of the nodes have not been created (or failed) we are
                # not done yet.
                if not status['node']:
                    complete = False

        # Return list of nodes
        node_list = []
        for status in status_list:
            node_list.append(status['node'])
        return node_list
