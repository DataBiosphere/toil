# Copyright (C) 2017 UCSC Computational Genomics Lab
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
import ConfigParser
import logging
import os.path
import sys
import tempfile
import uuid
import time
import urllib
import json
import subprocess
import re

from toil import applianceSelf
from toil.provisioners.node import Node
from toil.provisioners.abstractProvisioner import Shape
from toil.provisioners.ansibleDriver import AnsibleDriver
from toil.provisioners.aws import (leaderArgs, workerArgs)
from toil.provisioners import NoSuchClusterException


from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient


logger = logging.getLogger(__name__)
logging.getLogger("msrest.http_logger").setLevel(logging.WARNING)
logging.getLogger("msrest.pipeline").setLevel(logging.WARNING)
logging.getLogger("requests_oauthlib.oauth2_session").setLevel(logging.WARNING)

# Credentials:
# sshKey - needed for ssh and rsync access to the VM
# .azure/credentials for creating VM with Ansible
# .toilAzureCredentials for accessing jobStore


class AzureProvisioner(AnsibleDriver):
    def __init__(self, clusterName=None, zone=None, config=None):

        self.playbook = {
            'create-cluster': 'create-azure-resourcegroup.yml',
            'create': 'create-azure-vm.yml',
            'delete': 'delete-azure-vm.yml',
            'destroy': 'delete-azure-cluster.yml'
        }
        playbooks = os.path.dirname(os.path.realpath(__file__))
        super(AzureProvisioner, self).__init__(playbooks, clusterName, zone, config)

        # Fetch Azure credentials from the CLI config
        # FIXME: do error checking on this
        azureCredentials = ConfigParser.SafeConfigParser()
        azureCredentials.read(os.path.expanduser("~/.azure/credentials"))
        client_id = azureCredentials.get("default", "client_id")
        secret = azureCredentials.get("default", "secret")
        tenant = azureCredentials.get("default", "tenant")
        subscription = azureCredentials.get("default", "subscription_id")

        # Authenticate to Azure API and fetch list of instance types
        credentials = ServicePrincipalCredentials(client_id=client_id, secret=secret, tenant=tenant)
        self._azureComputeClient = ComputeManagementClient(credentials, subscription)
        self._azureNetworkClient = NetworkManagementClient(credentials, subscription)


        if config:
            # This is called when running on the leader.

            # get the leader metadata
            mdUrl = "http://169.254.169.254/metadata/instance?api-version=2017-08-01"
            header = {'Metadata': 'True'}
            request = urllib.request.Request(url=mdUrl, headers=header)
            response = urllib.request.urlopen(request)
            data = response.read()
            dataStr = data.decode("utf-8")
            metadata = json.loads(dataStr)

            # set values from the leader meta-data
            self.zone = metadata['compute']['location']
            self.clusterName = metadata['compute']['resourceGroupName']
            tagsStr = metadata['compute']['tags']
            tags = dict(item.split(":") for item in tagsStr.split(";"))
            self.vmTags = None
            #self.leaderPrivateIP = metadata['network']['interface'][0]['ipv4']['ipaddress'][0]['privateIpAddress']

            self.keyName = tags.get('owner', 'no-owner')
            leader = self.getLeader()
            self.leaderPrivateIP = leader.privateIP
            self._setSSH()  # create id_rsa.pub file on the leader if it is not there
            self.masterPublicKeyFile = '/root/.ssh/id_rsa.pub'

            # read given configuration parameters
            self.nodeStorage = config.nodeStorage
            self.nonPreemptableNodeTypes = []
            for nodeTypeStr in config.nodeTypes:
                nodeBidTuple = nodeTypeStr.split(":")
                if len(nodeBidTuple) != 2:
                    self.nonPreemptableNodeTypes.append(nodeTypeStr)
            self.nonPreemptableNodeShapes = [self.getNodeShape(nodeType=nodeType) for nodeType in self.nonPreemptableNodeTypes]
            self.nodeShapes = self.nonPreemptableNodeShapes
            self.nodeTypes = self.nonPreemptableNodeTypes
        else:
            # This is called when using launchCluster
            self.leaderPrivateIP = None
            self.keyName = None
            self.vmTags = None
            self.masterPublicKeyFile = None
            self.nodeStorage = None

    def launchCluster(self, leaderNodeType, keyName, userTags=None,
            vpcSubnet=None, leaderStorage=50, nodeStorage=50, botoPath=None, **kwargs):
        """
        Launches an Azure cluster using Ansible.
        A resource group is created for the cluster. All the virtual machines are created within this
        resource group.

        Cloud-config is called during vm creation to create directories and launch the appliance.
        """
        self.checkValidClusterName()
        self.keyName = keyName
        self.nodeStorage = nodeStorage
        self.masterPublicKeyFile = kwargs['publicKeyFile']

        # Try deleting the resource group. This will fail if it exists.
        ansibleArgs = {
            'resgrp': self.clusterName,
            'region': self.zone
        }
        try:
            self.callPlaybook(self.playbook['create-cluster'], ansibleArgs, wait=True)
        except RuntimeError:
            logger.info("The cluster could not be created. Try deleting the cluster if it already exits.")
            raise

        # Azure VMs must be named, so we need to generate one. Instance names must
        # be composed of only alphanumeric characters, underscores, and hyphens
        # (see https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions).
        instanceName = 'l' + str(uuid.uuid4())

        cloudConfigArgs = {
            'image': applianceSelf(),
            'role': "leader",
            'entrypoint': "mesos-master",
            '_args': leaderArgs.format(name=self.clusterName),
        }
        ansibleArgs = {
            'vmsize': leaderNodeType,
            'vmname': instanceName,
            'storagename': instanceName.replace('-', '')[:24],  # Azure limits the name to 24 characters, no dashes.
            'resgrp': self.clusterName,  # The resource group, which represents the cluster.
            'region': self.zone,
            'role': "leader",
            'owner': self.keyName,  # Just a tag.
            'diskSize': str(leaderStorage),  # TODO: not implemented
            'publickeyfile': self.masterPublicKeyFile   # The users public key to be added to authorized_keys
        }
        ansibleArgs['cloudconfig'] = self._cloudConfig(cloudConfigArgs)
        self.callPlaybook(self.playbook['create'], ansibleArgs, wait=True)
        # IP available as soon as the playbook finishes
        try:
            leaderNode = self.getLeader()
        except IndexError:
            raise RuntimeError("Failed to launcher leader")
        logger.info('Launched leader')

        self.leaderPrivateIP = leaderNode.privateIP

        # Make sure leader appliacne is up.
        leaderNode.waitForNode('toil_leader')

        # Transfer credentials
        containerUserPath = '/root/'
        storageCredentials = kwargs['azureStorageCredentials']
        if storageCredentials is not None:
            fullPathCredentials = os.path.expanduser(storageCredentials)
            if os.path.isfile(fullPathCredentials):
                leaderNode.injectFile(fullPathCredentials, containerUserPath, 'toil_leader')

        ansibleCredentials = '.azure/credentials'
        fullPathAnsibleCredentials = os.path.expanduser('~/' + ansibleCredentials)
        if os.path.isfile(fullPathAnsibleCredentials):
            leaderNode.sshAppliance('mkdir', '-p', containerUserPath + '.azure')
            leaderNode.injectFile(fullPathAnsibleCredentials, containerUserPath + ansibleCredentials,
                                  'toil_leader')

    def _cloudConfig(self, args):
        # Populate cloud-config file and pass it to Ansible
        with open(os.path.join(self.playbooks, "cloud-config"), "r") as f:
            configRaw = f.read()
        with tempfile.NamedTemporaryFile(delete=False) as t:
            t.write(configRaw.format(**args))
            return t.name

    def destroyCluster(self):
        ansibleArgs = {
            'resgrp': self.clusterName,
        }
        self.callPlaybook(self.playbook['destroy'], ansibleArgs)

    def addNodes(self, nodeType, numNodes, preemptable=False, spotBid=None):

        cloudConfigArgs = {
            'image': applianceSelf(),
            'role': "worker",
            'entrypoint': "mesos-slave",
            '_args': workerArgs.format(ip=self.leaderPrivateIP, preemptable=False, keyPath='')
        }

        ansibleArgs = dict(vmsize=nodeType,
                           resgrp=self.clusterName,
                           region=self.zone,
                           diskSize=str(self.nodeStorage),
                           owner=self.keyName,
                           role="worker",
                           publickeyfile=self.masterPublicKeyFile)
        ansibleArgs['cloudconfig'] = self._cloudConfig(cloudConfigArgs)

        instances = []
        wait = False
        for i in range(numNodes):
            # Wait for the last one
            if i == numNodes - 1:
                wait = True
            name = 'w' + str(uuid.uuid4())
            ansibleArgs['vmname'] = name
            storageName = name
            ansibleArgs['storagename'] = storageName.replace('-', '')[:24]
            instances.append(name)
            self.callPlaybook(self.playbook['create'], ansibleArgs, wait=wait, tags=['untagged'])

        # Wait for nodes
        allWorkers = self.getProvisionedWorkers(None)
        nodesIndex = dict((x.name, x) for x in allWorkers)
        for vmName in instances:
            if vmName in nodesIndex:
                nodesIndex[vmName].waitForNode('toil_worker')
                # TODO: add code to transfer sse key here
            else:
                logger.debug("Instance %s failed to launch", vmName)
        return len(instances)

    def getNodeShape(self, nodeType=None, preemptable=False):
        instanceTypes = self._azureComputeClient.virtual_machine_sizes.list(self.zone)

        # Data model: https://docs.microsoft.com/en-us/python/api/azure.mgmt.compute.v2017_12_01.models.virtualmachinesize?view=azure-python
        instanceType = (vmType for vmType in instanceTypes if vmType.name == nodeType).next()

        disk = instanceType.max_data_disk_count * instanceType.os_disk_size_in_mb * 2 ** 30

        # Underestimate memory by 100M to prevent autoscaler from disagreeing with
        # mesos about whether a job can run on a particular node type
        memory = (instanceType.memory_in_mb - 0.1) * 2 ** 30

        return Shape(wallTime=60 * 60,
                     memory=memory,
                     cores=instanceType.number_of_cores,
                     disk=disk,
                     preemptable=False)

    def getProvisionedWorkers(self, nodeType, preemptable=False):
        return self._getNodes('worker', nodeType)

    def getLeader(self):
        try:
            leader = self._getNodes('leader')[0]
        except IndexError:
            raise NoSuchClusterException(self.clusterName)
        return leader

    def _getNodes(self, role, nodeType=None):
        allNodes = self._azureComputeClient.virtual_machines.list(self.clusterName)
        rv = []
        allNodeNames = []
        workerNames = []
        for node in allNodes:
            allNodeNames.append(node.name)
            nodeRole = node.tags.get('role', None)
            if node.provisioning_state != 'Succeeded' or nodeRole != role:
                continue
            if nodeType and node.hardware_profile.vm_size != nodeType:
                continue

            network_interface = self._azureNetworkClient.network_interfaces.get(self.clusterName, node.name)
            if not network_interface.ip_configurations:
                continue # no networks assigned to this node
            publicIP = self._azureNetworkClient.public_ip_addresses.get(self.clusterName, node.name)
            workerNames.append(node.name)
            rv.append(Node(
                publicIP=publicIP.ip_address,
                privateIP=network_interface.ip_configurations[0].private_ip_address,
                name=node.name,
                launchTime=None,  # Not used with Azure.
                nodeType=node.hardware_profile.vm_size,
                preemptable=False) # Azure doesn't have preemptable nodes
            )
        logger.debug('All nodes in cluster: ' + ', '.join(allNodeNames))
        typeStr = ' of type ' + nodeType if nodeType else ''
        logger.debug('All %s nodes%s: %s' % (role, typeStr, ', '.join(workerNames)))
        return rv

    def terminateNodes(self, nodes, preemptable=False):
        for counter, node in enumerate(nodes):
            ansibleArgs = {
                'vmname': node.name,
                'resgrp': self.clusterName,
                'storagename': node.name.replace('-', '')[:24]
            }
            wait = True if counter == len(nodes) - 1 else False
            self.callPlaybook(self.playbook['delete'], ansibleArgs, wait=wait)

    def checkValidClusterName(self):
        """
        Cluster names are used for resource groups, security groups, virtual networks and subnets, and
        therefore, need to adhere to the naming requirements for each:
        * Between 2 and 64 characters
        * Using alphanumeric characters, underscores, hyphens, or periods.
        * Starts and ends with an alphanumeric character.
        More info on naming conventions:
        https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions
        """
        p = re.compile('^[a-zA-Z0-9][a-zA-Z0-9_.\-]*[a-zA-Z0-9]$')
        if len(self.clusterName) < 2 or len(self.clusterName) > 64 or not p.match(self.clusterName):
            raise RuntimeError("Invalid cluster name (%s)."
                                " It must be between 2 and 64 characters and contain only alpha-numeric"
                                " characters, hyphens, underscores, and periods. It must start and"
                                " end only with alpha-numeric characters." % self.clusterName)

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
