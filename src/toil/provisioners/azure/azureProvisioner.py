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
import tempfile
import uuid
import urllib
import json
import re
from toil import subprocess

from toil.provisioners.node import Node
from toil.provisioners.abstractProvisioner import Shape
from toil.provisioners.ansibleDriver import AnsibleDriver
from toil.provisioners import NoSuchClusterException

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient


logger = logging.getLogger(__name__)

# disable annoying messages
logging.getLogger("msrest.http_logger").setLevel(logging.WARNING)
logging.getLogger("msrest.pipeline").setLevel(logging.WARNING)
logging.getLogger("requests_oauthlib.oauth2_session").setLevel(logging.WARNING)


class AzureProvisioner(AnsibleDriver):
    """
    Manage provisioning a leader and workers on Azure using Ansible.

    To provision, Toil needs Azure credentials in .azure/credentials. This file must
    be transferred to the leader appliance to do auto-scaling because the native
    credentials aren't seen within the appliance.

    """

    def __init__(self, clusterName, zone, nodeStorage):

        self.playbook = {
            'check-cluster': 'create-azure-resourcegroup.yml',
            'create-cluster': 'create-azure-cluster.yml',
            'create': 'create-azure-vm.yml',
            'delete': 'delete-azure-vm.yml',
            'destroy': 'delete-azure-cluster.yml'
        }
        playbooks = os.path.dirname(os.path.realpath(__file__))
        super(AzureProvisioner, self).__init__(playbooks, clusterName, zone, nodeStorage)

        # Fetch Azure credentials from the CLI config
        # FIXME: do error checking on this
        azureCredentials = ConfigParser.SafeConfigParser()
        azureCredentials.read(os.path.expanduser("~/.azure/credentials"))
        client_id = azureCredentials.get("default", "client_id")
        secret = azureCredentials.get("default", "secret")
        tenant = azureCredentials.get("default", "tenant")
        subscription = azureCredentials.get("default", "subscription_id")

        # Authenticate to Azure API
        credentials = ServicePrincipalCredentials(client_id=client_id, secret=secret, tenant=tenant)
        self._azureComputeClient = ComputeManagementClient(credentials, subscription)
        self._azureNetworkClient = NetworkManagementClient(credentials, subscription)

        self._onLeader = False
        if not clusterName:
            # If no clusterName, Toil must be running on the leader.
            self._readClusterSettings()
            self._onLeader = True


    def _readClusterSettings(self):
        """
        Read the current instance's meta-data to get the cluster settings.
        """
        # get the leader metadata
        mdUrl = "http://169.254.169.254/metadata/instance?api-version=2017-08-01"
        header = {'Metadata': 'True'}
        request = urllib.request.Request(url=mdUrl, headers=header)
        response = urllib.request.urlopen(request)
        data = response.read()
        dataStr = data.decode("utf-8")
        metadata = json.loads(dataStr)

        # set values from the leader meta-data
        self._zone = metadata['compute']['location']
        self.clusterName = metadata['compute']['resourceGroupName']
        tagsStr = metadata['compute']['tags']
        tags = dict(item.split(":") for item in tagsStr.split(";"))
        self._owner = tags.get('owner', 'no-owner')
        leader = self.getLeader()
        self._leaderPrivateIP = leader.privateIP
        self._setSSH()  # create id_rsa.pub file on the leader if it is not there
        self._masterPublicKeyFile =  self.LEADER_HOME_DIR + '.ssh/id_rsa.pub'

        # Add static nodes to /etc/hosts since Azure sometimes fails to find them with DNS
        map(lambda x: self._addToHosts(x), self.getProvisionedWorkers(None))


    def launchCluster(self, leaderNodeType, leaderStorage, owner, **kwargs):
        """
        Launches an Azure cluster using Ansible.
        A resource group is created for the cluster. All the virtual machines are created within this
        resource group.

        Cloud-config is called during vm creation to create directories and launch the appliance.

        The azureStorageCredentials must be passed in kwargs. These credentials allow access to
        Azure jobStores.
        """
        self._owner = owner
        self._masterPublicKeyFile = kwargs['publicKeyFile']
        if not self._masterPublicKeyFile:
            raise RuntimeError("The Azure provisioner requires a public key file.")
        storageCredentials = kwargs['azureStorageCredentials']
        if not storageCredentials:
            raise RuntimeError("azureStorageCredentials must be given.")

        self._checkValidClusterName()
        self._checkIfClusterExists()

        # Create the cluster.
        clusterArgs = {
            'resgrp': self.clusterName,  # The resource group, which represents the cluster.
            'region': self._zone
        }
        self.callPlaybook(self.playbook['create-cluster'], clusterArgs, wait=True)

        ansibleArgs = {
            'vmsize': leaderNodeType,
            'resgrp': self.clusterName,  # The resource group, which represents the cluster.
            'region': self._zone,
            'role': "leader",
            'owner': self._owner,  # Just a tag.
            'diskSize': str(leaderStorage),  # TODO: not implemented
            'publickeyfile': self._masterPublicKeyFile   # The users public key to be added to authorized_keys
        }
        # Ansible reads the cloud-config script from a file.
        with tempfile.NamedTemporaryFile(delete=False) as t:
            userData =  self._getCloudConfigUserData('leader')
            t.write(userData)
            ansibleArgs['cloudconfig'] = t.name

        # Launch the leader VM.
        retries = 0
        while True:
            instanceName = 'l' + str(uuid.uuid4())
            ansibleArgs['vmname'] = instanceName
            # Azure limits the name to 24 characters, no dashes.
            ansibleArgs['storagename'] = instanceName.replace('-', '')[:24]

            self.callPlaybook(self.playbook['create'], ansibleArgs, wait=True)
            try:
                leaderNode = self.getLeader()
            except IndexError:
                raise RuntimeError("Failed to launcher leader")
            self._leaderPrivateIP = leaderNode.privateIP # IP available as soon as the playbook finishes

            try:
                # Fix for DNS failure.
                self._addToHosts(leaderNode, leaderNode.publicIP)

                leaderNode.waitForNode('toil_leader') # Make sure leader appliance is up.

                # Transfer credentials
                if storageCredentials is not None:
                    fullPathCredentials = os.path.expanduser(storageCredentials)
                    if os.path.isfile(fullPathCredentials):
                        leaderNode.injectFile(fullPathCredentials, self.LEADER_HOME_DIR, 'toil_leader')
                ansibleCredentials = '.azure/credentials'
                fullPathAnsibleCredentials = os.path.expanduser('~/' + ansibleCredentials)
                if os.path.isfile(fullPathAnsibleCredentials):
                    leaderNode.sshAppliance('mkdir', '-p', self.LEADER_HOME_DIR + '.azure')
                    leaderNode.injectFile(fullPathAnsibleCredentials,
                                          self.LEADER_HOME_DIR + ansibleCredentials,
                                          'toil_leader')
                break #success!
            except RuntimeError as e:
                self._terminateNode(instanceName, False) # remove failed leader
                retries += 1
                if retries == 3:
                    logger.debug("Leader appliance failed to start. Giving up.")
                    raise e
                logger.debug("Leader appliance failed to start, retrying. (Error %s)" % e)


        logger.info('Launched leader')

    def _checkIfClusterExists(self):
        """
        Try deleting the resource group. This will fail if it exists and raise an exception.
        """
        ansibleArgs = {
            'resgrp': self.clusterName,
            'region': self._zone
        }
        try:
            self.callPlaybook(self.playbook['check-cluster'], ansibleArgs, wait=True)
        except RuntimeError:
            logger.info("The cluster could not be created. Try deleting the cluster if it already exits.")
            raise

    def destroyCluster(self):
        nodes = self._getNodes()
        self.terminateNodes(nodes) # this will terminate nodes in parallel
        ansibleArgs = {
            'resgrp': self.clusterName,
        }
        self.callPlaybook(self.playbook['destroy'], ansibleArgs, wait=True)

    def addNodes(self, nodeType, numNodes, preemptable=False, spotBid=None):
        assert self._leaderPrivateIP # for getCloudConfigUserData

        ansibleArgs = dict(vmsize=nodeType,
                           resgrp=self.clusterName,
                           region=self._zone,
                           diskSize=str(self._nodeStorage),
                           owner=self._owner,
                           role="worker",
                           publickeyfile=self._masterPublicKeyFile)
        with tempfile.NamedTemporaryFile(delete=False) as t:
            userData =  self._getCloudConfigUserData('worker')
            t.write(userData)
            ansibleArgs['cloudconfig'] = t.name

        instances = []
        for i in range(numNodes):
            # Wait for the last one
            wait = True if i == numNodes - 1 else False
            name = 'w' + str(uuid.uuid4())
            ansibleArgs['vmname'] = name
            storageName = name
            ansibleArgs['storagename'] = storageName.replace('-', '')[:24]
            instances.append(name)
            self.callPlaybook(self.playbook['create'], ansibleArgs, wait=wait, tags=['untagged'])

        # Wait for nodes - needed if transferring credential files
        allWorkers = self.getProvisionedWorkers(None)
        nodesIndex = dict((x.name, x) for x in allWorkers)
        numLaunched = 0
        for vmName in instances:
            if vmName in nodesIndex:
                try:
                    self._addToHosts(nodesIndex[vmName], nodesIndex[vmName].publicIP)
                    if self._onLeader:
                        self._addToHosts(nodesIndex[vmName]) # add to leader, too
                    nodesIndex[vmName].waitForNode('toil_worker')
                    numLaunched += 1
                except RuntimeError as e:
                    print(e)
                # TODO: add code to transfer sse key here
            else:
                logger.debug("Instance %s failed to launch", vmName)
        return numLaunched

    def getNodeShape(self, nodeType=None, preemptable=False):
        # FIXME: this should only needs to be called once, but failed
        self._instanceTypes = self._azureComputeClient.virtual_machine_sizes.list(self._zone)

        instanceType = (vmType for vmType in self._instanceTypes if vmType.name == nodeType).next()
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

    def _addToHosts(self, node, destinationIP=None):
        """
        Add an "privateIP hostname" line to the /etc/hosts file. If destinationIP is given,
        do this on the remote machine.

        Azure VMs sometimes fail to initialize, causing the appliance to fail.
        This error is given:
           Failed to obtain the IP address for 'l7d41a19b-15a6-442c-8ba1-9678a951d824';
           the DNS service may not be able to resolve it: Name or service not known.
        This method is a fix.

        :param node: Node to add to /etc/hosts.
        :param destinationIP: A remote host's address
        """
        cmd = "echo %s %s | sudo tee --append /etc/hosts > /dev/null" % (node.privateIP, node.name)
        logger.debug("Running command %s on %s" % (cmd, destinationIP))
        if destinationIP:
            subprocess.Popen(["ssh", "-oStrictHostKeyChecking=no", "core@%s" % destinationIP, cmd])
        else:
            subprocess.Popen(cmd, shell=True)

    def _getNodes(self, role=None, nodeType=None):
        """
        Return a list of Node objects representing the instances in the cluster
        with the given role and nodeType.
        :param role: leader, work, or None for both
        :param nodeType: An instance type or None for all types.
        :return: A list of Node objects.
        """
        allNodes = self._azureComputeClient.virtual_machines.list(self.clusterName)
        rv = []
        allNodeNames = []
        for node in allNodes:
            allNodeNames.append(node.name)
            nodeRole = node.tags.get('role', None)
            if node.provisioning_state != 'Succeeded' or (role is not None and nodeRole != role):
                continue
            if nodeType and node.hardware_profile.vm_size != nodeType:
                continue

            network_interface = self._azureNetworkClient.network_interfaces.get(self.clusterName, node.name)
            if not network_interface.ip_configurations:
                continue # no networks assigned to this node
            publicIP = self._azureNetworkClient.public_ip_addresses.get(self.clusterName, node.name)
            rv.append(Node(
                publicIP=publicIP.ip_address,
                privateIP=network_interface.ip_configurations[0].private_ip_address,
                name=node.name,
                launchTime=None,  # Not used with Azure.
                nodeType=node.hardware_profile.vm_size,
                preemptable=False) # Azure doesn't have preemptable nodes
            )
        logger.debug('All nodes in cluster: ' + ', '.join(allNodeNames))
        return rv

    def terminateNodes(self, nodes, preemptable=False):
        for counter, node in enumerate(nodes):
            wait = True if counter == len(nodes) - 1 else False
            self._terminateNode(node.name, wait)
            ansibleArgs = {
                'vmname': node.name,
                'resgrp': self.clusterName,
                'storagename': node.name.replace('-', '')[:24]
            }

    def _terminateNode(self, name, wait):
        ansibleArgs = {
            'vmname': name,
            'resgrp': self.clusterName,
            'storagename': name.replace('-', '')[:24]
        }
        self.callPlaybook(self.playbook['delete'], ansibleArgs, wait=wait)

    def _checkValidClusterName(self):
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
