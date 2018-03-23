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
import urllib
import json
import subprocess
import time

from toil import applianceSelf
from toil.provisioners import Node
from toil.provisioners.abstractProvisioner import Shape
from toil.provisioners.ansibleDriver import AnsibleDriver
from toil.provisioners.aws import leaderArgs
from toil.provisioners.aws import workerArgs

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
    def __init__(self, config=None):

        self.playbook = {
            'create-cluster': 'create-azure-resourcegroup.yml',
            'create': 'create-azure-vm.yml',
            'delete': 'delete-azure-vm.yml',
            'destroy': 'delete-azure-cluster.yml'
        }
        playbooks = os.path.dirname(os.path.realpath(__file__))
        super(AzureProvisioner, self).__init__(playbooks, config)

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
            self.region = metadata['compute']['location']
            self.clusterName = metadata['compute']['resourceGroupName']
            tagsStr = metadata['compute']['tags']
            tags = dict(item.split(":") for item in tagsStr.split(";"))
            self.vmTags = None
            #self.leaderIp = metadata['network']['interface'][0]['ipv4']['ipaddress'][0]['privateIpAddress']

            self.keyName = tags.get('owner', 'no-owner')
            self.leaderIP = self._getNodes('leader')[0].privateIP
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
            self.clusterName = None
            self.leaderIP = None
            self.keyName = None
            self.vmTags = None
            self.masterPublicKeyFile = None
            self.nodeStorage = None

    def launchCluster(self, leaderNodeType, keyName, clusterName, zone,
                      leaderStorage=50, nodeStorage=50, spotBid=None, **kwargs):
        """
        Launches an Azure cluster using Ansible.
        A resource group is created for the cluster. All the virtual machines are created within this
        resource group.

        Cloud-config is called during vm creation to create directories and launch the appliance.
        """
        if spotBid:
            raise NotImplementedError("Ansible does not support provisioning spot instances")

        if not self.isValidClusterName(clusterName):
            raise RuntimeError("Invalid cluster name. See the Azure documentation for information "
                               "on cluster naming conventions: "
                               "https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions")
        self.clusterName = clusterName
        self.keyName = keyName
        self.region = zone
        self.nodeStorage = nodeStorage
        self.masterPublicKeyFile = kwargs['publicKeyFile']

        # Try deleting the resource group. This will fail if it exists.
        ansibleArgs = {
            'resgrp': self.clusterName,
            'region': self.region
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
            'region': self.region,
            'role': "leader",
            'owner': self.keyName,  # Just a tag.
            'diskSize': str(leaderStorage),  # TODO: not implemented
            'publickeyfile': self.masterPublicKeyFile   # The users public key to be added to authorized_keys
        }
        ansibleArgs['cloudconfig'] = self._cloudConfig(cloudConfigArgs)
        self.callPlaybook(self.playbook['create'], ansibleArgs, wait=True)

        logger.info('Launched non-preemptable leader')

        # IP available as soon as the playbook finishes
        leader = self._getNodes('leader')[0]
        self.leaderIP = leader.privateIP

        # Make sure leader container is up.
        self._waitForNode(leader.publicIP, 'toil_leader')

        # Transfer credentials
        containerUserPath = '/root/'
        storageCredentials = kwargs['azureStorageCredentials']
        if storageCredentials is not None:
            fullPathCredentials = os.path.expanduser(storageCredentials)
            if os.path.isfile(fullPathCredentials):
                self._rsyncNode(leader.publicIP, [fullPathCredentials, ':' + containerUserPath],
                                applianceName='toil_leader')

        ansibleCredentials = '.azure/credentials'
        fullPathAnsibleCredentials = os.path.expanduser('~/' + ansibleCredentials)
        if os.path.isfile(fullPathAnsibleCredentials):
            self._sshAppliance(leader.publicIP, 'mkdir', '-p', containerUserPath + '.azure')
            self._rsyncNode(leader.publicIP,
                            [fullPathAnsibleCredentials, ':' + containerUserPath + ansibleCredentials],
                            applianceName='toil_leader')
        # Add workers
        workersCreated = 0
        for nodeType, workers in zip(kwargs['nodeTypes'], kwargs['numWorkers']):
            workersCreated += self.addNodes(nodeType=nodeType, numNodes=workers)
        logger.info('Added %d workers', workersCreated)

    def _cloudConfig(self, args):
        # Populate cloud-config file and pass it to Ansible
        with open(os.path.join(self.playbooks, "cloud-config"), "r") as f:
            configRaw = f.read()
        with tempfile.NamedTemporaryFile(delete=False) as t:
            t.write(configRaw.format(**args))
            return t.name

    @staticmethod
    def destroyCluster(clusterName, zone):
        self = AzureProvisioner()
        ansibleArgs = {
            'resgrp': clusterName,
        }
        self.callPlaybook(self.playbook['destroy'], ansibleArgs)

    def addNodes(self, nodeType, numNodes, preemptable=False):

        cloudConfigArgs = {
            'image': applianceSelf(),
            'role': "worker",
            'entrypoint': "mesos-slave",
            '_args': workerArgs.format(ip=self.leaderIP, preemptable=False, keyPath='')
        }

        ansibleArgs = dict(vmsize=nodeType,
                           resgrp=self.clusterName,
                           region=self.region,
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
        allInstances = self.getProvisionedWorkers(None)
        instancesIndex = dict((x.name, x) for x in allInstances)
        for vmName in instances:
            if vmName in instancesIndex:
                self._waitForNode(instancesIndex[vmName].publicIP, 'toil_worker')
                # TODO: add code to transfer sse key here
            else:
                logger.debug("Instance %s failed to launch", vmName)

        return len(instances)

    def getNodeShape(self, nodeType=None, preemptable=False):
        instanceTypes = self._azureComputeClient.virtual_machine_sizes.list(self.region)

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

    def remainingBillingInterval(self, node):
        return 1

    def terminateNodes(self, nodes, preemptable=False):
        for counter, node in enumerate(nodes):
            ansibleArgs = {
                'vmname': node.name,
                'resgrp': self.clusterName,
                'storagename': node.name.replace('-', '')[:24]
            }
            wait = True if counter == len(nodes) - 1 else False
            self.callPlaybook(self.playbook['delete'], ansibleArgs, wait=wait)

    @staticmethod
    def isValidClusterName(name):
        """As Azure resource groups are used to identify instances in the same cluster,
        cluster names must also be valid Azure resource group names - that is:
        * Between 1 and 90 characters
        * Using alphanumeric characters
        * Using no special characterrs excepts underscores, parantheses, hyphens, and periods
        * Not ending with a period
        More info on naming conventions: https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions
        """
        #FIXME - recheck the link above and verify length (conflicts with other names: virtual network or network security group)
        acceptedSpecialChars = ('_', '-', '(', ')', '.')
        if len(name) < 1 or len(name) > 90:
            return False
        if name.endswith("."):
            return False
        if any(not(c.isdigit() or c.islower() or c in acceptedSpecialChars) for c in name):
            return False
        return True

    @classmethod
    def _getLeaderPublicIP(cls, clusterName):
        provisioner = AzureProvisioner()
        provisioner.clusterName = clusterName
        return provisioner._getNodes('leader')[0].publicIP


    @classmethod
    def sshLeader(cls, clusterName, args=None, zone=None, **kwargs):
        logger.info('SSH ready')
        kwargs['tty'] = sys.stdin.isatty()
        command = args if args else ['bash']
        cls._sshAppliance(cls._getLeaderPublicIP(clusterName), *command, **kwargs)

    @classmethod
    def rsyncLeader(cls, clusterName, args, zone=None, **kwargs):
        cls._coreRsync(cls._getLeaderPublicIP(clusterName), args, **kwargs)

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

    def _waitForNode(self, instanceIP, role):
        # wait here so docker commands can be used reliably afterwards
        # TODO: make this more robust, e.g. If applicance doesn't exist, then this waits forever.
        self._waitForSSHKeys(instanceIP)
        self._waitForDockerDaemon(instanceIP)
        self._waitForAppliance(instanceIP, role)

    @classmethod
    def _waitForSSHKeys(cls, instanceIP):
        # the propagation of public ssh keys vs. opening the SSH port is racey, so this method blocks until
        # the keys are propagated and the instance can be SSH into
        while True:
            try:
                logger.info('Attempting to establish SSH connection...')
                cls._sshInstance(instanceIP, 'ps', sshOptions=['-oBatchMode=yes'])
            except RuntimeError:
                logger.info('Connection rejected, waiting for public SSH key to be propagated. Trying again in 10s.')
                time.sleep(10)
            else:
                logger.info('...SSH connection established.')
                # ssh succeeded
                return

    @classmethod
    def _waitForDockerDaemon(cls, ip_address):
        logger.info('Waiting for docker on %s to start...', ip_address)
        while True:
            output = cls._sshInstance(ip_address, '/usr/bin/ps', 'auxww')
            time.sleep(5)
            if 'dockerd' in output:
                # docker daemon has started
                logger.info('...Docker daemon started')
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
    def _rsyncNode(cls, ip, args, applianceName='toil_leader', **kwargs):
        remoteRsync = "docker exec -i %s rsync" % applianceName  # Access rsync inside appliance
        parsedArgs = []
        sshCommand = "ssh"
        strict = kwargs.pop('strict', False)
        if not strict:
            sshCommand = "ssh -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no"
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

