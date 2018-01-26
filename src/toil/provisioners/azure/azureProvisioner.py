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

from toil import applianceSelf
from toil.provisioners import Node
from toil.provisioners.abstractProvisioner import Shape
from toil.provisioners.ansibleDriver import AnsibleDriver
from toil.provisioners.aws import leaderArgs
from toil.provisioners.aws import workerArgs

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient

logger = logging.getLogger(__name__)


class AzureProvisioner(AnsibleDriver):
    AnsibleDriver.inventory = 'azure_rm.py'

    def __init__(self, config=None):

        self.playbook = {
            'create': 'create-azure-vm.yml',
            'delete': 'delete-azure-vm.yml',
            'destroy': 'delete-azure-cluster.yml'
        }
        playbooks = os.path.dirname(os.path.realpath(__file__))
        super(AzureProvisioner, self).__init__(playbooks, config)


        if config:
            mdUrl = "http://169.254.169.254/metadata/instance?api-version=2017-08-01"
            header={'Metadata': 'True'}
            request = urllib.request.Request(url=mdUrl, headers=header)
            response = urllib.request.urlopen(request)
            data = response.read()
            dataStr = data.decode("utf-8")
            metadata = json.loads(dataStr)

            self.zone = metadata['compute']['location']
            self.clusterName = metadata['compute']['resourceGroupName']
            self.tags = metadata['compute']['tags']
            #self.leaderIp = metadata['network']['interface'][0]['ipv4']['ipaddress'][0]['privateIpAddress']

            logger.info('****LOADING %s', self.clusterName)

            self.keyName = 'core'
            self.leaderIP = self._getLeader(self.clusterName)['private_ip']
            self.masterPublicKey = self._setSSH()
            self.nodeStorage = config.nodeStorage
            self.nonPreemptableNodeTypes = []
            for nodeTypeStr in config.nodeTypes:
                nodeBidTuple = nodeTypeStr.split(":")
                if len(nodeBidTuple) != 2:
                    self.nonPreemptableNodeTypes.append(nodeTypeStr)
            self.nonPreemptableNodeShapes = [self.getNodeShape(nodeType=nodeType, preemptable=False) for nodeType in self.nonPreemptableNodeTypes]

            self.nodeShapes = self.nonPreemptableNodeShapes
            self.nodeTypes = self.nonPreemptableNodeTypes
        else:
            self.clusterName = None
            self.leaderIP = None
            self.keyName = None
            self.tags = None
            self.masterPublicKey = None
            self.nodeStorage = None

    def launchCluster(self, instanceType, keyName, clusterName, zone, spotBid=None, **kwargs):
        """Launches an Azure cluster using Ansible."""
        if spotBid:
            raise NotImplementedError("Ansible does not support provisioning spot instances")

        if not self.isValidClusterName(clusterName):
            raise RuntimeError("Invalid cluster name. See the Azure documentation for information "
                               "on cluster naming conventions: "
                               "https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions")
        self.clusterName = clusterName
        self.keyName = keyName
        self.region = zone

        # Azure VMs must be named, so we need to generate one. Instance names must
        # be composed of only alphanumeric characters, underscores, and hyphens
        # (see https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions).
        instanceName = 'l' + str(uuid.uuid4())

        # SSH keys aren't managed with Azure, so for now we can just take the path of a public key.
        with open(os.path.expanduser(keyName), "r") as k:
            self.sshKey = k.read().strip().split(" ")[1:]

        # TODO Check to see if resource group already exists and throw and error if so.

        args = {
            'vmsize': instanceType,
            'vmname': instanceName,
            'resgrp': self.clusterName,
            'region': self.region,
            'image': applianceSelf(),
            'role': "leader",
            'entrypoint': "mesos-master",
            # TODO: This is unclear and should be accompanied by an explanation.
            # 'sshKey': 'AAAAB3NzaC1yc2Enoauthorizedkeyneeded',
            'sshKey': " ".join(self.sshKey),
            'keyname': self.keyName,
            '_args': leaderArgs.format(name=self.clusterName),
        }
        # Populate cloud-config file and pass it to Ansible
        with open(os.path.join(self.playbooks, "cloud-config"), "r") as f:
            configRaw = f.read()
        with tempfile.NamedTemporaryFile(delete=False) as t:
            t.write(configRaw.format(**args))
            args['cloudconfig'] = t.name
        self.callPlaybook(self.playbook['create'], args)

        logger.info('Launched non-preemptable leader')


        self.leaderIP = self._getLeader(self.clusterName)['private_ip']

        workersCreated = 0
        for nodeType, workers in zip(kwargs['nodeTypes'], kwargs['numWorkers']):
            workersCreated += self.addNodes(nodeType=nodeType, numNodes=workers)
        logger.info('Added %d workers', workersCreated)

    def _provisionNode(self, args):
        # Populate cloud-config file and pass it to Ansible
        with open(os.path.join(self.playbooks, "cloud-config"), "r") as f:
            configRaw = f.read()
        with tempfile.NamedTemporaryFile(delete=False) as t:
            t.write(configRaw.format(**args))
            args['cloudconfig'] = t.name

        # Launch the cluster with Ansible
        self.callPlaybook(self.playbook['create'], args, tags=['untagged'])
        # Calling the playbook with `tags=['untagged']` skips creation of
        # resource group, security group, etc.

    @staticmethod
    def destroyCluster(clusterName, zone):
        self = AzureProvisioner()
        args = {
            'resgrp': clusterName,
        }
        self.callPlaybook(self.playbook['destroy'], args)

    def addNodes(self, nodeType, numNodes):
        args = {
            'vmsize': nodeType,
            'vmname': 'wrongName',
            'resgrp': self.clusterName,
            'region': self.region,
            'image': applianceSelf(),
            'role': "worker",
            'entrypoint': "mesos-slave",
            'sshKey': " ".join(self.sshKey), # self.masterPublicKey
            'keyname': self.keyName,
            '_args': workerArgs.format(ip=self.leaderIP, preemptable=False, keyPath='')
        }

        for i in range(numNodes):
            args['vmname'] = 'w' + str(uuid.uuid4())
            self._provisionNode(args)

        # TODO: check that nodes launched
        return numNodes

    def getNodeShape(self, nodeType):

        # Fetch Azure credentials from the CLI config
        azureCredentials = ConfigParser.SafeConfigParser()
        azureCredentials.read(os.path.expanduser("~/.azure/credentials"))
        client_id = azureCredentials.get("default", "client_id")
        secret = azureCredentials.get("default", "secret")
        tenant = azureCredentials.get("default", "tenant")
        subscription = azureCredentials.get("default", "subscription_id")

        # Authenticate to Azure API and fetch list of instance types
        credentials = ServicePrincipalCredentials(client_id=client_id, secret=secret, tenant=tenant)
        client = ComputeManagementClient(credentials, subscription)
        instanceTypes = client.virtual_machine_sizes.list(self.region)

        # Data model: https://docs.microsoft.com/en-us/python/api/azure.mgmt.compute.v2017_12_01.models.virtualmachinesize?view=azure-python
        instanceType = (vmType for vmType in instanceTypes if vmType["name"] == nodeType).next()

        disk = instanceType.max_data_disk_count * instanceType.os_disk_size_in_mb * 2 ** 30

        # Underestimate memory by 100M to prevent autoscaler from disagreeing with
        # mesos about whether a job can run on a particular node type
        memory = (instanceType.memory_in_mb - 0.1) * 2 ** 30

        return Shape(wallTime=60 * 60,
                     memory=memory,
                     cores=instanceType.number_of_cores,
                     disk=disk,
                     preemptable=False)

    # TODO: Implement nodeType
    def getProvisionedWorkers(self, nodeType, preemptable=False):
        # Data model info: https://docs.ansible.com/ansible/latest/guide_azure.html#dynamic-inventory-script
        allNodes = self._getInventory(self.clusterName)

        logger.debug('All nodes in cluster: ' + str(allNodes.get('role_leader', None)))
        logger.debug('All workers found in cluster: ' + str(allNodes.get('role_worker', None)))

        rv = []
        for node in allNodes['azure']:
            node = allNodes['_meta']['hostvars'][node]
            rv.append(Node(
                publicIP=node['public_ip'],
                privateIP=node['private_ip'],
                name=node['name'],
                launchTime=None,  # FIXME
                nodeType=node['virtual_machine_size'],
                preemptable=False)
            )
        return rv

    def remainingBillingInterval(self):
        raise NotImplementedError("Ansible does not support provisioning spot instances.")

    def terminateNodes(self, nodes, preemptable=False):
        for node in nodes:
            args = {
                'vmname': node.name,
                'resgrp': self.clusterName,
            }
            self.callPlaybook(self.playbook['delete'], args, tags=['cluster'])

    # TODO: Refactor such that cluster name is LCD of vnet name/storage name/etc
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
        acceptedSpecialChars = ('_', '-', '(', ')', '.')
        if len(name) < 1 or len(name) > 90:
            return False
        if name.endswith("."):
            return False
        if any(not(c.isdigit() or c.islower() or c in acceptedSpecialChars) for c in name):
            return False
        return True

    @classmethod
    def _getLeader(cls, clusterName):
        data = cls._getInventory(clusterName)
        return data['_meta']['hostvars'][data['role_leader'][0]]

    @classmethod
    def sshLeader(cls, clusterName, args=None, zone=None, **kwargs):
        logger.info('SSH ready')
        kwargs['tty'] = sys.stdin.isatty()
        command = args if args else ['bash']
        cls._sshAppliance(cls._getLeader(clusterName)['public_ip'], *command, **kwargs)

    @classmethod
    def rsyncLeader(cls, clusterName, args, zone=None, **kwargs):
        cls._coreRsync(cls._getLeader(clusterName)['public_ip'], args, **kwargs)


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
