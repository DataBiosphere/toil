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

from toil import applianceSelf
from toil.provisioners import Node
from toil.provisioners.abstractProvisioner import Shape
from toil.provisioners.ansibleDriver import AnsibleDriver
from toil.provisioners.aws import leaderArgs

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient

logger = logging.getLogger(__name__)


class AzureProvisioner(AnsibleDriver):
    def __init__(self, clusterName, **config):
        if not self.isValidClusterName(clusterName):
            raise RuntimeError("Invalid cluster name. See the Azure documentation for information "
                               "on cluster naming conventions: "
                               "https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions")

        self.clusterName = clusterName
        # TODO: --zone must be mandatory for the Azure provisioner, or we need
        # a way of setting a sane default.
        self.region = config.get("zone", None)
        self.playbook = {
            'create': 'create-azure-vm.yml',
            'delete': 'delete-azure-vm.yml',
        }
        playbooks = os.path.dirname(os.path.realpath(__file__))
        super(AzureProvisioner, self).__init__(playbooks, "azure_rm.py", config)

    def launchCluster(self, instanceType, keyName, spotBid=None, **kwargs):
        """Launches an Azure cluster using Ansible."""
        if spotBid:
            raise NotImplementedError("Ansible does not support provisioning spot instances")

        # Azure VMs must be named, so we need to generate one. Instance names must
        # be composed of only alphanumeric characters, underscores, and hyphens
        # (see https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions).
        instanceName = str(uuid.uuid4())

        # SSH keys aren't managed with Azure, so for now we can just take the path of a public key.
        with open(os.path.expanduser(keyName), "r") as k:
            sshKey = k.read().strip().split(" ")[1:]

        # TODO Check to see if resource group already exists?
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
            'sshKey': " ".join(sshKey),
            'keyname': keyName,
            '_args': leaderArgs.format(name=self.clusterName),
        }
        self._provisionNode(args, preemptable=True)

    def _provisionNode(self, args, preemptable=False):
        # Populate cloud-config file and pass it to Ansible
        with open(os.path.join(self.playbooks, "cloud-config"), "r") as f:
            configRaw = f.read()
        with tempfile.NamedTemporaryFile(delete=False) as t:
            t.write(configRaw.format(**args))
            args['cloudconfig'] = t.name

        # Launch the cluster with Ansible
        self.callPlaybook(self.playbook['create'], args, tags=['abridged'] if not preemptable else None)
        # Calling the playbook with `tags=['abridged']` skips creation of
        # resource group, security group, etc.

    @staticmethod
    def destroyCluster(clusterName, zone):
        self = AzureProvisioner(clusterName)
        self.clusterName = clusterName  # monkey patch fixme

        workers = self.getProvisionedWorkers(None, preemptable=False)
        self.terminateNodes(workers)

        leader = self.getProvisionedWorkers(None, preemptable=True)
        self.terminateNodes(leader)

    def addNodes(self, nodeType, numNodes, preemptable):
        # TODO: update to configure with cloud-config
        args = {
            'vmsize': nodeType,
            'sshkey': self.sshKey,
            'vmname': self.clusterName,
            'resgrp': self.clusterName,
            'role': "worker",
        }
        for i in range(numNodes):
            self._provisionNode(args, preemptable=False)

    def getNodeShape(self, nodeType, preemptable=False):
        if preemptable:
            raise NotImplementedError("Ansible does not support provisioning spot instances")

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
                     preemptable=preemptable)

    # TODO: Implement nodeType
    def getProvisionedWorkers(self, nodeType, preemptable):
        # Data model info: https://docs.ansible.com/ansible/latest/guide_azure.html#dynamic-inventory-script
        allNodes = self._getInventory()

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
                preemptable=preemptable)
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
            self.callPlaybook(self.playbook['delete'], args, tags=['cluster'] if preemptable else None)

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

    def _getLeader(self):
        data = self._getInventory()
        return data['_meta']['hostvars'][data['role_leader'][0]]

    def sshLeader(self, clusterName, args=None, zone=None, **kwargs):
        logger.info('SSH ready')
        kwargs['tty'] = sys.stdin.isatty()
        command = args if args else ['bash']
        self._sshAppliance(self._getLeader()['public_ip'], *command, **kwargs)

    def rsyncLeader(self, clusterName, args, zone=None, **kwargs):
        self._coreRsync(self._getLeader()['public_ip'], args, **kwargs)
