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
            raise RuntimeError("Invalid cluster name. Azure instance names must be between"
                               "3-24 characters and be composed of only numbers and lowercase"
                               "letters.")

        self.clusterName = clusterName
        self.region = config.region
        self.playbook = {
            'create': 'create-azure-vm.yml',
            'delete': 'delete-azure-vm.yml',
        }
        playbooks = os.path.dirname(os.path.realpath(__file__))
        super(AzureProvisioner, self).__init__(playbooks, "azure_rm.py", config)

    def launchCluster(self, instanceType, keyName, spotBid=None, **kwargs):
        if spotBid:
            raise NotImplementedError("Ansible does not support provisioning spot instances")

        # Generate instance name (cluster name followed by a random identifier)
        # fixme: length
        instanceName = self.clusterName + str(uuid.uuid1()).split("-")[-1][1:4]
        args = {
            'vmsize': instanceType,
            'vmname': instanceName,
            'resgrp': self.clusterName,
            'role': "leader",
            'image': applianceSelf(),
            'entrypoint': "mesos-master",
            # This is unclear and should be accompanied by an explanation.
            'sshKey': 'AAAAB3NzaC1yc2Enoauthorizedkeyneeded',
            '_args': leaderArgs.format(name=self.clusterName),
        }

        # Populate cloud-config file
        with open(os.path.join(self.playbooks, "cloud-config"), "r") as f:
            configRaw = f.read()

        with tempfile.NamedTemporaryFile(delete=False) as t:
            t.write(configRaw.format(**args))
            args['cloudconfig'] = t.name

        # Start and configure instance with Ansible
        self.callPlaybook(self.playbook['create'], args)

    def destroyCluster(self, zone):
        # TODO: should destroy entire cluster, then leader
        args = {
            'vmname': self.clusterName,
            'resgrp': self.clusterName,
        }
        self.callPlaybook(self.playbook['delete'], args)

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
            # Calling the playbook with `tags=['abridged']` skips creation of
            # resource group, security group, etc.
            self.callPlaybook(self.playbook['create'], args, tags=['abridged'])

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

    def getProvisionedWorkers(self, nodeType, preemptable):
        # Data model info: https://docs.ansible.com/ansible/latest/guide_azure.html#dynamic-inventory-script
        allNodes = self._getInventory()
        workerNodes = filter(lambda x: x['tags']['role'] == "worker", allNodes)
        healthyNodes = filter(lambda x: x['powerstate'] == "running", allNodes)

        logger.debug('All nodes in cluster: ' + allNodes)
        logger.debug('All workers found in cluster: ' + workerNodes)

        rv = []
        for node in healthyNodes:
            rv.append(Node(
                publicIP=node['public_ip'],
                privateIP=node['private_ip'],
                name=node['name'],
                launchTime=node['tags']['launchtime'],  # FIXME: Is defining this as a tag enough?
                nodeType=node['virtual_machine_size'],
                preemptable=preemptable)
            )
        return rv

    def remainingBillingInterval(self):
        raise NotImplementedError("Ansible does not support provisioning spot instances.")

    def terminateNodes(self, nodes):
        for node in nodes:
            args = {
                'vmname': node.name,
                'resgrp': self.clusterName,
            }
            self.callPlaybook(self.playbook['delete'], args, tags=['cluster'])

    @staticmethod
    def isValidClusterName(name):
        """Valid Azure cluster names must be between 3 and 24 characters in
        length and use numbers and lower-case letters only."""
        if len(name) > 24 or len(name) < 3:
            return False
        if any(not (c.isdigit() or c.islower()) for c in name):
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
