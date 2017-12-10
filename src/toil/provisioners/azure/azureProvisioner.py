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
import logging
import os.path
import sys
import tempfile
import uuid

from toil import applianceSelf
from toil.provisioners.ansibleDriver import AnsibleDriver
from toil.provisioners.aws import leaderArgs

logger = logging.getLogger(__name__)


class AzureProvisioner(AnsibleDriver):
    def __init__(self, clusterName, **config):
        if not self.isValidClusterName(clusterName):
            raise RuntimeError("Invalid cluster name. Azure instance names must be between"
                               "3-24 characters and be composed of only numbers and lowercase"
                               "letters.")

        self.clusterName = clusterName
        self.playbook = {
            'create': 'create-azure-vm.yml',
            'delete': 'delete-azure-vm.yml',
        }
        playbooks = os.path.dirname(os.path.realpath(__file__))
        inventory = "azure_rm.py"
        super(AzureProvisioner, self).__init__(playbooks, inventory, config)

    def launchCluster(self, instanceType, keyName, spotBid=None, **kwargs):
        if spotBid:
            raise NotImplementedError("Ansible does not support provisioning spot instances")

        # Generate instance name (cluster name followed by a random identifier)
        instanceName = self.clusterName + "-" + str(uuid.uuid1()).split("-")[-1]
        args = {
            'vmsize': instanceType,
            'vmname': instanceName,
            'resgrp': self.clusterName,
            'role': "leader",
            'image': applianceSelf(),
            'entrypoint': "mesos-master",
            # This is confusing and should be accompanied by an explanation.
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

    def getNodeShape(self):
        raise NotImplementedError
        # Commented because the current solution that I have seems a little much.
        # To my knowledge, Azure doesn't provide an easy way to get the specs of
        # an instance without authenticating, which seems like overkill for
        # this method. Plus, configparser in Python 2.x is kinda not as cool as
        # the one in Python 3.x and I don't want to deal with that.

        # from azure.common.credentials import ServicePrincipalCredentials
        # from azure.mgmt.compute import ComputeManagementClient
        # import ConfigParser
        # config = ConfigParser.ConfigParser()
        # config.read("~/.azure/credentials")
        # credentials = ServicePrincipalCredentials(**config['default'])
        # client = ComputeManagementClient(credentials)
        # Get node type from self._getInventory()
        # ...

    def getProvisionedWorkers(self):
        raise NotImplementedError("Ansible does not support provisioning spot instances.")

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
