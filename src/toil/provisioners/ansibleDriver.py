# Copyright (C) 2015 UCSC Computational Genomics Lab
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
import subprocess

import pipettor

from toil.provisioners.abstractProvisioner import AbstractProvisioner

logger = logging.getLogger(__name__)


class AnsibleDriver(AbstractProvisioner):
    contrib = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'contrib')
    inventory = '' # to be set by child class

    def __init__(self, playbooks, config=None):
        self.playbooks = playbooks
        super(AnsibleDriver, self).__init__(config)

    def callPlaybook(self, playbook, args, tags=[]):
        # playbook: path to playbook being executed
        playbook = os.path.join(self.playbooks, playbook)
        # extravar: variables being passed to Ansible
        extravar = " ".join(["=".join(i) for i in args.items()])  # Arguments being passed to Ansible
        # ssh_args:  since we need to manually provide the cloud-config file over ssh under a set of
        # special circumstances (i.e. we can't take advantage of the full ansible featureset) we have
        # to disable host key checking
        ssh_args = "--ssh-extra-args '-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no'"
        if tags:
            tags = "--tags '" + ",".join(tags) + "'"
        else:
            tags = ""
        command = "ansible-playbook -v %s %s --extra-vars '%s' %s" % (tags, ssh_args, extravar, playbook)

        logger.info("Executing Ansible call `%s`" % command)
        return pipettor.runlex(command, logger=logger)

    @classmethod
    def _getInventory(cls, clusterName):
        """Lists all nodes in the cluster"""
        # tags = ",".join([":".join(i) for i in tags.items()])
        command = ["python", os.path.join(cls.contrib, cls.inventory), "--resource-groups", clusterName]
        data = subprocess.check_output(command)
        return json.loads(data)
