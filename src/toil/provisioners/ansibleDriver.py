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

import pipettor

from toil.provisioners.abstractProvisioner import AbstractProvisioner

logger = logging.getLogger(__name__)


class AnsibleDriver(AbstractProvisioner):
    def __init__(self, playbooks, inventory, config=None):
        self.playbooks = playbooks
        self.inventory = inventory
        self.contrib = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'contrib')
        super(AnsibleDriver, self).__init__(config)

    def callPlaybook(self, playbook, args, tags=[]):
        playbook = os.path.join(self.playbooks, playbook)
        extravar = " ".join(["=".join(i) for i in args.items()])
        if tags:
            tags = "--tags '" + ",".join(tags) + "'"
        else:
            tags = ""
        command = "ansible-playbook -c local --extra-vars '%s' %s" % (extravar, playbook)

        logger.info("Executing Ansible call `%s`" % command)
        return pipettor.runlex(command, logger=logger)

    def _getInventory(self, tags={}):
        """Lists all nodes in the cluster"""
        # tags = ",".join([":".join(i) for i in tags.items()])
        command = "python %s --tags toil-cluster:%s" % (os.path.join(self.contrib, self.inventory), self.clusterName)
        data = pipettor.runlex(command, logger=logger)
        return json.loads(data)
