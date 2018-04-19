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
import logging
import os
import subprocess

from toil.provisioners.abstractProvisioner import AbstractProvisioner

logger = logging.getLogger(__name__)


class AnsibleDriver(AbstractProvisioner):
    """
    Wrapper class for Ansible calls.
    """
    def __init__(self, playbooks, clusterName, zone, nodeStorage):
        self.playbooks = playbooks
        super(AnsibleDriver, self).__init__(clusterName, zone, nodeStorage)

    def callPlaybook(self, playbook, ansibleArgs, wait=True, tags=["all"]):
        """
        Run a playbook.

        :param playbook: An Ansible playbook to run.
        :param ansibleArgs: Arguments to pass to the playbook.
        :param wait: Wait for the play to finish if true.
        :param tags: Control tags for the play.
        """
        playbook = os.path.join(self.playbooks, playbook)  # Path to playbook being executed
        verbosity = "-vvvvv" if logger.isEnabledFor(logging.DEBUG) else "-v"
        command = ["ansible-playbook", verbosity, "--tags", ",".join(tags), "--extra-vars"]
        command.append(" ".join(["=".join(i) for i in ansibleArgs.items()]))  # Arguments being passed to playbook
        command.append(playbook)

        logger.info("Executing Ansible call `%s`", " ".join(command))
        p = subprocess.Popen(command)
        if wait:
            p.communicate()
            if p.returncode != 0:
                # FIXME: parse error codes
                raise RuntimeError("Ansible reported an error when executing playbook %s" % playbook)

