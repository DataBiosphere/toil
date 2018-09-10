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
"""
Terminates the specified cluster and associated resources
"""
import os
from toil.provisioners import clusterFactory
from toil.lib.bioio import parseBasicOptions, getBasicOptionParser
from toil.utils import addBasicProvisionerOptions

def removeClusterFromList(name, provisioner, zone):
    """Remove a given cluster's information from the list of active clusters."""
    if os.path.exists('/tmp/toilClusterList.txt'):
        with open('/tmp/toilClusterList.txt.new', 'w') as new:
            with open('/tmp/toilClusterList.txt', 'r') as old:
                for line in old:
                    if not line.startswith('{}\t{}\t{}'.format(name, provisioner, zone)):
                        new.write(line)

        os.remove('/tmp/toilClusterList.txt')
        os.rename('/tmp/toilClusterList.txt.new', '/tmp/toilClusterList.txt')

def main():
    parser = getBasicOptionParser()
    parser = addBasicProvisionerOptions(parser)
    config = parseBasicOptions(parser)
    cluster = clusterFactory(provisioner=config.provisioner,
                             clusterName=config.clusterName,
                             zone=config.zone)
    cluster.destroyCluster()

    # Removing the entry here ensures that destroyCluster() is successful before removing the entry. In contrast,
    # the entries are added to this file via AbstractProvisioner.addClusterToList(). They are added in the provisioner,
    # immediately after their creation, to ensure they are tracked despite an error later on in
    # AbstractProvisioner.launchCluster().
    removeClusterFromList(name=config.clusterName,
                          provisioner=config.provisioner,
                          zone=config.zone)
