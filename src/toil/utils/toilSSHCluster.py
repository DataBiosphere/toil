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
SSHs into the toil appliance container running on the leader of the cluster
"""
import argparse
import logging
from toil.provisioners import Cluster
from toil.lib.bioio import parseBasicOptions, setLoggingFromOptions, getBasicOptionParser
from toil.utils import addBasicProvisionerOptions

logger = logging.getLogger( __name__ )


def main():
    parser = getBasicOptionParser()
    parser = addBasicProvisionerOptions(parser)
    parser.add_argument('args', nargs=argparse.REMAINDER)
    config = parseBasicOptions(parser)
    setLoggingFromOptions(config)
    cluster = Cluster(provisioner=config.provisioner,
                      clusterName=config.clusterName, zone=config.zone)
    cluster.sshCluster(args=config.args)
