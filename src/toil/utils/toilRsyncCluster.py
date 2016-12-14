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
Rsyncs into the toil appliance container running on the leader of the cluster
"""
import argparse
import logging

from toil.lib.bioio import parseBasicOptions, setLoggingFromOptions, getBasicOptionParser
from toil.provisioners import Cluster
from toil.utils import addBasicProvisionerOptions


logger = logging.getLogger(__name__)


def main():
    parser = getBasicOptionParser()
    parser = addBasicProvisionerOptions(parser)
    parser.add_argument("args", nargs=argparse.REMAINDER, help="Arguments to pass to"
                        "`rsync`. Takes any arguments that rsync accepts. Specify the"
                        " remote with a colon. For example, to upload `example.py`,"
                        " specify `toil rsync-cluster -p aws test-cluster example.py :`."
                        "\nOr, to download a file from the remote:, `toil rsync-cluster"
                        " -p aws test-cluster :example.py .`")
    config = parseBasicOptions(parser)
    setLoggingFromOptions(config)
    cluster = Cluster(provisioner=config.provisioner, clusterName=config.clusterName)
    cluster.rsyncCluster(args=config.args)
