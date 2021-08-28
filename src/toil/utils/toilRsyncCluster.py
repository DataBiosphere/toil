# Copyright (C) 2015-2021 Regents of the University of California
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
"""Rsyncs into the toil appliance container running on the leader of the cluster."""
import argparse
import logging

from toil.common import parser_with_common_options
from toil.provisioners import cluster_factory
from toil.statsAndLogging import set_logging_from_options

logger = logging.getLogger(__name__)


def main() -> None:
    parser = parser_with_common_options(provisioner_options=True, jobstore_option=False)
    parser.add_argument("--insecure", dest='insecure', action='store_true', required=False,
                        help="Temporarily disable strict host key checking.")
    parser.add_argument("args", nargs=argparse.REMAINDER, help="Arguments to pass to"
                        "`rsync`. Takes any arguments that rsync accepts. Specify the"
                        " remote with a colon. For example, to upload `example.py`,"
                        " specify `toil rsync-cluster -p aws test-cluster example.py :`."
                        "\nOr, to download a file from the remote:, `toil rsync-cluster"
                        " -p aws test-cluster :example.py .`")
    options = parser.parse_args()
    set_logging_from_options(options)
    cluster = cluster_factory(provisioner=options.provisioner,
                              clusterName=options.clusterName,
                              zone=options.zone)
    cluster.getLeader().coreRsync(args=options.args, strict=not options.insecure)
