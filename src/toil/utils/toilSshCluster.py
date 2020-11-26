# Copyright (C) 2015-2020 UCSC Computational Genomics Lab
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
"""SSH into the toil appliance container running on the leader of the cluster."""
import argparse
import logging
import sys

from toil.lib.bioio import parser_with_common_options, setLoggingFromOptions
from toil.provisioners import clusterFactory

logger = logging.getLogger(__name__)


def main():
    parser = parser_with_common_options(provisioner_options=True)
    parser.add_argument("--insecure", action='store_true',
                        help="Temporarily disable strict host key checking.")
    parser.add_argument("--sshOption", dest='sshOptions', default=[], action='append',
                        help="Pass an additional option to the SSH command.")
    parser.add_argument('args', nargs=argparse.REMAINDER)
    options = parser.parse_args()
    setLoggingFromOptions(options)
    cluster = clusterFactory(provisioner=options.provisioner,
                             clusterName=options.clusterName,
                             zone=options.zone)
    command = options.args if options.args else ['bash']
    cluster.getLeader().sshAppliance(*command, strict=not options.insecure, tty=sys.stdin.isatty(),
                                     sshOptions=options.sshOptions)
