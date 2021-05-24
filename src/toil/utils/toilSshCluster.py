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
"""SSH into the toil appliance container running on the leader of the cluster."""
import argparse
import logging
import sys

from toil.common import parser_with_common_options
from toil.provisioners import cluster_factory
from toil.statsAndLogging import set_logging_from_options
from typing import List

logger = logging.getLogger(__name__)


def main() -> None:
    parser = parser_with_common_options(provisioner_options=True, jobstore_option=False)
    parser.add_argument("--insecure", action='store_true',
                        help="Temporarily disable strict host key checking.")
    parser.add_argument("--sshOption", dest='sshOptions', default=[], action='append',
                        help="Pass an additional option to the SSH command.")
    parser.add_argument("--grafana_port", dest='grafana_port', default=3000,
                        help="Assign a local port to be used for the Grafana dashboard.")
    parser.add_argument('args', nargs=argparse.REMAINDER)
    options = parser.parse_args()
    set_logging_from_options(options)

    # Since we collect all the remaining arguments at the end for a command to
    # run, it's easy to lose options.
    if len(options.args) > 0 and options.args[0].startswith('-'):
        logger.warning('Argument \'%s\' interpreted as a command to run '
                       'despite looking like an option.', options.args[0])

    cluster = cluster_factory(provisioner=options.provisioner,
                              clusterName=options.clusterName,
                              zone=options.zone)
    command = options.args if options.args else ['bash']
    sshOptions: List[str] = options.sshOptions

    # Forward ports:
    # 3000 for Grafana dashboard
    # 9090 for Prometheus dashboard
    sshOptions.extend(['-L', f'{options.grafana_port}:localhost:3000',
                       '-L', '9090:localhost:9090'])

    cluster.getLeader().sshAppliance(*command, strict=not options.insecure, tty=sys.stdin.isatty(),
                                     sshOptions=sshOptions)
