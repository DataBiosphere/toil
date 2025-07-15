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
import socket
import sys

from toil.common import parser_with_common_options
from toil.provisioners import NoSuchClusterException, cluster_factory
from toil.statsAndLogging import set_logging_from_options

logger = logging.getLogger(__name__)

def have_ipv6() -> bool:
    """
    Return True if the IPv6 loopback interface is useable.
    """

    # We sniff for actual IPv6 like in
    # https://github.com/urllib3/urllib3/pull/840/files in urllib3 (which we
    # don't depend on)
    if socket.has_ipv6:
        # Built with IPv6 support
        try:
            socket.socket(socket.AF_INET6).bind(("::1", 0))
            return True
        except Exception:
            pass
    return False

def main() -> None:
    parser = parser_with_common_options(
        provisioner_options=True, jobstore_option=False, prog="toil ssh-cluster"
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Temporarily disable strict host key checking.",
    )
    parser.add_argument(
        "--sshOption",
        dest="sshOptions",
        default=[],
        action="append",
        help="Pass an additional option to the SSH command.",
    )
    parser.add_argument(
        "--grafana_port",
        dest="grafana_port",
        default=3000,
        help="Assign a local port to be used for the Grafana dashboard.",
    )
    parser.add_argument("args", nargs=argparse.REMAINDER)
    options = parser.parse_args()
    set_logging_from_options(options)

    # Since we collect all the remaining arguments at the end for a command to
    # run, it's easy to lose options.
    if len(options.args) > 0 and options.args[0].startswith("-"):
        logger.warning(
            "Argument '%s' interpreted as a command to run "
            "despite looking like an option.",
            options.args[0],
        )

    cluster = cluster_factory(
        provisioner=options.provisioner,
        clusterName=options.clusterName,
        zone=options.zone,
    )
    command = options.args if options.args else ["bash"]
    sshOptions: list[str] = options.sshOptions

    # Forward ports:
    # 3000 for Grafana dashboard
    # 9090 for Prometheus dashboard
    sshOptions.extend(
        ["-L", f"{options.grafana_port}:localhost:3000", "-L", "9090:localhost:9090"]
    )

    if not have_ipv6():
        # If we try to do SSH port forwarding without any other options, but
        # IPv6 is turned off on the host, we might get complaints that we
        # "Cannot assign requested address" on ports on [::1].
        sshOptions.append("-4")

    try:
        cluster.getLeader().sshAppliance(
            *command,
            strict=not options.insecure,
            tty=sys.stdin.isatty(),
            sshOptions=sshOptions,
        )
    except NoSuchClusterException as e:
        logger.error(e)
        sys.exit(1)
