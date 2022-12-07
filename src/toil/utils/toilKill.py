# Copyright (C) 2015-2022 Regents of the University of California
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
"""Kills rogue toil processes."""
import logging
import os
import signal
import sys

from toil.common import Config, Toil, getNodeID, parser_with_common_options
from toil.jobStores.abstractJobStore import NoSuchJobStoreException
from toil.statsAndLogging import set_logging_from_options

logger = logging.getLogger(__name__)


def main() -> None:
    parser = parser_with_common_options()
    parser.add_argument('--force', action='store_true',
                        help="Send SIGKILL to the leader process if local.")
    options = parser.parse_args()
    set_logging_from_options(options)
    config = Config()
    config.setOptions(options)

    # Get the job store
    try:
        job_store = Toil.resumeJobStore(config.jobStore)
    except NoSuchJobStoreException:
        logger.error("The job store %s does not exist.", config.jobStore)
        return

    # NOTE: the kill will not work if the leader is still initializing at this
    #  point. Changes to the kill flag will be ignored until the leader sets the
    #  kill flag.

    # Get the leader PID
    pid_to_kill = job_store.read_leader_pid()

    # Check if the leader is on the same machine
    leader_node_id = job_store.read_leader_node_id()
    local_leader = leader_node_id == getNodeID()

    if local_leader:
        # Check if we can send signals to the leader. If not, process might be
        # in another container so we fall back to using the kill flag through
        # the job store.
        try:
            os.kill(pid_to_kill, 0)
        except OSError:
            local_leader = False

    if local_leader:
        try:
            os.kill(pid_to_kill, signal.SIGKILL if options.force else signal.SIGTERM)
            logger.info("Toil process %i successfully terminated.", pid_to_kill)
        except OSError:
            logger.error("Could not signal process %i. Is it still running?", pid_to_kill)
            sys.exit(1)
    else:
        # Flip the flag inside the job store to signal kill
        job_store.write_kill_flag(kill=True)
        logger.info("Asked the leader to terminate.")
