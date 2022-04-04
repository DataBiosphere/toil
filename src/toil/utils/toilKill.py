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

from toil.common import Config, Toil, parser_with_common_options
from toil.jobStores.abstractJobStore import NoSuchJobStoreException
from toil.statsAndLogging import set_logging_from_options

logger = logging.getLogger(__name__)


def main() -> None:
    parser = parser_with_common_options()
    options = parser.parse_args()
    set_logging_from_options(options)
    config = Config()
    config.setOptions(options)

    # Kill the pid recorded in the job store.
    try:
        job_store = Toil.resumeJobStore(config.jobStore)
    except NoSuchJobStoreException:
        logger.error("The job store %s does not exist.", config.jobStore)
        return

    with job_store.read_shared_file_stream("pid.log") as f:
        pid_to_kill = int(f.read().strip())

    # TODO: We assume this is a local PID so we can't kill the leader if the
    #  process is on a different machine. We also rely on the leader to be
    #  responsive to clean up all its jobs.

    try:
        os.kill(pid_to_kill, signal.SIGTERM)
        logger.info("Toil process %i successfully terminated.", pid_to_kill)
    except OSError:
        logger.error("Toil process %i could not be terminated.", pid_to_kill)
        raise
