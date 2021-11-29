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
"""Kills rogue toil processes."""
import logging
import os
import signal

from toil.common import Config, Toil, parser_with_common_options
from toil.statsAndLogging import set_logging_from_options
from toil.jobStores.fileJobStore import FileJobStore

logger = logging.getLogger(__name__)


def main() -> None:
    parser = parser_with_common_options()
    options = parser.parse_args()
    set_logging_from_options(options)
    config = Config()
    config.setOptions(options)

    job_store_type, _ = Toil.parseLocator(config.jobStore)

    if job_store_type != 'file':
        # Remote (aws/google) jobstore; use the old (broken?) method
        job_store = Toil.resumeJobStore(config.jobStore)
        logger.info("Starting routine to kill running jobs in the toil workflow: %s", config.jobStore)
        # TODO: This behaviour is now broken: https://github.com/DataBiosphere/toil/commit/a3d65fc8925712221e4cda116d1825d4a1e963a1
        # There's no guarantee that the batch system in use can enumerate
        # running jobs belonging to the job store we've attached to. And
        # moreover we don't even bother trying to kill the leader at its
        # recorded PID, even if it is a local process.
        batch_system = Toil.createBatchSystem(job_store.config)  # Should automatically kill existing jobs, so we're good.
        for job_id in batch_system.getIssuedBatchJobIDs():  # Just in case we do it again.
            batch_system.killBatchJobs([job_id])
        logger.info("All jobs SHOULD have been killed")
    else:
        # otherwise, kill the pid recorded in the jobstore.
        # TODO: We assume thnis is a local PID.
        job_store = Toil.resumeJobStore(config.jobStore)
        assert isinstance(job_store, FileJobStore), "Need a FileJobStore which has a sharedFilesDir"
        pid_log = os.path.join(job_store.sharedFilesDir, 'pid.log')
        with open(pid_log) as f:
            pid_to_kill = f.read().strip()
        try:
            os.kill(int(pid_to_kill), signal.SIGTERM)
            logger.info("Toil process %s successfully terminated." % str(pid_to_kill))
        except OSError:
            logger.error("Toil process %s could not be terminated." % str(pid_to_kill))
            raise
