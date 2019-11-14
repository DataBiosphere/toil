# Copyright (C) 2015-2018 Regents of the University of California
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
from __future__ import absolute_import
import logging
import os
import signal

from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import Toil, jobStoreLocatorHelp, Config
from toil.version import version

logger = logging.getLogger( __name__ )

def main():
    parser = getBasicOptionParser()

    parser.add_argument("jobStore", type=str,
                        help="The location of the job store used by the workflow whose jobs should "
                             "be killed." + jobStoreLocatorHelp)
    parser.add_argument("--version", action='version', version=version)
    options = parseBasicOptions(parser)
    config = Config()
    config.setOptions(options)
    config.jobStore = config.jobStore[5:] if config.jobStore.startswith('file:') else config.jobStore

    # ':' means an aws/google jobstore; use the old (broken?) method
    if ':' in config.jobStore:
        jobStore = Toil.resumeJobStore(config.jobStore)
        logger.info("Starting routine to kill running jobs in the toil workflow: %s", config.jobStore)
        # TODO: This behaviour is now broken src: https://github.com/DataBiosphere/toil/commit/a3d65fc8925712221e4cda116d1825d4a1e963a1
        batchSystem = Toil.createBatchSystem(jobStore.config)  # Should automatically kill existing jobs, so we're good.
        for jobID in batchSystem.getIssuedBatchJobIDs():  # Just in case we do it again.
            batchSystem.killBatchJobs(jobID)
        logger.info("All jobs SHOULD have been killed")
    # otherwise, kill the pid recorded in the jobstore
    else:
        pid_log = os.path.join(os.path.abspath(config.jobStore), 'pid.log')
        with open(pid_log, 'r') as f:
            pid2kill = f.read().strip()
        try:
            os.kill(int(pid2kill), signal.SIGKILL)
            logger.info("Toil process %s successfully terminated." % str(pid2kill))
        except OSError:
            logger.error("Toil process %s could not be terminated." % str(pid2kill))
            raise
