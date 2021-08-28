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
"""Debug tool for running a toil job locally."""
import logging

from toil.common import Config, Toil, parser_with_common_options
from toil.statsAndLogging import set_logging_from_options
from toil.utils.toilDebugFile import printContentsOfJobStore
from toil.worker import workerScript

logger = logging.getLogger(__name__)


def main() -> None:
    parser = parser_with_common_options(jobstore_option=True)
    parser.add_argument("jobID", nargs=1,
                        help="The job store id of a job within the provided jobstore to run by itself.")
    parser.add_argument("--printJobInfo", nargs=1,
                        help="Return information about this job to the user including preceding jobs, "
                             "inputs, outputs, and runtime from the last known run.")

    options = parser.parse_args()
    set_logging_from_options(options)
    config = Config()
    config.setOptions(options)

    jobStore = Toil.resumeJobStore(config.jobStore)

    if options.printJobInfo:
        printContentsOfJobStore(jobStorePath=config.jobStore, nameOfJob=options.printJobInfo)

    # TODO: Option to print list of successor jobs
    # TODO: Option to run job within python debugger, allowing step through of arguments
    # idea would be to have option to import pdb and set breakpoint at the start of the user's code

    jobID = options.jobID[0]
    logger.debug(f"Running the following job locally: {jobID}")
    workerScript(jobStore, config, jobID, jobID, redirectOutputToLogFile=False)
    logger.debug(f"Finished running: {jobID}")
