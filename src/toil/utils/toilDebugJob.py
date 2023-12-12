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

import pprint
import sys

from toil.common import Config, Toil, parser_with_common_options
from toil.jobStores.fileJobStore import FileJobStore
from toil.statsAndLogging import set_logging_from_options
from toil.utils.toilDebugFile import printContentsOfJobStore
from toil.worker import workerScript

logger = logging.getLogger(__name__)


def main() -> None:
    parser = parser_with_common_options(jobstore_option=True, prog="toil debug-job")
    parser.add_argument("jobID", type=str, nargs='?', default=None,
                        help="The job store id of a job within the provided jobstore to run by itself.")
    parser.add_argument("--printJobInfo", type=str,
                        help="Dump debugging info about this job ID")

    options = parser.parse_args()
    set_logging_from_options(options)

    jobStore = Toil.resumeJobStore(options.jobStore)
    # Get the config with the workflow ID from the job store
    config = jobStore.config
    # But override its options
    config.setOptions(options)

    did_something = False

    if options.printJobInfo:
        if isinstance(jobStore, FileJobStore):
            # List all its files if we can
            printContentsOfJobStore(job_store=jobStore, job_id=options.printJobInfo)
        # Print the job description itself
        job_desc = jobStore.load_job(options.printJobInfo)
        print(f"Job: {job_desc}")
        pprint.pprint(job_desc.__dict__)

        did_something = True

    # TODO: Option to print list of successor jobs
    # TODO: Option to run job within python debugger, allowing step through of arguments
    # idea would be to have option to import pdb and set breakpoint at the start of the user's code

    if options.jobID is not None:
        # We actually want to run a job.

        jobID = options.jobID
        logger.debug(f"Running the following job locally: {jobID}")
        workerScript(jobStore, config, jobID, jobID, redirectOutputToLogFile=False)
        logger.debug(f"Finished running: {jobID}")
        # Even if the job fails, the worker script succeeds unless something goes wrong with it internally.

        did_something = True

    if not did_something:
        # Somebody forgot to tell us to do anything.
        # Show the usage instructions.
        parser.print_help()
        sys.exit(1)
