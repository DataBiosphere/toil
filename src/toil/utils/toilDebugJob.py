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
from toil.utils.toilStatus import ToilStatus
from toil.worker import workerScript

logger = logging.getLogger(__name__)


def main() -> None:
    parser = parser_with_common_options(jobstore_option=True, prog="toil debug-job")
    parser.add_argument("job", type=str,
                        help="The job store id or job name of a job within the provided jobstore")
    parser.add_argument("--printJobInfo", action="store_true",
                        help="Dump debugging info about the job instead of runnign it")

    options = parser.parse_args()
    set_logging_from_options(options)

    jobStore = Toil.resumeJobStore(options.jobStore)
    # Get the config with the workflow ID from the job store
    config = jobStore.config
    # But override its options
    config.setOptions(options)

    # Find the job

    if jobStore.job_exists(options.job):
        # The user asked for a particular job and it exists
        job_id = options.job
    else:
        # Go search by name and fill in job_id

        # TODO: break out job store scan logic so it doesn't need to re-connect
        # to the job store.
        status = ToilStatus(options.jobStore)
        hits = []
        suggestion = None
        for job in status.jobsToReport:
            if options.job in (job.jobName, job.unitName, job.displayName):
                # Find all the jobs that sort of match
                hits.append(job)
            if suggestion is None and job.remainingTryCount == 0:
                # How about this nice failing job instead?
                suggestion = job
        if len(hits) == 0:
            # No hits
            if suggestion is None:
                logger.critical("No job found with ID or name \"%s\". No jobs are completely failed.", options.job)
            else:
                logger.critical("No job found with ID or name \"%s\". How about the failed job %s instead?", options.job, suggestion)
            sys.exit(1)
        elif len(hits) > 1:
            # Several hits, maybe only one has failed
            completely_failed_hits = [job for job in hits if job.remainingTryCount == 0]
            if len(completely_failed_hits) == 0:
                logger.critical("Multiple jobs match \"%s\" but none are completely failed: %s", options.job, hits)
                sys.exit(1)
            elif len(completely_failed_hits) > 0:
                logger.critical("Multiple jobs matching \"%s\" are completely failed: %s", options.job, completely_failed_hits)
                sys.exit(1)
            else:
                # We found one completely failed job, they probably mean that one.
                logger.info("There are %s jobs matching \"%s\"; assuming you mean the failed one: %s", options.job, completely_failed_hits[0])
                job_id = completely_failed_hits[0].jobStoreID
        else:
            # We found one job with this name, so they must mean that one
            logger.info("Looked up job named \"%s\": %s", options.job, hits[0])
            job_id = hits[0].jobStoreID

    if options.printJobInfo:
        # Report on the job

        if isinstance(jobStore, FileJobStore):
            # List all its files if we can
            printContentsOfJobStore(job_store=jobStore, job_id=job_id)
        # Print the job description itself
        job_desc = jobStore.load_job(job_id)
        print(f"Job: {job_desc}")
        pprint.pprint(job_desc.__dict__)
    else:
        # Run the job
        logger.debug(f"Running the following job locally: {job_id}")
        workerScript(jobStore, config, job_id, job_id, redirect_output_to_log_file=False)
        logger.debug(f"Finished running: {job_id}")

    # TODO: Option to print list of successor jobs
    # TODO: Option to run job within python debugger, allowing step through of arguments
    # idea would be to have option to import pdb and set breakpoint at the start of the user's code
