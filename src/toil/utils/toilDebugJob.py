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
import gc
import logging
import os
import pprint
import sys
from pathlib import Path
from typing import Optional

from toil.common import Toil, parser_with_common_options
from toil.job import FilesDownloadedStoppingPointReached
from toil.jobStores.fileJobStore import FileJobStore
from toil.statsAndLogging import set_logging_from_options
from toil.utils.toilDebugFile import printContentsOfJobStore
from toil.utils.toilStatus import ToilStatus
from toil.worker import workerScript

logger = logging.getLogger(__name__)


def main() -> None:
    parser = parser_with_common_options(
        jobstore_option=True, prog="toil debug-job", default_log_level=logging.DEBUG
    )
    parser.add_argument(
        "job",
        type=str,
        help="The job store id or job name of a job within the provided jobstore",
    )
    parser.add_argument(
        "--printJobInfo",
        action="store_true",
        help="Dump debugging info about the job instead of running it",
    )
    parser.add_argument(
        "--retrieveTaskDirectory",
        dest="retrieve_task_directory",
        type=str,
        default=None,
        help="Download CWL or WDL task inputs to the given directory and stop.",
    )

    options = parser.parse_args()
    set_logging_from_options(options)

    if options.retrieve_task_directory is not None and os.path.exists(
        options.retrieve_task_directory
    ):
        # The logic to duplicate container mounts depends on stuff not already existing.
        logger.error(
            "The directory %s given for --retrieveTaskDirectory already exists. "
            "Stopping to avoid clobbering existing files.",
            options.retrieve_task_directory,
        )
        sys.exit(1)

    jobStore = Toil.resumeJobStore(options.jobStore)
    # Get the config with the workflow ID from the job store
    config = jobStore.config
    # But override its options
    config.setOptions(options)
    config.cleanWorkDir = "never"

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
                logger.critical(
                    'No job found with ID or name "%s". No jobs are completely failed.',
                    options.job,
                )
            else:
                logger.critical(
                    'No job found with ID or name "%s". How about the failed job %s instead?',
                    options.job,
                    suggestion,
                )
            sys.exit(1)
        elif len(hits) > 1:
            # Several hits, maybe only one has failed
            completely_failed_hits = [job for job in hits if job.remainingTryCount == 0]
            if len(completely_failed_hits) == 0:
                logger.critical(
                    'Multiple jobs match "%s" but none are completely failed: %s',
                    options.job,
                    hits,
                )
                sys.exit(1)
            elif len(completely_failed_hits) > 0:
                logger.critical(
                    'Multiple jobs matching "%s" are completely failed: %s',
                    options.job,
                    completely_failed_hits,
                )
                sys.exit(1)
            else:
                # We found one completely failed job, they probably mean that one.
                logger.info(
                    'There are %s jobs matching "%s"; assuming you mean the failed one: %s',
                    options.job,
                    completely_failed_hits[0],
                )
                job_id = completely_failed_hits[0].jobStoreID
        else:
            # We found one job with this name, so they must mean that one
            logger.info('Looked up job named "%s": %s', options.job, hits[0])
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

        debug_flags = set()
        local_worker_temp_dir = None
        if options.retrieve_task_directory is not None:
            # Pick a directory in it (which may be removed by the worker) as the worker's temp dir.
            local_worker_temp_dir = os.path.join(
                options.retrieve_task_directory, "worker"
            )
            # Make sure it exists
            os.makedirs(local_worker_temp_dir, exist_ok=True)
            # And tell the job to just download files
            debug_flags.add("download_only")
        # We might need to reconstruct a container environment.
        host_and_job_paths: Optional[list[tuple[str, str]]] = None
        # Track if the run succeeded without error
        run_succeeded = False

        logger.info(f"Running the following job locally: {job_id}")
        try:
            workerScript(
                jobStore,
                config,
                job_id,
                job_id,
                redirect_output_to_log_file=False,
                local_worker_temp_dir=local_worker_temp_dir,
                debug_flags=debug_flags,
            )
        except FilesDownloadedStoppingPointReached as e:
            # We asked for the files to be downloaded and now they are.
            assert options.retrieve_task_directory is not None
            if e.host_and_job_paths is not None:
                # Capture the container mapping so we can reconstruct the container environment after we unwind the worker stack.
                host_and_job_paths = e.host_and_job_paths
        else:
            # No error!
            run_succeeded = True

        # Make sure the deferred function manager cleans up and logs its
        # shutdown before we start writing any reports.
        gc.collect()

        if run_succeeded:
            logger.info(f"Successfully ran: {job_id}")

        if host_and_job_paths is not None:
            # We need to make a place that looks like the job paths half of these.

            # Sort by job-side path so we do children before parents, to
            # stop us from accidentally making children inside moutned
            # parents.
            sorted_mounts = sorted(host_and_job_paths, key=lambda t: t[1], reverse=True)

            fake_job_root = os.path.join(options.retrieve_task_directory, "inside")
            os.makedirs(fake_job_root, exist_ok=True)

            for host_path, job_path in sorted_mounts:
                if not os.path.exists(host_path):
                    logger.error(
                        "Job intended to mount %s as %s but it does not exist!",
                        host_path,
                        job_path,
                    )
                    continue
                if not job_path.startswith("/"):
                    logger.error(
                        "Job intended to mount %s as %s but destination is a relative path!",
                        host_path,
                        job_path,
                    )
                    continue
                # Drop the slash because we are building a chroot-ish mini filesystem.
                job_relative_path = job_path[1:]
                if job_relative_path.startswith("/"):
                    # We are having trouble understanding what the job
                    # intended to do. Stop working on this mount.
                    logger.error(
                        "Job intended to mount %s as %s but destination starts with multiple slashes for some reason!",
                        host_path,
                        job_path,
                    )
                    continue
                fake_job_path = os.path.join(fake_job_root, job_relative_path)
                if os.path.exists(fake_job_path):
                    logger.error(
                        "Job intended to mount %s as %s but that location is already mounted!",
                        host_path,
                        job_path,
                    )
                    continue

                logger.info("Job mounted %s as %s", host_path, job_path)

                # Make sure the directory to contain the mount exists.
                fake_job_containing_path = os.path.dirname(fake_job_path)
                os.makedirs(fake_job_containing_path, exist_ok=True)

                top_pathobj = Path(os.path.abspath(options.retrieve_task_directory))
                source_pathobj = Path(host_path)
                if top_pathobj in source_pathobj.parents:
                    # We're linking to a file we already downloaded (probably).
                    # Make a relative symlink so the whole assemblage can move.
                    host_path = os.path.relpath(host_path, fake_job_containing_path)

                # Make a symlink to simulate the mount
                os.symlink(host_path, fake_job_path)

            logger.info("Reconstructed job container filesystem at %s", fake_job_root)

    # TODO: Option to print list of successor jobs
    # TODO: Option to run job within python debugger, allowing step through of arguments
    # idea would be to have option to import pdb and set breakpoint at the start of the user's code
