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
"""Debug tool for copying files contained in a toil jobStore."""
import argparse
import logging
import os.path
import sys
from typing import Optional

from toil.common import Config, Toil, parser_with_common_options
from toil.jobStores.fileJobStore import FileJobStore
from toil.lib.conversions import strtobool
from toil.lib.resources import glob
from toil.statsAndLogging import set_logging_from_options

logger = logging.getLogger(__name__)


def fetchJobStoreFiles(jobStore: FileJobStore, options: argparse.Namespace) -> None:
    """
    Takes a list of file names as glob patterns, searches for these within a
    given directory, and attempts to take all of the files found and copy them
    into options.localFilePath.

    :param jobStore: A fileJobStore object.
    :param options.fetch: List of file glob patterns to search
        for in the jobStore and copy into options.localFilePath.
    :param options.localFilePath: Local directory to copy files into.
    :param options.jobStore: The path to the jobStore directory.
    """

    # TODO: Implement the necessary methods in the job store class and stop
    # globbing around inside it. Does this even work?

    for jobStoreFile in options.fetch:
        jobStoreHits = glob(directoryname=options.jobStore, glob_pattern=jobStoreFile)
        for jobStoreFileID in jobStoreHits:
            logger.debug(
                f"Copying job store file: {jobStoreFileID} to {options.localFilePath[0]}"
            )
            jobStore.read_file(
                jobStoreFileID,
                os.path.join(
                    options.localFilePath[0], os.path.basename(jobStoreFileID)
                ),
                symlink=options.useSymlinks,
            )


def printContentsOfJobStore(
    job_store: FileJobStore, job_id: Optional[str] = None
) -> None:
    """
    Fetch a list of all files contained in the job store if nameOfJob is not
    declared, otherwise it only prints out the names of files for that specific
    job for which it can find a match.  Also creates a log file of these file
    names in the current directory.

    :param job_store: Job store to ask for files from.
    :param job_id: Default is None, which prints out all files in the jobStore.
        If specified, it will print all jobStore files that have been written
        to the jobStore by that job.
    """

    # TODO: Implement the necessary methods for job stores other than
    # FileJobStore.

    if job_id:
        logFile = job_id.replace("/", "_") + "_fileset.txt"
    else:
        logFile = "jobstore_files.txt"

    list_of_files = job_store.list_all_file_names(for_job=job_id)
    if os.path.exists(logFile):
        os.remove(logFile)
    for gfile in sorted(list_of_files):
        if job_id:
            logger.debug(f"{job_id} File: {os.path.basename(gfile)}")
        else:
            logger.debug(f"File: {os.path.basename(gfile)}")
        with open(logFile, "a+") as f:
            f.write(os.path.basename(gfile))
            f.write("\n")


def main() -> None:
    parser = parser_with_common_options(jobstore_option=True, prog="toil debug-file")
    parser.add_argument(
        "--localFilePath", nargs=1, help="Location to which to copy job store files."
    )
    parser.add_argument(
        "--fetch",
        nargs="+",
        help="List of job-store files to be copied locally."
        "Use either explicit names (i.e. 'data.txt'), or "
        "specify glob patterns (i.e. '*.txt')",
    )
    parser.add_argument(
        "--listFilesInJobStore",
        type=strtobool,
        help="Prints a list of the current files in the jobStore.",
    )
    parser.add_argument(
        "--fetchEntireJobStore",
        type=strtobool,
        help="Copy all job store files into a local directory.",
    )
    parser.add_argument(
        "--useSymlinks",
        type=strtobool,
        help="Creates symlink 'shortcuts' of files in the localFilePath"
        " instead of hardlinking or copying, where possible.  If this is"
        " not possible, it will copy the files (shutil.copyfile()).",
    )

    # Load the jobStore
    options = parser.parse_args()
    set_logging_from_options(options)
    config = Config()
    config.setOptions(options)
    jobStore = Toil.resumeJobStore(config.jobStore)
    logger.debug("Connected to job store: %s", config.jobStore)

    if options.fetch:
        # Copy only the listed files locally

        if isinstance(jobStore, FileJobStore):
            logger.debug("Fetching local files: %s", options.fetch)
            fetchJobStoreFiles(jobStore=jobStore, options=options)
        else:
            # The user asked for something we can't do yet.
            # Tell them no but don't stack trace.
            logger.critical("Can only fetch by name or glob from file-based job stores")
            sys.exit(1)

    elif options.fetchEntireJobStore:
        # Copy all jobStore files locally

        if isinstance(jobStore, FileJobStore):
            logger.debug("Fetching all local files.")
            options.fetch = "*"
            fetchJobStoreFiles(jobStore=jobStore, options=options)
        else:
            logger.critical("Can only fetch by name or glob from file-based job stores")
            sys.exit(1)

    if options.listFilesInJobStore:
        # Log filenames and create a file containing these names in cwd

        if isinstance(jobStore, FileJobStore):
            printContentsOfJobStore(job_store=jobStore)
        else:
            logger.critical("Can only list files from file-based job stores")
            sys.exit(1)

    # TODO: We can't actually do *anything* for non-file job stores.


if __name__ == "__main__":
    main()
