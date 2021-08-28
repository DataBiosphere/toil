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
import logging
import os.path

from toil.common import Config, Toil, parser_with_common_options
from toil.jobStores.abstractJobStore import AbstractJobStore
from toil.statsAndLogging import set_logging_from_options
from toil.lib.resources import glob
from toil.lib.expando import Expando
from typing import Optional

logger = logging.getLogger(__name__)


def fetchJobStoreFiles(jobStore: AbstractJobStore, options: Expando) -> None:
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
    for jobStoreFile in options.fetch:
        jobStoreHits = glob(directoryname=options.jobStore,
                            glob_pattern=jobStoreFile)
        for jobStoreFileID in jobStoreHits:
            logger.debug(f"Copying job store file: {jobStoreFileID} to {options.localFilePath[0]}")
            jobStore.readFile(jobStoreFileID,
                              os.path.join(options.localFilePath[0],
                                           os.path.basename(jobStoreFileID)),
                              symlink=options.useSymlinks)


def printContentsOfJobStore(jobStorePath: str, nameOfJob: Optional[str] = None) -> None:
    """
    Fetch a list of all files contained in the jobStore directory input if
    nameOfJob is not declared, otherwise it only prints out the names of files
    for that specific job for which it can find a match.  Also creates a logFile
    containing this same record of job files in the working directory.

    :param jobStorePath: Directory path to recursively look for files.
    :param nameOfJob: Default is None, which prints out all files in the jobStore.
    If specified, it will print all jobStore files that have been written to the
    jobStore by that job.
    """

    if nameOfJob:
        glob_pattern = "*" + nameOfJob + "*"
        logFile = nameOfJob + "_fileset.txt"
    else:
        glob_pattern = "*"
        logFile = "jobstore_files.txt"
        nameOfJob = ""

    list_of_files = glob(directoryname=jobStorePath, glob_pattern=glob_pattern)
    if os.path.exists(logFile):
        os.remove(logFile)
    for gfile in sorted(list_of_files):
        if not gfile.endswith('.new'):
            logger.debug(f"{nameOfJob} File: {os.path.basename(gfile)}")
            with open(logFile, "a+") as f:
                f.write(os.path.basename(gfile))
                f.write("\n")


def main() -> None:
    parser = parser_with_common_options(jobstore_option=True)
    parser.add_argument("--localFilePath",
                        nargs=1,
                        help="Location to which to copy job store files.")
    parser.add_argument("--fetch",
                        nargs="+",
                        help="List of job-store files to be copied locally."
                             "Use either explicit names (i.e. 'data.txt'), or "
                             "specify glob patterns (i.e. '*.txt')")
    parser.add_argument("--listFilesInJobStore",
                        help="Prints a list of the current files in the jobStore.")
    parser.add_argument("--fetchEntireJobStore",
                        help="Copy all job store files into a local directory.")
    parser.add_argument("--useSymlinks",
                        help="Creates symlink 'shortcuts' of files in the localFilePath"
                             " instead of hardlinking or copying, where possible.  If this is"
                             " not possible, it will copy the files (shutil.copyfile()).")

    # Load the jobStore
    options = parser.parse_args()
    set_logging_from_options(options)
    config = Config()
    config.setOptions(options)
    jobStore = Toil.resumeJobStore(config.jobStore)
    logger.debug("Connected to job store: %s", config.jobStore)

    if options.fetch:
        # Copy only the listed files locally
        logger.debug("Fetching local files: %s", options.fetch)
        fetchJobStoreFiles(jobStore=jobStore, options=options)

    elif options.fetchEntireJobStore:
        # Copy all jobStore files locally
        logger.debug("Fetching all local files.")
        options.fetch = "*"
        fetchJobStoreFiles(jobStore=jobStore, options=options)

    if options.listFilesInJobStore:
        # Log filenames and create a file containing these names in cwd
        printContentsOfJobStore(jobStorePath=options.jobStore)


if __name__ == "__main__":
    main()
