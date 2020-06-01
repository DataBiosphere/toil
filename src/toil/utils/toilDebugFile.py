# Copyright (C) 2017-2018 Regents of the University of California
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

"""Debug tool for copying files contained in a toil jobStore.
"""

from __future__ import absolute_import
import logging
import fnmatch
import os.path

from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import Toil, jobStoreLocatorHelp, Config
from toil.version import version

logger = logging.getLogger( __name__ )

def fetchJobStoreFiles(jobStore, options):
    """
    Takes a list of file IDs in options.fetch, and attempts to download them
    all into options.localFilePath.

    :param jobStore: A fileJobStore object.
    :param options.fetch: List of file IDs to find in the jobStore and
                          copy into options.localFilePath.
    :param options.localFilePath: Local directory to copy files into.
    :param options.jobStore: The path to the jobStore directory.
    """
    for jobStoreFileID in options.fetch:
        logger.debug("Copying job store file: %s to %s",
                    jobStoreFileID,
                    options.localFilePath)
        jobStore.readFile(jobStoreFileID,
                          os.path.join(options.localFilePath,
                          os.path.basename(jobStoreFileID)),
                          symlink=options.useSymlinks)


def main():
    parser = getBasicOptionParser()

    parser.add_argument("jobStore",
                        type=str,
                        help="The location of the job store used by the workflow." +
                        jobStoreLocatorHelp)
    parser.add_argument("--localFilePath",
                        type=str,
                        default='.',
                        help="Location to which to copy job store files.")
    parser.add_argument("--fetch",
                        nargs="+",
                        help="List of job-store file IDs to be copied locally.")
    parser.add_argument("--useSymlinks",
                        help="Creates symlink 'shortcuts' of files in the localFilePath"
                        " instead of hardlinking or copying, where possible.  If this is"
                        " not possible, it will copy the files (shutil.copyfile()).")
    parser.add_argument("--version", action='version', version=version)
    
    # Load the jobStore
    options = parseBasicOptions(parser)
    config = Config()
    config.setOptions(options)
    jobStore = Toil.resumeJobStore(config.jobStore)
    logger.debug("Connected to job store: %s", config.jobStore)

    if options.fetch:
        # Copy only the listed files locally
        logger.debug("Fetching local files: %s", options.fetch)
        fetchJobStoreFiles(jobStore=jobStore, options=options)

if __name__=="__main__":
    main()
