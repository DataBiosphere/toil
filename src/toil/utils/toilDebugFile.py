# Copyright (C) 2017- Regents of the University of California
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
import os.path

from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import Toil, jobStoreLocatorHelp, Config
from toil.version import version

logger = logging.getLogger( __name__ )

def main():
    parser = getBasicOptionParser()

    parser.add_argument("jobStore", type=str,
                        help="The location of the job store used by the workflow." + jobStoreLocatorHelp)
    parser.add_argument("jobStoreFileIDs", nargs='+', help="List of N job-store file ids to be copied locally")
    parser.add_argument("localFilePath", nargs=1, help="Location to which to copy job store files")
    parser.add_argument("--version", action='version', version=version)
    
    # Load the jobStore
    options = parseBasicOptions(parser)
    config = Config()
    config.setOptions(options)
    jobStore = Toil.resumeJobStore(config.jobStore)
    logger.info("Connected to job store: %s", config.jobStore)
    
    # TODO: Option to symlink files from job store, where possible

    # Copy the files locally
    for jobStoreFileID in options.jobStoreFileIDs:
        localFileID = os.path.join(jobStoreFileID, options.localFilePath)
        logger.info("Copying job store file: %s to %s", jobStoreFileID, localFileID)
        jobStore.readGlobalFile(jobStoreFileID, localFileID)
    