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

"""Debug tool for running toil jobs locally.
"""

from __future__ import absolute_import
import logging

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
    parser.add_argument("jobID", nargs=1, help="The job store id of the job to run locally")
    
    # Parse options
    options = parseBasicOptions(parser)
    config = Config()
    config.setOptions(options)
    
    # TODO: Options to print job store file ids of files created by job
    # TODO: Option to print list of successor jobs
    # TODO: Option to run job within python debugger, allowing step through of arguments

    # Run the job locally
    logger.info("Going to run the following job locally: %s", options.jobID)
    statusCode = toil_worker.workerScript(jobName, config.jobStore, options.jobID)
    