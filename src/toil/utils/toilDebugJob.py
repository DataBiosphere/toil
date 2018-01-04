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

"""Debug tool for running a toil job locally.
"""

from __future__ import absolute_import
import logging

from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import jobStoreLocatorHelp, Config, Toil
from toil.version import version
from toil.worker import workerScript
from toil.utils.toilDebugFile import printContentsOfJobStore

logger = logging.getLogger( __name__ )

def print_successor_jobs():
    pass

def main():
    parser = getBasicOptionParser()

    parser.add_argument("jobStore", type=str,
                        help="The location of the job store used by the workflow." + jobStoreLocatorHelp)
    parser.add_argument("jobID", nargs=1, help="The job store id of a job "
                        "within the provided jobstore to run by itself.")
    parser.add_argument("--printJobInfo", nargs=1,
                        help="Return information about this job to the user"
                        " including preceding jobs, inputs, outputs, and runtime"
                        " from the last known run.")
    parser.add_argument("--version", action='version', version=version)
    
    # Parse options
    options = parseBasicOptions(parser)
    config = Config()
    config.setOptions(options)
    
    # Load the job store
    jobStore = Toil.resumeJobStore(config.jobStore)

    if options.printJobInfo:
        printContentsOfJobStore(jobStorePath=options.jobStore, nameOfJob=options.printJobInfo)

    # TODO: Option to print list of successor jobs
    # TODO: Option to run job within python debugger, allowing step through of arguments
    # idea would be to have option to import pdb and set breakpoint at the start of the user's code

    # Run the job locally
    jobID = options.jobID[0]
    logger.info("Going to run the following job locally: %s", jobID)
    workerScript(jobStore, config, jobID, jobID, redirectOutputToLogFile=False)
    logger.info("Ran the following job locally: %s", jobID)