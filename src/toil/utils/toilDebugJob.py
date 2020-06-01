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

from toil.lib.bioio import parseBasicOptions, getBasicOptionParser

from toil.common import jobStoreLocatorHelp, Config, Toil, addCoreOptions
from toil.version import version
from toil.worker import workerScript
from toil.resource import Resource, setRunningOnWorker

logger = logging.getLogger( __name__ )

def print_successor_jobs():
    pass

def main():
    parser = getBasicOptionParser()
    
    # Set up options
    config = Config()
    # We need just the options required to talk to the job store and configure
    # the worker, but nothing about batch systems
    addCoreOptions(parser, config)
    
    
    parser.add_argument("jobID", nargs=1, help="The job store id of a job "
                        "within the provided jobstore to run by itself.")
    parser.add_argument("--version", action='version', version=version)
    
    # Parse options and set up logging
    options = parseBasicOptions(parser)
    config.setOptions(options)
    
    # Load the job store
    jobStore = Toil.resumeJobStore(config.jobStore)

    # TODO: Option to print list of successor jobs
    # TODO: Option to run job within python debugger, allowing step through of arguments
    # idea would be to have option to import pdb and set breakpoint at the start of the user's code

    # Run the job locally
    jobID = options.jobID[0]
    logger.debug("Going to run the following job locally: %s", jobID)
    # Tell the resource system we are a worker, whatever it might think.
    setRunningOnWorker()
    # Set up for resource download
    # TODO: Don't we need to get the environment from the job and set it up first?
    Resource.prepareSystem()
    workerScript(jobStore, config, jobID, jobID, redirectOutputToLogFile=False)
    logger.debug("Ran the following job locally: %s", jobID)
