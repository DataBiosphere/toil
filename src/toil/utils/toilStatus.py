# Copyright (C) 2015-2016 Regents of the University of California
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

"""
Reports the state of a Toil workflow
"""
from __future__ import absolute_import
from __future__ import print_function

import logging
import sys

from toil.lib.bioio import logStream
from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import Toil, jobStoreLocatorHelp, Config
from toil.leader import ToilState
from toil.job import JobException
from toil.version import version

logger = logging.getLogger( __name__ )

def main():
    """Reports the state of the toil.
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################  
    
    parser = getBasicOptionParser()
    
    parser.add_argument("jobStore", type=str,
                        help="The location of a job store that holds the information about the "
                             "workflow whose status is to be reported on." + jobStoreLocatorHelp)
    
    parser.add_argument("--verbose", dest="verbose", action="store_true",
                      help="Print loads of information, particularly all the log files of \
                      jobs that failed. default=%(default)s",
                      default=False)
    
    parser.add_argument("--failIfNotComplete", dest="failIfNotComplete", action="store_true",
                      help="Return exit value of 1 if toil jobs not all completed. default=%(default)s",
                      default=False)
    parser.add_argument("--version", action='version', version=version)
    options = parseBasicOptions(parser)
    logger.info("Parsed arguments")
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    ##########################################
    #Do some checks.
    ##########################################
    
    logger.info("Checking if we have files for Toil")
    assert options.jobStore is not None
    config = Config()
    config.setOptions(options)
    ##########################################
    #Survey the status of the job and report.
    ##########################################  
    
    jobStore = Toil.resumeJobStore(config.jobStore)
    try:
        rootJob = jobStore.loadRootJob()
    except JobException:
        print('The root job of the job store is absent, the workflow completed successfully.',
              file=sys.stderr)
        sys.exit(0)
    
    toilState = ToilState(jobStore, rootJob )

    # The first element of the toilState.updatedJobs tuple is the jobWrapper we want to inspect
    totalJobs = set(toilState.successorCounts.keys()) | \
                {jobTuple[0] for jobTuple in toilState.updatedJobs}

    failedJobs = [ job for job in totalJobs if job.remainingRetryCount == 0 ]

    print('There are %i active jobs, %i parent jobs with children, and %i totally failed jobs '
          'currently in %s.' % (len(toilState.updatedJobs), len(toilState.successorCounts),
                                len(failedJobs), config.jobStore), file=sys.stderr)
    
    if options.verbose: #Verbose currently means outputting the files that have failed.
        for job in failedJobs:
            if job.logJobStoreFileID is not None:
                with job.getLogFileHandle(jobStore) as logFileHandle:
                    logStream(logFileHandle, job.jobStoreID, logger.warn)
            else:
                print('Log file for job %s is absent.' % job.jobStoreID, file=sys.stderr)
        if len(failedJobs) == 0:
            print('There are no failed jobs to report.', file=sys.stderr)
    
    if (len(toilState.updatedJobs) + len(toilState.successorCounts)) != 0 and \
        options.failIfNotComplete:
        sys.exit(1)
    
def _test():
    import doctest      
    return doctest.testmod()
