# Copyright (C) 2015 UCSC Computational Genomics Lab
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

"""Reports the state of your given toil.
"""
from __future__ import absolute_import
import logging
import os
import sys

from toil.lib.bioio import logStream
from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import loadJobStore
from toil.leader import ToilState
from toil.job import Job, JobException
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
              help=("Store in which to place job management files \
              and the global accessed temporary files"
              "(If this is a file path this needs to be globally accessible "
              "by all machines running jobs).\n"
              "If the store already exists and restart is false an"
              " ExistingJobStoreException exception will be thrown."))
    
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
    
    logger.info("Checking if we have files for toil")
    assert options.jobStore != None
    
    ##########################################
    #Survey the status of the job and report.
    ##########################################  
    
    jobStore = loadJobStore(options.jobStore)
    try:
        rootJob = Job._loadRootJob(jobStore)
    except JobException:
        print "The root job of the jobStore is not present, the toil workflow has probably completed okay"
        sys.exit(0)
    
    toilState = ToilState(jobStore, rootJob )
    
    failedJobs = [ job for job in toilState.updatedJobs | \
                  set(toilState.successorCounts.keys()) \
                  if job.remainingRetryCount == 0 ]
    
    print "There are %i active jobs, %i parent jobs with children, and \
    %i totally failed jobs currently in toil workflow: %s" % \
    (len(toilState.updatedJobs), len(toilState.successorCounts),
     len(failedJobs), options.jobStore)
    
    if options.verbose: #Verbose currently means outputting the files that have failed.
        for job in failedJobs:
            if job.logJobStoreFileID is not None:
                with job.getLogFileHandle(jobStore) as logFileHandle:
                    logStream(logFileHandle, job.jobStoreID, logger.warn)
            else:
                print "Log file for job %s is not present" % job.jobStoreID
        if len(failedJobs) == 0:
            print "There are no failed jobs to report"   
    
    if (len(toilState.updatedJobs) + len(toilState.successorCounts)) != 0 and \
        options.failIfNotComplete:
        sys.exit(1)
    
def _test():
    import doctest      
    return doctest.testmod()
