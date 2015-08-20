#Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.

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

logger = logging.getLogger( __name__ )

def main():
    """Reports the state of the toil.
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################  
    
    parser = getBasicOptionParser("usage: %prog [--jobStore] JOB_TREE_DIR [options]", "%prog 0.1")
    
    parser.add_option("--jobStore", dest="jobStore",
                      help="Job store path. Can also be specified as the single argument to the script.\
                       default=%default", default=os.path.abspath("./toil"))
    
    parser.add_option("--verbose", dest="verbose", action="store_true",
                      help="Print loads of information, particularly all the log files of \
                      jobs that failed. default=%default",
                      default=False)
    
    parser.add_option("--failIfNotComplete", dest="failIfNotComplete", action="store_true",
                      help="Return exit value of 1 if toil jobs not all completed. default=%default",
                      default=False)
    
    options, args = parseBasicOptions(parser)
    logger.info("Parsed arguments")
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    assert len(args) <= 1 #Only toil may be specified as argument
    if len(args) == 1: #Allow toil directory as arg
        options.jobStore = args[0]
    
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
