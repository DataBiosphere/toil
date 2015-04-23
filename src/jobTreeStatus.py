#!/usr/bin/env python

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

"""Reports the state of your given job tree.
"""

import sys
import os

import xml.etree.cElementTree as ET
from sonLib.bioio import logger
from sonLib.bioio import logFile 

from sonLib.bioio import getBasicOptionParser
from sonLib.bioio import parseBasicOptions

from jobTree.src.master import getConfigFileName
from jobTree.src.job import Job, JobDB

def main():
    """Reports the state of the job tree.
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################  
    
    parser = getBasicOptionParser("usage: %prog [--jobTree] JOB_TREE_DIR [options]", "%prog 0.1")
    
    parser.add_option("--jobTree", dest="jobTree", 
                      help="Directory containing the job tree. The jobTree location can \
                      also be specified as the argument to the script. default=%default", default='./jobTree')
    
    parser.add_option("--verbose", dest="verbose", action="store_true",
                      help="Print loads of information, particularly all the log files of \
                      jobs that failed. default=%default",
                      default=False)
    
    parser.add_option("--failIfNotComplete", dest="failIfNotComplete", action="store_true",
                      help="Return exit value of 1 if job tree jobs not all completed. default=%default",
                      default=False)
    
    options, args = parseBasicOptions(parser)
    logger.info("Parsed arguments")
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    assert len(args) <= 1 #Only jobtree may be specified as argument
    if len(args) == 1: #Allow jobTree directory as arg
        options.jobTree = args[0]
    
    ##########################################
    #Do some checks.
    ##########################################
    
    logger.info("Checking if we have files for job tree")
    assert options.jobTree != None
    assert os.path.isdir(options.jobTree) #The given job dir tree must exist.
    assert os.path.isfile(getConfigFileName(options.jobTree)) #A valid job tree must contain the config gile
    
    ##########################################
    #Survey the status of the job and report.
    ##########################################  
    
    config = ET.parse(getConfigFileName(options.jobTree)).getroot()
    jobDB = JobDB(config)
    jobDB.loadJobTreeState() #This initialises the object jobTree.jobTreeState used to track the active jobTree
    
    failedJobs = [ job for job in jobDB.jobTreeState.updatedJobs | \
                  set(jobDB.jobTreeState.childCounts.keys()) \
                  if job.remainingRetryCount == 0 ]
    
    print "There are %i active jobs, %i parent jobs with children, \
    %i totally failed jobs and %i empty jobs (i.e. finished but not cleaned up) \
    currently in job tree: %s" % \
    (len(jobDB.jobTreeState.updatedJobs), len(jobDB.jobTreeState.childCounts), 
     len(failedJobs), len(jobDB.jobTreeState.shellJobs), options.jobTree)
    
    if options.verbose: #Verbose currently means outputting the files that have failed.
        for job in failedJobs:
            if os.path.isfile(jobDB.getJobLogFileName(job.jobStoreID)):
                print "Log file of failed job: %s" % jobDB.getJobLogFileName(job.jobStoreID)
                logFile(jobDB.getJobLogFileName(job.jobStoreID), logger.critical)
            else:
                print "Log file for job %s is not present" % job.jobStoreID 
        if len(failedJobs) == 0:
            print "There are no failed jobs to report"   
    
    if (len(jobDB.jobTreeState.updatedJobs) + len(jobDB.jobTreeState.childCounts)) != 0 and \
        options.failIfNotComplete:
        sys.exit(1)
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
