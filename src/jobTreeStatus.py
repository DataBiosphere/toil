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

from jobTree.src.master import getJobFileDirName, getConfigFileName, listChildDirs
from jobTree.src.job import Job, getJobFileName

def parseJobFile(absFileName):
    try:
        job = Job.read(absFileName)
        return job
    except IOError:
        logger.info("Encountered error while parsing job file %s, so we will ignore it" % absFileName)
    return None

def _parseJobFiles(jobTreeJobsRoot, updatedJobFiles, childJobFileToParentJob, childCounts, shellJobs):
    #Read job
    job = parseJobFile(getJobFileName(jobTreeJobsRoot))
    #Get children
    childJobs = reduce(lambda x,y:x+y, [ parseJobFiles(childDir, updatedJobFiles, childJobFileToParentJob, childCounts, shellJobs) for childDir in listChildDirs(jobTreeJobsRoot) ], [])
    if len(childJobs) > 0:
        childCounts[job] = len(childJobs)
        for childJob in childJobs:
            childJobFileToParentJob[childJob.getJobFileName()] = job
    elif len(job.followOnCommands) > 0:
        updatedJobFiles.add(job)
    else: #Job is stub with nothing left to do, so ignore
        shellJobs.add(job)
        return []
    return [ job ]

def parseJobFiles(jobTreeJobsRoot, updatedJobFiles, childJobFileToParentJob, childCounts, shellJobs):
    jobFile = getJobFileName(jobTreeJobsRoot)
    if os.path.exists(jobFile):
        return _parseJobFiles(jobTreeJobsRoot, updatedJobFiles, childJobFileToParentJob, childCounts, shellJobs)
    return reduce(lambda x,y:x+y, [ parseJobFiles(childDir, updatedJobFiles, childJobFileToParentJob, childCounts, shellJobs) for childDir in listChildDirs(jobTreeJobsRoot) ], [])    

def main():
    """Reports the state of the job tree.
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################  
    
    parser = getBasicOptionParser("usage: %prog [options]", "%prog 0.1")
    
    parser.add_option("--jobTree", dest="jobTree", 
                      help="Directory containing the job tree")
    
    parser.add_option("--verbose", dest="verbose", action="store_true",
                      help="Print loads of information, particularly all the log files of errors. default=%default",
                      default=False)
    
    parser.add_option("--failIfNotComplete", dest="failIfNotComplete", action="store_true",
                      help="Return exit value of 1 if job tree jobs not all completed. default=%default",
                      default=False)
    
    options, args = parseBasicOptions(parser)
    logger.info("Parsed arguments")
    assert len(args) == 0
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    ##########################################
    #Do some checks.
    ##########################################
    
    logger.info("Checking if we have files for job tree")
    assert options.jobTree != None
    assert os.path.isdir(options.jobTree) #The given job dir tree must exist.
    assert os.path.isfile(getConfigFileName(options.jobTree)) #A valid job tree must contain the config gile
    assert os.path.isdir(getJobFileDirName(options.jobTree)) #A job tree must have a directory of jobs.
    
    ##########################################
    #Survey the status of the job and report.
    ##########################################  
    
    childJobFileToParentJob, childCounts, updatedJobFiles, shellJobs = {}, {}, set(), set()
    parseJobFiles(getJobFileDirName(options.jobTree), updatedJobFiles, childJobFileToParentJob, childCounts, shellJobs)
    
    failedJobs = [ job for job in updatedJobFiles | set(childCounts.keys()) if job.remainingRetryCount == 0 ]
           
    print "There are %i active jobs, %i parent jobs with children, %i totally failed jobs and %i empty jobs (i.e. finished by not cleaned up) currently in job tree: %s" % \
    (len(updatedJobFiles), len(childCounts), len(failedJobs), len(shellJobs), options.jobTree)
    
    if options.verbose: #Verbose currently means outputting the files that have failed.
        for job in failedJobs:
            if os.path.isfile(job.getLogFileName()):
                print "Log file of failed job: %s" % job.getJobFileName()
            else:
                print "Log file for job %s is not present" % job.getJobFileName()    
    
    if (len(updatedJobFiles) + len(childCounts)) != 0 and options.failIfNotComplete:
        sys.exit(1)
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
