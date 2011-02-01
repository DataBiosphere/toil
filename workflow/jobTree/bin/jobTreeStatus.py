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

import xml.etree.ElementTree as ET
from workflow.jobTree.lib.bioio import logger
from workflow.jobTree.lib.bioio import logFile 

from workflow.jobTree.lib.bioio import getBasicOptionParser
from workflow.jobTree.lib.bioio import parseBasicOptions
from workflow.jobTree.lib.bioio import TempFileTree

def parseJobFile(absFileName):
    try:
        job = ET.parse(absFileName).getroot()
        return job
    except IOError:
        logger.info("Encountered error while parsing job file %s, so we will ignore it" % absFileName)
    return None

def main():
    """Reports the state of the job tree.
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################  
    
    parser = getBasicOptionParser("usage: %prog [options] \nThe colours returned indicate the state of the job.\n\
\twhite: job has not been started yet\n\
\tgrey: job is issued to batch system\n\
\tred: job failed\n\
\tblue: job has children currently being processed\n\
\tblack: job has finished and will be processed (transient state)\n\
\tdead: job is totally finished and is awaiting deletion (transient state)", "%prog 0.1")
    
    parser.add_option("--jobTree", dest="jobTree", 
                      help="Directory containing the job tree")
    
    parser.add_option("--verbose", dest="verbose", action="store_true",
                      help="Print loads of information, particularly all the log files of errors",
                      default=False)
    
    parser.add_option("--failIfNotComplete", dest="failIfNotComplete", action="store_true",
                      help="Return exit value of 1 if job tree jobs not all completed",
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
    assert os.path.isfile(os.path.join(options.jobTree, "config.xml")) #A valid job tree must contain the config gile
    assert os.path.isdir(os.path.join(options.jobTree, "jobs")) #A job tree must have a directory of jobs.
    assert os.path.isdir(os.path.join(options.jobTree, "tempDirDir")) #A job tree must have a directory of temporary directories (for jobs to make temp files in).
    assert os.path.isdir(os.path.join(options.jobTree, "logFileDir")) #A job tree must have a directory of log files.
    assert os.path.isdir(os.path.join(options.jobTree, "slaveLogFileDir")) #A job tree must have a directory of slave log files.
    
    ##########################################
    #Read the total job number
    ##########################################  
    
    config = ET.parse(os.path.join(options.jobTree, "config.xml")).getroot()
    
    ##########################################
    #Survey the status of the job and report.
    ##########################################  
    
    colours = {}
    jobFiles = TempFileTree(config.attrib["job_file_dir"]).listFiles()
    if len(jobFiles) > 0:
        logger.info("Collating the colours of the job tree")
        for absFileName in jobFiles:
            job = parseJobFile(absFileName)
            if job != None:
                if not colours.has_key(job.attrib["colour"]):
                    colours[job.attrib["colour"]] = 0
                colours[job.attrib["colour"]] += 1
    else:
        logger.info("There are no jobs to collate")
    
    print "There are %i jobs currently in job tree: %s" % \
    (len(jobFiles), options.jobTree)
    
    for colour in colours.keys():
        print "\tColour: %s, number of jobs: %s" % (colour, colours[colour])
    
    if options.verbose: #Verbose currently means outputting the files that have failed.
        for absFileName in jobFiles:
            job = parseJobFile(absFileName)
            if job != None:
                if job.attrib["colour"] == "red":
                    if os.path.isfile(job.attrib["log_file"]):
                        def fn(string):
                            print string
                        logFile(job.attrib["log_file"], fn)
                    else:
                        logger.info("Log file for job %s is not present" % job.attrib["file"])
    
    if len(jobFiles) != 0 and options.failIfNotComplete:
        sys.exit(1)
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
