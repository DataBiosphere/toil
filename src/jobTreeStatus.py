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
from sonLib.bioio import TempFileTree

from jobTree.src.master import getEnvironmentFileName, getJobFileDirName, getParasolResultsFileName, getConfigFileName
from jobTree.src.master import readJob
from jobTree.src.job import Job
from jobTree.src.jobTreeSlave import loadStack

def parseJobFile(absFileName):
    try:
        job = readJob(absFileName)
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
                      help="Print loads of information, particularly all the log files of errors. default=%default",
                      default=False)
    
    parser.add_option("--graph", dest="graphFile", default=None,
                      help="Prints info on the current job tree graph in the given file, in dot format.")
    
    parser.add_option("--leaves", dest="leaves", action="store_true",
                      help="Prints leaves of the tree in the graph file. default=%default",
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
    
    jobFiles = [ os.path.join(jobDir, "job.xml") for jobDir in TempFileTree(getJobFileDirName(options.jobTree)).listFiles() ]
    jobFiles = [ (job, jobFile) for (job, jobFile) in zip([  parseJobFile(absFileName) for absFileName in jobFiles ], jobFiles) if job != None ]
    colours = {}
    
    if len(jobFiles) > 0:
        logger.info("Collating the colours of the job tree")
        for job, jobFile, in jobFiles:
            if not colours.has_key(job.getColour()):
                colours[job.getColour()] = 0
            colours[job.getColour()] += 1
    else:
        logger.info("There are no jobs to collate")
    
    print "There are %i jobs currently in job tree: %s" % \
    (len(jobFiles), options.jobTree)
    
    for colour in colours.keys():
        print "\tColour: %s, number of jobs: %s" % (Job.translateColourToString(colour), colours[colour])
    
    if options.verbose: #Verbose currently means outputting the files that have failed.
        for job, jobFile in jobFiles:
            if job.getColour() == Job.red:
                if os.path.isfile(job.getLogFileName()):
                    def fn(string):
                        print string
                    logFile(job.getLogFileName(), fn)
                else:
                    logger.info("Log file for job %s is not present" % job.getJobFileName())
                    
    i = 0            
    if options.graphFile != None:
        fileHandle = open(options.graphFile, 'w')
        fileHandle.write("graph G {\n")
        fileHandle.write("overlap=false\n")
        fileHandle.write("node[];\n")
        nodeNames = {} #Hash of node names to nodes
        if not options.leaves:
            jobFiles = [ (job, jobFile) for (job, jobFile) in jobFiles if job.getColour() != Job.grey ]
        for job, jobFile in jobFiles:
            colour = job.getColour()
            command = "None"
            if job.getNumberOfFollowOnCommandsToIssue() > 0:
                command = job.getNextFollowOnCommandToIssue()[0]
                if command[:10] == "scriptTree":
                    try:
                        stack = loadStack(command)
                        target = stack.stack[-1]
                        command = str(target.__class__)
                    except IOError:
                        command = "Gone"
            fileHandle.write("n%sn [label=\"%s %s\"];\n" % (i, colour, command))
            nodeNames[jobFile] = i
            i = i+1
        fileHandle.write("edge[dir=forward];\n")
        for job, jobFile in jobFiles:
            nodeName = nodeNames[jobFile]
            if job.getParentJobFile() != None:
                parentNodeName = nodeNames[job.getParentJobFile()]
                fileHandle.write("n%sn -- n%sn;\n" % (parentNodeName, nodeName))
        fileHandle.write("}\n")
        fileHandle.close()
    
    if len(jobFiles) != 0 and options.failIfNotComplete:
        sys.exit(1)
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
