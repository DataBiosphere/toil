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

"""Kills any running jobs trees in a rogue jobtree.
"""
import logging
import sys

from jobTree.lib.bioio import getBasicOptionParser
from jobTree.lib.bioio import parseBasicOptions
from jobTree.src.common import loadJobStore, loadBatchSystem

logger = logging.getLogger( __name__ )


def main():
    parser = getBasicOptionParser("usage: %prog [--jobTree] JOB_TREE_DIR [more options]", "%prog 0.1")
    
    parser.add_option("--jobTree", dest="jobTree", 
                      help="Job store path. Can also be specified as the single argument to the script.")
    
    options, args = parseBasicOptions(parser)
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    assert len(args) <= 1 #Only jobtree may be specified as argument
    if len(args) == 1: #Allow jobTree directory as arg
        options.jobTree = args[0]
        
    logger.info("Parsed arguments")
    assert options.jobTree != None #The jobtree should not be null
    jobStore = loadJobStore(options.jobTree)
    
    logger.info("Starting routine to kill running jobs in the jobTree: %s" % options.jobTree)
    ####This behaviour is now broken
    batchSystem = loadBatchSystem(jobStore.config) #This should automatically kill the existing jobs.. so we're good.
    for jobID in batchSystem.getIssuedJobIDs(): #Just in case we do it again.
        batchSystem.killJobs(jobID)
    logger.info("All jobs SHOULD have been killed")

def _test():
    import doctest
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
