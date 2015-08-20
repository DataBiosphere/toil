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

"""Kills any running jobs trees in a rogue toil.
"""
from __future__ import absolute_import
import logging
import sys

from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import loadJobStore, loadBatchSystem

logger = logging.getLogger( __name__ )

def main():
    parser = getBasicOptionParser("usage: %prog [--jobStore] JOB_TREE_DIR [more options]", "%prog 0.1")
    
    parser.add_option("--jobStore", dest="jobStore",
                      help="Job store path. Can also be specified as the single argument to the script.")
    
    options, args = parseBasicOptions(parser)
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    assert len(args) <= 1 #Only toil may be specified as argument
    if len(args) == 1: #Allow toil directory as arg
        options.jobStore = args[0]
        
    logger.info("Parsed arguments")
    if options.jobStore == None:
        parser.error("Specify --jobStore")

    jobStore = loadJobStore(options.jobStore)
    
    logger.info("Starting routine to kill running jobs in the toil workflow: %s" % options.jobStore)
    ####This behaviour is now broken
    batchSystem = loadBatchSystem(jobStore.config) #This should automatically kill the existing jobs.. so we're good.
    for jobID in batchSystem.getIssuedBatchJobIDs(): #Just in case we do it again.
        batchSystem.killBatchJobs(jobID)
    logger.info("All jobs SHOULD have been killed")

def _test():
    import doctest
    return doctest.testmod()
