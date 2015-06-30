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

"""A script to setup and run a hierarchical run of cluster jobs.
"""

import sys
from optparse import OptionParser

from jobTree.leader import mainLoop
from jobTree.common import addOptions, setupJobTree
from jobTree.lib.bioio import setLoggingFromOptions


def main():
    """Restarts a jobTree.
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################  
    
    parser = OptionParser()
    addOptions(parser)
    
    options, args = parser.parse_args()
    
    if len(args) != 0:
        parser.error("Unrecognised input arguments: %s" % " ".join(args))
        
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    assert len(args) <= 1 #Only jobtree may be specified as argument
    if len(args) == 1: #Allow jobTree directory as arg
        options.jobTree = args[0]
        
    ##########################################
    #Now run the job tree construction/leader
    ##########################################  
        
    setLoggingFromOptions(options)
    config, batchSystem, jobStore = setupJobTree(options)
    if "rootJob" not in config.attrib:
        print "There is no root job in the jobTree from which to start, exiting"
        sys.exit(0)
    return mainLoop(config, batchSystem, jobStore, jobStore.load(config.attrib["rootJob"]))
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    main()
