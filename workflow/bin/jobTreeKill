#!/usr/bin/env python

"""Kills any running jobs trees in a rogue jobtree.
"""

import os

import xml.etree.ElementTree as ET
from workflow.jobTree.lib.bioio import logger
from workflow.jobTree.lib.bioio import getBasicOptionParser
from workflow.jobTree.lib.bioio import parseBasicOptions

from workflow.jobTree.bin.jobTree import loadTheBatchSystem

def main():
    parser = getBasicOptionParser("usage: %prog [options]", "%prog 0.1")
    
    parser.add_option("--jobTree", dest="jobTree", 
                      help="Directory containing the job tree to kill")
    
    options, args = parseBasicOptions(parser)
    logger.info("Parsed arguments")
    assert len(args) == 0 #This program takes no arguments
    assert options.jobTree != None #The jobtree should not be null
    assert os.path.isdir(options.jobTree) #The job tree must exist if we are going to kill it.
    logger.info("Starting routine to kill running jobs in the jobTree: %s" % options.jobTree)
    config = ET.parse(os.path.join(options.jobTree, "config.xml")).getroot()
    batchSystem = loadTheBatchSystem(config) #This should automatically kill the existing jobs.. so we're good.
    for job in batchSystem.getIssuedJobIDs(): #Just in case we do it again.
        batchSystem.killJobs(job)
    logger.info("All jobs SHOULD have been killed")

def _test():
    import doctest
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
