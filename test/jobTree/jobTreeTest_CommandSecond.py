#!/usr/bin/env python

"""A test program used by jobTreeTest to test the jobTree system works.
"""

import random
import os
import sys
import xml.etree.cElementTree as ET

from sonLib.bioio import logger
from sonLib.bioio import addLoggingFileHandler
from sonLib.bioio import getBasicOptionParser
from sonLib.bioio import parseBasicOptions
from sonLib.bioio import setLogLevel

def main():
    parser = getBasicOptionParser("usage: %prog [options]", "%prog 0.1")
    
    parser.add_option("--job", dest="jobFile", 
                      help="Job file containing command to run",
                      default="None")
    
    parser.add_option("--treePointer", dest="treePointer", 
                      help="File containing pointer to the tree data",
                      default="None")
    
    options, args = parseBasicOptions(parser)
    
    logger.info("Parsed the input arguments")
    
    print >>sys.stderr, "Checking that we can report to std err" #These lines should end up in the logs
    print "Checking that we can report to std out"
    
    job = ET.parse(options.jobFile).getroot() 
    setLogLevel(job.attrib["log_level"])
    
    logger.info("Parsed the job XML")
    
    treePointer = ET.parse(options.treePointer).getroot() 
    
    logger.info("Parsed the tree pointer XML")
    
    tree = ET.parse(treePointer.attrib["file"]).getroot()
    
    logger.info("Parsed the tree XML")
    
    i = 0
    children = tree.find("children").findall("child")
    if len(children) > 0:
        for child in children:
            #Parse the child XML tree
            childTree = ET.parse(child.attrib["file"]).getroot()
            i += int(childTree.attrib["count"])
    else:
        i = 1
    
    tree.attrib["count"] = str(i)
    
    logger.info("Calculated the leaf count: %i" % i)
    
    fileHandle = open(treePointer.attrib["file"], 'w')
    ET.ElementTree(tree).write(fileHandle)
    fileHandle.close()
    
    logger.info("Updated the tree file: %s" % treePointer.attrib["file"])
        
    for childPointer in treePointer.find("children").findall("child"):
        if os.path.isfile(childPointer.attrib["file"]):
            os.remove(childPointer.attrib["file"])
    
    logger.info("Removed the child pointer files")
    
    logger.info("No need to update the job file, as we didn't make anything new!")

    if random.random() > 0.9:
        logger.info("Going to fail the job")
        sys.exit(1)
    logger.info("Going to pass the job done okay")
    sys.exit(0)

def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
