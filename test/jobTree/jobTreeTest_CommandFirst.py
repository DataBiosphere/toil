#!/usr/bin/env python

"""A test program used by jobTreeTest to test the jobTree system works.
"""

import sys
import random
import xml.etree.ElementTree as ET
from jobTree.src.bioio import logger
from jobTree.src.bioio import addLoggingFileHandler

from jobTree.src.bioio import getBasicOptionParser
from jobTree.src.bioio import parseBasicOptions
from jobTree.src.bioio import getTempFile
from jobTree.src.bioio import setLogLevel

def makeTreePointer(treeNode, tempFile):
    tree = ET.Element("tree_pointer")
    tree.attrib["file"] = treeNode
    ET.SubElement(tree, "children")
    fileHandle = open(tempFile, 'w')
    ET.ElementTree(tree).write(fileHandle)
    fileHandle.close()
    return tempFile

def main():
    parser = getBasicOptionParser("usage: %prog [options]", "%prog 0.1")
    
    parser.add_option("--job", dest="jobFile", 
                      help="Job file containing command to run",
                      default="None")
    
    parser.add_option("--treePointer", dest="treePointerFile", 
                      help="File containing pointer to the tree data",
                      default="None")
    
    options, args = parseBasicOptions(parser)
    
    logger.info("Parsed the input arguments")
    
    job = ET.parse(options.jobFile).getroot() 
    setLogLevel(job.attrib["log_level"])
    
    logger.info("Parsed the job XML")
    
    treePointer = ET.parse(options.treePointerFile).getroot() 
    
    logger.info("Parsed the tree pointer XML")
    
    tree = ET.parse(treePointer.attrib["file"]).getroot()
    
    logger.info("Parsed the tree XML")
    
    for child in tree.find("children").findall("child"):
        #Make the chuld tree pointer
        childTreePointerFile = makeTreePointer(child.attrib["file"], getTempFile(rootDir=job.attrib["global_temp_dir"]))
        #Make the child command
        unbornChild = ET.SubElement(job.find("children"), "child")
        command = "jobTreeTest_CommandFirst.py --treePointer %s --job JOB_FILE" % \
        (childTreePointerFile,)
        unbornChild.attrib["command"] = command
        if random.random() > 0.2:
            unbornChild.attrib["time"] = str(random.random() * 10)
        #Make the child tree pointer
        ET.SubElement(treePointer.find("children"), "child", { "file":childTreePointerFile })
    
    job.attrib["command"] = "jobTreeTest_CommandSecond.py --treePointer %s --job JOB_FILE" % \
    (options.treePointerFile,)
    logger.info("Made new command")

    fileHandle = open(options.jobFile, 'w')
    ET.ElementTree(job).write(fileHandle)
    fileHandle.close()
    
    logger.info("Updated the job file")
    
    print >>sys.stderr, "Checking that we can report to std err" #These lines should end up in the logs
    print "Checking that we can report to std out"

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
