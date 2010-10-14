#!/usr/bin/env python
"""Tests jobTree with the single machine batch system.
"""

import unittest
import os
import os.path
import sys
import random
import subprocess
import xml.etree.ElementTree as ET

from workflow.jobTree.lib.bioio import system 
from workflow.jobTree.lib.bioio import TestStatus
from workflow.jobTree.lib.bioio import parseSuiteTestOptions
from workflow.jobTree.lib.bioio import logger
from workflow.jobTree.lib.bioio import TempFileTree

from workflow.jobTree.test.jobTree.jobTreeTest_CommandFirst import makeTreePointer

class TestCase(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.testNo = TestStatus.getTestSetup(1, 1, 2, 2)
        self.depth = TestStatus.getTestSetup(1, 2, 3, 5)
        self.jobTreeDir = os.path.join(os.getcwd(), "jobTree") #A directory for the job tree to be created in
        self.tempFileTreeDir = os.path.join(os.getcwd(), "tempFileTree") #Ensures that file tree is visible
        self.tempFileTree = TempFileTree(self.tempFileTreeDir) #A place to get temp files from
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.tempFileTree.destroyTempFiles()
        system("rm -rf %s %s" % (self.jobTreeDir, self.tempFileTreeDir)) #Cleanup the job tree in case it hasn't already been cleaned up.
     
    #def testJobTree_SingleMachine(self):
        #testJobTree(self.testNo, self.depth, self.tempFileTree, self.jobTreeDir, "single_machine")    
    
    #def testJobTree_Parasol(self):
        #if os.system("parasol status") == 0:
            #testJobTree(self.testNo, self.depth, self.tempFileTree, self.jobTreeDir, "parasol") 
    def testJobTree_gridengine(self):
        testJobTree(self.testNo, self.depth, self.tempFileTree, self.jobTreeDir, "gridengine") 

def testJobTree(testNo, depth, tempFileTree, jobTreeDir, batchSystem):
    """Runs a test program using the job tree using the single machine batch system.
    """
    for test in xrange(testNo):
        jobTreeCommand, fileTreeRootFile = setupJobTree(tempFileTree, jobTreeDir, 
                                                        batchSystem, depth=depth)
        #Run the job
        while True:
            print "job tree command", jobTreeCommand
            
            process = subprocess.Popen(jobTreeCommand, shell=True)
            sts = os.waitpid(process.pid, 0)
            assert sts[1] == 0
            logger.info("The job tree master ended, with an okay exit value")
        
            if checkEndStateOfJobTree(jobTreeDir): #Check the state of the job files, exit if none
                break
            
            jobTreeCommand = "jobTree --jobTree %s --logInfo" % jobTreeDir
            
        checkFileTreeCounts(fileTreeRootFile)
        os.system("rm -rf %s" % jobTreeDir)
        logger.info("Test done okay")
    
def setupJobTree(tempFileTree, jobTreeDir, batchSystem, depth=2):
    """Sets up a job tree using the jobTreeSetup.py command.
    """
    #Setup a job
    retryCount = random.choice(xrange(1,10))
    
    logger.info("Setup the basic files for the test")
    
    fileTreeRootFile = tempFileTree.getTempFile()
    makeFileTree(fileTreeRootFile, depth, tempFileTree)
    treePointerFile = makeTreePointer(fileTreeRootFile, tempFileTree.getTempFile())
    
    #Setup the job
    command = "jobTreeTest_CommandFirst.py --treePointer %s --job JOB_FILE" % \
    (treePointerFile)
    
    jobTreeCommand = "jobTree --jobTree %s --retryCount %i\
     --command '%s' --logLevel=INFO --maxJobDuration 100 --batchSystem %s" % \
    (jobTreeDir, retryCount, command, batchSystem)
        
    logger.info("Setup the job okay")
    return (jobTreeCommand, fileTreeRootFile)

def checkEndStateOfJobTree(jobTreeDir):
    """Checks the state of the jobtree after successful exit by the job tree master.
    Return True, if there are no remaining job files.
    """
    #jobFiles = os.listdir(os.path.join(jobTreeDir, "jobs"))
    jobFiles = TempFileTree(os.path.join(jobTreeDir, "jobs")).listFiles()
    if len(jobFiles) > 0:
        for absFileName in jobFiles:
            job = ET.parse(absFileName).getroot()
            assert job.attrib["colour"] in ("red", "blue")
            if job.attrib["colour"] == "red":
                assert job.attrib["remaining_retry_count"] == "0"
        return False
    else:
        return True
    
def makeFileTree(rootFile, remainingDepth, tempFileTree):
    """Makes a random tree of linked files.
    """  
    tree = ET.Element("tree")
    tree.attrib["count"] = "0"
    children = ET.SubElement(tree, "children")
    if remainingDepth > 0:
        for i in xrange(random.choice(xrange(1, 10))):
            childFile = tempFileTree.getTempFile()
            ET.SubElement(children, "child", { "file":childFile })
            makeFileTree(childFile, remainingDepth-1, tempFileTree)
    fileHandle = open(rootFile, 'w')
    ET.ElementTree(tree).write(fileHandle)
    fileHandle.close()

def checkFileTreeCounts(rootFile):
    """Check the file tree produced by the test.
    """
    tree = ET.parse(rootFile).getroot()
    i = 0
    children = tree.find("children").findall("child")
    if len(children) == 0:
        i = 1
    else:
        for child in children:
            i += checkFileTreeCounts(child.attrib["file"])
    logger.info("File tree counts: %i %i" % (i, int(tree.attrib["count"])))
    assert i == int(tree.attrib["count"])
    return i

def runJobTreeStatusAndFailIfNotComplete(jobTreeDir):
    command = "jobTreeStatus --jobTree %s --failIfNotComplete --verbose" % jobTreeDir
    system(command)

def main():
    parseSuiteTestOptions()
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()
