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

from sonLib.bioio import system 
from sonLib.bioio import TestStatus
from sonLib.bioio import parseSuiteTestOptions
from sonLib.bioio import logger
from sonLib.bioio import TempFileTree
from sonLib.bioio import getTempFile

from jobTree.src.common import parasolIsInstalled, gridEngineIsInstalled

from jobTree.test.jobTree.jobTreeTest_CommandFirst import makeTreePointer

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
     
    def testJobTree_SingleMachine(self):
        if not parasolIsInstalled() and not gridEngineIsInstalled():
            testJobTree(self.testNo, self.depth, self.tempFileTree, self.jobTreeDir, "singleMachine")
    
    def testJobTree_Parasol(self):
        if parasolIsInstalled():
            testJobTree(self.testNo, self.depth, self.tempFileTree, self.jobTreeDir, "parasol") 
    
    def testJobTree_gridengine(self):
        if gridEngineIsInstalled():
            testJobTree(self.testNo, self.depth, self.tempFileTree, self.jobTreeDir, "gridengine") 
    
    # only done in singleMachine for now.  Experts can run manually on other systems if they choose
    def dependenciesTest(self, batchSystem="singleMachine"):
        def fn(tree, maxJobs, maxThreads, size, cpusPerJob, sleepTime):
            system("rm -rf %s" % self.jobTreeDir)
            logName = self.tempFileTree.getTempFile(suffix="_comblog.txt", makeDir=False)
            commandLine = "jobTreeTest_Dependencies.py --jobTree %s --logFile %s --batchSystem %s --tree %s --maxJobs %s --maxThreads %s --size %s --cpusPerJob=%s --sleepTime %s" % \
            (self.jobTreeDir, logName, batchSystem, tree, maxJobs, maxThreads, size, cpusPerJob, sleepTime)
            system(commandLine)
        
        fn("comb", 10, 100, 100, 1, 30)
        fn("comb", 200, 100, 100, 20, 30)
       
        fn("fly", 10, 8, 100, 1, 20)
        fn("fly", 10, 8, 100, 2, 20)
        
        fn("balanced", 5, 10, 100, 1, 15)
        fn("balanced", 5, 10, 100, 3, 15)
        
    def testJobTree_dependencies_singleMachine(self):
        self.dependenciesTest(batchSystem="singleMachine")
        
    def testJobTree_dependencies_parasol(self):
        if parasolIsInstalled():
            self.dependenciesTest(batchSystem="parasol")
            
    def testJobTree_dependencies_girdengine(self):
        if gridEngineIsInstalled():
            self.dependenciesTest(batchSystem="gridengine")
        
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
            
            jobTreeCommand = "jobTreeRun --jobTree %s --logInfo" % jobTreeDir
            
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
    
    jobTreeCommand = "jobTreeRun --jobTree %s --retryCount %i\
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
