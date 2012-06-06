#!/usr/bin/env python
"""Tests jobTree with the single machine batch system.
"""

import unittest
import os
import os.path
import sys
import random
import subprocess
import xml.etree.cElementTree as ET

from sonLib.bioio import system 
from sonLib.bioio import TestStatus
from sonLib.bioio import parseSuiteTestOptions
from sonLib.bioio import logger
from sonLib.bioio import TempFileTree
from sonLib.bioio import getTempFile

from jobTree.src.common import parasolIsInstalled, gridEngineIsInstalled

class TestCase(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.jobTreeDir = os.path.join(os.getcwd(), "jobTree") #A directory for the job tree to be created in
        self.tempFileTreeDir = os.path.join(os.getcwd(), "tempFileTree") #Ensures that file tree is visible
        self.tempFileTree = TempFileTree(self.tempFileTreeDir) #A place to get temp files from
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.tempFileTree.destroyTempFiles()
        system("rm -rf %s %s" % (self.jobTreeDir, self.tempFileTreeDir)) #Cleanup the job tree in case it hasn't already been cleaned up.
   
    # only done in singleMachine for now.  Experts can run manually on other systems if they choose
    def dependenciesTest(self, batchSystem="singleMachine"):
        def fn(tree, maxJobs, maxThreads, size, cpusPerJob, sleepTime):
            system("rm -rf %s" % self.jobTreeDir)
            logName = self.tempFileTree.getTempFile(suffix="_comblog.txt", makeDir=False)
            commandLine = "jobTreeTest_Dependencies.py --jobTree %s --logFile %s --batchSystem '%s' --tree %s --maxJobs %s --maxThreads %s --size %s --cpusPerJob=%s --sleepTime %s" % \
            (self.jobTreeDir, logName, batchSystem, tree, maxJobs, maxThreads, size, cpusPerJob, sleepTime)
            system(commandLine)
        
        fn("comb", 10, 100, 100, 1, 5)
        fn("comb", 200, 100, 100, 20, 5)
       
        fn("fly", 10, 8, 100, 1, 3)
        fn("fly", 10, 8, 100, 2, 3)
        
        fn("balanced", 5, 10, 100, 1, 2)
        fn("balanced", 5, 10, 100, 3, 2)
        
    def testJobTree_dependencies_singleMachine(self):
        self.dependenciesTest(batchSystem="singleMachine")
        
    def testJobTree_dependencies_combined(self):
        self.dependenciesTest(batchSystem="singleMachine singleMachine 1000000")
        
    def testJobTree_dependencies_parasol(self):
        return
        if parasolIsInstalled():
            self.dependenciesTest(batchSystem="parasol")
            
    def testJobTree_dependencies_gridengine(self):
        return
        if gridEngineIsInstalled():
            self.dependenciesTest(batchSystem="gridengine")

def checkEndStateOfJobTree(jobTreeDir):
    """Checks the state of the jobtree after successful exit by the job tree master.
    Return True, if there are no remaining job files.
    """
    #jobFiles = os.listdir(os.path.join(jobTreeDir, "jobs"))
    jobFiles = TempFileTree(os.path.join(jobTreeDir, "jobs")).listFiles()
    if len(jobFiles) > 0:
        for absFileName in jobFiles:
            assert os.path.isdir(absFileName)
            for jobFile in os.listdir(absFileName):
                if "job.xml" == jobFile[:7]:
                    job = ET.parse(os.path.join(absFileName, jobFile)).getroot()
                    assert job.attrib["colour"] in ("red", "blue")
                    if job.attrib["colour"] == "red":
                        assert job.attrib["remaining_retry_count"] == "0"
        return False
    else:
        return True

def runJobTreeStatusAndFailIfNotComplete(jobTreeDir):
    command = "jobTreeStatus --jobTree %s --failIfNotComplete --verbose" % jobTreeDir
    system(command)

def main():
    parseSuiteTestOptions()
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()
