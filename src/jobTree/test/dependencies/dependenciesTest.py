#!/usr/bin/env python

import unittest
import os
import os.path
import sys

from jobTree.lib.bioio import system
from jobTree.lib.bioio import parseSuiteTestOptions
from jobTree.lib.bioio import TempFileTree
from jobTree.common import parasolIsInstalled, gridEngineIsInstalled, jobTreePackageDirPath
from jobTree.test import JobTreeTest

@unittest.skip
class DependenciesTest(JobTreeTest):
    """
    Tests jobTree with the single machine batch system.
    """

    def setUp(self):
        super( DependenciesTest, self).setUp()
        self.jobTreeDir = os.path.join(os.getcwd(), "testJobTree") #A directory for the job tree to be created in
        self.tempFileTreeDir = os.path.join(os.getcwd(), "tempFileTree") #Ensures that file tree is visible
        self.tempFileTree = TempFileTree(self.tempFileTreeDir) #A place to get temp files from
    
    def tearDown(self):
        super( DependenciesTest, self).tearDown( )
        self.tempFileTree.destroyTempFiles()
        system("rm -rf %s %s" % (self.jobTreeDir, self.tempFileTreeDir)) #Cleanup the job tree in case it hasn't already been cleaned up.
   
    # only done in singleMachine for now.  Experts can run manually on other systems if they choose
    def dependenciesTest(self, batchSystem="singleMachine", furtherOptionsString=""):
        def fn(tree, maxCpus, maxThreads, size, cpusPerJob, sleepTime):
            system("rm -rf %s" % self.jobTreeDir)
            logName = self.tempFileTree.getTempFile(suffix="_comblog.txt", makeDir=False)
            commandLine = "%s/dependencies.py " \
                          "--jobTree %s " \
                          "--logFile %s " \
                          "--batchSystem '%s' " \
                          "--tree %s " \
                          "--maxCpus %s " \
                          "--maxThreads %s " \
                          "--size %s " \
                          "--cpusPerJob=%s " \
                          "--sleepTime %s " \
                          "%s" % \
            (os.path.join(jobTreePackageDirPath(), "test", "dependencies"), self.jobTreeDir, logName,
             batchSystem, tree, maxCpus, maxThreads, size, cpusPerJob, sleepTime, furtherOptionsString)
            system(commandLine)
        
        fn("comb", 10, 100, 100, 1, 10)
        fn("comb", 200, 100, 100, 20, 10)
       
        fn("fly", 10, 8, 100, 1, 10)
        fn("fly", 10, 8, 100, 2, 10)
        
        fn("balanced", 5, 10, 100, 1, 10)
        fn("balanced", 5, 10, 100, 3, 10)
        
    def testJobTree_dependencies_singleMachine(self):
        self.dependenciesTest(batchSystem="singleMachine")
        
    def testJobTree_dependencies_combined(self):
        self.dependenciesTest(batchSystem="singleMachine",
                              furtherOptionsString="--bigBatchSystem singleMachine --bigMemoryThreshold 1000000")
        
    def testJobTree_dependencies_parasol(self):
        if parasolIsInstalled():
            self.dependenciesTest(batchSystem="parasol")
            
    def testJobTree_dependencies_gridengine(self):
        if gridEngineIsInstalled():
            self.dependenciesTest(batchSystem="gridengine")


if __name__ == '__main__':
    unittest.main()
