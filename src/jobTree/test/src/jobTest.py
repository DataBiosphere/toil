#!/usr/bin/env python
"""Test Job class
"""

import unittest
import os
import time
import sys
import random
from optparse import OptionParser

from jobTree.lib.bioio import parseSuiteTestOptions
from jobTree.lib.bioio import system
from jobTree.common import setupJobTree
from jobTree.test import JobTreeTest


class JobTest(JobTreeTest):

    def setUp(self):
        super( JobTest, self ).setUp( )
        self.testJobTree = os.path.join(os.getcwd(), "testJobDir")
        parser = OptionParser()
        Target.addJobTreeOptions(parser)
        options, args = parser.parse_args()
        options.jobTree = self.testJobTree
        self.contextManager = setupJobTree(options)
        config, batchSystem, jobStore, jobTreeState = self.contextManager.__enter__()
        self.jobStore = jobStore
        
    def tearDown(self):
        self.contextManager.__exit__(None, None, None)
        system("rm -rf %s" % self.testJobTree)
        super( JobTest, self ).tearDown( )

    def testJobStoreLoadWriteAndDelete(self):        
        command = "by your command"
        memory = 2^32
        cpu = 1
        tryCount = int(self.jobStore.config.attrib["try_count"])
        
        for i in xrange(10):
            startTime = time.time()
            for j in xrange(100):
                j = self.jobStore.createFirstJob(command, memory, cpu)
                self.assertEquals(j.remainingRetryCount, tryCount)
                self.assertEquals(j.children, [])
                self.assertEquals(j.followOnCommands, [ (command, memory, cpu, 0)])
                self.jobStore.store(j)
                jobStoreID = j.jobStoreID
                j = self.jobStore.load(j.jobStoreID)
                self.assertEquals(j.remainingRetryCount, tryCount)
                self.assertEquals(j.jobStoreID, jobStoreID)
                self.assertEquals(j.children, [])
                self.assertEquals(j.followOnCommands, [ (command, memory, cpu, 0)])
                self.assertTrue(self.jobStore.exists(j.jobStoreID))
                self.jobStore.delete(j)
                self.assertTrue(not self.jobStore.exists(j.jobStoreID))
            print "It took %f seconds to load/unload jobs" % (time.time() - startTime) #We've just used it for benchmarking, so far 
            #Would be good to extend this trivial test
        
    def testJobUpdate(self):
        command = "by your command"
        memory = 2^32
        cpu = 1
        tryCount = int(self.jobStore.config.attrib["try_count"])
        
        for i in xrange(40):
            startTime = time.time()
            j = self.jobStore.createFirstJob(command, memory, cpu)
            childNumber = random.choice(range(20))
            children = map(lambda i : (command, memory, cpu), xrange(childNumber))
            self.jobStore.addChildren(j, children)
            jobStoreID = j.jobStoreID
            j = self.jobStore.load(j.jobStoreID)
            self.assertEquals(len(j.children), childNumber)
            for childJobStoreID, memory, cpu in j.children:
                cJ = self.jobStore.load(childJobStoreID)
                self.assertEquals(cJ.remainingRetryCount, tryCount)
                #self.assertEquals(cJ.jobDir, os.path.split(cJ)[0])
                self.assertEquals(cJ.children, [])
                self.assertEquals(cJ.followOnCommands, [ (command, memory, cpu, 0)])
                self.assertTrue(self.jobStore.exists(cJ.jobStoreID))
                self.jobStore.delete(cJ)
                self.assertTrue(not self.jobStore.exists(cJ.jobStoreID))
            self.assertTrue(self.jobStore.exists(j.jobStoreID))
            self.jobStore.delete(j)
            self.assertTrue(not self.jobStore.exists(j.jobStoreID))
            print "It took %f seconds to update jobs" % (time.time() - startTime) #We've just used it for benchmarking, so far 
            

if __name__ == '__main__':
    unittest.main()
