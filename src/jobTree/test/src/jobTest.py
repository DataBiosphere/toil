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
        config, batchSystem, jobStore = self.contextManager.__enter__()
        self.jobStore = jobStore
        
    def tearDown(self):
        self.contextManager.__exit__(None, None, None)
        system("rm -rf %s" % self.testJobTree)
        super( JobTest, self ).tearDown( )
    
    def testJob(self):        
        command = "by your command"
        memory = 2^32
        cpu = 1
        predecessorCount = 0
        updateID = 1000
        tryCount = int(self.jobStore.config.attrib["try_count"])
        
        for i in xrange(10):
            startTime = time.time()
            for j in xrange(100):
                j = self.jobStore.createFirstJob(command, memory, cpu, updateID, predecessorCount)
                self.assertEquals(j.remainingRetryCount, tryCount)
                self.assertEquals(j.command, command)
                self.assertEquals(j.memory, memory)
                self.assertEquals(j.cpu, cpu)
                self.assertEquals(j.predecessorCount, predecessorCount)
                self.assertEquals(j.stack, [])
                self.assertEquals(j.predecessorsFinished, set())
                #Test the storage of attributes
                j.predecessorsFinished = set(("1", "2"))
                j.stack = [ "foo", "bar" ]
                #Update status in store
                self.jobStore.update(j)
                j2 = self.jobStore.load(j.jobStoreID)
                self.assertEquals(j, j2)
                self.jobStore.delete(j)
                self.assertTrue(not self.jobStore.exists(j.jobStoreID))
            print "It took %f seconds to load/unload jobs" % (time.time() - startTime) #We've just used it for benchmarking, so far 
            #Would be good to extend this trivial test


if __name__ == '__main__':
    unittest.main()
