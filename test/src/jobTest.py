#!/usr/bin/env python
"""Test Job class
"""

import unittest
import os
import time
import sys
import random
from optparse import OptionParser

from sonLib.bioio import parseSuiteTestOptions
from sonLib.bioio import system
from jobTree.src.stack import Stack
from jobTree.src.common import setupJobTree
from jobTree.test import JobTreeTest


class TestCase(JobTreeTest):

    def setUp(self):
        super( TestCase, self ).setUp( )
        self.testJobTree = os.path.join(os.getcwd(), "testJobDir")
        parser = OptionParser()
        Stack.addJobTreeOptions(parser)
        options, args = parser.parse_args()
        options.jobTree = self.testJobTree
        config, batchSystem, jobStore, jobTreeState = setupJobTree(options)
        self.jobStore = jobStore
        
    def tearDown(self):
        super( TestCase, self ).tearDown( )
        system("rm -rf %s" % self.testJobTree)
    
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
                self.assertEquals(j.messages, [])
                self.jobStore.store(j)
                jobStoreID = j.jobStoreID
                j = self.jobStore.load(j.jobStoreID)
                self.assertEquals(j.remainingRetryCount, tryCount)
                self.assertEquals(j.jobStoreID, jobStoreID)
                self.assertEquals(j.children, [])
                self.assertEquals(j.followOnCommands, [ (command, memory, cpu, 0)])
                self.assertEquals(j.messages, [])
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
                self.assertEquals(cJ.messages, [])
                self.assertTrue(self.jobStore.exists(cJ.jobStoreID))
                self.jobStore.delete(cJ)
                self.assertTrue(not self.jobStore.exists(cJ.jobStoreID))
            self.assertTrue(self.jobStore.exists(j.jobStoreID))
            self.jobStore.delete(j)
            self.assertTrue(not self.jobStore.exists(j.jobStoreID))
            print "It took %f seconds to update jobs" % (time.time() - startTime) #We've just used it for benchmarking, so far 
            

def main():
    parseSuiteTestOptions()
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    #import cProfile
    #cProfile.run('main()', "fooprof")
    #import pstats
    #p = pstats.Stats('fooprof')
    #p.strip_dirs().sort_stats(-1).print_stats()
    #print p
    main()
