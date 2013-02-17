#!/usr/bin/env python
"""Test Job class
"""

import unittest
import os
import time
import sys

from sonLib.bioio import parseSuiteTestOptions
from sonLib.bioio import logger, system

from jobTree.src.job import Job

class TestCase(unittest.TestCase):
    def testJobReadWriteAndDelete(self):
        jobDir = os.path.join(os.getcwd(), "testJobDir")
        os.mkdir(jobDir) #If directory already exists then the test will fail
        command = "by your command"
        memory = 2^32
        cpu = 1
        retryCount = 100
        
        for i in xrange(10):
            startTime = time.time()
            for j in xrange(1000):
                j = Job(command, memory, cpu, retryCount, jobDir)
                self.assertEquals(j.remainingRetryCount, retryCount)
                self.assertEquals(j.jobDir, jobDir)
                self.assertEquals(j.children, [])
                self.assertEquals(j.followOnCommands, [ (command, memory, cpu, 0)])
                self.assertEquals(j.messages, [])
                j.write()
                j = Job.read(j.getJobFileName())
                self.assertEquals(j.remainingRetryCount, retryCount)
                self.assertEquals(j.jobDir, jobDir)
                self.assertEquals(j.children, [])
                self.assertEquals(j.followOnCommands, [ (command, memory, cpu, 0)])
                self.assertEquals(j.messages, [])
                self.assertTrue(os.path.exists(j.getJobFileName()))
                j.delete()
                self.assertTrue(not os.path.exists(j.getJobFileName()))
            print "It took %f seconds to load/unload jobs" % (time.time() - startTime) #We've just used it for benchmarking, so far 
            #Would be good to extend this trivial test
            
        system("rm -rf %s" % jobDir)
        
    def testJobUpdate(self):
        jobDir = os.path.join(os.getcwd(), "testJobDir")
        os.mkdir(jobDir) #If directory already exists then the test will fail
        command = "by your command"
        memory = 2^32
        cpu = 1
        retryCount = 100
        
        for i in xrange(10):
            startTime = time.time()
            for j in xrange(1000):
                j = Job(command, memory, cpu, retryCount, jobDir)
                self.assertEquals(j.remainingRetryCount, retryCount)
                self.assertEquals(j.jobDir, jobDir)
                self.assertEquals(j.children, [])
                self.assertEquals(j.followOnCommands, [ (command, memory, cpu, 0)])
                self.assertEquals(j.messages, [])
                j.write()
                j = Job.read(j.getJobFileName())
                self.assertEquals(j.remainingRetryCount, retryCount)
                self.assertEquals(j.jobDir, jobDir)
                self.assertEquals(j.children, [])
                self.assertEquals(j.followOnCommands, [ (command, memory, cpu, 0)])
                self.assertEquals(j.messages, [])
                self.assertTrue(os.path.exists(j.getJobFileName()))
                j.delete()
                self.assertTrue(not os.path.exists(j.getJobFileName()))
            print "It took %f seconds to load/unload jobs" % (time.time() - startTime) #We've just used it for benchmarking, so far 
            #Would be good to extend this trivial test
            
        system("rm -rf %s" % jobDir)

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
