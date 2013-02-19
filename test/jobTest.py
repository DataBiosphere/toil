#!/usr/bin/env python
"""Test Job class
"""

import unittest
import os
import time
import sys
import random

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
        tryCount = 100
        
        for i in xrange(10):
            startTime = time.time()
            for j in xrange(100):
                j = Job(command, memory, cpu, tryCount, jobDir)
                self.assertEquals(j.remainingRetryCount, tryCount)
                self.assertEquals(j.jobDir, jobDir)
                self.assertEquals(j.children, [])
                self.assertEquals(j.followOnCommands, [ (command, memory, cpu, 0)])
                self.assertEquals(j.messages, [])
                j.write()
                j = Job.read(j.getJobFileName())
                self.assertEquals(j.remainingRetryCount, tryCount)
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
        tryCount = 100
        
        for i in xrange(40):
            startTime = time.time()
            j = Job(command, memory, cpu, tryCount, jobDir)
            childNumber = random.choice(range(20))
            for k in xrange(childNumber):
                j.children.append((command, memory, cpu))
            self.assertEquals(len(j.children), childNumber)
            j.update(tryCount=tryCount, depth=0)
            j = Job.read(j.getJobFileName())
            self.assertEquals(len(j.children) + len(j.followOnCommands), childNumber + 1)
            for childJobFile, memory, cpu in j.children:
                cJ = Job.read(childJobFile)
                self.assertEquals(cJ.remainingRetryCount, tryCount)
                #self.assertEquals(cJ.jobDir, os.path.split(cJ)[0])
                self.assertEquals(cJ.children, [])
                self.assertEquals(cJ.followOnCommands, [ (command, memory, cpu, 0)])
                self.assertEquals(cJ.messages, [])
                self.assertTrue(os.path.exists(cJ.getJobFileName()))
                cJ.delete()
                self.assertTrue(not os.path.exists(cJ.getJobFileName()))
            self.assertEquals(os.listdir(jobDir), [ "job" ])
            j.delete()
            print "It took %f seconds to update jobs" % (time.time() - startTime) #We've just used it for benchmarking, so far 
            
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
