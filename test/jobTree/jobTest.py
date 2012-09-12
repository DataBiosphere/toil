#!/usr/bin/env python
"""Test Job class
"""

import unittest
import os
import time
import sys

from sonLib.bioio import parseSuiteTestOptions
from sonLib.bioio import logger

from jobTree.src.job import Job, readJob

class TestCase(unittest.TestCase):
    def testJob(self):
        tempJobFile = os.path.join(os.getcwd(), "jobTestTempJob")
        command = "by your command"
        memory = 2^32
        cpu = 1
        parentFile = "the parent file"
        globalTempDir = "the_global_temp_dir"
        retryCount = 100
        
        for i in xrange(10):
            startTime = time.time()
            for j in xrange(1000):
                j = Job(command, memory, cpu, parentFile, globalTempDir, retryCount)
                j.write(tempJobFile)
                j = readJob(tempJobFile)
            print "It took %f seconds to load/unload jobs" % (time.time() - startTime) #We've just used it for benchmarking, so far 
            #Would be good to extend this trivial test
        os.remove(tempJobFile)

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
