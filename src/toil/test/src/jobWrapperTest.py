#!/usr/bin/env python
"""Test Batchjob class
"""

import unittest
import os
from toil.lib.bioio import system
from optparse import OptionParser
from toil.common import setupToil
from toil.job import Job
from toil.test import ToilTest
from toil.jobWrapper import JobWrapper

class JobWrapperTest(ToilTest):
    
    def setUp(self):
        super( JobWrapperTest, self ).setUp( )
        self.testToil = os.path.join(os.getcwd(), "testJobDir")
        parser = OptionParser()
        Job.Runner.addToilOptions(parser)
        options, args = parser.parse_args()
        options.toil = self.testToil
        self.contextManager = setupToil(options)
        config, batchSystem, jobStore = self.contextManager.__enter__()
        self.jobStore = jobStore
        
    def tearDown(self):
        self.contextManager.__exit__(None, None, None)
        system("rm -rf %s" % self.testToil)
        super( JobWrapperTest, self ).tearDown( )
    
    def testJob(self):       
        """
        Tests functions of a job.
        """ 
    
        command = "by your command"
        memory = 2^32
        disk = 2^32
        cpu = 1
        jobStoreID = 100
        remainingRetryCount = 5
        predecessorNumber = 0
        updateID = 1000
        
        j = JobWrapper(command, memory, cpu, disk, jobStoreID, remainingRetryCount,
                  updateID, predecessorNumber)
        
        #Check attributes
        #
        self.assertEquals(j.command, command)
        self.assertEquals(j.memory, memory)
        self.assertEquals(j.disk, disk)
        self.assertEquals(j.cpu, cpu)
        self.assertEquals(j.jobStoreID, jobStoreID)
        self.assertEquals(j.remainingRetryCount, remainingRetryCount)
        self.assertEquals(j.predecessorNumber, predecessorNumber)
        self.assertEquals(j.updateID, updateID)
        self.assertEquals(j.stack, [])
        self.assertEquals(j.predecessorsFinished, set())
        self.assertEquals(j.logJobStoreFileID, None)
        
        #Check equals function
        j2 = JobWrapper(command, memory, cpu, disk, jobStoreID, remainingRetryCount,
                  updateID, predecessorNumber)
        self.assertEquals(j, j2)
        #Change an attribute and check not equal
        j.predecessorsFinished = set(("1", "2"))
        self.assertNotEquals(j, j2)
        
        ###TODO test other functionality

if __name__ == '__main__':
    unittest.main()
