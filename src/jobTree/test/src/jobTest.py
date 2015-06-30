#!/usr/bin/env python
"""Test Job class
"""

import unittest
from jobTree.test import JobTreeTest
from jobTree.job import Job

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
        """
        Tests functions of a job.
        """ 
    
        command = "by your command"
        memory = 2^32
        cpu = 1
        jobStoreID = 100
        remainingRetryCount = 5
        predecessorNumber = 0
        updateID = 1000
        
        j = Job(command, memory, cpu, jobStoreID, remainingRetryCount, 
                  updateID, predecessorNumber)
        
        #Check attributes
        #
        self.assertEquals(j.command, command)
        self.assertEquals(j.memory, memory)
        self.assertEquals(j.cpu, cpu)
        self.assertEquals(j.jobStoreID, jobStoreID)
        self.assertEquals(j.remainingRetryCount, remainingRetryCount)
        self.assertEquals(j.predecessorNumber, predecessorNumber)
        self.assertEquals(j.updateID, updateID)
        self.assertEquals(j.stack, [])
        self.assertEquals(j.predecessorsFinished, set())
        self.assertEquals(j.logJobStoreFileID, None)
        
        #Check equals function
        j2 = Job(command, memory, cpu, jobStoreID, remainingRetryCount, 
                  updateID, predecessorNumber)
        self.assertEquals(j, j2)
        #Change an attribute and check not equal
        j.predecessorsFinished = set(("1", "2"))
        self.assertNotEquals(j, j2)
        
        ###TODO test other functionality

if __name__ == '__main__':
    unittest.main()
