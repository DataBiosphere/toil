"""Tests the scriptTree jobTree-script compiler.
"""

import unittest
import sys
import os

from sonLib.bioio import TestStatus
from sonLib.bioio import parseSuiteTestOptions
from sonLib.bioio import system
from sonLib.bioio import getTempDirectory
from jobTree.src.common import runJobTreeStatusAndFailIfNotComplete
 
class TestCase(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.testNo = TestStatus.getTestSetup(1, 2, 10, 10)
        self.tempDir = getTempDirectory(os.getcwd())
        self.jobTreeDir = os.path.join(self.tempDir, "testJobTree") #A directory for the job tree to be created in
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        system("rm -rf %s" % self.tempDir) #Cleanup the job tree in case it hasn't already been cleaned up.
        
    def testScriptTree_Example(self):
        """Uses the jobTreeTest code to test the scriptTree Target wrapper.
        """
        for test in xrange(self.testNo):
            command = "scriptTreeTest_Wrapper.py --jobTree %s --logLevel=DEBUG --retryCount=99" % self.jobTreeDir
            system(command)
            runJobTreeStatusAndFailIfNotComplete(self.jobTreeDir)
    
    def testScriptTree_Example2(self):
        """Tests that the global and local temp dirs of a job behave as expected.
        """
        for test in xrange(self.testNo):
            command = "scriptTreeTest_Wrapper2.py --jobTree %s --logLevel=DEBUG --retryCount=99" % self.jobTreeDir
            system(command)
            runJobTreeStatusAndFailIfNotComplete(self.jobTreeDir)
                   
def main():
    parseSuiteTestOptions()
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()
