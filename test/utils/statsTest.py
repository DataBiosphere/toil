"""Tests the scriptTree jobTree-script compiler.
"""

import unittest
import sys
import os

from jobTree.lib.bioio import TestStatus
from jobTree.lib.bioio import parseSuiteTestOptions
from jobTree.lib.bioio import system
from jobTree.lib.bioio import getTempDirectory
from jobTree.lib.bioio import getTempFile

from jobTree.test.sort.sortTest import makeFileToSort
from jobTree.src.common import workflowRootPath
from jobTree.test import JobTreeTest

class TestCase(JobTreeTest):

    def setUp( self ):
        super( TestCase, self ).setUp( )
        self.testNo = TestStatus.getTestSetup(1, 2, 10, 10)
    
    def testJobTreeStats_SortSimple(self):
        """Tests the jobTreeStats utility using the scriptTree_sort example.
        """
        for test in xrange(self.testNo):
            tempDir = getTempDirectory(os.getcwd())
            tempFile = getTempFile(rootDir=tempDir)
            outputFile = getTempFile(rootDir=tempDir)
            jobTreeDir = os.path.join(tempDir, "testJobTree")
            lines=10000
            maxLineLength=10
            N=1000
            makeFileToSort(tempFile, lines, maxLineLength)
            #Sort the file
            command = "%s/sort.py --jobTree %s --logLevel=DEBUG \
            --fileToSort=%s --N %s --stats --jobTime 0.5 --retryCount 99" % \
            (os.path.join(workflowRootPath(), "test/sort"), jobTreeDir, tempFile, N)
            system(command)
            #Now get the stats
            system("jobTreeStats --jobTree %s --outputFile %s" % \
                   (jobTreeDir, outputFile))
            #Cleanup
            system("rm -rf %s" % tempDir)
            
                   
def main():
    parseSuiteTestOptions()
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()
