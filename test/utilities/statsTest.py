"""Tests the scriptTree jobTree-script compiler.
"""

import unittest
import sys
import os

from jobTree.src.bioio import TestStatus
from jobTree.src.bioio import parseSuiteTestOptions
from jobTree.src.bioio import system
from jobTree.src.bioio import getTempDirectory
from jobTree.src.bioio import getTempFile

from jobTree.test.sort.sortTest import makeFileToSort

class TestCase(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.testNo = TestStatus.getTestSetup(1, 2, 10, 10)
    
    def testJobTreeStats_SortSimple(self):
        """Tests the jobTreeStats utility using the scriptTree_sort example.
        """
        for test in xrange(self.testNo):
            tempDir = getTempDirectory(os.getcwd())
            tempFile = getTempFile(rootDir=tempDir)
            outputFile = getTempFile(rootDir=tempDir)
            jobTreeDir = os.path.join(tempDir, "jobTree")
            lines=100000
            maxLineLength=10
            N=1000
            makeFileToSort(tempFile, lines, maxLineLength)
            #Sort the file
            command = "scriptTreeTest_Sort.py --jobTree %s --logLevel=DEBUG --fileToSort=%s --N %s --stats --jobTime 0.5" % (jobTreeDir, tempFile, N)
            system(command)
            #Now get the stats
            system("jobTreeStats --jobTree %s --outputFile %s" % (jobTreeDir, outputFile))
            #Cleanup
            system("rm -rf %s" % tempDir)
                   
def main():
    parseSuiteTestOptions()
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()
