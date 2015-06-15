import unittest
import os

from jobTree.lib.bioio import TestStatus
from jobTree.lib.bioio import system
from jobTree.lib.bioio import getTempDirectory
from jobTree.lib.bioio import getTempFile
from jobTree.test.sort.sortTest import makeFileToSort
from jobTree.common import workflowRootPath
from jobTree.test import JobTreeTest


class StatsTest(JobTreeTest):
    """
    Tests the scriptTree jobTree-script compiler.
    """

    def setUp(self):
        super(StatsTest, self).setUp()
        self.testNo = TestStatus.getTestSetup(1, 2, 10, 10)

    def testJobTreeStats_SortSimple(self):
        """
        Tests the jobTreeStats utility using the scriptTree_sort example.
        """
        for test in xrange(self.testNo):
            tempDir = getTempDirectory(os.getcwd())
            tempFile = getTempFile(rootDir=tempDir)
            outputFile = getTempFile(rootDir=tempDir)
            jobTreeDir = os.path.join(tempDir, "testJobTree")
            lines = 10000
            maxLineLength = 10
            N = 1000
            makeFileToSort(tempFile, lines, maxLineLength)
            # Sort the file
            rootPath = os.path.join(workflowRootPath(), "test/sort")
            system("{rootPath}/sort.py "
                   "--jobTree {jobTreeDir} "
                   "--logLevel=DEBUG "
                   "--fileToSort={tempFile} "
                   "--N {N} --stats "
                   "--jobTime 0.5 "
                   "--retryCount 99".format(**locals()))
            # Now get the stats
            jobTreeStats = self.getScriptPath('jobTreeStats')
            system("{jobTreeStats} "
                   "--jobTree {jobTreeDir} "
                   "--outputFile {outputFile}".format(**locals()))
            # Cleanup
            system("rm -rf %s" % tempDir)


if __name__ == '__main__':
    unittest.main()
