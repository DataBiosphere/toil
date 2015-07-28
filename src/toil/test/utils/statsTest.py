import unittest
import os

from toil.lib.bioio import TestStatus
from toil.lib.bioio import system
from toil.lib.bioio import getTempDirectory
from toil.lib.bioio import getTempFile
from toil.test.sort.sortTest import makeFileToSort
from toil.common import toilPackageDirPath
from toil.test import ToilTest


class StatsTest(ToilTest):
    """
    Tests the scriptTree toil-script compiler.
    """

    def setUp(self):
        super(StatsTest, self).setUp()
        self.testNo = TestStatus.getTestSetup(1, 2, 10, 10)

    def testToilStats_SortSimple(self):
        """
        Tests the toilStats utility using the scriptTree_sort example.
        """
        for test in xrange(self.testNo):
            tempDir = getTempDirectory(os.getcwd())
            tempFile = getTempFile(rootDir=tempDir)
            outputFile = getTempFile(rootDir=tempDir)
            toilDir = os.path.join(tempDir, "testToil")
            lines = 10000
            maxLineLength = 10
            N = 1000
            makeFileToSort(tempFile, lines, maxLineLength)
            # Sort the file
            rootPath = os.path.join(toilPackageDirPath(), "test", "sort")
            system("{rootPath}/sort.py "
                   "--toil {toilDir} "
                   "--logLevel=DEBUG "
                   "--fileToSort={tempFile} "
                   "--N {N} --stats "
                   "--jobTime 0.5 "
                   "--retryCount 99".format(**locals()))
            # Now get the stats
            toilStats = self.getScriptPath('toilStats')
            system("{toilStats} "
                   "--toil {toilDir} "
                   "--outputFile {outputFile}".format(**locals()))
            # Cleanup
            system("rm -rf %s" % tempDir)


if __name__ == '__main__':
    unittest.main()
