from __future__ import absolute_import
import unittest
import os
from subprocess import CalledProcessError

from toil.lib.bioio import system
from toil.lib.bioio import getTempDirectory
from toil.lib.bioio import getTempFile
from toil.test.sort.sortTest import makeFileToSort, checkEqual
from toil.common import toilPackageDirPath
from toil.test import ToilTest


class UtilsTest(ToilTest):
    """
    Tests the scriptTree toil-script compiler.
    """

    def setUp(self):
        super(UtilsTest, self).setUp()
        self.testNo = 1

    def testUtilsSort(self):
        """
        Tests the restart, status and stats commands of the toil command line utility using the
        sort example.
        """
        for test in xrange(self.testNo):
            tempDir = getTempDirectory(os.getcwd())
            tempFile = getTempFile(rootDir=tempDir)
            outputFile = getTempFile(rootDir=tempDir)
            toilDir = os.path.join(tempDir, "testToil")
            self.assertFalse(os.path.exists(toilDir))
            lines = 1000
            maxLineLength = 10
            N = 1000
            makeFileToSort(tempFile, lines, maxLineLength)

            # First make our own sorted version
            with open(tempFile, 'r') as fileHandle:
                l = fileHandle.readlines()
                l.sort()

            # Get the sort command to run
            rootPath = os.path.join(toilPackageDirPath(), "test", "sort")
            toilCommandString = ("{rootPath}/sort.py "
                                 "--jobStore {toilDir} "
                                 "--logLevel=DEBUG "
                                 "--fileToSort={tempFile} "
                                 "--N {N} --stats "
                                 "--retryCount 2".format(**locals()))

            # Try restarting it to check that a JobStoreException is thrown
            self.assertRaises(CalledProcessError, system, toilCommandString + " --restart")
            # Check that trying to run it in restart mode does not create the jobStore
            self.assertFalse(os.path.exists(toilDir))

            # Run the script for the first time
            system(toilCommandString)
            self.assertTrue(os.path.exists(toilDir))

            # Try running it without restart and check an exception is thrown
            self.assertRaises(CalledProcessError, system, toilCommandString)

            # Now restart it until done
            while True:
                # Run the status command
                rootPath = os.path.join(toilPackageDirPath(), "utils")
                toilStatusString = ("{rootPath}/toilMain.py status "
                                    "--jobStore {toilDir} --failIfNotComplete".format(**locals()))
                try:
                    system(toilStatusString)
                except CalledProcessError:
                    # If there is an exception there are still failed jobs then restart
                    system(toilCommandString + " --restart")
                else:
                    break


            # Check if we try to launch after its finished that we get a JobException
            self.assertRaises(CalledProcessError, system, toilCommandString + " --restart")

            # Check we can run 'toil stats'
            rootPath = os.path.join(toilPackageDirPath(), "utils")
            toilStatsString = ("{rootPath}/toilMain.py stats "
                               "--jobStore {toilDir} --pretty".format(**locals()))
            system(toilStatsString)

            # Check the file is properly sorted
            with open(tempFile, 'r') as fileHandle:
                l2 = fileHandle.readlines()
                checkEqual(l, l2)

            # Cleanup
            system("rm -rf %s" % tempDir)


if __name__ == '__main__':
    unittest.main()
