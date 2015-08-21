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
        self.tempDir = getTempDirectory(os.getcwd())
        self.tempFile = getTempFile(rootDir=self.tempDir)
        self.outputFile = getTempFile(rootDir=self.tempDir)
        self.toilDir = os.path.join(self.tempDir, "testToil")
        self.assertFalse(os.path.exists(self.toilDir))
        self.lines = 10000
        self.maxLineLength = 10
        self.N = 1000
        makeFileToSort(self.tempFile, self.lines, self.maxLineLength)
        # First make our own sorted version
        with open(self.tempFile, 'r') as fileHandle:
            self.correctSort = fileHandle.readlines()
            self.correctSort.sort()
            
    def tearDown(self):
        ToilTest.tearDown(self)
        system("rm -rf %s" % self.tempDir)

    def testUtilsSort(self):
        """
        Tests the restart, status and stats commands of the toil command line utility using the
        sort example.
        """
        # Get the sort command to run
        rootPath = os.path.join(toilPackageDirPath(), "test", "sort")
        toilCommandString = ("{rootPath}/sort.py "
                             "--jobStore {self.toilDir} "
                             "--logLevel=DEBUG "
                             "--fileToSort={self.tempFile} "
                             "--N {self.N} --stats "
                             "--retryCount 2".format(**locals()))

        # Try restarting it to check that a JobStoreException is thrown
        self.assertRaises(CalledProcessError, system, toilCommandString + " --restart")
        # Check that trying to run it in restart mode does not create the jobStore
        self.assertFalse(os.path.exists(self.toilDir))

        # Run the script for the first time
        system(toilCommandString)
        self.assertTrue(os.path.exists(self.toilDir))

        # Try running it without restart and check an exception is thrown
        self.assertRaises(CalledProcessError, system, toilCommandString)

        # Now restart it until done
        while True:
            # Run the status command
            rootPath = os.path.join(toilPackageDirPath(), "utils")
            toilStatusString = ("{rootPath}/toilMain.py status "
                                "--jobStore {self.toilDir} --failIfNotComplete".format(**locals()))
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
                           "--jobStore {self.toilDir} --pretty".format(**locals()))
        system(toilStatsString)

        # Check the file is properly sorted
        with open(self.tempFile, 'r') as fileHandle:
            l2 = fileHandle.readlines()
            checkEqual(self.correctSort, l2)
            
    def testUtilsStatsSort(self):
        """
        Tests the stats commands on a complete run of the stats test.
        """
        # Get the sort command to run
        rootPath = os.path.join(toilPackageDirPath(), "test", "sort")
        toilCommandString = ("{rootPath}/sort.py "
                             "--jobStore {self.toilDir} "
                             "--logLevel=DEBUG "
                             "--fileToSort={self.tempFile} "
                             "--N {self.N} --stats "
                             "--retryCount 99".format(**locals()))
    
        # Run the script for the first time
        system(toilCommandString)
        self.assertTrue(os.path.exists(self.toilDir))

        # Check we can run 'toil stats'
        rootPath = os.path.join(toilPackageDirPath(), "utils")
        toilStatsString = ("{rootPath}/toilMain.py stats "
                           "--jobStore {self.toilDir} --pretty".format(**locals()))
        system(toilStatsString)

        # Check the file is properly sorted
        with open(self.tempFile, 'r') as fileHandle:
            l2 = fileHandle.readlines()
            checkEqual(self.correctSort, l2)

if __name__ == '__main__':
    unittest.main()
