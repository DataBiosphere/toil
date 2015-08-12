import unittest
import os

from toil.lib.bioio import system
from toil.lib.bioio import getTempDirectory
from toil.lib.bioio import getTempFile
from toil.test.sort.sortTest import makeFileToSort
from toil.common import toilPackageDirPath
from toil.test import ToilTest
import subprocess

class UtilsTest(ToilTest):
    """
    Tests the scriptTree toil-script compiler.
    """

    def setUp(self):
        super(UtilsTest, self).setUp()
        self.testNo = 1

    def testUtilsSort(self):
        """
        Tests the toilRestart/Status/Stats utilities using the sort example.
        """
        for test in xrange(self.testNo):
            tempDir = getTempDirectory(os.getcwd())
            tempFile = getTempFile(rootDir=tempDir)
            outputFile = getTempFile(rootDir=tempDir)
            toilDir = os.path.join(tempDir, "testToil")
            self.assertFalse(os.path.exists(toilDir))
            lines = 10000
            maxLineLength = 10
            N = 1000
            makeFileToSort(tempFile, lines, maxLineLength)
            
            #Get the sort command to run
            rootPath = os.path.join(toilPackageDirPath(), "test", "sort")
            toilCommandString = ("{rootPath}/sort.py "
                   "--toil {toilDir} "
                   "--logLevel=DEBUG "
                   "--fileToSort={tempFile} "
                   "--N {N} --stats "
                   "--retryCount 2".format(**locals()))
            
            #Try restarting it to check that a JobStoreException is thrown
            try:
                system(toilCommandString + " --restart")
                self.assertTrue(False) #This should not be executed
            except subprocess.CalledProcessError:
                pass
            #Check that trying to run it in restart mode does not create the jobStore
            self.assertFalse(os.path.exists(toilDir))
            
            #Run the script for the first time
            system(toilCommandString)
            self.assertTrue(os.path.exists(toilDir))
            
            #Try running it without restart and check an exception is thrown
            try:
                system(toilCommandString)
                self.assertTrue(False) #This should not be executed
            except subprocess.CalledProcessError:
                pass
            
            #Now restart it until done
            while True:
                #Run the status command
                rootPath = os.path.join(toilPackageDirPath(), "utils")
                toilStatusString = ("{rootPath}/toilStatus.py "
                       "--toil {toilDir} --failIfNotComplete".format(**locals()))
                try:
                    system(toilStatusString)
                    break
                except:
                    #If there is an exception there are still failed jobs then restart
                    system(toilCommandString + " --restart")
                
            #Check if we try to launch after its finished that we get a JobException
            try:
                system(toilCommandString + " --restart")
                self.assertTrue(False) #This should not be executed
            except subprocess.CalledProcessError:
                pass
            
            #Check we can run toilStats
            rootPath = os.path.join(toilPackageDirPath(), "utils")
            toilStatsString = ("{rootPath}/toilStats.py "
                                "--toil {toilDir} --pretty".format(**locals()))
            system(toilStatsString)
            
            # Cleanup
            system("rm -rf %s" % tempDir)


if __name__ == '__main__':
    unittest.main()
