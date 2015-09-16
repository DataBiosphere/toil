# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
import unittest
import os
from subprocess import CalledProcessError

from toil.lib.bioio import system
from toil.lib.bioio import getTempFile
from toil.common import toilPackageDirPath
from toil.test import ToilTest
from toil.test.sort.sortTest import makeFileToSort


class UtilsTest(ToilTest):
    """
    Tests the utilities that toil ships with, e.g. stats and status, in conjunction with restart
    functionality.
    """

    def setUp(self):
        super(UtilsTest, self).setUp()
        self.tempDir = self._createTempDir()
        self.tempFile = getTempFile(rootDir=self.tempDir)
        self.outputFile = getTempFile(rootDir=self.tempDir)
        self.toilDir = os.path.join(self.tempDir, "jobstore")
        self.assertFalse(os.path.exists(self.toilDir))
        self.lines = 1000
        self.lineLen = 10
        self.N = 1000
        makeFileToSort(self.tempFile, self.lines, self.lineLen)
        # First make our own sorted version
        with open(self.tempFile, 'r') as fileHandle:
            self.correctSort = fileHandle.readlines()
            self.correctSort.sort()

    def tearDown(self):
        ToilTest.tearDown(self)
        system("rm -rf %s" % self.tempDir)

    @property
    def toilMain(self):
        return self._getUtilScriptPath('toilMain')

    @property
    def sort(self):
        return os.path.join(toilPackageDirPath(), "test", "sort", "sort.py")

    def testUtilsSort(self):
        """
        Tests the restart, status and stats commands of the toil command line utility using the
        sort example.
        """
        # Get the sort command to run
        toilCommandString = ("{self.sort} "
                             "{self.toilDir} "
                             "--logLevel=DEBUG "
                             "--fileToSort={self.tempFile} "
                             "--N {self.N} --stats "
                             "--retryCount 2".format(**locals()))

        # Try restarting it to check that a JobStoreException is thrown
        self.assertRaises(CalledProcessError, system, toilCommandString + " --restart")
        # Check that trying to run it in restart mode does not create the jobStore
        self.assertFalse(os.path.exists(self.toilDir))

        # Status command
        toilStatusCommandString = ("{self.toilMain} status "
                                   "{self.toilDir} "
                                   "--failIfNotComplete".format(**locals()))

        # Run the script for the first time
        try:
            system(toilCommandString)
            finished = True
        except CalledProcessError:  # This happens when the script fails due to having unfinished jobs
            self.assertRaises(CalledProcessError, system, toilStatusCommandString)
            finished = False
        self.assertTrue(os.path.exists(self.toilDir))

        # Try running it without restart and check an exception is thrown
        self.assertRaises(CalledProcessError, system, toilCommandString)

        # Now restart it until done
        while not finished:
            try:
                system(toilCommandString + " --restart")
                finished = True
            except CalledProcessError:  # This happens when the script fails due to having unfinished jobs
                self.assertRaises(CalledProcessError, system, toilStatusCommandString)

        # Check the toil status command does not issue an exception
        system(toilStatusCommandString)

        # Check if we try to launch after its finished that we get a JobException
        self.assertRaises(CalledProcessError, system, toilCommandString + " --restart")

        # Check we can run 'toil stats'
        toilStatsString = ("{self.toilMain} stats "
                           "{self.toilDir} "
                           "--pretty".format(**locals()))
        system(toilStatsString)

        # Check the file is properly sorted
        with open(self.tempFile, 'r') as fileHandle:
            l2 = fileHandle.readlines()
            self.assertEquals(self.correctSort, l2)

        # Check we can run 'toil clean'
        toilCleanString = ("{self.toilMain} clean "
                           "{self.toilDir}".format(**locals()))
        system(toilCleanString)
            
    def testUtilsStatsSort(self):
        """
        Tests the stats commands on a complete run of the stats test.
        """
        # Get the sort command to run
        toilCommandString = ("{self.sort} "
                             "{self.toilDir} "
                             "--logLevel=DEBUG "
                             "--fileToSort={self.tempFile} "
                             "--N {self.N} --stats "
                             "--retryCount 99".format(**locals()))

        # Run the script for the first time
        system(toilCommandString)
        self.assertTrue(os.path.exists(self.toilDir))

        # Check we can run 'toil stats'
        rootPath = os.path.join(toilPackageDirPath(), "utils")
        toilStatsString = ("{self.toilMain} stats "
                           "{self.toilDir} --pretty".format(**locals()))
        system(toilStatsString)

        # Check the file is properly sorted
        with open(self.tempFile, 'r') as fileHandle:
            l2 = fileHandle.readlines()
            self.assertEquals(self.correctSort, l2)


if __name__ == '__main__':
    unittest.main()
