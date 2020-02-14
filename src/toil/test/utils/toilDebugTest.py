# Copyright (C) 2018 Regents of the University of California
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
import shutil
import logging

import subprocess
from toil.test import ToilTest, slow, travis_test
from toil.utils.toilDebugFile import recursiveGlob

logger = logging.getLogger(__name__)

class ToilDebugFileTest(ToilTest):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        """Initial set up of variables for the test."""
        subprocess.check_call(['python', os.path.abspath('src/toil/test/utils/ABCWorkflowDebug/debugWorkflow.py')])
        self.jobStoreDir = os.path.abspath('toilWorkflowRun')
        self.tempDir = self._createTempDir(purpose='tempDir')

    def tearDown(self):
        """Default tearDown for unittest."""

        shutil.rmtree(self.jobStoreDir)
        ABC = os.path.abspath('src/toil/test/utils/ABCWorkflowDebug/ABC.txt')
        if os.path.exists(ABC):
            os.remove(ABC)

        unittest.TestCase.tearDown(self)

    @slow
    def testJobStoreContents(self):
        """Test toilDebugFile.printContentsOfJobStore().

        Runs a workflow that imports 'B.txt' and 'mkFile.py' into the
        jobStore.  'A.txt', 'C.txt', 'ABC.txt' are then created.  This checks to
        make sure these contents are found in the jobStore and printed."""

        contents = ['A.txt', 'B.txt', 'C.txt', 'ABC.txt', 'mkFile.py']

        subprocess.check_call(['python', os.path.abspath('src/toil/utils/toilDebugFile.py'), self.jobStoreDir, '--listFilesInJobStore=True'])
        jobstoreFileContents = os.path.abspath('jobstore_files.txt')
        files = []
        match = 0
        with open(jobstoreFileContents, 'r') as f:
            for line in f:
                files.append(line.strip())
        for xfile in files:
            for expected_file in contents:
                if xfile.endswith(expected_file):
                    match = match + 1
        logger.debug(files)
        logger.debug(contents)
        logger.debug(match)
        # C.txt will match twice (once with 'C.txt', and once with 'ABC.txt')
        assert match == 6
        os.remove(jobstoreFileContents)

    # expected run time = 4s
    @travis_test
    def testFetchJobStoreFiles(self):
        """Test toilDebugFile.fetchJobStoreFiles() without using symlinks."""
        self.fetchFiles(symLink=False)

    # expected run time = 4s
    @travis_test
    def testFetchJobStoreFilesWSymlinks(self):
        """Test toilDebugFile.fetchJobStoreFiles() using symlinks."""
        self.fetchFiles(symLink=True)

    def fetchFiles(self, symLink):
        """
        Fn for testFetchJobStoreFiles() and testFetchJobStoreFilesWSymlinks().

        Runs a workflow that imports 'B.txt' and 'mkFile.py' into the
        jobStore.  'A.txt', 'C.txt', 'ABC.txt' are then created.  This test then
        attempts to get a list of these files and copy them over into ./src from
        the jobStore, confirm that they are present, and then delete them.
        """
        contents = ['A.txt', 'B.txt', 'C.txt', 'ABC.txt', 'mkFile.py']
        outputDir = os.path.abspath('src')
        cmd = ['python', os.path.abspath('src/toil/utils/toilDebugFile.py'),
               self.jobStoreDir,
               '--fetch', '*A.txt', '*B.txt', '*C.txt', '*ABC.txt', '*mkFile.py',
               '--localFilePath=' + os.path.abspath('src'),
               '--useSymlinks=' + str(symLink)]
        subprocess.check_call(cmd)
        for xfile in contents:
            matchingFilesFound = recursiveGlob(outputDir, '*' + xfile)
            self.assertGreaterEqual(len(matchingFilesFound), 1)
            for fileFound in matchingFilesFound:
                assert fileFound.endswith(xfile) and os.path.exists(fileFound)
                if fileFound.endswith('-' + xfile):
                    os.remove(fileFound)
