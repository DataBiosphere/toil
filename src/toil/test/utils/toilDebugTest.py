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
# from builtins import str

import unittest
import subprocess
import os
import shutil
import logging
from toil.test import ToilTest, needs_aws, needs_rsync3, integrative, slow
import toil.utils.toilDebugFile

logger = logging.getLogger(__name__)

class ToilDebugFileTest(ToilTest):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        """
        Initial set up of variables for the test.
        """

        subprocess.check_call(['python', os.path.abspath('src/toil/test/utils/ABC/debugWorkflow.py')])
        self.jobStoreDir = os.path.abspath('toilWorkflowRun')
        self.tempDir = self._createTempDir(purpose='tempDir')

    def tearDown(self):
        """Default tearDown for unittest."""

        shutil.rmtree(self.jobStoreDir)

        unittest.TestCase.tearDown(self)

    # estimated run time
    @slow
    def testJobStoreContents(self):
        # toilDebugFile.printContentsOfJobStore()

        contents = ['A.txt', 'B.txt', 'C.txt', 'ABC.txt', 'mkFile.py']

        subprocess.check_call(['python', os.path.abspath('src/toil/utils/toilDebugFile.py'), self.jobStoreDir, '--listFilesInJobStore=True'])
        jobstoreFileContents = os.path.abspath('jobstore_files.txt')
        files = []
        match = 0
        with open(jobstoreFileContents, 'r') as f:
            for line in f:
                files.append(line.strip())
        for file in files:
            for expected_file in contents:
                if file.endswith(expected_file):
                    match = match + 1
        logger.info(files)
        logger.info(contents)
        logger.info(match)
        # C.txt will match twice (once with 'C.txt', and once with 'ABC.txt')
        assert match == 6


    # estimated run time
    @slow
    def testRecursiveGlob(self):
        # toilDebugFile.recursiveGlob()
        pass

    # estimated run time
    @slow
    def testFetchJobStoreFiles(self):
        # toilDebugFile.fetchJobStoreFiles()
        pass