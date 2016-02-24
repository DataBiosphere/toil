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
from __future__ import absolute_import, print_function
import os
import shutil
from toil.job import Job
from toil.test import ToilTest

class CleanWorkDirTest(ToilTest):
    """
    Tests testing the Job.FileStore class
    """
    def testNever(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.workDir = self.testDir
        options.clean = "always"
        options.cleanWorkDir = "never"
        options.logLevel = "debug"
        A = Job.wrapJobFn(tempFileTestJob)
        Job.Runner.startToil(A, options)
        retainedTempData = os.listdir(self.testDir)
        self.assertNotEqual(retainedTempData, [], "The worker's temporary workspace was deleted despite "
                                                  "cleanWorkDir being set to 'never'")

    def testAlways(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.workDir = self.testDir
        options.clean = "always"
        options.cleanWorkDir = "always"
        A = Job.wrapJobFn(tempFileTestJob)
        Job.Runner.startToil(A, options)
        retainedTempData = os.listdir(self.testDir)
        self.assertEqual(retainedTempData, [], "The worker's temporary workspace was not deleted despite "
                                               "cleanWorkDir being set to 'always'")

    def testOnErrorWithError(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.workDir = self.testDir
        options.clean = "always"
        options.cleanWorkDir = "onError"
        A = Job.wrapJobFn(tempFileTestErrorJob)
        try:
            Job.Runner.startToil(A, options)
        except:
            pass  # we expect a job to fail here
        retainedTempData = os.listdir(self.testDir)
        self.assertEqual(retainedTempData, [], "The worker's temporary workspace was not deleted despite "
                                               "an error occurring and cleanWorkDir being set to 'onError'")

    def testOnErrorWithNoError(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.workDir = self.testDir
        options.clean = "always"
        options.cleanWorkDir = "onError"
        A = Job.wrapJobFn(tempFileTestJob)
        Job.Runner.startToil(A, options)
        retainedTempData = os.listdir(self.testDir)
        self.assertNotEqual(retainedTempData, [], "The worker's temporary workspace was deleted despite "
                                                  "no error occurring and cleanWorkDir being set to 'onError'")

    def testOnSuccessWithError(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.workDir = self.testDir
        options.clean = "always"
        options.cleanWorkDir = "onSuccess"
        A = Job.wrapJobFn(tempFileTestErrorJob)
        try:
            Job.Runner.startToil(A, options)
        except:
            pass  # we expect a job to fail here
        retainedTempData = os.listdir(self.testDir)
        self.assertNotEqual(retainedTempData, [], "The worker's temporary workspace was deleted despite "
                                                  "an error occurring and cleanWorkDir being set to 'onSuccesss'")

    def testOnSuccessWithSuccess(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.workDir = self.testDir
        options.clean = "always"
        options.cleanWorkDir = "onSuccess"
        A = Job.wrapJobFn(tempFileTestJob)
        Job.Runner.startToil(A, options)
        retainedTempData = os.listdir(self.testDir)
        self.assertEqual(retainedTempData, [], "The worker's temporary workspace was not deleted despite "
                                               "a successful job execution and cleanWorkDir being set to 'onSuccesss'")

    def setUp(self):
        super(CleanWorkDirTest, self).setUp()
        self.testDir = self._createTempDir()

    def tearDown(self):
        super(CleanWorkDirTest, self).tearDown()
        shutil.rmtree(self.testDir)

def tempFileTestJob(job):
    with open(job.fileStore.getLocalTempFile(), "w") as f:
        f.write("test file retention")

def tempFileTestErrorJob(job):
    with open(job.fileStore.getLocalTempFile(), "w") as f:
        f.write("test file retention")
    raise RuntimeError()  # test failure
