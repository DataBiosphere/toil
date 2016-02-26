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
from toil.leader import FailedJobsException
from toil.test import ToilTest

class CleanWorkDirTest(ToilTest):
    """
    Tests testing the Job.FileStore class
    """
    def setUp(self):
        super(CleanWorkDirTest, self).setUp()
        self.testDir = self._createTempDir()

    def tearDown(self):
        super(CleanWorkDirTest, self).tearDown()
        shutil.rmtree(self.testDir)

    def testNever(self):
        retainedTempData = self._runAndReturnWorkDir("never", job=tempFileTestJob)
        self.assertNotEqual(retainedTempData, [], "The worker's temporary workspace was deleted despite "
                                                  "cleanWorkDir being set to 'never'")

    def testAlways(self):
        retainedTempData = self._runAndReturnWorkDir("always", job=tempFileTestJob)
        self.assertEqual(retainedTempData, [], "The worker's temporary workspace was not deleted despite "
                                               "cleanWorkDir being set to 'always'")

    def testOnErrorWithError(self):
        retainedTempData = self._runAndReturnWorkDir("onError", job=tempFileTestErrorJob, expectError=True)
        self.assertEqual(retainedTempData, [], "The worker's temporary workspace was not deleted despite "
                                               "an error occurring and cleanWorkDir being set to 'onError'")

    def testOnErrorWithNoError(self):
        retainedTempData = self._runAndReturnWorkDir("onError", job=tempFileTestJob)
        self.assertNotEqual(retainedTempData, [], "The worker's temporary workspace was deleted despite "
                                                  "no error occurring and cleanWorkDir being set to 'onError'")

    def testOnSuccessWithError(self):
        retainedTempData = self._runAndReturnWorkDir("onSuccess", job=tempFileTestErrorJob, expectError=True)
        self.assertNotEqual(retainedTempData, [], "The worker's temporary workspace was deleted despite "
                                                  "an error occurring and cleanWorkDir being set to 'onSuccesss'")

    def testOnSuccessWithSuccess(self):
        retainedTempData = self._runAndReturnWorkDir("onSuccess", job=tempFileTestJob)
        self.assertEqual(retainedTempData, [], "The worker's temporary workspace was not deleted despite "
                                               "a successful job execution and cleanWorkDir being set to 'onSuccesss'")

    def _runAndReturnWorkDir(self, cleanWorkDir, job, expectError=False):
        """
        Runs toil with the specified job and cleanWorkDir setting. expectError determines whether the test's toil
        run is expected to succeed, and the test will fail if that expectation is not met. returns the contents of
        the workDir after completion of the run
        """
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.workDir = self.testDir
        options.clean = "always"
        options.cleanWorkDir = cleanWorkDir
        A = Job.wrapJobFn(job)
        if expectError:
            self._launchError(A, options)
        else:
            self._launchRegular(A, options)
        return os.listdir(self.testDir)

    def _launchRegular(self, A, options):
        Job.Runner.startToil(A, options)

    def _launchError(self, A, options):
        try:
            Job.Runner.startToil(A, options)
        except FailedJobsException:
            pass  # we expect a job to fail here
        else:
            self.fail("Toil run succeeded unexpectedly")

def tempFileTestJob(job):
    with open(job.fileStore.getLocalTempFile(), "w") as f:
        f.write("test file retention")

def tempFileTestErrorJob(job):
    with open(job.fileStore.getLocalTempFile(), "w") as f:
        f.write("test file retention")
    raise RuntimeError()  # test failure
