# Copyright (C) 2016 UCSC Computational Genomics Lab
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
from builtins import range
import os

# Python 3 compatibility imports
from six.moves import xrange

from toil.job import Job
from toil.test import ToilTest, slow
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.leader import FailedJobsException

@slow
class ResumabilityTest(ToilTest):
    """
    https://github.com/BD2KGenomics/toil/issues/808
    """
    def test(self):
        """
        Tests that a toil workflow that fails once can be resumed without a NoSuchJobException.
        """
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"
        options.retryCount = 0
        root = Job.wrapJobFn(parent)
        with self.assertRaises(FailedJobsException):
            # This one is intended to fail.
            Job.Runner.startToil(root, options)

        # Resume the workflow. Unfortunately, we have to check for
        # this bug using the logging output, since although the
        # NoSuchJobException causes the worker to fail, the batch
        # system code notices that the job has been deleted despite
        # the failure and avoids the failure.
        options.restart = True
        tempDir = self._createTempDir()
        options.logFile = os.path.join(tempDir, "log.txt")
        Job.Runner.startToil(root, options)
        with open(options.logFile) as f:
            logString = f.read()
            # We are looking for e.g. "Batch system is reporting that
            # the jobGraph with batch system ID: 1 and jobGraph
            # store ID: n/t/jobwbijqL failed with exit value 1"
            self.assertTrue("failed with exit value" not in logString)

def parent(job):
    """
    Set up a bunch of dummy child jobs, and a bad job that needs to be
    restarted as the follow on.
    """
    for _ in range(5):
        job.addChildJobFn(goodChild)
    job.addFollowOnJobFn(badChild)

def goodChild(job):
    """
    Does nothing.
    """
    return

def badChild(job):
    """
    Fails the first time it's run, succeeds the second time.
    """
    try:
        with job.fileStore.jobStore.readSharedFileStream("alreadyRun") as fileHandle:
            fileHandle.read()
    except NoSuchFileException as ex:
        with job.fileStore.jobStore.writeSharedFileStream("alreadyRun", isProtected=False) as fileHandle:
            fileHandle.write(b"failed once\n")
        raise RuntimeError("this is an expected error: {}".format(str(ex)))
