# Copyright (C) 2015-2017 Regents of the University of California
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
from toil.job import Job
from toil.test import ToilTest
from toil.leader import FailedJobsException
from toil.jobStores.abstractJobStore import NoSuchFileException

class CheckpointTest(ToilTest):
    def testCheckpointNotRetried(self):
        """A checkpoint job with a retryCount of 0 should not be retried."""
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.retryCount = 0
        # We set the workflow to *succeed* if the checkpointed job is
        # retried despite the retry count of 0. So, if the workflow
        # fails, this test succeeds.
        # This ends up being a bit confusing, but it's the easiest way
        # to check that the job wasn't retried short of parsing the
        # log.
        with self.assertRaises(FailedJobsException):
            Job.Runner.startToil(Checkpointed(succeedOnRetry=True), options)

    def testCheckpointedRestart(self):
        """A checkpointed job should succeed on restart of a failed run if its child job succeeds."""
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        # The child job should fail the first time
        with self.assertRaises(FailedJobsException):
            Job.Runner.startToil(Checkpointed(), options)

        # The second time, everything should work
        options.restart = True
        try:
            Job.Runner.startToil(Checkpointed(), options)
        except FailedJobsException:
            self.fail("Checkpointed restart is broken.")

class Checkpointed(Job):
    def __init__(self, succeedOnRetry=False):
        super(Checkpointed, self).__init__(checkpoint=True)
        self.succeedOnRetry = succeedOnRetry

    def checkForRetry(self, fileStore):
        try:
            with fileStore.jobStore.readSharedFileStream("checkpointRun") as f:
                # Read to avoid the other thread blocking.
                f.read()
                # We only get here if the file exists--bad news.
                return True
        except NoSuchFileException:
            with fileStore.jobStore.writeSharedFileStream("checkpointRun") as f:
                f.write("")
            return False

    def run(self, fileStore):
        if not (self.succeedOnRetry and self.checkForRetry(fileStore)):
            self.addChild(FailOnce())

class FailOnce(Job):
    """Fail the first time the workflow is run, but succeed thereafter."""
    def run(self, fileStore):
        if fileStore.jobStore.config.workflowAttemptNumber < 1:
            raise RuntimeError("first time around")
