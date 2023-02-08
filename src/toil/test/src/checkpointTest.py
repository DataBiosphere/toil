# Copyright (C) 2015-2021 Regents of the University of California
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

from toil.job import Job
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.exceptions import FailedJobsException
from toil.test import ToilTest, slow


class CheckpointTest(ToilTest):
    def testCheckpointNotRetried(self):
        """A checkpoint job should not be retried if the workflow has a retryCount of 0."""
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.retryCount = 0
        # We set the workflow to *succeed* if the checkpointed job is
        # retried despite the retry count of 0. So, if the workflow
        # fails, this test succeeds.
        # This ends up being a bit confusing, but it's the easiest way
        # to check that the job wasn't retried short of parsing the
        # log.
        with self.assertRaises(FailedJobsException):
            Job.Runner.startToil(CheckRetryCount(numFailuresBeforeSuccess=1), options)

    @slow
    def testCheckpointRetriedOnce(self):
        """A checkpoint job should be retried exactly once if the workflow has a retryCount of 1."""
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.retryCount = 1

        # This should succeed (the checkpoint should be retried once, then succeed)
        try:
            Job.Runner.startToil(CheckRetryCount(numFailuresBeforeSuccess=1), options)
        except FailedJobsException:
            self.fail("The checkpoint job wasn't retried enough times.")

        # This should fail (the checkpoint should be retried once, then fail again)
        with self.assertRaises(FailedJobsException):
            Job.Runner.startToil(CheckRetryCount(numFailuresBeforeSuccess=2), options)

    @slow
    def testCheckpointedRestartSucceeds(self):
        """A checkpointed job should succeed on restart of a failed run if its child job succeeds."""
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        # The child job should fail the first time
        with self.assertRaises(FailedJobsException):
            Job.Runner.startToil(CheckpointFailsFirstTime(), options)

        # The second time, everything should work
        options.restart = True
        try:
            Job.Runner.startToil(CheckpointFailsFirstTime(), options)
        except FailedJobsException:
            self.fail("Checkpointed workflow restart doesn't clean failures.")

class CheckRetryCount(Job):
    """Fail N times, succeed on the next try."""
    def __init__(self, numFailuresBeforeSuccess):
        super().__init__(checkpoint=True)
        self.numFailuresBeforeSuccess = numFailuresBeforeSuccess

    def getNumRetries(self, fileStore):
        """Mark a retry in the fileStore, and return the number of retries so far."""
        try:
            with fileStore.jobStore.read_shared_file_stream("checkpointRun") as f:
                timesRun = int(f.read().decode('utf-8'))
        except NoSuchFileException:
            timesRun = 0
        with fileStore.jobStore.write_shared_file_stream("checkpointRun") as f:
            f.write(str(timesRun + 1).encode('utf-8'))
        return timesRun

    def run(self, fileStore):
        retryCount = self.getNumRetries(fileStore)
        fileStore.logToMaster(str(retryCount))
        if retryCount < self.numFailuresBeforeSuccess:
            self.addChild(AlwaysFail())

class AlwaysFail(Job):
    def run(self, fileStore):
        raise RuntimeError(":(")

class CheckpointFailsFirstTime(Job):
    def __init__(self):
        super().__init__(checkpoint=True)

    def run(self, fileStore):
        self.addChild(FailOnce())

class FailOnce(Job):
    """Fail the first time the workflow is run, but succeed thereafter."""
    def run(self, fileStore):
        if fileStore.jobStore.config.workflowAttemptNumber < 1:
            raise RuntimeError("first time around")
