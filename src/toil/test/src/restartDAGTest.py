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


import logging
import os
import shutil
import signal

from toil.common import Toil
from toil.job import Job
from toil.exceptions import FailedJobsException
from toil.test import ToilTest, slow

logger = logging.getLogger(__name__)


class RestartDAGTest(ToilTest):
    """
    Tests that restarted job DAGs don't run children of jobs that failed in the first run till the
    parent completes successfully in the restart.
    """
    def setUp(self):
        super().setUp()
        self.tempDir = self._createTempDir(purpose='tempDir')
        self.testJobStore = self._getTestJobStorePath()

    def tearDown(self):
        super().tearDown()
        shutil.rmtree(self.testJobStore)

    @slow
    def testRestartedWorkflowSchedulesCorrectJobsOnFailedParent(self):
        self._testRestartedWorkflowSchedulesCorrectJobs('raise')

    @slow
    def testRestartedWorkflowSchedulesCorrectJobsOnKilledParent(self):
        self._testRestartedWorkflowSchedulesCorrectJobs('kill')

    def _testRestartedWorkflowSchedulesCorrectJobs(self, failType):
        """
        Creates a diamond DAG
            /->passingParent-\
        root                 |-->child
           \\->failingParent--/

        where root and passingParent are guaranteed to pass, while failingParent will fail.
        child should not run on start or restart and we assert that by ensuring that a file create
        iff child is run is not present on the system.

        :param str failType: Does failingParent fail on an assertionError, or is it killed.
        """
        # Specify options
        options = Job.Runner.getDefaultOptions(self.testJobStore)
        options.logLevel = 'DEBUG'
        options.retryCount = 0
        options.clean = "never"

        parentFile = os.path.join(self.tempDir, 'parent')
        childFile = os.path.join(self.tempDir, 'child')

        # Make the first job
        root = Job.wrapJobFn(passingFn)
        passingParent = Job.wrapJobFn(passingFn)
        failingParent = Job.wrapJobFn(failingFn, failType=failType, fileName=parentFile)
        child = Job.wrapJobFn(passingFn, fileName=childFile)

        # define the DAG
        root.addChild(passingParent)
        root.addChild(failingParent)
        passingParent.addChild(child)
        failingParent.addChild(child)

        failReasons = []

        assert not os.path.exists(childFile)

        # Run the test
        for runMode in 'start', 'restart':
            self.errorRaised = None
            try:
                with Toil(options) as toil:
                    if runMode == 'start':
                        toil.start(root)
                    else:
                        toil.restart()
            except Exception as e:
                logger.exception(e)
                self.errorRaised = e
            finally:
                # The processing of an AssertionError and FailedJobsException is the same so we do
                # it together in this finally clause.
                if self.errorRaised is not None:
                    if not os.path.exists(parentFile):
                        failReasons.append('The failing parent file did not exist on toil "%s".'
                                           % runMode)
                    if os.path.exists(childFile):
                        failReasons.append('The child file existed.  i.e. the child was run on '
                                           'toil "%s".' % runMode)
                    if isinstance(self.errorRaised, FailedJobsException):
                        if self.errorRaised.numberOfFailedJobs != 3:
                            failReasons.append('FailedJobsException was raised on toil "%s" but '
                                               'the number of failed jobs (%s) was not 3.'
                                               % (runMode, self.errorRaised.numberOfFailedJobs))
                    elif isinstance(self.errorRaised, AssertionError):
                        failReasons.append('Toil raised an AssertionError instead of a '
                                           'FailedJobsException on toil "%s".' % runMode)
                    else:
                        failReasons.append("Toil raised error: %s" % self.errorRaised)
                    self.errorRaised = None
                    options.restart = True
                else:
                    self.fail('No errors were raised on toil "%s".' % runMode)
        if failReasons:
            self.fail('Test failed for ({}) reasons:\n\t{}'.format(len(failReasons),
                                                               '\n\t'.join(failReasons)))

def passingFn(job, fileName=None):
    """
    This function is guaranteed to pass as it does nothing out of the ordinary.  If fileName is
    provided, it will be created.

    :param str fileName: The name of a file that must be created if provided.
    """
    if fileName is not None:
        # Emulates system touch.
        open(fileName, 'w').close()

def failingFn(job, failType, fileName):
    """
    This function is guaranteed to fail via a raised assertion, or an os.kill

    :param job: Job
    :param str failType: 'raise' or 'kill
    :param str fileName: The name of a file that must be created.
    """
    assert failType in ('raise', 'kill')
    # Use that function to avoid code redundancy
    passingFn(job, fileName)

    if failType == 'raise':
        assert False
    else:
        os.kill(os.getpid(), signal.SIGKILL)
