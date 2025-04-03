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
from pathlib import Path
import signal
from typing import Optional

from toil.common import Toil
from toil.exceptions import FailedJobsException
from toil.job import Job
from toil.test import pslow as slow

import pytest

logger = logging.getLogger(__name__)


class TestRestartDAG:
    """
    Tests that restarted job DAGs don't run children of jobs that failed in the first run till the
    parent completes successfully in the restart.
    """

    @slow
    @pytest.mark.slow
    def testRestartedWorkflowSchedulesCorrectJobsOnFailedParent(
        self, tmp_path: Path
    ) -> None:
        self._testRestartedWorkflowSchedulesCorrectJobs(tmp_path, "raise")

    @slow
    @pytest.mark.slow
    def testRestartedWorkflowSchedulesCorrectJobsOnKilledParent(
        self, tmp_path: Path
    ) -> None:
        self._testRestartedWorkflowSchedulesCorrectJobs(tmp_path, "kill")

    def _testRestartedWorkflowSchedulesCorrectJobs(
        self, tmp_path: Path, failType: str
    ) -> None:
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
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.logLevel = "DEBUG"
        options.retryCount = 0
        options.clean = "never"

        parentFile = tmp_path / "parent"
        childFile = tmp_path / "child"

        # Make the first job
        root = Job.wrapJobFn(passingFn)
        passingParent = Job.wrapJobFn(passingFn)
        failingParent = Job.wrapJobFn(failingFn, failType=failType, file=parentFile)
        child = Job.wrapJobFn(passingFn, file=childFile)

        # define the DAG
        root.addChild(passingParent)
        root.addChild(failingParent)
        passingParent.addChild(child)
        failingParent.addChild(child)

        failReasons = []

        assert not childFile.exists()

        errorRaised: Optional[BaseException] = None
        # Run the test
        for runMode in "start", "restart":
            errorRaised = None
            try:
                with Toil(options) as toil:
                    if runMode == "start":
                        toil.start(root)
                    else:
                        toil.restart()
            except Exception as e:
                logger.exception(e)
                errorRaised = e
            finally:
                # The processing of an AssertionError and FailedJobsException is the same so we do
                # it together in this finally clause.
                if errorRaised is not None:
                    if not parentFile.exists():
                        failReasons.append(
                            'The failing parent file did not exist on toil "%s".'
                            % runMode
                        )
                    if childFile.exists():
                        failReasons.append(
                            "The child file existed.  i.e. the child was run on "
                            'toil "%s".' % runMode
                        )
                    if isinstance(errorRaised, FailedJobsException):
                        if errorRaised.numberOfFailedJobs != 3:
                            failReasons.append(
                                'FailedJobsException was raised on toil "%s" but '
                                "the number of failed jobs (%s) was not 3."
                                % (runMode, errorRaised.numberOfFailedJobs)
                            )
                    elif isinstance(errorRaised, AssertionError):
                        failReasons.append(
                            "Toil raised an AssertionError instead of a "
                            'FailedJobsException on toil "%s".' % runMode
                        )
                    else:
                        failReasons.append("Toil raised error: %s" % errorRaised)
                    errorRaised = None
                    options.restart = True
                else:
                    pytest.fail('No errors were raised on toil "%s".' % runMode)
        if failReasons:
            pytest.fail(
                "Test failed for ({}) reasons:\n\t{}".format(
                    len(failReasons), "\n\t".join(failReasons)
                )
            )


def passingFn(job: Job, file: Optional[Path] = None) -> None:
    """
    This function is guaranteed to pass as it does nothing out of the ordinary.

    If "file" is provided, it will be created.

    :param file: The path of a file that must be created if provided.
    """
    if file is not None:
        # Emulates system touch.
        file.open("w").close()


def failingFn(job: Job, failType: str, file: Path) -> None:
    """
    This function is guaranteed to fail via a raised assertion, or an os.kill

    :param job: Job
    :param failType: 'raise' or 'kill
    :param file: The path of a file that must be created.
    """
    assert failType in ("raise", "kill")
    # Use that function to avoid code redundancy
    passingFn(job, file)

    if failType == "raise":
        assert False
    else:
        os.kill(os.getpid(), signal.SIGKILL)
