# Copyright (C) 2015-2022 Regents of the University of California
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
import subprocess
import sys
import time
import unittest
from pathlib import Path

from toil.common import Toil
from toil.jobStores.abstractJobStore import NoSuchFileException, NoSuchJobStoreException
from toil.jobStores.utils import generate_locator
from toil.test import get_data, needs_aws_s3, needs_cwl

logger = logging.getLogger(__name__)

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa


class _ToilKillTest:
    """A set of test cases for "toil kill"."""

    def _test_cwl_toil_kill(self, job_store: str) -> None:
        """Test "toil kill" on a CWL workflow with a 100 second sleep."""

        with get_data("test/utils/ABCWorkflowDebug/sleep.cwl") as cwl_file:
            with get_data("test/utils/ABCWorkflowDebug/sleep.yaml") as input_file:
                run_cmd = [
                    "toil-cwl-runner",
                    "--jobStore",
                    job_store,
                    str(cwl_file),
                    str(input_file),
                ]
                kill_cmd = ["toil", "kill", job_store]
                clean_cmd = ["toil", "clean", job_store]

                try:
                    # run the sleep workflow
                    logger.info("Running workflow: %s", " ".join(run_cmd))
                    cwl_process = subprocess.Popen(run_cmd)

                    # wait until workflow starts running
                    while True:
                        assert (
                            cwl_process.poll() is None
                        ), "toil-cwl-runner finished too soon"
                        try:
                            job_store_real = Toil.resumeJobStore(job_store)
                            job_store_real.read_leader_pid()
                            # pid file exists, now wait for the kill flag to exist
                            if not job_store_real.read_kill_flag():
                                # kill flag exists to be deleted to kill the leader
                                break
                            else:
                                logger.info("Waiting for kill flag...")
                        except (NoSuchJobStoreException, NoSuchFileException):
                            logger.info("Waiting for job store to be openable...")
                        time.sleep(2)

                    # run toil kill
                    subprocess.check_call(kill_cmd)

                    # after toil kill succeeds, the workflow should've exited
                    assert cwl_process.poll() is None
                finally:
                    # Clean up the job store since the workflow won't do it
                    # since it got killed.
                    subprocess.check_call(clean_cmd)


class TestToilKill(_ToilKillTest):
    """A set of test cases for "toil kill"."""

    @needs_cwl
    def test_cwl_toil_kill(self, tmp_path: Path) -> None:
        """Test "toil kill" on a CWL workflow with a 100 second sleep."""
        self._test_cwl_toil_kill(str(tmp_path / "job_store"))


@needs_aws_s3
class TestToilKillWithAWSJobStore(_ToilKillTest):
    """A set of test cases for "toil kill" using the AWS job store."""

    @needs_cwl
    @needs_aws_s3
    def test_cwl_toil_kill(self) -> None:
        """Test "toil kill" on a CWL workflow with a 100 second sleep."""
        self._test_cwl_toil_kill(generate_locator("aws", decoration="testkill"))


if __name__ == "__main__":
    unittest.main()  # run all tests
