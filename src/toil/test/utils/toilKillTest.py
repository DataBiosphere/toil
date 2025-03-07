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
import shutil
import subprocess
import sys
import time
import unittest

from toil.common import Toil
from toil.jobStores.abstractJobStore import NoSuchFileException, NoSuchJobStoreException
from toil.jobStores.utils import generate_locator
from toil.test import ToilTest, get_data, needs_aws_s3, needs_cwl

logger = logging.getLogger(__name__)

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa


class ToilKillTest(ToilTest):
    """A set of test cases for "toil kill"."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_store = os.path.join(os.getcwd(), "testkill")

    def setUp(self):
        """Shared test variables."""
        self.cwl = get_data("test/utils/ABCWorkflowDebug/sleep.cwl")
        self.yaml = get_data("test/utils/ABCWorkflowDebug/sleep.yaml")

    def tearDown(self):
        """Default tearDown for unittest."""
        cmd = ["toil", "clean", self.job_store]
        subprocess.check_call(cmd)

        if os.path.exists("tmp"):
            shutil.rmtree("tmp")
        unittest.TestCase.tearDown(self)

    @needs_cwl
    def test_cwl_toil_kill(self):
        """Test "toil kill" on a CWL workflow with a 100 second sleep."""

        run_cmd = ["toil-cwl-runner", "--jobStore", self.job_store, self.cwl, self.yaml]
        kill_cmd = ["toil", "kill", self.job_store]

        # run the sleep workflow
        logger.info("Running workflow: %s", " ".join(run_cmd))
        cwl_process = subprocess.Popen(run_cmd)

        # wait until workflow starts running
        while True:
            assert cwl_process.poll() is None, "toil-cwl-runner finished too soon"
            try:
                job_store = Toil.resumeJobStore(self.job_store)
                job_store.read_leader_pid()
                # pid file exists, now wait for the kill flag to exist
                if not job_store.read_kill_flag():
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


@needs_aws_s3
class ToilKillTestWithAWSJobStore(ToilKillTest):
    """A set of test cases for "toil kill" using the AWS job store."""

    def setUp(self):
        super().setUp()
        self.job_store = generate_locator("aws", decoration="testkill")


if __name__ == "__main__":
    unittest.main()  # run all tests
