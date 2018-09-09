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
import unittest
import os
import sys
import time
import shutil

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from toil import subprocess
from toil.test import ToilTest, needs_cwl

class ToilKillTest(ToilTest):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        """Shared test variables."""
        self.cwl = os.path.abspath('ABCWorkflowDebug/sleep.cwl')
        self.yaml = os.path.abspath('ABCWorkflowDebug/sleep.yaml')

    def tearDown(self):
        """Default tearDown for unittest."""
        unittest.TestCase.tearDown(self)

    @needs_cwl
    def testCWLToilKill(self):
        """Test "toil kill" on a CWL workflow with a 100 second sleep."""
        jobstore = os.path.join(os.getcwd(), 'testkill')

        run_cmd = ['toil-cwl-runner', '--jobStore', jobstore, self.cwl, self.yaml]
        kill_cmd = ['toil', 'kill', jobstore]

        subprocess.Popen(run_cmd)
        time.sleep(2)
        p = subprocess.Popen(kill_cmd, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        assert 'successfully terminated' in stderr, stderr

        # clean up
        if os.path.exists(jobstore):
            shutil.rmtree(jobstore)
        if os.path.exists('tmp'):
            shutil.rmtree('tmp')


if __name__ == "__main__":
    unittest.main()  # run all tests
