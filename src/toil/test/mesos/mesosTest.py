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

from __future__ import absolute_import
import logging
import tempfile
import shutil
import os
import threading
import sys

from toil.lib.bioio import getLogLevelString, system
from toil.test.mesos.stress import main as stressMain
from toil.test import ToilTest, needs_mesos
from toil.batchSystems.mesos.test import MesosTestSupport
from toil.test.mesos import helloWorld

lock = threading.Lock()
numCores = 2

log = logging.getLogger(__name__)


@needs_mesos
class MesosTest(ToilTest, MesosTestSupport):
    """
    FIXME: What's the point of this? Anything not covered by the generic batch system tests?
    """

    @classmethod
    def setUpClass(cls):
        super(MesosTest, cls).setUpClass()
        shutil.rmtree('/tmp/mesos', ignore_errors=True)

    def setUp(self):
        super(MesosTest, self).setUp()
        self.savedWorkingDir = os.getcwd()
        self.tempDir = tempfile.mkdtemp()
        log.info("Using '%s' for files and directories created by this test run.", self.tempDir)
        os.chdir(self.tempDir)
        self._startMesos(numCores)

    def tearDown(self):
        self._stopMesos()
        os.chdir(self.savedWorkingDir)
        shutil.rmtree(self.tempDir)
        super(MesosTest, self).tearDown()

    def test_hello_world(self):
        system([sys.executable,
                '-m', helloWorld.__name__,
                './toilTest',
                '--batchSystem=mesos',
                '--logLevel', getLogLevelString()])

    def test_stress_good(self):
        stressMain(numJobs=2)
