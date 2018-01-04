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
# from builtins import str

import os
import unittest
import sys
import uuid
import shutil
import subprocess
import tempfile

import pytest

import toil
import logging
import toil.test.sort.sort
from toil import resolveEntryPoint
from toil.job import Job
from toil.lib.bioio import getTempFile, system
from toil.test import ToilTest, needs_aws, needs_rsync3, integrative, slow
from toil.test.sort.sortTest import makeFileToSort
from toil.utils.toilStats import getStats, processData
from toil.common import Toil, Config

class ToilStatusTest(ToilTest):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        """
        Initial set up of variables for the test.
        """

        # self.program = os.path.abspath("src/toil/wdl/toilwdl.py")
        #
        # self.test_directory = os.path.abspath("src/toil/test/wdl/")
        # self.output_dir = self._createTempDir(purpose='tempDir')
        #
        # self.encode_data = os.path.join(self.test_directory, "ENCODE_data.zip")
        # self.encode_data_dir = os.path.join(self.test_directory, "ENCODE_data")

    def tearDown(self):
        """Default tearDown for unittest."""

        unittest.TestCase.tearDown(self)

    # estimated run time
    @slow
    def testPrintDot(self):
        pass

    # estimated run time
    @slow
    def testPerJobStats(self):
        pass

    # estimated run time
    @slow
    def testStub(self):
        pass