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
import os
import unittest

from toil.lib.bioio import getTempFile
from toil.job import Job
from toil.test import ToilTest

class JobServiceTest(ToilTest):
    """
    Tests testing the Job.Service class
    """
    def testService(self):
        """
        Tests the creation of a Job.Service.
        """
        # Temporary file
        outFile = getTempFile(rootDir=self._createTempDir())
        try:
            # Wire up the services/jobs
            t = Job.wrapFn(f, "1", outFile)
            t.addChildFn(f, t.addService(TestService("2", "3", outFile)), outFile)
            # Create the runner for the workflow.
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "INFO"
            # Run the workflow, the return value being the number of failed jobs
            Job.Runner.startToil(t, options)
            # Check output
            self.assertEquals(open(outFile, 'r').readline(), "123")
        finally:
            os.remove(outFile)

class TestService(Job.Service):
    def __init__(self, startString, stopString, outFile):
        Job.Service.__init__(self)
        self.startString = startString
        self.stopString = stopString
        self.outFile = outFile

    def start(self):
        return self.startString

    def stop(self):
        f(self.stopString, self.outFile)


def f(string, outFile):
    """
    Function appends string to output file
    """
    with open(outFile, 'a') as fH:
        fH.write(string)
