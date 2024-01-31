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
import os

from toil.job import Job
from toil.test import ToilTest, get_temp_file
from toil.test.src.jobTest import fn1Test


class JobEncapsulationTest(ToilTest):
    """Tests testing the EncapsulationJob class."""
    def testEncapsulation(self):
        """
        Tests the Job.encapsulation method, which uses the EncapsulationJob
        class.
        """
        # Temporary file
        outFile = get_temp_file(rootDir=self._createTempDir())
        try:
            # Encapsulate a job graph
            a = Job.wrapJobFn(encapsulatedJobFn, "A", outFile, name="a")
            a = a.encapsulate(name="a-encap")
            # Now add children/follow to the encapsulated graph
            d = Job.wrapFn(fn1Test, a.rv(), outFile, name="d")
            e = Job.wrapFn(fn1Test, d.rv(), outFile, name="e")
            a.addChild(d)
            a.addFollowOn(e)
            # Create the runner for the workflow.
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "INFO"
            # Run the workflow, the return value being the number of failed jobs
            Job.Runner.startToil(a, options)
            # Check output
            self.assertEqual(open(outFile).readline(), "ABCDE")
        finally:
            os.remove(outFile)

    def testAddChildEncapsulate(self):
        """
        Make sure that the encapsulate child does not have two parents
        with unique roots.
        """
        # Temporary file
        a = Job.wrapFn(noOp)
        b = Job.wrapFn(noOp)
        a.addChild(b).encapsulate()
        self.assertEqual(len(a.getRootJobs()), 1)


def noOp():
    pass

def encapsulatedJobFn(job, string, outFile):
    a = job.addChildFn(fn1Test, string, outFile, name="inner-a")
    b = a.addFollowOnFn(fn1Test, a.rv(), outFile, name="inner-b")
    return b.rv()
