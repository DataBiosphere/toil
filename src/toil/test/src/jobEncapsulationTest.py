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

from toil.lib.bioio import getTempFile
from toil.job import Job as T
from toil.test import ToilTest
from toil.test.src.jobTest import f


class JobEncapsulationTest(ToilTest):
    """
    Tests testing the EncapsulationJob class
    """

    def testEncapsulation(self):
        """
        Tests the Job.encapsulation method, which uses the EncapsulationJob
        class.
        """
        # Temporary file
        outFile = getTempFile(rootDir=self._createTempDir())
        try:
            # Make a job graph
            a = T.wrapFn(f, "A", outFile)
            b = a.addChildFn(f, a.rv(), outFile)
            c = a.addFollowOnFn(f, b.rv(), outFile)
            # Encapsulate it
            a = a.encapsulate()
            # Now add children/follow to the encapsulated graph
            d = T.wrapFn(f, c.rv(), outFile)
            e = T.wrapFn(f, d.rv(), outFile)
            a.addChild(d)
            a.addFollowOn(e)
            # Create the runner for the workflow.
            options = T.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "INFO"
            # Run the workflow, the return value being the number of failed jobs
            T.Runner.startToil(a, options)
            # Check output
            self.assertEquals(open(outFile, 'r').readline(), "ABCDE")
        finally:
            os.remove(outFile)
