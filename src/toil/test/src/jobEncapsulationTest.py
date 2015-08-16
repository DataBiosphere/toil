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
        outFile = getTempFile(rootDir=os.getcwd())
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
            options = T.Runner.getDefaultOptions()
            options.jobStore = self._getTestJobStorePath()
            options.logLevel = "INFO"
            # Run the workflow, the return value being the number of failed jobs
            self.assertEquals(T.Runner.startToil(a, options), 0)
            # Check output
            self.assertEquals(open(outFile, 'r').readline(), "ABCDE")
        finally:
            os.remove(outFile)
