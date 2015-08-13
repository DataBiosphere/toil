import os
import unittest

from toil.lib.bioio import getTempFile
from toil.job import Job
from toil.test import ToilTest


class JobServiceTest(ToilTest):
    """
    Tests testing the Job.Service class
    """
    @unittest.expectedFailure
    def testService(self):
        """
        Tests the creation of a Job.Service.
        """
        # Temporary file
        outFile = getTempFile(rootDir=os.getcwd())
        try:
            # Wire up the services/jobs
            t = Job.wrapFn(f, "1", outFile)
            t.addChildFn(f, t.addService(TestService("2", "3", outFile)), outFile)
            # Create the runner for the workflow.
            options = Job.Runner.getDefaultOptions()
            options.logLevel = "INFO"
            options.toil = self._getTestJobStorePath()
            # Run the workflow, the return value being the number of failed jobs
            self.assertEquals(Job.Runner.startToil(t, options), 0)
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
