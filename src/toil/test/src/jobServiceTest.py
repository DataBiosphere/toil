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
        outFile = getTempFile(rootDir=os.getcwd())
        try:
            # Wire up the services/jobs
            t = Job.wrapFn(f, "1", outFile)

            # This is a temporary fix for https://github.com/BD2KGenomics/toil/issues/268. The
            # current implementation of ServiceJob ignores the fact that a Job.Service subclass
            # in a user script requires that user script to be imported first. This is very
            # similar to how the FunctionWrappingJob first imports the module containing the user
            #  function before referencing it. The pickled job references the module containing
            # the service class, likely causing the unpickling to fail since the referenced
            # module hasn't been loaded yet. The reason this test worked without this temporary
            # fix was that the test runner imports this test module using the fully-qualified
            # package name, i.e. "toil.test.src.jobServiceTest". That package name can be
            # resolved if the top-level src directory is present on the sys.path, which is the
            # case when the test is run from the command line, e.g. on Jenkins. OTOH, when this
            # test is run from PyCharm, the test module is imported from the directory containing
            #  the module, yielding the unqalified module name "jobServiceTest". The directory
            # containing the module is not on the worker's sys.path and the module name can't
            # therefore be resolved. But don't be fooled: the problem is not limited to PyCharm,
            # it will happen in every case where the user script is not part of the Toil source
            # tree or not loaded from the top-level source tree.
            #
            # By aliasing the service class it will be referenced in the pickled job using its
            # fully qualified name which, as mentioned above, *can* be resolved in the worker
            # process.

            from toil.test.src.jobServiceTest import TestService as _TestService


            t.addChildFn(f, t.addService(_TestService("2", "3", outFile)), outFile)
            # Create the runner for the workflow.
            options = Job.Runner.getDefaultOptions()
            options.logLevel = "INFO"
            options.jobStore = self._getTestJobStorePath()
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
