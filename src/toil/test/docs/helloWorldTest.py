from __future__ import absolute_import
import unittest
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from toil import subprocess
from toil.test import ToilTest
# import toil.test.docs.scripts.helloWorld

class ToilWdlIntegrationTest(ToilTest):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        self.program = os.path.abspath("scripts/helloWorld.py")
        pass

    def tearDown(self):
        pass

        unittest.TestCase.tearDown(self)

    def testOutput(self):
        out = subprocess.check_output(['python', self.program, 'file:my-jobstore', '--clean=always'])
        expectedOut = "Hello, world!, here's a message: You did it!"
        assert out.strip() == expectedOut, out

if __name__ == "__main__":
    unittest.main()  # run all tests


