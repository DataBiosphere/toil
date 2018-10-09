from __future__ import absolute_import
import unittest
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from toil import subprocess
from toil.test import ToilTest

class ToilWdlIntegrationTest(ToilTest):
    # a test for invokeworkflow2.py

    def setUp(self):
        self.program = os.path.abspath("scripts/invokeworkflow2.py")

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def testExitCode(self):
        out = subprocess.call(["python", self.program, "file:my-jobstore", "--clean=always"])
        assert out == 0, out

    def testOutput(self):
        out = subprocess.check_output(['python', self.program, 'file:my-jobstore', '--clean=always'])
        expectedOut = "Hello, world!, I have a message: Woot!\n"

        # Search for the expected output string among all the log messages
        p = out.find(expectedOut)
        assert p > -1, p

if __name__ == "__main__":
    unittest.main()  # run all tests


