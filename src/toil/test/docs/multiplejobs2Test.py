from __future__ import absolute_import
import unittest
import os
import re
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from toil import subprocess
from toil.test import ToilTest

class ToilWdlIntegrationTest(ToilTest):
    # a test for multiplejobs2.py

    def setUp(self):
        self.program = os.path.abspath("scripts/multiplejobs2.py")

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def testtutorial01(self):
        self.exitCode(p='scripts/multiplejobs2.py')

    def testtutorial02(self):
        self.exitCode(p='scripts/multiplejobs1.py')

    def exitCode(self, p):
        out = subprocess.call(["python", p, "file:my-jobstore", "--clean=always"])
        assert out == 0, out

    def testExitCode(self):
        out = subprocess.call(["python", self.program, "file:my-jobstore", "--clean=always"])
        assert out == 0, out

    def testOutput(self):
        out = subprocess.check_output(['python', self.program, 'file:my-jobstore', '--clean=always'])
        pattern = re.compile("Hello world, I have a message: first.*Hello world, I have a message: second or third.*"
                             "Hello world, I have a message: second or third.*Hello world, I have a message: last",
                             re.DOTALL)

        p = re.search(pattern, out)
        assert p != None, p

if __name__ == "__main__":
    unittest.main()  # run all tests


