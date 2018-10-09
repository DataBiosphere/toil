from __future__ import absolute_import
import unittest
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from toil import subprocess
from toil.test import ToilTest

class ToilWdlIntegrationTest(ToilTest):
    # a test for promises2.py

    def setUp(self):
        self.program = os.path.abspath("scripts/promises2.py")

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def testExitCode(self):
        out = subprocess.call(["python", self.program, "file:my-jobstore", "--clean=always"])
        assert out == 0, out

    def testOutput(self):
         out = subprocess.check_output(['python', self.program, 'file:my-jobstore', '--clean=always'])
         expectedOut = "['00000', '00001', '00010', '00011', '00100', '00101', '00110', '00111', '01000'," \
                      " '01001', '01010', '01011', '01100', '01101', '01110', '01111', '10000', '10001', " \
                      "'10010', '10011', '10100', '10101', '10110', '10111', '11000', '11001', '11010', " \
                      "'11011', '11100', '11101', '11110', '11111']"
         p = out.find(expectedOut)
         assert p > -1, p

if __name__ == "__main__":
    unittest.main()  # run all tests


