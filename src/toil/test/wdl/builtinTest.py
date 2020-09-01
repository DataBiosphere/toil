import unittest
import os
import subprocess
import shutil
import uuid

from toil.version import exactPython
from toil.test import ToilTest


class WdlStandardLibraryWorkflowsTest(ToilTest):
    """
    A set of test cases for toil's conformance with the WDL built-in standard library:

    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#standard-library

    All tests should include a simple wdl and json file for toil to run that checks the output.
    """
    def setUp(self):
        """Runs anew before each test to create farm fresh temp dirs."""
        self.output_dir = os.path.join('/tmp/', 'toil-wdl-test-' + str(uuid.uuid4()))
        os.makedirs(self.output_dir)

    @classmethod
    def setUpClass(cls):
        cls.program = os.path.abspath("src/toil/wdl/toilwdl.py")

    def tearDown(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        unittest.TestCase.tearDown(self)

    def test_ceil(self):
        wdl_file = os.path.abspath('src/toil/test/wdl/standard_library/ceil.wdl')
        json_file = os.path.abspath('src/toil/test/wdl/standard_library/ceil.json')
        subprocess.check_call([exactPython, self.program, wdl_file, json_file, '-o', self.output_dir])
        output = os.path.join(self.output_dir, 'the_ceiling.txt')
        with open(output, 'r') as f:
            assert float(f.read()) == 12


if __name__ == "__main__":
    unittest.main()
