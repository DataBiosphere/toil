import unittest
import os
import subprocess
import shutil
import uuid
from toil.wdl.wdl_functions import ceil
from toil.wdl.wdl_functions import floor

from toil.version import exactPython
from toil.test import ToilTest


class WdlStandardLibraryFunctionsTest(ToilTest):
    """ A set of test cases for toil's wdl functions."""
    @classmethod
    def setUpClass(cls):
        pass

    def testFn_Ceil(self):
        """Test the wdl built-in functional equivalent of 'ceil()', which converts
        a Float value into an Int by rounding up to the next higher integer"""
        assert ceil(1.999) == 2
        assert ceil(-1.5) == -1

    def testFn_Floor(self):
        """Test the wdl built-in functional equivalent of 'floor()', which converts
        a Float value into an Int by rounding down to the next lower integer"""
        assert floor(1.999) == 1
        assert floor(-1.5) == -2


class WdlStandardLibraryWorkflowsTest(ToilTest):
    """
    A set of test cases for toil's conformance with the WDL built-in standard library:

    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#standard-library

    All tests should include a simple wdl and json file for toil to run that checks the output.
    """
    @classmethod
    def setUpClass(cls):
        cls.program = os.path.abspath("src/toil/wdl/toilwdl.py")

    def check_function(self, function_name, expected_result):
        wdl_files = [os.path.abspath(f'src/toil/test/wdl/standard_library/{function_name}_as_input.wdl'),
                     os.path.abspath(f'src/toil/test/wdl/standard_library/{function_name}_as_command.wdl')]
        json_file = os.path.abspath(f'src/toil/test/wdl/standard_library/{function_name}.json')
        for wdl_file in wdl_files:
            with self.subTest(f'Testing: {wdl_file} {json_file}'):
                output_dir = f'/tmp/toil-wdl-test-{uuid.uuid4()}'
                os.makedirs(output_dir)
                subprocess.check_call([exactPython, self.program, wdl_file, json_file, '-o', output_dir])
                output = os.path.join(output_dir, 'output.txt')
                with open(output, 'r') as f:
                    result = f.read().strip()
                self.assertEqual(result, expected_result)
                shutil.rmtree(output_dir)

    def test_ceil(self):
        self.check_function('ceil', expected_result='12')

    def test_floor(self):
        self.check_function('floor', expected_result='11')

    def test_round(self):
        self.check_function('round', expected_result='11')


if __name__ == "__main__":
    unittest.main()
