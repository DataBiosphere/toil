import json
import unittest
import os
import subprocess
import shutil
import uuid
from toil.wdl.wdl_functions import ceil
from toil.wdl.wdl_functions import floor
from toil.wdl.wdl_functions import read_lines
from toil.wdl.wdl_functions import read_tsv
from toil.wdl.wdl_functions import read_json
from toil.wdl.wdl_functions import read_map
from toil.wdl.wdl_functions import read_int
from toil.wdl.wdl_functions import read_string
from toil.wdl.wdl_functions import read_float
from toil.wdl.wdl_functions import read_boolean
from toil.wdl.wdl_functions import write_lines
from toil.wdl.wdl_functions import write_tsv
from toil.wdl.wdl_functions import write_json
from toil.wdl.wdl_functions import write_map

from toil.version import exactPython
from toil.test import ToilTest


class WdlStandardLibraryFunctionsTest(ToilTest):
    """ A set of test cases for toil's wdl functions."""
    def setUp(self):
        """Runs anew before each test to create farm fresh temp dirs."""
        self.output_dir = os.path.join('/tmp/', 'toil-wdl-test-' + str(uuid.uuid4()))
        os.makedirs(self.output_dir)
        os.makedirs(os.path.join(self.output_dir, 'execution'))

    @classmethod
    def setUpClass(cls):
        pass

    def tearDown(self):
        """Clean up outputs."""
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)

    def _check_output(self, path, expected_result, strip=True):
        """ Compare expected_result to content from file."""
        with open(path, 'r') as f:
            result = f.read()
        if strip:
            result = result.strip()
        self.assertEqual(expected_result, result)

    def _write_temp_file(self, function_name, content):
        """ Write content to a temp file."""
        path = os.path.join(self.output_dir, f'{function_name}_{uuid.uuid4()}.tmp')
        with open(path, 'w') as f:
            f.write(content + '\n')
        return path

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

    def testFn_ReadLines(self):
        """Test the wdl built-in functional equivalent of 'read_lines()'."""
        # trailing newlines are stripped; spaces are kept
        lines = 'line 1\nline 2\t\t\n \n\n'
        path = self._write_temp_file('read_lines', lines)
        self.assertEqual(['line 1', 'line 2\t\t', ' '], read_lines(path))

        # preceding newlines are kept
        lines = '\n\n\nline 1\nline 2\t\t\n '
        path = self._write_temp_file('read_lines', lines)
        self.assertEqual(['', '', '', 'line 1', 'line 2\t\t', ' '], read_lines(path))

    def testFn_ReadTsv(self):
        """Test the wdl built-in functional equivalent of 'read_tsv()'."""
        tsv = [['1', '2', '3'], ['4', '5', '6'], ['7', '8', '9']]
        tsv_str = '1\t2\t3\n4\t5\t6\n7\t8\t9'

        path = self._write_temp_file('read_tsv', tsv_str)
        self.assertEqual(tsv, read_tsv(path))

    def testFn_ReadJson(self):
        """Test the wdl built-in functional equivalent of 'read_json()'."""
        json_obj = {'str': 'some string', 'num': 3.14, 'bool': True, 'null': None, 'arr': ['test']}
        json_arr = ['1', '2']
        json_num = 3.14

        path = self._write_temp_file('read_json', json.dumps(json_obj))
        self.assertEqual(json_obj, read_json(path))

        path = self._write_temp_file('read_json', json.dumps(json_arr))
        self.assertEqual(json_arr, read_json(path))

        path = self._write_temp_file('read_json', json.dumps(json_num))
        self.assertEqual(json_num, read_json(path))

    def testFn_ReadMap(self):
        """Test the wdl built-in functional equivalent of 'read_map()'."""
        map_str = 'key1\tvalue1\nkey2\tvalue2'
        path = self._write_temp_file('read_map', map_str)
        self.assertEqual({'key1': 'value1', 'key2': 'value2'}, read_map(path))

        # extra lines and spaces are stripped, except spaces in keys are kept.
        map_str = '\n\n\nkey1   \tvalue1\nkey2\tvalue2   \n   \n                \t  \n'
        path = self._write_temp_file('read_map', map_str)
        self.assertEqual({'key1   ': 'value1', 'key2': 'value2'}, read_map(path))

    def testFn_ReadInt(self):
        """Test the wdl built-in functional equivalent of 'read_int()'."""
        num = 10
        path = self._write_temp_file('read_int', content=str(num))
        self.assertEqual(num, read_int(path))

        num = 10.0
        path = self._write_temp_file('read_int', content=str(num))
        self.assertRaises(ValueError, read_int, path)

        num = 10.5
        path = self._write_temp_file('read_int', content=str(num))
        self.assertRaises(ValueError, read_int, path)

    def testFn_ReadString(self):
        """Test the wdl built-in functional equivalent of 'read_string()'."""
        some_str = 'some string'
        path = self._write_temp_file('read_string', content=some_str)
        self.assertEqual(some_str, read_string(path))

        # with preceding newlines. Cromwell strips from the front and the end.
        path = self._write_temp_file('read_string', content='\n\n\n' + some_str)
        self.assertEqual(some_str, read_string(path))

        # with trailing newlines
        path = self._write_temp_file('read_string', content=some_str + '\n\n')
        self.assertEqual(some_str, read_string(path))

    def testFn_ReadFloat(self):
        """Test the wdl built-in functional equivalent of 'read_float()'."""
        num = 2.718281828459045
        path = self._write_temp_file('read_float', content=str(num))
        self.assertEqual(num, read_float(path))

    def testFn_ReadBoolean(self):
        """Test the wdl built-in functional equivalent of 'read_boolean()'."""
        for val in (True, False):
            path = self._write_temp_file('read_boolean', content=str(val))
            self.assertEqual(val, read_boolean(path))

            # upper
            path = self._write_temp_file('read_boolean', content=str(val).upper())
            self.assertEqual(val, read_boolean(path))

            # lower
            path = self._write_temp_file('read_boolean', content=str(val).lower())
            self.assertEqual(val, read_boolean(path))

    def testFn_WriteLines(self):
        """Test the wdl built-in functional equivalent of 'write_lines()'."""
        # 'line 1'          \n
        # 'line 2\t\t'      \n
        # ' '               \n
        # '\n'              \n
        path = write_lines(['line 1', 'line 2\t\t', ' ', '\n'], temp_dir=self.output_dir)
        self._check_output(path, 'line 1\nline 2\t\t\n \n\n\n', strip=False)

    def testFn_WriteTsv(self):
        """Test the wdl built-in functional equivalent of 'write_tsv()'."""
        path = write_tsv([['1', '2', '3'], ['4', '5', '6'], ['7', '8', '9']], temp_dir=self.output_dir)
        self._check_output(path, '1\t2\t3\n4\t5\t6\n7\t8\t9')

    def testFn_WriteJson(self):
        """Test the wdl built-in functional equivalent of 'write_json()'."""
        json_obj = {'str': 'some string', 'num': 3.14, 'bool': True, 'null': None, 'arr': ['test']}
        json_arr = ['1', '2']
        json_num = 3.14
        json_str = 'test string'
        json_bool = False
        json_null = None

        path = write_json(json_obj, temp_dir=self.output_dir)
        self._check_output(path, '{"str":"some string","num":3.14,"bool":true,"null":null,"arr":["test"]}')

        path = write_json(json_arr, temp_dir=self.output_dir)
        self._check_output(path, '["1","2"]')

        path = write_json(json_num, temp_dir=self.output_dir)
        self._check_output(path, '3.14')

        path = write_json(json_str, temp_dir=self.output_dir)
        self._check_output(path, '"test string"')

        path = write_json(json_bool, temp_dir=self.output_dir)
        self._check_output(path, 'false')

        path = write_json(json_null, temp_dir=self.output_dir)
        self._check_output(path, 'null')

    def testFn_WriteMap(self):
        """Test the wdl built-in functional equivalent of 'write_map()'."""
        path = write_map({'key1': 'value1', 'key2': 'value2'}, temp_dir=self.output_dir)
        self._check_output(path, 'key1\tvalue1\nkey2\tvalue2')


class WdlStandardLibraryWorkflowsTest(ToilTest):
    """
    A set of test cases for toil's conformance with the WDL built-in standard library:

    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#standard-library

    All tests should include a simple wdl and json file for toil to run that checks the output.
    """
    @classmethod
    def setUpClass(cls):
        cls.program = os.path.abspath("src/toil/wdl/toilwdl.py")

    def check_function(self, function_name, cases, expected_result):
        wdl_files = [os.path.abspath(f'src/toil/test/wdl/standard_library/{function_name}_{case}.wdl') for case in cases]
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
        self.check_function('ceil', cases=['as_input', 'as_command'], expected_result='12')

    def test_floor(self):
        self.check_function('floor', cases=['as_input', 'as_command'], expected_result='11')

    def test_round(self):
        self.check_function('round', cases=['as_input', 'as_command'], expected_result='11')

    def test_stdout(self):
        self.check_function('stdout', cases=['as_output'], expected_result='A Whale of a Tale.')
        self.check_function('stderr', cases=['as_output'], expected_result='a journey straight to stderr')

    def test_read(self):
        """ Test the set of WDL read functions."""
        # NOTE: these tests depends on stdout() and the write_*() functions.

        self.check_function('read_lines', cases=['as_output'],
                            expected_result='line 1\n\t\tline 2 with tabs\n line 3\n\nline 5')

        self.check_function('read_tsv', cases=['as_output'],
                            expected_result='1\t2\t3\n4\t5\t6\n7\t8\t9')

        self.check_function('read_json', cases=['as_output'],
                            expected_result='{"key1":"value1","key2":"value2"}')

        self.check_function('read_map', cases=['as_output'],
                            expected_result='key1\tvalue1\nkey2\tvalue2')

        # primitives
        self.check_function('read_int', cases=['as_command'], expected_result='11')
        self.check_function('read_string', cases=['as_command'], expected_result='A Whale of a Tale.')
        self.check_function('read_float', cases=['as_command'], expected_result='11.2345')
        self.check_function('read_boolean', cases=['as_command'], expected_result='True')

    def test_write(self):
        """ Test the set of WDL write functions."""
        self.check_function('write_lines', cases=['as_command'],
                            expected_result='first\nsecond\nthird')

        self.check_function('write_tsv', cases=['as_command'],
                            expected_result='one\ttwo\tthree\nun\tdeux\ttrois')

        self.check_function('write_json', cases=['as_command'],
                            expected_result='{"key1":"value1","key2":"value2"}')

        self.check_function('write_map', cases=['as_command'],
                            expected_result='key1\tvalue1\nkey2\tvalue2')


if __name__ == "__main__":
    unittest.main()
