import json
import unittest
import os
import subprocess
import shutil
import uuid
from typing import Optional, List

from toil.wdl.wdl_functions import sub
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
from toil.wdl.wdl_functions import transpose
from toil.wdl.wdl_functions import length

from toil.wdl.wdl_functions import WDLPair

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

    def testFn_Sub(self):
        """Test the wdl built-in functional equivalent of 'sub()'."""
        # example from the WDL spec.
        chocolike = "I like chocolate when it's late"
        self.assertEqual("I love chocolate when it's late", sub(chocolike, 'like', 'love'))
        self.assertEqual("I like chocoearly when it's early", sub(chocolike, 'late', 'early'))
        self.assertEqual("I like chocolate when it's early", sub(chocolike, 'late$', 'early'))

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

        # Pair[Int, Pair[Int, Pair[Int, Pair[Int, Int]]]]
        json_pairs = WDLPair(1, WDLPair(2, WDLPair(3, WDLPair(4, 5))))

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

        path = write_json(json_pairs, temp_dir=self.output_dir)
        self._check_output(path, '{"left":1,"right":{"left":2,"right":{"left":3,"right":{"left":4,"right":5}}}}')

    def testFn_WriteMap(self):
        """Test the wdl built-in functional equivalent of 'write_map()'."""
        path = write_map({'key1': 'value1', 'key2': 'value2'}, temp_dir=self.output_dir)
        self._check_output(path, 'key1\tvalue1\nkey2\tvalue2')

    def testFn_Transpose(self):
        """Test the wdl built-in functional equivalent of 'transpose()'."""
        self.assertEqual([[0, 3], [1, 4], [2, 5]], transpose([[0, 1, 2], [3, 4, 5]]))
        self.assertEqual([[0, 1, 2], [3, 4, 5]], transpose([[0, 3], [1, 4], [2, 5]]))

        self.assertEqual([], transpose([]))
        self.assertEqual([], transpose([[]]))  # same as Cromwell
        self.assertEqual([[0]], transpose([[0]]))
        self.assertRaises(AssertionError, transpose, [[0, 1, 2], [3, 4, 5, 6]])

    def testFn_Length(self):
        """Test the WDL 'length()' built-in."""
        self.assertEqual(3, length([1, 2, 3]))
        self.assertEqual(3, length(['a', 'b', 'c']))
        self.assertEqual(0, length([]))



class WdlWorkflowsTest(ToilTest):
    """
    A set of test cases for toil's conformance with WDL.

    All tests should include a simple wdl and json file for toil to run that checks the output.
    """

    @classmethod
    def setUpClass(cls):
        super(WdlWorkflowsTest, cls).setUpClass()
        cls.program = os.path.abspath("src/toil/wdl/toilwdl.py")
        cls.test_path = os.path.abspath("src/toil/test/wdl")

    def check_function(self,
                       function_name: str,
                       cases: List[str],
                       json_file_name: Optional[str] = None,
                       expected_result: Optional[str] = None,
                       expected_exception: Optional[str] = None):
        """
        Run the given WDL workflow and check its output. The WDL workflow
        should store its output inside a 'output.txt' file that can be
        compared to `expected_result`.

        If `expected_exception` is set, this test passes only when both the
        workflow fails and that the given `expected_exception` string is
        present in standard error.
        """
        wdl_files = [os.path.abspath(f'{self.test_path}/{function_name}_{case}.wdl')
                     for case in cases]
        json_file = os.path.abspath(f'{self.test_path}/{json_file_name or function_name}.json')
        for wdl_file in wdl_files:
            with self.subTest(f'Testing: {wdl_file} {json_file}'):
                output_dir = f'/tmp/toil-wdl-test-{uuid.uuid4()}'
                os.makedirs(output_dir)

                if expected_exception is not None:
                    with self.assertRaises(subprocess.CalledProcessError) as context:
                        # use check_output() here so that the output is read before return.
                        subprocess.check_output([exactPython, self.program, wdl_file, json_file, '-o', output_dir],
                                                stderr=subprocess.PIPE)

                    stderr = context.exception.stderr
                    self.assertIsInstance(stderr, bytes)
                    self.assertIn(expected_exception, stderr.decode('utf-8'))

                elif expected_result is not None:
                    subprocess.check_call([exactPython, self.program, wdl_file, json_file, '-o', output_dir])
                    output = os.path.join(output_dir, 'output.txt')
                    with open(output, 'r') as f:
                        result = f.read().strip()
                    self.assertEqual(result, expected_result)

                else:
                    self.fail("Invalid test. Either `expected_result` or `expected_exception` must be set.")

                shutil.rmtree(output_dir)


class WdlLanguageSpecWorkflowsTest(WdlWorkflowsTest):
    """
    A set of test cases for toil's conformance with the WDL language specification:

    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#language-specification
    """

    @classmethod
    def setUpClass(cls):
        super(WdlLanguageSpecWorkflowsTest, cls).setUpClass()
        cls.test_path = os.path.abspath("src/toil/test/wdl/wdl_specification")

    def test_type_pair(self):
        # NOTE: these tests depend on read_lines(), write_json(), and select_first().

        expected_result = '[23,"twenty-three","a.bai",{"left":23,"right":"twenty-three"}]'
        self.check_function('type_pair', cases=['basic'], expected_result=expected_result)

        # tests if files from the pair type are correctly imported.
        # the array of three arrays consists content from:
        # 1. src/toil/test/wdl/testfiles/test_string.txt        -> 'A Whale of a Tale.'
        # 2. src/toil/test/wdl/testfiles/test_boolean.txt       -> 'true'
        # 3. src/toil/test/wdl/testfiles/test_int.txt           -> '11'
        expected_result = '[["A Whale of a Tale."],["true"],["11"]]'
        self.check_function('type_pair', cases=['with_files'], expected_result=expected_result)


class WdlStandardLibraryWorkflowsTest(WdlWorkflowsTest):
    """
    A set of test cases for toil's conformance with the WDL built-in standard library:

    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#standard-library
    """

    @classmethod
    def setUpClass(cls):
        super(WdlStandardLibraryWorkflowsTest, cls).setUpClass()
        cls.test_path = os.path.abspath("src/toil/test/wdl/standard_library")

    def test_sub(self):
        # this workflow swaps the extension of a TSV file to CSV, with String and File inputs.
        self.check_function('sub', cases=['as_input'], expected_result='src/toil/test/wdl/test.csv')

        # NOTE: the result differs from Cromwell since we copy the file to the file store without
        # preserving the path. Cromwell would return 'src/toil/test/wdl/test.csv' instead.
        self.check_function('sub', cases=['as_input_with_file'], expected_result='test.csv')

    def test_size(self):
        self.check_function('size', cases=['as_command'], expected_result='19.0')

        # this workflow outputs the size of a 22-byte file in 'B', 'K', and 'Ki' separated with spaces.

        # NOTE: Cromwell treats the decimal and binary units (e.g.: 'K' and 'Ki') the same, which differs from
        # the spec (https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#float-sizefile-string).
        # The correct output should be '22.0 0.022 0.021484375' not '22.0 0.021484375 0.021484375'
        self.check_function('size', cases=['as_output'], expected_result='22.0 0.022 0.021484375')

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
        # NOTE: these tests depend on stdout() and the write_*() functions.

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

    def test_range(self):
        # NOTE: this test depends on write_lines().
        self.check_function('range', cases=['as_input'],
                            expected_result='0\n1\n2\n3\n4\n5\n6\n7')

        self.check_function('range', cases=['as_input'],
                            json_file_name='range_0',
                            expected_result='')

        self.check_function('range', cases=['as_input'],
                            json_file_name='range_invalid',
                            expected_exception='WDLRuntimeError')

    def test_transpose(self):
        # NOTE: this test depends on write_tsv().

        # this workflow writes a transposed 2-dimensional array as a TSV file.
        self.check_function('transpose', cases=['as_input'], expected_result='0\t3\n1\t4\n2\t5')

    def test_length(self):
        self.check_function('length', cases=['as_input'], expected_result='3')

        self.check_function('length', cases=['as_input'],
                            json_file_name='length_invalid',
                            expected_exception='WDLRuntimeError')

        # length() should not work with Map[X, Y].
        self.check_function('length', cases=['as_input_with_map'],
                            expected_exception='WDLRuntimeError')


if __name__ == "__main__":
    unittest.main()
