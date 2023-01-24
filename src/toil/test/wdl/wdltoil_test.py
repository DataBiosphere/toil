import json
import os
import shutil
import subprocess
import unittest
import uuid
import zipfile
from urllib.request import urlretrieve

import pytest

from toil.test import ToilTest, needs_docker, needs_java, slow
from toil.version import exactPython
from toil.test.wdl.toilwdlTest import ToilWdlTest

class WdlToilTest(ToilWdlTest):
    """
    Version of the old Toil WDL tests that tests the new MiniWDL-based implementation.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Runs once for all tests."""
        cls.base_command = [exactPython, '-m', 'toil.wdl.wdltoil']

    # We inherit a testMD5sum but it's not able to succeed yet
    @pytest.mark.xfail
    @needs_docker
    def testMD5sum(self):
        """
        Mark base class MD5Sum test as borken until it can be implemented.
        """
        return super().testMD5sum()

    @needs_docker
    def test_miniwdl_self_test(self):
        """Test if the MiniWDL self test runs and produces the expected output."""
        wdl_file = os.path.abspath('src/toil/test/wdl/miniwdl_self_test/self_test.wdl')
        json_file = os.path.abspath('src/toil/test/wdl/miniwdl_self_test/inputs.json')

        result_json = subprocess.check_output(self.base_command + [wdl_file, json_file, '-o', self.output_dir, '--outputDialect', 'miniwdl'])
        result = json.loads(result_json)

        # Expect MiniWDL-style output with a designated "dir"

        assert 'dir' in result
        assert isinstance(result['dir'], str)
        out_dir = result['dir']

        assert 'outputs' in result
        assert isinstance(result['outputs'], dict)
        outputs = result['outputs']

        assert 'hello_caller.message_files' in outputs
        assert isinstance(outputs['hello_caller.message_files'], list)
        assert len(outputs['hello_caller.message_files']) == 2
        for item in outputs['hello_caller.message_files']:
            # All the files should be strings in the "out" direcotry
            assert isinstance(item, str)
            assert item.startswith(out_dir)

        assert 'hello_caller.messages' in outputs
        assert outputs['hello_caller.messages'] == ["Hello, Alyssa P. Hacker!", "Hello, Ben Bitdiddle!"]

if __name__ == "__main__":
    unittest.main()  # run all tests
