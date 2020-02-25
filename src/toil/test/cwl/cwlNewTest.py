# Copyright (C) 2015 Curoverse, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
from __future__ import print_function
import json
import os
import sys
import unittest
import re
import logging
import shutil
import zipfile
import pytest
import uuid
from future.moves.urllib.request import urlretrieve
from six.moves import StringIO
from six import u as str
from six import text_type

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

logger = logging.getLogger(__name__)
import subprocess
from toil.test import (ToilTest, needs_cwl, slow, needs_docker, needs_lsf,
                       needs_mesos, needs_parasol, needs_gridengine, needs_slurm,
                       needs_torque)


@needs_cwl
class CWLTest(ToilTest):
    @classmethod
    def setUpClass(cls):
        """Runs anew before each test to create farm fresh temp dirs."""
        from builtins import str as normal_str
        cls.outDir = os.path.join('/tmp/', 'toil-cwl-1.1-test-' + normal_str(uuid.uuid4()))
        os.makedirs(cls.outDir)
        cls.rootDir = cls._projectRootPath()
        cls.cwlSpec = os.path.join(cls.rootDir, 'src/toil/test/cwl/spec')
        # cls.workDir = os.path.join(cls.cwlSpec, 'tests')
        cls.test_yaml = os.path.join(cls.cwlSpec, 'conformance_tests.yaml')
        url = 'https://github.com/common-workflow-language/cwl-v1.1.git'
        p = subprocess.Popen(f'git clone {url} {cls.cwlSpec}', shell=True)
        stdout, stderr = p.communicate()
        # /home/quokka/cwl1.1/toil/src/toil/test/cwl/spec/conformance_test.yaml
        #

    def tearDown(self):
        """Clean up outputs."""
        if os.path.exists(self.outDir):
            shutil.rmtree(self.outDir)
        unittest.TestCase.tearDown(self)

    @slow
    @pytest.mark.timeout(2400)
    # Cannot work until we fix https://github.com/DataBiosphere/toil/issues/2801
    @pytest.mark.xfail
    def test_run_conformance_with_caching(self):
        self.test_run_conformance(caching=True)

    @slow
    @pytest.mark.timeout(2400)
    def test_run_new_conformance(self, batchSystem=None, caching=False):
        try:
            cmd = ['cwltest', '--tool', 'toil-cwl-runner', f'--test={self.test_yaml}',
                   '--timeout=2400', '--basedir=' + self.cwlSpec]
            if batchSystem:
                cmd.extend(["--batchSystem", batchSystem])
            if caching:
                cmd.extend(['--', '--disableCaching="False"'])
            subprocess.check_output(cmd, cwd=self.cwlSpec, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            only_unsupported = False
            # check output -- if we failed but only have unsupported features, we're okay
            p = re.compile(r"(?P<failures>\d+) failures, (?P<unsupported>\d+) unsupported features")

            error_log = e.output
            if isinstance(e.output, bytes):
                # py2/3 string handling
                error_log = e.output.decode('utf-8')

            for line in error_log.split('\n'):
                m = p.search(line)
                if m:
                    if int(m.group("failures")) == 0 and int(m.group("unsupported")) > 0:
                        only_unsupported = True
                        break
            if not only_unsupported:
                print(error_log)
                raise e
