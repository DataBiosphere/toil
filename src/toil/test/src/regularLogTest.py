from __future__ import print_function
# Copyright (C) 2015-2016 Regents of the University of California
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
import mimetypes
import sys
import os

from toil import subprocess
from toil.test import ToilTest, slow
from toil.test.mesos import helloWorld


class RegularLogTest(ToilTest):

    def setUp(self):
        super(RegularLogTest, self).setUp()
        self.tempDir = self._createTempDir(purpose='tempDir')

    def _getFiles(self, dir):
        return [os.path.join(dir, f) for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]

    def _assertFileTypeExists(self, dir, extension, encoding=None):
        # an encoding of None implies no compression
        onlyFiles = self._getFiles(dir)
        print(os.listdir(dir))
        onlyLogs = [f for f in onlyFiles if f.endswith(extension)]
        assert onlyLogs
        for log in onlyLogs:
            with open(log, "r") as f:
                if encoding == "gzip":
                    # Check for gzip magic header '\x1f\x8b'
                    assert f.read().startswith('\x1f\x8b')
                else:
                    mime = mimetypes.guess_type(log)
                    self.assertEqual(mime[1], encoding)


    @slow
    def testLogToMaster(self):
        toilOutput = subprocess.check_output([sys.executable,
                                              '-m', helloWorld.__name__,
                                              './toilTest',
                                              '--clean=always',
                                              '--logLevel=info'], stderr=subprocess.STDOUT)
        assert helloWorld.childMessage in toilOutput

    def testWriteLogs(self):
        toilOutput = subprocess.check_output([sys.executable,
                                              '-m', helloWorld.__name__,
                                              './toilTest',
                                              '--clean=always',
                                              '--logLevel=debug',
                                              '--writeLogs=%s' % self.tempDir],
                                             stderr=subprocess.STDOUT)
        self._assertFileTypeExists(self.tempDir, '.log')

    @slow
    def testWriteGzipLogs(self):
        toilOutput = subprocess.check_output([sys.executable,
                                              '-m', helloWorld.__name__,
                                              './toilTest',
                                              '--clean=always',
                                              '--logLevel=debug',
                                              '--writeLogsGzip=%s' % self.tempDir],
                                              stderr=subprocess.STDOUT)
        self._assertFileTypeExists(self.tempDir, '.log.gz', 'gzip')

    @slow
    def testMultipleLogToMaster(self):
        toilOutput = subprocess.check_output([sys.executable,
                                              '-m', helloWorld.__name__,
                                              './toilTest',
                                              '--clean=always',
                                              '--logLevel=info'], stderr=subprocess.STDOUT)
        assert helloWorld.parentMessage in toilOutput

    def testRegularLog(self):
        toilOutput = subprocess.check_output([sys.executable,
                                              '-m', helloWorld.__name__,
                                              './toilTest',
                                              '--clean=always',
                                              '--batchSystem=singleMachine',
                                              '--logLevel=info'], stderr=subprocess.STDOUT)
        assert "single machine batch system" in toilOutput
