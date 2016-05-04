# Copyright (C) 2015 UCSC Computational Genomics Lab
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
import subprocess
import sys
from toil.test import ToilTest
from toil.test.mesos import helloWorld


class RegularLogTest(ToilTest):

    def testLogToMaster(self):
        toilOutput = subprocess.check_output([sys.executable,
                                              '-m', helloWorld.__name__,
                                              './toilTest',
                                              '--clean=always',
                                              '--logLevel=info'], stderr=subprocess.STDOUT)
        assert helloWorld.childMessage in toilOutput

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
