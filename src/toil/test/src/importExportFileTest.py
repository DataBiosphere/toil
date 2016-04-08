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

from __future__ import absolute_import

import uuid

from toil.common import Toil
from toil.job import Job
from toil.leader import FailedJobsException
from toil.test import ToilTest, make_tests


class ImportExportFileTest(ToilTest):
    def setUp(self):
        super(ImportExportFileTest, self).setUp()
        self._tempDir = self._createTempDir()
        self.dstFile = '%s/%s' % (self._tempDir, 'out')

    def _importExportFile(self, options, fail):
        with Toil(options) as toil:
            if not options.restart:

                srcFile = '%s/%s%s' % (self._tempDir, 'in', uuid.uuid4())
                with open(srcFile, 'w') as f:
                    f.write('Hello')
                inputFileID = toil.importFile('file://' + srcFile)

                # Write a boolean that determines whether the job fails.
                with toil._jobStore.writeFileStream() as (f, failFileID):
                    self.failFileID = failFileID
                    f.write(str(fail))

                outputFileID = toil.start(HelloWorld(inputFileID, self.failFileID))
            else:
                # Set up job for failure
                with toil._jobStore.updateFileStream(self.failFileID) as f:
                    f.write('False')

                outputFileID = toil.restart()

            toil.exportFile(outputFileID, 'file://' + self.dstFile)
            with open(self.dstFile, 'r') as f:
                assert f.read() == "HelloWorld!"

    def _importExport(self, restart):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"

        if restart:
            try:
                self._importExportFile(options, fail=True)
            except FailedJobsException:
                options.restart = True

        self._importExportFile(options, fail=False)

    def testImportExportRestartTrue(self):
        self._importExport(restart=True)

    def testImportExportRestartFalse(self):
        self._importExport(restart=False)


class HelloWorld(Job):
    def __init__(self, inputFileID, failFileID):
        Job.__init__(self,  memory=100000, cores=1, disk="1M")
        self.inputFileID = inputFileID
        self.failFileID = failFileID

    def run(self, fileStore):
        with fileStore.readGlobalFileStream(self.failFileID) as failValue:
            if failValue.read() == 'True':
                raise RuntimeError('planned exception')
            else:
                with fileStore.readGlobalFileStream(self.inputFileID) as fi:
                    with fileStore.writeGlobalFileStream() as (fo, outputFileID):
                        fo.write(fi.read() + 'World!')
                        return outputFileID
