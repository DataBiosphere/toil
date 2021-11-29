# Copyright (C) 2015-2021 Regents of the University of California
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

import os

from toil.common import Toil, ToilContextManagerException
from toil.job import Job
from toil.test import ToilTest, get_temp_file, slow


@slow
class ToilContextManagerTest(ToilTest):
    def setUp(self):
        self.exportPath = get_temp_file()

    def tearDown(self):
        os.remove(self.exportPath)

    def testContextManger(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = 'INFO'
        with Toil(options) as toil:
            toil.start(HelloWorld())

    def testNoContextManger(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = 'INFO'
        toil = Toil(options)
        self.assertRaises(ToilContextManagerException, toil.start, HelloWorld())

    def testExportAfterFailedExport(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        try:
            with Toil(options) as toil:
                _ = toil.start(HelloWorld())
                # oh no, an error! :(
                raise RuntimeError("we died after workflow completion but before our export finished")
        except RuntimeError:
            pass

        options.restart = True
        with Toil(options) as toil:
            fileID = toil.restart()
            print(fileID)
            # Hopefully the error didn't cause us to lose all our work!
            toil.exportFile(fileID, 'file://' + self.exportPath)
        with open(self.exportPath) as f:
            # The file should have all our content
            self.assertEqual(f.read(), "Hello, World!")

class HelloWorld(Job):
    def __init__(self):
        Job.__init__(self, memory=100000, cores=2, disk='1M')

    def run(self, fileStore):
        fileID = self.addChildJobFn(childFn, cores=1, memory='1M', disk='1M').rv()
        return self.addFollowOn(FollowOn(fileID)).rv()


def childFn(job):
    with job.fileStore.writeGlobalFileStream() as (fH, fileID):
        fH.write(b"Hello, World!")
        return fileID


class FollowOn(Job):
    def __init__(self, fileId):
        Job.__init__(self)
        self.fileId = fileId

    def run(self, fileStore):
        tempDir = fileStore.getLocalTempDir()
        tempFilePath = "/".join([tempDir, 'LocalCopy'])
        with fileStore.readGlobalFileStream(self.fileId) as globalFile:
            with open(tempFilePath, "wb") as localFile:
                localFile.write(globalFile.read())
        return self.fileId
