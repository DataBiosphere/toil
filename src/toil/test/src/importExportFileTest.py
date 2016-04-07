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

import shutil

import os
from toil.common import Toil
from toil.job import Job
from toil.test import ToilTest
import tempfile


class ImportExportFileTest(ToilTest):
    def setUp(self):
        self.srcFile = "%s/%s" % (tempfile.mkdtemp(), uuid.uuid4())
        with open(self.srcFile, 'w') as f:
            f.write(os.urandom(2**10))

    def tearDown(self):
        shutil.rmtree(self.srcFile.rsplit('/', 1)[0])

    def testImportExportFile(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"
        job = HelloWorld()
        with Toil(options) as toil:
            jobStoreFileID = toil.importFile('file://%s' % self.srcFile)
            assert toil.jobStore.fileExists(jobStoreFileID)

            destPath = '%s/%s' % (self.srcFile.rsplit('/', 1)[0], uuid.uuid4())
            toil.exportFile(jobStoreFileID, 'file://%s' % destPath)
            assert os.path.exists(destPath)

            toil.run(job)


class HelloWorld(Job):
    def __init__(self):
        Job.__init__(self,  memory=100000, cores=2, disk="3G")

    def run(self, fileStore):
        fileID = self.addChildJobFn(childFn, cores=1, memory="1M", disk="1M").rv()
        self.addFollowOn(FollowOn(fileID))


def childFn(job):
    with job.fileStore.writeGlobalFileStream() as (fH, fileID):
        fH.write("Hello, World!")
        return fileID


class FollowOn(Job):
    def __init__(self,fileId):
        Job.__init__(self)
        self.fileId=fileId

    def run(self, fileStore):
        tempDir = fileStore.getLocalTempDir()
        tempFilePath = "/".join([tempDir,"LocalCopy"])
        with fileStore.readGlobalFileStream(self.fileId) as globalFile:
            with open(tempFilePath, "w") as localFile:
                localFile.write(globalFile.read())
