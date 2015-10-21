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
from toil.job import Job
from toil.test import ToilTest

class HelloWorldTest(ToilTest):
    def testHelloWorld(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"
        Job.Runner.startToil(HelloWorld(), options)

class HelloWorld(Job):
    def __init__(self):
        Job.__init__(self,  memory=100000, cores=2, disk="3G")

    def run(self, fileStore):
        fileID = self.addChildJobFn(childFn, cores=1, memory="1M", disk="3G").rv()
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
