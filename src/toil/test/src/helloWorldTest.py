'''
Created on Sep 16, 2015

@author: benedictpaten
'''
from toil.job import Job
from argparse import ArgumentParser

import unittest
from toil.test import ToilTest

class HelloWorldTest(ToilTest):
    def testHelloWorld(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"
        # Run the workflow, the return value being the number of failed jobs
        Job.Runner.startToil(HelloWorld(), options)

class HelloWorld(Job):
    def __init__(self):
        Job.__init__(self,  memory=100000, cores=2, disk=20000)
    def run(self, fileStore):
        fileID = self.addChildJobFn(childFn, cores=1, memory="1M", disk="10M").rv()
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
