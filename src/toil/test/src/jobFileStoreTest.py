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
import random

from toil.job import Job
from toil.test import ToilTest

PREFIX_LENGTH=200

class JobFileStoreTest(ToilTest):
    """
    Tests testing the Job.FileStore class
    """
    def testJobFileStore(self):
        """
        Creates a chain of jobs, each reading and writing files using the 
        Job.FileStore interface. Verifies the files written are always what we expect.
        The chain tests the caching behavior. 
        """
        for test in xrange(10):
            #Make a list of random strings, each of 100k chars and hash the first 200 
            #base prefix to the string
            def randomString():
                chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                s = "".join(map(lambda i : random.choice(chars), xrange(100000)))
                return s[:PREFIX_LENGTH], s
            #Total length is 2 million characters (20 strings of length 100K each) 
            testStrings = dict(map(lambda i : randomString(), xrange(20)))
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "INFO"
            options.cacheSize = 1000000
            options.retryCount=100
            options.badWorker=0.5
            options.badWorkerFailInterval = 1.0
            chainLength = 10
            # Run the workflow, the return value being the number of failed jobs
            Job.Runner.startToil(Job.wrapJobFn(fileTestJob, [], 
                                               testStrings, chainLength), 
                                 options)
        
def fileTestJob(job, inputFileStoreIDs, testStrings, chainLength):
    """
    Test job exercises Job.FileStore functions
    """
    for fileStoreID in inputFileStoreIDs:
        #Load the input jobStoreFileIDs and check that they map to the 
        #same set of random input strings
        if random.random() > 0.666:
            tempFile = job.fileStore.readGlobalFile(fileStoreID)
            with open(tempFile, 'r') as fH:
                string = fH.readline()
                assert testStrings[string[:PREFIX_LENGTH]] == string
        elif random.random() > 0.666:
            #with job.fileStore.jobStore.readFileStream(fileStoreID) as fH:
            with job.fileStore.readGlobalFileStream(fileStoreID) as fH:
                string = fH.readline()
                assert testStrings[string[:PREFIX_LENGTH]] == string
        else:
            #This tests deletion
            job.fileStore.deleteGlobalFile(fileStoreID)

    if chainLength > 0:
        #For each job write some these strings into the file store, collecting 
        #the fileIDs
        outputFileStoreIds = []
        for testPrefix in testStrings:
            if random.random() > 0.5:
                #Make a local copy of the file
                tempFile = job.fileStore.getLocalTempFile()
                with open(tempFile, 'w') as fH:
                    fH.write(testStrings[testPrefix])
                #Write a local copy of the file using the local file
                outputFileStoreIds.append(job.fileStore.writeGlobalFile(tempFile))
            else:
                with job.fileStore.writeGlobalFileStream() as (fH, fileStoreID):
                    fH.write(testStrings[testPrefix])
                    outputFileStoreIds.append(fileStoreID)
    
        #Make a child that will read these files and check it gets the same results
        job.addChildJobFn(fileTestJob, outputFileStoreIds, testStrings, chainLength-1)
        
        