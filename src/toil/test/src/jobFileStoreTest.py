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
import os
from toil.job import Job
from toil.test import ToilTest

PREFIX_LENGTH=200

class JobFileStoreTest(ToilTest):
    """
    Tests testing the Job.FileStore class
    """
    def testJobFileStoreWithSmallCache(self, retryCount=0, badWorker=0.0, 
                         stringNo=1, stringLength=1000000, cacheSize=10000, testNo=2):
        """
        Creates a chain of jobs, each reading and writing files using the 
        Job.FileStore interface. Verifies the files written are always what we expect.
        The chain tests the caching behavior. 
        """
        for test in xrange(testNo):
            #Make a list of random strings, each of 100k chars and hash the first 200 
            #base prefix to the string
            def randomString():
                chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                s = "".join(map(lambda i : random.choice(chars), xrange(stringLength)))
                return s[:PREFIX_LENGTH], s
            #Total length is 2 million characters (20 strings of length 100K each) 
            testStrings = dict(map(lambda i : randomString(), xrange(stringNo)))
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "INFO"
            options.cacheSize = cacheSize
            options.retryCount=retryCount
            options.badWorker=badWorker
            options.badWorkerFailInterval = 1.0
            chainLength = 10
            # Run the workflow, the return value being the number of failed jobs
            Job.Runner.startToil(Job.wrapJobFn(fileTestJob, [], 
                                               testStrings, chainLength), 
                                 options)
            
    def testJobFileStoreWithOverLargeCache(self):
        """
        Tests case that all files are cached.
        """
        self.testJobFileStoreWithSmallCache(retryCount=0, badWorker=0.0, 
                         stringNo=5, stringLength=1000000, 
                         cacheSize=10000000)
        
    def testJobFileStoreWithMediumCache(self):
        """
        Tests case that about half the files are cached
        """
        self.testJobFileStoreWithSmallCache(retryCount=0, badWorker=0.0, 
                         stringNo=5, stringLength=1000000, 
                         cacheSize=3000000)
    
    def testJobFileStoreWithMediumCacheAndBadWorker(self):
        """
        Tests case that about half the files are cached and the worker is randomly
        failing.
        """
        self.testJobFileStoreWithSmallCache(retryCount=100, badWorker=0.5, 
                         stringNo=5, stringLength=1000000, 
                         cacheSize=3000000)
        
def fileTestJob(job, inputFileStoreIDs, testStrings, chainLength):
    """
    Test job exercises Job.FileStore functions
    """
    outputFileStoreIds = [] #Strings passed to the next job in the chain
    
    #Load the input jobStoreFileIDs and check that they map to the 
    #same set of random input strings, exercising the different functions in the fileStore interface
    for fileStoreID in inputFileStoreIDs:
        if random.random() > 0.5:
            #Read the file for the fileStoreID, randomly picking a way to invoke readGlobalFile
            if random.random() > 0.5:
                tempFile = job.fileStore.readGlobalFile(fileStoreID, 
                                                        job.fileStore.getLocalTempFile() if 
                                                        random.random() > 0.5 else None,
                                                        cache=random.random() > 0.5)
                with open(tempFile, 'r') as fH:
                    string = fH.readline()
            else:
                #Check the local file is as we expect
                with job.fileStore.readGlobalFileStream(fileStoreID) as fH:
                    string = fH.readline()
                    
            #Check the string we get back is what we expect
            assert testStrings[string[:PREFIX_LENGTH]] == string
            
            #This allows the file to be passed to the next job
            outputFileStoreIds.append(fileStoreID)
        else:
            #This tests deletion
            job.fileStore.deleteGlobalFile(fileStoreID)
    
    #Fill out the output strings until we have the same number as the input strings
    #exercising different ways of writing files to the file store
    while len(outputFileStoreIds) < len(testStrings):
        #Pick a string and write it into a file
        testString = random.choice(testStrings.values())
        if random.random() > 0.5:
            #Make a local copy of the file
            tempFile = job.fileStore.getLocalTempFile() if random.random() > 0.5 \
            else os.path.join(job.fileStore.getLocalTempDir(), "temp.txt")
            with open(tempFile, 'w') as fH:
                fH.write(testString)
            #Write a local copy of the file using the local file
            outputFileStoreIds.append(job.fileStore.writeGlobalFile(tempFile))
        else:
            #Use the writeGlobalFileStream method to write the file
            with job.fileStore.writeGlobalFileStream() as (fH, fileStoreID):
                fH.write(testString)
                outputFileStoreIds.append(fileStoreID)

    if chainLength > 0:
        #Make a child that will read these files and check it gets the same results
        job.addChildJobFn(fileTestJob, outputFileStoreIds, testStrings, chainLength-1)
