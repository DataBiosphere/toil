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
import codecs
import errno
import logging
import os
import random

from toil.common import Toil
from toil.fileStores import FileID
from toil.job import Job
from toil.lib.retry import retry_flaky_test
from toil.test import ToilTest, slow, travis_test

logger = logging.getLogger(__name__)

PREFIX_LENGTH=200


# TODO: This test is ancient and while similar tests exist in `fileStoreTest.py`, none of them look
# at the contents of read files and thus we will let this test remain as-is.
class JobFileStoreTest(ToilTest):
    """
    Tests testing the methods defined in :class:toil.fileStores.abstractFileStore.AbstractFileStore.
    """

    @travis_test
    def testCachingFileStore(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        with Toil(options) as workflow:
            workflow.start(Job.wrapJobFn(simpleFileStoreJob))

    @travis_test
    def testNonCachingFileStore(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.disableCaching = True
        with Toil(options) as workflow:
            workflow.start(Job.wrapJobFn(simpleFileStoreJob))

    @travis_test
    def _testJobFileStore(self, retryCount=0, badWorker=0.0, stringNo=1, stringLength=1000000,
                          testNo=2):
        """
        Creates a chain of jobs, each reading and writing files using the
        toil.fileStores.abstractFileStore.AbstractFileStore interface. Verifies the files written are always what we
        expect.
        """
        for test in range(testNo):
            #Make a list of random strings, each of 100k chars and hash the first 200
            #base prefix to the string
            def randomString():
                chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                s = "".join([random.choice(chars) for i in range(stringLength)])
                return s[:PREFIX_LENGTH], s
            #Total length is 2 million characters (20 strings of length 100K each)
            testStrings = dict([randomString() for i in range(stringNo)])
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "DEBUG"
            options.retryCount=retryCount
            options.badWorker=badWorker
            options.badWorkerFailInterval = 1.0
            chainLength = 10
            # Run the workflow, the return value being the number of failed jobs
            Job.Runner.startToil(Job.wrapJobFn(fileTestJob, [],
                                               testStrings, chainLength),
                                 options)

    def testJobFileStore(self):
        """
        Tests case that about half the files are cached
        """
        self._testJobFileStore(retryCount=0, badWorker=0.0,  stringNo=5, stringLength=1000000)

    @slow
    def testJobFileStoreWithBadWorker(self):
        """
        Tests case that about half the files are cached and the worker is randomly
        failing.
        """
        self._testJobFileStore(retryCount=100, badWorker=0.5,  stringNo=5, stringLength=1000000)


def fileTestJob(job, inputFileStoreIDs, testStrings, chainLength):
    """
    Test job exercises toil.fileStores.abstractFileStore.AbstractFileStore functions
    """
    outputFileStoreIds = [] #Strings passed to the next job in the chain

    #Load the input jobStoreFileIDs and check that they map to the
    #same set of random input strings, exercising the different functions in the fileStore interface
    for fileStoreID in inputFileStoreIDs:
        if random.random() > 0.5:
            #Read the file for the fileStoreID, randomly picking a way to invoke readGlobalFile
            if random.random() > 0.5:
                local_path = job.fileStore.getLocalTempFileName() if random.random() > 0.5 else None
                cache = random.random() > 0.5

                tempFile = job.fileStore.readGlobalFile(fileStoreID, 
                                                        local_path,
                                                        cache=cache)
                with open(tempFile, 'r') as fH:
                    string = fH.readline()
                logging.info("Downloaded %s to local path %s with cache %s and got %s with %d letters",
                              fileStoreID, local_path, cache, tempFile, len(string))
            else:
                #Check the local file is as we expect
                with job.fileStore.readGlobalFileStream(fileStoreID, 'utf-8') as fH:
                    string = fH.readline()
                logging.info("Streamed %s and got %d letters", fileStoreID, len(string))
            #Check the string we get back is what we expect
            assert string[:PREFIX_LENGTH] in testStrings, f"Could not find string: {string[:PREFIX_LENGTH]}"
            assert testStrings[string[:PREFIX_LENGTH]] == string, f"Mismatch in string: {string[:PREFIX_LENGTH]}"

            #This allows the file to be passed to the next job
            outputFileStoreIds.append(fileStoreID)
        else:
            #This tests deletion
            logging.info("Deleted %s", fileStoreID)
            job.fileStore.deleteGlobalFile(fileStoreID)

    #Fill out the output strings until we have the same number as the input strings
    #exercising different ways of writing files to the file store
    while len(outputFileStoreIds) < len(testStrings):
        #Pick a string and write it into a file
        testString = random.choice(list(testStrings.values()))
        if random.random() > 0.5:
            #Make a local copy of the file
            tempFile = job.fileStore.getLocalTempFile() if random.random() > 0.5 \
            else os.path.join(job.fileStore.getLocalTempDir(), "temp.txt")
            with open(tempFile, 'w') as fH:
                fH.write(testString)
            #Write a local copy of the file using the local file
            fileStoreID = job.fileStore.writeGlobalFile(tempFile)

            # Make sure it returned a valid and correct FileID with the right size
            assert isinstance(fileStoreID, FileID)
            assert fileStoreID.size == len(testString.encode('utf-8'))

            outputFileStoreIds.append(fileStoreID)
        else:
            #Use the writeGlobalFileStream method to write the file
            with job.fileStore.writeGlobalFileStream() as (fH, fileStoreID):
                fH.write(testString.encode('utf-8'))
                outputFileStoreIds.append(fileStoreID)

            #Make sure it returned a valid and correct FileID with the right size
            assert isinstance(fileStoreID, FileID)
            assert fileStoreID.size == len(testString.encode('utf-8'))

    if chainLength > 0:
        #Make a child that will read these files and check it gets the same results
        job.addChildJobFn(fileTestJob, outputFileStoreIds, testStrings, chainLength-1)


fileStoreString = "Testing writeGlobalFile"
streamingFileStoreString = "Testing writeGlobalFileStream"


def simpleFileStoreJob(job):
    localFilePath = os.path.join(job.fileStore.getLocalTempDir(), "parentTemp.txt")
    with open(localFilePath, 'w') as f:
        f.write(fileStoreString)
    testID1 = job.fileStore.writeGlobalFile(localFilePath)

    testID2 = None
    with job.fileStore.writeGlobalFileStream() as (f, fileID):
        f.write(streamingFileStoreString.encode('utf-8'))
        testID2 = fileID

    job.addChildJobFn(fileStoreChild, testID1, testID2)


def fileStoreChild(job, testID1, testID2):
    with job.fileStore.readGlobalFileStream(testID1) as f:
        assert(f.read().decode('utf-8') == fileStoreString)

    localFilePath = os.path.join(job.fileStore.getLocalTempDir(), "childTemp.txt")
    job.fileStore.readGlobalFile(testID2, localFilePath)
    with open(localFilePath, 'r') as f:
        assert(f.read() == streamingFileStoreString)

    job.fileStore.deleteLocalFile(testID2)
    try:
        job.fileStore.deleteLocalFile(testID1)
    except OSError as e:
        if e.errno == errno.ENOENT:  # indicates that the file was not found
            pass
        else:
            raise
    else:
        raise RuntimeError("Deleting a non-existant file did not throw an exception")

    for fileID in (testID1, testID2):
        job.fileStore.deleteGlobalFile(fileID)
