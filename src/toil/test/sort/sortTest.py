#!/usr/bin/env python

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
import unittest
import os
import random
from uuid import uuid4
import logging
import subprocess

from toil.batchSystems.parasolTestSupport import ParasolTestSupport
from toil.job import Job, JobException
from toil.lib.bioio import getLogLevelString
from toil.batchSystems.mesos.test import MesosTestSupport
from toil.test.sort.lib import merge, sort, copySubRangeOfFile, getMidPoint
from toil.test.sort.sort import setup, sortMemory
from toil.test import ToilTest, needs_aws, needs_mesos, needs_azure, needs_parasol, needs_gridengine
from toil.jobStores.abstractJobStore import JobStoreCreationException
from toil.leader import FailedJobsException

log = logging.getLogger(__name__)

defaultLineLen = int(os.environ.get('TOIL_TEST_SORT_LINE_LEN', '10'))
defaultLines = int(os.environ.get('TOIL_TEST_SORT_LINES', '10'))
defaultN = int(os.environ.get('TOIL_TEST_SORT_N', str(defaultLineLen * defaultLines / 5)))

class SortTest(ToilTest, MesosTestSupport, ParasolTestSupport):
    """
    Tests Toil by sorting a file in parallel on various combinations of job stores and batch
    systems.
    """
    def setUp(self):
        super(SortTest, self).setUp()
        self.tempDir = self._createTempDir(purpose='tempDir')

    def _toilSort(self, jobStore, batchSystem,
                  lines=defaultLines, N=defaultN, testNo=1, lineLen=defaultLineLen):
        """
        Generate a file consisting of the given number of random lines, each line of the given
        length. Sort the file with Toil by splitting the file recursively until each part is less
        than the given number of bytes, sorting each part and merging them back together. Then
        verify the result.

        :param jobStore: a job store string

        :param batchSystem: the name of the batch system

        :param lines: the number of random lines to generate

        :param N: the size in bytes of each split

        :param testNo: the number of repeats of this test

        :param lineLen: the length of each random line in the file
        """
        for test in xrange(testNo):
            try:
                # Specify options
                options = Job.Runner.getDefaultOptions(jobStore)
                options.logLevel = getLogLevelString()
                options.retryCount = 2
                options.batchSystem = batchSystem
                options.clean = "never"

                # Make the file to sort
                tempSortFile = os.path.join(self.tempDir, "fileToSort.txt")
                makeFileToSort(tempSortFile, lines=lines, lineLen=lineLen)

                # First make our own sorted version
                with open(tempSortFile, 'r') as fileHandle:
                    l = fileHandle.readlines()
                    l.sort()

                # Make the first job
                firstJob = Job.wrapJobFn(setup, tempSortFile, N, memory=sortMemory)

                # Check we get an exception if we try to restart a workflow that doesn't exist
                options.restart = True
                try:
                    Job.Runner.startToil(firstJob, options)
                    self.fail()
                except JobStoreCreationException:
                    pass

                options.restart = False

                # Now actually run the workflow
                try:
                    Job.Runner.startToil(firstJob, options)
                    i = 0
                except FailedJobsException as e:
                    i = e.numberOfFailedJobs

                # Check we get an exception if we try to run without restart on an existing store
                try:
                    Job.Runner.startToil(firstJob, options)
                    self.fail()
                except JobStoreCreationException:
                    pass

                options.restart = True

                # This loop tests the restart behavior
                while i != 0:
                    options.useExistingOptions = random.random() > 0.5
                    try:
                        Job.Runner.startToil(firstJob, options)
                        i = 0
                    except FailedJobsException as e:
                        i = e.numberOfFailedJobs

                # Now check that if you try to restart from here it will raise an exception
                # indicating that there are no jobs remaining in the workflow.
                try:
                    Job.Runner.startToil(firstJob, options)
                    self.fail()
                except JobException:
                    pass

                # Now check the file is properly sorted..
                with open(tempSortFile, 'r') as fileHandle:
                    l2 = fileHandle.readlines()
                    self.assertEquals(l, l2)
            finally:
                subprocess.check_call([self._getUtilScriptPath('toilMain'), 'clean', jobStore])

    @needs_aws
    def testAwsSingle(self):
        self._toilSort(jobStore=self._awsJobStore(), batchSystem='singleMachine')

    @needs_aws
    @needs_mesos
    def testAwsMesos(self):
        self._startMesos()
        try:
            self._toilSort(jobStore=self._awsJobStore(), batchSystem="mesos")
        finally:
            self._stopMesos()

    @needs_mesos
    def testFileMesos(self):
        self._startMesos()
        try:
            self._toilSort(jobStore=self._getTestJobStorePath(), batchSystem="mesos")
        finally:
            self._stopMesos()

    @needs_azure
    def testAzureSingle(self):
        self._toilSort(jobStore=self._azureJobStore(), batchSystem='singleMachine')

    @needs_azure
    @needs_mesos
    def testAzureMesos(self):
        self._startMesos()
        try:
            self._toilSort(jobStore=self._azureJobStore(), batchSystem="mesos")
        finally:
            self._stopMesos()

    def testFileSingle(self):
        self._toilSort(jobStore=self._getTestJobStorePath(), batchSystem='singleMachine')

    def testFileSingle10000(self):
        self._toilSort(jobStore=self._getTestJobStorePath(), batchSystem='singleMachine',
                       lines=10000, N=10000)

    @needs_gridengine
    def testFileGridEngine(self):
        self._toilSort(jobStore=self._getTestJobStorePath(), batchSystem='gridengine')

    @needs_parasol
    def testFileParasol(self):
        self._startParasol()
        try:
            self._toilSort(jobStore=self._getTestJobStorePath(), batchSystem='parasol')
        finally:
            self._stopParasol()

    # The following functions test the functions in the test

    testNo = 5

    def testSort(self):
        for test in xrange(self.testNo):
            tempFile1 = os.path.join(self.tempDir, "fileToSort.txt")
            makeFileToSort(tempFile1)
            lines1 = self._loadFile(tempFile1)
            lines1.sort()
            sort(tempFile1)
            with open(tempFile1, 'r') as f:
                lines2 = f.readlines()
            self.assertEquals(lines1, lines2)

    def testMerge(self):
        for test in xrange(self.testNo):
            tempFile1 = os.path.join(self.tempDir, "fileToSort1.txt")
            tempFile2 = os.path.join(self.tempDir, "fileToSort2.txt")
            tempFile3 = os.path.join(self.tempDir, "mergedFile.txt")
            makeFileToSort(tempFile1)
            makeFileToSort(tempFile2)
            sort(tempFile1)
            sort(tempFile2)
            with open(tempFile3, 'w') as fileHandle:
                with open(tempFile1) as tempFileHandle1:
                    with open(tempFile2) as tempFileHandle2:
                        merge(tempFileHandle1, tempFileHandle2, fileHandle)
            lines1 = self._loadFile(tempFile1) + self._loadFile(tempFile2)
            lines1.sort()
            with open(tempFile3, 'r') as f:
                lines2 = f.readlines()
            self.assertEquals(lines1, lines2)

    def testCopySubRangeOfFile(self):
        for test in xrange(self.testNo):
            tempFile = os.path.join(self.tempDir, "fileToSort1.txt")
            outputFile = os.path.join(self.tempDir, "outputFileToSort1.txt")
            makeFileToSort(tempFile, lines=10, lineLen=defaultLineLen)
            fileSize = os.path.getsize(tempFile)
            assert fileSize > 0
            fileStart = random.choice(xrange(0, fileSize))
            fileEnd = random.choice(xrange(fileStart, fileSize))
            fileHandle = open(outputFile, 'w')
            copySubRangeOfFile(tempFile, fileStart, fileEnd, fileHandle)
            fileHandle.close()
            l = open(outputFile, 'r').read()
            l2 = open(tempFile, 'r').read()[fileStart:fileEnd]
            self.assertEquals(l, l2)

    def testGetMidPoint(self):
        for test in xrange(self.testNo):
            tempFile = os.path.join(self.tempDir, "fileToSort.txt")
            makeFileToSort(tempFile)
            l = open(tempFile, 'r').read()
            fileSize = os.path.getsize(tempFile)
            midPoint = getMidPoint(tempFile, 0, fileSize)
            print "the mid point is %i of a file of %i bytes" % (midPoint, fileSize)
            assert midPoint < fileSize
            assert l[midPoint] == '\n'
            assert midPoint >= 0

    # Support methods

    def _awsJobStore(self):
        return "aws:us-west-2:sort-test-%s" % uuid4()

    def _azureJobStore(self):
        return "azure:toiltest:sort-test-%s" % uuid4()

    def _loadFile(self, path):
        with open(path, 'r') as f:
            return f.readlines()


def makeFileToSort(fileName, lines=defaultLines, lineLen=defaultLineLen):
    with open(fileName, 'w') as fileHandle:
        for _ in xrange(lines):
            line = "".join(random.choice('actgACTGNXYZ') for _ in xrange(lineLen - 1)) + '\n'
            fileHandle.write(line)


if __name__ == '__main__':
    unittest.main()
