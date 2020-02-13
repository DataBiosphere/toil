# Copyright (C) 2015-2018 Regents of the University of California
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
from __future__ import absolute_import, print_function
from builtins import str
from builtins import range
import unittest
import os
import random
import shutil
from contextlib import contextmanager
from uuid import uuid4
import logging

from toil import subprocess
from toil import resolveEntryPoint
from toil.batchSystems.parasolTestSupport import ParasolTestSupport
from toil.common import Toil
from toil.job import Job
from toil.lib.bioio import getLogLevelString
from toil.batchSystems.mesos.test import MesosTestSupport
from toil.test.sort.sort import merge, sort, copySubRangeOfFile, getMidPoint, makeFileToSort, main
from toil.test import (ToilTest,
                       needs_aws_ec2,
                       needs_mesos,
                       needs_parasol,
                       needs_gridengine,
                       needs_torque,
                       needs_google,
                       slow)
from toil.jobStores.abstractJobStore import NoSuchJobStoreException, JobStoreExistsException
from toil.leader import FailedJobsException

log = logging.getLogger(__name__)

defaultLineLen = int(os.environ.get('TOIL_TEST_SORT_LINE_LEN', 10))
defaultLines = int(os.environ.get('TOIL_TEST_SORT_LINES', 10))
defaultN = int(os.environ.get('TOIL_TEST_SORT_N', defaultLineLen * defaultLines / 5))


@contextmanager
def runMain(options):
    """
    make sure the output file is deleted every time main is run
    """
    main(options)
    yield
    if os.path.exists(options.outputFile):
        os.remove(options.outputFile)


@slow
class SortTest(ToilTest, MesosTestSupport, ParasolTestSupport):
    """
    Tests Toil by sorting a file in parallel on various combinations of job stores and batch
    systems.
    """
    def setUp(self):
        super(SortTest, self).setUp()
        self.tempDir = self._createTempDir(purpose='tempDir')
        self.outputFile = os.path.join(self.tempDir, 'sortedFile.txt')
        self.inputFile = os.path.join(self.tempDir, "fileToSort.txt")

    def tearDown(self):
        if os.path.exists(self.tempDir):
            shutil.rmtree(self.tempDir)
        ToilTest.tearDown(self)

    def _toilSort(self, jobStoreLocator, batchSystem,
                  lines=defaultLines, N=defaultN, testNo=1, lineLen=defaultLineLen,
                  retryCount=2, badWorker=0.5, downCheckpoints=False, disableCaching=False):
        """
        Generate a file consisting of the given number of random lines, each line of the given
        length. Sort the file with Toil by splitting the file recursively until each part is less
        than the given number of bytes, sorting each part and merging them back together. Then
        verify the result.

        :param jobStoreLocator: The location of the job store.

        :param batchSystem: the name of the batch system

        :param lines: the number of random lines to generate

        :param N: the size in bytes of each split

        :param testNo: the number of repeats of this test

        :param lineLen: the length of each random line in the file
        """
        for test in range(testNo):
            try:
                # Specify options
                options = Job.Runner.getDefaultOptions(jobStoreLocator)
                options.logLevel = getLogLevelString()
                options.retryCount = retryCount
                options.batchSystem = batchSystem
                options.clean = "never"
                options.badWorker = badWorker
                options.badWorkerFailInterval = 0.05
                options.disableCaching = disableCaching
                # This is required because mesosMasterAddress now defaults to the IP of the machine
                # that is starting the workflow while the mesos *tests* run locally.
                if batchSystem == 'mesos':
                    options.mesosMasterAddress = 'localhost:5050'
                options.downCheckpoints = downCheckpoints
                options.N = N
                options.outputFile = self.outputFile
                options.fileToSort = self.inputFile
                options.overwriteOutput = True
                options.realTimeLogging = True

                # Make the file to sort
                makeFileToSort(options.fileToSort, lines=lines, lineLen=lineLen)

                # First make our own sorted version
                with open(options.fileToSort, 'r') as fileHandle:
                    l = fileHandle.readlines()
                    l.sort()

                # Check we get an exception if we try to restart a workflow that doesn't exist
                options.restart = True
                with self.assertRaises(NoSuchJobStoreException):
                    with runMain(options):
                        # Now check the file is properly sorted..
                        with open(options.outputFile, 'r') as fileHandle:
                            l2 = fileHandle.readlines()
                            self.assertEqual(l, l2)

                options.restart = False

                # Now actually run the workflow
                try:
                    with runMain(options):
                        pass
                    i = 0
                except FailedJobsException as e:
                    i = e.numberOfFailedJobs

                # Check we get an exception if we try to run without restart on an existing store
                with self.assertRaises(JobStoreExistsException):
                    with runMain(options):
                        pass

                options.restart = True

                # This loop tests the restart behavior
                totalTrys = 1
                while i != 0:
                    options.useExistingOptions = random.random() > 0.5
                    try:
                        with runMain(options):
                            pass
                        i = 0
                    except FailedJobsException as e:
                        i = e.numberOfFailedJobs
                        if totalTrys > 32:  # p(fail after this many restarts) = 0.5**32
                            self.fail('Exceeded a reasonable number of restarts')
                        totalTrys += 1
            finally:
                subprocess.check_call([resolveEntryPoint('toil'), 'clean', jobStoreLocator])
                # final test to make sure the jobStore was actually deleted
                self.assertRaises(NoSuchJobStoreException, Toil.resumeJobStore, jobStoreLocator)



    @needs_aws_ec2
    def testAwsSingle(self):
        self._toilSort(jobStoreLocator=self._awsJobStore(), batchSystem='singleMachine')

    @needs_aws_ec2
    @needs_mesos
    def testAwsMesos(self):
        self._startMesos()
        try:
            self._toilSort(jobStoreLocator=self._awsJobStore(), batchSystem="mesos")
        finally:
            self._stopMesos()

    @needs_mesos
    def testFileMesos(self):
        self._startMesos()
        try:
            self._toilSort(jobStoreLocator=self._getTestJobStorePath(), batchSystem="mesos")
        finally:
            self._stopMesos()

    @needs_google
    def testGoogleSingle(self):
        self._toilSort(jobStoreLocator=self._googleJobStore(), batchSystem="singleMachine")

    @needs_google
    @needs_mesos
    def testGoogleMesos(self):
        self._startMesos()
        try:
            self._toilSort(jobStoreLocator=self._googleJobStore(), batchSystem="mesos")
        finally:
            self._stopMesos()

    def testFileSingle(self):
        self._toilSort(jobStoreLocator=self._getTestJobStorePath(), batchSystem='singleMachine')

    def testFileSingleNonCaching(self):
        self._toilSort(jobStoreLocator=self._getTestJobStorePath(), batchSystem='singleMachine',
                       disableCaching=True)

    def testFileSingleCheckpoints(self):
        self._toilSort(jobStoreLocator=self._getTestJobStorePath(), batchSystem='singleMachine',
                       retryCount=2, downCheckpoints=True)

    def testFileSingle10000(self):
        self._toilSort(jobStoreLocator=self._getTestJobStorePath(), batchSystem='singleMachine',
                       lines=10000, N=10000)

    @needs_gridengine
    @unittest.skip('GridEngine does not support shared caching')
    def testFileGridEngine(self):
        self._toilSort(jobStoreLocator=self._getTestJobStorePath(), batchSystem='gridengine')

    @needs_torque
    @unittest.skip('PBS/Torque does not support shared caching')
    def testFileTorqueEngine(self):
        self._toilSort(jobStoreLocator=self._getTestJobStorePath(), batchSystem='torque')

    @needs_parasol
    @unittest.skip("skipping until parasol support is less flaky (see github issue #449")
    def testFileParasol(self):
        self._startParasol()
        try:
            self._toilSort(jobStoreLocator=self._getTestJobStorePath(), batchSystem='parasol')
        finally:
            self._stopParasol()

    # The following functions test the functions in the test

    testNo = 5

    def testSort(self):
        for test in range(self.testNo):
            tempFile1 = os.path.join(self.tempDir, "fileToSort.txt")
            makeFileToSort(tempFile1)
            lines1 = self._loadFile(tempFile1)
            lines1.sort()
            sort(tempFile1)
            with open(tempFile1, 'r') as f:
                lines2 = f.readlines()
            self.assertEqual(lines1, lines2)

    def testMerge(self):
        for test in range(self.testNo):
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
            self.assertEqual(lines1, lines2)

    def testCopySubRangeOfFile(self):
        for test in range(self.testNo):
            tempFile = os.path.join(self.tempDir, "fileToSort1.txt")
            outputFile = os.path.join(self.tempDir, "outputFileToSort1.txt")
            makeFileToSort(tempFile, lines=10, lineLen=defaultLineLen)
            fileSize = os.path.getsize(tempFile)
            assert fileSize > 0
            fileStart = random.choice(range(0, fileSize))
            fileEnd = random.choice(range(fileStart, fileSize))
            with open(outputFile, 'w') as f:
                f.write(copySubRangeOfFile(tempFile, fileStart, fileEnd))
            with open(outputFile, 'r') as f:
                l = f.read()
            with open(tempFile, 'r') as f:
                l2 = f.read()[fileStart:fileEnd]
            self.assertEqual(l, l2)

    def testGetMidPoint(self):
        for test in range(self.testNo):
            makeFileToSort(self.inputFile)
            with open(self.inputFile, 'r') as f:
                sorted_contents = f.read()
            fileSize = os.path.getsize(self.inputFile)
            midPoint = getMidPoint(self.inputFile, 0, fileSize)
            print("the mid point is %i of a file of %i bytes" % (midPoint, fileSize))
            assert midPoint < fileSize
            assert sorted_contents[midPoint] == '\n'
            assert midPoint >= 0

    # Support methods

    def _awsJobStore(self):
        return 'aws:%s:sort-test-%s' % (self.awsRegion(), uuid4())

    def _googleJobStore(self):
        projectID = os.getenv('TOIL_GOOGLE_PROJECTID')
        return 'google:%s:sort-test-%s' % (projectID, str(uuid4()))

    def _loadFile(self, path):
        with open(path, 'r') as f:
            return f.readlines()
