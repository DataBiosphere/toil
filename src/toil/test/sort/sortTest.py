#!/usr/bin/env python
"""Tests the scriptTree toil-script compiler.
"""

from __future__ import absolute_import
import unittest
import os
import random
from uuid import uuid4
import logging
import shutil
import tempfile

from toil.job import Job, JobException
from toil.lib.bioio import getLogLevelString
from toil.batchSystems.mesos.test import MesosTestSupport
from toil.test.sort.lib import merge, sort, copySubRangeOfFile, getMidPoint
from toil.test.sort.sort import setup
from toil.test import ToilTest, needs_aws, needs_mesos
from toil.jobStores.abstractJobStore import JobStoreCreationException

log = logging.getLogger(__name__)


class SortTest(ToilTest, MesosTestSupport):
    def setUp(self):
        super(SortTest, self).setUp()
        self.jobStore = self._getTestJobStorePath()
        self.tempDir = tempfile.mkdtemp(prefix="tempDir")
        self.testNo = 5

    def tearDown(self):
        super(SortTest, self).tearDown()
        if os.path.exists(self.jobStore):
            shutil.rmtree(self.jobStore)
        if os.path.exists(self.tempDir):
            shutil.rmtree(self.tempDir)

    def toilSortTest(self, testNo=1, batchSystem="singleMachine", jobStore='file',
                     lines=10000, maxLineLength=10, N=10000):
        """
        Tests toil by sorting a file in parallel.
        """
        for test in xrange(testNo):
            options = Job.Runner.getDefaultOptions()

            # toil
            if jobStore == 'file':
                options.jobStore = self.jobStore
            else:
                options.jobStore = jobStore

            # Specify options
            options.logLevel = getLogLevelString()
            options.retryCount = 2
            options.batchSystem = batchSystem

            # Make the file to sort
            tempSortFile = os.path.join(self.tempDir, "fileToSort.txt")
            makeFileToSort(tempSortFile, lines=lines, maxLineLength=maxLineLength)

            # First make our own sorted version
            with open(tempSortFile, 'r') as fileHandle:
                l = fileHandle.readlines()
                l.sort()

            # Make the first job
            firstJob = Job.wrapJobFn(setup, tempSortFile, N, memory=5000)

            # Check we get an exception if we try to restart a workflow that doesn't exist
            options.restart = True
            try:
                Job.Runner.startToil(firstJob, options)
                self.fail()
            except JobStoreCreationException:
                pass

            options.restart = False

            # Now actually run the workflow
            i = Job.Runner.startToil(firstJob, options)

            # Check we get an exception if we try to run without restart on an existing job store
            try:
                Job.Runner.startToil(firstJob, options)
                self.fail()
            except JobStoreCreationException:
                pass

            options.restart = True

            # This loop tests the restart behavior
            while i != 0:
                options.useExistingOptions = random.random() > 0.5
                i = Job.Runner.startToil(firstJob, options)

            # Now check that if you try to restart from here it will raise an exception
            # indicating that there are no jobs remaining in the workflow.
            try:
                Job.Runner.startToil(firstJob, options)
                self.fail()
            except JobException:
                pass
                #self.assertTrue(e.message.endswith('left in toil workflow (workflow has finished successfully?)'))

            # Now check the file is properly sorted..
            with open(tempSortFile, 'r') as fileHandle:
                l2 = fileHandle.readlines()
                checkEqual(l, l2)

    @needs_aws
    def testToilSortOnAWS(self):
        """Tests scriptTree/toil by sorting a file in parallel.
        """
        self.toilSortTest(jobStore="aws:us-west-2:sort-test-%s" % uuid4(),
                          lines=100, N=100)

    @needs_aws
    @needs_mesos
    def testScriptTree_SortSimpleOnAWSWithMesos(self):
        self._startMesos()
        try:
            self.toilSortTest(testNo=1,
                              batchSystem="mesos",
                              jobStore="aws:us-west-2:sort-test-%s" % uuid4(),
                              lines=100,
                              N=100)
        finally:
            self._stopMesos()

    def testToilSort(self):
        """
        Tests scriptTree/toil by sorting a file in parallel.
        """
        self.toilSortTest()

    # The following functions test the functions in the test!

    def testSort(self):
        for test in xrange(self.testNo):
            tempFile1 = os.path.join(self.tempDir, "fileToSort.txt")
            makeFileToSort(tempFile1)
            lines1 = loadFile(tempFile1)
            lines1.sort()
            sort(tempFile1)
            with open(tempFile1, 'r') as f:
                lines2 = f.readlines()
            checkEqual(lines1, lines2)

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
            lines1 = loadFile(tempFile1) + loadFile(tempFile2)
            lines1.sort()
            with open(tempFile3, 'r') as f:
                lines2 = f.readlines()
            checkEqual(lines1, lines2)

    def testCopySubRangeOfFile(self):
        for test in xrange(self.testNo):
            tempFile = os.path.join(self.tempDir, "fileToSort1.txt")
            outputFile = os.path.join(self.tempDir, "outputFileToSort1.txt")
            makeFileToSort(tempFile)
            fileSize = os.path.getsize(tempFile)
            assert fileSize > 0
            fileStart = random.choice(xrange(0, fileSize))
            fileEnd = random.choice(xrange(fileStart, fileSize))
            fileHandle = open(outputFile, 'w')
            copySubRangeOfFile(tempFile, fileStart, fileEnd, fileHandle)
            fileHandle.close()
            l = open(outputFile, 'r').read()
            l2 = open(tempFile, 'r').read()[fileStart:fileEnd]
            checkEqual(l, l2)

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


###########################################
# Functions to generate file store and check result is okay
###########################################  

def checkEqual(i, j):
    if i != j:
        print "lengths", len(i), len(j)
        print "true", i
        print "false", j
    assert i == j

def loadFile(file):
    with open(file, 'r') as fileHandle:
        return fileHandle.readlines()

def getRandomLine(maxLineLength):
    return "".join(
        [random.choice(['a', 'c', 't', 'g', "A", "C", "G", "T", "N", "X", "Y", "Z"]) for i in
         xrange(maxLineLength)]) + "\n"

def makeFileToSort(fileName, lines=10, maxLineLength=10):
    with open(fileName, 'w') as fileHandle:
        for line in xrange(lines):
            fileHandle.write(getRandomLine(maxLineLength))

if __name__ == '__main__':
    unittest.main()
