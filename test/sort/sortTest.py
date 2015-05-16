#!/usr/bin/env python
"""Tests the scriptTree jobTree-script compiler.
"""
import logging

import unittest
import sys
import os
import random
from uuid import uuid4

from sonLib.bioio import TestStatus
from sonLib.bioio import parseSuiteTestOptions
from sonLib.bioio import system
from sonLib.bioio import getTempDirectory
from sonLib.bioio import getTempFile

from jobTree.src.common import parasolIsInstalled, gridEngineIsInstalled, workflowRootPath

from jobTree.test.sort.lib import merge, sort, copySubRangeOfFile, getMidPoint
from test import JobTreeTest


class TestCase(JobTreeTest):


    def setUp(self):
        super( TestCase, self ).setUp( )
        self.testNo = TestStatus.getTestSetup(1, 2, 10, 10)

    def testScriptTree_SortSimple(self):
        """Tests scriptTree/jobTree by sorting a file in parallel.
        """
        scriptTree_SortTest(self.testNo, "singleMachine")


    def testScriptTree_SortGridEngine(self):
        #Tests scriptTree/jobTree by sorting a file in parallel.
        if gridEngineIsInstalled():
            scriptTree_SortTest(self.testNo, "gridengine")

    def testScriptTree_Parasol(self):
        #Tests scriptTree/jobTree by sorting a file in parallel.
        if parasolIsInstalled():
            scriptTree_SortTest(self.testNo, "parasol")

    """
    def testScriptTree_SortAcid(self):
        #Tests scriptTree/jobTree by sorting a file in parallel.
        scriptTree_SortTest(self.testNo, "acid_test")
    """

    #The following functions test the functions in the test!

    def testSort(self):
        for test in xrange(self.testNo):
            tempDir = getTempDirectory(os.getcwd())
            tempFile1 = getTempFile(rootDir=tempDir)
            makeFileToSort(tempFile1)
            lines1 = loadFile(tempFile1)
            lines1.sort()
            sort(tempFile1)
            lines2 = loadFile(tempFile1)
            checkEqual(lines1, lines2)
            system("rm -rf %s" % tempDir)

    def testMerge(self):
        for test in xrange(self.testNo):
            tempDir = getTempDirectory(os.getcwd())
            tempFile1 = getTempFile(rootDir=tempDir)
            tempFile2 = getTempFile(rootDir=tempDir)
            tempFile3 = getTempFile(rootDir=tempDir)
            makeFileToSort(tempFile1)
            makeFileToSort(tempFile2)
            sort(tempFile1)
            sort(tempFile2)
            with open(tempFile3, 'w') as fileHandle:
                with open( tempFile1 ) as tempFileHandle1:
                    with open( tempFile2 ) as tempFileHandle2:
                        merge(tempFileHandle1, tempFileHandle2, fileHandle)
            lines1 = loadFile(tempFile1) + loadFile(tempFile2)
            lines1.sort()
            lines2 = loadFile(tempFile3)
            checkEqual(lines1, lines2)
            system("rm -rf %s" % tempDir)

    def testCopySubRangeOfFile(self):
        for test in xrange(self.testNo):
            tempDir = getTempDirectory(os.getcwd())
            tempFile = getTempFile(rootDir=tempDir)
            outputFile = getTempFile(rootDir=tempDir)
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
            system("rm -rf %s" % tempDir)

    def testGetMidPoint(self):
        for test in xrange(self.testNo):
            tempDir = getTempDirectory(os.getcwd())
            tempFile = getTempFile(rootDir=tempDir)
            makeFileToSort(tempFile)
            l = open(tempFile, 'r').read()
            fileSize = os.path.getsize(tempFile)
            midPoint = getMidPoint(tempFile, 0, fileSize)
            print "the mid point is %i of a file of %i bytes woth byte" % (midPoint, fileSize)
            assert midPoint < fileSize
            assert l[midPoint] == '\n'
            assert midPoint >= 0
            system("rm -rf %s" % tempDir)

def scriptTree_SortTest(testNo, batchSystem, jobStore='file', lines=10000, maxLineLength=10, N=10000):
    """Tests scriptTree/jobTree by sorting a file in parallel.
    """
    for test in xrange(testNo):
        tempDir = getTempDirectory(os.getcwd())
        tempFile = getTempFile(rootDir=tempDir)
        if jobStore == 'file':
            jobTreeDir = os.path.join(tempDir, "testJobTree")
        else:
            jobTreeDir = jobStore
        makeFileToSort(tempFile, lines=lines, maxLineLength=maxLineLength)
        #First make our own sorted version
        fileHandle = open(tempFile, 'r')
        l = fileHandle.readlines()
        l.sort()
        fileHandle.close()
        #Sort the file
        rootPath = os.path.join(workflowRootPath(), "test/sort")
        while True:
            command = "{rootPath}/sort.py " \
                      "--jobTree '{jobTreeDir}' " \
                      "--logLevel=DEBUG " \
                      "--fileToSort='{tempFile}' " \
                      "--N {N:d} " \
                      "--batchSystem {batchSystem} " \
                      "--jobTime 1000.0 " \
                      "--maxCpus 20 " \
                      "--retryCount 2".format( **locals() )
            system(command)
            try:
                system("jobTreeStatus --jobTree %s --failIfNotComplete" % jobTreeDir)
                break
            except:
                print "The jobtree failed and will be restarted"
                #raise RuntimeError()
                continue

        #Now check the file is properly sorted..
        #Now get the sorted file
        fileHandle = open(tempFile, 'r')
        l2 = fileHandle.readlines()
        fileHandle.close()
        checkEqual(l, l2)
        system("rm -rf %s" % tempDir)

def checkEqual(i, j):
    if i != j:
        print "lengths", len(i), len(j)
        print "true", i
        print "false", j
    assert i == j

def loadFile(file):
    fileHandle = open(file, 'r')
    lines = fileHandle.readlines()
    fileHandle.close()
    return lines

def getRandomLine(maxLineLength):
    return "".join([ random.choice([ 'a', 'c', 't', 'g', "A", "C", "G", "T", "N", "X", "Y", "Z" ]) for i in xrange(maxLineLength) ]) + "\n"

def makeFileToSort(fileName, lines=10, maxLineLength=10):
    fileHandle = open(fileName, 'w')
    for line in xrange(lines):
        fileHandle.write(getRandomLine(maxLineLength))
    fileHandle.close()

def main():
    parseSuiteTestOptions()
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()
