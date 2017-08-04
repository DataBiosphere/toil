# Copyright (C) 2015-2016 Regents of the University of California
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

"""A demonstration of toil. Sorts the lines of a file into ascending order by doing a parallel merge sort.
"""
from __future__ import absolute_import
from __future__ import division
from builtins import range
from past.utils import old_div
from argparse import ArgumentParser
import os
import random
import logging
import shutil

from toil.common import Toil
from toil.job import Job

defaultLines = 1000
defaultLineLen = 50
sortMemory = '1000M'


def setup(job, inputFile, N, downCheckpoints):
    """
    Sets up the sort.
    Returns the FileID of the sorted file
    """
    job.fileStore.logToMaster("Starting the merge sort")
    return job.addChildJobFn(down,
                             inputFile, N,
                             downCheckpoints,
                             memory='1000M').rv()


def down(job, inputFileStoreID, N, downCheckpoints, memory=sortMemory):
    """
    Input is a file and a range into that file to sort and an output location in which
    to write the sorted file.
    If the range is larger than a threshold N the range is divided recursively and
    a follow on job is then created which merges back the results else
    the file is sorted and placed in the output.
    """
    # Read the file
    inputFile = job.fileStore.readGlobalFile(inputFileStoreID, cache=False)
    length = os.path.getsize(inputFile)
    if length > N:
        # We will subdivide the file
        job.fileStore.logToMaster("Splitting file: %s of size: %s"
                                  % (inputFileStoreID, length), level=logging.CRITICAL)
        # Split the file into two copies
        midPoint = getMidPoint(inputFile, 0, length)
        t1 = job.fileStore.getLocalTempFile()
        with open(t1, 'w') as fH:
            copySubRangeOfFile(inputFile, 0, midPoint+1, fH)
        t2 = job.fileStore.getLocalTempFile()
        with open(t2, 'w') as fH:
            copySubRangeOfFile(inputFile, midPoint+1, length, fH)
        # Call down recursively. By giving the rv() of the two jobs as inputs to the follow-on job, up,
        # we communicate the dependency without hindering concurrency.
        return job.addFollowOnJobFn(up,
                                    job.addChildJobFn(down, job.fileStore.writeGlobalFile(t1), N, downCheckpoints,
                                                      checkpoint=downCheckpoints, memory=sortMemory).rv(),
                                    job.addChildJobFn(down, job.fileStore.writeGlobalFile(t2), N, downCheckpoints,
                                                      checkpoint=downCheckpoints, memory=sortMemory).rv()).rv()
    else:
        # We can sort this bit of the file
        job.fileStore.logToMaster("Sorting file: %s of size: %s"
                                  % (inputFileStoreID, length), level=logging.CRITICAL)
        # Sort the copy and write back to the fileStore
        shutil.copyfile(inputFile, inputFile + '.sort')
        sort(inputFile + '.sort')
        return job.fileStore.writeGlobalFile(inputFile + '.sort')


def up(job, inputFileID1, inputFileID2, memory=sortMemory):
    """
    Merges the two files and places them in the output.
    """
    with job.fileStore.writeGlobalFileStream() as (fileHandle, outputFileStoreID):
        with job.fileStore.readGlobalFileStream(inputFileID1) as inputFileHandle1:
            with job.fileStore.readGlobalFileStream(inputFileID2) as inputFileHandle2:
                merge(inputFileHandle1, inputFileHandle2, fileHandle)
                job.fileStore.logToMaster("Merging %s and %s to %s"
                                          % (inputFileID1, inputFileID2, outputFileStoreID))
        # Cleanup up the input files - these deletes will occur after the completion is successful.
        job.fileStore.deleteGlobalFile(inputFileID1)
        job.fileStore.deleteGlobalFile(inputFileID2)
        return outputFileStoreID


def sort(file):
    """
    Sorts the given file.
    """
    fileHandle = open(file, 'r')
    lines = fileHandle.readlines()
    fileHandle.close()
    lines.sort()
    fileHandle = open(file, 'w')
    for line in lines:
        fileHandle.write(line)
    fileHandle.close()


def merge(fileHandle1, fileHandle2, outputFileHandle):
    """
    Merges together two files maintaining sorted order.
    """
    line2 = fileHandle2.readline()
    for line1 in fileHandle1.readlines():
        while line2 != '' and line2 <= line1:
            outputFileHandle.write(line2)
            line2 = fileHandle2.readline()
        outputFileHandle.write(line1)
    while line2 != '':
        outputFileHandle.write(line2)
        line2 = fileHandle2.readline()


def copySubRangeOfFile(inputFile, fileStart, fileEnd, outputFileHandle):
    """
    Copies the range (in bytes) between fileStart and fileEnd to the given
    output file handle.
    """
    with open(inputFile, 'r') as fileHandle:
        fileHandle.seek(fileStart)
        data = fileHandle.read(fileEnd - fileStart)
        assert len(data) == fileEnd - fileStart
        outputFileHandle.write(data)


def getMidPoint(file, fileStart, fileEnd):
    """
    Finds the point in the file to split.
    Returns an int i such that fileStart <= i < fileEnd
    """
    fileHandle = open(file, 'r')
    midPoint = old_div((fileStart + fileEnd), 2)
    assert midPoint >= fileStart
    fileHandle.seek(midPoint)
    line = fileHandle.readline()
    assert len(line) >= 1
    if len(line) + midPoint < fileEnd:
        return midPoint + len(line) - 1
    fileHandle.seek(fileStart)
    line = fileHandle.readline()
    assert len(line) >= 1
    assert len(line) + fileStart <= fileEnd
    return len(line) + fileStart - 1


def makeFileToSort(fileName, lines=defaultLines, lineLen=defaultLineLen):
    with open(fileName, 'w') as fileHandle:
        for _ in range(lines):
            line = "".join(random.choice('actgACTGNXYZ') for _ in range(lineLen - 1)) + '\n'
            fileHandle.write(line)


def main(options=None):
    if not options:
        parser = ArgumentParser()
        Job.Runner.addToilOptions(parser)
        parser.add_argument('--numLines', default=defaultLines, help='Number of lines in file to sort.', type=int)
        parser.add_argument('--lineLength', default=defaultLineLen, help='Length of lines in file to sort.', type=int)
        parser.add_argument("--fileToSort", dest="fileToSort", help="The file you wish to sort")
        parser.add_argument("--N", dest="N",
                            help="The threshold below which a serial sort function is used to sort file. "
                                 "All lines must of length less than or equal to N or program will fail",
                            default=10000)
        parser.add_argument('--downCheckpoints', action='store_true',
                            help='If this option is set, the workflow will make checkpoints on its way through'
                                 'the recursive "down" part of the sort')
        options = parser.parse_args()

    fileName = options.fileToSort

    if options.fileToSort is None:
        # make the file ourselves
        fileName = 'fileToSort.txt'
        print 'No sort file specified. Generating one automatically called %s.' % fileName
        makeFileToSort(fileName=fileName, lines=options.numLines, lineLen=options.lineLength)
    else:
        if not os.path.exists(options.fileToSort):
            raise RuntimeError("File to sort does not exist: %s" % options.fileToSort)

    if int(options.N) <= 0:
        raise RuntimeError("Invalid value of N: %s" % options.N)

    # Now we are ready to run
    with Toil(options) as toil:
        sortFileURL = 'file://' + os.path.abspath(fileName)
        if not toil.options.restart:
            sortFileURL = 'file://' + os.path.abspath(fileName)
            sortFileID = toil.importFile(sortFileURL)
            sortedFileID = toil.start(Job.wrapJobFn(setup, sortFileID, int(options.N), options.downCheckpoints,
                                                    memory=sortMemory))
        else:
            sortedFileID = toil.restart()
        toil.exportFile(sortedFileID, sortFileURL)
if __name__ == '__main__':
    main()
