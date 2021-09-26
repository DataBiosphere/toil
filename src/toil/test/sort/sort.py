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

"""A demonstration of toil. Sorts the lines of a file into ascending order by doing a parallel merge sort.
"""
import codecs
import os
import random
import shutil
from argparse import ArgumentParser

from toil.common import Toil
from toil.job import Job
from toil.realtimeLogger import RealtimeLogger

defaultLines = 1000
defaultLineLen = 50
sortMemory = '600M'


def setup(job, inputFile, N, downCheckpoints, options):
    """
    Sets up the sort.
    Returns the FileID of the sorted file
    """
    RealtimeLogger.info("Starting the merge sort")
    return job.addChildJobFn(down,
                             inputFile, N, 'root',
                             downCheckpoints,
                             options = options,
                             preemptable=True,
                             memory=sortMemory).rv()


def down(job, inputFileStoreID, N, path, downCheckpoints, options, memory=sortMemory):
    """
    Input is a file, a subdivision size N, and a path in the hierarchy of jobs.
    If the range is larger than a threshold N the range is divided recursively and
    a follow on job is then created which merges back the results else
    the file is sorted and placed in the output.
    """

    RealtimeLogger.info("Down job starting: %s" % path)

    # Read the file
    inputFile = job.fileStore.readGlobalFile(inputFileStoreID, cache=False)
    length = os.path.getsize(inputFile)
    if length > N:
        # We will subdivide the file
        RealtimeLogger.critical("Splitting file: %s of size: %s"
                % (inputFileStoreID, length))
        # Split the file into two copies
        midPoint = getMidPoint(inputFile, 0, length)
        t1 = job.fileStore.getLocalTempFile()
        with open(t1, 'w') as fH:
            fH.write(copySubRangeOfFile(inputFile, 0, midPoint+1))
        t2 = job.fileStore.getLocalTempFile()
        with open(t2, 'w') as fH:
            fH.write(copySubRangeOfFile(inputFile, midPoint+1, length))
        # Call down recursively. By giving the rv() of the two jobs as inputs to the follow-on job, up,
        # we communicate the dependency without hindering concurrency.
        result = job.addFollowOnJobFn(up,
                                    job.addChildJobFn(down, job.fileStore.writeGlobalFile(t1), N, path + '/0',
                                                      downCheckpoints, checkpoint=downCheckpoints, options=options,
                                                      preemptable=True, memory=options.sortMemory).rv(),
                                    job.addChildJobFn(down, job.fileStore.writeGlobalFile(t2), N, path + '/1',
                                                      downCheckpoints, checkpoint=downCheckpoints, options=options,
                                                      preemptable=True, memory=options.mergeMemory).rv(),
                                    path + '/up', preemptable=True, options=options, memory=options.sortMemory).rv()
    else:
        # We can sort this bit of the file
        RealtimeLogger.critical("Sorting file: %s of size: %s"
                % (inputFileStoreID, length))
        # Sort the copy and write back to the fileStore
        shutil.copyfile(inputFile, inputFile + '.sort')
        sort(inputFile + '.sort')
        result = job.fileStore.writeGlobalFile(inputFile + '.sort')

    RealtimeLogger.info("Down job finished: %s" % path)
    return result


def up(job, inputFileID1, inputFileID2, path, options, memory=sortMemory):
    """
    Merges the two files and places them in the output.
    """

    RealtimeLogger.info("Up job starting: %s" % path)

    with job.fileStore.writeGlobalFileStream() as (fileHandle, outputFileStoreID):
        fileHandle = codecs.getwriter('utf-8')(fileHandle)
        with job.fileStore.readGlobalFileStream(inputFileID1) as inputFileHandle1:
            inputFileHandle1 = codecs.getreader('utf-8')(inputFileHandle1)
            with job.fileStore.readGlobalFileStream(inputFileID2) as inputFileHandle2:
                inputFileHandle2 = codecs.getreader('utf-8')(inputFileHandle2)
                RealtimeLogger.info("Merging %s and %s to %s"
                    % (inputFileID1, inputFileID2, outputFileStoreID))
                merge(inputFileHandle1, inputFileHandle2, fileHandle)
        # Cleanup up the input files - these deletes will occur after the completion is successful.
        job.fileStore.deleteGlobalFile(inputFileID1)
        job.fileStore.deleteGlobalFile(inputFileID2)

        RealtimeLogger.info("Up job finished: %s" % path)

        return outputFileStoreID


def sort(file):
    """Sorts the given file."""
    with open(file) as f:
        lines = f.readlines()

    lines.sort()

    with open(file, 'w') as f:
        for line in lines:
            f.write(line)


def merge(fileHandle1, fileHandle2, outputFileHandle):
    """
    Merges together two files maintaining sorted order.

    All handles must be text-mode streams.
    """
    line2 = fileHandle2.readline()
    for line1 in fileHandle1.readlines():
        while len(line2) != 0 and line2 <= line1:
            outputFileHandle.write(line2)
            line2 = fileHandle2.readline()
        outputFileHandle.write(line1)
    while len(line2) != 0:
        outputFileHandle.write(line2)
        line2 = fileHandle2.readline()


def copySubRangeOfFile(inputFile, fileStart, fileEnd):
    """
    Copies the range (in bytes) between fileStart and fileEnd to the given
    output file handle.
    """
    with open(inputFile) as fileHandle:
        fileHandle.seek(fileStart)
        data = fileHandle.read(fileEnd - fileStart)
        assert len(data) == fileEnd - fileStart
    return data


def getMidPoint(file, fileStart, fileEnd):
    """
    Finds the point in the file to split.
    Returns an int i such that fileStart <= i < fileEnd
    """
    with open(file) as f:
        midPoint = (fileStart + fileEnd) // 2
        assert midPoint >= fileStart
        f.seek(midPoint)
        line = f.readline()
        assert len(line) >= 1
        if len(line) + midPoint < fileEnd:
            return midPoint + len(line) - 1
        f.seek(fileStart)
        line = f.readline()
        assert len(line) >= 1
        assert len(line) + fileStart <= fileEnd
    return len(line) + fileStart - 1


def makeFileToSort(fileName, lines=defaultLines, lineLen=defaultLineLen):
    with open(fileName, 'w') as f:
        for _ in range(lines):
            line = "".join(random.choice('actgACTGNXYZ') for _ in range(lineLen - 1)) + '\n'
            f.write(line)


def main(options=None):
    if not options:
        # deal with command line arguments
        parser = ArgumentParser()
        Job.Runner.addToilOptions(parser)
        parser.add_argument('--numLines', default=defaultLines, help='Number of lines in file to sort.', type=int)
        parser.add_argument('--lineLength', default=defaultLineLen, help='Length of lines in file to sort.', type=int)
        parser.add_argument("--fileToSort", help="The file you wish to sort")
        parser.add_argument("--outputFile", help="Where the sorted output will go")
        parser.add_argument("--overwriteOutput", help="Write over the output file if it already exists.", default=True)
        parser.add_argument("--N", dest="N",
                            help="The threshold below which a serial sort function is used to sort file. "
                                 "All lines must of length less than or equal to N or program will fail",
                            default=10000)
        parser.add_argument('--downCheckpoints', action='store_true',
                            help='If this option is set, the workflow will make checkpoints on its way through'
                                 'the recursive "down" part of the sort')
        parser.add_argument("--sortMemory", dest="sortMemory",
                        help="Memory for jobs that sort chunks of the file.",
                        default=None)

        parser.add_argument("--mergeMemory", dest="mergeMemory",
                        help="Memory for jobs that collate results.",
                        default=None)

        options = parser.parse_args()
    if not hasattr(options, "sortMemory") or not options.sortMemory:
        options.sortMemory = sortMemory
    if not hasattr(options, "mergeMemory") or not options.mergeMemory:
        options.mergeMemory = sortMemory

    # do some input verification
    sortedFileName = options.outputFile or "sortedFile.txt"
    if not options.overwriteOutput and os.path.exists(sortedFileName):
        print(f'Output file {sortedFileName} already exists.  '
              f'Delete it to run the sort example again or use --overwriteOutput=True')
        exit()

    fileName = options.fileToSort
    if options.fileToSort is None:
        # make the file ourselves
        fileName = 'fileToSort.txt'
        if os.path.exists(fileName):
            print(f'Sorting existing file: {fileName}')
        else:
            print(f'No sort file specified. Generating one automatically called: {fileName}.')
            makeFileToSort(fileName=fileName, lines=options.numLines, lineLen=options.lineLength)
    else:
        if not os.path.exists(options.fileToSort):
            raise RuntimeError("File to sort does not exist: %s" % options.fileToSort)

    if int(options.N) <= 0:
        raise RuntimeError("Invalid value of N: %s" % options.N)

    # Now we are ready to run
    with Toil(options) as workflow:
        sortedFileURL = 'file://' + os.path.abspath(sortedFileName)
        if not workflow.options.restart:
            sortFileURL = 'file://' + os.path.abspath(fileName)
            sortFileID = workflow.importFile(sortFileURL)
            sortedFileID = workflow.start(Job.wrapJobFn(setup,
                                                        sortFileID,
                                                        int(options.N),
                                                        options.downCheckpoints,
                                                        options=options,
                                                        memory=sortMemory))
        else:
            sortedFileID = workflow.restart()
        workflow.exportFile(sortedFileID, sortedFileURL)


if __name__ == '__main__':
    main()
