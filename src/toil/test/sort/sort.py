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

"""A demonstration of toil. Sorts the lines of a file into ascending order by doing a parallel merge sort.
"""
from __future__ import absolute_import
from argparse import ArgumentParser
import os
import random
from bd2k.util.humanize import human2bytes

from toil.job import Job
from toil.test.sort.lib import merge, sort, copySubRangeOfFile, getMidPoint

success_ratio = 0.5
sortMemory = human2bytes('1000M')

def setup(job, inputFile, N):
    """Sets up the sort.
    """
    job.addFollowOnJobFn(cleanup, job.addChildJobFn(down, 
        inputFile, 0, os.path.getsize(inputFile), N).rv(), inputFile, memory=sortMemory)

def down(job, inputFile, fileStart, fileEnd, N):
    """Input is a file and a range into that file to sort and an output location in which
    to write the sorted file.
    If the range is larger than a threshold N the range is divided recursively and
    a follow on job is then created which merges back the results else
    the file is sorted and placed in the output.
    """
    if random.random() > success_ratio:
        raise RuntimeError() #This error is a test error, it does not mean the tests have failed.
    length = fileEnd - fileStart
    if length > N:
        #We will subdivide the file
        job.fileStore.logToMaster( "Splitting range (%i..%i) of file: %s"
                                      % (fileStart, fileEnd, inputFile) )
        midPoint = getMidPoint(inputFile, fileStart, fileEnd)
        return job.addFollowOnJobFn(up,
            job.addChildJobFn(down, inputFile, fileStart, midPoint+1, N, memory=sortMemory).rv(),
            job.addChildJobFn(down, inputFile, midPoint+1, fileEnd, N, memory=sortMemory).rv()).rv()          
    else:
        #We can sort this bit of the file
        job.fileStore.logToMaster( "Sorting range (%i..%i) of file: %s"
                                      % (fileStart, fileEnd, inputFile) )
        t = job.fileStore.getLocalTempFile()
        with open(t, 'w') as fH:
            copySubRangeOfFile(inputFile, fileStart, fileEnd, fH)
        sort(t)
        return job.fileStore.writeGlobalFile(t)

def up(job, inputFileID1, inputFileID2):
    """Merges the two files and places them in the output.
    """
    if random.random() > success_ratio:
        raise RuntimeError() #This error is a test error, it does not mean the tests have failed.
    with job.fileStore.writeGlobalFileStream() as (fileHandle, outputFileStoreID):
        with job.fileStore.readGlobalFileStream( inputFileID1 ) as inputFileHandle1:
            with job.fileStore.readGlobalFileStream( inputFileID2 ) as inputFileHandle2:
                merge(inputFileHandle1, inputFileHandle2, fileHandle)
                job.fileStore.logToMaster( "Merging %s and %s to %s"
                                       % (inputFileID1, inputFileID2, outputFileStoreID) )
        #Cleanup up the input files - these deletes will occur after the completion is successful. 
        job.fileStore.deleteGlobalFile(inputFileID1)
        job.fileStore.deleteGlobalFile(inputFileID2)
        return outputFileStoreID

def cleanup(job, tempOutputFileStoreID, outputFile):
    """Copies back the temporary file to input once we've successfully sorted the temporary file.
    """
    if random.random() > success_ratio:
        raise RuntimeError() #This is a test error and not a failure of the tests
    job.fileStore.readGlobalFile(tempOutputFileStoreID, outputFile)
    #sort(outputFile)

def main():
    parser = ArgumentParser()
    Job.Runner.addToilOptions(parser)

    parser.add_argument("--fileToSort", dest="fileToSort",
                      help="The file you wish to sort")

    parser.add_argument("--N", dest="N",
                      help="The threshold below which a serial sort function is"
                      "used to sort file. All lines must of length less than or equal to N or program will fail",
                      default=10000)

    options = parser.parse_args()

    if options.fileToSort is None:
        raise RuntimeError("No file to sort given")

    if not os.path.exists(options.fileToSort):
        raise RuntimeError("File to sort does not exist: %s" % options.fileToSort)

    if int(options.N) <= 0:
        raise RuntimeError("Invalid value of N: %s" % options.N)

    #Now we are ready to run
    Job.Runner.startToil(Job.wrapJobFn(setup, options.fileToSort, int(options.N),
                                       memory=sortMemory), options)

if __name__ == '__main__':
    main()
