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

from toil.job import Job
from toil.test.sort.lib import merge, sort, copySubRangeOfFile, getMidPoint

success_ratio = 0.5

def setup(job, inputFile, N):
    """Sets up the sort.
    """
    tempOutputFileStoreID = job.fileStore.getEmptyFileStoreID()
    job.addChildJobFn(down, inputFile, 0, os.path.getsize(inputFile), N, tempOutputFileStoreID)
    job.addFollowOnJobFn(cleanup, tempOutputFileStoreID, inputFile)

def down(job, inputFile, fileStart, fileEnd, N, outputFileStoreID):
    """Input is a file and a range into that file to sort and an output location in which
    to write the sorted file.
    If the range is larger than a threshold N the range is divided recursively and
    a follow on job is then created which merges back the results else
    the file is sorted and placed in the output.
    """
    if random.random() > success_ratio:
        raise RuntimeError() #This error is a test error, it does not mean the tests have failed.
    length = fileEnd - fileStart
    assert length >= 0
    if length > N:
        job.fileStore.logToMaster( "Splitting range (%i..%i) of file: %s"
                                      % (fileStart, fileEnd, inputFile) )
        midPoint = getMidPoint(inputFile, fileStart, fileEnd)
        assert midPoint >= fileStart
        assert midPoint+1 < fileEnd
        #We will subdivide the file
        tempFileStoreID1 = job.fileStore.getEmptyFileStoreID()
        tempFileStoreID2 = job.fileStore.getEmptyFileStoreID()
        #The use of rv here is for testing purposes
        #The rv() of the first child job is tempFileStoreID1,
        #similarly rv() of the second child is tempFileStoreID2
        job.addFollowOnJobFn(up,
                                   job.addChildJobFn(down, inputFile, fileStart,
                                                           midPoint+1, N, tempFileStoreID1).rv(),
                                   job.addChildJobFn(down, inputFile, midPoint+1,
                                                           fileEnd, N, tempFileStoreID2).rv(), #Add one to avoid the newline
                                   outputFileStoreID)                
    else:
        #We can sort this bit of the file
        job.fileStore.logToMaster( "Sorting range (%i..%i) of file: %s"
                                      % (fileStart, fileEnd, inputFile) )
        with job.fileStore.updateGlobalFileStream(outputFileStoreID) as fileHandle:
            copySubRangeOfFile(inputFile, fileStart, fileEnd, fileHandle)
        #Make a local copy and sort the file
        tempOutputFile = job.fileStore.readGlobalFile(outputFileStoreID)
        sort(tempOutputFile)
        job.fileStore.updateGlobalFile(outputFileStoreID, tempOutputFile)
    return outputFileStoreID

def up(job, inputFileID1, inputFileID2, outputFileStoreID):
    """Merges the two files and places them in the output.
    """
    if random.random() > success_ratio:
        raise RuntimeError() #This error is a test error, it does not mean the tests have failed.
    with job.fileStore.updateGlobalFileStream(outputFileStoreID) as fileHandle:
        with job.fileStore.readGlobalFileStream( inputFileID1 ) as inputFileHandle1:
            with job.fileStore.readGlobalFileStream( inputFileID2 ) as inputFileHandle2:
                merge(inputFileHandle1, inputFileHandle2, fileHandle)
    job.fileStore.logToMaster( "Merging %s and %s to %s"
                                  % (inputFileID1, inputFileID2, outputFileStoreID) )

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
    Job.Runner.startToil(Job.wrapJobFn(setup, options.fileToSort, int(options.N)), options)

if __name__ == '__main__':
    main()
