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
import logging
import shutil
from bd2k.util.humanize import human2bytes

from toil.job import Job
from toil.test.sort.lib import merge, sort, copySubRangeOfFile, getMidPoint

sortMemory = human2bytes('1000M')

def setup(job, inputFile, N, downCheckpoints):
    """Sets up the sort.
    """
    #Write the input file to the file store
    inputFileStoreID = job.fileStore.writeGlobalFile(inputFile, True)
    job.fileStore.logToMaster(" Starting the merge sort ")
    job.addFollowOnJobFn(cleanup, job.addChildJobFn(down, 
                        inputFileStoreID, N, downCheckpoints, 
                        checkpoint=downCheckpoints).rv(), inputFile)

def down(job, inputFileStoreID, N, downCheckpoints, memory=sortMemory):
    """Input is a file and a range into that file to sort and an output location in which
    to write the sorted file.
    If the range is larger than a threshold N the range is divided recursively and
    a follow on job is then created which merges back the results else
    the file is sorted and placed in the output.
    """
    #Read the file
    inputFile = job.fileStore.readGlobalFile(inputFileStoreID, cache=False)
    length = os.path.getsize(inputFile)
    if length > N:
        #We will subdivide the file
        job.fileStore.logToMaster( "Splitting file: %s of size: %s"
                                      % (inputFileStoreID, length), level=logging.CRITICAL )
        #Split the file into two copies
        midPoint = getMidPoint(inputFile, 0, length)
        t1 = job.fileStore.getLocalTempFile()
        with open(t1, 'w') as fH:
            copySubRangeOfFile(inputFile, 0, midPoint+1, fH)
        t2 = job.fileStore.getLocalTempFile()
        with open(t2, 'w') as fH:
            copySubRangeOfFile(inputFile, midPoint+1, length, fH)
        #Call down recursively
        return job.addFollowOnJobFn(up,
            job.addChildJobFn(down, job.fileStore.writeGlobalFile(t1), N, 
                              downCheckpoints, checkpoint=downCheckpoints, memory=sortMemory).rv(),
            job.addChildJobFn(down, job.fileStore.writeGlobalFile(t2), N, 
                              downCheckpoints, checkpoint=downCheckpoints, memory=sortMemory).rv()).rv()          
    else:
        #We can sort this bit of the file
        job.fileStore.logToMaster( "Sorting file: %s of size: %s"
                                      % (inputFileStoreID, length), level=logging.CRITICAL )
        #Sort the copy and write back to the fileStore
        sort(inputFile)
        return job.fileStore.writeGlobalFile(inputFile)

def up(job, inputFileID1, inputFileID2, memory=sortMemory):
    """Merges the two files and places them in the output.
    """
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

def cleanup(job, tempOutputFileStoreID, outputFile, cores=1, memory=sortMemory, disk="3G"):
    """Copies back the temporary file to input once we've successfully sorted the temporary file.
    """
    job.fileStore.readGlobalFile(tempOutputFileStoreID, userPath=outputFile)
    job.fileStore.logToMaster("Finished copying sorted file to output: %s" % outputFile)

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
    Job.Runner.startToil(Job.wrapJobFn(setup, options.fileToSort, int(options.N), False,
                                       memory=sortMemory), options)

if __name__ == '__main__':
    main()
