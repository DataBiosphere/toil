#!/usr/bin/env python

"""
A demonstration of jobTree. 
Sorts the lines of a file into ascending order by doing a parallel merge sort.
"""
from optparse import OptionParser
import os
import random
from jobTree.src.target import Target
from jobTree.src.stack import Stack
from jobTree.test.sort.lib import merge, sort, copySubRangeOfFile, getMidPoint

success_ratio = 0.5

def setup(target, inputFile, N):
    """
    Sets up the sort.
    """
    sortedFileID = target.addChildTargetFn(down, inputFile, 0, 
                                         os.path.getsize(inputFile), N).rv(0)
    target.setFollowOnTargetFn(cleanup, sortedFileID, inputFile)

def down(target, inputFile, fileStart, fileEnd, N):
    """
    Input is a file and a range into that file to sort. Returns a jobStoreFileID
    referencing the sorted file.
    If the range is larger than a threshold N the range is divided recursively and
    a follow on job is then created which merges back the results else
    the file is sorted directly.
    """
    if random.random() > success_ratio:
        raise RuntimeError() #This error is a test error, it does not mean the tests have failed.
    length = fileEnd - fileStart
    assert length >= 0
    if length > N:
        target.logToMaster( "Splitting range (%i..%i) of file: %s"
                            % (fileStart, fileEnd, inputFile) )
        midPoint = getMidPoint(inputFile, fileStart, fileEnd)
        assert midPoint >= fileStart
        assert midPoint+1 < fileEnd
        #We will subdivide the file
        outputFileID1 = target.addChildTargetFn(down, inputFile, 
                                                fileStart, midPoint+1, N).rv(0)
        #Add one midPoint to avoid the newline 
        outputFileID2 = target.addChildTargetFn(down, inputFile, 
                                                midPoint+1, fileEnd, N).rv(0)
        return target.setFollowOnTargetFn(up, outputFileID1, outputFileID2).rv(0)         
    else:
        #We can sort this bit of the file
        target.logToMaster( "Sorting range (%i..%i) of file: %s"
                            % (fileStart, fileEnd, inputFile) )
        with target.writeGlobalFileStream() as (fileHandle, outputFileStoreID):
            copySubRangeOfFile(inputFile, fileStart, fileEnd, fileHandle)
        #Make a local copy and sort the file
        tempOutputFile = target.readGlobalFile(outputFileStoreID)
        sort(tempOutputFile)
        target.updateGlobalFile(outputFileStoreID, tempOutputFile)
        return outputFileStoreID

def up(target, inputFileID1, inputFileID2):
    """
    Merges the two files, return a jobStoreFileID reference to the sorted output file.
    """
    if random.random() > success_ratio:
        raise RuntimeError() #This error is a test error, it does not mean the tests have failed.
    with target.writeGlobalFileStream() as (fileHandle, outputFileStoreID):
        with target.readGlobalFileStream( inputFileID1 ) as inputFileHandle1:
            with target.readGlobalFileStream( inputFileID2 ) as inputFileHandle2:
                merge(inputFileHandle1, inputFileHandle2, fileHandle)
    target.logToMaster( "Merging %s and %s to %s"
                        % (inputFileID1, inputFileID2, outputFileStoreID) )
    return outputFileStoreID

def cleanup(target, tempOutputFileStoreID, outputFile):
    """
    Copies back the temporary file to input once we've successfully 
    sorted the temporary file.
    """
    if random.random() > success_ratio:
        raise RuntimeError() #This is a test error and not a failure of the tests
    target.readGlobalFile(tempOutputFileStoreID, outputFile)
    #sort(outputFile)

def main():
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    
    parser.add_option("--fileToSort", dest="fileToSort",
                      help="The file you wish to sort")
    
    parser.add_option("--N", dest="N",
                      help="The threshold below which a serial sort function is used to sort file. All lines must of length less than or equal to N or program will fail", 
                      default=10000)
    
    options, args = parser.parse_args()
    
    if options.fileToSort is None:
        raise RuntimeError("No file to sort given")

    if not os.path.exists(options.fileToSort):
        raise RuntimeError("File to sort does not exist: %s" % options.fileToSort)
    
    if int(options.N) <= 0:
        raise RuntimeError("Invalid value of N: %s" % options.N)
    
    if len(args) != 0:
        raise RuntimeError("Unrecognised input arguments: %s" % " ".join(args))
    
    #Now we are ready to run
    i = Stack(Target.wrapTargetFn(setup, options.fileToSort, int(options.N))).startJobTree(options)
    
    #if i:
    #    raise RuntimeError("The jobtree contained %i failed jobs" % i)

if __name__ == '__main__':
    main()
