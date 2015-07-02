#!/usr/bin/env python

"""A demonstration of jobTree. Sorts the lines of a file into ascending order by doing a parallel merge sort.
"""
from optparse import OptionParser
import os
import random

from jobTree.target import Target
from jobTree.test.sort.lib import merge, sort, copySubRangeOfFile, getMidPoint

success_ratio = 0.5
logToMaster = False

def setup(target, inputFile, N):
    """Sets up the sort.
    """
    tempOutputFileStoreID = target.getEmptyFileStoreID()
    target.addChildTargetFn(down, inputFile, 0, os.path.getsize(inputFile), N, tempOutputFileStoreID)
    target.setFollowOnTargetFn(cleanup, tempOutputFileStoreID, inputFile)

def down(target, inputFile, fileStart, fileEnd, N, outputFileStoreID):
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
        if logToMaster:
            target.logToMaster( "Splitting range (%i..%i) of file: %s"
                                % (fileStart, fileEnd, inputFile) )
        midPoint = getMidPoint(inputFile, fileStart, fileEnd)
        assert midPoint >= fileStart
        assert midPoint+1 < fileEnd
        #We will subdivide the file
        tempFileStoreID1 = target.getEmptyFileStoreID()
        tempFileStoreID2 = target.getEmptyFileStoreID()
        #The use of rv here is for testing purposes
        #The rv(0) of the first child target is tempFileStoreID1, 
        #similarly rv(0) of the second child is tempFileStoreID2
        target.setFollowOnTargetFn(up, 
                                   target.addChildTargetFn(down, inputFile, fileStart, 
                                                           midPoint+1, N, tempFileStoreID1).rv(0),
                                   target.addChildTargetFn(down, inputFile, midPoint+1, 
                                                           fileEnd, N, tempFileStoreID2).rv(0), #Add one to avoid the newline
                                   outputFileStoreID)                
    else:
        #We can sort this bit of the file
        if logToMaster:
            target.logToMaster( "Sorting range (%i..%i) of file: %s"
                                % (fileStart, fileEnd, inputFile) )
        with target.updateGlobalFileStream(outputFileStoreID) as fileHandle:
            copySubRangeOfFile(inputFile, fileStart, fileEnd, fileHandle)
        #Make a local copy and sort the file
        tempOutputFile = target.readGlobalFile(outputFileStoreID)
        sort(tempOutputFile)
        target.updateGlobalFile(outputFileStoreID, tempOutputFile)
    return outputFileStoreID

def up(target, inputFileID1, inputFileID2, outputFileStoreID):
    """Merges the two files and places them in the output.
    """
    if random.random() > success_ratio:
        raise RuntimeError() #This error is a test error, it does not mean the tests have failed.
    with target.updateGlobalFileStream(outputFileStoreID) as fileHandle:
        with target.readGlobalFileStream( inputFileID1 ) as inputFileHandle1:
            with target.readGlobalFileStream( inputFileID2 ) as inputFileHandle2:
                merge(inputFileHandle1, inputFileHandle2, fileHandle)
    if logToMaster:
        target.logToMaster( "Merging %s and %s to %s"
                            % (inputFileID1, inputFileID2, outputFileStoreID) )

def cleanup(target, tempOutputFileStoreID, outputFile):
    """Copies back the temporary file to input once we've successfully sorted the temporary file.
    """
    if random.random() > success_ratio:
        raise RuntimeError() #This is a test error and not a failure of the tests
    target.readGlobalFile(tempOutputFileStoreID, outputFile)
    #sort(outputFile)

def main():
    parser = OptionParser()
    Target.addJobTreeOptions(parser)
    
    parser.add_option("--fileToSort", dest="fileToSort",
                      help="The file you wish to sort")
    
    parser.add_option("--N", dest="N",
                      help="The threshold below which a serial sort function is"
                      "used to sort file. All lines must of length less than or equal to N or program will fail", 
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
    i = Target(Target.wrapTargetFn(setup, options.fileToSort, int(options.N))).startJobTree(options)
    
    if i:
        raise RuntimeError("The jobtree contained %i failed jobs" % i)

if __name__ == '__main__':
    main()
