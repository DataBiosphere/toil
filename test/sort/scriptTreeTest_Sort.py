#!/usr/bin/env python

"""A demonstration of jobTree. Sorts the lines of a file into ascending order by doing a parallel merge sort.
"""
from optparse import OptionParser
import os
import random
import shutil
from sonLib.bioio import getTempFile
from jobTree.scriptTree.target import Target
from jobTree.scriptTree.stack import Stack
from jobTree.test.sort.lib import merge, sort, copySubRangeOfFile, getMidPoint

def setup(target, inputFile, N):
    """Sets up the sort.
    """
    tempOutputFile = getTempFile(rootDir=target.getGlobalTempDir())
    target.addChildTargetFn(down, (inputFile, 0, os.path.getsize(inputFile), N, tempOutputFile))
    target.setFollowOnFn(cleanup, (tempOutputFile, inputFile))

def down(target, inputFile, fileStart, fileEnd, N, outputFile):
    """Input is a file and a range into that file to sort and an output location in which
    to write the sorted file.
    If the range is larger than a threshold N the range is divided recursively and
    a follow on job is then created which merges back the results else
    the file is sorted and placed in the output.
    """
    if random.random() > 0.5:
        raise RuntimeError() #This error is a test error, it does not mean the tests have failed.
    length = fileEnd - fileStart
    target.logToMaster("Am running a down target with length: %i from input file: %s" % (length, inputFile))
    assert length >= 0
    if length > N:
        midPoint = getMidPoint(inputFile, fileStart, fileEnd)
        assert midPoint >= fileStart
        assert midPoint+1 < fileEnd
        #We will subdivide the file
        tempFile1 = getTempFile(rootDir=target.getGlobalTempDir())
        tempFile2 = getTempFile(rootDir=target.getGlobalTempDir())
        target.addChildTargetFn(down, (inputFile, fileStart, midPoint+1, N, tempFile1))
        target.addChildTargetFn(down, (inputFile, midPoint+1, fileEnd, N, tempFile2)) #Add one to avoid the newline
        target.setFollowOnTargetFn(up, (tempFile1, tempFile2, outputFile))                
    else:
        #We can sort this bit of the file
        copySubRangeOfFile(inputFile, fileStart, fileEnd, outputFile)
        sort(outputFile)

def up(target, inputFile1, inputFile2, outputFile):
    """Merges the two files and places them in the output.
    """
    if random.random() > 0.5:
        raise RuntimeError() #This error is a test error, it does not mean the tests have failed.
    merge(inputFile1, inputFile2, outputFile)
    target.logToMaster("Am running an up target with input files: %s and %s" % (inputFile1, inputFile2))

def cleanup(tempOutputFile, outputFile):
    """Copies back the temporary file to input once we've successfully sorted the temporary file.
    """
    if random.random() > 0.5:
        raise RuntimeError() #This is a test error and not a failure of the tests
    shutil.copyfile(tempOutputFile, outputFile)

def main():
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    
    parser.add_option("--fileToSort", dest="fileToSort",
                      help="The file you wish to sort")
    
    parser.add_option("--N", dest="N",
                      help="The threshold below which a serial sort function is used to sort file. All lines must of length less than or equal to N or program will fail", 
                      default=10000)
    
    options, args = parser.parse_args()
    
    if options.fileToSort == None:
        raise RuntimeError("No file to sort given")

    if not os.path.exists(options.fileToSort):
        raise RuntimeError("File to sort does not exist: %s" % options.fileToSort)
    
    if int(options.N) <= 0:
        raise RuntimeError("Invalid value of N: %s" % options.N)
    
    if len(args) != 0:
        raise RuntimeError("Unrecognised input arguments: %s" % " ".join(args))
    
    #Now we are ready to run
    i = Stack(Target.makeTargetFn(setup, (options.fileToSort, int(options.N)))).startJobTree(options)
    
    #if i:
    #    raise RuntimeError("The jobtree contained %i failed jobs" % i)

if __name__ == '__main__':
    from jobTree.test.sort.scriptTreeTest_Sort import *
    main()
