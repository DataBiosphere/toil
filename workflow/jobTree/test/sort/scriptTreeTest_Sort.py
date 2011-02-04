#!/usr/bin/env python

"""A demonstration of jobTree. Sorts the lines of a file into ascending order by doing a parallel merge sort.
"""
from optparse import OptionParser
import os
import shutil
from workflow.jobTree.lib.bioio import getTempFile
from workflow.jobTree.scriptTree.target import Target
from workflow.jobTree.scriptTree.stack import Stack
from workflow.jobTree.test.sort.lib import merge, sort, copySubRangeOfFile, getMidPoint

class Setup(Target):
    """Sets up the sort.
    """
    def __init__(self, inputFile, N):
        Target.__init__(self, time=0.00025, memory=1000000, cpu=1)
        self.inputFile = inputFile
        self.N = N
    
    def run(self):
        tempOutputFile = getTempFile(rootDir=self.getGlobalTempDir())
        self.addChildTarget(Down(self.inputFile, 0, os.path.getsize(self.inputFile), self.N, tempOutputFile))
        self.setFollowOnTarget(Cleanup(tempOutputFile, self.inputFile))

class Cleanup(Target):
    """Copies back the temporary file to input once we've successfully sorted the temporary file.
    """
    def __init__(self, tempOutputFile, outputFile):
        Target.__init__(self, time=0.0031)
        self.tempOutputFile = tempOutputFile
        self.outputFile = outputFile
    
    def run(self):
        shutil.copyfile(self.tempOutputFile, self.outputFile)

class Down(Target):
    """Input is a file and a range into that file to sort and an output location in which
    to write the sorted file.
    If the range is larger than a threshold N the range is divided recursively and
    a follow on job is then created which merges back the results else
    the file is sorted and placed in the output.
    """
    def __init__(self, inputFile, fileStart, fileEnd, N, outputFile):
        assert fileStart >= 0
        assert fileStart <= fileEnd
        Target.__init__(self, time=0.00045)
        self.inputFile = inputFile
        self.fileStart = fileStart
        self.fileEnd = fileEnd
        self.N = N
        self.outputFile = outputFile
    
    def run(self):
        length = self.fileEnd - self.fileStart
        self.logToMaster("Am running a down target with length: %i from input file: %s" % (length, self.inputFile))
        assert length >= 0
        if length > self.N:
            midPoint = getMidPoint(self.inputFile, self.fileStart, self.fileEnd)
            assert midPoint >= self.fileStart
            assert midPoint+1 < self.fileEnd
            #We will subdivide the file
            tempFile1 = getTempFile(rootDir=self.getGlobalTempDir())
            tempFile2 = getTempFile(rootDir=self.getGlobalTempDir())
            self.addChildTarget(Down(self.inputFile, self.fileStart, midPoint+1, self.N, tempFile1))
            self.addChildTarget(Down(self.inputFile, midPoint+1, self.fileEnd, self.N, tempFile2)) #Add one to avoid the newline
            self.setFollowOnTarget(Up(tempFile1, tempFile2, self.outputFile))                
        else:
            #We can sort this bit of the file
            copySubRangeOfFile(self.inputFile, self.fileStart, self.fileEnd, self.outputFile)
            sort(self.outputFile)
            
class Up(Target):
    """Merges the two files and places them in the output.
    """
    def __init__(self, inputFile1, inputFile2, outputFile):
        Target.__init__(self, time=0.0007)
        self.inputFile1 = inputFile1
        self.inputFile2 = inputFile2
        self.outputFile = outputFile
        
    def run(self):
        merge(self.inputFile1, self.inputFile2, self.outputFile)
        self.logToMaster("Am running an up target with input files: %s and %s" % (self.inputFile1, self.inputFile2))
        
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
    i = Stack(Setup(options.fileToSort, int(options.N))).startJobTree(options)
    
    if i:
        raise RuntimeError("The jobtree contained %i failed jobs" % i)

if __name__ == '__main__':
    from workflow.jobTree.test.sort.scriptTreeTest_Sort import *
    main()
