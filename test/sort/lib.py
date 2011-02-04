#!/usr/bin/env python

"""Functions for scriptTreeTest_Sort.py
"""

def sort(file):
    """Sorts the given file.
    """
    fileHandle = open(file, 'r')
    lines = fileHandle.readlines()
    fileHandle.close()
    lines.sort()
    fileHandle = open(file, 'w')
    for line in lines:
        fileHandle.write(line)
    fileHandle.close()

def merge(inputFile1, inputFile2, outputFile):
    """Merges together two files maintaining sorted order.
    """
    fileHandle1 = open(inputFile1, 'r')
    fileHandle2 = open(inputFile2, 'r')
    fileHandle3 = open(outputFile, 'w')
    line2 = fileHandle2.readline()
    for line1 in fileHandle1.readlines():
        while line2 != '' and line2 <= line1:
            fileHandle3.write(line2)
            line2 = fileHandle2.readline()
        fileHandle3.write(line1)
    while line2 != '':
        fileHandle3.write(line2)
        line2 = fileHandle2.readline()
    fileHandle1.close()
    fileHandle2.close()
    fileHandle3.close()

def copySubRangeOfFile(inputFile, fileStart, fileEnd, outputFile):
    """Copies the range (in bytes) between fileStart and fileEnd.
    """
    fileHandle = open(inputFile, 'r')
    fileHandle.seek(fileStart) 
    data = fileHandle.read(fileEnd - fileStart)
    assert len(data) == fileEnd - fileStart
    fileHandle.close()
    fileHandle = open(outputFile, 'w')
    fileHandle.write(data)
    fileHandle.close()
    
def getMidPoint(file, fileStart, fileEnd):
    """Finds the point in the file to split. 
    Returns an int i such that fileStart <= i < fileEnd
    """
    fileHandle = open(file, 'r')
    midPoint = (fileStart + fileEnd) / 2
    assert midPoint >= fileStart
    fileHandle.seek(midPoint)
    line = fileHandle.readline()
    assert len(line) >= 1
    if len(line) + midPoint < fileEnd:
        return midPoint + len(line) -1
    fileHandle.seek(fileStart)
    line = fileHandle.readline()
    assert len(line) >= 1
    assert len(line) + fileStart <= fileEnd
    return len(line) + fileStart -1
