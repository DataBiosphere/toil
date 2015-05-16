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

def merge(fileHandle1, fileHandle2, outputFileHandle):
    """Merges together two files maintaining sorted order.
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
    """Copies the range (in bytes) between fileStart and fileEnd to the given
    output file handle.
    """
    with open(inputFile, 'r') as fileHandle:
        fileHandle.seek(fileStart)
        data = fileHandle.read(fileEnd - fileStart)
        assert len(data) == fileEnd - fileStart
        outputFileHandle.write(data)
    
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
