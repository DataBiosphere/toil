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

from __future__ import absolute_import

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
