"""Wrapper functions for running the various programs in the jobTree package.
"""

#Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.

from sonLib.bioio import logger
from sonLib.bioio import system

def runJobTreeStats(jobTree, outputFile):
    system("jobTreeStats --jobTree %s --outputFile %s" % (jobTree, outputFile))
    logger.info("Ran the job-tree stats command apparently okay")
    
def gridEngineIsInstalled():
    """Returns True if grid-engine is installed, else False.
    """
    try:
        return system("qstat -help") == 0
    except RuntimeError:
        return False
    
def parasolIsInstalled():
    """Returns True if parasol is installed, else False.
    """
    try:
        return system("parasol status") == 0
    except RuntimeError:
        return False
    
def runJobTreeStatusAndFailIfNotComplete(jobTreeDir):
    command = "jobTreeStatus --jobTree %s --failIfNotComplete --verbose" % jobTreeDir
    system(command)
