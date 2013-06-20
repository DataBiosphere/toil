#!/usr/bin/env python
""" Test program designed to catch two bugs encountered when developing
progressive cactus:
1) spawning a daemon process causes indefinite jobtree hangs
2) jobtree does not properly parallelize jobs in certain types of
recursions

If jobs get hung up until the daemon process
finsishes, that would be a case of bug 1).  If jobs do not
get issued in parallel (ie the begin UP does not happen
concurrently for the leaves of the comb tree), then that is
a case of bug 2).  This is now verified if a log file is 
specified (--logFile [path] option)

--Glenn Hickey
"""

import os
from time import sleep
import random
import datetime
import sys
import math

from sonLib.bioio import system
from optparse import OptionParser
import xml.etree.cElementTree as ET

from jobTree.src.bioio import getLogLevelString
from jobTree.src.bioio import logger
from jobTree.src.bioio import setLoggingFromOptions
  
from jobTree.scriptTree.target import Target 
from jobTree.scriptTree.stack import Stack 


from sonLib.bioio import spawnDaemon

def writeLog(self, msg, startTime):
    timeStamp = str(datetime.datetime.now() - startTime)       
    self.logToMaster("%s %s" % (timeStamp, msg))

def balancedTree():
    t = dict()
    t["Anc00"] = [1,2]
    t[1] = [11, 12]
    t[2] = [21, 22]
    t[11] = [111, 112]
    t[12] = [121, 122]
    t[21] = [211, 212]
    t[22] = [221, 222]
    t[111] = [1111, 1112]
    t[112] = [1121, 1122]
    t[121] = [1211, 1212]
    t[122] = [1221, 1222]
    t[211] = [2111, 2112]
    t[212] = [2121, 2122]
    t[221] = [2211, 2212]
    t[222] = [2221, 2222]
    t[1111] = []
    t[1112] = []
    t[1121] = []
    t[1122] = []
    t[1211] = []
    t[1212] = []
    t[1221] = []
    t[1222] = []
    t[2111] = []
    t[2112] = []
    t[2121] = []
    t[2122] = []
    t[2211] = []
    t[2212] = []
    t[2221] = []
    t[2222] = []
    return t

def starTree(n = 10):
    t = dict()
    t["Anc00"] = range(1,n)
    for i in range(1,n):
        t[i] = []
    return t

# odd numbers are leaves
def combTree(n = 100):
    t = dict()
    for i in range(0,n):
        if i % 2 == 0:
            t[i] = [i+1, i+2]
        else:
            t[i] = []
        t[i+1] = []
        t[i+2] = []
    t["Anc00"] = t[0]
    return t

# dependencies of the internal nodes of the fly12 tree
# note that 2, 5, 8 and 10 have no dependencies
def flyTree():
    t = dict()
    t["Anc00"] = ["Anc01", "Anc03"]
    t["Anc02"] = []
    t["Anc01"] = ["Anc02"]
    t["Anc03"] = ["Anc04"]
    t["Anc04"] = ["Anc05", "Anc06"]
    t["Anc05"] = []
    t["Anc06"] = ["Anc07"]
    t["Anc07"] = ["Anc08", "Anc09"]
    t["Anc08"] = []
    t["Anc09"] = ["Anc10"]
    t["Anc10"] = []
    return t

class FirstJob(Target):
    def __init__(self, tree, event, sleepTime, startTime, cpu):
        Target.__init__(self, cpu=cpu)
        self.tree = tree
        self.event = event
        self.sleepTime = sleepTime
        self.startTime = startTime
        self.cpu = cpu

    def run(self):
        sleep(1)
        self.addChildTarget(DownJob(self.tree, self.event,
                                    self.sleepTime, self.startTime, self.cpu))

        self.setFollowOnTarget(LastJob())

class LastJob(Target):
    def __init__(self):
        Target.__init__(self)

    def run(self):
        sleep(1)
        pass
    
class DownJob(Target):
    def __init__(self, tree, event, sleepTime, startTime, cpu):
        Target.__init__(self, cpu=cpu)
        self.tree = tree
        self.event = event
        self.sleepTime = sleepTime
        self.startTime = startTime
        self.cpu = cpu

    def run(self):
        writeLog(self, "begin Down: %s" % self.event, self.startTime)
        children = self.tree[self.event]
        for child in children:
            writeLog(self, "add %s as child of %s" % (child, self.event),
                     self.startTime)
            self.addChildTarget(DownJob(self.tree, child,
                                        self.sleepTime, self.startTime, self.cpu))

        if len(children) == 0:
            self.setFollowOnTarget(UpJob(self.tree, self.event,
                                         self.sleepTime, self.startTime, self.cpu))
        return 0
    
class UpJob(Target):
    def __init__(self, tree, event, sleepTime, startTime, cpu):
        Target.__init__(self, cpu=cpu)
        self.tree = tree
        self.event = event
        self.sleepTime = sleepTime
        self.startTime = startTime
        self.cpu = cpu

    def run(self):
        writeLog(self, "begin UP: %s" % self.event, self.startTime)

        sleep(self.sleepTime)
        spawnDaemon("sleep %s" % str(int(self.sleepTime) * 10))       
        writeLog(self, "end UP: %s" % self.event, self.startTime)

# let k = maxThreads.  we make sure that jobs are fired in batches of k
# so the first k jobs all happen within epsilon time of each other, 
# same for the next k jobs and so on.  we allow at most alpha time
# between the different batches (ie between k+1 and k).  
def checkLog(options):
    epsilon = float(options.sleepTime) / 2.0
    alpha = options.sleepTime * 2.0
    logFile = open(options.logFile, "r")    
    stamps = []
    for logLine in logFile:
        if "begin UP" in logLine:
            chunks = logLine.split()
            assert len(chunks) == 12
            timeString = chunks[8]
            timeObj = datetime.datetime.strptime(timeString, "%H:%M:%S.%f")
            timeStamp = timeObj.hour * 3600. + timeObj.minute * 60. + \
            timeObj.second + timeObj.microsecond / 1000000.
            stamps.append(timeStamp)
    
    stamps.sort()
    
    maxThreads = int(options.maxThreads)
    maxCpus = int(options.maxCpus)
    maxConcurrentJobs = min(maxThreads, maxCpus)
    cpusPerThread = float(maxCpus) / maxConcurrentJobs
    cpusPerJob = int(options.cpusPerJob)
    assert cpusPerJob >= 1
    assert cpusPerThread >= 1
    threadsPerJob = 1
    if cpusPerJob > cpusPerThread:
        threadsPerJob = math.ceil(cpusPerJob / cpusPerThread)
    maxConcurrentJobs = int(maxConcurrentJobs / threadsPerJob)
    #print "Info on jobs", cpusPerThread, cpusPerJob, threadsPerJob, maxConcurrentJobs
    assert maxConcurrentJobs >= 1
    for i in range(1,len(stamps)):
        delta = stamps[i] - stamps[i-1]
        if i % maxConcurrentJobs != 0:
            if delta > epsilon:
                raise RuntimeError("jobs out of sync: i=%d delta=%f threshold=%f" % 
                             (i, delta, epsilon))
        elif delta > alpha:
            raise RuntimeError("jobs out of sync: i=%d delta=%f threshold=%f" % 
                             (i, delta, alpha))
            
    logFile.close()
    
def main():
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    parser.add_option("--sleepTime", dest="sleepTime", type="int",
                     help="sleep [default=5] seconds", default=5)
    parser.add_option("--tree", dest="tree",
                      help="tree [balanced|comb|star|fly]", default="comb")
    parser.add_option("--size", dest="size", type="int",
                      help="tree size (for comb or star) [default=10]", 
                      default=10) 
    parser.add_option("--cpusPerJob", dest="cpusPerJob",
                      help="Cpus per job", default="1")
        
    options, args = parser.parse_args()
    setLoggingFromOptions(options)

    startTime = datetime.datetime.now()

    if options.tree == "star":
        tree = starTree(options.size)
    elif options.tree == "balanced":
        tree = balancedTree()
    elif options.tree == "fly":
        tree = flyTree()
    else:
        tree = combTree(options.size)
    
    baseTarget = FirstJob(tree, "Anc00", options.sleepTime, startTime, int(options.cpusPerJob))
    Stack(baseTarget).startJobTree(options)
    
    if options.logFile is not None:
        checkLog(options)
    
if __name__ == '__main__':
    from jobTree.test.jobTreeTest_Dependencies import *
    main()
    
    
