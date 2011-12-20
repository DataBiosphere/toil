#!/usr/bin/env python
""" Test program designed to catch two bugs encountered when developing
progressive cactus:
1) spawning a daemon process causes indefinite jobtree hangs
2) jobtree does not parallelize jobs in certain types of
recursions

These cases can be tested for by checking the timestamps output
by this program.  If jobs get hung up until the daemon process
finsishes, that would be a case of bug 1).  If jobs do not
get issued in parallel (ie the begin UP does not happen
concurrently for the leaves of the comb tree), then that is
a case of bug 2)

--Glenn Hickey
"""

import os
from time import sleep
import random
import datetime
import sys

from sonLib.bioio import system
from optparse import OptionParser
import xml.etree.ElementTree as ET

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
def combTree(n = 10):
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
    def __init__(self, tree, event, sleepTime, startTime):
        Target.__init__(self)
        self.tree = tree
        self.event = event
        self.sleepTime = sleepTime
        self.startTime = startTime

    def run(self):
        sleep(1)
        self.addChildTarget(DownJob(self.tree, self.event,
                                    self.sleepTime, self.startTime))

        self.setFollowOnTarget(LastJob())

class LastJob(Target):
    def __init__(self):
        Target.__init__(self)

    def run(self):
        sleep(1)
        pass
    
class DownJob(Target):
    def __init__(self, tree, event, sleepTime, startTime):
        Target.__init__(self)
        self.tree = tree
        self.event = event
        self.sleepTime = sleepTime
        self.startTime = startTime

    def run(self):
        writeLog(self, "begin Down: %s" % self.event, self.startTime)
        children = self.tree[self.event]
        for child in children:
            writeLog(self, "add %s as child of %s" % (child, self.event),
                     self.startTime)
            self.addChildTarget(DownJob(self.tree, child,
                                        self.sleepTime, self.startTime))

        if len(children) == 0:
            self.setFollowOnTarget(UpJob(self.tree, self.event,
                                         self.sleepTime, self.startTime))
        return 0
    
class UpJob(Target):
    def __init__(self, tree, event, sleepTime, startTime):
        Target.__init__(self)
        self.tree = tree
        self.event = event
        self.sleepTime = sleepTime
        self.startTime = startTime

    def run(self):
        writeLog(self, "begin UP: %s" % self.event, self.startTime)

        sleep(self.sleepTime)
        spawnDaemon("sleep 33.666")       
        writeLog(self, "end UP: %s" % self.event, self.startTime)

def main():
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    parser.add_option("--sleepTime", dest="sleepTime", type="int",
                     help="sleep [default=5] seconds", default="5")
    parser.add_option("--tree", dest="tree",
                      help="tree [balanced|comb|star|fly]", default="comb")
    options, args = parser.parse_args()
    setLoggingFromOptions(options)

    startTime = datetime.datetime.now()

    tree = combTree()
    if options.tree == "star":
        tree = starTree()
    elif options.tree == "balanced":
        tree = balancedTree()
    elif options.tree == "fly":
        tree = flyTree()
    
    baseTarget = FirstJob(tree, "Anc00", options.sleepTime, startTime)
    Stack(baseTarget).startJobTree(options)
    
if __name__ == '__main__':
    from jobTree.test.jobTree.jobTreeTest_Dependencies import *
    main()
    
    
