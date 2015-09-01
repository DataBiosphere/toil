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
import unittest
import os
import tempfile
import shutil
import time
import math
import datetime

from toil.job import Job
from toil.test import ToilTest


class DependenciesTest(ToilTest):
    """
    Test program designed to catch two bugs encountered when developing progressive cactus:

    1) spawning a daemon process causes indefinite toil hangs

    2) toil does not properly parallelize jobs in certain types of recursions

    If jobs get hung up until the daemon process finsishes, that would be a case of bug 1).  If
    jobs do not get issued in parallel (ie the begin UP does not happen concurrently for the
    leaves of the comb tree), then that is a case of bug 2).  This is now verified if a log file
    is specified (--logFile [path] option)

    --Glenn Hickey
    """


    def setUp(self):
        super(DependenciesTest, self).setUp()
        self.jobStore = self._createTempDir('jobStore')
        self.tempDir = self._createTempDir('tempDir')

    # FIXME: test methods can't have parameters, AFAIK (Hannes)

    @unittest.expectedFailure
    def testDependencies(self, batchSystem="singleMachine"):
        """
        Only done in singleMachine for now.  Experts can run manually on other systems if they so choose.
        """

        def fn(tree, maxCores, maxThreads, size, coresPerJob, sleepTime):
            """
            Function runs the dependencies test
            """
            startTime = datetime.datetime.now()

            if os.path.exists(self.jobStore):
                shutil.rmtree(self.jobStore)

            logFd, logName = tempfile.mkstemp(prefix="log", dir=self.tempDir)

            # Switch out the tree with the topology
            if tree == "star":
                tree = starTree(size)
            elif tree == "balanced":
                tree = balancedTree()
            elif tree == "fly":
                tree = flyTree()
            else:
                tree = combTree(size)

            options = Job.Runner.getDefaultOptions(self.jobStore)
            options.maxThreads = maxThreads
            options.batchSystem = batchSystem
            options.logFile = logName
            options.maxCores = maxCores

            baseJob = FirstJob(tree, "Anc00", sleepTime, startTime, int(coresPerJob))
            Job.Runner.startToil(baseJob, options)
            checkLog(logName, maxCores, maxThreads, coresPerJob, sleepTime)

            os.close(logFd)

        fn("comb", 10, 100, 100, 1, 10)
        fn("comb", 200, 100, 100, 20, 10)

        fn("fly", 10, 8, 100, 1, 10)
        fn("fly", 10, 8, 100, 2, 10)

        fn("balanced", 5, 10, 100, 1, 10)
        fn("balanced", 5, 10, 100, 3, 10)


###############################################
# Classes/functions for testing dependencies
###############################################    

def writeLog(fileStore, msg, startTime):
    timeStamp = str(datetime.datetime.now() - startTime)
    fileStore.logToMaster("%s %s" % (timeStamp, msg))


def balancedTree():
    t = dict()
    t["Anc00"] = [1, 2]
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


def starTree(n=10):
    t = dict()
    t["Anc00"] = range(1, n)
    for i in range(1, n):
        t[i] = []
    return t


# odd numbers are leaves
def combTree(n=100):
    t = dict()
    for i in range(0, n):
        if i % 2 == 0:
            t[i] = [i + 1, i + 2]
        else:
            t[i] = []
        t[i + 1] = []
        t[i + 2] = []
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


class FirstJob(Job):
    def __init__(self, tree, event, sleepTime, startTime, cores):
        Job.__init__(self, cores=cores)
        self.tree = tree
        self.event = event
        self.sleepTime = sleepTime
        self.startTime = startTime
        self.cores = cores

    def run(self, fileStore):
        time.sleep(1)
        self.addChild(DownJob(self.tree, self.event,
                              self.sleepTime, self.startTime, self.cores))

        self.addFollowOn(LastJob())


class LastJob(Job):
    def __init__(self):
        Job.__init__(self)

    def run(self, fileStore):
        time.sleep(1)


class DownJob(Job):
    def __init__(self, tree, event, sleepTime, startTime, cores):
        Job.__init__(self, cores=cores)
        self.tree = tree
        self.event = event
        self.sleepTime = sleepTime
        self.startTime = startTime
        self.cores = cores

    def run(self, fileStore):
        writeLog(fileStore, "begin Down: %s" % self.event, self.startTime)
        children = self.tree[self.event]
        for child in children:
            writeLog(fileStore, "add %s as child of %s" % (child, self.event),
                     self.startTime)
            self.addChild(DownJob(self.tree, child,
                                  self.sleepTime, self.startTime, self.cores))

        if len(children) == 0:
            self.addFollowOn(UpJob(self.tree, self.event,
                                   self.sleepTime, self.startTime, self.cores))
        return 0


class UpJob(Job):
    def __init__(self, tree, event, sleepTime, startTime, cores):
        Job.__init__(self, cores=cores)
        self.tree = tree
        self.event = event
        self.sleepTime = sleepTime
        self.startTime = startTime
        self.cores = cores

    """
    def spawnDaemon(self, command):
        ""
        Launches a command as a daemon.  It will need to be explicitly killed
        ""
        sonLib_daemonize_py = os.path.join( os.path.dirname( os.path.abspath( __file__ ) ), 'sonLib_daemonize.py' )
        #return system( "%s \'%s\'" % ( sonLib_daemonize_py, command) )
    """

    def run(self, fileStore):
        writeLog(fileStore, "begin UP: %s" % self.event, self.startTime)

        time.sleep(self.sleepTime)
        # self.spawnDaemon("sleep %s" % str(int(self.sleepTime) * 10))
        writeLog(fileStore, "end UP: %s" % self.event, self.startTime)


# let k = maxThreads.  we make sure that jobs are fired in batches of k
# so the first k jobs all happen within epsilon time of each other,
# same for the next k jobs and so on.  we allow at most alpha time
# between the different batches (ie between k+1 and k).  
def checkLog(logFile, maxCores, maxThreads, coresPerJob, sleepTime):
    epsilon = float(sleepTime) / 2.0
    alpha = sleepTime * 2.0
    logFile = open(logFile, "r")
    stamps = []
    for logLine in logFile:
        if "begin UP" in logLine:
            chunks = logLine.split()
            assert len(chunks) == 13
            timeString = chunks[9]
            timeObj = datetime.datetime.strptime(timeString, "%H:%M:%S.%f")
            timeStamp = timeObj.hour * 3600. + timeObj.minute * 60. + \
                        timeObj.second + timeObj.microsecond / 1000000.
            stamps.append(timeStamp)

    stamps.sort()

    maxThreads = int(maxThreads)
    maxCores = int(maxCores)
    maxConcurrentJobs = min(maxThreads, maxCores)
    coresPerThread = float(maxCores) / maxConcurrentJobs
    coresPerJob = int(coresPerJob)
    assert coresPerJob >= 1
    assert coresPerThread >= 1
    threadsPerJob = 1
    if coresPerJob > coresPerThread:
        threadsPerJob = math.ceil(coresPerJob / coresPerThread)
    maxConcurrentJobs = int(maxConcurrentJobs / threadsPerJob)
    # print "Info on jobs", coresPerThread, coresPerJob, threadsPerJob, maxConcurrentJobs
    assert maxConcurrentJobs >= 1
    for i in range(1, len(stamps)):
        delta = stamps[i] - stamps[i - 1]
        if i % maxConcurrentJobs != 0:
            if delta > epsilon:
                raise RuntimeError("jobs out of sync: i=%d delta=%f threshold=%f" %
                                   (i, delta, epsilon))
        elif delta > alpha:
            raise RuntimeError("jobs out of sync: i=%d delta=%f threshold=%f" %
                               (i, delta, alpha))

    logFile.close()


if __name__ == '__main__':
    unittest.main()
