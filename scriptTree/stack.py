#!/usr/bin/env python

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

import sys
import os
import time
from optparse import OptionParser
try:
    import cPickle 
except ImportError:
    import pickle as cPickle
    
import xml.etree.cElementTree as ET

from sonLib.bioio import logger
from sonLib.bioio import setLogLevel
from sonLib.bioio import setLoggingFromOptions
from sonLib.bioio import getTempFile
from sonLib.bioio import getTempDirectory 
from sonLib.bioio import system, absSymPath
from sonLib.bioio import getTotalCpuTimeAndMemoryUsage, getTotalCpuTime

from jobTree.src.jobTreeRun import addOptions
from jobTree.src.jobTreeRun import createJobTree
from jobTree.src.jobTreeRun import reloadJobTree
from jobTree.src.jobTreeRun import createFirstJob
from jobTree.src.jobTreeRun import loadEnvironment
from jobTree.src.master import mainLoop

from jobTree.scriptTree.target import Target

class Stack(object):
    """Holds together a stack of targets and runs them.
    The only public methods are documented at the top of this file..
    """
    def __init__(self, target):
        self.target = target
        self.verifyTargetAttributesExist(target)
        
    @staticmethod
    def getDefaultOptions():
        """Returns am optparse.Values object name (string) : value
        options used by job-tree. See the help string 
        of jobTree to see these options.
        """
        parser = OptionParser()
        Stack.addJobTreeOptions(parser)
        options, args = parser.parse_args(args=[])
        assert len(args) == 0
        return options
        
    @staticmethod
    def addJobTreeOptions(parser):
        """Adds the default job-tree options to an optparse
        parser object.
        """
        addOptions(parser)

    def startJobTree(self, options):
        """Runs jobtree using the given options (see Stack.getDefaultOptions
        and Stack.addJobTreeOptions).
        """
        self.verifyJobTreeOptions(options)
        setLoggingFromOptions(options)
        options.jobTree = absSymPath(options.jobTree)
        if os.path.isdir(options.jobTree):
            config, batchSystem = reloadJobTree(options.jobTree)
        else:
            config, batchSystem = createJobTree(options)
            #Setup first job.
            command = self.makeRunnable(options.jobTree)
            memory = self.getMemory()
            cpu = self.getCpu()
            createFirstJob(command, config, memory=memory, cpu=cpu)
        loadEnvironment(config)
        return mainLoop(config, batchSystem)

#####
#The remainder of the class is private to the user
####
        
    def makeRunnable(self, tempDir):
        pickleFile = getTempFile(".pickle", tempDir)
        fileHandle = open(pickleFile, 'w')
        cPickle.dump(self, fileHandle, cPickle.HIGHEST_PROTOCOL)
        fileHandle.close() 
        i = set()
        for importString in self.target.importStrings:
            i.add(importString)
        classNames = " ".join(i)
        return "scriptTree %s %s" % (pickleFile, classNames)
    
    def getMemory(self, defaultMemory=sys.maxint):
        memory = self.target.getMemory()
        if memory == sys.maxint:
            return defaultMemory
        return memory
    
    def getCpu(self, defaultCpu=sys.maxint):
        cpu = self.target.getCpu()
        if cpu == sys.maxint:
            return defaultCpu
        return cpu
    
    def getLocalTempDir(self):
        self.tempDirAccessed = True
        return self.localTempDir
    
    def getGlobalTempDir(self):
        return getTempDirectory(rootDir=self.globalTempDir)

    def execute(self, job, stats, localTempDir, globalTempDir, 
                memoryAvailable, cpuAvailable,
                defaultMemory, defaultCpu, depth):
        self.tempDirAccessed = False
        self.localTempDir = localTempDir
        self.globalTempDir = globalTempDir
        
        if stats != None:
            startTime = time.time()
            startClock = getTotalCpuTime()
        
        baseDir = os.getcwd()
        
        self.target.setStack(self)
        #Debug check that we have the right amount of CPU and memory for the job in hand
        targetMemory = self.target.getMemory()
        if targetMemory != sys.maxint:
            assert targetMemory <= memoryAvailable
        targetCpu = self.target.getCpu()
        if targetCpu != sys.maxint:
            assert targetCpu <= cpuAvailable
        #Run the target, first cleanup then run.
        self.target.run()
        #Change dir back to cwd dir, if changed by target (this is a safety issue)
        if os.getcwd() != baseDir:
            os.chdir(baseDir)
        #Cleanup after the target
        if self.tempDirAccessed:
            system("rm -rf %s/*" % self.localTempDir)
            self.tempDirAccessed = False
        #Handle the follow on
        followOn = self.target.getFollowOn()
        if followOn is not None: #Target to get rid of follow on when done.
            if self.target.isGlobalTempDirSet():
                followOn.setGlobalTempDir(self.target.getGlobalTempDir())
            followOnStack = Stack(followOn)
            job.followOnCommands.append((followOnStack.makeRunnable(self.globalTempDir),
                                         followOnStack.getMemory(defaultMemory),
                                         followOnStack.getCpu(defaultCpu),
                                         depth))
        
        #Now add the children to the newChildren stack
        newChildren = self.target.getChildren()
        newChildren.reverse()
        assert len(job.children) == 0
        while len(newChildren) > 0:
            childStack = Stack(newChildren.pop())
            job.children.append((childStack.makeRunnable(self.globalTempDir),
                     childStack.getMemory(defaultMemory),
                     childStack.getCpu(defaultCpu)))
        
         #Now build jobs for each child command
        for childCommand, runTime in self.target.getChildCommands():
            job.children.append((childCommand, defaultMemory, defaultCpu))
            
        for message in self.target.getMasterLoggingMessages():
            job.messages.append(message)
        
        #Finish up the stats
        if stats != None:
            stats = ET.SubElement(stats, "target")
            stats.attrib["time"] = str(time.time() - startTime)
            totalCpuTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            stats.attrib["clock"] = str(totalCpuTime - startClock)
            stats.attrib["class"] = ".".join((self.target.__class__.__name__,))
            stats.attrib["memory"] = str(totalMemoryUsage)
    
    def verifyJobTreeOptions(self, options):
        """ verifyJobTreeOptions() returns None if all necessary values
        are present in options, otherwise it raises an error.
        It can also serve to validate the values of the options.
        """
        required = ['logLevel', 'command', 'batchSystem', 'jobTree']
        for r in required:
            if r not in vars(options):
                raise RuntimeError("Error, there is a missing option (%s) from the scriptTree Stack, "
                                   "did you remember to call Stack.addJobTreeOptions()?" % r)
        if options.jobTree is None:
            raise RuntimeError("Specify --jobTree")

    def verifyTargetAttributesExist(self, target):
        """ verifyTargetAttributesExist() checks to make sure that the Target
        instance has been properly instantiated. Returns None if instance is OK,
        raises an error otherwise.
        """
        required = ['_Target__followOn', '_Target__children', '_Target__childCommands', 
                    '_Target__time', '_Target__memory', '_Target__cpu', 'globalTempDir']
        for r in required:
            if r not in vars(target):
                raise RuntimeError("Error, there is a missing attribute, %s, from a Target sub instance %s, "
                                   "did you remember to call Target.__init__(self) in the %s "
                                   "__init__ method?" % ( r, target.__class__.__name__,
                                                          target.__class__.__name__))
            
