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
    
import xml.etree.ElementTree as ET

from sonLib.bioio import logger
from sonLib.bioio import setLogLevel
from sonLib.bioio import setLoggingFromOptions
from sonLib.bioio import getTempFile
from sonLib.bioio import getTempDirectory 
from sonLib.bioio import system
from sonLib.bioio import getTotalCpuTime

from jobTree.src.jobTreeRun import addOptions
from jobTree.src.jobTreeRun import createJobTree
from jobTree.src.jobTreeRun import reloadJobTree
from jobTree.src.jobTreeRun import createFirstJob
from jobTree.src.jobTreeRun import loadEnvironment
from jobTree.src.master import mainLoop

from jobTree.scriptTree.target import Target
        
class JobFile:
    def __init__(self, filename):
        self.jobfilename = filename
        self.xmlRoot = ET.parse(self.jobfilename).getroot()
        self.attrib = self.xmlRoot.attrib
    def getGlobalTempDir(self):
        return self.xmlRoot.attrib["global_temp_dir"]
    def addChild(self, cmd, time):
        c = self.xmlRoot.find("children")
        ET.SubElement(c, "child", {"command":str(cmd), "time":str(time)})
        return self
    def write(self):
        fh = open(self.jobfilename, mode="w")
        ET.ElementTree(self.xmlRoot).write(fh)
        fh.close();
        return self

class Stack:
    """Holds together a stack of targets and runs them.
    The only public methods are documented at the top of this file..
    """
    def __init__(self, target):
        self.stack = [ target ]
        self.verifyTargetAttributesExist(target)
        self.runTime = target.getRunTime()
        
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
    
    def addToJobFile(self, jobFile):
        job = JobFile(jobFile)
        cmd = self.makeRunnable(job.getGlobalTempDir())
        job.addChild(cmd, 1000)
        job.write()

    def startJobTree(self, options):
        """Runs jobtree using the given options (see Stack.getDefaultOptions
        and Stack.addJobTreeOptions).
        """
        self.verifyJobTreeOptions(options)
        setLoggingFromOptions(options)
        options.jobTree = os.path.abspath(options.jobTree)
        if os.path.isdir(options.jobTree):
            config, batchSystem = reloadJobTree(options.jobTree)
        else:
            config, batchSystem = createJobTree(options)
            #Setup first job.
            command = self.makeRunnable(options.jobTree)
            memory = self.getMemory()
            cpu = self.getCpu()
            time = self.getRunTime()
            if memory != sys.maxint:
                if cpu != sys.maxint:
                    createFirstJob(command, config, memory=memory, cpu=cpu, time=time)
                else:
                    createFirstJob(command, config, memory=memory, time=time)
            else:
                if cpu != sys.maxint:
                    createFirstJob(command, config, cpu=cpu, time=time)
                else:
                    createFirstJob(command, config, time=time)
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
        for target in self.stack:
            for importString in target.importStrings:
                i.add(importString)
        classNames = " ".join(i)
        return "scriptTree --job JOB_FILE --target %s %s" % (pickleFile, classNames)
    
    def addTarget(self, target):
        self.stack.append(target)
        self.runTime += target.getRunTime()
        
    def popTarget(self):
        target = self.stack.pop()
        self.runTime -= target.getRunTime()
        return target
    
    def hasRemaining(self):
        return len(self.stack) > 0
    
    def getRunTime(self):
        return self.runTime
    
    def getMemory(self):
        l = [ target.getMemory() for target in self.stack if target.getMemory() != sys.maxint ]
        if len(l) > 0:
            return max(l)
        return sys.maxint
    
    def getCpu(self):
        l = [ target.getCpu() for target in self.stack if target.getCpu() != sys.maxint ]
        if len(l) > 0:
            return max(l)
        return sys.maxint
    
    def getLocalTempDir(self):
        self.tempDirAccessed = True
        return self.localTempDir
    
    def getGlobalTempDir(self):
        return getTempDirectory(rootDir=self.globalTempDir)

    def execute(self, jobFile):
        job = ET.parse(jobFile).getroot()
        setLogLevel(job.attrib["log_level"])
        logger.info("Setup logging with level: %s" % job.attrib["log_level"])
        self.tempDirAccessed = False
        self.localTempDir = job.attrib["local_temp_dir"]
        self.globalTempDir = job.attrib["global_temp_dir"]
        maxTime = float(job.attrib["job_time"])
        memory = int(job.attrib["available_memory"])
        cpu = int(job.attrib["available_cpu"])
        
        if job.attrib.has_key("stats"):
            stats = ET.Element("stack")
            startTime = time.time()
            startClock = time.clock()
        else:
            stats = None
        
        newChildren = [] #List to add all the children to before we package them
        #off into stacks
        newChildCommands = [] #Ditto for the child commands
        newFollowOns = [] #Ditto for the follow-ons 
        while self.hasRemaining():
            if stats is not None: #Getting the runtime of the stats module
                targetStartTime = time.time()
                targetStartClock = getTotalCpuTime()
                
            target = self.popTarget()
            target.setStack(self)
            #Debug check that we have the right amount of CPU and memory for the job in hand
            targetMemory = target.getMemory()
            if targetMemory != sys.maxint:
                assert targetMemory <= memory
            targetCpu = target.getCpu()
            if targetCpu != sys.maxint:
                assert targetCpu <= cpu
            #Run the target, first cleanup then run.
            target.run()
            #Cleanup after the target
            if self.tempDirAccessed:
                system("rm -rf %s/*" % self.localTempDir)
                self.tempDirAccessed = False
            #Handle the follow on
            followOn = target.getFollowOn()
            #if target.__class__ != CleanupGlobalTempDirTarget and followOn == None:
            #    followOn = CleanupGlobalTempDirTarget()
            if followOn is not None: #Target to get rid of follow on when done.
                if target.isGlobalTempDirSet():
                    followOn.setGlobalTempDir(target.getGlobalTempDir())
                newFollowOns.append(followOn)
            
            #Now add the children to the newChildren stack
            newChildren += target.getChildren()
            
            #Now add the child commands to the newChildCommands stack
            newChildCommands += target.getChildCommands()
            
            if stats is not None:
                ET.SubElement(stats, "target", { "time":str(time.time() - targetStartTime), 
                                                "clock":str(getTotalCpuTime() - targetStartClock),
                                                "class":".".join((target.__class__.__name__,)),
                                                "e_time":str(target.getRunTime())})
                
            for message in target.getMasterLoggingMessages():
                if job.find("messages") is None:
                    ET.SubElement(job, "messages")
                ET.SubElement(job.find("messages"), "message", { "message": message} )
        
        #######
        #Now build the new stacks and corresponding jobs
        #######
        
        #First add all the follow ons to the existing stack and make it a follow on job for job-tree
        assert not self.hasRemaining()
        
        #First sort out the follow on job
        if len(newFollowOns) > 0: #If we have follow ons
            followOnRuntime = sum([ followOn.getRunTime() for followOn in newFollowOns ])
            
            if followOnRuntime > maxTime: #We create a parallel list of follow ons
                followOnStack = Stack(ParallelFollowOnTarget(newFollowOns))
            else:
                followOnStack = Stack(newFollowOns.pop())
                while len(newFollowOns) > 0:
                    followOnStack.addTarget(newFollowOns.pop())
        
            job.attrib["command"] = followOnStack.makeRunnable(self.globalTempDir)
            job.attrib["time"] = str(followOnStack.getRunTime())
            followOnMemory = followOnStack.getMemory()
            assert not job.attrib.has_key("memory")
            if followOnMemory != sys.maxint:
                job.attrib["memory"] = str(followOnMemory)
            assert not job.attrib.has_key("cpu")
            followOnCpu = followOnStack.getCpu()
            if followOnCpu != sys.maxint:
                job.attrib["cpu"] = str(followOnCpu)
              
        #Now build stacks of children..
        childrenTag = job.find("children")
        while len(newChildren) > 0:
            childStack = Stack(newChildren.pop())
            while len(newChildren) > 0 and childStack.getRunTime() <= maxTime:
                childStack.addTarget(newChildren.pop())
            childJob = ET.SubElement(childrenTag, "child", { "command":childStack.makeRunnable(self.globalTempDir),
                                              "time":str(childStack.getRunTime()) })
            childMemory = childStack.getMemory()
            assert not childJob.attrib.has_key("memory")
            if childMemory != sys.maxint:
                childJob.attrib["memory"] = str(childMemory)
            assert not childJob.attrib.has_key("cpu")
            childCpu = childStack.getCpu()
            if childCpu != sys.maxint:
                childJob.attrib["cpu"] = str(childCpu)
        
        #Now build jobs for each child command
        for childCommand, runTime in newChildCommands:
            ET.SubElement(childrenTag, "child", { "command":str(childCommand),
                                              "time":str(runTime) })
    
        #Now write the updated job file
        fileHandle = open(jobFile, 'w')
        ET.ElementTree(job).write(fileHandle)
        fileHandle.close()
        
        #Finish up the stats
        if stats is not None:
            stats.attrib["time"] = str(time.time() - startTime)
            stats.attrib["clock"] = str(getTotalCpuTime() - startClock)
            fileHandle = open(job.attrib["stats"], 'w')
            ET.ElementTree(stats).write(fileHandle)
            fileHandle.close()
    
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

def loadPickleFile(pickleFile):
    """Loads the first object from a pickle file.
    """
    fileHandle = open(pickleFile, 'r')
    i = cPickle.load(fileHandle)
    fileHandle.close()
    return i
    
class CleanupGlobalTempDirTarget(Target):
    """Target to clean everything up once the 
    temporary directory is all done.
    """
    def __init__(self):
        Target.__init__(self, 0)
    
    def run(self):
        system("rm -rf %s" % self.getGlobalTempDir())

class ParallelFollowOnTarget(Target):
    """Target sets up a bunch of follow ons to run in parallel.
    """
    def __init__(self, followOnTargets):
        Target.__init__(self, 0)
        self.followOnTargets = followOnTargets
        for target in self.followOnTargets: #So that we can un-pickle the children
            for importString in target.importStrings:
                self.importStrings.add(importString)
    
    def run(self):
        for followOnTarget in self.followOnTargets:
            self.addChildTarget(followOnTarget)
            
