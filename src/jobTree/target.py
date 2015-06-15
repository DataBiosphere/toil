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
from collections import namedtuple
import sys
import importlib
import time
from optparse import OptionParser
import xml.etree.cElementTree as ET

from jobTree.resource import ModuleDescriptor
from jobTree.lib.bioio import getTempFile

try:
    import cPickle 
except ImportError:
    import pickle as cPickle
    
import logging
logger = logging.getLogger( __name__ )

from jobTree.lib.bioio import getTempFile

from jobTree.lib.bioio import setLoggingFromOptions
from jobTree.lib.bioio import system
from jobTree.lib.bioio import getTotalCpuTimeAndMemoryUsage
from jobTree.lib.bioio import getTotalCpuTime

from jobTree.common import setupJobTree, addOptions
from jobTree.master import mainLoop

class Target(object):
    """
    Represents a unit of work jobTree.
    """
    def __init__(self, memory=sys.maxint, cpu=sys.maxint):
        """
        This method must be called by any overiding constructor.
        """
        self.__followOn = None
        self.__children = []
        self.__childCommands = []
        self.__memory = memory
        self.__cpu = cpu
        self.userModule = ModuleDescriptor.forModule(self.__module__)
        self.__loggingMessages = []
        self.__rvs = {}
        
        #A target A is a "predecessor" of another target B if B is a child or follow 
        #on of A.
        
        #A target A is a "logical follow-on" of a target B if A is a follow on of B, 
        #or A is on a directed path to a target C that also has a directed path of 
        #follow on edges to B.
        
        #Each target can have multiple parent targets and be the follow-on of another
        #target, providing that the graph remain a DAG, and no parent is also 
        #a logical follow-on of its child.
        
        #On the master, as each parent job is scheduled its list of children and any
        #follow-on is loaded, 
        #each is a pair represented as a job and its number of predecessors.
        #We maintain a hash of jobs to predecessors to be finished.
        #If a job has just one predecessor it is run immediately. 
        #If a job has multiple predecessors we look in the hash, if it is already
        #there then we deduct one from the number of predecessors to run. If the number
        #is zero, we remove it from the hash and run it, else we take no further 
        #action as we must wait for its remaining predecessors to be logged as 
        #finished.
        #If the child/follow-on job is not yet in the hash, we add it, with -1 
        #the number of predecessors.
        
        #These combined make the predecessors
        self.__followOnFrom = None #The target which this target follows on from
        self.__parents = set()

    def run(self):
        """
        Do user stuff here, including creating any follow on jobs.
        
        The return values can be passed to other targets
        by means of the rv() function. 
        If the return value is a tuple, rv(1) would refer to the second
        member of the tuple. If the return value is not a tuple then rV(0) would
        refer to the return value of the function. 
        
        We disallow return values to be PromisedTargetReturnValue instances (generated
        by the Target.rv() function - see below). 
        A check is made when deserialisaing the values PromisedTargetReturnValue instances 
        that will result in a runtime error if you attempt to do this.
        Allowing PromisedTargetReturnValue instances to be returned does not work, because
        the mechanism to pass the promise uses a fileStoreID that will be deleted once
        the current job and its follow ons have been completed. This is similar to
        scope rules in a language like C, where returning a reference to memory allocated
        on the stack within a function will produce an undefined reference. 
        Disallowing this also avoids nested promises (PromisedTargetReturnValue instances that contain
        other PromisedTargetReturnValue). 
        """
        pass
    
    def setFollowOn(self, followOnTarget):
        """
        Set the follow on target, returns the followOnTarget.
        """
        assert self.__followOn == None
        self.__followOn = followOnTarget 
        followOnTarget._setFollowOnFrom(self)
        return followOnTarget
        
    def addChild(self, childTarget):
        """
        Adds the child target to be run as child of this target. Returns childTarget.
        """
        self.__children.append(childTarget)
        childTarget._addParent(self)
        return childTarget
        
    ##Convenience functions for creating targets

    def setFollowOnFn(self, fn, *args, **kwargs):
        """
        Sets a follow on fn. See FunctionWrappingTarget. Returns new follow-on Target.
        """
        return self.setFollowOn(FunctionWrappingTarget(fn, *args, **kwargs))

    def setFollowOnTargetFn(self, fn, *args, **kwargs):
        """
        Sets a follow on target fn. See TargetFunctionWrappingTarget. 
        Returns new follow-on Target.
        """
        return self.setFollowOn(TargetFunctionWrappingTarget(fn, *args, **kwargs)) 
    
    def addChildFn(self, fn, *args, **kwargs):
        """
        Adds a child fn. See FunctionWrappingTarget. Returns new child Target.
        """
        return self.addChild(FunctionWrappingTarget(fn, *args, **kwargs))

    def addChildTargetFn(self, fn, *args, **kwargs):
        """
        Adds a child target fn. See TargetFunctionWrappingTarget. 
        Returns new child Target.
        """
        return self.addChild(TargetFunctionWrappingTarget(fn, *args, **kwargs)) 
    
    def addChildCommand(self, childCommand):
        """
        A command to be run as child of the job tree.
        """
        return self.__childCommands.append(str(childCommand))
    
    @staticmethod
    def wrapTargetFn(fn, *args, **kwargs):
        """
        Makes a Target out of a target function.
        
        Convenience function for constructor of TargetFunctionWrappingTarget
        """
        return TargetFunctionWrappingTarget(fn, *args, **kwargs)
 
    @staticmethod
    def wrapFn(fn, *args, **kwargs):
        """
        Makes a Target out of a function.
        
        Convenience function for constructor of FunctionWrappingTarget
        """
        return FunctionWrappingTarget(fn, *args, **kwargs)
    
    ##The following functions are used for creating/writing/updating/reading/deleting global files.
    ##
    
    def writeGlobalFile(self, localFileName):
        """
        Takes a file (as a path) and uploads it to to the global file store, returns
        an ID that can be used to retrieve the file. 
        """
        return self.jobStore.writeFile(self.job.jobStoreID, localFileName)
    
    def updateGlobalFile(self, fileStoreID, localFileName):
        """
        Replaces the existing version of a file in the global file store, keyed by the fileStoreID. 
        Throws an exception if the file does not exist.
        """
        self.jobStore.updateFile(fileStoreID, localFileName)
    
    def readGlobalFile(self, fileStoreID, localFilePath=None):
        """
        Returns a path to a local copy of the file keyed by fileStoreID. The version
        will be consistent with the last copy of the file written/updated to the global
        file store. If localFilePath is not None, the returned file path will be localFilePath.
        """
        if localFilePath is None:
            localFilePath = getTempFile(rootDir=self.getLocalTempDir())
        self.jobStore.readFile(fileStoreID, localFilePath)
        return localFilePath
    
    def deleteGlobalFile(self, fileStoreID):
        """
        Deletes a global file with the given fileStoreID. Returns true if file exists, else false.
        """
        return self.jobStore.deleteFile(fileStoreID)
    
    def writeGlobalFileStream(self):
        """
        Similar to writeGlobalFile, but returns a context manager yielding a tuple of 1) a file
        handle which can be written to and 2) the ID of the resulting file in the job store. The
        yielded file handle does not need to and should not be closed explicitly.
        """
        return self.jobStore.writeFileStream(self.job.jobStoreID)
    
    def updateGlobalFileStream(self, fileStoreID):
        """
        Similar to updateGlobalFile, but returns a context manager yielding a file handle which
        can be written to. The yielded file handle does not need to and should not be closed
        explicitly.
        """
        return self.jobStore.updateFileStream(fileStoreID)
    
    def getEmptyFileStoreID(self):
        """
        Returns the ID of a new, empty file.
        """
        return self.jobStore.getEmptyFileStoreID(self.job.jobStoreID)
    
    def readGlobalFileStream(self, fileStoreID):
        """
        Similar to readGlobalFile, but returns a context manager yielding a file handle which can
        be read from. The yielded file handle does not need to and should not be closed explicitly.
        """
        return self.jobStore.readFileStream(fileStoreID)
    
    ##The following function is used for passing return values between target run functions
    
    def rv(self, argIndex):
        """
        Gets a PromisedTargetReturnValue, representing the argIndex return 
        value of the run function.
        This PromisedTargetReturnValue, if a class attribute of a Target instance, 
        call it T, will be replaced
        by the actual return value just before the run function of T is called. 
        rv therefore allows the output from one Target to be wired as input to another 
        Target before either is actually run.  
        """
        #Check if the return value has already been promised and if it has
        #return it
        if argIndex in self.__rvs:
            return self.__rvs[argIndex]
        #Create, store, return new PromisedTargetReturnValue
        self.__rvs[argIndex] = PromisedTargetReturnValue()
        return self.__rvs[argIndex]
       
    ##Functions interrogating attributes of the target
    
    def getLocalTempDir(self):
        """Get the local temporary directory.
        """
        return self.localTempDir

    def getCpu(self, defaultCpu=sys.maxint):
        """Returns the number of cpus requested by the job.
        """
        return defaultCpu if self.__cpu == sys.maxint else self.__cpu
    
    def getMemory(self, defaultMemory=sys.maxint):
        """Returns the number of bytes of memory that were requested by the job.
        """
        return defaultMemory if self.__memory == sys.maxint else self.__memory
    
    def getFollowOn(self):
        """Get the follow on target.
        """
        return self.__followOn
     
    def getChildren(self):
        """Get the child targets.
        """
        return self.__children[:]
    
    def getChildCommands(self):
        """Gets the child commands, as a list of tuples of strings and floats, representing the run times.
        """
        return self.__childCommands[:]
    
    def logToMaster(self, string):
        """Send a logging message to the master. Will only reported if logging is set to INFO level in the master.
        """
        self.__loggingMessages.append(str(string))
        
    def getUserScript(self):
        return self.userModule

    #Functions used for setting up and running a jobTree
    
    @staticmethod
    def getDefaultOptions():
        """
        Returns am optparse.Values object name (string) : value
        options used by job-tree. See the help string 
        of jobTree to see these options.
        """
        parser = OptionParser()
        Target.addJobTreeOptions(parser)
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
        """Runs jobtree using the given options (see Target.getDefaultOptions
        and Target.addJobTreeOptions).
        """
        setLoggingFromOptions(options)
        config, batchSystem, jobStore, jobTreeState = setupJobTree(options)
        if not jobTreeState.started: #We setup the first job.
            memory = self.getMemory(defaultMemory=float(config.attrib["default_memory"]))
            cpu = self.getCpu(defaultCpu=float(config.attrib["default_cpu"]))
            #Make job, set the command to None initially
            logger.info("Adding the first job")
            job = jobStore.createFirstJob(command=None, memory=memory, cpu=cpu)
            #This calls gives valid jobStoreFileIDs to each promised value
            self._setFileIDsForPromisedValues(jobStore, job.jobStoreID)
            #Now set the command properly (this is a hack)
            job.followOnCommands[-1] = (self._makeRunnable(jobStore, job.jobStoreID), memory, cpu, 0)
            #Now write
            jobStore.store(job)
            jobTreeState = jobStore.loadJobTreeState() #This reloads the state
        else:
            logger.critical("Jobtree is being reloaded from previous run with %s jobs to start" % len(jobTreeState.updatedJobs))
        return mainLoop(config, batchSystem, jobStore, jobTreeState)
    
    def cleanup(self, options):
        """Removes the jobStore backing the jobTree.
        """
        config, batchSystem, jobStore, jobTreeState = setupJobTree(options)
        jobStore.deleteJobStore()

####
    # Protected functions
#### 
    
    def _switchOutPromisedTargetReturnValues(self):
        """
        Replaces each PromisedTargetReturnValue instance that is a class 
        attribute of the target with PromisedTargetReturnValue's stored value.
        Will do this also for PromisedTargetReturnValue instances within lists, 
        tuples, sets or dictionaries that are class attributes of the Target.
        
        This function is called just before the run method.
        """

        # FIXME: why is the substitution only done one level deep? IOW, what about, say, a dict of lists

        # FIXME: This replaces subclasses of list, tuple and dict with the instances of the respective base ...
        # FIXME: ... class. For a potential fix, see my quick fix for tuples.

        # FIXME: This unnecessarily touches attributes that don't have a promise. It touches attributes of the ...
        # FIXME: ... target base classes which provably have no PromisedTargetReturnValues in them

        #Iterate on the class attributes of the Target instance.
        for attr, value in self.__dict__.iteritems():
            #If the variable is a PromisedTargetReturnValue replace with the 
            #actual stored return value of the PromisedTargetReturnValue
            #else if the variable is a list, tuple or set or dict replace any 
            #PromisedTargetReturnValue instances within
            #the container with the stored return value.

            # FIXME: This lambda might become more readable if "value" wasn't closed over but passed in.

            # FIXME: I find lambdas awkward. A simple nested def should suffice

            f = lambda : map(lambda x : x.loadValue(self.jobStore) if 
                        isinstance(x, PromisedTargetReturnValue) else x, value)
            if isinstance(value, PromisedTargetReturnValue):
                self.__dict__[attr] = value.loadValue(self.jobStore)
            elif isinstance(value, list):
                self.__dict__[attr] = f()
            elif value.__class__ == tuple:
                self.__dict__[attr] = tuple(f())
            elif isinstance(value, tuple):
                self.__dict__[attr] = value.__class__(*f())
            elif isinstance(value, set):
                self.__dict__[attr] = set(f())
            elif isinstance(value, dict):
                self.__dict__[attr] = dict(map(lambda x : (x, value[x].loadValue(self.jobStore) if 
                        isinstance(x, PromisedTargetReturnValue) else value[x]), value))
    
    def _setFileVariables(self, jobStore, job, localTempDir):
        """
        Sets the jobStore, job and localTemptDir for the target, each
        of which is used for computation.
        """
        self.jobStore = jobStore
        self.job = job
        self.localTempDir = localTempDir
        
    def _unsetFileVariables(self):
        """
        Unsets the file variables, so that they don't get pickled.
        """
        self.jobStore = None
        self.job = None
        self.localTempDir = None
    
    def _addParent(self, parentTarget):
        """
        Adds a parent target to the set of parent targets.
        """
        if parentTarget in self.__parents:
            raise RuntimeError("The given target is already a parent of this target")
        self._parent.add(parentTarget)
        
    def _setFollowOnFrom(self, followOnFrom):
        if self.__followOnFrom != None:
            raise RuntimeError("This target already has a follow on set")
        self.__followOnFrom = followOnFrom
      
    def _setFileIDsForPromisedValues(self, jobStore, jobStoreID):
        """
        Sets the jobStoreFileID for each PromisedTargetReturnValue in the 
        graph of targets created.
        """
        #Replace any None references with valid jobStoreFileIDs. We 
        #do this here, rather than within the original constructor of the
        #promised value because we don't necessarily have access to the jobStore when 
        #the PromisedTargetReturnValue instances are created.
        for PromisedTargetReturnValue in self.__rvs.values():
            if PromisedTargetReturnValue.jobStoreFileID == None:
                PromisedTargetReturnValue.jobStoreFileID = jobStore.getEmptyFileStoreID(jobStoreID)
        #Now recursively do the same for the children and follow ons.
        for childTarget in self.getChildren():
            childTarget._setFileIDsForPromisedValues(jobStore, jobStoreID)
        if self.getFollowOn() != None:
            self.getFollowOn()._setFileIDsForPromisedValues(jobStore, jobStoreID)
        
    def _makeRunnable(self, jobStore, jobStoreID):
        with jobStore.writeFileStream(jobStoreID) as ( fileHandle, fileStoreID ):
            cPickle.dump(self, fileHandle, cPickle.HIGHEST_PROTOCOL)

        i = set( self.__importStrings )
        classNames = " ".join(i)
        return "scriptTree %s %s %s" % (fileStoreID, self.__dirName, classNames)
    
    def _verifyTargetAttributesExist(self):
        """ _verifyTargetAttributesExist() checks to make sure that the Target
        instance has been properly instantiated. Returns None if instance is OK,
        raises an error otherwise.
        """
        attributes = vars(self)
        required = ['_Target__followOn', '_Target__children', '_Target__childCommands',
                '_Target__memory', '_Target__cpu']
        for r in required:
            if r not in attributes:
                raise RuntimeError("Error, there is a missing attribute, %s, "
                                    "from a Target sub instance %s, "
                                    "did you remember to call Target.__init__(self) in the %s "
                                    "__init__ method?" % ( r, self.__class__.__name__,
                                                           self.__class__.__name__))
        
    def _execute(self, job, stats, localTempDir, jobStore, 
                memoryAvailable, cpuAvailable,
                defaultMemory, defaultCpu):
        """This is the core method for running the target within a worker.
        """ 
        if stats != None:
            startTime = time.time()
            startClock = getTotalCpuTime()
        
        baseDir = os.getcwd()
        
        #Debug check that we have the right amount of CPU and memory for the job in hand
        targetMemory = self.getMemory()
        if targetMemory != sys.maxint:
            assert targetMemory <= memoryAvailable
        targetCpu = self.getCpu()
        if targetCpu != sys.maxint:
            assert targetCpu <= cpuAvailable
        #Set the jobStore for the target, used for file access
        self._setFileVariables(jobStore, job, localTempDir)
        #Switch out any promised return value instances with the actual values
        self._switchOutPromisedTargetReturnValues()
        #Run the target, first cleanup then run.
        returnValues = self.run()
        #Set the promised value jobStoreFileIDs
        self._setFileIDsForPromisedValues(jobStore, job.jobStoreID)
        #Store the return values for any promised return value
        self._setReturnValuesForPromises(self.target, returnValues, jobStore)
        #Now unset the job store to prevent it being serialised
        self._unsetFileVariables()
        #Change dir back to cwd dir, if changed by target (this is a safety issue)
        if os.getcwd() != baseDir:
            os.chdir(baseDir)
        #Cleanup after the target
        system("rm -rf %s/*" % localTempDir)
        #Handle the follow on
        followOn = self.getFollowOn()
        if followOn is not None: 
            job.followOnCommands.append((followOn._makeRunnable(jobStore, job.jobStoreID),
                                         followOn.getMemory(defaultMemory),
                                         followOn.getCpu(defaultCpu),
                                         len(followOn.__parents) + 1))
        #Now add the children to the newChildren target
        newChildren = self.getChildren()
        newChildren.reverse()
        assert len(job.children) == 0
        while len(newChildren) > 0:
            child = newChildren.pop()
            job.children.append((child._makeRunnable(jobStore, job.jobStoreID),
                                 child.getMemory(defaultMemory),
                                 child.getCpu(defaultCpu),
                                 len(child.__parents) + \
                                 (0 if child.__followOnFrom == None else 1)))
        
        #Now build jobs for each child command
        for childCommand in self.getChildCommands():
            job.children.append((childCommand, defaultMemory, defaultCpu))
        
        #Finish up the stats
        if stats != None:
            stats = ET.SubElement(stats, "target")
            stats.attrib["time"] = str(time.time() - startTime)
            totalCpuTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            stats.attrib["clock"] = str(totalCpuTime - startClock)
            stats.attrib["class"] = ".".join((self.__class__.__name__,))
            stats.attrib["memory"] = str(totalMemoryUsage)
        
        #Return any logToMaster logging messages
        return self.__loggingMessages
    
    @staticmethod
    def _setReturnValuesForPromises(target, returnValues, jobStore):
        """
        Sets the values for promises using the return values from the target's
        run function.
        """
        for i in target.__rvs.keys():
            if isinstance(returnValues, tuple):
                argToStore = returnValues[i]
            else:
                if i != 0:
                    raise RuntimeError("Referencing return value index (%s)"
                                " that is out of range: %s" % (i, returnValues))
                argToStore = returnValues
            target.__rvs[i]._storeValue(argToStore, jobStore)

class FunctionWrappingTarget(Target):
    """
    Target used to wrap a function.
    
    Function can not be nested function or class function, currently.
    *args and **kwargs are used as the arguments to the function.
    """
    def __init__(self, userFunction, *args, **kwargs):
        # FIXME: I'd rather not duplicate the defaults here, unless absolutely necessary
        cpu = kwargs.pop("cpu") if "cpu" in kwargs else sys.maxint
        memory = kwargs.pop("memory") if "memory" in kwargs else sys.maxint
        Target.__init__(self, memory=memory, cpu=cpu)
        self.userFunctionModule = ModuleDescriptor.forModule(userFunction.__module__)
        self.userFunctionName = str(userFunction.__name__)
        self._args=args
        self._kwargs=kwargs
        
    def _getUserFunction(self):
        userFunctionModule = self.userFunctionModule.localize()
        if userFunctionModule.dirPath not in sys.path:
            # FIXME: prepending to sys.path will probably fix #103
            sys.path.append(userFunctionModule.dirPath)
        return getattr(importlib.import_module(userFunctionModule.name), self.userFunctionName)

    def run(self):
        userFunction = self._getUserFunction( )
        return userFunction(*self._args, **self._kwargs)

    def getUserScript(self):
        return self.userFunctionModule


class TargetFunctionWrappingTarget(FunctionWrappingTarget):
    """
    Target used to wrap a function.
    A target function is a function which takes as its first argument a reference
    to the wrapping target.
    """
    def run(self):
        userFunction = self._getUserFunction()
        return userFunction(*((self,) + tuple(self._args)), **self._kwargs)

class PromisedTargetReturnValue():
    """
    References a return value from a Target's run function. Let T be a target. 
    Instances of PromisedTargetReturnValue are created by
    T.rv(i), where i is an integer reference to a return value of T's run function
    (casting the return value as a tuple). 
    When passed to the constructor of a different Target the PromisedTargetReturnValue
    will be replaced by the actual referenced return value after the Target's run function 
    has finished (see Target._switchOutPromisedTargetReturnValues). 
    This mechanism allows a return values from one Target's run method to be input
    argument to Target before the former Target's run function has been executed.
    """ 
    def __init__(self):
        self.jobStoreFileID = None #The None value is
        #replaced with a real jobStoreFileID by the Target object.
        
    def loadValue(self, jobStore):
        """
        Unpickles the promised value and returns it. 
        
        If it encounters a chain of promises it will traverse the chain until
        if finds the intended value.
        """
        assert self.jobStoreFileID != None 
        with jobStore.readFileStream(self.jobStoreFileID) as fileHandle:
            value = cPickle.load(fileHandle) #If this doesn't work, then it is 
            #likely the Target that is promising value has not yet been run.
            if isinstance(value, PromisedTargetReturnValue):
                raise RuntimeError("A nested PromisedTargetReturnValue has been found.") #We do not allow the return of PromisedTargetReturnValue instance from the run function
            return value

    def _storeValue(self, valueToStore, jobStore):
        """
        Pickle the promised value. This is done by the target.
        """
        assert self.jobStoreFileID != None
        with jobStore.updateFileStream(self.jobStoreFileID) as fileHandle:
            cPickle.dump(valueToStore, fileHandle, cPickle.HIGHEST_PROTOCOL)
