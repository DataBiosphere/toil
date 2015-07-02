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

from jobTree.resource import ModuleDescriptor
from jobTree.lib.bioio import getTempFile

try:
    import cPickle 
except ImportError:
    import pickle as cPickle


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
        self.loggingMessages = []
        self._rvs = {}

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
        return followOnTarget
        
    def addChild(self, childTarget):
        """
        Adds the child target to be run as child of this target. Returns childTarget.
        """
        self.__children.append(childTarget)
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
        if argIndex in self._rvs:
            return self._rvs[argIndex]
        #Create, store, return new PromisedTargetReturnValue
        self._rvs[argIndex] = PromisedTargetReturnValue()
        return self._rvs[argIndex]
       
    ##Functions interrogating attributes of the target
    
    def getLocalTempDir(self):
        """Get the local temporary directory.
        """
        return self.localTempDir
        
    def getMemory(self):
        """Returns the number of bytes of memory that were requested by the job.
        """
        return self.__memory
    
    def getCpu(self):
        """Returns the number of cpus requested by the job.
        """
        return self.__cpu
    
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
        self.loggingMessages.append(str(string))

    def getUserScript(self):
        return self.userModule

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
            # If the variable is a PromisedTargetReturnValue replace with the
            # actual stored return value of the PromisedTargetReturnValue
            # else if the variable is a list, tuple or set or dict replace any
            # PromisedTargetReturnValue instances within
            # the container with the stored return value.

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
        
    def _getMasterLoggingMessages(self):
        return self.loggingMessages[:]

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
        self._args = args
        self._kwargs = kwargs

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
        #replaced with a real jobStoreFileID by the Stack object.
        
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
        Pickle the promised value. This is done by the stack.
        """
        assert self.jobStoreFileID != None
        with jobStore.updateFileStream(self.jobStoreFileID) as fileHandle:
            cPickle.dump(valueToStore, fileHandle, cPickle.HIGHEST_PROTOCOL)
