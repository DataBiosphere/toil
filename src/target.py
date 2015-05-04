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
import logging
import marshal
import types
from sonLib.bioio import getTempFile
import importlib

class Target(object):
    """Each job wrapper extends this class.
    """
    
    def __init__(self, time=sys.maxint, memory=sys.maxint, cpu=sys.maxint):
        """This method must be called by any overiding constructor.
        """
        self.__followOn = None
        self.__children = []
        self.__childCommands = []
        self.__memory = memory
        self.__time = time #This parameter is no longer used by the batch system.
        self.__cpu = cpu
        if self.__module__ == "__main__":
            raise RuntimeError("The module name of class %s is __main__, which prevents us from serialising it properly, \
please ensure you re-import targets defined in main" % self.__class__.__name__)
        self.importStrings = set((".".join((self.__module__, self.__class__.__name__)),))
        self.loggingMessages = []

    def run(self):
        """Do user stuff here, including creating any follow on jobs.
        This function must not re-pickle the pickle file, which is an input file.
        """
        pass
    
    def setFollowOnTarget(self, followOn):
        """Set the follow on target.
        Will complain if follow on already set.
        """
        assert self.__followOn == None
        self.__followOn = followOn

    def setFollowOnFn(self, fn, args=(), kwargs={}, time=sys.maxint, memory=sys.maxint, cpu=sys.maxint):
        """Sets a follow on target fn. See FunctionWrappingTarget.
        """
        self.setFollowOnTarget(FunctionWrappingTarget(fn=fn, args=args, kwargs=kwargs, time=time, memory=memory, cpu=cpu))

    def setFollowOnTargetFn(self, fn, args=(), kwargs={}, time=sys.maxint, memory=sys.maxint, cpu=sys.maxint):
        """Sets a follow on target fn. See TargetFunctionWrappingTarget.
        """
        self.setFollowOnTarget(TargetFunctionWrappingTarget(fn=fn, args=args, kwargs=kwargs, time=time, memory=memory, cpu=cpu)) 

    def addChildTarget(self, childTarget):
        """Adds the child target to be run as child of this target.
        """
        self.__children.append(childTarget)
    
    def addChildFn(self, fn, args=(), kwargs={}, time=sys.maxint, memory=sys.maxint, cpu=sys.maxint):
        """Adds a child fn. See FunctionWrappingTarget.
        """
        self.addChildTarget(FunctionWrappingTarget(fn=fn, args=args, kwargs=kwargs, time=time, memory=memory, cpu=cpu))

    def addChildTargetFn(self, fn, args=(), kwargs={}, time=sys.maxint, memory=sys.maxint, cpu=sys.maxint):
        """Adds a child target fn. See TargetFunctionWrappingTarget.
        """
        self.addChildTarget(TargetFunctionWrappingTarget(fn=fn, args=args, kwargs=kwargs, time=time, memory=memory, cpu=cpu)) 
    
    def addChildCommand(self, childCommand, runTime=sys.maxint):
        """A command to be run as child of the job tree.
        """
        self.__childCommands.append((str(childCommand), float(runTime)))
    
    def getRunTime(self):
        """Get the time the target is anticipated to run.
        """
        return self.__time
    
    ##The following functions are used for creating/writing/updating/reading/deleting global files.
    ##
    
    def writeGlobalFile(self, localFileName):
        """Takes a file (as a path) and uploads it to to the global file store, returns
        an ID that can be used to retrieve the file. 
        """
        return self.jobStore.writeFile(self.job.jobStoreID, localFileName)
    
    def updateGlobalFile(self, fileStoreID, localFileName):
        """Replaces the existing version of a file in the global file store, keyed by the fileStoreID. 
        Throws an exception if the file does not exist.
        """
        self.jobStore.updateFile(fileStoreID, localFileName)
    
    def readGlobalFile(self, fileStoreID, localFilePath=None):
        """Returns a path to a local copy of the file keyed by fileStoreID. The version
        will be consistent with the last copy of the file written/updated to the global
        file store. If localFilePath is not None, the returned file path will be localFilePath.
        """
        if localFilePath is None:
            localFilePath = getTempFile(rootDir=self.getLocalTempDir())
        self.jobStore.readFile(fileStoreID, localFilePath)
        return localFilePath
    
    def deleteGlobalFile(self, fileStoreID):
        """Deletes a global file with the given fileStoreID. Returns true if file exists, else false.
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
        """Returns the ID of a new, empty file.
        """
        return self.jobStore.getEmptyFileStoreID(self.job.jobStoreID)
    
    def readGlobalFileStream(self, fileStoreID):
        """
        Similar to readGlobalFile, but returns a context manager yielding a file handle which can
        be read from. The yielded file handle does not need to and should not be closed explicitly.
        """
        return self.jobStore.readFileStream(fileStoreID)
    
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
        
    @staticmethod
    def makeTargetFn(fn, args=(), kwargs={}, time=sys.maxint, memory=sys.maxint, cpu=sys.maxint):
        """Makes a Target out of a target function! 
        In a target function, the first argument to the function will be a reference to the wrapping target, allowing
        the function to create children/follow ons.
        
        Convenience function for constructor of TargetFunctionWrappingTarget
        """
        return TargetFunctionWrappingTarget(fn=fn, args=args, kwargs=kwargs, time=time, memory=memory, cpu=cpu)
 
####
#Private functions
#### 
    
    def setFileVariables(self, jobStore, job, localTempDir):
        """Sets the jobStore for the target.
        """
        self.jobStore = jobStore
        self.job = job
        self.localTempDir = localTempDir
        
    def unsetFileVariables(self):
        """Unsets the file variables, so that they don't get pickled.
        """
        self.jobStore = None
        self.job = None
        self.localTempDir = None
        
    def getMasterLoggingMessages(self):
        return self.loggingMessages[:]

class FunctionWrappingTarget(Target):
    """Target used to wrap a function.
    
    Function can not be nested function or class function, currently.
    """
    def __init__(self, fn, args=(), kwargs={}, time=sys.maxint, memory=sys.maxint, cpu=sys.maxint):
        Target.__init__(self, time=time, memory=time, cpu=time)
        moduleName = fn.__module__
        if moduleName== '__main__':
            # FIXME: Document why we are doing this, explain why it works in single node
            # TODO: Review with Benedict
            # looks up corresponding module in sys.modules, gets base name, drops .py or .pyc
            moduleDir,moduleName = os.path.split(sys.modules[moduleName].__file__)
            if moduleName.endswith('.py'):
                moduleName = moduleName[:-3]
            elif moduleName.endswith('.pyc'):
                moduleName = moduleName[:-4]
            else:
                raise RuntimeError(
                    "Can only handle main modules loaded from .py or .pyc files, but not '%s'" %
                    moduleName )
        else:
            moduleDir = None

        self.fnModuleDir = moduleDir
        self.fnModule = moduleName #Module of function
        self.fnName = str(fn.__name__) #Name of function
        self.args=args
        self.kwargs=kwargs

    def _getFunc( self ):
        if self.fnModuleDir not in sys.path:
            sys.path.append( self.fnModuleDir )
        return getattr( importlib.import_module( self.fnModule ), self.fnName )

    def run(self):
        func = self._getFunc( )
        func(*self.args, **self.kwargs)

class TargetFunctionWrappingTarget(FunctionWrappingTarget):
    """Target used to wrap a function.
    A target function is a function which takes as its first argument a reference
    to the wrapping target.
    
    Target function can not be closure.
    """
    def run(self):
        func = self._getFunc( )
        func(*((self,) + tuple(self.args)), **self.kwargs)
