#!/usr/bin/env python

import sys
from workflow.jobTree.lib.bioio import system

class Target:
    """Each job wrapper extends this class.
    """
    
    def __init__(self, time=sys.maxint, memory=sys.maxint, cpu=sys.maxint):
        """This method must be called by any overiding constructor.
        """
        self.__followOn = None
        self.__children = []
        self.__childCommands = []
        self.__time = time
        self.__memory = memory
        self.__cpu = cpu
        self.globalTempDir = None
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
        
    def addChildTarget(self, childTarget):
        """Adds the child target to be run as child of this target.
        """
        self.__children.append(childTarget)
        
    def addChildCommand(self, childCommand, runTime=sys.maxint):
        """A command to be run as child of the job tree.
        """
        self.__childCommands.append((str(childCommand), float(runTime)))
    
    def getRunTime(self):
        """Get the time the target is anticipated to run.
        """
        return self.__time
    
    def getGlobalTempDir(self):
        """Get the global temporary directory.
        """
        #Check if we have initialised the global temp dir - doing this
        #just in time prevents us from creating temp directories unless we have to.
        if self.globalTempDir == None:
            self.globalTempDir = self.stack.getGlobalTempDir()
        return self.globalTempDir
    
    def getLocalTempDir(self):
        """Get the local temporary directory.
        """
        return self.stack.getLocalTempDir()
        
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
        """Send a logging message to the master.
        """
        self.loggingMessages.append(string)
    
####
#Private functions
#### 
    
    def setGlobalTempDir(self, globalTempDir):
        """Sets the global temp dir.
        """
        self.globalTempDir = globalTempDir
        
    def isGlobalTempDirSet(self):
        return self.globalTempDir != None
    
    def setStack(self, stack):
        """Sets the stack object that is calling the target.
        """
        self.stack = stack
