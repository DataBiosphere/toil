import os
import re
import time
from jobTree.src.bioio import system
from sonLib.bioio import logFile
from sonLib.bioio import logger

class JobTreeState:
    """Represents the state of the jobTree.
    """
    def __init__(self):
        self.childJobStoreIdToParentJob = {}
        self.childCounts = {}
        self.updatedJobs = set()
        self.shellJobs = set()

class AbstractJobStore:
    """Represents the jobTree on disk/in a db.
    """
    def __init__(self, config, create=True):
        self.jobTreeState = None #This will be none until "loadJobTree" is called.
        self.config = config
    
    def createFirstJob(self, command, memory, cpu):
        """Creates the root job of the jobTree from which all others must be created.
        """
        return Job(command=command, memory=memory, cpu=cpu, 
              tryCount=int(self.config.attrib["try_count"]), 
              jobStoreID=self._getJobFileDirName())
    
    def exists(self, jobStoreID):
        """Returns true if the job is in the store, else false.
        """
        pass
    
    def load(self, jobStoreID):
        """Loads a job.
        """
        pass
    
    def write(self, job):
        """Updates a job's status in store atomically
        """
        pass
    
    def update(self, job, childCommands):
        """Creates a set of child jobs for the given job using the list of child-commands 
        and updates state of job atomically on disk with new children.
        """
        pass
    
    def delete(self, job):
        """Removes from store atomically, can not then subsequently call load(), 
        write(), update(), etc. with the job.
        """
        pass
    
    def loadJobTreeState(self):
        """Initialises self.jobTreeState from the contents of the store.
        """
        pass
    
    def transmitJobLogFile(self, jobStoreID, localLogFile):
        pass
    
    def getJobLogFile(self, jobStoreID):
        pass
            
    def getJobLogFileName(self, jobStoreID):
        return os.path.join(jobStoreID, "log.txt")