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
        pass
    
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
        ##Consider factory method for creating jobs so that input childCommands are jobs.
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
    
    def writeFile(self, jobStoreID, localFileName):
        """Takes a file (as a path) and uploads it to to the jobStore file system, returns
        an ID that can be used to retrieve the file. jobStoreID is the id of the job from 
        which the file is being created. When delete(job) is called all files written with the given
        job.jobStoreID will be removed from the jobStore.
        """
        pass
    
    def updateFile(self, jobStoreFileID, localFileName):
        """Replaces the existing version of a file in the jobStore. Throws an exception if
        the file does not exist.
        """
        pass
    
    def readFile(self, jobStoreFileID):
        """Returns a path to a copy of the file keyed by jobStoreFileID. The version
        will be consistent with the last copy of the file written/updated.
        """
        pass
    
    def deleteFile(self, jobStoreFileID):
        """Deletes a file with the given jobStoreFileID. Throws an exception if the file
        does not exist.
        """
        pass
    
    def readFileStream(self, jobStoreFileID):
        """As readFile, but returns a file handle instead of a path.
        """
        pass
