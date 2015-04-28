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
        self.childJobStoreIdToParentJob = {} #This is a hash of jobStoreIDs to
        #the parent jobs.
        self.childCounts = {} #Hash of jobs to counts of numbers of children.
        self.updatedJobs = set() #Jobs that have no children but 
        #one or more follow-on commands
        self.shellJobs = set() #Jobs that have no children or follow-on commands

class AbstractJobStore:
    """Represents the jobTree on disk/in a db.
    """
    def __init__(self, config, create=True):
        self.jobTreeState = None #This will be none until "loadJobTree" is called.
        self.config = config
    
    def createFirstJob(self, command, memory, cpu):
        """Creates and returns the root job of the jobTree from which all others must be created. 
        This will only be called once, at the very beginning of the jobTree creation.
        """
        pass
    
    def exists(self, jobStoreID):
        """Returns true if the job is in the store, else false.
        """
        pass
    
    def load(self, jobStoreID):
        """Loads a job for the given jobStoreID.
        """
        pass
    
    def write(self, job):
        """Updates a job's status in the store atomically
        """
        pass
    
    def update(self, job, childCommands):
        """Creates a set of child jobs for the given job using the list of child-commands 
        and updates state of job atomically on disk with new children.
        Each child command is represented as a tuple of command, memory and cpu.
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
    
    def readFile(self, jobStoreFileID, localFileName):
        """Copies the file referenced by jobStoreFileID to the given local file path. The version
        will be consistent with the last copy of the file written/updated.
        """
        pass
    
    def deleteFile(self, jobStoreFileID):
        """Deletes a file with the given jobStoreFileID. Throws an exception if the file
        does not exist.
        """
        pass
    
    def writeFileStream(self, jobStoreID):
        """As writeFile, but returns a fileHandle which can be written from. Handle must be closed
        to ensure transmission of the file.
        """
        pass
    
    def updateFileStream(self, jobStoreFileID):
        """As updateFile, but returns a fileHandle which can be written to. Handle must be closed
        to ensure transmission of the file.
        """
        pass
    
    def getEmptyFileStoreID(self, jobStoreID):
        """Returns the ID of a new, empty file.
        """
        pass
    
    def readFileStream(self, jobStoreFileID):
        """As readFile, but returns a file handle instead of a path.
        """
        pass
    
    def writeSharedFileStream(self, globalName):
        """As writeFileStream, but not require a jobStoreID. Used for storing global
        config files. Returns pair of file handle and job store ID.
        """
        pass
