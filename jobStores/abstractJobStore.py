from abc import ABCMeta, abstractmethod
import xml.etree.cElementTree as ET

class JobTreeState(object):
    """Represents the state of the jobTree.
    """
    def __init__(self):
        self.childJobStoreIdToParentJob = {} #This is a hash of jobStoreIDs to
        #the parent jobs.
        self.childCounts = {} #Hash of jobs to counts of numbers of children.
        self.updatedJobs = set() #Jobs that have no children but 
        #one or more follow-on commands
        self.shellJobs = set() #Jobs that have no children or follow-on commands

class AbstractJobStore(object):

    __metaclass__ = ABCMeta

    """Represents the jobTree on disk/in a db.
    """ 
    def __init__(self, jobStoreString, create, config):
        """jobStoreString is a configuration string used to initialise the jobStore.
        If config != None then the config option is written to the global shared file
        "config.xml", which can be retrieved using the readSharedFileStream method with the 
        argument "config.xml".
        If config == None (in which case create must be False), then the self.config 
        object is read from the file-store.
        """
        self.jobTreeState = None #This will be none until "loadJobTree" is called.
        self.jobStoreString = jobStoreString
        if create or config is not None:
            assert config is not None
            self.updateConfig(config)
        else:
            fileHandle = self.readSharedFileStream("config.xml")
            self.config = ET.parse(fileHandle).getroot()
            fileHandle.close()

    @abstractmethod
    def createFirstJob(self, command, memory, cpu):
        """Creates and returns the root job of the jobTree from which all others must be created. 
        This will only be called once, at the very beginning of the jobTree creation.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def exists(self, jobStoreID):
        """Returns true if the job is in the store, else false.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def load(self, jobStoreID):
        """Loads a job for the given jobStoreID.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def write(self, job):
        """Updates a job's status in the store atomically
        """
        raise NotImplementedError()
    
    @abstractmethod
    def update(self, job, childCommands):
        """Creates a set of child jobs for the given job using the list of child-commands 
        and updates state of job atomically on disk with new children.
        Each child command is represented as a tuple of command, memory and cpu.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def delete(self, job):
        """Removes from store atomically, can not then subsequently call load(), 
        write(), update(), etc. with the job.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def loadJobTreeState(self):
        """Initialises self.jobTreeState from the contents of the store.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def writeFile(self, jobStoreID, localFileName):
        """Takes a file (as a path) and uploads it to to the jobStore file system, returns
        an ID that can be used to retrieve the file. jobStoreID is the id of the job from 
        which the file is being created. When delete(job) is called all files written with the given
        job.jobStoreID will be removed from the jobStore.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def updateFile(self, jobStoreFileID, localFileName):
        """Replaces the existing version of a file in the jobStore. Throws an exception if
        the file does not exist.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def readFile(self, jobStoreFileID, localFileName):
        """Copies the file referenced by jobStoreFileID to the given local file path. The version
        will be consistent with the last copy of the file written/updated.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def deleteFile(self, jobStoreFileID):
        """Deletes a file with the given jobStoreFileID. Throws an exception if the file
        does not exist.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def writeFileStream(self, jobStoreID):
        """As writeFile, but returns a fileHandle which can be written from. Handle must be closed
        to ensure transmission of the file.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def updateFileStream(self, jobStoreFileID):
        """As updateFile, but returns a fileHandle which can be written to. Handle must be closed
        to ensure transmission of the file.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def getEmptyFileStoreID(self, jobStoreID):
        """Returns the ID of a new, empty file.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def readFileStream(self, jobStoreFileID):
        """As readFile, but returns a file handle instead of a path.
        """
        raise NotImplementedError()
    
    @abstractmethod
    def writeSharedFileStream(self, globalFileID):
        """Returns a writable file-handle to a global file using the globalFileID. 
        """
        raise NotImplementedError()
    
    def updateConfig(self, config):
        """Updates the config.
        """
        self.config = config
        fileHandle = self.writeSharedFileStream("config.xml")
        ET.ElementTree(config).write(fileHandle)
        fileHandle.close()
    
    @abstractmethod
    def readSharedFileStream(self, globalFileID):
        """Returns a readable file-handle to the global file referenced by globalFileID.
        """
        raise NotImplementedError()
