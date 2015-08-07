import logging

logger = logging.getLogger( __name__ )

class JobWrapper( object ):
    """
    A class encapsulating the state of a toil batchjob.
    """
    def __init__( self, command, memory, cpu, disk,
                  jobStoreID, remainingRetryCount, 
                  updateID, predecessorNumber,
                  jobsToDelete=None, predecessorsFinished=None, 
                  stack=None, logJobStoreFileID=None): 
        #The command to be executed and its memory and cpu requirements.
        self.command = command
        self.memory = memory #Max number of bytes used by the batchjob
        self.cpu = cpu #Number of cores to be used by the batchjob
        self.disk = disk #Max number of bytes on disk space used by the batchjob
        
        #The jobStoreID of the batchjob. JobStore.load(jobStoreID) will return
        #the batchjob
        self.jobStoreID = jobStoreID
        
        #The number of times the batchjob should be retried if it fails
        #This number is reduced by retries until it is zero
        #and then no further retries are made
        self.remainingRetryCount = remainingRetryCount
        
        #These two variables are used in creating a graph of jobs.
        #The updateID is a unique identifier.
        #The jobsToDelete is a set of updateIDs for jobs being created.
        #During the creation of a root batchjob and its successors, first the
        #root batchjob is created with the list of jobsToDelete including all
        #the successors (referenced by updateIDs). Next the successor jobs are created.
        #Finally the jobsToDelete variable in the root batchjob is set to an empty
        #set and updated. It is easy to verify that as single batchjob
        #creations/updates are atomic, if failure 
        #occurs at any stage up to the completion of the final update 
        #we can revert to the state immediately before the creation
        #of the graph of jobs. 
        self.updateID = updateID
        self.jobsToDelete = jobsToDelete or []
        
        #The number of predecessor jobs of a given batchjob.
        #A predecessor is a batchjob which references this batchjob in its stack.
        self.predecessorNumber = predecessorNumber
        #The IDs of predecessors that have finished. 
        #When len(predecessorsFinished) == predecessorNumber then the
        #batchjob can be run.
        self.predecessorsFinished = predecessorsFinished or set()
        
        #The list of successor jobs to run. Successor jobs are stored
        #as 5-tuples of the form (jobStoreId, memory, cpu, disk, predecessorNumber).
        #Successor jobs are run in reverse order from the stack.
        self.stack = stack or []
        
        #A jobStoreFileID of the log file for a batchjob.
        #This will be none unless the batchjob failed and the logging
        #has been captured to be reported on the leader.
        self.logJobStoreFileID = logJobStoreFileID 

    def setupJobAfterFailure(self, config):
        """
        Reduce the remainingRetryCount if greater than zero and set the memory
        to be at least as big as the default memory (in case of exhaustion of memory,
        which is common).
        """
        self.remainingRetryCount = max(0, self.remainingRetryCount - 1)
        logger.warn("Due to failure we are reducing the remaining retry count of batchjob %s to %s",
                    self.jobStoreID, self.remainingRetryCount)
        # Set the default memory to be at least as large as the default, in
        # case this was a malloc failure (we do this because of the combined
        # batch system)
        if self.memory < float(config.attrib["default_memory"]):
            self.memory = float(config.attrib["default_memory"])
            logger.warn("We have increased the default memory of the failed batchjob to %s bytes",
                        self.memory)

    def clearLogFile( self, jobStore ):
        """
        Clears the log file, if it is set.
        """
        if self.logJobStoreFileID is not None:
            jobStore.deleteFile( self.logJobStoreFileID )
            self.logJobStoreFileID = None

    def setLogFile( self, logFile, jobStore ):
        """
        Sets the log file in the file store. 
        """
        if self.logJobStoreFileID is not None:
            self.clearLogFile(jobStore)
        self.logJobStoreFileID = jobStore.writeFile( self.jobStoreID, logFile )
        assert self.logJobStoreFileID is not None

    def getLogFileHandle( self, jobStore ):
        """
        Returns a context manager that yields a file handle to the log file
        """
        return jobStore.readFileStream( self.logJobStoreFileID )

    # Serialization support methods

    def toDict( self ):
        return self.__dict__.copy( )

    @classmethod
    def fromDict( cls, d ):
        return cls( **d )

    def copy(self):
        """
        :rtype: JobWrapper
        """
        return self.__class__( **self.__dict__ )
    
    def __hash__( self ):
        return hash( self.jobStoreID )

    def __eq__( self, other ):
        return (
            isinstance( other, self.__class__ )
            and self.remainingRetryCount == other.remainingRetryCount
            and self.jobStoreID == other.jobStoreID
            and self.updateID == other.updateID
            and self.jobsToDelete == other.jobsToDelete
            and self.stack == other.stack
            and self.predecessorNumber == other.predecessorNumber
            and self.predecessorsFinished == other.predecessorsFinished
            and self.logJobStoreFileID == other.logJobStoreFileID )

    def __ne__( self, other ):
        return not self.__eq__( other )

    def __repr__( self ):
        return '%s( **%r )' % ( self.__class__.__name__, self.__dict__ )
    
    def __str__(self):
        return str(self.toDict())
