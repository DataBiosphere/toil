# Copyright (C) 2015-2016 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import absolute_import
import logging

logger = logging.getLogger( __name__ )

class JobWrapper( object ):
    """
    A class encapsulating the minimal state of a Toil job. Instances of this class are persisted
    in the job store and held in memory by the master. The actual state of job objects in user
    scripts is persisted separately since it may be much bigger than the state managed by this
    class and should therefore only be held in memory for brief periods of time.
    """
    def __init__( self, command, memory, cores, disk, preemptable,
                  jobStoreID, remainingRetryCount, predecessorNumber,
                  filesToDelete=None, predecessorsFinished=None, 
                  stack=None, services=None, 
                  startJobStoreID=None, terminateJobStoreID=None,
                  errorJobStoreID=None,
                  logJobStoreFileID=None,
                  checkpoint=None,
                  checkpointFilesToDelete=None ): 
        # The command to be executed and its memory and cores requirements
        self.command = command
        # Max number of bytes used by the job
        self.memory = memory
        # Number of cores to be used by the job
        self.cores = cores
        # Max number of bytes on disk space used by the job
        self.disk = disk
        # Can the job be run on a node that can be preempted
        self.preemptable = preemptable
        # The jobStoreID of the job. JobStore.load(jobStoreID) will return the job
        self.jobStoreID = jobStoreID
        
        # The number of times the job should be retried if it fails This number is reduced by
        # retries until it is zero and then no further retries are made
        self.remainingRetryCount = remainingRetryCount
        
        # This variable is used in creating a graph of jobs. If a job crashes after an update to
        # the jobWrapper but before the list of files to remove is deleted then this list can be
        # used to clean them up.
        self.filesToDelete = filesToDelete or []
        
        # The number of predecessor jobs of a given job. A predecessor is a job which references
        # this job in its stack.
        self.predecessorNumber = predecessorNumber
        # The IDs of predecessors that have finished. When len(predecessorsFinished) ==
        # predecessorNumber then the job can be run.
        self.predecessorsFinished = predecessorsFinished or set()
        
        # The list of successor jobs to run. Successor jobs are stored as 5-tuples of the form (
        # jobStoreId, memory, cores, disk, predecessorNumber). Successor jobs are run in reverse
        # order from the stack.
        self.stack = stack or []
        
        # A jobStoreFileID of the log file for a job. This will be none unless the job failed and
        #  the logging has been captured to be reported on the leader.
        self.logJobStoreFileID = logJobStoreFileID 
        
        # A list of lists of service jobs to run. Each sub list is a list of service jobs
        # descriptions, each of which is stored as a 6-tuple of the form (jobStoreId, memory,
        # cores, disk, startJobStoreID, terminateJobStoreID).
        self.services = services or []
        
        # An empty file in the jobStore which when deleted is used to signal that the service
        # should cease.
        self.terminateJobStoreID = terminateJobStoreID
        
        # Similarly a empty file which when deleted is used to signal that the service is
        # established
        self.startJobStoreID = startJobStoreID
        
        # An empty file in the jobStore which when deleted is used to signal that the service
        # should terminate signaling an error.
        self.errorJobStoreID = errorJobStoreID
        
        # None, or a copy of the original command string used to reestablish the job after failure.
        self.checkpoint = checkpoint
        
        # Files that can not be deleted until the job and its successors have completed
        self.checkpointFilesToDelete = checkpointFilesToDelete

    def setupJobAfterFailure(self, config):
        """
        Reduce the remainingRetryCount if greater than zero and set the memory
        to be at least as big as the default memory (in case of exhaustion of memory,
        which is common).
        """
        self.remainingRetryCount = max(0, self.remainingRetryCount - 1)
        logger.warn("Due to failure we are reducing the remaining retry count of job %s to %s",
                    self.jobStoreID, self.remainingRetryCount)
        # Set the default memory to be at least as large as the default, in
        # case this was a malloc failure (we do this because of the combined
        # batch system)
        if self.memory < config.defaultMemory:
            self.memory = config.defaultMemory
            logger.warn("We have increased the default memory of the failed job to %s bytes",
                        self.memory)

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
            and self.filesToDelete == other.filesToDelete
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
