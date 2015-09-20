# Copyright (C) 2015 UCSC Computational Genomics Lab
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
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
import re
try:
    import cPickle 
except ImportError:
    import pickle as cPickle

class NoSuchJobException( Exception ):
    def __init__( self, jobStoreID ):
        super( NoSuchJobException, self ).__init__( "The job '%s' does not exist" % jobStoreID )

class ConcurrentFileModificationException( Exception ):
    def __init__( self, jobStoreFileID ):
        super( ConcurrentFileModificationException, self ).__init__(
            'Concurrent update to file %s detected.' % jobStoreFileID )

class NoSuchFileException( Exception ):
    def __init__( self, fileJobStoreID ):
        super( NoSuchFileException, self ).__init__( "The file '%s' does not exist" % fileJobStoreID )

class JobStoreCreationException( Exception ):
    def __init__( self, message ):
        super( JobStoreCreationException, self ).__init__( message )

class AbstractJobStore( object ):
    """ 
    Represents the physical storage for the jobs and associated files in a toil.
    """
    __metaclass__ = ABCMeta

    def __init__( self, config=None ):
        """
        :param config: If config is not None then the
        given configuration object will be written to the shared file "config.pickle" which can
        later be retrieved using the readSharedFileStream. See writeConfigToStore. 
        If this file already exists it will be overwritten. If config is None, 
        the shared file "config.pickle" is assumed to exist and is retrieved. See loadConfigFromStore.
        """
        #Now get on with reading or writing the config
        if config is None:
            with self.readSharedFileStream( "config.pickle", isProtected=False ) as fileHandle:
                self.__config = cPickle.load(fileHandle)
        else:
            self.__config = config
            self.writeConfigToStore()
            
    def writeConfigToStore(self):
        """
        Re-writes the config attribute to the jobStore, so that its values can be retrieved 
        if the jobStore is reloaded.
        """
        with self.writeSharedFileStream( "config.pickle", isProtected=False ) as fileHandle:
            cPickle.dump(self.__config, fileHandle, cPickle.HIGHEST_PROTOCOL)
    
    @property
    def config( self ):
        return self.__config
    
    @staticmethod
    def _checkJobStoreCreation(create, exists, jobStoreString):
        """
        Consistency checks which will result in exceptions if we attempt to overwrite an existing
        jobStore.

        :type create: boolean

        :type exists: boolean

        :raise JobStoreCreationException:  Thrown if create=True and exists=True or create=False
                                           and exists=False
        """
        if create and exists:
            raise JobStoreCreationException("The job store '%s' already exists. " 
                               "Use --restart or 'toil restart' to resume this jobStore, "
                               "else remove it to start from scratch" % jobStoreString)
        if not create and not exists:
            raise JobStoreCreationException("The job store '%s' does not exist, so there "
                                "is nothing to restart." % jobStoreString)
    
    @abstractmethod
    def deleteJobStore( self ):
        """
        Removes the jobStore from the disk/store. Careful!
        """
        raise NotImplementedError( )
    
    ##Cleanup functions
    
    def clean(self):
        """
        Function to cleanup the state of a jobStore after a restart.
        Fixes jobs that might have been partially updated.
        Resets the try counts.
        """
        #Collate any jobs that were in the process of being created/deleted
        jobsToDelete = set()
        for job in self.jobs():
            for updateID in job.jobsToDelete:
                jobsToDelete.add(updateID)
            
        #Delete the jobs that should be deleted
        if len(jobsToDelete) > 0:
            for job in self.jobs():
                if job.updateID in jobsToDelete:
                    self.delete(job.jobStoreID)
        
        #Cleanup the state of each job
        for job in self.jobs():
            changed = False #Flag to indicate if we need to update the job
            #on disk
            
            if len(job.jobsToDelete) != 0:
                job.jobsToDelete = set()
                changed = True
                
            #While jobs at the end of the stack are already deleted remove
            #those jobs from the stack (this cleans up the case that the job
            #had successors to run, but had not been updated to reflect this)
            while len(job.stack) > 0:
                jobs = [ command for command in job.stack[-1] if self.exists(command[0]) ]
                if len(jobs) < len(job.stack[-1]):
                    changed = True
                    if len(jobs) > 0:
                        job.stack[-1] = jobs
                        break
                    else:
                        job.stack.pop()
                else:
                    break
                          
            #Reset the retry count of the job 
            if job.remainingRetryCount < self._defaultTryCount():
                job.remainingRetryCount = self._defaultTryCount()
                changed = True
                          
            #This cleans the old log file which may 
            #have been left if the job is being retried after a job failure.
            if job.logJobStoreFileID != None:
                job.clearLogFile(self)
                changed = True
            
            if changed: #Update, but only if a change has occurred
                self.update(job)
        
        #Remove any crufty stats/logging files from the previous run
        self.readStatsAndLogging(lambda x : None)
    
    ##########################################
    #The following methods deal with creating/loading/updating/writing/checking for the
    #existence of jobs
    ##########################################  

    @abstractmethod
    def create( self, command, memory, cores, disk, updateID=None,
                predecessorNumber=0 ):
        """
        Creates a job, adding it to the store.
        
        Command, memory, cores, updateID, predecessorNumber
        are all arguments to the job's constructor.

        :rtype : toil.jobWrapper.JobWrapper
        """
        raise NotImplementedError( )

    @abstractmethod
    def exists( self, jobStoreID ):
        """
        Returns true if the job is in the store, else false.

        :rtype : bool
        """
        raise NotImplementedError( )

    @abstractmethod
    def getPublicUrl( self,  FileName):
        """
        Returns a publicly accessible URL to the given file in the job store.
        The returned URL starts with 'http:',  'https:' or 'file:'.
        The returned URL may expire as early as 1h after its been returned.
        Throw an exception if the file does not exist.
        :param jobStoreFileID:
        :return:
        """
        raise NotImplementedError()

    @abstractmethod
    def getSharedPublicUrl( self,  jobStoreFileID):
        """
        Returns a publicly accessible URL to the given file in the job store.
        The returned URL starts with 'http:',  'https:' or 'file:'.
        The returned URL may expire as early as 1h after its been returned.
        Throw an exception if the file does not exist.
        :param jobStoreFileID:
        :return:
        """
        raise NotImplementedError()

    @abstractmethod
    def load( self, jobStoreID ):
        """
        Loads a job for the given jobStoreID and returns it.

        :rtype: toil.jobWrapper.JobWrapper

        :raises: NoSuchJobException if there is no job with the given jobStoreID
        """
        raise NotImplementedError( )

    @abstractmethod
    def update( self, job ):
        """
        Persists the job in this store atomically.
        """
        raise NotImplementedError( )

    @abstractmethod
    def delete( self, jobStoreID ):
        """
        Removes from store atomically, can not then subsequently call load(), write(), update(),
        etc. with the job.

        This operation is idempotent, i.e. deleting a job twice or deleting a non-existent job
        will succeed silently.
        """
        raise NotImplementedError( )
    
    def jobs(self):
        """
        Returns iterator on the jobs in the store.
        
        :rtype : iterator
        """
        raise NotImplementedError( )

    ##########################################
    #The following provide an way of creating/reading/writing/updating files 
    #associated with a given job.
    ##########################################  

    @abstractmethod
    def writeFile( self, localFilePath, jobStoreID=None ):
        """
        Takes a file (as a path) and places it in this job store. Returns an ID that can be used
        to retrieve the file at a later time. 
        
        jobStoreID is the id of a job, or None. If specified, when delete(job) 
        is called all files written with the given job.jobStoreID will be 
        removed from the jobStore.
        """
        raise NotImplementedError( )
    
    @abstractmethod
    @contextmanager
    def writeFileStream( self, jobStoreID=None ):
        """
        Similar to writeFile, but returns a context manager yielding a tuple of 
        1) a file handle which can be written to and 2) the ID of the resulting 
        file in the job store. The yielded file handle does not need to and 
        should not be closed explicitly.
        """
        raise NotImplementedError( )
    
    @abstractmethod
    def getEmptyFileStoreID( self, jobStoreID=None ):
        """
        :rtype : string, the ID of a new, empty file. 
        
        jobStoreID is the id of a job, or None. If specified, when delete(job) 
        is called all files written with the given job.jobStoreID will be 
        removed from the jobStore.
        
        Call to fileExists(getEmptyFileStoreID(jobStoreID)) will return True.
        """
        raise NotImplementedError( )

    @abstractmethod
    def readFile( self, jobStoreFileID, localFilePath ):
        """
        Copies the file referenced by jobStoreFileID to the given local file path. The version
        will be consistent with the last copy of the file written/updated.
        """
        raise NotImplementedError( )
    
    @abstractmethod
    @contextmanager
    def readFileStream( self, jobStoreFileID ):
        """
        Similar to readFile, but returns a context manager yielding a file handle which can be
        read from. The yielded file handle does not need to and should not be closed explicitly.
        """
        raise NotImplementedError( )

    @abstractmethod
    def deleteFile( self, jobStoreFileID ):
        """
        Deletes the file with the given ID from this job store.
        This operation is idempotent, i.e. deleting a file twice or deleting a non-existent file
        will succeed silently.
        """
        raise NotImplementedError( )
    
    @abstractmethod
    def fileExists(self, jobStoreFileID ):
        """
        :rtype : True if the jobStoreFileID exists in the jobStore, else False
        """
        raise NotImplementedError()
    
    @abstractmethod
    def updateFile( self, jobStoreFileID, localFilePath ):
        """
        Replaces the existing version of a file in the jobStore. Throws an exception if the file
        does not exist.

        :raises ConcurrentFileModificationException: if the file was modified concurrently during
        an invocation of this method
        """
        raise NotImplementedError( )
    
    ##########################################
    #The following methods deal with shared files, i.e. files not associated 
    #with specific jobs.
    ##########################################  

    sharedFileNameRegex = re.compile( r'^[a-zA-Z0-9._-]+$' )

    # FIXME: Rename to updateSharedFileStream

    @abstractmethod
    @contextmanager
    def writeSharedFileStream( self, sharedFileName, isProtected=True ):
        """
        Returns a context manager yielding a writable file handle to the global file referenced
        by the given name.

        :param sharedFileName: A file name matching AbstractJobStore.fileNameRegex, unique within
        the physical storage represented by this job store

        :raises ConcurrentFileModificationException: if the file was modified concurrently during
        an invocation of this method
        """
        raise NotImplementedError( )

    @abstractmethod
    @contextmanager
    def readSharedFileStream( self, sharedFileName, isProtected=True ):
        """
        Returns a context manager yielding a readable file handle to the global file referenced
        by the given name.
        """
        raise NotImplementedError( )

    @abstractmethod
    def writeStatsAndLogging( self, statsAndLoggingString ):
        """
        Adds the given statistics/logging string to the store of statistics info.
        """
        raise NotImplementedError( )

    @abstractmethod
    def readStatsAndLogging( self, statsAndLoggingCallBackFn):
        """
        Reads stats/logging strings accumulated by "writeStatsAndLogging" function. 
        For each stats/logging file calls the statsAndLoggingCallBackFn with 
        an open, readable file-handle that can be used to parse the stats.
        Returns the number of stat/logging strings processed. 
        Stats/logging files are only read once and are removed from the 
        file store after being written to the given file handle.
        """
        raise NotImplementedError( )

    ## Helper methods for subclasses

    def _defaultTryCount( self ):
        return int( self.config.retryCount+1 )

    @classmethod
    def _validateSharedFileName( cls, sharedFileName ):
        return bool( cls.sharedFileNameRegex.match( sharedFileName ) )
