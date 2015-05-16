from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
import re
import xml.etree.cElementTree as ET


class JobTreeState( object ):
    """
    Represents the state of the jobTree. This is returned by jobStore.loadJobTreeState()
    """

    def __init__( self ):
        # TODO: document this field
        self.started = False
        # This is a hash of jobStoreIDs to the parent jobs.
        self.childJobStoreIdToParentJob = { }
        # Hash of parent jobs to counts of numbers of children. There are no entries for jobs
        # without children in this map. IOW, there is no entry in the map whose value is 0.
        self.childCounts = { }
        # Jobs that have no children but one or more follow-on commands
        self.updatedJobs = set( )
        # Jobs that have no children or follow-on commands
        self.shellJobs = set( )


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

class AbstractJobStore( object ):
    """ 
    Represents the physical storage for the jobs and associated files in a jobTree.
    """

    __metaclass__ = ABCMeta

    def __init__( self, config=None ):
        """
        FIXME: describe purpose and post-condition

        :param config: If config is not None then a new physical store will be created and the
        given configuration object will be written to the shared file "config.xml" which can
        later be retrieved using the readSharedFileStream. If config is None, the physical store
        is assumed to already exist and the configuration object is read the shared file
        "config.xml" in that .
        """
        if config is None:
            with self.readSharedFileStream( "config.xml" ) as fileHandle:
                self.__config = ET.parse( fileHandle ).getroot( )
        else:
            with self.writeSharedFileStream( "config.xml" ) as fileHandle:
                ET.ElementTree( config ).write( fileHandle )
            self.__config = config

    @property
    def config( self ):
        return self.__config

    #
    # The following methods deal with creating/loading/updating/writing/checking for the
    # existence of jobs
    #

    @abstractmethod
    def createFirstJob( self, command, memory, cpu ):
        """
        Creates and returns the root job of the jobTree from which all others must be created.
        This will only be called once, at the very beginning of the jobTree creation.

        :rtype : src.job.Job
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
    def load( self, jobStoreID ):
        """
        Loads a job for the given jobStoreID and returns it.

        :rtype : src.job.Job

        :raises: NoSuchJobException if there is no job with the given jobStoreID
        """
        raise NotImplementedError( )

    @abstractmethod
    def store( self, job ):
        """
        Persists the job in this store atomically.
        """
        raise NotImplementedError( )

    @abstractmethod
    def addChildren( self, job, childCommands ):
        """
        Creates a set of child jobs for the given job using the list of child commands and
        persists the job along with the new children atomically to this store. Each child command
        is represented as a tuple of ( command, memory and cpu ). The given job must be persisted
        even if the given list of children is empty.
        """
        raise NotImplementedError( )

    @abstractmethod
    def delete( self, job ):
        """
        Removes from store atomically, can not then subsequently call load(), write(), update(),
        etc. with the job.

        This operation is idempotent, i.e. deleting a job twice or deleting a non-existent job
        will succeed silently.
        """
        raise NotImplementedError( )

    @abstractmethod
    def loadJobTreeState( self ):
        """
        Returns a jobTreeState object based on the state of the store.

        :rtype : JobTreeState
        """
        raise NotImplementedError( )

    ##The following provide an way of creating/reading/writing/updating files associated with a given job.

    @abstractmethod
    def writeFile( self, jobStoreID, localFilePath ):
        """
        Takes a file (as a path) and places it in this job store. Returns an ID that can be used
        to retrieve the file at a later time. jobStoreID is the id of the job from which the file
        is being created. When delete(job) is called all files written with the given
        job.jobStoreID will be removed from the jobStore.
        """
        raise NotImplementedError( )

    @abstractmethod
    def updateFile( self, jobStoreFileID, localFilePath ):
        """
        Replaces the existing version of a file in the jobStore. Throws an exception if the file
        does not exist.

        :raises ConcurrentFileModificationException: if the file was modified concurrently during
        an invocation of this method
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
    def deleteFile( self, jobStoreFileID ):
        """
        Deletes the file with the given ID from this job store. Throws an exception if the file
        does not exist.
        """
        raise NotImplementedError( )

    @abstractmethod
    @contextmanager
    def writeFileStream( self, jobStoreID ):
        """
        Similar to writeFile, but returns a context manager yielding a tuple of 1) a file handle
        which can be written to and 2) the ID of the resulting file in the job store. The yielded
        file handle does not need to and should not be closed explicitly.
        """
        raise NotImplementedError( )

    @abstractmethod
    @contextmanager
    def updateFileStream( self, jobStoreFileID ):
        """
        Similar to updateFile, but returns a context manager yielding a file handle which can be
        written to. The yielded file handle does not need to and should not be closed explicitly.

        :raises ConcurrentFileModificationException: if the file was modified concurrently during
        an invocation of this method
        """
        raise NotImplementedError( )

    @abstractmethod
    def getEmptyFileStoreID( self, jobStoreID ):
        """
        Returns the ID of a new, empty file.
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

    #
    # The following methods deal with shared files, i.e. files not associated with specific jobs.
    #

    sharedFileNameRegex = re.compile( r'^[a-zA-Z0-9._-]+$' )

    # FIXME: Rename to updateSharedFileStream

    @abstractmethod
    @contextmanager
    def writeSharedFileStream( self, sharedFileName ):
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
    def readSharedFileStream( self, sharedFileName ):
        """
        Returns a context manager yielding a readable file handle to the global file referenced
        by the given ID.
        """
        raise NotImplementedError( )

    @abstractmethod
    def writeStats( self, statsString ):
        """
        Adds the given statistics string to the store of statistics info.
        """
        raise NotImplementedError( )

    @abstractmethod
    def readStats( self, fileHandle ):
        """
        Reads stats strings accumulated by "writeStats" function, writing each
        one to the given fileHandle. Returns the number of stat strings processed. Stats are
        only read once and are removed from the file store after being written to the given 
        file handle.
        """
        raise NotImplementedError( )
    
    @abstractmethod
    def deleteJobStore( self ):
        """
        Removes the jobStore from the disk/store. Careful!
        """
        raise NotImplementedError( )

    ## Helper methods for subclasses

    def _defaultTryCount( self ):
        return int( self.config.attrib[ "try_count" ] )

    @classmethod
    def _validateSharedFileName( cls, sharedFileName ):
        return bool( cls.sharedFileNameRegex.match( sharedFileName ) )
