from StringIO import StringIO
from collections import defaultdict
from contextlib import contextmanager
import logging
import os
import re
from threading import Thread
import uuid

# noinspection PyUnresolvedReferences
from boto.sdb.domain import Domain
# noinspection PyUnresolvedReferences
from boto.s3.bucket import Bucket
# noinspection PyUnresolvedReferences
from boto.s3.connection import S3Connection
# noinspection PyUnresolvedReferences
from boto.sdb.connection import SDBConnection

import boto.s3
from boto.exception import SDBResponseError, S3ResponseError
import itertools
import time
import errno

from jobStores.abstractJobStore import AbstractJobStore, JobTreeState
from src.job import Job

logger = logging.getLogger( __name__ )

# FIXME: Command length is currently limited to 1024 characters

# NB: Number of messages per job is limited to 256-x, 1024 bytes each, with x being the number of
# other attributes in the item

# FIXME: enforce SimpleDB limits early

class AWSJobStore( AbstractJobStore ):
    """
    A job store that uses Amazon's S3 for file storage and SimpleDB for storing job info and
    enforcing strong consistency on the S3 file storage. The schema in SimpleDB is as follows:

    Jobs are stored in the "xyz.jobs" domain where xyz is the name prefix this job store was
    constructed with. Each item in that domain uses the job store job ID (jobStoreID) as the item
    name. The command, memory and cpu fields of a job will be stored as attributes. The messages
    field of a job will be stored as a multivalued attribute.
    """

    @classmethod
    def create( cls, jobStoreString, config=None ):
        region, namePrefix = cls._parseArgs( jobStoreString )
        return cls( region=region, namePrefix=namePrefix, config=config )

    def __init__( self, region, namePrefix, config=None ):
        self.region = region
        self.namePrefix = namePrefix
        create = config is not None
        self.db = self._connectSimpleDB( )
        self.s3 = self._connectS3( )
        assert self.db is not None
        self.jobs = None
        self.fileVersions = None
        self.files = None
        self.stats = None
        try:
            self.jobs = self._getOrCreateDomain( 'jobs', create )
            self.fileVersions = self._getOrCreateDomain( 'fileVersions', create )
            self.files = self._getOrCreateBucket( 'files', create, versioning=True )
            self.stats = self._getOrCreateBucket( 'stats', create )
            super( AWSJobStore, self ).__init__( config=config )
        except:
            self.destroy()
            raise

    def createFirstJob( self, command, memory, cpu ):
        jobStoreID = self._newJobID( )
        job = Job.create( jobStoreID=jobStoreID,
                          command=command, memory=memory, cpu=cpu,
                          tryCount=self._defaultTryCount( ), logJobStoreFileID=None )
        # The root job is its own parent. This avoids having to represent None in the database.
        assert self.jobs.put_attributes( item_name=jobStoreID,
                                         attributes=self._jobToItem( job=job,
                                                                     parentJobStoreID=jobStoreID ) )
        return job

    def exists( self, jobStoreID ):
        return bool( self.jobs.get_attributes( item_name=jobStoreID,
                                               attribute_name='parentJobStoreID',
                                               consistent_read=True ) )

    def load( self, jobStoreID ):
        attributes = self.jobs.get_attributes( item_name=jobStoreID,
                                               consistent_read=True )
        del attributes[ 'parentJobStoreID' ]
        return Job( jobStoreID=jobStoreID, **attributes )

    def store( self, job ):
        assert self.jobs.put_attributes( item_name=job.jobStoreID,
                                         attributes=job.fromDict( ) )

    def _jobToItem( self, job, parentJobStoreID ):
        item = job.toDict( )
        item[ 'parentJobStoreID' ] = parentJobStoreID
        del item[ 'jobStoreID' ]
        return item

    def addChildren( self, job, childCommands ):
        # The elements of childCommands are tuples (command, memory, cpu). The astute reader will
        # notice that we are careful to avoid specifically referencing all but the first element
        # of those tuples as they might change in the future.
        children = [ ]
        items = { }
        for childCommand in childCommands:
            child = Job.create( tryCount=self._defaultTryCount( ),
                                jobStoreID=self._newJobID( ),
                                logJobStoreFileID=None,
                                *childCommand )
            children.append( child )
            job.children.append( ( child.jobStoreID, ) + childCommand[ 1: ] )
            items[ child.jobStoreID ] = self._jobToItem( child, job.jobStoreID )
        # Batch updates are atomic
        self.jobs.batch_put_attributes( items=items )

    def delete( self, job ):
        self.jobs.delete_attributes( item_name=job.jobStoreID )
        query = 'select itemName() where jobStoreID = %s' % job.jobStoreID
        # probably safer to eagerly consume the returned generator before deleting
        items = dict( (item.name, None) for item in self.fileVersions.select( query ) )
        self.fileVersions.batch_delete_attributes( items )

    def loadJobTreeState( self ):
        jobs = { }
        for item in self.jobs.select( 'select * from %s' % self.jobs.name ):
            jobStoreID = item.name
            parentJobStoreID = item.pop( 'parentJobStoreID' )
            job = Job( jobStoreID=jobStoreID, **item )
            jobs[ jobStoreID ] = ( job, parentJobStoreID )
        state = JobTreeState( )
        if jobs:
            state.started = True
            state.childCounts = defaultdict( int )
            for job, parentJobStoreID in jobs.itervalues( ):
                if job.jobStoreID != parentJobStoreID:
                    if not job.children:
                        if job.followOnCommands:
                            state.updatedJobs.add( job )
                        else:
                            state.shellJobs.add( job )
                    state.childCounts[ parentJobStoreID ] += 1
                    state.childJobStoreIdToParentJob[ job.jobStoreID ] = jobs[ parentJobStoreID ]
            state.childCounts.default_factory = None
        return state

    def writeFile( self, jobStoreID, localFilePath ):
        jobStoreFileID = self._newFileID( )
        version = self._upload( jobStoreFileID, localFilePath )
        self._postWrite( jobStoreFileID, jobStoreID, version )
        return jobStoreFileID

    @contextmanager
    def writeFileStream( self, jobStoreID ):
        jobStoreFileID = self._newFileID( )
        with self._uploadStream( jobStoreFileID ) as ( writable, key ):
            yield writable, jobStoreFileID
        version = key.version_id
        assert version is not None
        self._postWrite( jobStoreFileID, jobStoreID, version )

    @contextmanager
    def writeSharedFileStream( self, sharedFileName ):
        jobStoreFileID = self._newFileID( sharedFileName )
        with self._uploadStream( jobStoreFileID ) as ( writable, key ):
            yield writable
        version = key.version_id
        self._postWrite( jobStoreFileID, str( self.sharedFileJobID ), version )

    def updateFile( self, jobStoreFileID, localFilePath ):
        oldVersion = self.__getFileVersion( jobStoreFileID )
        newVersion = self._upload( jobStoreFileID, localFilePath )
        self._postUpdate( jobStoreFileID, oldVersion, newVersion )

    @contextmanager
    def updateFileStream( self, jobStoreFileID ):
        oldVersion = self.__getFileVersion( jobStoreFileID )
        with self._uploadStream( jobStoreFileID ) as ( writable, key ):
            yield writable
        newVersion = key.version_id
        self._postUpdate( jobStoreFileID, oldVersion, newVersion )

    def readFile( self, jobStoreFileID, localFilePath ):
        version = self.__getFileVersion( jobStoreFileID )
        self._download( jobStoreFileID, localFilePath, version )

    @contextmanager
    def readFileStream( self, jobStoreFileID ):
        version = self.__getFileVersion( jobStoreFileID )
        with self._downloadStream( jobStoreFileID, version ) as readable:
            yield readable

    @contextmanager
    def readSharedFileStream( self, sharedFileName ):
        jobStoreFileID = self._newFileID( sharedFileName )
        version = self.__getFileVersion( jobStoreFileID )
        with self._downloadStream( jobStoreFileID, version ) as readable:
            yield readable

    def deleteFile( self, jobStoreFileID ):
        version = self.__getFileVersion( jobStoreFileID )
        self.fileVersions.delete_attributes( jobStoreFileID,
                                             expected_values=[ 'version', version ] )
        self.files.delete_key( jobStoreFileID, version_id=version )
        assert self.files.get_key( jobStoreFileID ) is None

    def getEmptyFileStoreID( self, jobStoreID ):
        return self._newFileID( )

    def writeStats( self, statsString ):
        raise NotImplementedError( )

    def readStats( self, fileHandle ):
        raise NotImplementedError( )

    # Dots in bucket names should be avoided because bucket names are used in HTTPS bucket
    # URLs where the may interfere with the certificate common name. We use a double
    # underscore as a separator instead.
    bucketNameRe = re.compile( r'[a-z0-9][a-z0-9-]+[a-z0-9]' )

    bucketNameSeparator = '--'

    @classmethod
    def _parseArgs( cls, jobStoreString ):
        region, namePrefix = jobStoreString.split( ':' )
        # See http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html, reserve 10 characters for separator and suffixes
        if not cls.bucketNameRe.match( namePrefix ):
            raise ValueError( "Invalid name prefix '%s'. Name prefixes must contain only digits, "
                              "hyphens or lower-case letters and must not start or end in a "
                              "hyphen." % namePrefix )
        # reserve 13 for separator and suffix
        if len( namePrefix ) > 50:
            raise ValueError( "Invalid name prefix '%s'. Name prefixes may not be longer than 50 "
                              "characters." % namePrefix )
        if '--' in namePrefix:
            raise ValueError( "Invalid name prefix '%s'. Name prefixes may not contain "
                              "%s." % ( namePrefix, cls.bucketNameSeparator ) )

        return region, namePrefix

    def _connectSimpleDB( self ):
        """
        rtype: SDBConnection
        """
        db = boto.sdb.connect_to_region( self.region )
        if db is None:
            raise ValueError( "Could not connect to SimpleDB. Make sure '%s' is a valid SimpleDB "
                              "region." % self.region )
        return db

    def _connectS3( self ):
        """
        :rtype: S3Connection
        """
        s3 = boto.s3.connect_to_region( self.region )
        if s3 is None:
            raise ValueError( "Could not connect to S3. Make sure '%s' is a valid S3 region." %
                              self.region )
        return s3

    def _getOrCreateBucket( self, bucket_name, create=False, versioning=False ):
        """
        :rtype Bucket
        """
        bucket_name = self.namePrefix + self.bucketNameSeparator + bucket_name
        assert self.bucketNameRe.match( bucket_name )
        assert 3 <= len( bucket_name ) <= 63
        try:
            bucket = self.s3.get_bucket( bucket_name, validate=True )
            assert versioning is self.__get_bucket_versioning( bucket )
            return bucket
        except S3ResponseError as e:
            if e.error_code == 'NoSuchBucket' and create:
                bucket = self.s3.create_bucket( bucket_name, location=self.region )
                if versioning:
                    bucket.configure_versioning( versioning )
                return bucket
            else:
                raise

    def _getOrCreateDomain( self, domain_name, create=False ):
        """
        :rtype : Domain
        """
        domain_name = self.namePrefix + '.' + domain_name
        # CreateDomain is idempotent
        self.db.create_domain( domain_name )
        # According to post on AWS forum "domain creation can take a while"
        while True:
            try:
                return self.db.get_domain( domain_name )
            except SDBResponseError as e:
                if e.error_code == 'NoSuchDomain':
                    time.sleep( 5 )

    def _newJobID( self ):
        return str( uuid.uuid4( ) )

    # A dummy job ID under which all shared files are stored.
    sharedFileJobID = uuid.UUID( '891f7db6-e4d9-4221-a58e-ab6cc4395f94' )

    def _newFileID( self, sharedFileName=None ):
        if sharedFileName is None:
            return str( uuid.uuid4( ) )
        else:
            return str( uuid.uuid5( self.sharedFileJobID, sharedFileName ) )

    def __getFileVersion( self, jobStoreFileID ):
        return self.fileVersions.get_attributes( item_name=jobStoreFileID,
                                                 attribute_name='version' )[ 'version' ]

    _s3_part_size = 50 * 1024 * 1024

    def _upload( self, jobStoreFileID, localFilePath ):
        file_size, file_time = self._fileSizeAndTime( localFilePath )
        if file_size <= self._s3_part_size:
            key = self.files.new_key( )
            key.name = jobStoreFileID
            key.set_contents_from_file( localFilePath )
            version = key.version_id
        else:
            with open( localFilePath, 'rb' ) as f:
                upload = self.files.initiate_multipart_upload( key_name=jobStoreFileID )
                try:
                    start = 0
                    part_num = itertools.count( )
                    while start < file_size:
                        end = min( start + self._s3_part_size, file_size )
                        assert f.tell( ) == start
                        upload.upload_part_from_file( f, next( part_num ), size=end - start )
                        start = end
                    assert f.tell( ) == file_size == start
                except:
                    upload.cancel_upload( )
                    raise
                else:
                    version = upload.complete_upload( ).version_id
        key = self.files.get_key( jobStoreFileID )
        assert key.size == file_size
        assert self._fileSizeAndTime( localFilePath ) == (file_size, file_time)
        return version

    @contextmanager
    def _uploadStream( self, jobStoreFileID ):
        key = self.files.new_key( key_name=jobStoreFileID )
        assert key.version_id is None
        readable_fh, writable_fh = os.pipe( )
        try:
            def reader( ):
                try:
                    readable = os.fdopen( readable_fh, 'r' )
                    try:
                        upload = self.files.initiate_multipart_upload( key_name=jobStoreFileID )
                        try:
                            for part_num in itertools.count( ):
                                buf = readable.read( self._s3_part_size )
                                # There must be at least one part, even if the file is empty.
                                if len( buf ) == 0 and part_num > 0: break
                                upload.upload_part_from_file( fp=StringIO( buf ),
                                                              # S3 part numbers are 1-based
                                                              part_num=part_num )
                                if len( buf ) == 0: break
                        except BaseException:
                            upload.cancel_upload( )
                            raise
                        else:
                            key.version_id = upload.complete_upload( ).version_id
                    finally:
                        readable.close( )
                except BaseException:
                    logger.exception( 'Exception in reader thread' )

            writable = os.fdopen( writable_fh, 'w' )
            try:
                thread = Thread( target=reader )
                thread.start( )
                # Yield the key now with version_id unset. When reader() returns
                # key.version_id will be set.
                yield writable, key
            finally:
                writable.close( )
            thread.join( )
        finally:
            for fh in readable_fh, writable_fh:
                self.__try_close( fh )
        assert key.version_id is not None

    @contextmanager
    def _downloadStream( self, jobStoreFileID, version ):
        key = self.files.get_key( jobStoreFileID, validate=False )
        readable, writable = os.pipe( )
        try:
            readable = os.fdopen( readable, 'r' )
            try:
                writable = os.fdopen( writable, 'w' )
                try:
                    def writer( ):
                        key.get_contents_to_file( writable, version_id=version )
                        # This close() will send EOF to the reading end and ultimately cause the
                        # yield to return
                        writable.close( )

                    thread = Thread( target=writer )
                    thread.start( )
                    yield readable
                    thread.join( )
                finally:
                    # Redundant but not harmful since the close() method is idempotent
                    writable.close( )
            finally:
                readable.close( )
        finally:
            # In case an exception occurs before both fdopen() calls succeed
            for f in ( readable, writable ):
                if isinstance( f, int ):
                    os.close( f )

    class ConcurrentFileModificationException( Exception ):
        def __init__( self, jobStoreFileID ):
            super( AWSJobStore.ConcurrentFileModificationException, self ).__init__(
                'Concurrent update to file %s detected.' % jobStoreFileID )

    def _postUpdate( self, jobStoreFileID, newVersion, oldVersion ):
        if self._registerFileVersion( jobStoreFileID,
                                      newVersion=newVersion,
                                      oldVersion=oldVersion ):
            self.files.delete_key( jobStoreFileID, version_id=oldVersion )
        else:
            raise self.ConcurrentFileModificationException( jobStoreFileID )

    def _postWrite( self, jobStoreFileID, jobStoreID, version ):
        if not self._registerFileVersion( jobStoreFileID,
                                          newVersion=version,
                                          jobStoreID=jobStoreID ):
            raise self.ConcurrentFileModificationException( jobStoreFileID )

    def _registerFileVersion( self, jobStoreFileID, newVersion, jobStoreID=None, oldVersion=None ):
        """
        Register a new version for a file in the store

        :param jobStoreFileID: the file's ID
        :param newVersion: the file's new version
        :param jobStoreID: the ID of the job owning the file
        :param oldVersion: the previous version of the file or None if this is the first version

        :return: True if the operation was successful, False if there already is a version (
        oldVersion is None) or if the existing verison is not oldVersion (oldVersion is not
        None). Other errors should raise an exception.
        """
        if oldVersion is None: assert jobStoreID is not None
        assert newVersion is not None
        attributes = dict( version=newVersion )
        if jobStoreID is not None:
            attributes[ 'jobStoreID' ] = jobStoreID
        # False stands for absence
        expected = [ 'version', False if oldVersion is None else oldVersion ]
        try:
            assert self.fileVersions.put_attributes( item_name=jobStoreFileID,
                                                     attributes=attributes,
                                                     expected_value=expected )
        except SDBResponseError as e:
            if e.error_code == 'ConditionalCheckFailed':
                return False
            else:
                raise
        else:
            return True

    def _fileSizeAndTime( self, localFilePath ):
        file_stat = os.stat( localFilePath )
        file_size, file_time = file_stat.st_size, file_stat.st_mtime
        return file_size, file_time

    def _download( self, jobStoreFileID, localFilePath, version ):
        key = self.files.get_key( jobStoreFileID, validate=False )
        key.get_contents_to_filename( localFilePath, version_id=version )

    versionings = dict( Enabled=True, Disabled=False, Suspended=None )

    def __get_bucket_versioning( self, bucket ):
        """
        A valueable lesson in how to feck up a simple tri-state boolean.

        For newly created buckets get_versioning_status returns None. We map that to False.

        TODO: This may actually be a result of eventual consistency

        Otherwise, the 'Versioning' entry in the status dictionary can be 'Enabled', 'Suspended'
        or 'Disabled' which we map to True, None and False respectively. Calling
        configure_versioning with False on a bucket will cause get_versioning_status to then
        return 'Suspended' for some reason.
        """
        status = bucket.get_versioning_status( )
        return bool( status ) and self.versionings[ status[ 'Versioning' ] ]

    def __try_close( self, fh ):
        try:
            os.close( fh )
        except OSError as e:
            if e.errno == errno.EBADF:
                pass
            else:
                raise

    # TODO: Move up

    def destroy( self ):
        for bucket in ( self.files, self.stats ):
            for upload in bucket.list_multipart_uploads( ):
                upload.cancel_upload( )
            if self.__get_bucket_versioning( bucket ) in ( True, None ):
                for key in list( bucket.list_versions( ) ):
                    bucket.delete_key( key.name, version_id=key.version_id )
            else:
                for key in list( bucket.list( ) ):
                    key.delete( )
            bucket.delete( )
        for domain in ( self.fileVersions, self.jobs ):
            domain.delete( )
