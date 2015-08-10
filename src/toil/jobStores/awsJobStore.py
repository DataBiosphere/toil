from StringIO import StringIO
from ast import literal_eval
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
from boto.sdb.item import Item
import boto.s3
from boto.exception import SDBResponseError, S3ResponseError
import itertools
import time

from toil.jobStores.abstractJobStore import AbstractJobStore, NoSuchJobException, \
    ConcurrentFileModificationException, NoSuchFileException
from toil.jobWrapper import JobWrapper

log = logging.getLogger( __name__ )


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

    def fileExists( self, jobStoreFileID ):
        return bool( self.versions.get_item( item_name=jobStoreFileID, consistent_read=True ) )

    def jobs( self ):
        for attempt in retry_sdb( ):
            with attempt:
                result = list( self.jobDomain.select(
                    query="select * from `{domain}` ".format( domain=self.jobDomain.name ),
                    consistent_read=True ) )
        for jobItem in result:
            yield AWSJob.fromItem( jobItem )

    def create( self, command, memory, cpu, disk, updateID=None,
                predecessorNumber=0 ):
        jobStoreID = self._newJobID( )
        log.debug( "Creating job %s for '%s'",
                   jobStoreID, '<no command>' if command is None else command )
        job = AWSJob( jobStoreID=jobStoreID,
                      command=command, memory=memory, cpu=cpu, disk=disk,
                      remainingRetryCount=self._defaultTryCount( ), logJobStoreFileID=None,
                      updateID=updateID, predecessorNumber=predecessorNumber )
        for attempt in retry_sdb( ):
            with attempt:
                assert self.jobDomain.put_attributes( item_name=jobStoreID,
                                                      attributes=job.toItem( ) )
        return job

    def __init__( self, region, namePrefix, config=None, create=False ):
        """
        TODO: Document region and namePrefix
        
        :param create: If True create the jobStore. 
        :type create: Boolean
        :exception RuntimeError: if create=True and the jobStore already exists or
        create=False and the jobStore does not already exist. 
        """
        log.debug( "Instantiating %s for region %s and name prefix '%s'",
                   self.__class__, region, namePrefix )
        self.region = region
        self.namePrefix = namePrefix
        self.jobDomain = None
        self.versions = None
        self.files = None
        self.stats = None
        self.db = self._connectSimpleDB( )
        self.s3 = self._connectS3( )

        def creationCheck( exists ):
            self._checkJobStoreCreation( create, exists, region + " " + namePrefix )

        self.jobDomain = self._getOrCreateDomain( 'jobs', creationCheck )
        self.versions = self._getOrCreateDomain( 'versions', creationCheck )
        self.files = self._getOrCreateBucket( 'files', create, versioning=True )
        self.stats = self._getOrCreateBucket( 'stats', create, versioning=True )
        super( AWSJobStore, self ).__init__( config=config )

    def exists( self, jobStoreID ):
        for attempt in retry_sdb( ):
            with attempt:
                return bool( self.jobDomain.get_attributes( item_name=jobStoreID,
                                                            attribute_name=[ ],
                                                            consistent_read=True ) )

    def getPublicUrl( self, jobStoreFileID ):
        """
        For Amazon SimpleDB requests, use HTTP GET requests that are URLs with query strings.
        http://awsdocs.s3.amazonaws.com/SDB/latest/sdb-dg.pdf
        Create url, check if valid, return.
        """
        key = self.files.get_key( key_name=jobStoreFileID )
        return key.generate_url( expires_in=3600 )  # one hour

    def getSharedPublicUrl( self, FileName ):
        jobStoreFileID = self._newFileID( FileName )
        return self.getPublicUrl( jobStoreFileID )

    def load( self, jobStoreID ):
        # TODO: check if mentioning individual attributes is faster than using *
        for attempt in retry_sdb( ):
            with attempt:
                result = list( self.jobDomain.select(
                    query="select * from `{domain}` "
                          "where itemName() = '{jobStoreID}'".format( domain=self.jobDomain.name,
                                                                      jobStoreID=jobStoreID ),
                    consistent_read=True ) )
        if len( result ) != 1:
            raise NoSuchJobException( jobStoreID )
        job = AWSJob.fromItem( result[ 0 ] )
        if job is None:
            raise NoSuchJobException( jobStoreID )
        log.debug( "Loaded job %s", jobStoreID )
        return job

    def update( self, job ):
        log.debug( "Updating job %s", job.jobStoreID )
        for attempt in retry_sdb( ):
            with attempt:
                assert self.jobDomain.put_attributes( item_name=job.jobStoreID,
                                                      attributes=job.toItem( ) )

    def delete( self, jobStoreID ):
        # remove job and replace with jobStoreId.
        log.debug( "Deleting job %s", jobStoreID )
        for attempt in retry_sdb( ):
            with attempt:
                self.jobDomain.delete_attributes( item_name=jobStoreID )
        for attempt in retry_sdb( ):
            with attempt:
                items = list( self.versions.select(
                    query="select * from `%s` "
                          "where jobStoreID='%s'" % (self.versions.name, jobStoreID),
                    consistent_read=True ) )
        if items:
            log.debug( "Deleting %d file(s) associated with job %s", len( items ), jobStoreID )
            for attempt in retry_sdb( ):
                with attempt:
                    self.versions.batch_delete_attributes( { item.name: None for item in items } )
            for item in items:
                if 'version' in item:
                    self.files.delete_key( key_name=item.name,
                                           version_id=item[ 'version' ] )
                else:
                    self.files.delete_key( key_name=item.name )

    def writeFile( self, jobStoreID, localFilePath ):
        jobStoreFileID = self._newFileID( )
        firstVersion = self._upload( jobStoreFileID, localFilePath )
        self._registerFile( jobStoreFileID, jobStoreID=jobStoreID, newVersion=firstVersion )
        log.debug( "Wrote initial version %s of file %s for job %s from path '%s'",
                   firstVersion, jobStoreFileID, jobStoreID, localFilePath )
        return jobStoreFileID

    @contextmanager
    def writeFileStream( self, jobStoreID ):
        jobStoreFileID = self._newFileID( )
        with self._uploadStream( jobStoreFileID, self.files ) as (writable, key):
            yield writable, jobStoreFileID
        firstVersion = key.version_id
        assert firstVersion is not None
        self._registerFile( jobStoreFileID, jobStoreID=jobStoreID, newVersion=firstVersion )
        log.debug( "Wrote initial version %s of file %s for job %s",
                   firstVersion, jobStoreFileID, jobStoreID )

    @contextmanager
    def writeSharedFileStream( self, sharedFileName ):
        assert self._validateSharedFileName( sharedFileName )
        jobStoreFileID = self._newFileID( sharedFileName )
        oldVersion = self._getFileVersion( jobStoreFileID )
        with self._uploadStream( jobStoreFileID, self.files ) as (writable, key):
            yield writable
        newVersion = key.version_id
        jobStoreId = str( self.sharedFileJobID ) if oldVersion is None else None
        self._registerFile( jobStoreFileID,
                            jobStoreID=jobStoreId, oldVersion=oldVersion, newVersion=newVersion )
        if oldVersion is None:
            log.debug( "Wrote initial version %s of shared file %s (%s)",
                       newVersion, sharedFileName, jobStoreFileID )
        else:
            log.debug( "Wrote version %s of file %s (%s), replacing version %s",
                       newVersion, sharedFileName, jobStoreFileID, oldVersion )

    def updateFile( self, jobStoreFileID, localFilePath ):
        oldVersion = self._getFileVersion( jobStoreFileID )
        newVersion = self._upload( jobStoreFileID, localFilePath )
        self._registerFile( jobStoreFileID, oldVersion=oldVersion, newVersion=newVersion )
        log.debug( "Wrote version %s of file %s from path '%s', replacing version %s",
                   newVersion, jobStoreFileID, localFilePath, oldVersion )

    @contextmanager
    def updateFileStream( self, jobStoreFileID ):
        oldVersion = self._getFileVersion( jobStoreFileID )
        with self._uploadStream( jobStoreFileID, self.files ) as (writable, key):
            yield writable
        newVersion = key.version_id
        self._registerFile( jobStoreFileID, oldVersion=oldVersion, newVersion=newVersion )
        log.debug( "Wrote version %s of file %s, replacing version %s",
                   newVersion, jobStoreFileID, oldVersion )

    def readFile( self, jobStoreFileID, localFilePath ):
        version = self._getFileVersion( jobStoreFileID )
        if version is None: raise NoSuchFileException( jobStoreFileID )
        log.debug( "Reading version %s of file %s to path '%s'",
                   version, jobStoreFileID, localFilePath )
        self._download( jobStoreFileID, localFilePath, version )

    @contextmanager
    def readFileStream( self, jobStoreFileID ):
        version = self._getFileVersion( jobStoreFileID )
        if version is None: raise NoSuchFileException( jobStoreFileID )
        log.debug( "Reading version %s of file %s", version, jobStoreFileID )
        with self._downloadStream( jobStoreFileID, version, self.files ) as readable:
            yield readable

    @contextmanager
    def readSharedFileStream( self, sharedFileName ):
        assert self._validateSharedFileName( sharedFileName )
        jobStoreFileID = self._newFileID( sharedFileName )
        version = self._getFileVersion( jobStoreFileID )
        if version is None: raise NoSuchFileException( jobStoreFileID )
        log.debug( "Read version %s from shared file %s (%s)",
                   version, sharedFileName, jobStoreFileID )
        with self._downloadStream( jobStoreFileID, version, self.files ) as readable:
            yield readable

    def deleteFile( self, jobStoreFileID ):
        version, bucket = self._getFileVersionAndBucket( jobStoreFileID )
        if bucket:
            for attempt in retry_sdb( ):
                with attempt:
                    if version:
                        self.versions.delete_attributes( jobStoreFileID,
                                                         expected_values=[ 'version', version ] )
                    else:
                        self.versions.delete_attributes( jobStoreFileID )

            bucket.delete_key( key_name=jobStoreFileID, version_id=version )
            if version:
                log.debug( "Deleted version %s of file %s", version, jobStoreFileID )
            else:
                log.debug( "Deleted unversioned file %s", jobStoreFileID )
        else:
            log.debug( "File %s does not exist", jobStoreFileID )

    def getEmptyFileStoreID( self, jobStoreID ):
        jobStoreFileID = self._newFileID( )
        self._registerFile( jobStoreFileID, jobStoreID=jobStoreID )
        log.debug( "Registered empty file %s for job %s", jobStoreFileID, jobStoreID )
        return jobStoreFileID

    def writeStatsAndLogging( self, statsAndLoggingString ):
        jobStoreFileId = self._newFileID( )
        with self._uploadStream( jobStoreFileId, self.stats, multipart=False ) as (writeable, key):
            writeable.write( statsAndLoggingString )
        firstVersion = key.version_id
        self._registerFile( jobStoreFileId, bucketName='stats', newVersion=firstVersion )

    def readStatsAndLogging( self, statsCallBackFn ):
        itemsProcessed = 0
        for attempt in retry_sdb( ):
            with attempt:
                items = list( self.versions.select(
                    query="select * from `%s` "
                          "where bucketName='stats'" % (self.versions.name,),
                    consistent_read=True ) )
        for item in items:
            with self._downloadStream( item.name, item[ 'version' ], self.stats ) as readable:
                statsCallBackFn( readable )
            self.deleteFile( item.name )
            itemsProcessed += 1
        return itemsProcessed

    # Dots in bucket names should be avoided because bucket names are used in HTTPS bucket
    # URLs where the may interfere with the certificate common name. We use a double
    # underscore as a separator instead.
    bucketNameRe = re.compile( r'^[a-z0-9][a-z0-9-]+[a-z0-9]$' )

    nameSeparator = '--'

    @classmethod
    def _parseArgs( cls, jobStoreString ):
        region, namePrefix = jobStoreString.split( ':' )
        # See http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html,
        # reserve 10 characters for separator and suffixes
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
                              "%s." % (namePrefix, cls.nameSeparator) )

        return region, namePrefix

    def _connectSimpleDB( self ):
        """
        rtype: SDBConnection
        """
        db = boto.sdb.connect_to_region( self.region )
        if db is None:
            raise ValueError( "Could not connect to SimpleDB. Make sure '%s' is a valid SimpleDB "
                              "region." % self.region )
        assert db is not None
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
        bucket_name = self.namePrefix + self.nameSeparator + bucket_name
        assert self.bucketNameRe.match( bucket_name )
        assert 3 <= len( bucket_name ) <= 63
        try:
            bucket = self.s3.get_bucket( bucket_name, validate=True )
            assert versioning is self.__getBucketVersioning( bucket )
            return bucket
        except S3ResponseError as e:
            if e.error_code == 'NoSuchBucket' and create:
                bucket = self.s3.create_bucket( bucket_name, location=self.region )
                if versioning:
                    bucket.configure_versioning( versioning )
                return bucket
            else:
                raise

    def _getOrCreateDomain( self, domain_name, creation_check ):
        """
        Return the boto Domain object representing the SDB domain with the given name. If the
        domain does not exist it will be created unless the given callback prevents that by
        raising an exception.

        :param domain_name: the unqualified name of the domain to be created

        :param creation_check: a callback that is invoked with True if the domain already exists,
        False if it is missing. If the callback wants to prevent the creation of a missing
        domain, it should raise an exception.

        :rtype : Domain
        """
        domain_name = self.namePrefix + self.nameSeparator + domain_name
        for i in itertools.count( ):
            try:
                domain = self.db.get_domain( domain_name )
                if i == 0:
                    creation_check( True )
                return domain
            except SDBResponseError as e:
                if e.error_code == 'NoSuchDomain':
                    if i == 0:
                        creation_check( False )
                        self.db.create_domain( domain_name )
                    else:
                        log.warn( "Creation of '%s' still pending, retrying in 5s" % domain_name )
                        time.sleep( 5 )

    def _newJobID( self ):
        return str( uuid.uuid4( ) )

    # A dummy job ID under which all shared files are stored.
    sharedFileJobID = uuid.UUID( '891f7db6-e4d9-4221-a58e-ab6cc4395f94' )

    def _newFileID( self, sharedFileName=None ):
        if sharedFileName is None:
            return str( uuid.uuid4( ) )
        else:
            return str( uuid.uuid5( self.sharedFileJobID, str( sharedFileName ) ) )

    def _getFileVersionAndBucket( self, jobStoreFileID ):
        """
        :rtype: tuple(str version, AWS bucket)
        """
        for attempt in retry_sdb( ):
            with attempt:
                item = self.versions.get_attributes( item_name=jobStoreFileID,
                                                     attribute_name=[ 'version', 'bucketName' ],
                                                     consistent_read=True )
        bucketName = item.get( 'bucketName', None )
        if bucketName is None:
            return None, None
        else:
            return item.get( 'version', None ), getattr( self, bucketName )

    def _getFileVersion( self, jobStoreFileID, expectedBucket=None ):
        version, bucket = self._getFileVersionAndBucket( jobStoreFileID )
        if bucket is None:
            assert version is None
        else:
            if expectedBucket is None:
                expectedBucket = self.files
            assert bucket is expectedBucket
        return version

    _s3_part_size = 50 * 1024 * 1024

    def _upload( self, jobStoreFileID, localFilePath ):
        file_size, file_time = self._fileSizeAndTime( localFilePath )
        if file_size <= self._s3_part_size:
            key = self.files.new_key( key_name=jobStoreFileID )
            key.name = jobStoreFileID
            key.set_contents_from_filename( localFilePath )
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
                        upload.upload_part_from_file( fp=f,
                                                      part_num=next( part_num ) + 1,
                                                      size=end - start )
                        start = end
                    assert f.tell( ) == file_size == start
                except:
                    upload.cancel_upload( )
                    raise
                else:
                    version = upload.complete_upload( ).version_id
        key = self.files.get_key( jobStoreFileID )
        assert key.size == file_size
        assert self._fileSizeAndTime( localFilePath ) == (
        file_size, file_time)  # why do this? No one can touch the file while it is uploaded?
        return version

    @contextmanager
    def _uploadStream( self, jobStoreFileID, bucket, multipart=True ):
        key = bucket.new_key( key_name=jobStoreFileID )
        assert key.version_id is None
        readable_fh, writable_fh = os.pipe( )
        with os.fdopen( readable_fh, 'r' ) as readable:
            with os.fdopen( writable_fh, 'w' ) as writable:
                def reader( ):
                    try:
                        upload = bucket.initiate_multipart_upload( key_name=jobStoreFileID )
                        try:
                            for part_num in itertools.count( ):
                                # FIXME: Consider using a key.set_contents_from_stream and rip ...
                                # FIXME: ... the query_args logic from upload_part_from_file in ...
                                # FIXME: ... in MultipartUpload. Possible downside is that ...
                                # FIXME: ... implicit retries won't work.
                                buf = readable.read( self._s3_part_size )
                                # There must be at least one part, even if the file is empty.
                                if len( buf ) == 0 and part_num > 0: break
                                upload.upload_part_from_file( fp=StringIO( buf ),
                                                              # S3 part numbers are 1-based
                                                              part_num=part_num + 1 )
                                if len( buf ) == 0: break
                        except:
                            upload.cancel_upload( )
                            raise
                        else:
                            key.version_id = upload.complete_upload( ).version_id
                    except:
                        log.exception( 'Exception in reader thread' )

                def simpleReader( ):
                    log.debug( "Using single part upload" )
                    try:
                        buf = StringIO( readable.read( ) )
                        assert key.set_contents_from_file( fp=buf ) == buf.len
                    except:
                        log.exception( "Exception in simple reader thread" )

                thread = Thread( target=reader if multipart else simpleReader )
                thread.start( )
                # Yield the key now with version_id unset. When reader() returns
                # key.version_id will be set.
                yield writable, key
            # The writable is now closed. This will send EOF to the readable and cause that
            # thread to finish.
            thread.join( )
            assert key.version_id is not None

    def _download( self, jobStoreFileID, localFilePath, version ):
        key = self.files.get_key( jobStoreFileID, validate=False )
        key.get_contents_to_filename( localFilePath, version_id=version )

    @contextmanager
    def _downloadStream( self, jobStoreFileID, version, bucket ):
        key = bucket.get_key( jobStoreFileID, validate=False )
        readable_fh, writable_fh = os.pipe( )
        with os.fdopen( readable_fh, 'r' ) as readable:
            with os.fdopen( writable_fh, 'w' ) as writable:
                def writer( ):
                    key.get_contents_to_file( writable, version_id=version )
                    # This close() will send EOF to the reading end and ultimately cause the
                    # yield to return. It also makes the implict .close() done by the enclosing
                    # "with" context redundant but that should be ok since .close() on file
                    # objects are idempotent.
                    writable.close( )

                thread = Thread( target=writer )
                thread.start( )
                yield readable
                thread.join( )

    def _registerFile( self, jobStoreFileID,
                       bucketName='files', jobStoreID=None, newVersion=None, oldVersion=None ):
        """
        Register a a file in the store

        :param jobStoreFileID: the file's ID, mandatory

        :param bucketName: the name of the S3 bucket the file was placed in

        :param jobStoreID: the ID of the job owning the file, only allowed for first version of
                           file or when file is registered without content

        :param newVersion: the file's new version or None if the file is to be registered without
                           content, in which case jobStoreId must be passed

        :param oldVersion: the expected previous version of the file or None if newVersion is the
                           first version or file is registered without content
        """
        # Must pass either jobStoreID or newVersion, or both
        assert jobStoreID is not None or newVersion is not None
        # Must pass newVersion if passing oldVersion
        assert oldVersion is None or newVersion is not None
        attributes = dict( bucketName=bucketName )
        if newVersion is not None:
            attributes[ 'version' ] = newVersion
        if jobStoreID is not None:
            attributes[ 'jobStoreID' ] = jobStoreID
        # False stands for absence
        expected = [ 'version', False if oldVersion is None else oldVersion ]
        try:
            for attempt in retry_sdb( ):
                with attempt:
                    assert self.versions.put_attributes( item_name=jobStoreFileID,
                                                         attributes=attributes,
                                                         expected_value=expected )
            if oldVersion is not None:
                bucket = getattr( self, bucketName )
                bucket.delete_key( jobStoreFileID, version_id=oldVersion )
        except SDBResponseError as e:
            if e.error_code == 'ConditionalCheckFailed':
                raise ConcurrentFileModificationException( jobStoreFileID )
            else:
                raise

    def _fileSizeAndTime( self, localFilePath ):
        file_stat = os.stat( localFilePath )
        file_size, file_time = file_stat.st_size, file_stat.st_mtime
        return file_size, file_time

    versionings = dict( Enabled=True, Disabled=False, Suspended=None )

    def __getBucketVersioning( self, bucket ):
        """
        A valueable lesson in how to feck up a simple tri-state boolean.

        For newly created buckets get_versioning_status returns None. We map that to False.

        TBD: This may actually be a result of eventual consistency

        Otherwise, the 'Versioning' entry in the dictionary returned by get_versioning_status can
        be 'Enabled', 'Suspended' or 'Disabled' which we map to True, None and False
        respectively. Calling configure_versioning with False on a bucket will cause
        get_versioning_status to then return 'Suspended' for some reason.
        """
        status = bucket.get_versioning_status( )
        return bool( status ) and self.versionings[ status[ 'Versioning' ] ]

    def deleteJobStore( self ):
        for bucket in (self.files, self.stats):
            if bucket is not None:
                for upload in bucket.list_multipart_uploads( ):
                    upload.cancel_upload( )
                if self.__getBucketVersioning( bucket ) in (True, None):
                    for key in list( bucket.list_versions( ) ):
                        bucket.delete_key( key.name, version_id=key.version_id )
                else:
                    for key in list( bucket.list( ) ):
                        key.delete( )
                bucket.delete( )
        for domain in (self.versions, self.jobDomain):
            if domain is not None:
                domain.delete( )


# Boto converts all attribute values to strings by default, so an attribute value of None would
# becomes 'None' in SimpleDB. To truly represent attribute values of None, we'd have to always
# call delete_attributes in addition to put_attributes but there is no way to do that atomically.
# Instead we map None to the empty string and vice versa. The same applies to empty iterables.
# The empty iterable is a no-op for put_attributes, so we map that to '' instead. This means that
# we can't serialize [''] or '' because the former would be deserialized as [] and the latter as
# None.

def toNoneable( v ):
    return v if v else None


def fromNoneable( v ):
    assert v != ""
    return '' if v is None else v


sort_prefix_length = 3


def toSet( vs ):
    """
    :param vs: list[str] | str
    :return: set(str) | set()

    Lists returned by simpleDB is not guaranteed to be in their original order, but because we are converting them
    to sets, the loss of order is not a problem.

    >>> toSet(["x", "y", "z"])
    set(['y', 'x', 'z'])

    Instead of a set, a single String can also be returned by SimpleDB.

    >>> toSet("x")
    set(['x'])

    An empty set is serialized as ""

    >>> toSet("")
    set([])

    """
    return set( vs ) if vs else set( )


def fromSet( vs ):
    """
    :type vs: set(str)
    :rtype str|list[str]

    Empty set becomes empty string

    >>> fromSet(set())
    ''

    Singleton set becomes its sole element

    >>> fromSet({'x'})
    'x'

    Set elements are unordered, so sort_prefixes used in fromList are not needed here.

    >>> fromSet({'x','y'})
    ['y', 'x']

    Only sets with non-empty strings are allowed

    >>> fromSet(set(['']))
    Traceback (most recent call last):
    ...
    AssertionError
    >>> fromSet({'x',''})
    Traceback (most recent call last):
    ...
    AssertionError
    >>> fromSet({'x',1})
    Traceback (most recent call last):
    ...
    AssertionError
    """
    if len( vs ) == 0:
        return ""
    elif len( vs ) == 1:
        v = vs.pop( )
        assert isinstance( v, basestring ) and v
        return v
    else:
        assert len( vs ) <= 256
        assert all( isinstance( v, basestring ) and v for v in vs )
        return list( vs )


def toList( vs ):
    """
    :param vs: list[str] | str
    :return: list[str] | []

    Lists are not guaranteed to be in their original order, so they are sorted based on a prefixed string.

    >>> toList(["000x", "001y", "002z"])
    ['x', 'y', 'z']

    Instead of a List of length 1, a single String will be returned by SimpleDB.
    A single element is can only have 1 order, no need to sort.

    >>> toList("x")
    ['x']

    An empty list is serialized as ""

    >>> toList("")
    []

    """
    if isinstance( vs, basestring ):
        return [ vs ] if vs else [ ]
    else:
        return [ v[ sort_prefix_length: ] for v in sorted( vs ) ]


def fromList( vs ):
    """
    :type vs: list[str]
    :rtype str|list[str]

    Empty lists becomes empty string

    >>> fromList([])
    ''

    Singleton list becomes its sole element

    >>> fromList(['x'])
    'x'

    Lists elements are prefixed with their position because lists don't retain their order in SDB

    >>> fromList(['x','y'])
    ['000x', '001y']

    Only lists with non-empty strings are allowed

    >>> fromList([''])
    Traceback (most recent call last):
    ...
    AssertionError
    >>> fromList(['x',''])
    Traceback (most recent call last):
    ...
    AssertionError
    >>> fromList(['x',1])
    Traceback (most recent call last):
    ...
    AssertionError
    """
    if len( vs ) == 0:
        return ''
    elif len( vs ) == 1:
        v = vs[ 0 ]
        assert isinstance( v, basestring ) and v
        return v
    else:
        assert len( vs ) <= 256
        assert all( isinstance( v, basestring ) and v for v in vs )
        return [ str( i ).zfill( sort_prefix_length ) + v for i, v in enumerate( vs ) ]


def passThrough( v ): return v


def skip( _ ): return None


class AWSJob( JobWrapper ):
    """
    A Job that can be converted to and from a SimpleDB Item
    """
    fromItemTransform = defaultdict( lambda: passThrough,
                                     predecessorNumber=int,
                                     memory=float,
                                     disk=float,
                                     cpu=float,
                                     updateID=str,
                                     command=toNoneable,
                                     stack=lambda v: map( literal_eval, toList( v ) ),
                                     jobsToDelete=toList,
                                     predecessorsFinished=toSet,
                                     remainingRetryCount=int,
                                     logJobStoreFileID=toNoneable )

    @classmethod
    def fromItem( cls, item, jobStoreID=None ):
        """
        :type item: Item
        :rtype: AWSJob
        """
        if jobStoreID is None: jobStoreID = item.name
        try:
            del item[ 'parentJobStoreID' ]
        except KeyError:
            pass
        item = { k: cls.fromItemTransform[ k ]( v ) for k, v in item.iteritems( ) }
        return cls( jobStoreID=jobStoreID, **item )

    toItemTransform = defaultdict( lambda: passThrough,
                                   command=fromNoneable,
                                   jobStoreID=skip,
                                   updateID=str,
                                   children=skip,
                                   stack=lambda v: fromList( map( repr, v ) ),
                                   logJobStoreFileID=fromNoneable,
                                   predecessorsFinished=fromSet,
                                   jobsToDelete=fromList,
                                   predecessorNumber=str,
                                   remainingRetryCount=str )

    def toItem( self, parentJobStoreID=None ):
        """
        :rtype: Item
        """
        item = self.toDict( )
        if parentJobStoreID is not None:
            item[ 'parentJobStoreID' ] = parentJobStoreID
        item = ((k, self.toItemTransform[ k ]( v )) for k, v in item.iteritems( ))
        return { k: v for k, v in item if v is not None }


# FIXME: This was lifted from cgcloud-lib where we use it for EC2 retries. The only difference
# FIXME: ... between that code and this is the name of the exception.

a_short_time = 5

a_long_time = 60 * 60


def no_such_domain( e ):
    return e.error_code.endswith( 'NoSuchDomain' )


def true( _ ):
    return True


def false( _ ):
    return False


def retry_sdb( retry_after=a_short_time,
               retry_for=10 * a_short_time,
               retry_while=no_such_domain ):
    """
    Retry an SDB operation while the failure matches a given predicate and until a given timeout
    expires, waiting a given amount of time in between attempts. This function is a generator
    that yields contextmanagers. See doctests below for example usage.

    :param retry_after: the delay in seconds between attempts

    :param retry_for: the timeout in seconds.

    :param retry_while: a callable with one argument, an instance of SDBResponseError, returning
    True if another attempt should be made or False otherwise

    :return: a generator yielding contextmanagers

    Retry for a limited amount of time:
    >>> i = 0
    >>> for attempt in retry_sdb( retry_after=0, retry_for=.1, retry_while=true ):
    ...     with attempt:
    ...         i += 1
    ...         raise SDBResponseError( 'foo', 'bar' )
    Traceback (most recent call last):
    ...
    SDBResponseError: SDBResponseError: foo bar
    <BLANKLINE>
    >>> i > 1
    True

    Do exactly one attempt:
    >>> i = 0
    >>> for attempt in retry_sdb( retry_for=0 ):
    ...     with attempt:
    ...         i += 1
    ...         raise SDBResponseError( 'foo', 'bar' )
    Traceback (most recent call last):
    ...
    SDBResponseError: SDBResponseError: foo bar
    <BLANKLINE>
    >>> i
    1

    Don't retry on success
    >>> i = 0
    >>> for attempt in retry_sdb( retry_after=0, retry_for=.1, retry_while=true ):
    ...     with attempt:
    ...         i += 1
    >>> i
    1

    Don't retry on unless condition returns
    >>> i = 0
    >>> for attempt in retry_sdb( retry_after=0, retry_for=.1, retry_while=false ):
    ...     with attempt:
    ...         i += 1
    ...         raise SDBResponseError( 'foo', 'bar' )
    Traceback (most recent call last):
    ...
    SDBResponseError: SDBResponseError: foo bar
    <BLANKLINE>
    >>> i
    1
    """
    if retry_for > 0:
        go = [ None ]

        @contextmanager
        def repeated_attempt( ):
            try:
                yield
            except SDBResponseError as e:
                if time.time( ) + retry_after < expiration:
                    if retry_while( e ):
                        log.info( '... got %s, trying again in %is ...', e.error_code, retry_after )
                        time.sleep( retry_after )
                    else:
                        log.info( 'Exception failed predicate, giving up.' )
                        raise
                else:
                    log.info( 'Retry timeout expired, giving up.' )
                    raise
            else:
                go.pop( )

        expiration = time.time( ) + retry_for
        while go:
            yield repeated_attempt( )
    else:
        @contextmanager
        def single_attempt( ):
            yield

        yield single_attempt( )
