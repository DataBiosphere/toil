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

from jobTree.jobStores.abstractJobStore import AbstractJobStore, JobTreeState, NoSuchJobException, \
    ConcurrentFileModificationException, NoSuchFileException
from jobTree.job import Job

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

    # FIXME: Eliminate after consolidating behaviour with FileJobStore

    resetJobInLoadState = True
    """Whether to reset the messages, remainingRetryCount and children attributes of a job when
    it is loaded by loadJobTreeState."""

    def loadJobsInStore( self ):
        raise NotImplementedError

    @classmethod
    def create( cls, jobStoreString, config=None ):
        region, namePrefix = cls._parseArgs( jobStoreString )
        return cls( region=region, namePrefix=namePrefix, config=config )

    def __init__( self, region, namePrefix, config=None ):
        log.debug( "Instantiating %s for region %s and name prefix '%s'",
                   self.__class__, region, namePrefix )
        self.region = region
        self.namePrefix = namePrefix
        self.jobs = None
        self.versions = None
        self.files = None
        self.stats = None
        self.db = self._connectSimpleDB( )
        self.s3 = self._connectS3( )
        create = config is not None
        self.jobs = self._getOrCreateDomain( 'jobs', create )
        self.versions = self._getOrCreateDomain( 'versions', create )
        self.files = self._getOrCreateBucket( 'files', create, versioning=True )
        self.stats = self._getOrCreateBucket( 'stats', create, versioning=True )
        self.bucketMap = {'files':self.files, 'stats':self.stats}
        super( AWSJobStore, self ).__init__( config=config )

    def createFirstJob( self, command, memory, cpu ):
        jobStoreID = self._newJobID( )
        log.debug( "Creating first job %s for '%s'",
                   jobStoreID, '<no command>' if command is None else command )
        job = AWSJob.create( jobStoreID=jobStoreID,
                             command=command, memory=memory, cpu=cpu,
                             tryCount=self._defaultTryCount( ), logJobStoreFileID=None )
        for attempt in retry_sdb( ):
            with attempt:
                assert self.jobs.put_attributes( item_name=jobStoreID,
                                                 attributes=job.toItem( ) )
        return job

    def exists( self, jobStoreID ):
        for attempt in retry_sdb( ):
            with attempt:
                return bool( self.jobs.get_attributes( item_name=jobStoreID,
                                                       attribute_name=[ ],
                                                       consistent_read=True ) )

    def _addChild( self, job, child ):
        if len( child.followOnCommands ) > 0:
            job.children.append( (child.jobStoreID,) + child.followOnCommands[ 0 ][ 1:-1 ] )

    def load( self, jobStoreID ):
        # TODO: check if mentioning individual attributes is faster than using *
        for attempt in retry_sdb( ):
            with attempt:
                result = list( self.jobs.select(
                    query="select * from `{domain}` "
                          "where parentJobStoreID = '{jobStoreID}' "
                          "or itemName() = '{jobStoreID}'".format( domain=self.jobs.name,
                                                                   jobStoreID=jobStoreID ),
                    consistent_read=True ) )
        job = None
        children = [ ]
        for item in result:
            if item.name == jobStoreID:
                job = AWSJob.fromItem( item )
            else:
                children.append( item )
        if job is None:
            raise NoSuchJobException( jobStoreID )
        for child in children:
            self._addChild( job, AWSJob.fromItem( child ) )
        log.debug( "Loaded job %s with %d children", job.jobStoreID, len( job.children ) )
        return job

    def store( self, job ):
        log.debug( "Storing job %s", job.jobStoreID )
        for attempt in retry_sdb( ):
            with attempt:
                assert self.jobs.put_attributes( item_name=job.jobStoreID,
                                                 attributes=job.toItem( ) )

    def addChildren( self, job, childCommands ):
        log.debug( "Adding %d children to job %s", len( childCommands ), job.jobStoreID )
        # The elements of childCommands are tuples (command, memory, cpu). The astute reader will
        # notice that we are careful to avoid specifically referencing all but the first element
        # of those tuples as they might change in the future.
        items = { }
        for childCommand in childCommands:
            child = AWSJob.create( tryCount=self._defaultTryCount( ),
                                   jobStoreID=self._newJobID( ),
                                   logJobStoreFileID=None,
                                   *childCommand )
            job.children.append( (child.jobStoreID,) + childCommand[ 1: ] )
            items[ child.jobStoreID ] = child.toItem( job.jobStoreID )
        # Persist parent and children
        # TODO: There might be no children, should we still persist the parent?
        items[ job.jobStoreID ] = job.toItem( )
        for attempt in retry_sdb( ):
            with attempt:
                self.jobs.batch_put_attributes( items=items )

    def delete( self, job ):
        log.debug( "Deleting job %s", job.jobStoreID )
        for attempt in retry_sdb( ):
            with attempt:
                self.jobs.delete_attributes( item_name=job.jobStoreID )
        for attempt in retry_sdb( ):
            with attempt:
                items = list( self.versions.select(
                    query="select * from `%s` "
                          "where jobStoreID='%s'" % (self.versions.name, job.jobStoreID),
                    consistent_read=True ) )
        if items:
            log.debug( "Deleting %d file(s) associated with job %s", len( items ), job.jobStoreID )
            for attempt in retry_sdb( ):
                with attempt:
                    self.versions.batch_delete_attributes( { item.name: None for item in items } )
            for item in items:
                self.files.delete_key( key_name=item.name,
                                       version_id=item[ 'version' ] )

    def loadJobTreeState( self ):
        jobs = { }
        for attempt in retry_sdb( ):
            with attempt:
                items = list( self.jobs.select( query='select * from `%s`' % self.jobs.name,
                                                consistent_read=True ) )
        for item in items:
            parentJobStoreID = item.get( 'parentJobStoreID', None )
            job = AWSJob.fromItem( item )
            if self.resetJobInLoadState:
                job.remainingRetryCount = self._defaultTryCount( )
            jobs[ job.jobStoreID ] = (job, parentJobStoreID)
        state = JobTreeState( )
        if jobs:
            state.started = True
            state.childCounts = defaultdict( int )
            try:
                for job, parentJobStoreID in jobs.itervalues( ):
                    if parentJobStoreID is not None:
                        parent = jobs[ parentJobStoreID ][ 0 ]
                        if not self.resetJobInLoadState:
                            self._addChild( parent, job )
                        state.childCounts[ parent ] += 1
                        state.childJobStoreIdToParentJob[ job.jobStoreID ] = parent
            finally:
                state.childCounts.default_factory = None
            for job, _ in jobs.itervalues( ):
                if self.resetJobInLoadState:
                    has_children = job in state.childCounts
                else:
                    has_children = len( job.children ) > 0
                if not has_children:
                    if job.followOnCommands:
                        state.updatedJobs.add( job )
                    else:
                        state.shellJobs.add( job )
        log.debug( "Loaded job tree state for %d jobs, "
                   "%d of which have a parent, "
                   "%d have children, "
                   "%d are updated and "
                   "%d are shells.", len( jobs ), len( state.childJobStoreIdToParentJob ),
                   len( state.childCounts ), len( state.updatedJobs ), len( state.shellJobs ) )
        return state

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
        oldVersion, bucket = self._getFileVersion( jobStoreFileID )
        if oldVersion is None:
            assert bucket is None
        else:
            assert bucket is self.files
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
        oldVersion, bucket = self._getFileVersion( jobStoreFileID )
        assert bucket is self.files
        newVersion = self._upload( jobStoreFileID, localFilePath )
        self._registerFile( jobStoreFileID, oldVersion=oldVersion, newVersion=newVersion )
        log.debug( "Wrote version %s of file %s from path '%s', replacing version %s",
                   newVersion, jobStoreFileID, localFilePath, oldVersion )

    @contextmanager
    def updateFileStream( self, jobStoreFileID ):
        oldVersion, bucket = self._getFileVersion( jobStoreFileID )
        assert bucket is self.files
        with self._uploadStream( jobStoreFileID, self.files ) as (writable, key):
            yield writable
        newVersion = key.version_id
        self._registerFile( jobStoreFileID, oldVersion=oldVersion, newVersion=newVersion )
        log.debug( "Wrote version %s of file %s, replacing version %s",
                   newVersion, jobStoreFileID, oldVersion )

    def readFile( self, jobStoreFileID, localFilePath ):
        version, bucket = self._getFileVersion( jobStoreFileID )
        if version is None: raise NoSuchFileException( jobStoreFileID )
        assert bucket is self.files
        log.debug( "Reading version %s of file %s to path '%s'",
                   version, jobStoreFileID, localFilePath )
        self._download( jobStoreFileID, localFilePath, version )

    @contextmanager
    def readFileStream( self, jobStoreFileID ):
        version, bucket = self._getFileVersion( jobStoreFileID )
        if version is None: raise NoSuchFileException( jobStoreFileID )
        assert bucket is self.files
        log.debug( "Reading version %s of file %s", version, jobStoreFileID )
        with self._downloadStream( jobStoreFileID, version, self.files ) as readable:
            yield readable

    @contextmanager
    def readSharedFileStream( self, sharedFileName ):
        assert self._validateSharedFileName( sharedFileName )
        jobStoreFileID = self._newFileID( sharedFileName )
        version, bucket = self._getFileVersion( jobStoreFileID )
        if version is None: raise NoSuchFileException( jobStoreFileID )
        assert bucket is self.files
        log.debug( "Read version %s from shared file %s (%s)",
                   version, sharedFileName, jobStoreFileID )
        with self._downloadStream( jobStoreFileID, version, self.files ) as readable:
            yield readable

    def deleteFile( self, jobStoreFileID ):
        version, bucket = self._getFileVersion( jobStoreFileID )
        if version is None: raise NoSuchFileException( jobStoreFileID )
        for attempt in retry_sdb( ):
            with attempt:
                self.versions.delete_attributes( jobStoreFileID,
                                                 expected_values=[ 'version', version ] )
        bucket.delete_key( key_name=jobStoreFileID, version_id=version )
        log.debug( "Deleted version %s of file %s", version, jobStoreFileID )

    def getEmptyFileStoreID( self, jobStoreID ):
        jobStoreFileID = self._newFileID( )
        self._registerFile( jobStoreFileID, jobStoreID=jobStoreID )
        log.debug( "Registered empty file %s for job %s", jobStoreFileID, jobStoreID )
        return jobStoreFileID

    def writeStatsAndLogging( self, statsAndLoggingString ):
        jobStoreFileId = self._newFileID()
        with self._uploadStream(jobStoreFileId, self.stats, multipart=False) as (writeable, key):
            writeable.write(statsAndLoggingString)
        firstVersion=key.version_id
        self._registerFile(jobStoreFileId,bucketName='stats',newVersion=firstVersion)

    def readStatsAndLogging( self, statsCallBackFn ):
        itemsProcessed=0
        for attempt in retry_sdb( ):
            with attempt:
                items = list( self.versions.select(
                            query="select * from `%s` "
                                  "where bucket='stats'" % (self.versions.name,),
                            consistent_read=True ) )
        for item in items:
            with self._downloadStream(item.name, item['version'], self.stats) as readable:
                statsCallBackFn(readable)
            self.deleteFile(item.name)
            itemsProcessed+=1
        return itemsProcessed

    # Dots in bucket names should be avoided because bucket names are used in HTTPS bucket
    # URLs where the may interfere with the certificate common name. We use a double
    # underscore as a separator instead.
    bucketNameRe = re.compile( r'^[a-z0-9][a-z0-9-]+[a-z0-9]$' )

    nameSeparator = '--'

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
        domain_name = self.namePrefix + self.nameSeparator + domain_name
        for i in itertools.count( ):
            try:
                return self.db.get_domain( domain_name )
            except SDBResponseError as e:
                if e.error_code == 'NoSuchDomain':
                    if i == 0 and create:
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
            return str( uuid.uuid5( self.sharedFileJobID, sharedFileName ) )

    def _getFileVersion( self, jobStoreFileID ):
        """
        :rtype: str
        """
        for attempt in retry_sdb( ):
            with attempt:
                item = self.versions.get_attributes( item_name=jobStoreFileID,
                                                     attribute_name=['version','bucket'],
                                                     consistent_read=True )
        return (item.get( 'version', None ), self.bucketMap.get(item.get('bucket',None),None))

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
        assert self._fileSizeAndTime( localFilePath ) == (file_size, file_time)
        return version

    @contextmanager
    def _uploadStream( self, jobStoreFileID, bucketObj, multipart=True):
        key = bucketObj.new_key( key_name=jobStoreFileID )
        assert key.version_id is None
        readable_fh, writable_fh = os.pipe( )
        with os.fdopen( readable_fh, 'r' ) as readable:
            with os.fdopen( writable_fh, 'w' ) as writable:
                def reader( ):
                    try:
                        upload = bucketObj.initiate_multipart_upload( key_name=jobStoreFileID )
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
                    log.debug("Using single part upload")
                    try:
                        buf = readable.read()
                        upload = key.set_contents_from_file(fp=StringIO(buf))
                    except:
                        log.exception("Exception in simple reader thread")

                thread = Thread( target=reader ) if multipart else Thread(target=simpleReader)
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
        Register a a file in the store. Register a

        :param jobStoreFileID: the file's ID, mandatory

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
        attributes = { }
        attributes['bucket']=bucketName
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
                bucket = self.bucketMap[bucketName]
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

    def __get_bucket_versioning( self, bucket ):
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
                if self.__get_bucket_versioning( bucket ) in (True, None):
                    for key in list( bucket.list_versions( ) ):
                        bucket.delete_key( key.name, version_id=key.version_id )
                else:
                    for key in list( bucket.list( ) ):
                        key.delete( )
                bucket.delete( )
        for domain in (self.versions, self.jobs):
            if domain is not None:
                domain.delete( )


# An attribute value of None becomes 'None' in SimpleDB. To truly represent attribute values of
# None, we'd have to always call delete_attributes in addition to put_attributes but there is no
# way to do that atomically. Instead we map None to the empty string and vice versa. The same
# applies to empty iterables. The empty iterable is a no-op for put_attributes, so we map that to
# "". This means that we can't serialize [""] or "" because the former would be deserialized as
# [] and the latter as None.

def toNoneable( v ):
    return v if v else None


def fromNoneable( v ):
    assert v != ""
    return '' if v is None else v


sort_prefix_length = 3


def toList( vs ):
    if isinstance( vs, basestring ):
        return [ vs ] if vs else [ ]
    else:
        return [ v[ sort_prefix_length: ] for v in sorted( vs ) ]


def fromList( vs ):
    if len( vs ) == 0:
        return ""
    elif len( vs ) == 1:
        return vs[ 0 ]
    else:
        assert len( vs ) <= 256
        assert all( isinstance( v, basestring ) and v for v in vs )
        return [ str( i ).zfill( sort_prefix_length ) + v for i, v in enumerate( vs ) ]


def passThrough( v ): return v


def skip( _ ): return None


class AWSJob( Job ):
    """
    A Job that can be converted to and from a SimpleDB Item
    """
    fromItemTransform = defaultdict( lambda: passThrough,
                                     messages=toList,
                                     followOnCommands=lambda v: map( literal_eval, toList( v ) ),
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
                                   messages=fromList,
                                   jobStoreID=skip,
                                   children=skip,
                                   logJobStoreFileID=fromNoneable,
                                   remainingRetryCount=str,
                                   followOnCommands=lambda v: fromList( map( repr, v ) ) )

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
                        log.info( '... got %s, trying again in %is ...' % ( e.error_code, retry_after ) )
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
