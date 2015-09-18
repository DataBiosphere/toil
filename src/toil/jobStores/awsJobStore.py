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
from StringIO import StringIO
from contextlib import contextmanager
import logging
import os
import re
from threading import Thread
import uuid
import bz2
import cPickle
import base64
import hashlib

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
import boto.sdb
from boto.exception import SDBResponseError, S3ResponseError, BotoServerError
import itertools
import time

from toil.jobStores.abstractJobStore import AbstractJobStore, NoSuchJobException, \
    ConcurrentFileModificationException, NoSuchFileException
from toil.jobWrapper import JobWrapper

log = logging.getLogger(__name__)


# FIXME: Command length is currently limited to 1024 characters

# FIXME: Passing in both headers and validate=False caused BotoClientError: When providing 'validate=False', no other
# params are allowed. Solution, validate=False was removed completely, but could potentially be passed if not encrypting

# NB: Number of messages per job is limited to 256-x, 1024 bytes each, with x being the number of
# other attributes in the item

# FIXME: enforce SimpleDB limits early


class AWSJobStore(AbstractJobStore):
    """
    A job store that uses Amazon's S3 for file storage and SimpleDB for storing job info and enforcing strong
    consistency on the S3 file storage. There will be SDB domains for jobs and versions and versioned S3 buckets for
    files and stats. The content of files and stats are stored as keys on the respective bucket while the latest
    version of a key is stored in the versions SDB domain. Job objects are pickled, compressed, partitioned into
    chunks of 1024 bytes and each chunk is stored as a an attribute of the SDB item representing the job. UUIDs are
    used to identify jobs and files.
    """

    def fileExists(self, jobStoreFileID):
        return bool(self.versions.get_item(item_name=jobStoreFileID, consistent_read=True))

    def jobs(self):
        result = None
        for attempt in retry_sdb():
            with attempt:
                result = list(self.jobDomain.select(
                    query="select * from `{domain}` ".format(domain=self.jobDomain.name),
                    consistent_read=True))
        assert result is not None
        for jobItem in result:
            yield AWSJob.fromItem(jobItem)

    def create(self, command, memory, cores, disk, updateID=None,
               predecessorNumber=0):
        jobStoreID = self._newJobID()
        log.debug("Creating job %s for '%s'",
                  jobStoreID, '<no command>' if command is None else command)
        job = AWSJob(jobStoreID=jobStoreID,
                     command=command, memory=memory, cores=cores, disk=disk,
                     remainingRetryCount=self._defaultTryCount(), logJobStoreFileID=None,
                     updateID=updateID, predecessorNumber=predecessorNumber)
        for attempt in retry_sdb():
            with attempt:
                assert self.jobDomain.put_attributes(item_name=jobStoreID,
                                                     attributes=job.toItem())
        return job

    def __init__(self, region, namePrefix, config=None):
        """
        Create a new job store in AWS or load an existing one from there.

        :param region: the AWS region to create the job store in, e.g. 'us-west-2'

        :param namePrefix: S3 bucket names and SDB tables will be prefixed with this

        :param config: the config object to written to this job store. Must be None for existing
        job stores. Must not be None for new job stores.
        """
        log.debug("Instantiating %s for region %s and name prefix '%s'",
                  self.__class__, region, namePrefix)
        self.region = region
        self.namePrefix = namePrefix
        self.jobDomain = None
        self.versions = None
        self.files = None
        self.stats = None
        self.db = self._connectSimpleDB()
        self.s3 = self._connectS3()
        self.sseKey = None

        # Check global registry domain for existence of this job store. The first time this is
        # being executed in an AWS account, the registry domain will be created on the fly.
        create = config is not None
        self.registry_domain = self._getOrCreateDomain('toil-registry')
        for attempt in retry_sdb():
            with attempt:
                attributes = self.registry_domain.get_attributes(item_name=namePrefix,
                                                                 attribute_name='exists',
                                                                 consistent_read=True)
                exists = parse_bool(attributes.get('exists', str(False)))
                self._checkJobStoreCreation(create, exists, region + ":" + namePrefix)

        self.jobDomain = self._getOrCreateDomain(self.qualify('jobs'))
        self.versions = self._getOrCreateDomain(self.qualify('versions'))
        self.files = self._getOrCreateBucket(self.qualify('files'), versioning=True)
        self.stats = self._getOrCreateBucket(self.qualify('stats'), versioning=True)

        # Now register this job store
        for attempt in retry_sdb():
            with attempt:
                self.registry_domain.put_attributes(item_name=namePrefix,
                                                    attributes=dict(exists='True'))

        super(AWSJobStore, self).__init__(config=config)

        if self.config.sseKey is not None:
            with open(self.config.sseKey) as f:
                self.sseKey = f.read()

    def qualify(self, name):
        return self.namePrefix + self.nameSeparator + name

    def exists(self, jobStoreID):
        for attempt in retry_sdb():
            with attempt:
                return bool(self.jobDomain.get_attributes(item_name=jobStoreID,
                                                          attribute_name=[],
                                                          consistent_read=True))

    def getPublicUrl(self, jobStoreFileID):
        """
        For Amazon SimpleDB requests, use HTTP GET requests that are URLs with query strings.
        http://awsdocs.s3.amazonaws.com/SDB/latest/sdb-dg.pdf
        Create url, check if valid, return.
        Encrypted file urls are currently not supported
        """
        key = self.files.get_key(key_name=jobStoreFileID)
        # There should be no practical upper limit on when a job is allowed to access a public
        # URL so we set the expiration to 20 years.
        return key.generate_url(expires_in=60 * 60 * 24 * 365 * 20)

    def getSharedPublicUrl(self, FileName):
        jobStoreFileID = self._newFileID(FileName)
        return self.getPublicUrl(jobStoreFileID)

    def load(self, jobStoreID):
        # TODO: check if mentioning individual attributes is faster than using *
        result = None
        for attempt in retry_sdb():
            with attempt:
                result = list(self.jobDomain.select(
                    query="select * from `{domain}` "
                          "where itemName() = '{jobStoreID}'".format(domain=self.jobDomain.name,
                                                                     jobStoreID=jobStoreID),
                    consistent_read=True))
        assert result is not None
        if len(result) != 1:
            raise NoSuchJobException(jobStoreID)
        job = AWSJob.fromItem(result[0])
        if job is None:
            raise NoSuchJobException(jobStoreID)
        log.debug("Loaded job %s", jobStoreID)
        return job

    def update(self, job):
        log.debug("Updating job %s", job.jobStoreID)
        for attempt in retry_sdb():
            with attempt:
                assert self.jobDomain.put_attributes(item_name=job.jobStoreID,
                                                     attributes=job.toItem())

    items_per_batch_delete = 25

    def delete(self, jobStoreID):
        # remove job and replace with jobStoreId.
        log.debug("Deleting job %s", jobStoreID)
        for attempt in retry_sdb():
            with attempt:
                self.jobDomain.delete_attributes(item_name=jobStoreID)
        items = None
        for attempt in retry_sdb():
            with attempt:
                items = list(self.versions.select(
                    query="select * from `%s` "
                          "where jobStoreID='%s'" % (self.versions.name, jobStoreID),
                    consistent_read=True))
        assert items is not None
        if items:
            log.debug("Deleting %d file(s) associated with job %s", len(items), jobStoreID)
            n = self.items_per_batch_delete
            batches = [items[i:i + n] for i in range(0, len(items), n)]
            for batch in batches:
                for attempt in retry_sdb():
                    with attempt:
                        self.versions.batch_delete_attributes({item.name: None for item in batch})
            for item in items:
                if 'version' in item:
                    self.files.delete_key(key_name=item.name,
                                          version_id=item['version'])
                else:
                    self.files.delete_key(key_name=item.name)

    def writeFile(self, localFilePath, jobStoreID=None):
        jobStoreFileID = self._newFileID()
        firstVersion = self._upload(jobStoreFileID, localFilePath)
        self._registerFile(jobStoreFileID, jobStoreID=jobStoreID, newVersion=firstVersion)
        log.debug("Wrote initial version %s of file %s for job %s from path '%s'",
                  firstVersion, jobStoreFileID, jobStoreID, localFilePath)
        return jobStoreFileID

    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        jobStoreFileID = self._newFileID()
        with self._uploadStream(jobStoreFileID, self.files) as (writable, key):
            yield writable, jobStoreFileID
        firstVersion = key.version_id
        assert firstVersion is not None
        self._registerFile(jobStoreFileID, jobStoreID=jobStoreID, newVersion=firstVersion)
        log.debug("Wrote initial version %s of file %s for job %s",
                  firstVersion, jobStoreFileID, jobStoreID)

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=True):
        assert self._validateSharedFileName(sharedFileName)
        jobStoreFileID = self._newFileID(sharedFileName)
        oldVersion = self._getFileVersion(jobStoreFileID)
        with self._uploadStream(jobStoreFileID, self.files,
                                encrypted=isProtected) as (writable, key):
            yield writable
        newVersion = key.version_id
        jobStoreId = str(self.sharedFileJobID) if oldVersion is None else None
        self._registerFile(jobStoreFileID,
                           jobStoreID=jobStoreId, oldVersion=oldVersion, newVersion=newVersion)
        if oldVersion is None:
            log.debug("Wrote initial version %s of shared file %s (%s)",
                      newVersion, sharedFileName, jobStoreFileID)
        else:
            log.debug("Wrote version %s of file %s (%s), replacing version %s",
                      newVersion, sharedFileName, jobStoreFileID, oldVersion)

    def updateFile(self, jobStoreFileID, localFilePath):
        oldVersion = self._getFileVersion(jobStoreFileID)
        newVersion = self._upload(jobStoreFileID, localFilePath)
        self._registerFile(jobStoreFileID, oldVersion=oldVersion, newVersion=newVersion)
        log.debug("Wrote version %s of file %s from path '%s', replacing version %s",
                  newVersion, jobStoreFileID, localFilePath, oldVersion)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        oldVersion = self._getFileVersion(jobStoreFileID)
        with self._uploadStream(jobStoreFileID, self.files) as (writable, key):
            yield writable
        newVersion = key.version_id
        self._registerFile(jobStoreFileID, oldVersion=oldVersion, newVersion=newVersion)
        log.debug("Wrote version %s of file %s, replacing version %s",
                  newVersion, jobStoreFileID, oldVersion)

    def readFile(self, jobStoreFileID, localFilePath):
        version = self._getFileVersion(jobStoreFileID)
        if version is None: raise NoSuchFileException(jobStoreFileID)
        log.debug("Reading version %s of file %s to path '%s'",
                  version, jobStoreFileID, localFilePath)
        self._download(jobStoreFileID, localFilePath, version)

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        version = self._getFileVersion(jobStoreFileID)
        if version is None: raise NoSuchFileException(jobStoreFileID)
        log.debug("Reading version %s of file %s", version, jobStoreFileID)
        with self._downloadStream(jobStoreFileID, version, self.files) as readable:
            yield readable

    @contextmanager
    def readSharedFileStream(self, sharedFileName, isProtected=True):
        assert self._validateSharedFileName(sharedFileName)
        jobStoreFileID = self._newFileID(sharedFileName)
        version = self._getFileVersion(jobStoreFileID)
        if version is None: raise NoSuchFileException(jobStoreFileID)
        log.debug("Read version %s from shared file %s (%s)",
                  version, sharedFileName, jobStoreFileID)
        with self._downloadStream(jobStoreFileID, version, self.files,
                                  encrypted=isProtected) as readable:
            yield readable

    def deleteFile(self, jobStoreFileID):
        version, bucket = self._getFileVersionAndBucket(jobStoreFileID)
        if bucket:
            for attempt in retry_sdb():
                with attempt:
                    if version:
                        self.versions.delete_attributes(jobStoreFileID,
                                                        expected_values=['version', version])
                    else:
                        self.versions.delete_attributes(jobStoreFileID)

            bucket.delete_key(key_name=jobStoreFileID, version_id=version)
            if version:
                log.debug("Deleted version %s of file %s", version, jobStoreFileID)
            else:
                log.debug("Deleted unversioned file %s", jobStoreFileID)
        else:
            log.debug("File %s does not exist", jobStoreFileID)

    def getEmptyFileStoreID(self, jobStoreID=None):
        jobStoreFileID = self._newFileID()
        self._registerFile(jobStoreFileID, jobStoreID=jobStoreID)
        log.debug("Registered empty file %s for job %s", jobStoreFileID, jobStoreID)
        return jobStoreFileID

    def writeStatsAndLogging(self, statsAndLoggingString):
        jobStoreFileId = self._newFileID()
        with self._uploadStream(jobStoreFileId, self.stats, multipart=False) as (writeable, key):
            writeable.write(statsAndLoggingString)
        firstVersion = key.version_id
        self._registerFile(jobStoreFileId, bucketName='stats', newVersion=firstVersion)

    def readStatsAndLogging(self, statsCallBackFn):
        itemsProcessed = 0
        items = None
        for attempt in retry_sdb():
            with attempt:
                items = list(self.versions.select(
                    query="select * from `%s` "
                          "where bucketName='stats'" % (self.versions.name,),
                    consistent_read=True))
        assert items is not None
        for item in items:
            with self._downloadStream(item.name, item['version'], self.stats) as readable:
                statsCallBackFn(readable)
            self.deleteFile(item.name)
            itemsProcessed += 1
        return itemsProcessed

    # Dots in bucket names should be avoided because bucket names are used in HTTPS bucket
    # URLs where the may interfere with the certificate common name. We use a double
    # underscore as a separator instead.
    bucketNameRe = re.compile(r'^[a-z0-9][a-z0-9-]+[a-z0-9]$')

    nameSeparator = '--'

    @classmethod
    def _parseArgs(cls, jobStoreString):
        region, namePrefix = jobStoreString.split(':')
        # See http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html,
        # reserve 10 characters for separator and suffixes
        if not cls.bucketNameRe.match(namePrefix):
            raise ValueError("Invalid name prefix '%s'. Name prefixes must contain only digits, "
                             "hyphens or lower-case letters and must not start or end in a "
                             "hyphen." % namePrefix)
        # reserve 13 for separator and suffix
        if len(namePrefix) > 50:
            raise ValueError("Invalid name prefix '%s'. Name prefixes may not be longer than 50 "
                             "characters." % namePrefix)
        if '--' in namePrefix:
            raise ValueError("Invalid name prefix '%s'. Name prefixes may not contain "
                             "%s." % (namePrefix, cls.nameSeparator))

        return region, namePrefix

    def _connectSimpleDB(self):
        """
        rtype: SDBConnection
        """
        db = boto.sdb.connect_to_region(self.region)
        if db is None:
            raise ValueError("Could not connect to SimpleDB. Make sure '%s' is a valid SimpleDB "
                             "region." % self.region)
        assert db is not None
        return db

    def _connectS3(self):
        """
        :rtype: S3Connection
        """
        s3 = boto.s3.connect_to_region(self.region)
        if s3 is None:
            raise ValueError("Could not connect to S3. Make sure '%s' is a valid S3 region." %
                             self.region)
        return s3

    def _getOrCreateBucket(self, bucket_name, versioning=False):
        """
        :rtype Bucket
        """
        assert self.bucketNameRe.match(bucket_name)
        assert 3 <= len(bucket_name) <= 63
        try:
            bucket = self.s3.get_bucket(bucket_name, validate=True)
            assert versioning is self.__getBucketVersioning(bucket)
            return bucket
        except S3ResponseError as e:
            if e.error_code == 'NoSuchBucket':
                bucket = self.s3.create_bucket(bucket_name, location=self.region)
                if versioning:
                    bucket.configure_versioning(versioning)
                return bucket
            else:
                raise

    def _getOrCreateDomain(self, domain_name):
        """
        Return the boto Domain object representing the SDB domain with the given name. If the
        domain does not exist it will be created.

        :param domain_name: the unqualified name of the domain to be created

        :rtype : Domain
        """
        try:
            return self.db.get_domain(domain_name)
        except SDBResponseError as e:
            if no_such_domain(e):
                for attempt in retry_sdb(retry_while=sdb_unavailable):
                    with attempt:
                        return self.db.create_domain(domain_name)
            else:
                raise

    def _newJobID(self):
        return str(uuid.uuid4())

    # A dummy job ID under which all shared files are stored.
    sharedFileJobID = uuid.UUID('891f7db6-e4d9-4221-a58e-ab6cc4395f94')

    def _newFileID(self, sharedFileName=None):
        if sharedFileName is None:
            return str(uuid.uuid4())
        else:
            return str(uuid.uuid5(self.sharedFileJobID, str(sharedFileName)))

    def _getFileVersionAndBucket(self, jobStoreFileID):
        """
        :rtype: tuple(str version, AWS bucket)
        """
        item = None
        for attempt in retry_sdb():
            with attempt:
                item = self.versions.get_attributes(item_name=jobStoreFileID,
                                                    attribute_name=['version', 'bucketName'],
                                                    consistent_read=True)
        assert item is not None
        bucketName = item.get('bucketName')
        if bucketName is None:
            return None, None
        else:
            # noinspection PyTypeChecker
            return item.get('version'), getattr(self, bucketName)

    def _getFileVersion(self, jobStoreFileID, expectedBucket=None):
        version, bucket = self._getFileVersionAndBucket(jobStoreFileID)
        if bucket is None:
            assert version is None
        else:
            if expectedBucket is None:
                expectedBucket = self.files
            assert bucket is expectedBucket
        return version

    _s3_part_size = 50 * 1024 * 1024

    def _upload(self, jobStoreFileID, localFilePath):
        file_size, file_time = self._fileSizeAndTime(localFilePath)
        headers = {}
        self.__add_encryption_headers(headers)
        if file_size <= self._s3_part_size:
            key = self.files.new_key(key_name=jobStoreFileID)
            key.name = jobStoreFileID
            key.set_contents_from_filename(localFilePath, headers=headers)
            version = key.version_id
        else:
            with open(localFilePath, 'rb') as f:
                upload = self.files.initiate_multipart_upload(key_name=jobStoreFileID,
                                                              headers=headers)
                try:
                    start = 0
                    part_num = itertools.count()
                    while start < file_size:
                        end = min(start + self._s3_part_size, file_size)
                        assert f.tell() == start
                        upload.upload_part_from_file(fp=f,
                                                     part_num=next(part_num) + 1,
                                                     size=end - start,
                                                     headers=headers)
                        start = end
                    assert f.tell() == file_size == start
                except:
                    upload.cancel_upload()
                    raise
                else:
                    version = upload.complete_upload().version_id
        key = self.files.get_key(jobStoreFileID, headers=headers)

        assert key.size == file_size
        # Make resonably sure that the file wasn't touched during the upload
        assert self._fileSizeAndTime(localFilePath) == (file_size, file_time)
        return version

    @contextmanager
    def _uploadStream(self, jobStoreFileID, bucket, multipart=True, encrypted=True):
        key = bucket.new_key(key_name=jobStoreFileID)
        assert key.version_id is None
        readable_fh, writable_fh = os.pipe()
        headers = {}
        if encrypted:
            self.__add_encryption_headers(headers)
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def reader():
                    try:
                        upload = bucket.initiate_multipart_upload(key_name=jobStoreFileID,
                                                                  headers=headers)
                        try:
                            for part_num in itertools.count():
                                # FIXME: Consider using a key.set_contents_from_stream and rip ...
                                # FIXME: ... the query_args logic from upload_part_from_file in ...
                                # FIXME: ... in MultipartUpload. Possible downside is that ...
                                # FIXME: ... implicit retries won't work.
                                buf = readable.read(self._s3_part_size)
                                # There must be at least one part, even if the file is empty.
                                if len(buf) == 0 and part_num > 0: break
                                upload.upload_part_from_file(fp=StringIO(buf),
                                                             # S3 part numbers are 1-based
                                                             part_num=part_num + 1, headers=headers)
                                if len(buf) == 0: break
                        except:
                            upload.cancel_upload()
                            raise
                        else:
                            key.version_id = upload.complete_upload().version_id
                    except:
                        log.exception('Exception in reader thread')

                def simpleReader():
                    log.debug("Using single part upload")
                    try:
                        buf = StringIO(readable.read())
                        assert key.set_contents_from_file(fp=buf, headers=headers) == buf.len
                    except:
                        log.exception("Exception in simple reader thread")

                thread = Thread(target=reader if multipart else simpleReader)
                thread.start()
                # Yield the key now with version_id unset. When reader() returns
                # key.version_id will be set.
                yield writable, key
            # The writable is now closed. This will send EOF to the readable and cause that
            # thread to finish.
            thread.join()
            assert key.version_id is not None

    def _download(self, jobStoreFileID, localFilePath, version):
        headers = {}
        self.__add_encryption_headers(headers)
        key = self.files.get_key(jobStoreFileID, headers=headers)
        key.get_contents_to_filename(localFilePath, version_id=version, headers=headers)

    @contextmanager
    def _downloadStream(self, jobStoreFileID, version, bucket, encrypted=True):
        headers = {}
        if encrypted:
            self.__add_encryption_headers(headers)
        key = bucket.get_key(jobStoreFileID, headers=headers)
        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def writer():
                    key.get_contents_to_file(writable, headers=headers, version_id=version)
                    # This close() will send EOF to the reading end and ultimately cause the
                    # yield to return. It also makes the implict .close() done by the enclosing
                    # "with" context redundant but that should be ok since .close() on file
                    # objects are idempotent.
                    writable.close()

                thread = Thread(target=writer)
                thread.start()
                yield readable
                thread.join()

    def _registerFile(self, jobStoreFileID,
                      bucketName='files', jobStoreID=None, newVersion=None, oldVersion=None):
        """
        Register a a file in the store

        :param jobStoreFileID: the file's ID, mandatory

        :param bucketName: the name of the S3 bucket the file was placed in

        :param jobStoreID: optional ID of the job owning the file, only allowed for first version of
                           file

        :param newVersion: the file's new version or None if the file is to be registered without
                           content, in which case jobStoreId must be passed

        :param oldVersion: the expected previous version of the file or None if newVersion is the
                           first version or file is registered without content
        """
        # Must pass newVersion if passing oldVersion
        assert oldVersion is None or newVersion is not None
        attributes = dict(bucketName=bucketName)
        if newVersion is not None:
            attributes['version'] = newVersion
        if jobStoreID is not None:
            attributes['jobStoreID'] = jobStoreID
        # False stands for absence
        expected = ['version', False if oldVersion is None else oldVersion]
        try:
            for attempt in retry_sdb():
                with attempt:
                    assert self.versions.put_attributes(item_name=jobStoreFileID,
                                                        attributes=attributes,
                                                        expected_value=expected)
            if oldVersion is not None:
                bucket = getattr(self, bucketName)
                bucket.delete_key(jobStoreFileID, version_id=oldVersion)
        except SDBResponseError as e:
            if e.error_code == 'ConditionalCheckFailed':
                raise ConcurrentFileModificationException(jobStoreFileID)
            else:
                raise

    def _fileSizeAndTime(self, localFilePath):
        file_stat = os.stat(localFilePath)
        file_size, file_time = file_stat.st_size, file_stat.st_mtime
        return file_size, file_time

    versionings = dict(Enabled=True, Disabled=False, Suspended=None)

    def __getBucketVersioning(self, bucket):
        """
        A valueable lesson in how to feck up a simple tri-state boolean.

        For newly created buckets get_versioning_status returns None. We map that to False.

        TBD: This may actually be a result of eventual consistency

        Otherwise, the 'Versioning' entry in the dictionary returned by get_versioning_status can
        be 'Enabled', 'Suspended' or 'Disabled' which we map to True, None and False
        respectively. Calling configure_versioning with False on a bucket will cause
        get_versioning_status to then return 'Suspended' for some reason.
        """
        status = bucket.get_versioning_status()
        return bool(status) and self.versionings[status['Versioning']]

    def __add_encryption_headers(self, headers):
        if self.sseKey is not None:
            self._add_encryption_headers(self.sseKey, headers)

    def deleteJobStore(self):
        self.registry_domain.put_attributes(self.namePrefix, dict(exists=str(False)))
        for bucket in (self.files, self.stats):
            if bucket is not None:
                for upload in bucket.list_multipart_uploads():
                    upload.cancel_upload()
                if self.__getBucketVersioning(bucket) in (True, None):
                    for key in list(bucket.list_versions()):
                        bucket.delete_key(key.name, version_id=key.version_id)
                else:
                    for key in list(bucket.list()):
                        key.delete()
                bucket.delete()
        for domain in (self.versions, self.jobDomain):
            if domain is not None:
                domain.delete()

    @staticmethod
    def _add_encryption_headers(sse_key, headers):
        assert len(sse_key) == 32
        encoded_sse_key = base64.b64encode(sse_key)
        encoded_sse_key_md5 = base64.b64encode(hashlib.md5(sse_key).digest())
        headers['x-amz-server-side-encryption-customer-algorithm'] = 'AES256'
        headers['x-amz-server-side-encryption-customer-key'] = encoded_sse_key
        headers['x-amz-server-side-encryption-customer-key-md5'] = encoded_sse_key_md5


class AWSJob(JobWrapper):
    """
    A Job that can be converted to and from a SimpleDB Item
    """

    @classmethod
    def fromItem(cls, item):
        """
        :type item: Item
        :rtype: AWSJob
        """
        chunkedJob = item.items()
        chunkedJob.sort()
        if len(chunkedJob) == 1:
            # First element of list = tuple, second element of tuple = serialized job
            wholeJobString = chunkedJob[0][1]
        else:
            wholeJobString = ''.join(item[1] for item in chunkedJob)
        return cPickle.loads(bz2.decompress(base64.b64decode(wholeJobString)))

    def toItem(self):
        """
        :rtype: Item
        """
        item = {}
        serializedAndEncodedJob = base64.b64encode(bz2.compress(cPickle.dumps(self)))
        # this convoluted expression splits the string into chunks of 1024 - the max value for an attribute in SDB
        jobChunks = [serializedAndEncodedJob[i:i + 1024]
                     for i in range(0, len(serializedAndEncodedJob), 1024)]
        for attributeOrder, chunk in enumerate(jobChunks):
            item[str(attributeOrder).zfill(3)] = chunk
        return item


# FIXME: This was lifted from cgcloud-lib where we use it for EC2 retries. The only difference
# FIXME: ... between that code and this is the name of the exception.

a_short_time = 5

a_long_time = 60 * 60


def no_such_domain(e):
    return isinstance(e, SDBResponseError) and e.error_code.endswith('NoSuchDomain')


def sdb_unavailable(e):
    return e.__class__ == BotoServerError and e.status.startswith("503")


def true(_):
    return True


def false(_):
    return False


def retry_sdb(retry_after=a_short_time,
              retry_for=10 * a_short_time,
              retry_while=no_such_domain):
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
        go = [None]

        @contextmanager
        def repeated_attempt():
            try:
                yield
            except BotoServerError as e:
                if time.time() + retry_after < expiration:
                    if retry_while(e):
                        log.info('... got %s, trying again in %is ...', e.error_code, retry_after)
                        time.sleep(retry_after)
                    else:
                        log.info('Exception failed predicate, giving up.')
                        raise
                else:
                    log.info('Retry timeout expired, giving up.')
                    raise
            else:
                go.pop()

        expiration = time.time() + retry_for
        while go:
            yield repeated_attempt()
    else:
        @contextmanager
        def single_attempt():
            yield

        yield single_attempt()


def parse_bool(s):
    if s == 'True':
        return True
    elif s == 'False':
        return False
    else:
        raise ValueError(s)
