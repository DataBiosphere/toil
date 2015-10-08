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
import uuid
import bz2
import cPickle
import base64
import hashlib
import itertools
import time
from bd2k.util.threading import ExceptionalThread

from boto.sdb.domain import Domain
from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection
from boto.sdb.connection import SDBConnection
from boto.sdb.item import Item
import boto.s3
import boto.sdb

from boto.exception import SDBResponseError, S3ResponseError, BotoServerError
from enum import Enum

from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             ConcurrentFileModificationException,
                                             NoSuchFileException)
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
    A job store that uses Amazon's S3 for file storage and SimpleDB for storing job info and
    enforcing strong consistency on the S3 file storage. There will be SDB domains for jobs and
    files and a versioned S3 bucket for file contents. Job objects are pickled, compressed,
    partitioned into chunks of 1024 bytes and each chunk is stored as a an attribute of the SDB
    item representing the job. UUIDs are used to identify jobs and files.
    """

    def fileExists(self, jobStoreFileID):
        return bool(self.filesDomain.get_item(item_name=jobStoreFileID, consistent_read=True))

    def jobs(self):
        result = None
        for attempt in retry_sdb():
            with attempt:
                result = list(self.jobsDomain.select(
                    query="select * from `{domain}` ".format(domain=self.jobsDomain.name),
                    consistent_read=True))
        assert result is not None
        for jobItem in result:
            yield AWSJob.fromItem(jobItem)

    def create(self, command, memory, cores, disk, predecessorNumber=0):
        jobStoreID = self._newJobID()
        log.debug("Creating job %s for '%s'",
                  jobStoreID, '<no command>' if command is None else command)
        job = AWSJob(jobStoreID=jobStoreID,
                     command=command, memory=memory, cores=cores, disk=disk,
                     remainingRetryCount=self._defaultTryCount(), logJobStoreFileID=None,
                     predecessorNumber=predecessorNumber)
        for attempt in retry_sdb():
            with attempt:
                assert self.jobsDomain.put_attributes(item_name=jobStoreID,
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
        self.jobsDomain = None
        self.filesDomain = None
        self.filesBucket = None
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

        self.jobsDomain = self._getOrCreateDomain(self.qualify('jobs'))
        self.filesDomain = self._getOrCreateDomain(self.qualify('files'))
        self.filesBucket = self._getOrCreateBucket(self.qualify('files'), versioning=True)

        # Now register this job store
        for attempt in retry_sdb():
            with attempt:
                self.registry_domain.put_attributes(item_name=namePrefix,
                                                    attributes=dict(exists='True'))

        super(AWSJobStore, self).__init__(config=config)

        if self.config.sseKey is not None:
            with open(self.config.sseKey) as f:
                self.sseKey = f.read()

    def qualify(self, name=None):
        return self.namePrefix if name is None else self.namePrefix + self.nameSeparator + name

    def exists(self, jobStoreID):
        for attempt in retry_sdb():
            with attempt:
                return bool(self.jobsDomain.get_attributes(item_name=jobStoreID,
                                                           attribute_name=[],
                                                           consistent_read=True))

    def getPublicUrl(self, jobStoreFileID):
        """
        For Amazon SimpleDB requests, use HTTP GET requests that are URLs with query strings.
        http://awsdocs.s3.amazonaws.com/SDB/latest/sdb-dg.pdf
        Create url, check if valid, return.
        Encrypted file urls are currently not supported
        """
        key = self.filesBucket.get_key(key_name=jobStoreFileID)
        # There should be no practical upper limit on when a job is allowed to access a public
        # URL so we set the expiration to 20 years.
        return key.generate_url(expires_in=60 * 60 * 24 * 365 * 20)

    def getSharedPublicUrl(self, sharedFileName):
        jobStoreFileID = self._sharedFileID(sharedFileName)
        return self.getPublicUrl(jobStoreFileID)

    def load(self, jobStoreID):
        item = None
        for attempt in retry_sdb():
            with attempt:
                item = self.jobsDomain.get_attributes(jobStoreID, consistent_read=True)
        if not item:
            raise NoSuchJobException(jobStoreID)
        job = AWSJob.fromItem(item)
        if job is None:
            raise NoSuchJobException(jobStoreID)
        log.debug("Loaded job %s", jobStoreID)
        return job

    def update(self, job):
        log.debug("Updating job %s", job.jobStoreID)
        for attempt in retry_sdb():
            with attempt:
                assert self.jobsDomain.put_attributes(item_name=job.jobStoreID,
                                                      attributes=job.toItem())

    items_per_batch_delete = 25

    def delete(self, jobStoreID):
        # remove job and replace with jobStoreId.
        log.debug("Deleting job %s", jobStoreID)
        for attempt in retry_sdb():
            with attempt:
                self.jobsDomain.delete_attributes(item_name=jobStoreID)
        items = None
        for attempt in retry_sdb():
            with attempt:
                items = list(self.filesDomain.select(
                    query="select itemName() from `%s` "
                          "where jobStoreID='%s'" % (self.filesDomain.name, jobStoreID),
                    consistent_read=True))
        assert items is not None
        if items:
            log.debug("Deleting %d file(s) associated with job %s", len(items), jobStoreID)
            n = self.items_per_batch_delete
            batches = [items[i:i + n] for i in range(0, len(items), n)]
            for batch in batches:
                itemsDict = {item.name: None for item in batch}
                for attempt in retry_sdb():
                    with attempt:
                        self.filesDomain.batch_delete_attributes(itemsDict)
            for item in items:
                if 'version' in item:
                    self.filesBucket.delete_key(key_name=item.name,
                                                version_id=item['version'])
                else:
                    self.filesBucket.delete_key(key_name=item.name)

    def writeFile(self, localFilePath, jobStoreID=None):
        info = FileInfo(self._newFileID(), jobID=jobStoreID)
        info.version = self._upload(info.fileID, localFilePath)
        self._saveFileInfo(info)
        log.debug("Wrote initial version %s of file %s for job %s from path '%s'",
                  info.version, info.fileID, info.jobID, localFilePath)
        return info.fileID

    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        info = FileInfo(self._newFileID(), jobID=jobStoreID)
        with self._uploadStream(info.fileID) as (writable, key):
            yield writable, (info.fileID)
        info.version = key.version_id
        self._saveFileInfo(info)
        log.debug("Wrote initial version %s of file %s for job %s",
                  info.version, info.fileID, info.jobID)

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=True):
        assert self._validateSharedFileName(sharedFileName)
        fileId = self._sharedFileID(sharedFileName)
        jobID = str(self.sharedFileJobID)
        info = self._loadFileInfo(fileId)
        if info is None:
            info = FileInfo(fileId, jobID=jobID)
        else:
            assert info.jobID == jobID
        with self._uploadStream(info.fileID, encrypted=isProtected) as (writable, key):
            yield writable
        info.version = key.version_id
        self._saveFileInfo(info)
        if info.oldVersion is None:
            log.debug("Wrote initial version %s of shared file %s (%s)",
                      info.version, sharedFileName, info.fileID)
        else:
            log.debug("Wrote version %s of file %s (%s), replacing version %s",
                      info.version, sharedFileName, info.fileID, info.oldVersion)

    def updateFile(self, jobStoreFileID, localFilePath):
        info = self._loadFileInfo(jobStoreFileID)
        info.version = self._upload(jobStoreFileID, localFilePath)
        self._saveFileInfo(info)
        log.debug("Wrote version %s of file %s from path '%s', replacing version %s",
                  info.version, jobStoreFileID, localFilePath, info.oldVersion)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        info = self._loadFileInfo(jobStoreFileID)
        with self._uploadStream(jobStoreFileID) as (writable, key):
            yield writable
        info.version = key.version_id
        self._saveFileInfo(info)
        log.debug("Wrote version %s of file %s, replacing version %s",
                  info.version, jobStoreFileID, info.oldVersion)

    def readFile(self, jobStoreFileID, localFilePath):
        info = self._loadFileInfo(jobStoreFileID)
        if info is None:
            raise NoSuchFileException(jobStoreFileID)
        log.debug("Reading version %s of file %s to path '%s'",
                  info.version, info.fileID, localFilePath)
        self._download(info.fileID, localFilePath, info.version)

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        info = self._loadFileInfo(jobStoreFileID)
        if info is None:
            raise NoSuchFileException(jobStoreFileID)
        log.debug("Reading version %s of file %s", info.version, info.fileID)
        with self._downloadStream(info.fileID, info.version) as readable:
            yield readable

    @contextmanager
    def readSharedFileStream(self, sharedFileName, isProtected=True):
        assert self._validateSharedFileName(sharedFileName)
        jobStoreFileID = self._sharedFileID(sharedFileName)
        info = self._loadFileInfo(jobStoreFileID)
        if info is None:
            raise NoSuchFileException(jobStoreFileID, sharedFileName)
        log.debug("Read version %s from shared file %s (%s)",
                  info.version, sharedFileName, info.fileID)
        with self._downloadStream(info.fileID, info.version, encrypted=isProtected) as readable:
            yield readable

    def deleteFile(self, jobStoreFileID):
        info = self._loadFileInfo(jobStoreFileID)
        if info is None:
            log.debug("File %s does not exist", jobStoreFileID)
        else:
            for attempt in retry_sdb():
                with attempt:
                    if info.version:
                        self.filesDomain.delete_attributes(
                            jobStoreFileID,
                            expected_values=['version', info.version])
                    else:
                        self.filesDomain.delete_attributes(jobStoreFileID)
            self.filesBucket.delete_key(key_name=jobStoreFileID, version_id=info.version)
            if info.version is None:
                log.debug("Deleted unversioned file %s", jobStoreFileID)
            else:
                log.debug("Deleted version %s of file %s", info.version, jobStoreFileID)

    def getEmptyFileStoreID(self, jobStoreID=None):
        info = FileInfo(self._newFileID(), jobID=jobStoreID)
        self._saveFileInfo(info)
        log.debug("Registered empty file %s for job %s", info.fileID, info.jobID)
        return info.fileID

    def writeStatsAndLogging(self, statsAndLoggingString):
        info = FileInfo(self._newFileID(), fileType=FileType.stats)
        with self._uploadStream(info.fileID, multipart=False) as (writeable, key):
            writeable.write(statsAndLoggingString)
        info.version = key.version_id
        self._saveFileInfo(info)

    def readStatsAndLogging(self, statsCallBackFn):
        itemsProcessed = 0
        items = None
        for attempt in retry_sdb():
            with attempt:
                items = list(self.filesDomain.select(
                    query="select * from `%s` "
                          "where fileType='%s'" % (self.filesDomain.name, FileType.stats.name),
                    consistent_read=True))
        assert items is not None
        for item in items:
            with self._downloadStream(item.name, item['version']) as readable:
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
        :rtype: SDBConnection
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
        :rtype: Bucket
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

    def _sharedFileID(self, sharedFileName):
        return str(uuid.uuid5(self.sharedFileJobID, str(sharedFileName)))

    def _newFileID(self):
        return str(uuid.uuid4())

    def _loadFileInfo(self, jobStoreFileID):
        """
        Returns the version of the S3 key storing the file with the given ID or None if the file
        has not been written to the bucket.

        :rtype: FileInfo
        """
        item = None
        for attempt in retry_sdb():
            with attempt:
                item = self.filesDomain.get_attributes(item_name=jobStoreFileID,
                                                       attribute_name=['version', 'fileType',
                                                                       'jobStoreID'],
                                                       consistent_read=True)
        assert item is not None
        fileType = item.get('fileType')
        if fileType is None:
            assert 'version' not in item
            return None
        else:
            return FileInfo(jobStoreFileID,
                            jobID=item.get('jobStoreID'),
                            version=item.get('version'),
                            fileType=FileType[fileType])

    _s3_part_size = 50 * 1024 * 1024

    def _upload(self, jobStoreFileID, localFilePath):
        file_size, file_time = self._fileSizeAndTime(localFilePath)
        headers = {}
        self.__add_encryption_headers(headers)
        if file_size <= self._s3_part_size:
            key = self.filesBucket.new_key(key_name=jobStoreFileID)
            key.name = jobStoreFileID
            key.set_contents_from_filename(localFilePath, headers=headers)
            version = key.version_id
        else:
            with open(localFilePath, 'rb') as f:
                upload = self.filesBucket.initiate_multipart_upload(key_name=jobStoreFileID,
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
        key = self.filesBucket.get_key(jobStoreFileID, headers=headers)

        assert key.size == file_size
        # Make resonably sure that the file wasn't touched during the upload
        assert self._fileSizeAndTime(localFilePath) == (file_size, file_time)
        return version

    @contextmanager
    def _uploadStream(self, jobStoreFileID, multipart=True, encrypted=True):
        key = self.filesBucket.new_key(key_name=jobStoreFileID)
        assert key.version_id is None
        readable_fh, writable_fh = os.pipe()
        headers = {}
        if encrypted:
            self.__add_encryption_headers(headers)
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def reader():
                    upload = self.filesBucket.initiate_multipart_upload(key_name=jobStoreFileID,
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

                def simpleReader():
                    log.debug("Using single part upload")
                    buf = StringIO(readable.read())
                    assert key.set_contents_from_file(fp=buf, headers=headers) == buf.len

                thread = ExceptionalThread(target=reader if multipart else simpleReader)
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
        key = self.filesBucket.get_key(jobStoreFileID, headers=headers)
        key.get_contents_to_filename(localFilePath, version_id=version, headers=headers)

    @contextmanager
    def _downloadStream(self, jobStoreFileID, version, encrypted=True):
        headers = {}
        if encrypted:
            self.__add_encryption_headers(headers)
        key = self.filesBucket.get_key(jobStoreFileID, headers=headers)
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

                thread = ExceptionalThread(target=writer)
                thread.start()
                yield readable
                thread.join()

    def _saveFileInfo(self, info):
        """
        Save the file info to the
        """
        attributes = dict(fileType=info.fileType.name)
        if info.version is not None:
            attributes['version'] = info.version
        if info.jobID is not None:
            attributes['jobStoreID'] = info.jobID
        # False stands for absence
        expected = ['version', False if info.oldVersion is None else info.oldVersion]
        try:
            for attempt in retry_sdb():
                with attempt:
                    assert self.filesDomain.put_attributes(item_name=info.fileID,
                                                           attributes=attributes,
                                                           expected_value=expected)
            if info.oldVersion is not None:
                self.filesBucket.delete_key(info.fileID, version_id=info.oldVersion)
        except SDBResponseError as e:
            if e.error_code == 'ConditionalCheckFailed':
                raise ConcurrentFileModificationException(info.fileID)
            else:
                raise

    def _fileSizeAndTime(self, localFilePath):
        file_stat = os.stat(localFilePath)
        file_size, file_time = file_stat.st_size, file_stat.st_mtime
        return file_size, file_time

    # FIXME: Should use Enum for this

    versionings = dict(Enabled=True, Disabled=False, Suspended=None)

    def __getBucketVersioning(self, bucket):
        """
        A valueable lesson in how to botch a simple tri-state boolean.

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
        if self.filesBucket is not None:
            for upload in self.filesBucket.list_multipart_uploads():
                upload.cancel_upload()
            if self.__getBucketVersioning(self.filesBucket) in (True, None):
                for key in list(self.filesBucket.list_versions()):
                    self.filesBucket.delete_key(key.name, version_id=key.version_id)
            else:
                for key in list(self.filesBucket.list()):
                    key.delete()
            self.filesBucket.delete()
        for domain in (self.filesDomain, self.jobsDomain):
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


FileType = Enum('FileType', 'plain stats')


class FileInfo(object):
    """
    Represents the metadata describing a file
    """

    def __init__(self, fileID, jobID=None, version=None, fileType=FileType.plain):
        """
        :type fileID: str
        :param fileID: the file's ID

        :type jobID: str
        :param jobID: optional ID of the job owning the file

        :type version: str
        :param version: the file's current version

        :type fileType: FileType
        :param fileType: the type of the file
        """
        super(FileInfo, self).__init__()
        self._fileID = fileID
        self._jobID = jobID
        self._version = version
        self._oldVersion = version
        self._fileType = fileType

    @property
    def fileID(self):
        return self._fileID

    @property
    def jobID(self):
        return self._jobID

    @property
    def fileType(self):
        return self._fileType

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        # Enforce that version can't be reset, ...
        assert version is not None
        # ... can only be set once ...
        assert self._oldVersion == self._version
        # ... and to a different value.
        assert version != self._version
        self._version = version

    @property
    def oldVersion(self):
        return self._oldVersion


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
