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
import cPickle
import base64
import hashlib
import itertools
import repr as reprlib

from bd2k.util import strict_bool
from bd2k.util.objects import InnerClass
from bd2k.util.threading import ExceptionalThread
from boto.sdb.domain import Domain
from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection
from boto.sdb.connection import SDBConnection
from boto.sdb.item import Item
import boto.s3
import boto.sdb
from boto.exception import SDBResponseError, S3ResponseError

from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             ConcurrentFileModificationException,
                                             NoSuchFileException)
from toil.jobStores.aws.sdbUtils import (SDBHelper, retry_sdb, no_such_domain, sdb_unavailable,
                                         monkeyPatchSdbConnection)
from toil.jobWrapper import JobWrapper
import toil.lib.encryption as encryption

log = logging.getLogger(__name__)

defaultPartSize = 2 ** 20 * 50


def copyKeyMultipart(srcKey, dstBucketName, dstKeyName, headers=None):
    """
    Copies a key from a source key to a destination key in multiple parts. Note that if the
    destination key exists it will be overwritten implicitly, and if it does not exist a new
    key will be created.

    :param boto.s3.key.Key srcKey: The source key to be copied from.
    :param str dstBucketName: The name of the destination bucket for the copy.
    :param str dstKeyName: The name of the destination key that will be created or overwritten.
    :param dict headers: Any headers that should be passed.

    :rtype: boto.s3.multipart.CompletedMultiPartUpload
    :return: An object representing the completed upload.
    """
    partSize = defaultPartSize
    s3 = boto.connect_s3()
    headers = headers or {}
    totalSize = srcKey.size

    # initiate copy
    upload = s3.get_bucket(dstBucketName).initiate_multipart_upload(dstKeyName, headers=headers)
    try:
        start = 0
        partIndex = itertools.count()
        while start < totalSize:
            end = min(start + partSize, totalSize)
            upload.copy_part_from_key(src_bucket_name=srcKey.bucket.name,
                                      src_key_name=srcKey.name,
                                      part_num=next(partIndex)+1,
                                      start=start,
                                      end=end-1,
                                      headers=headers)
            start += partSize
    except:
        upload.cancel_upload()
        raise
    else:
        return upload.complete_upload()


class AWSJobStore(AbstractJobStore):
    """
    A job store that uses Amazon's S3 for file storage and SimpleDB for storing job info and
    enforcing strong consistency on the S3 file storage. There will be SDB domains for jobs and
    files and a versioned S3 bucket for file contents. Job objects are pickled, compressed,
    partitioned into chunks of 1024 bytes and each chunk is stored as a an attribute of the SDB
    item representing the job. UUIDs are used to identify jobs and files.
    """

    @classmethod
    def loadOrCreateJobStore(cls, jobStoreString, config=None, **kwargs):
        region, namePrefix = jobStoreString.split(':')
        if not cls.bucketNameRe.match(namePrefix):
            raise ValueError("Invalid name prefix '%s'. Name prefixes must contain only digits, "
                             "hyphens or lower-case letters and must not start or end in a "
                             "hyphen." % namePrefix)
        # Reserve 13 for separator and suffix
        if len(namePrefix) > cls.maxBucketNameLen - cls.maxNameLen - len(cls.nameSeparator):
            raise ValueError("Invalid name prefix '%s'. Name prefixes may not be longer than 50 "
                             "characters." % namePrefix)
        if '--' in namePrefix:
            raise ValueError("Invalid name prefix '%s'. Name prefixes may not contain "
                             "%s." % (namePrefix, cls.nameSeparator))
        return cls(region, namePrefix, config=config, **kwargs)

    # Dots in bucket names should be avoided because bucket names are used in HTTPS bucket
    # URLs where the may interfere with the certificate common name. We use a double
    # underscore as a separator instead.
    #
    bucketNameRe = re.compile(r'^[a-z0-9][a-z0-9-]+[a-z0-9]$')

    # See http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    #
    minBucketNameLen = 3
    maxBucketNameLen = 63
    maxNameLen = 10
    nameSeparator = '--'

    # Do not invoke the constructor, use the factory method above.

    def __init__(self, region, namePrefix, config=None, partSize=defaultPartSize):
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
        self.partSize = partSize

        # Check global registry domain for existence of this job store. The first time this is
        # being executed in an AWS account, the registry domain will be created on the fly.
        create = config is not None
        self.registry_domain = self._getOrCreateDomain('toil-registry')
        for attempt in retry_sdb():
            with attempt:
                attributes = self.registry_domain.get_attributes(item_name=namePrefix,
                                                                 attribute_name='exists',
                                                                 consistent_read=True)
                exists = strict_bool(attributes.get('exists', str(False)))
                self._checkJobStoreCreation(create, exists, region + ":" + namePrefix)

        def qualify(name):
            assert len(name) <= self.maxNameLen
            return self.namePrefix + self.nameSeparator + name

        self.jobsDomain = self._getOrCreateDomain(qualify('jobs'))
        self.filesDomain = self._getOrCreateDomain(qualify('files'))
        self.filesBucket = self._getOrCreateBucket(qualify('files'), versioning=True)

        # Now register this job store
        for attempt in retry_sdb():
            with attempt:
                self.registry_domain.put_attributes(item_name=namePrefix,
                                                    attributes=dict(exists='True'))

        super(AWSJobStore, self).__init__(config=config)

        self.sseKeyPath = self.config.sseKey

    def create(self, command, memory, cores, disk, preemptable, predecessorNumber=0):
        jobStoreID = self._newJobID()
        log.debug("Creating job %s for '%s'",
                  jobStoreID, '<no command>' if command is None else command)
        job = AWSJob(jobStoreID=jobStoreID, command=command,
                     memory=memory, cores=cores, disk=disk, preemptable=preemptable,
                     remainingRetryCount=self._defaultTryCount(), logJobStoreFileID=None,
                     predecessorNumber=predecessorNumber)
        for attempt in retry_sdb():
            with attempt:
                assert self.jobsDomain.put_attributes(*job.toItem())
        return job

    def exists(self, jobStoreID):
        for attempt in retry_sdb():
            with attempt:
                return bool(self.jobsDomain.get_attributes(item_name=jobStoreID,
                                                           attribute_name=[],
                                                           consistent_read=True))

    def jobs(self):
        result = None
        for attempt in retry_sdb():
            with attempt:
                result = list(self.jobsDomain.select(
                    consistent_read=True,
                    query="select * from `%s`" % self.jobsDomain.name))
        assert result is not None
        for jobItem in result:
            yield AWSJob.fromItem(jobItem)

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
                assert self.jobsDomain.put_attributes(*job.toItem())

    itemsPerBatchDelete = 25

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
                    consistent_read=True,
                    query="select version from `%s` where ownerID='%s'" % (
                        self.filesDomain.name, jobStoreID)))
        assert items is not None
        if items:
            log.debug("Deleting %d file(s) associated with job %s", len(items), jobStoreID)
            n = self.itemsPerBatchDelete
            batches = [items[i:i + n] for i in range(0, len(items), n)]
            for batch in batches:
                itemsDict = {item.name: None for item in batch}
                for attempt in retry_sdb():
                    with attempt:
                        self.filesDomain.batch_delete_attributes(itemsDict)
            for item in items:
                version = item.get('version')
                if version:
                    self.filesBucket.delete_key(key_name=item.name,
                                                version_id=version)
                else:
                    self.filesBucket.delete_key(key_name=item.name)

    def getEmptyFileStoreID(self, jobStoreID=None):
        info = self.FileInfo.create(jobStoreID)
        info.save()
        log.debug("Created %r.", info)
        return info.fileID

    def _importFile(self, otherCls, url):
        if issubclass(otherCls, AWSJobStore):
            srcBucket, srcKey = self._extractKeyInfoFromUrl(url, existing=True)
            info = self.FileInfo.create(srcKey.name)
            info.copyFrom(srcKey)
            info.save()
            return info.fileID
        else:
            return super(AWSJobStore, self)._importFile(otherCls, url)

    def _exportFile(self, otherCls, jobStoreFileID, url):
        if issubclass(otherCls, AWSJobStore):
            dstBucket, newKey = self._extractKeyInfoFromUrl(url)
            info = self.FileInfo.loadOrFail(jobStoreFileID)
            info.copyTo(newKey.name, dstBucket)
        else:
            super(AWSJobStore, self)._exportFile(otherCls, jobStoreFileID, url)

    @classmethod
    def _readFromUrl(cls, url, writable):
        srcBucket, srcKey = cls._extractKeyInfoFromUrl(url, existing=True)
        srcKey.get_contents_to_file(writable)

    @classmethod
    def _writeToUrl(cls, readable, url):
        dstBucket, dstKey = cls._extractKeyInfoFromUrl(url)
        dstKey.set_contents_from_string(readable.read())

    @staticmethod
    def _extractKeyInfoFromUrl(url, existing=None):
        """
        Extracts bucket and key from URL. The existing parameter determines if a
        particular state of existence should be enforced. Note also that if existing
        is not True and the key does not exist.

        :param existing: determines what the state
        :return: (bucket, key)
        """
        s3 = boto.connect_s3()
        bucket = s3.get_bucket(url.netloc)
        key = bucket.get_key(url.path[1:])

        if existing is True:
            if key is None:
                raise RuntimeError('Key does not exist.')
        elif existing is False:
            if key is not None:
                raise RuntimeError('Key exists.')
        elif existing is None:
            pass
        else:
            assert False

        if key is None:
            key = bucket.new_key(url.path[1:])

        return bucket, key

    @classmethod
    def _supportsUrl(cls, url):
        return url.scheme.lower() == 's3'

    def writeFile(self, localFilePath, jobStoreID=None):
        info = self.FileInfo.create(jobStoreID)
        info.upload(localFilePath)
        info.save()
        log.debug("Wrote %r of from %r", info, localFilePath)
        return info.fileID

    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        info = self.FileInfo.create(jobStoreID)
        with info.uploadStream() as writable:
            yield writable, info.fileID
        info.save()
        log.debug("Wrote %r.", info)

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        assert self._validateSharedFileName(sharedFileName)
        info = self.FileInfo.loadOrCreate(jobStoreFileID=self._sharedFileID(sharedFileName),
                                          ownerID=str(self.sharedFileOwnerID),
                                          encrypted=isProtected)
        with info.uploadStream() as writable:
            yield writable
        info.save()
        log.debug("Wrote %r for shared file %r.", info, sharedFileName)

    def updateFile(self, jobStoreFileID, localFilePath):
        info = self.FileInfo.loadOrFail(jobStoreFileID)
        info.upload(localFilePath)
        info.save()
        log.debug("Wrote %r from path %r.", info, localFilePath)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        info = self.FileInfo.loadOrFail(jobStoreFileID)
        with info.uploadStream() as writable:
            yield writable
        info.save()
        log.debug("Wrote %r from stream.", info)

    def fileExists(self, jobStoreFileID):
        return self.FileInfo.load(jobStoreFileID) is not None

    def readFile(self, jobStoreFileID, localFilePath):
        info = self.FileInfo.loadOrFail(jobStoreFileID)
        log.debug("Reading %r into %r.", info, localFilePath)
        info.download(localFilePath)

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        info = self.FileInfo.loadOrFail(jobStoreFileID)
        log.debug("Reading %r into stream.", info)
        with info.downloadStream() as readable:
            yield readable

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        assert self._validateSharedFileName(sharedFileName)
        jobStoreFileID = self._sharedFileID(sharedFileName)
        info = self.FileInfo.loadOrFail(jobStoreFileID, customName=sharedFileName)
        log.debug("Reading %r for shared file %r into stream.", info, sharedFileName)
        with info.downloadStream() as readable:
            yield readable

    def deleteFile(self, jobStoreFileID):
        info = self.FileInfo.load(jobStoreFileID)
        if info is None:
            log.debug("File %s does not exist, skipping deletion.", jobStoreFileID)
        else:
            info.delete()

    def writeStatsAndLogging(self, statsAndLoggingString):
        info = self.FileInfo.create(str(self.statsFileOwnerID))
        with info.uploadStream(multipart=False) as writeable:
            writeable.write(statsAndLoggingString)
        info.save()

    def readStatsAndLogging(self, callback, readAll=False):
        itemsProcessed = 0

        for info in self._readStatsAndLogging(callback, self.statsFileOwnerID):
            info._ownerID = self.readStatsFileOwnerID
            info.save()
            itemsProcessed += 1

        if readAll:
            for _ in self._readStatsAndLogging(callback, self.readStatsFileOwnerID):
                itemsProcessed += 1

        return itemsProcessed

    def _readStatsAndLogging(self, callback, ownerId):
        items = None
        for attempt in retry_sdb():
            with attempt:
                items = list(self.filesDomain.select(
                    consistent_read=True,
                    query="select * from `%s` where ownerID='%s'" % (
                        self.filesDomain.name, str(ownerId))))
        assert items is not None
        for item in items:
            info = self.FileInfo.fromItem(item)
            with info.downloadStream() as readable:
                callback(readable)
            yield info

    def getPublicUrl(self, jobStoreFileID):
        info = self.FileInfo.loadOrFail(jobStoreFileID)
        if info.content is not None:
            with info.uploadStream(allowInlining=False) as f:
                f.write(info.content)
        key = self.filesBucket.get_key(key_name=jobStoreFileID, version_id=info.version)
        return key.generate_url(expires_in=self.publicUrlExpiration.total_seconds())

    def getSharedPublicUrl(self, sharedFileName):
        assert self._validateSharedFileName(sharedFileName)
        return self.getPublicUrl(self._sharedFileID(sharedFileName))

    def _connectSimpleDB(self):
        """
        :rtype: SDBConnection
        """
        db = boto.sdb.connect_to_region(self.region)
        if db is None:
            raise ValueError("Could not connect to SimpleDB. Make sure '%s' is a valid SimpleDB "
                             "region." % self.region)
        assert db is not None
        monkeyPatchSdbConnection(db)
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
        assert self.minBucketNameLen <= len(bucket_name) <= self.maxBucketNameLen
        assert self.bucketNameRe.match(bucket_name)
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

    # A dummy job ID under which all shared files are stored
    sharedFileOwnerID = uuid.UUID('891f7db6-e4d9-4221-a58e-ab6cc4395f94')

    # A dummy job ID under which all unread stats files are stored
    statsFileOwnerID = uuid.UUID('bfcf5286-4bc7-41ef-a85d-9ab415b69d53')

    # A dummy job ID under which all read stats files are stored
    readStatsFileOwnerID = uuid.UUID('e77fc3aa-d232-4255-ae04-f64ee8eb0bfa')

    def _sharedFileID(self, sharedFileName):
        return str(uuid.uuid5(self.sharedFileOwnerID, str(sharedFileName)))

    @InnerClass
    class FileInfo(SDBHelper):
        """
        Represents a file in this job store.
        """
        outer = None
        """
        :type: AWSJobStore
        """

        def __init__(self, fileID, ownerID, encrypted,
                     version=None, content=None, numContentChunks=0):
            """
            :type fileID: str
            :param fileID: the file's ID

            :type ownerID: str
            :param ownerID: ID of the entity owning this file, typically a job ID aka jobStoreID

            :type encrypted: bool
            :param encrypted: whether the file is stored in encrypted form

            :type version: str|None
            :param version: a non-empty string containing the most recent version of the S3
            object storing this file's content, None if the file is new, or empty string if the
            file is inlined.

            :type content: str|None
            :param content: this file's inlined content

            :type numContentChunks: int
            :param numContentChunks: the number of SDB domain attributes occupied by this files
            inlined content. Note that an inlined empty string still occupies one chunk.
            """
            super(AWSJobStore.FileInfo, self).__init__()
            self._fileID = fileID
            self._ownerID = ownerID
            self.encrypted = encrypted
            self._version = version
            self._previousVersion = version
            self._content = content
            self._numContentChunks = numContentChunks

        @property
        def fileID(self):
            return self._fileID

        @property
        def ownerID(self):
            return self._ownerID

        @property
        def version(self):
            return self._version

        @version.setter
        def version(self, version):
            # Version should only change once
            assert self._previousVersion == self._version
            self._version = version
            if version:
                self.content = None

        @property
        def previousVersion(self):
            return self._previousVersion

        @property
        def content(self):
            return self._content

        @content.setter
        def content(self, content):
            self._content = content
            if content is not None:
                self.version = ''

        @classmethod
        def create(cls, ownerID):
            return cls(str(uuid.uuid4()), ownerID, encrypted=cls.outer.sseKeyPath is not None)

        @classmethod
        def load(cls, jobStoreFileID):
            for attempt in retry_sdb():
                with attempt:
                    self = cls.fromItem(
                        cls.outer.filesDomain.get_attributes(item_name=jobStoreFileID,
                                                             consistent_read=True))
                    return self

        @classmethod
        def loadOrCreate(cls, jobStoreFileID, ownerID, encrypted):
            self = cls.load(jobStoreFileID)
            if encrypted is None:
                encrypted = cls.outer.sseKeyPath is not None
            if self is None:
                self = cls(jobStoreFileID, ownerID, encrypted=encrypted)
            else:
                assert self.fileID == jobStoreFileID
                assert self.ownerID == ownerID
                self.encrypted = encrypted
            return self

        @classmethod
        def loadOrFail(cls, jobStoreFileID, customName=None):
            """
            :rtype: FileInfo
            :return: an instance of this class representing the file with the given ID
            :raises NoSuchFileException: if given file does not exist
            """
            self = cls.load(jobStoreFileID)
            if self is None:
                raise NoSuchFileException(jobStoreFileID, customName=customName)
            else:
                return self

        @classmethod
        def fromItem(cls, item):
            """
            Convert an SDB item to an instance of this class.

            :type item: Item
            """
            assert item is not None
            # Strings come back from SDB as unicode
            def strOrNone(s):
                return s if s is None else str(s)

            # ownerID and encrypted are the only mandatory attributes
            ownerID = strOrNone(item.get('ownerID'))
            encrypted = item.get('encrypted')
            if ownerID is None:
                assert encrypted is None
                return None
            else:
                version = strOrNone(item['version'])
                encrypted = strict_bool(encrypted)
                content, numContentChunks = cls.attributesToBinary(item)
                if encrypted:
                    sseKeyPath = cls.outer.sseKeyPath
                    if sseKeyPath is None:
                        raise AssertionError('Content is encrypted but no key was provided.')
                    if content is not None:
                        content = encryption.decrypt(content, sseKeyPath)
                self = cls(fileID=item.name, ownerID=ownerID, encrypted=encrypted, version=version,
                           content=content, numContentChunks=numContentChunks)
                return self

        def toItem(self):
            """
            Convert this instance to an attribute dictionary suitable for SDB put_attributes().

            :rtype: (dict,int)

            :return: the attributes dict and an integer specifying the the number of chunk
                     attributes in the dictionary that are used for storing inlined content.
            """
            if self.content is None:
                numChunks = 0
                attributes = {}
            else:
                content = self.content
                if self.encrypted:
                    sseKeyPath = self.outer.sseKeyPath
                    if sseKeyPath is None:
                        raise AssertionError('Encryption requested but no key was provided.')
                    content = encryption.encrypt(content, sseKeyPath)
                attributes = self.binaryToAttributes(content)
                numChunks = len(attributes)
            attributes.update(dict(ownerID=self.ownerID,
                                   encrypted=self.encrypted,
                                   version=self.version or ''))
            return attributes, numChunks

        @classmethod
        def _reservedAttributes(cls):
            return 3

        @classmethod
        def maxInlinedSize(cls, encrypted):
            return cls.maxBinarySize() - (encryption.overhead if encrypted else 0)

        def _maxInlinedSize(self):
            return self.maxInlinedSize(self.encrypted)

        def save(self):
            attributes, numNewContentChunks = self.toItem()
            # False stands for absence
            expected = ['version', False if self.previousVersion is None else self.previousVersion]
            try:
                for attempt in retry_sdb():
                    with attempt:
                        assert self.outer.filesDomain.put_attributes(item_name=self.fileID,
                                                                     attributes=attributes,
                                                                     expected_value=expected)
                if self.previousVersion:
                    self.outer.filesBucket.delete_key(self.fileID, version_id=self.previousVersion)
                self._previousVersion = self._version
                if numNewContentChunks < self._numContentChunks:
                    residualChunks = xrange(numNewContentChunks, self._numContentChunks)
                    attributes = [self._chunkName(i) for i in residualChunks]
                    for attempt in retry_sdb():
                        with attempt:
                            self.outer.filesDomain.delete_attributes(self.fileID,
                                                                     attributes=attributes)
                self._numContentChunks = numNewContentChunks
            except SDBResponseError as e:
                if e.error_code == 'ConditionalCheckFailed':
                    raise ConcurrentFileModificationException(self.fileID)
                else:
                    raise

        def upload(self, localFilePath):
            file_size, file_time = self._fileSizeAndTime(localFilePath)
            if file_size <= self._maxInlinedSize():
                with open(localFilePath) as f:
                    self.content = f.read()
            else:
                headers = self._s3EncryptionHeaders()
                if file_size <= self.outer.partSize:
                    key = self.outer.filesBucket.new_key(key_name=self.fileID)
                    key.name = self.fileID
                    key.set_contents_from_filename(localFilePath, headers=headers)
                    self.version = key.version_id
                else:
                    with open(localFilePath, 'rb') as f:
                        upload = self.outer.filesBucket.initiate_multipart_upload(
                            key_name=self.fileID,
                            headers=headers)
                        try:
                            start = 0
                            part_num = itertools.count()
                            while start < file_size:
                                end = min(start + self.outer.partSize, file_size)
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
                            self.version = upload.complete_upload().version_id
                key = self.outer.filesBucket.get_key(self.fileID,
                                                     headers=headers,
                                                     version_id=self.version)
                assert key.size == file_size
                # Make resonably sure that the file wasn't touched during the upload
                assert self._fileSizeAndTime(localFilePath) == (file_size, file_time)

        @contextmanager
        def uploadStream(self, multipart=True, allowInlining=True):
            store = self.outer
            readable_fh, writable_fh = os.pipe()
            with os.fdopen(readable_fh, 'r') as readable:
                with os.fdopen(writable_fh, 'w') as writable:
                    def multipartReader():
                        buf = readable.read(self.outer.partSize)
                        if allowInlining and len(buf) <= self._maxInlinedSize():
                            self.content = buf
                        else:
                            headers = self._s3EncryptionHeaders()
                            upload = store.filesBucket.initiate_multipart_upload(
                                key_name=self.fileID,
                                headers=headers)
                            try:
                                for part_num in itertools.count():
                                    # There must be at least one part, even if the file is empty.
                                    if len(buf) == 0 and part_num > 0: break
                                    upload.upload_part_from_file(fp=StringIO(buf),
                                                                 # S3 part numbers are 1-based
                                                                 part_num=part_num + 1,
                                                                 headers=headers)
                                    if len(buf) == 0: break
                                    buf = readable.read(self.outer.partSize)
                            except:
                                upload.cancel_upload()
                                raise
                            else:
                                self.version = upload.complete_upload().version_id

                    def reader():
                        buf = readable.read()
                        if allowInlining and len(buf) <= self._maxInlinedSize():
                            self.content = buf
                        else:
                            key = store.filesBucket.new_key(key_name=self.fileID)
                            buf = StringIO(buf)
                            headers = self._s3EncryptionHeaders()
                            assert buf.len == key.set_contents_from_file(fp=buf, headers=headers)
                            self.version = key.version_id

                    thread = ExceptionalThread(target=multipartReader if multipart else reader)
                    thread.start()
                    yield writable
                # The writable is now closed. This will send EOF to the readable and cause that
                # thread to finish.
                thread.join()
                assert bool(self.version) == (self.content is None)

        def copyFrom(self, srcKey):
            """
            Copies contents of source key into file.

            :param srcKey: The key that will be copied from
            """
            size = srcKey.bucket.get_key(srcKey.name).size
            if size <= self._maxInlinedSize():
                self.content = srcKey.get_contents_as_string()
            else:
                self.version = self._copyKey(srcKey=srcKey,
                                             dstBucketName=self.outer.filesBucket.name,
                                             dstKeyName=self._fileID,
                                             headers=self._s3EncryptionHeaders()).version_id

        def copyTo(self, dstKeyName, dstBucket):
            """
            Copies contents of file to destination key.

            :param str newKeyName: The name of the new key to be created or copied to from the file.
            :param Bucket dstBucket: The bucket that contains or will contain the destination key.
            """
            dstKey = dstBucket.new_key(dstKeyName)

            if self.content is not None:
                dstKey.set_contents_from_string(self.content)

            elif self.version:
                srcKey = self.outer.filesBucket.get_key(self.fileID, headers=self._s3EncryptionHeaders())
                self._copyKey(srcKey=srcKey,
                              dstBucketName=dstBucket.name,
                              dstKeyName=dstKeyName,
                              headers={"amz-copy-source".join(k.split('amz')): v
                                       for k, v in self._s3EncryptionHeaders().items()})
            else:
                assert False

        def _copyKey(self, srcKey, dstBucketName, dstKeyName, headers=None):
            headers = headers or {}
            s3 = boto.connect_s3()

            if srcKey.size > self.outer.partSize:
                return copyKeyMultipart(srcKey=srcKey,
                                        dstBucketName=dstBucketName,
                                        dstKeyName=dstKeyName,
                                        headers=headers)
            else:
                dstBucket = s3.get_bucket(dstBucketName)
                return dstBucket.copy_key(new_key_name=dstKeyName,
                                          src_bucket_name=srcKey.bucket.name,
                                          src_key_name=srcKey.name,
                                          metadata=srcKey.metadata,
                                          headers=headers)

        def _hashesAreEqual(self, srcHash, dstHash):
            if self.encrypted:
                log.debug("MD5 hash of imported or exported files cannot be checked because it is encrypted."
                          % self._fileID)
                return True
            else:
                return dstHash == srcHash

        def download(self, localFilePath):
            if self.content is not None:
                with open(localFilePath, 'w') as f:
                    f.write(self.content)
            elif self.version:
                headers = self._s3EncryptionHeaders()
                key = self.outer.filesBucket.get_key(self.fileID, validate=False)
                key.get_contents_to_filename(localFilePath,
                                             version_id=self.version,
                                             headers=headers)
            else:
                assert False

        @contextmanager
        def downloadStream(self):
            readable_fh, writable_fh = os.pipe()
            with os.fdopen(readable_fh, 'r') as readable:
                with os.fdopen(writable_fh, 'w') as writable:
                    def writer():
                        try:
                            if self.content is not None:
                                writable.write(self.content)
                            elif self.version:
                                headers = self._s3EncryptionHeaders()
                                key = self.outer.filesBucket.get_key(self.fileID, validate=False)
                                key.get_contents_to_file(writable,
                                                         headers=headers,
                                                         version_id=self.version)
                            else:
                                assert False
                        finally:
                            # This close() will send EOF to the reading end and ultimately cause
                            # the yield to return. It also makes the implict .close() done by the
                            #  enclosing "with" context redundant but that should be ok since
                            # .close() on file objects are idempotent.
                            writable.close()

                    thread = ExceptionalThread(target=writer)
                    thread.start()
                    yield readable
                    thread.join()

        def delete(self):
            store = self.outer
            if self.previousVersion is not None:
                for attempt in retry_sdb():
                    with attempt:
                        store.filesDomain.delete_attributes(
                            self.fileID,
                            expected_values=['version', self.previousVersion])
                if self.previousVersion:
                    store.filesBucket.delete_key(key_name=self.fileID,
                                                 version_id=self.previousVersion)

        def _s3EncryptionHeaders(self):
            sseKeyPath = self.outer.sseKeyPath
            if self.encrypted:
                if sseKeyPath is None:
                    raise AssertionError('Content is encrypted but no key was provided.')
                else:
                    with open(sseKeyPath) as f:
                        sseKey = f.read()
                    assert len(sseKey) == 32
                    encodedSseKey = base64.b64encode(sseKey)
                    encodedSseKeyMd5 = base64.b64encode(hashlib.md5(sseKey).digest())
                    return {'x-amz-server-side-encryption-customer-algorithm': 'AES256',
                            'x-amz-server-side-encryption-customer-key': encodedSseKey,
                            'x-amz-server-side-encryption-customer-key-md5': encodedSseKeyMd5}
            else:
                return {}

        def _fileSizeAndTime(self, localFilePath):
            file_stat = os.stat(localFilePath)
            file_size, file_time = file_stat.st_size, file_stat.st_mtime
            return file_size, file_time

        def __repr__(self):
            r = custom_repr
            d = (('fileID', r(self.fileID)),
                 ('ownerID', r(self.ownerID)),
                 ('encrypted', r(self.encrypted)),
                 ('version', r(self.version)),
                 ('previousVersion', r(self.previousVersion)),
                 ('content', r(self.content)),
                 ('_numContentChunks', r(self._numContentChunks)))
            return "{}({})".format(type(self).__name__,
                                   ', '.join('%s=%s' % (k, v) for k, v in d))

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


aRepr = reprlib.Repr()
aRepr.maxstring = 38  # so UUIDs don't get truncated (36 for UUID plus 2 for quotes)
custom_repr = aRepr.repr


class AWSJob(JobWrapper, SDBHelper):
    """
    A Job that can be converted to and from an SDB item.
    """

    @classmethod
    def fromItem(cls, item):
        """
        :type item: Item
        :rtype: AWSJob
        """
        binary, _ = cls.attributesToBinary(item)
        assert binary is not None
        return cPickle.loads(binary)

    def toItem(self):
        """
        To to a peculiarity of Boto's SDB bindings, this method does not return an Item,
        but a tuple. The returned tuple can be used with put_attributes like so

        domain.put_attributes( *toItem(...) )

        :rtype: (str,dict)
        :return: a str for the item's name and a dictionary for the item's attributes
        """
        return self.jobStoreID, self.binaryToAttributes(cPickle.dumps(self))
