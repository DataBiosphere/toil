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

from bd2k.util import memoize
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
from toil.jobStores.awsJobStore.sdbUtils import SDBHelper, retry_sdb, no_such_domain, \
    sdb_unavailable
from toil.jobWrapper import JobWrapper
from toil.lib.encryption import encryptionOverhead, encrypt, decrypt

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

        def qualify(name):
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

    @property
    @memoize
    def _sseKey(self):
        if self.sseKeyPath is None:
            return None
        with open(self.sseKeyPath) as f:
            return f.read()

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
                    consistent_read=True,
                    query="select itemName() from `%s` where ownerID='%s'" % (
                        self.filesDomain.name, jobStoreID)))
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

    def getEmptyFileStoreID(self, jobStoreID=None):
        info = self.FileInfo.create(jobStoreID)
        info.save()
        log.debug("Created %r.", info)
        return info.fileID

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
    def writeSharedFileStream(self, sharedFileName, isProtected=True):
        assert self._validateSharedFileName(sharedFileName)
        info = self.FileInfo.loadOrCreate(fileID=self._sharedFileID(sharedFileName),
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
    def readSharedFileStream(self, sharedFileName, isProtected=True):
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

    def readStatsAndLogging(self, statsCallBackFn):
        itemsProcessed = 0
        items = None
        for attempt in retry_sdb():
            with attempt:
                items = list(self.filesDomain.select(
                    consistent_read=True,
                    query="select * from `%s` where ownerID='%s'" % (
                        self.filesDomain.name, str(self.statsFileOwnerID))))
        assert items is not None
        for item in items:
            info = self.FileInfo.fromItem(item)
            with info.downloadStream() as readable:
                statsCallBackFn(readable)
            self.deleteFile(item.name)
            itemsProcessed += 1
        return itemsProcessed

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

    # A dummy job ID under which all shared files are stored
    sharedFileOwnerID = uuid.UUID('891f7db6-e4d9-4221-a58e-ab6cc4395f94')

    # A dummy job ID under which all stats files are stored
    statsFileOwnerID = uuid.UUID('bfcf5286-4bc7-41ef-a85d-9ab415b69d53')

    def _sharedFileID(self, sharedFileName):
        return str(uuid.uuid5(self.sharedFileOwnerID, str(sharedFileName)))

    @InnerClass
    class FileInfo(SDBHelper):
        """
        Represents the metadata describing a file in the job store
        """
        outer = None
        """
        :type: AWSJobStore
        """

        def __init__(self, fileID, ownerID,
                     encrypted=True, version=None, content=None, numContentChunks=0):
            """
            :type fileID: str
            :param fileID: the file's ID

            :type ownerID: str
            :param ownerID: ID of the entity owning this file, typically a job ID aka jobStoreID

            :type version: str

            :param version: the most recent version of the S3 key storing the file or None if this
                            file is inlined

            :type encrypted: bool
            :param encrypted: whether the file is stored in encrypted form
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
            if version is not None:
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
                self.version = None

        @classmethod
        def create(cls, ownerID):
            return cls(str(uuid.uuid4()), ownerID)

        @classmethod
        def load(cls, jobStoreFileID):
            for attempt in retry_sdb():
                with attempt:
                    self = cls.fromItem(
                        cls.outer.filesDomain.get_attributes(item_name=jobStoreFileID,
                                                             consistent_read=True))
                    return self

        @classmethod
        def loadOrCreate(cls, fileID, ownerID, encrypted=True):
            self = cls.load(fileID)
            if self is None:
                self = cls(fileID, ownerID=ownerID, encrypted=encrypted)
            else:
                assert self.fileID == fileID
                assert self.ownerID == ownerID
                assert self.encrypted is encrypted, "Can't change encryption status of file"
            return self

        @classmethod
        def loadOrFail(cls, jobStoreFileID, customName=None):
            """
            :rtype: FileInfo
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
            :type item: Item
            """
            assert item is not None
            # ownerID and encrypted are the only mandatory attributes
            ownerID = item.get('ownerID')
            encrypted = item.get('encrypted')
            if ownerID is None:
                assert encrypted is None
                return None
            else:
                content, numContentChunks = cls.attributesToBinary(item)
                self = cls(fileID=item.name,
                           ownerID=ownerID,
                           encrypted=parse_bool(encrypted),
                           version=item['version'],
                           content=content,
                           numContentChunks=numContentChunks)
                return self

        def toAttributes(self):
            numChunks = 0
            attributes = dict(ownerID=self.ownerID,
                              encrypted=self.encrypted,
                              version=self.version or '')
            if self.content is not None:
                chunks = self.binaryToAttributes(self.content)
                numChunks = len(chunks)
                attributes.update(chunks)
            return attributes, numChunks

        def save(self):
            # False stands for absence
            expected = ['version', False if self.previousVersion is None else self.previousVersion]
            attributes, numNewContentChunks = self.toAttributes()
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
                    extraneousChunks = xrange(numNewContentChunks, self._numContentChunks)
                    attributes = [str(i).zfill(3) for i in extraneousChunks]
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

        _s3_part_size = 50 * 1024 * 1024

        def upload(self, localFilePath):
            file_size, file_time = self._fileSizeAndTime(localFilePath)
            if file_size < self.maxInlinedSize():
                with open(localFilePath) as f:
                    self._inline(f.read())
            else:
                headers = self._s3EncryptionHeaders()
                if file_size <= self._s3_part_size:
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
                        buf = readable.read(self._s3_part_size)
                        if allowInlining and len(buf) <= self.maxInlinedSize():
                            self._inline(buf)
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
                                    buf = readable.read(self._s3_part_size)
                            except:
                                upload.cancel_upload()
                                raise
                            else:
                                self.version = upload.complete_upload().version_id

                    def reader():
                        buf = readable.read()
                        if allowInlining and len(buf) <= self.maxInlinedSize():
                            self._inline(buf)
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
                assert (self.version is None) != (self.content is None)

        def download(self, localFilePath):
            if self.content is not None:
                with open(localFilePath, 'w') as f:
                    f.write(self._extract())
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
                                writable.write(self._extract())
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

        def _inline(self, content):
            """
            Inline the given content in this file info object. This will skip uploading the
            content to S3 and store along (aka inline) with the other file-related metadata in SDB.
            """
            if self.encrypted:
                sseKeyPath = self.outer.sseKeyPath
                if sseKeyPath is not None:
                    content = encrypt(content, sseKeyPath)
                else:
                    self.encrypted = False
            self.content = content

        def _extract(self):
            """
            Extract inlined content from this file info object.
            """
            content = self.content
            if self.encrypted:
                sseKeyPath = self.outer.sseKeyPath
                if sseKeyPath is not None:
                    content = decrypt(content, sseKeyPath)
            return content

        def delete(self):
            store = self.outer
            if self.previousVersion is not None:
                for attempt in retry_sdb():
                    with attempt:
                        store.filesDomain.delete_attributes(
                            self.fileID,
                            expected_values=['version', self.previousVersion])
                store.filesBucket.delete_key(key_name=self.fileID, version_id=self.previousVersion)

        @classmethod
        def _reservedAttributes(cls):
            return 3

        def maxInlinedSize(self):
            return self.maxBinarySize() - (encryptionOverhead if self.encrypted else 0)

        def _s3EncryptionHeaders(self):
            sseKey = self.outer._sseKey
            if not self.encrypted or sseKey is None:
                return {}
            else:
                assert len(sseKey) == 32
                encodedSseKey = base64.b64encode(sseKey)
                encodedSseKeyMd5 = base64.b64encode(hashlib.md5(sseKey).digest())
                return {'x-amz-server-side-encryption-customer-algorithm': 'AES256',
                        'x-amz-server-side-encryption-customer-key': encodedSseKey,
                        'x-amz-server-side-encryption-customer-key-md5': encodedSseKeyMd5}

        def _fileSizeAndTime(self, localFilePath):
            file_stat = os.stat(localFilePath)
            file_size, file_time = file_stat.st_size, file_stat.st_mtime
            return file_size, file_time

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


def parse_bool(s):
    if s == 'True':
        return True
    elif s == 'False':
        return False
    else:
        raise ValueError(s)
