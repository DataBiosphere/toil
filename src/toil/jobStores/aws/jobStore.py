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

from StringIO import StringIO
from contextlib import contextmanager, closing
import logging
from multiprocessing import cpu_count

import os
import re
import uuid
import cPickle
import base64
import hashlib
import itertools
import repr as reprlib

from bd2k.util import strict_bool
from bd2k.util.exceptions import panic
from bd2k.util.objects import InnerClass
from boto.sdb.domain import Domain
from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection
from boto.sdb.connection import SDBConnection
from boto.sdb.item import Item
import boto.s3
import boto.sdb
from boto.exception import S3CreateError
from boto.s3.key import Key
from boto.exception import SDBResponseError, S3ResponseError
from concurrent.futures import ThreadPoolExecutor

from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             NoSuchJobException,
                                             ConcurrentFileModificationException,
                                             NoSuchFileException,
                                             NoSuchJobStoreException,
                                             JobStoreExistsException)
from toil.jobStores.aws.utils import (SDBHelper,
                                      retry_sdb,
                                      no_such_sdb_domain,
                                      sdb_unavailable,
                                      monkeyPatchSdbConnection,
                                      retry_s3,
                                      bucket_location_to_region,
                                      region_to_bucket_location)
from toil.jobStores.utils import WritablePipe, ReadablePipe
from toil.jobWrapper import JobWrapper
import toil.lib.encryption as encryption

log = logging.getLogger(__name__)


def copyKeyMultipart(srcKey, dstBucketName, dstKeyName, partSize, headers=None):
    """
    Copies a key from a source key to a destination key in multiple parts. Note that if the
    destination key exists it will be overwritten implicitly, and if it does not exist a new
    key will be created. If the destination bucket does not exist an error will be raised.

    :param boto.s3.key.Key srcKey: The source key to be copied from.
    :param str dstBucketName: The name of the destination bucket for the copy.
    :param str dstKeyName: The name of the destination key that will be created or overwritten.
    :param int partSize: The size of each individual part, must be >= 5 MiB but large enough to
           not exceed 10k parts for the whole file
    :param dict headers: Any headers that should be passed.

    :rtype: boto.s3.multipart.CompletedMultiPartUpload
    :return: An object representing the completed upload.
    """

    def copyPart(partIndex):
        if exceptions:
            return None
        try:
            for attempt in retry_s3():
                with attempt:
                    start = partIndex * partSize
                    end = min(start + partSize, totalSize)
                    part = upload.copy_part_from_key(src_bucket_name=srcKey.bucket.name,
                                                     src_key_name=srcKey.name,
                                                     src_version_id=srcKey.version_id,
                                                     # S3 part numbers are 1-based
                                                     part_num=partIndex + 1,
                                                     # S3 range intervals are closed at the end
                                                     start=start, end=end - 1,
                                                     headers=headers)
        except Exception as e:
            if len(exceptions) < 5:
                exceptions.append(e)
                log.error('Failed to copy part number %d:', partIndex, exc_info=True)
            else:
                log.warn('Also failed to copy part number %d due to %s.', partIndex, e)
            return None
        else:
            log.debug('Successfully copied part %d of %d.', partIndex, totalParts)
            # noinspection PyUnboundLocalVariable
            return part

    totalSize = srcKey.size
    totalParts = (totalSize + partSize - 1) / partSize
    exceptions = []
    # We need a location-agnostic connection to S3 so we can't use the one that we
    # normally use for interacting with the job store bucket.
    with closing(boto.connect_s3()) as s3:
        for attempt in retry_s3():
            with attempt:
                dstBucket = s3.get_bucket(dstBucketName)
                upload = dstBucket.initiate_multipart_upload(dstKeyName, headers=headers)
        log.info("Initiated multipart copy from 's3://%s/%s' to 's3://%s/%s'.",
                 srcKey.bucket.name, srcKey.name, dstBucketName, dstKeyName)
        try:
            # We can oversubscribe cores by at least a factor of 16 since each copy task just
            # blocks, waiting on the server. Limit # of threads to 128, since threads aren't
            # exactly free either. Lastly, we don't need more threads than we have parts.
            with ThreadPoolExecutor(max_workers=min(cpu_count() * 16, totalParts, 128)) as executor:
                parts = list(executor.map(copyPart, xrange(0, totalParts)))
                if exceptions:
                    raise RuntimeError('Failed to copy at least %d part(s)' % len(exceptions))
                assert len(filter(None, parts)) == totalParts
        except:
            with panic(log=log):
                upload.cancel_upload()
        else:
            for attempt in retry_s3():
                with attempt:
                    completed = upload.complete_upload()
                    log.info("Completed copy from 's3://%s/%s' to 's3://%s/%s'.",
                             srcKey.bucket.name, srcKey.name, dstBucketName, dstKeyName)
                    return completed


class AWSJobStore(AbstractJobStore):
    """
    A job store that uses Amazon's S3 for file storage and SimpleDB for storing job info and
    enforcing strong consistency on the S3 file storage. There will be SDB domains for jobs and
    files and a versioned S3 bucket for file contents. Job objects are pickled, compressed,
    partitioned into chunks of 1024 bytes and each chunk is stored as a an attribute of the SDB
    item representing the job. UUIDs are used to identify jobs and files.
    """

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

    def __init__(self, locator, partSize=50 << 20):
        """
        Create a new job store in AWS or load an existing one from there.

        :param int partSize: The size of each individual part used for multipart operations like
               upload and copy, must be >= 5 MiB but large enough to not exceed 10k parts for the
               whole file
        """
        super(AWSJobStore, self).__init__()
        region, namePrefix = locator.split(':')
        if not self.bucketNameRe.match(namePrefix):
            raise ValueError("Invalid name prefix '%s'. Name prefixes must contain only digits, "
                             "hyphens or lower-case letters and must not start or end in a "
                             "hyphen." % namePrefix)
        # Reserve 13 for separator and suffix
        if len(namePrefix) > self.maxBucketNameLen - self.maxNameLen - len(self.nameSeparator):
            raise ValueError("Invalid name prefix '%s'. Name prefixes may not be longer than 50 "
                             "characters." % namePrefix)
        if '--' in namePrefix:
            raise ValueError("Invalid name prefix '%s'. Name prefixes may not contain "
                             "%s." % (namePrefix, self.nameSeparator))
        log.debug("Instantiating %s for region %s and name prefix '%s'",
                  self.__class__, region, namePrefix)
        self.locator = locator
        self.region = region
        self.namePrefix = namePrefix
        self.partSize = partSize
        self.jobsDomain = None
        self.filesDomain = None
        self.filesBucket = None
        self.db = self._connectSimpleDB()
        self.s3 = self._connectS3()

    def initialize(self, config):
        if self._registered:
            raise JobStoreExistsException(self.locator)
        self._registered = None
        self._bind(create=True)
        super(AWSJobStore, self).initialize(config)
        # Only register after job store has been full initialized
        self._registered = True

    @property
    def sseKeyPath(self):
        return self.config.sseKey

    def resume(self):
        if not self._registered:
            raise NoSuchJobStoreException(self.locator)
        self._bind(create=False)
        super(AWSJobStore, self).resume()

    def _bind(self, create=False, block=True):
        def qualify(name):
            assert len(name) <= self.maxNameLen
            return self.namePrefix + self.nameSeparator + name

        if self.jobsDomain is None:
            self.jobsDomain = self._bindDomain(qualify('jobs'), create=create, block=block)
        if self.filesDomain is None:
            self.filesDomain = self._bindDomain(qualify('files'), create=create, block=block)
        if self.filesBucket is None:
            self.filesBucket = self._bindBucket(qualify('files'),
                                                create=create,
                                                block=block,
                                                versioning=True)

    @property
    def _registered(self):
        """
        A optional boolean property indidcating whether this job store is registered. The
        registry is the authority on deciding if a job store exists or not. If True, this job
        store exists, if None the job store is transitioning from True to False or vice versa,
        if False the job store doesn't exist.

        :type: bool|None
        """
        # The weird mapping of the SDB item attribute value to the property value is due to
        # backwards compatibility. 'True' becomes True, that's easy. Toil < 3.3.0 writes this at
        # the end of job store creation. Absence of either the registry, the item or the
        # attribute becomes False, representing a truly absent, non-existing job store. An
        # attribute value of 'False', which is what Toil < 3.3.0 writes at the *beginning* of job
        # store destruction, indicates a job store in transition, reflecting the fact that 3.3.0
        # may leak buckets or domains even though the registry reports 'False' for them. We
        # can't handle job stores that were partially created by 3.3.0, though.
        registry_domain = self._bindDomain(domain_name='toil-registry',
                                           create=False,
                                           block=False)
        if registry_domain is None:
            return False
        else:
            for attempt in retry_sdb():
                with attempt:
                    attributes = registry_domain.get_attributes(item_name=self.namePrefix,
                                                                attribute_name='exists',
                                                                consistent_read=True)
                    try:
                        exists = attributes['exists']
                    except KeyError:
                        return False
                    else:
                        if exists == 'True':
                            return True
                        elif exists == 'False':
                            return None
                        else:
                            assert False

    @_registered.setter
    def _registered(self, value):

        registry_domain = self._bindDomain(domain_name='toil-registry',
                                           # Only create registry domain when registering or
                                           # transitioning a store
                                           create=value is not False,
                                           block=False)
        if registry_domain is None and value is False:
            pass
        else:
            for attempt in retry_sdb():
                with attempt:
                    if value is False:
                        registry_domain.delete_attributes(item_name=self.namePrefix)
                    else:
                        if value is True:
                            attributes = dict(exists='True')
                        elif value is None:
                            attributes = dict(exists='False')
                        else:
                            assert False
                        registry_domain.put_attributes(item_name=self.namePrefix,
                                                       attributes=attributes)

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
                return bool(self.jobsDomain.get_attributes(
                    item_name=jobStoreID,
                    attribute_name=[AWSJob.presenceIndicator()],
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
                for attempt in retry_s3():
                    with attempt:
                        if version:
                            self.filesBucket.delete_key(key_name=item.name, version_id=version)
                        else:
                            self.filesBucket.delete_key(key_name=item.name)

    def getEmptyFileStoreID(self, jobStoreID=None):
        info = self.FileInfo.create(jobStoreID)
        info.save()
        log.debug("Created %r.", info)
        return info.fileID

    def _importFile(self, otherCls, url, sharedFileName=None):
        if issubclass(otherCls, AWSJobStore):
            srcKey = self._getKeyForUrl(url, existing=True)
            try:
                if sharedFileName is None:
                    info = self.FileInfo.create(srcKey.name)
                else:
                    self._requireValidSharedFileName(sharedFileName)
                    jobStoreFileID = self._sharedFileID(sharedFileName)
                    info = self.FileInfo.loadOrCreate(jobStoreFileID=jobStoreFileID,
                                                      ownerID=str(self.sharedFileOwnerID),
                                                      encrypted=None)
                info.copyFrom(srcKey)
                info.save()
            finally:
                srcKey.bucket.connection.close()
            return info.fileID if sharedFileName is None else None
        else:
            return super(AWSJobStore, self)._importFile(otherCls, url,
                                                        sharedFileName=sharedFileName)

    def _exportFile(self, otherCls, jobStoreFileID, url):
        if issubclass(otherCls, AWSJobStore):
            dstKey = self._getKeyForUrl(url)
            try:
                info = self.FileInfo.loadOrFail(jobStoreFileID)
                info.copyTo(dstKey)
            finally:
                dstKey.bucket.connection.close()
        else:
            super(AWSJobStore, self)._exportFile(otherCls, jobStoreFileID, url)

    @classmethod
    def _readFromUrl(cls, url, writable):
        srcKey = cls._getKeyForUrl(url, existing=True)
        try:
            srcKey.get_contents_to_file(writable)
        finally:
            srcKey.bucket.connection.close()

    @classmethod
    def _writeToUrl(cls, readable, url):
        dstKey = cls._getKeyForUrl(url)
        try:
            dstKey.set_contents_from_string(readable.read())
        finally:
            dstKey.bucket.connection.close()

    @staticmethod
    def _getKeyForUrl(url, existing=None):
        """
        Extracts a key from a given s3:// URL. On return, but not on exceptions, this method
        leaks an S3Connection object. The caller is responsible to close that by calling
        key.bucket.connection.close().

        :param bool existing: If True, key is expected to exist. If False, key is expected not to
               exists and it will be created. If None, the key will be created if it doesn't exist.

        :rtype: Key
        """
        # Get the bucket's region to avoid a redirect per request
        try:
            with closing(boto.connect_s3()) as s3:
                location = s3.get_bucket(url.netloc).get_location()
                region = bucket_location_to_region(location)
        except S3ResponseError as e:
            if e.error_code == 'AccessDenied':
                log.warn("Could not determine location of bucket hosting URL '%s', reverting "
                         "to generic S3 endpoint.", url.geturl())
                s3 = boto.connect_s3()
            else:
                raise
        else:
            # Note that caller is responsible for closing the connection
            s3 = boto.s3.connect_to_region(region)

        try:
            keyName = url.path[1:]
            bucketName = url.netloc
            bucket = s3.get_bucket(bucketName)
            key = bucket.get_key(keyName)
            if existing is True:
                if key is None:
                    raise RuntimeError("Key '%s' does not exist in bucket '%s'." %
                                       (keyName, bucketName))
            elif existing is False:
                if key is not None:
                    raise RuntimeError("Key '%s' exists in bucket '%s'." %
                                       (keyName, bucketName))
            elif existing is None:
                pass
            else:
                assert False
            if key is None:
                key = bucket.new_key(keyName)
        except:
            with panic():
                s3.close()
        else:
            return key

    @classmethod
    def _supportsUrl(cls, url, export=False):
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
        return self.FileInfo.exists(jobStoreFileID)

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
        for attempt in retry_s3():
            with attempt:
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

    def _bindBucket(self, bucket_name, create=False, block=True, versioning=False):
        """
        Return the Boto Bucket object representing the S3 bucket with the given name. If the
        bucket does not exist and `create` is True, it will be created.

        :param str bucket_name: the name of the bucket to bind to

        :param bool create: Whether to create bucket the if it doesn't exist

        :param bool block: If False, return None if the bucket doesn't exist. If True, wait until
               bucket appears. Ignored if `create` is True.

        :rtype: Bucket|None
        :raises S3ResponseError: If `block` is True and the bucket still doesn't exist after the
                retry timeout expires.
        """
        assert self.minBucketNameLen <= len(bucket_name) <= self.maxBucketNameLen
        assert self.bucketNameRe.match(bucket_name)
        log.info("Binding to job store bucket '%s'.", bucket_name)

        def bucket_creation_pending(e):
            # https://github.com/BD2KGenomics/toil/issues/955
            # https://github.com/BD2KGenomics/toil/issues/995
            return (isinstance(e, S3CreateError)
                    and e.error_code in ('BucketAlreadyOwnedByYou', 'OperationAborted'))

        for attempt in retry_s3(predicate=bucket_creation_pending):
            with attempt:
                try:
                    bucket = self.s3.get_bucket(bucket_name, validate=True)
                except S3ResponseError as e:
                    if e.error_code == 'NoSuchBucket':
                        log.debug("Bucket '%s' does not exist.", bucket_name)
                        if create:
                            log.debug("Creating bucket '%s'.", bucket_name)
                            location = region_to_bucket_location(self.region)
                            bucket = self.s3.create_bucket(bucket_name, location=location)
                            assert self.__getBucketRegion(bucket) == self.region
                            if versioning:
                                bucket.configure_versioning(versioning)
                            log.debug("Created new job store bucket '%s'.", bucket_name)
                            return bucket
                        elif block:
                            raise
                        else:
                            return None
                    else:
                        raise
                else:
                    assert self.__getBucketRegion(bucket) == self.region
                    assert versioning is self.__getBucketVersioning(bucket)
                    log.debug("Using existing job store bucket '%s'.", bucket_name)
                    return bucket

    def _bindDomain(self, domain_name, create=False, block=True):
        """
        Return the Boto Domain object representing the SDB domain of the given name. If the
        domain does not exist and `create` is True, it will be created.

        :param str domain_name: the name of the domain to bind to

        :param bool create: True if domain should be created if it doesn't exist

        :param bool block: If False, return None if the domain doesn't exist. If True, wait until
               domain appears. This parameter is ignored if create is True.

        :rtype: Domain|None
        :raises SDBResponseError: If `block` is True and the domain still doesn't exist after the
                retry timeout expires.
        """
        log.info("Binding to job store domain '%s'.", domain_name)
        for attempt in retry_sdb(predicate=lambda e: no_such_sdb_domain(e) or sdb_unavailable(e)):
            with attempt:
                try:
                    return self.db.get_domain(domain_name)
                except SDBResponseError as e:
                    if no_such_sdb_domain(e):
                        if create:
                            return self.db.create_domain(domain_name)
                        elif block:
                            raise
                        else:
                            return None
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
        def presenceIndicator(cls):
            return 'encrypted'

        @classmethod
        def exists(cls, jobStoreFileID):
            for attempt in retry_sdb():
                with attempt:
                    return bool(cls.outer.filesDomain.get_attributes(
                        item_name=jobStoreFileID,
                        attribute_name=[cls.presenceIndicator()],
                        consistent_read=True))

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
            :rtype: AWSJobStore.FileInfo
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
                # clean up the old version of the file if necessary and safe
                if self.previousVersion and (self.previousVersion != self.version):
                    for attempt in retry_s3():
                        with attempt:
                            self.outer.filesBucket.delete_key(self.fileID,
                                                              version_id=self.previousVersion)
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
                    for attempt in retry_s3():
                        with attempt:
                            key.set_contents_from_filename(localFilePath, headers=headers)
                    self.version = key.version_id
                else:
                    with open(localFilePath, 'rb') as f:
                        for attempt in retry_s3():
                            with attempt:
                                upload = self.outer.filesBucket.initiate_multipart_upload(
                                    key_name=self.fileID,
                                    headers=headers)
                        try:
                            start = 0
                            part_num = itertools.count()
                            while start < file_size:
                                end = min(start + self.outer.partSize, file_size)
                                assert f.tell() == start
                                for attempt in retry_s3():
                                    with attempt:
                                        upload.upload_part_from_file(fp=f,
                                                                     part_num=next(part_num) + 1,
                                                                     size=end - start,
                                                                     headers=headers)
                                start = end
                            assert f.tell() == file_size == start
                        except:
                            with panic(log=log):
                                for attempt in retry_s3():
                                    with attempt:
                                        upload.cancel_upload()
                        else:
                            for attempt in retry_s3():
                                with attempt:
                                    self.version = upload.complete_upload().version_id
                for attempt in retry_s3():
                    with attempt:
                        key = self.outer.filesBucket.get_key(self.fileID,
                                                             headers=headers,
                                                             version_id=self.version)
                assert key.size == file_size
                # Make resonably sure that the file wasn't touched during the upload
                assert self._fileSizeAndTime(localFilePath) == (file_size, file_time)

        @contextmanager
        def uploadStream(self, multipart=True, allowInlining=True):
            info = self
            store = self.outer

            class MultiPartPipe(WritablePipe):
                def readFrom(self, readable):
                    buf = readable.read(store.partSize)
                    if allowInlining and len(buf) <= info._maxInlinedSize():
                        info.content = buf
                    else:
                        headers = info._s3EncryptionHeaders()
                        for attempt in retry_s3():
                            with attempt:
                                upload = store.filesBucket.initiate_multipart_upload(
                                    key_name=info.fileID,
                                    headers=headers)
                        try:
                            for part_num in itertools.count():
                                # There must be at least one part, even if the file is empty.
                                if len(buf) == 0 and part_num > 0:
                                    break
                                for attempt in retry_s3():
                                    with attempt:
                                        upload.upload_part_from_file(fp=StringIO(buf),
                                                                     # part numbers are 1-based
                                                                     part_num=part_num + 1,
                                                                     headers=headers)
                                if len(buf) == 0:
                                    break
                                buf = readable.read(info.outer.partSize)
                        except:
                            with panic(log=log):
                                for attempt in retry_s3():
                                    with attempt:
                                        upload.cancel_upload()
                        else:
                            for attempt in retry_s3():
                                with attempt:
                                    info.version = upload.complete_upload().version_id

            class SinglePartPipe(WritablePipe):
                def readFrom(self, readable):
                    buf = readable.read()
                    if allowInlining and len(buf) <= info._maxInlinedSize():
                        info.content = buf
                    else:
                        key = store.filesBucket.new_key(key_name=info.fileID)
                        buf = StringIO(buf)
                        headers = info._s3EncryptionHeaders()
                        for attempt in retry_s3():
                            with attempt:
                                assert buf.len == key.set_contents_from_file(fp=buf,
                                                                             headers=headers)
                        info.version = key.version_id

            with MultiPartPipe() if multipart else SinglePartPipe() as writable:
                yield writable

            assert bool(self.version) == (self.content is None)

        def copyFrom(self, srcKey):
            """
            Copies contents of source key into this file.

            :param srcKey: The key that will be copied from
            """
            assert srcKey.size is not None
            if srcKey.size <= self._maxInlinedSize():
                self.content = srcKey.get_contents_as_string()
            else:
                self.version = self._copyKey(srcKey=srcKey,
                                             dstBucketName=self.outer.filesBucket.name,
                                             dstKeyName=self._fileID,
                                             headers=self._s3EncryptionHeaders()).version_id

        def copyTo(self, dstKey):
            """
            Copies contents of this file to the given key.

            :param Key dstKey: The key to copy this file's content to
            """
            if self.content is not None:
                for attempt in retry_s3():
                    with attempt:
                        dstKey.set_contents_from_string(self.content)
            elif self.version:
                for attempt in retry_s3():
                    srcKey = self.outer.filesBucket.get_key(self.fileID,
                                                            validate=False)
                    srcKey.version_id = self.version
                    with attempt:
                        headers = {k.replace('amz-', 'amz-copy-source-', 1): v
                                   for k, v in self._s3EncryptionHeaders().iteritems()}
                        self._copyKey(srcKey=srcKey,
                                      dstBucketName=dstKey.bucket.name,
                                      dstKeyName=dstKey.name,
                                      headers=headers)
            else:
                assert False

        def _copyKey(self, srcKey, dstBucketName, dstKeyName, headers=None):
            headers = headers or {}
            if srcKey.size > self.outer.partSize:
                return copyKeyMultipart(srcKey=srcKey,
                                        dstBucketName=dstBucketName,
                                        dstKeyName=dstKeyName,
                                        partSize=self.outer.partSize,
                                        headers=headers)
            else:
                # We need a location-agnostic connection to S3 so we can't use the one that we
                # normally use for interacting with the job store bucket.
                with closing(boto.connect_s3()) as s3:
                    for attempt in retry_s3():
                        with attempt:
                            dstBucket = s3.get_bucket(dstBucketName)
                            return dstBucket.copy_key(new_key_name=dstKeyName,
                                                      src_bucket_name=srcKey.bucket.name,
                                                      src_version_id=srcKey.version_id,
                                                      src_key_name=srcKey.name,
                                                      metadata=srcKey.metadata,
                                                      headers=headers)

        def download(self, localFilePath):
            if self.content is not None:
                with open(localFilePath, 'w') as f:
                    f.write(self.content)
            elif self.version:
                headers = self._s3EncryptionHeaders()
                key = self.outer.filesBucket.get_key(self.fileID, validate=False)
                for attempt in retry_s3():
                    with attempt:
                        key.get_contents_to_filename(localFilePath,
                                                     version_id=self.version,
                                                     headers=headers)
            else:
                assert False

        @contextmanager
        def downloadStream(self):
            info = self

            class DownloadPipe(ReadablePipe):
                def writeTo(self, writable):
                    if info.content is not None:
                        writable.write(info.content)
                    elif info.version:
                        headers = info._s3EncryptionHeaders()
                        key = info.outer.filesBucket.get_key(info.fileID, validate=False)
                        for attempt in retry_s3():
                            with attempt:
                                key.get_contents_to_file(writable,
                                                         headers=headers,
                                                         version_id=info.version)
                    else:
                        assert False

            with DownloadPipe() as readable:
                yield readable

        def delete(self):
            store = self.outer
            if self.previousVersion is not None:
                for attempt in retry_sdb():
                    with attempt:
                        store.filesDomain.delete_attributes(
                            self.fileID,
                            expected_values=['version', self.previousVersion])
                if self.previousVersion:
                    for attempt in retry_s3():
                        with attempt:
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
        for attempt in retry_s3():
            with attempt:
                status = bucket.get_versioning_status()
        return bool(status) and self.versionings[status['Versioning']]

    def __getBucketRegion(self, bucket):
        for attempt in retry_s3():
            with attempt:
                return bucket_location_to_region(bucket.get_location())

    def destroy(self):
        # FIXME: Destruction of encrypted stores only works after initialize() or .resume()
        # See https://github.com/BD2KGenomics/toil/issues/1041
        self._bind(create=False, block=False)
        self._registered = None
        if self.filesBucket is not None:
            self._delete_bucket(self.filesBucket)
            self.filesBucket = None
        for name in 'filesDomain', 'jobsDomain':
            domain = getattr(self, name)
            if domain is not None:
                self._delete_domain(domain)
                setattr(self, name, None)
        self._registered = False

    def _delete_domain(self, domain):
        for attempt in retry_sdb():
            with attempt:
                try:
                    domain.delete()
                except SDBResponseError as e:
                    if no_such_sdb_domain(e):
                        pass
                    else:
                        raise

    def _delete_bucket(self, bucket):
        for attempt in retry_s3():
            with attempt:
                try:
                    for upload in bucket.list_multipart_uploads():
                        upload.cancel_upload()
                    if self.__getBucketVersioning(bucket) in (True, None):
                        for key in list(bucket.list_versions()):
                            bucket.delete_key(key.name, version_id=key.version_id)
                    else:
                        for key in list(bucket.list()):
                            key.delete()
                    bucket.delete()
                except S3ResponseError as e:
                    if e.error_code == 'NoSuchBucket':
                        pass
                    else:
                        raise


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
