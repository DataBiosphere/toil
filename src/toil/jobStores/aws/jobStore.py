# Copyright (C) 2015-2021 Regents of the University of California
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
import os
import logging
import pickle
import re
import stat
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager
from typing import Optional, Tuple

from botocore.exceptions import ClientError

from toil.fileStores import FileID
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             JobStoreExistsException,
                                             NoSuchJobException,
                                             NoSuchJobStoreException)
from toil.jobStores.aws.utils import SDBHelper, uploadFile
from toil.jobStores.aws.file_info import AWSFile
from toil.lib.compatibility import compat_bytes
from toil.lib.ec2 import establish_boto3_session
from toil.lib.aws.s3 import (create_bucket,
                             delete_bucket,
                             bucket_exists,
                             bucket_is_registered_with_toil)
from toil.lib.ec2nodes import EC2Regions
from toil.lib.retry import retry

boto3_session = establish_boto3_session()
s3_boto3_resource = boto3_session.resource('s3')
s3_boto3_client = boto3_session.client('s3')
logger = logging.getLogger(__name__)

DEFAULT_AWS_PART_SIZE = 52428800


def parse_jobstore_identifier(jobstore_identifier: str) -> Tuple[str, str]:
    region, jobstore_name = jobstore_identifier.split(':')

    bucket_name = f'{jobstore_name}--toil'

    regions = EC2Regions.keys()
    if region not in regions:
        raise ValueError(f'AWS Region "{region}" is not one of: {regions}')

    if not 3 <= len(jobstore_name) <= 56:
        raise ValueError(f'AWS jobstore name must be between 3 and 56 chars: '
                         f'{jobstore_name} (len: {len(jobstore_name)})')

    if not re.compile(r'^[a-z0-9][a-z0-9-]+[a-z0-9]$').match(jobstore_name):
        raise ValueError(f"Invalid AWS jobstore name: '{jobstore_name}'.  Must contain only digits, "
                         f"lower-case letters, and hyphens.  Must also not start or end in a hyphen.")

    if '--' in jobstore_name:
        raise ValueError(f"AWS jobstore names may not contain '--': {jobstore_name}")

    if bucket_exists(bucket_name):
        response = s3_boto3_client.get_bucket_location(Bucket=bucket_name)
        if response["LocationConstraint"] != region:
            raise ValueError(f'Bucket region conflict.  Bucket already exists in region '
                             f'"{response["LocationConstraint"]}" but "{region}" was specified '
                             f'in the jobstore_identifier: {jobstore_identifier}')
    return region, bucket_name


class AWSJobStore(AbstractJobStore):
    """
    A job store that uses Amazon's S3 for file storage and SimpleDB for storing job info and
    enforcing strong consistency on the S3 file storage. There will be SDB domains for jobs and
    files and a versioned S3 bucket for file contents. Job objects are pickled, compressed,
    partitioned into chunks of 1024 bytes and each chunk is stored as a an attribute of the SDB
    item representing the job. UUIDs are used to identify jobs and files.
    """
    # A dummy job ID under which all shared files are stored
    sharedFileOwnerID = '891f7db6-e4d9-4221-a58e-ab6cc4395f94'

    # A dummy job ID under which all unread stats files are stored
    statsFileOwnerID = 'bfcf5286-4bc7-41ef-a85d-9ab415b69d53'

    # A dummy job ID under which all read stats files are stored
    readStatsFileOwnerID = 'e77fc3aa-d232-4255-ae04-f64ee8eb0bfa'

    items_per_batch_delete = items_per_batch_upload = 25

    def __init__(self, locator: str, part_size: int = DEFAULT_AWS_PART_SIZE):
        """
        Create a new job store in AWS or load an existing one from there.

        :param int part_size: The size of each individual part used for multipart operations like
               upload and copy, must be >= 5 MiB but large enough to not exceed 10k parts for the
               whole file
        """
        super(AWSJobStore, self).__init__()

        self.region, self.bucket_name = parse_jobstore_identifier(locator)

        self.job_key = 'jobs'
        self.files_key = 'files'
        self.access_log_file = 'access.log'

        logger.debug(f"Instantiating {self.__class__} for region {self.region} with bucket: '{self.bucket_name}'")
        self.locator = locator
        self.partSize = part_size
        self.bucket = None

        self.sse_key = self.config.sseKey

        self.s3_resource = boto3_session.resource('s3', region_name=self.region)
        self.s3_client = self.s3_resource.meta.client

    def jobstore_exists(self):
        """Make sure the bucket we need doesn't exist.  Check for the toil-specific registry file."""
        return bucket_exists(self.bucket_name) and bucket_is_registered_with_toil(self.bucket_name)

    def initialize(self, config):
        if bucket_exists(self.bucket_name):
            raise JobStoreExistsException(self.locator)
        self.bucket = create_bucket(self.bucket_name, versioning=True)
        super(AWSJobStore, self).initialize(config)

    def resume(self):
        if not self.bucket:
            self.bucket = bucket_exists(self.bucket_name)
        if not self.bucket or not bucket_is_registered_with_toil(self.bucket_name):
            raise NoSuchJobStoreException(self.locator)
        super(AWSJobStore, self).resume()

    def job_from_item(self, item: dict):
        logger.debug("Loading job from S3.")
        with self.readFileStream(item["fileID"]) as fh:
            binary = fh.read()
        job = pickle.loads(binary)
        if job is not None:
            job.assignConfig(self.config)
        return job

    def job_to_item(self, job) -> dict:
        binary = pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL)
        with self.writeFileStream() as (writable, fileID):
            writable.write(binary)
        item = SDBHelper.binaryToAttributes(None)
        item["fileID"] = fileID
        return item

    @contextmanager
    def batch(self):
        self._batchedUpdates = []
        yield
        jobs_per_batch = 25
        batches = [self._batchedUpdates[i:i + jobs_per_batch] for i in
                   range(0, len(self._batchedUpdates), jobs_per_batch)]

        for batch in batches:
            items = {compat_bytes(jobDescription.jobStoreID): self.job_to_item(jobDescription) for jobDescription in batch}
            assert self.jobsDomain.batch_put_attributes(items)
        self._batchedUpdates = None

    def assignID(self, jobDescription):
        jobDescription.jobStoreID = jobstore_id = str(uuid.uuid4())
        cmd = '<no command>' if jobDescription.command is None else jobDescription.command
        logger.debug(f"Assigning ID to job {jobstore_id} for '{cmd}'")

    def create(self, jobDescription):
        if hasattr(self, "_batchedUpdates") and self._batchedUpdates is not None:
            self._batchedUpdates.append(jobDescription)
        else:
            self.update(jobDescription)
        return jobDescription

    def exists(self, jobStoreID):
        # check metadata in db/bucket
        return

    def jobs(self):
        result = list(self.jobsDomain.select(
            consistent_read=True,
            query="select * from `%s`" % self.jobsDomain.name))
        assert result is not None
        for jobItem in result:
            yield self.job_from_item(jobItem)

    def load(self, jobStoreID):
        item = self.jobsDomain.get_attributes(compat_bytes(jobStoreID), consistent_read=True)
        if not item:
            raise NoSuchJobException(jobStoreID)
        job = self.job_from_item(item)
        if job is None:
            raise NoSuchJobException(jobStoreID)
        logger.debug("Loaded job %s", jobStoreID)
        return job

    def update(self, jobDescription):
        logger.debug(f"Updating job {jobDescription.jobStoreID}", )
        item = self.job_to_item(jobDescription)
        assert self.jobsDomain.put_attributes(compat_bytes(jobDescription.jobStoreID), item)

    itemsPerBatchDelete = 25

    def delete(self, jobStoreID):
        # remove job and replace with jobStoreId.
        logger.debug("Deleting job %s", jobStoreID)

        item = self.jobsDomain.get_attributes(compat_bytes(jobStoreID), consistent_read=True)
        self._checkItem(item)
        self.deleteFile(item["fileID"])
        self.jobsDomain.delete_attributes(item_name=compat_bytes(jobStoreID))
        items = list(self.filesDomain.select(
                    consistent_read=True,
                    query="select version from `%s` where ownerID='%s'" % (
                        self.filesDomain.name, jobStoreID)))
        assert items is not None
        if items:
            logger.debug("Deleting %d file(s) associated with job %s", len(items), jobStoreID)
            n = self.itemsPerBatchDelete
            batches = [items[i:i + n] for i in range(0, len(items), n)]
            for batch in batches:
                itemsDict = {item.name: None for item in batch}
                self.filesDomain.batch_delete_attributes(itemsDict)
            for item in items:
                version = item.get('version')
                obj = {'Bucket': self.bucket.name, 'Key': compat_bytes(item.name)}
                if version:
                    obj['VersionId'] = version
                self.s3_client.delete_object(obj)

    def getEmptyFileStoreID(self, jobStoreID=None, cleanup=False, basename=None):
        info = AWSFile.create(jobStoreID if cleanup else None)
        with info.uploadStream() as _:
            # Empty
            pass
        info.save()
        logger.debug("Created %r.", info)
        return info.fileID

    def _importFile(self, otherCls, url, sharedFileName=None, hardlink=False, symlink=False):
        if issubclass(otherCls, AWSJobStore):
            srcObj = self._getObjectForUrl(url, existing=True)
            size = srcObj.content_length
            if sharedFileName is None:
                info = AWSFile.create(srcObj.key)
            else:
                self._requireValidSharedFileName(sharedFileName)
                jobStoreFileID = self._sharedFileID(sharedFileName)
                info = AWSFile.loadOrCreate(jobStoreFileID=jobStoreFileID,
                                                  ownerID=self.sharedFileOwnerID,
                                                  encrypted=None)
            info.copyFrom(srcObj)
            info.save()
            return FileID(info.fileID, size) if sharedFileName is None else None
        else:
            return super(AWSJobStore, self)._importFile(otherCls, url,
                                                        sharedFileName=sharedFileName)

    def _exportFile(self, otherCls, jobStoreFileID, url):
        if issubclass(otherCls, AWSJobStore):
            dstObj = self._getObjectForUrl(url)
            info = AWSFile.loadOrFail(jobStoreFileID)
            info.copyTo(dstObj)
        else:
            super(AWSJobStore, self)._defaultExportFile(otherCls, jobStoreFileID, url)

    @classmethod
    def getSize(cls, url):
        return cls._getObjectForUrl(url, existing=True).content_length

    @classmethod
    def _readFromUrl(cls, url, writable):
        srcObj = cls._getObjectForUrl(url, existing=True)
        srcObj.download_fileobj(writable)
        return (
            srcObj.content_length,
            False  # executable bit is always False
        )

    @classmethod
    def _writeToUrl(cls, readable, url, executable=False):
        dstObj = cls._getObjectForUrl(url)

        logger.debug("Uploading %s", dstObj.key)
        uploadFile(readable=readable,
                   resource=s3_boto3_resource,
                   bucketName=dstObj.bucket_name,
                   fileID=dstObj.key,
                   partSize=DEFAULT_AWS_PART_SIZE)

    def _getObjectForUrl(self, url, existing: Optional[bool] = None):
        """
        Extracts a key (object) from a given s3:// URL.

        :param bool existing: If True, key is expected to exist. If False, key is expected not to
               exists and it will be created. If None, the key will be created if it doesn't exist.

        :rtype: S3.Object
        """
        keyName = url.path[1:]
        bucketName = url.netloc

        botoargs = {}
        host = os.environ.get('TOIL_S3_HOST', None)
        port = os.environ.get('TOIL_S3_PORT', None)
        protocol = 'https'
        if os.environ.get('TOIL_S3_USE_SSL', True) == 'False':
            protocol = 'http'
        if host:
            botoargs['endpoint_url'] = f'{protocol}://{host}' + f':{port}' if port else ''

        s3 = boto3_session.resource('s3', region_name=self.region, **botoargs)
        obj = s3.Object(bucketName, keyName)
        objExists = True

        try:
            obj.load()
        except ClientError as e:
            if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                objExists = False
            else:
                raise
        if existing is True and not objExists:
            raise RuntimeError(f"Key '{keyName}' does not exist in bucket '{bucketName}'.")
        elif existing is False and objExists:
            raise RuntimeError(f"Key '{keyName}' exists in bucket '{bucketName}'.")

        if not objExists:
            obj.put()  # write an empty file
        return obj

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() == 's3'

    def writeFile(self, localFilePath, jobStoreID=None, cleanup=False):
        info = AWSFile.create(jobStoreID if cleanup else None)
        info.upload(localFilePath, not self.config.disableJobStoreChecksumVerification)
        info.save()
        logger.debug("Wrote %r of from %r", info, localFilePath)
        return info.fileID

    @contextmanager
    def writeFileStream(self, jobStoreID=None, cleanup=False, basename=None):
        info = AWSFile.create(jobStoreID if cleanup else None)
        with info.uploadStream() as writable:
            yield writable, info.fileID
        info.save()
        logger.debug("Wrote %r.", info)

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        self._requireValidSharedFileName(sharedFileName)
        info = AWSFile.loadOrCreate(jobStoreFileID=self._sharedFileID(sharedFileName),
                                          ownerID=self.sharedFileOwnerID,
                                          encrypted=isProtected)
        with info.uploadStream() as writable:
            yield writable
        info.save()
        logger.debug("Wrote %r for shared file %r.", info, sharedFileName)

    def updateFile(self, jobStoreFileID, localFilePath):
        info = AWSFile.loadOrFail(jobStoreFileID)
        info.upload(localFilePath, not self.config.disableJobStoreChecksumVerification)
        info.save()
        logger.debug("Wrote %r from path %r.", info, localFilePath)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        info = AWSFile.loadOrFail(jobStoreFileID)
        with info.uploadStream() as writable:
            yield writable
        info.save()
        logger.debug("Wrote %r from stream.", info)

    def fileExists(self, jobStoreFileID):
        return AWSFile.exists(jobStoreFileID)

    def getFileSize(self, jobStoreFileID):
        if not self.fileExists(jobStoreFileID):
            return 0
        info = AWSFile.loadOrFail(jobStoreFileID)
        return info.getSize()

    def readFile(self, jobStoreFileID, localFilePath, symlink=False):
        info = AWSFile.loadOrFail(jobStoreFileID)
        logger.debug("Reading %r into %r.", info, localFilePath)
        info.download(localFilePath, not self.config.disableJobStoreChecksumVerification)
        if getattr(jobStoreFileID, 'executable', False):
            os.chmod(localFilePath, os.stat(localFilePath).st_mode | stat.S_IXUSR)

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        info = AWSFile.loadOrFail(jobStoreFileID)
        logger.debug("Reading %r into stream.", info)
        with info.downloadStream() as readable:
            yield readable

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        self._requireValidSharedFileName(sharedFileName)
        jobStoreFileID = self._sharedFileID(sharedFileName)
        info = AWSFile.loadOrFail(jobStoreFileID, customName=sharedFileName)
        logger.debug("Reading %r for shared file %r into stream.", info, sharedFileName)
        with info.downloadStream() as readable:
            yield readable

    def deleteFile(self, jobStoreFileID):
        info = AWSFile.load(jobStoreFileID)
        if info is None:
            logger.debug("File %s does not exist, skipping deletion.", jobStoreFileID)
        else:
            info.delete()

    def writeStatsAndLogging(self, statsAndLoggingString):
        info = AWSFile.create(self.statsFileOwnerID)
        with info.uploadStream(multipart=False) as writeable:
            if isinstance(statsAndLoggingString, str):
                # This stream is for binary data, so encode any non-encoded things
                statsAndLoggingString = statsAndLoggingString.encode('utf-8', errors='ignore')
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
        items = list(self.filesDomain.select(
            consistent_read=True,
            query="select * from `%s` where ownerID='%s'" % (
                self.filesDomain.name, ownerId)))
        assert items is not None
        for item in items:
            info = AWSFile.fromItem(item)
            with info.downloadStream() as readable:
                callback(readable)
            yield info

    # TODO: Make this retry more specific?
    #  example: https://github.com/DataBiosphere/toil/issues/3378
    @retry()
    def getPublicUrl(self, jobStoreFileID):
        info = AWSFile.loadOrFail(jobStoreFileID)
        if info.content is not None:
            with info.uploadStream(allowInlining=False) as f:
                f.write(info.content)

        self.bucket.Object(compat_bytes(jobStoreFileID)).Acl().put(ACL='public-read')
        url = self.s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': self.bucket.name,
                                                            'Key': compat_bytes(jobStoreFileID),
                                                            'VersionId': info.version},
                                                    ExpiresIn=self.publicUrlExpiration.total_seconds())

        # boto doesn't properly remove the x-amz-security-token parameter when
        # query_auth is False when using an IAM role (see issue #2043). Including the
        # x-amz-security-token parameter without the access key results in a 403,
        # even if the resource is public, so we need to remove it.
        scheme, netloc, path, query, fragment = urllib.parse.urlsplit(url)
        params = urllib.parse.parse_qs(query)
        for param_key in ['x-amz-security-token', 'AWSAccessKeyId', 'Signature']:
            if param_key in params:
                del params[param_key]
        query = urllib.parse.urlencode(params, doseq=True)
        url = urllib.parse.urlunsplit((scheme, netloc, path, query, fragment))
        return url

    def getSharedPublicUrl(self, sharedFileName):
        self._requireValidSharedFileName(sharedFileName)
        return self.getPublicUrl(self._sharedFileID(sharedFileName))

    def _sharedFileID(self, sharedFileName):
        return str(uuid.uuid5(uuid.UUID(self.sharedFileOwnerID), sharedFileName))

    # TODO: Make this retry more specific?
    #  example: https://github.com/DataBiosphere/toil/issues/3378
    @retry()
    def destroy(self):
        delete_bucket(self.bucket.name)
