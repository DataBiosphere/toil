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
"""
This file contains the AWS jobstore, which has its own docstring defining its use.

This docstring is about the organization of the file.

All direct AWS boto calls should live in toil.lib.aws, except for creating the
session instance and the resource/client (which should only be made ONCE in the jobstore).
Reasons for this:
 - DRY.
 - All retries are on their individual boto functions, instead of here.
 - Simple clear functions ~> simple clear unit tests (ideally).

Variables defining part size, parallelization, and other constants should live in toil.lib.aws.config.
"""
import os
import json
import logging
import pickle
import re
import stat
import uuid
from io import BytesIO
from contextlib import contextmanager
from typing import Optional, Tuple, Union

from typing import Optional
from botocore.exceptions import ClientError

from toil.fileStores import FileID
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             JobStoreExistsException,
                                             NoSuchJobException,
                                             NoSuchJobStoreException)
from toil.lib.aws.credentials import resource
from toil.lib.aws.s3 import (create_bucket,
                             delete_bucket,
                             bucket_exists,
                             copy_s3_to_s3,
                             copy_local_to_s3,
                             boto_args,
                             parse_s3_uri,
                             MultiPartPipe,
                             list_s3_items,
                             upload_to_s3,
                             download_stream,
                             s3_key_exists,
                             get_s3_object,
                             create_public_url,
                             AWSKeyNotFoundError,
                             AWSKeyAlreadyExistsError)
from toil.lib.ec2nodes import EC2Regions
from toil.lib.checksum import compute_checksum_for_file, ChecksumError
from toil.lib.io import AtomicFileCreate

# Sometimes we have to wait for multipart uploads to become real. How long
# should we wait?
CONSISTENCY_TICKS = 5
CONSISTENCY_TIME = 1


DEFAULT_AWS_PART_SIZE = 52428800
logger = logging.getLogger(__name__)


class AWSJobStore(AbstractJobStore):
    """
    The AWS jobstore can be thought of as an AWS s3 bucket, with functions to
    centralize, store, and track files for the workflow.

    The AWS jobstore stores 4 things:
        1. Jobs: These are pickled as files, and contain the information necessary to run a job when unpickled.
            A job's file is deleted when finished, and its absence means it completed.
        2. Files: The inputs and outputs of jobs.  Each file is actually two keys in s3:
              1. The actual file content, written with the file pattern: "files/{etag}"
              2. The file's reference and metadata, written with the file pattern: "metadata/{uuid}".
                 Note: This is a small json containing only: etag checksum, & executibility.
            The reference files act like unique keys in a database, referencing the original content.
            This deduplicates data on s3 if 2+ inputs/outputs have the same content.
        3. Logs: The written log files of jobs that have run, plus the log file for the main Toil process. (check this!)
        4. Shared Files: These are a small set of special files.  Most are needed by all jobs:
            * environment.pickle:  (environment variables)
            * config.pickle        (user options)
            * pid.log              (process ID of the workflow; when it finishes, the workflow either succeeded/failed)
            * userScript           (hot deployment(?);  this looks like the job module;  poke this)
            * rootJobReturnValue   (workflow succeeded or not)
            * TODO: are there any others?  do either vg or cactus use this?  should these have locks and when are they
               accessed?  are these only written once, but read many times?
            * TODO: A file with the date and toil version the workflow/bucket/jobstore was initialized with

    NOTES:
     - The AWS jobstore does not use a database (directly, at least) currently.  We can get away with this because:
           1. AWS s3 has strong consistency.
           2. s3's filter/query speed is pretty good.
         However, there may be reasons in the future to provide users with a database:
           * s3 throttling has limits (3,500/5,000 requests; something like dynamodb supports 100,000+ requests).
           * Access and filtering would be sped up, though how much faster this would be needs testing.
         ALSO NOTE: The caching filestore uses a local (per node) database with a very similar structure that maybe
                    could be synced up with this.

     - Etags are s3's native checksum, so we use that for file integrity checking since it's free when fetching
         object headers from s3.  Using an md5sum in addition to this would work well with the current filestore.
         WARNING: Etag values differ for the same file when the part size changes, so part size should always
         be Set In Stone, unless we hit s3's 10,000 part limit, and we need to account for that.

     - This class inherits self.config only when initialized/restarted and is None upon class instantiation.  These
         are the options/config set by the user.  When jobs are loaded/unpickled, they must re-incorporate this.
         The config.sse_key is the source of truth for bucket encryption and a clear error should be raised if
         restarting a bucket with a different encryption key set than it was initialized with.

     - The Toil bucket should log the version of Toil it was initialized with and warn the user if restarting with
         a different version.
    """
    def __init__(self, locator: str, part_size: int = DEFAULT_AWS_PART_SIZE):
        super(AWSJobStore, self).__init__()
        # TODO: parsing of user options seems like it should be done outside of this class;
        #  pass in only the bucket name and region?
        self.region, self.bucket_name = self.parse_jobstore_identifier(locator)
        self.s3_resource = resource('s3', region_name=self.region, **boto_args())
        self.s3_client = self.s3_resource.meta.client
        self.check_bucket_region_conflict()
        logger.debug(f"Instantiating {self.__class__} with region: {self.region}")
        self.locator = locator
        self.part_size = DEFAULT_AWS_PART_SIZE  # don't let users set the part size; it will throw off etag values

        # created anew during self.initialize() or loaded using self.resume()
        self.bucket = None

        # pickled job files named with uuid4
        self.job_key_prefix = 'jobs/'
        # the content of input/output files named with etag hashes to deduplicate
        self.file_key_prefix = 'files/'
        # these are special files, like 'environment.pickle'; place them in root
        self.shared_key_prefix = 'files/'
        # these represent input/output files, but are small json files pointing to the files with the real content in
        # self.file_key_prefix; also contains the file's metadata, like if it should executable; named with uuid4
        self.metadata_key_prefix = 'metadata/'
        # read and unread; named with uuid4
        self.log_key_prefix = 'logs/'

        # needs "self.config", which is not set until self.initialize() or self.resume() are called
        self.sse_key = None
        self.encryption_args = None

        self._batchedUpdates = []  # unused; we don't batch requests to simpledb anymore

    def set_encryption_from_config(self, config):
        if config.sseKey:
            with open(config.sseKey, 'r') as f:
                sse_key = f.read()
            assert len(sse_key) == 32  # TODO: regex
            return sse_key, {'SSECustomerAlgorithm': 'AES256', 'SSECustomerKey': sse_key}
        else:
            return None, {}

    def initialize(self, config):
        """
        Called when starting a new jobstore with a non-existent bucket.

        Create bucket, raise if it already exists.
        Set options from config.  Set sse key from that.
        """
        self.sse_key, self.encryption_args = self.set_encryption_from_config(config)
        logger.debug(f"Instantiating {self.__class__} for region {self.region} with bucket: '{self.bucket_name}'")
        if bucket_exists(self.s3_resource, self.bucket_name):
            raise JobStoreExistsException(self.locator)
        self.bucket = create_bucket(self.s3_resource, self.bucket_name)
        # upload a root/shared file with the toil version and date initialized
        # warn the user if they restart the same bucket with any other version than the one it was initialized with

        # upload_to_s3(s3_resource=self.s3_resource,
        #              bucket=self.bucket_name,
        #              key='toil-bucket.initialized.datetime',
        #              readable=BytesIO(str(datetime.datetime.now()).encode('utf-8')))
        super(AWSJobStore, self).initialize(config)

    def resume(self):
        """Called when reusing an old jobstore with an existing bucket.  Raise if the bucket doesn't exist."""
        super(AWSJobStore, self).resume()  # this sets self.config to not be None
        self.sse_key, self.encryption_args = self.set_encryption_from_config(self.config)
        if self.bucket is None:
            self.bucket = bucket_exists(self.s3_resource, self.bucket_name)
        if not self.bucket:
            raise NoSuchJobStoreException(self.locator)

    def unpickle_job(self, job_id: str):
        """Use a job_id to unpickle and return a job from the jobstore's s3 bucket."""
        assert job_id.startswith(self.job_key_prefix), f'Key "{job_id}" must be prefixed with: {self.job_key_prefix}'
        with self.readFileStream(job_id) as fh:
            job = pickle.loads(fh.read())
        if job is not None:
            job.assignConfig(self.config)
        return job

    def pickle_job(self, job) -> str:
        """Pickle a job object, save it in the jobstore's s3 bucket, and return its job_id reference."""
        self.s3_client.upload_fileobj(Bucket=self.bucket_name,
                                      Key=f'{self.job_key_prefix}{job.jobStoreID}',
                                      Fileobj=BytesIO(pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL)),
                                      ExtraArgs=self.encryption_args)
        return job.jobStoreID

    @contextmanager
    def batch(self):
        # TODO: This function doesn't make sense now that we don't batch requests to simpledb anymore
        yield

    def assign_job_id(self, jobDescription):
        jobDescription.jobStoreID = str(uuid.uuid4())
        cmd = '<no command>' if jobDescription.command is None else jobDescription.command
        logger.debug(f"Assigning ID to job {jobDescription.jobStoreID} for '{cmd}'")

    def create_job(self, jobDescription):
        self.pickle_job(jobDescription)
        return jobDescription

    def job_exists(self, job_id: str, check: bool = False):
        """Checks if the job_id is found in s3."""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=f'{self.job_key_prefix}{job_id}')
            return True
        except self.s3_client.exceptions.NoSuchKey:
            if check:
                raise NoSuchJobException(job_id)
        return False

    def jobs(self):
        for job_id in list_s3_items(bucket=self.bucket_name, prefix=self.job_key_prefix):
            yield self.unpickle_job(job_id)

    def load_job(self, job_id: str):
        job = self.unpickle_job(f'{self.job_key_prefix}{job_id}')
        if job is None:
            raise NoSuchJobException(job_id)
        return job

    def update_job(self, jobDescription):
        self.pickle_job(jobDescription)

    def delete_job(self, job_id: str):
        logger.debug("Deleting job %s", job_id)
        self.s3_client.delete_object({'Bucket': self.bucket_name, 'Key': f'{self.job_key_prefix}{job_id}'})

    def getEmptyFileStoreID(self, jobStoreID=None, cleanup=False, basename=None):
        """WWWHHHHYYYYYYY"""
        new_file_id = str(uuid.uuid4())
        self.s3_client.upload_fileobj(Bucket=self.bucket_name,
                                      Key=new_file_id,
                                      Fileobj=BytesIO(b''),
                                      ExtraArgs=self.encryption_args)

        # use head_object with the SSE headers to fetch content_length
        response = self.s3_client.head_object(Bucket=self.bucket_name,
                                              Key=new_file_id,
                                              **self.encryption_args)
        assert 0 == response.get('ContentLength')
        return new_file_id

    def _importFile(self, otherCls, url, sharedFileName=None, hardlink=False, symlink=False) -> FileID:
        """
        Upload a file into the s3 bucket jobstore from the source uri.

        This db entry's existence should always be in sync with the file's existence (when one exists,
        so must the other).
        """
        # we are copying from s3 to s3
        if issubclass(otherCls, AWSJobStore):
            src_bucket_name, src_key_name = parse_s3_uri(url)
            response = s3_key_exists(bucket=src_bucket_name, key=src_key_name, check=True)
            content_length = response['ContentLength']  # e.g. 65536
            content_type = response['ContentType']  # e.g. "binary/octet-stream"
            etag = response['ETag'].strip('\"')  # e.g. "\"586af4cbd7416e6aefd35ccef9cbd7c8\""

            # etags may not match if the person who first uploaded the file had a different part size so...
            # if the file is small, re-calculate the etag here
            # if the file is large calculate as we go; rename if different than original
            file_id = sharedFileName or str(uuid.uuid4())

            # upload actual file content if it does not exist already
            # etags are unique hashes, so this may exist if another process uploaded the exact same file
            actual_file_content_path = f'{self.file_key_prefix}{etag}'
            if not s3_key_exists(bucket=self.bucket_name, key=actual_file_content_path):
                copy_s3_to_s3(src_bucket=src_bucket_name, src_key=src_key_name,
                              dst_bucket=self.bucket_name, dst_key=actual_file_content_path)
                # verify etag after copying here

            # upload metadata reference; there may be multiple references pointing to the same etag path
            self.s3_client.upload_fileobj(Bucket=self.bucket_name,
                                          Key=f'{self.file_key_prefix}{file_id}',
                                          Fileobj=BytesIO(json.dumps(
                                              {'etag': etag,
                                               'size': content_length,
                                               # cannot determine exec bit from foreign s3 so default to False
                                               'executable': 0})),
                                          ExtraArgs=self.encryption_args)
            return FileID(fileStoreID=file_id, size=content_length) if sharedFileName is None else None
        else:
            return super(AWSJobStore, self)._importFile(otherCls, url, sharedFileName=sharedFileName)

    def _exportFile(self, otherCls, jobStoreFileID, url) -> None:
        if issubclass(otherCls, AWSJobStore):
            dst_bucket_name, dst_key_name = parse_s3_uri(url)
            key = f'{self.metadata_key_prefix}{jobStoreFileID}'
            try:
                metadata = json.loads(get_s3_object(bucket=self.bucket_name, key=key))
            except self.s3_client.exceptions.NoSuchKey:
                raise AWSKeyNotFoundError(f"File '{key}' not found in AWS jobstore bucket: '{self.bucket_name}'")
            copy_s3_to_s3(src_bucket=self.bucket_name, src_key=f'{self.file_key_prefix}{metadata["etag"]}',
                          dst_bucket=dst_bucket_name, dst_key=dst_key_name)
        else:
            super(AWSJobStore, self)._defaultExportFile(otherCls, jobStoreFileID, url)

    @classmethod
    def _readFromUrl(cls, url, writable):
        # TODO
        srcObj = cls._getObjectForUrl(url, existing=True)
        srcObj.download_fileobj(writable)
        executable = False
        return srcObj.content_length, executable

    def _writeToUrl(self, readable, url, executable=False):
        # TODO
        dstObj = self._getObjectForUrl(url)
        upload_to_s3(readable=readable,
                     s3_resource=self.s3_resource,
                     bucket=dstObj.bucket_name,
                     key=dstObj.key,
                     partSize=DEFAULT_AWS_PART_SIZE)

    def _getObjectForUrl(self, uri: str, existing: Optional[bool] = None):
        """
        Extracts a key (object) from a given s3:// URL.

        :param bool existing: If True, key is expected to exist. If False or None, key is
            expected not to exist and it will be created.

        :rtype: S3.Object
        """
        bucket_name, key_name = parse_s3_uri(uri)
        obj = self.s3_resource.Object(bucket_name, key_name)

        try:
            obj.load()
            objExists = True
        except ClientError as e:
            if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                objExists = False
            else:
                raise
        if existing is True and not objExists:
            raise AWSKeyNotFoundError(f"Key '{key_name}' does not exist in bucket '{bucket_name}'.")
        elif existing is False and objExists:
            raise AWSKeyAlreadyExistsError(f"Key '{key_name}' exists in bucket '{bucket_name}'.")

        if not objExists:
            obj.put()  # write an empty file
        return obj

    @classmethod
    def _supportsUrl(cls, url, export=False):
        # TODO: export seems unused
        return url.scheme.lower() == 's3'

    def writeFile(self, localFilePath, jobStoreID=None, cleanup=False):
        etag = compute_checksum_for_file(localFilePath, algorithm='etag')
        file_attributes = os.stat(localFilePath)
        size = file_attributes.st_size
        executable = file_attributes.st_mode & stat.S_IXUSR != 0
        actual_file_content_path = f'{self.file_key_prefix}{etag}'
        file_id = jobStoreID or str(uuid.uuid4())
        metadata_path = f'{self.metadata_key_prefix}{file_id}'
        if not s3_key_exists(bucket=self.bucket_name, key=actual_file_content_path):
            copy_local_to_s3(local_file_path=localFilePath,
                             dst_bucket=self.bucket_name,
                             dst_key=actual_file_content_path)
            # TODO: verify etag after copying here

        # upload metadata reference; there may be multiple references pointing to the same etag path
        self.s3_client.upload_fileobj(Bucket=self.bucket_name,
                                      Key=metadata_path,
                                      Fileobj=BytesIO(json.dumps({'etag': etag,
                                                                  'size': size,
                                                                  'executable': int(executable)})),
                                      ExtraArgs=self.encryption_args)
        return FileID(fileStoreID=file_id, size=size, executable=executable)

    @contextmanager
    def writeFileStream(self, jobStoreID=None, cleanup=False, basename=None, encoding=None, errors=None):
        # TODO; updateFileStream???
        file_id = jobStoreID or str(uuid.uuid4())
        pipe = MultiPartPipe(encoding=encoding,
                             errors=errors,
                             part_size=self.part_size,
                             s3_client=self.s3_client,
                             bucket_name=self.bucket_name,
                             file_id=f'{self.file_key_prefix}{jobStoreID}',
                             encryption_args=self.encryption_args)
        with pipe as writable:
            yield writable, file_id

    # determine isProtected from presence of sse_path?  why is this an input here?
    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None, encoding=None, errors=None):
        # TODO
        self._requireValidSharedFileName(sharedFileName)
        pipe = MultiPartPipe(encoding=encoding,
                             errors=errors,
                             part_size=self.part_size,
                             s3_client=self.s3_client,
                             bucket_name=self.bucket_name,
                             file_id=f'{self.shared_key_prefix}{sharedFileName}',
                             encryption_args=self.encryption_args)
        with pipe as writable:
            yield writable

    def updateFile(self, jobStoreFileID, localFilePath):
        # Why use this over plain write file?
        self.writeFile(jobStoreID=jobStoreFileID, localFilePath=localFilePath)

    @contextmanager
    def updateFileStream(self, jobStoreFileID, encoding=None, errors=None):
        pipe = MultiPartPipe(encoding=encoding,
                             errors=errors,
                             part_size=self.part_size,
                             s3_client=self.s3_client,
                             bucket_name=self.bucket_name,
                             file_id=f'{self.file_key_prefix}{jobStoreFileID}',
                             encryption_args=self.encryption_args)
        with pipe as writable:
            yield writable

    def fileExists(self, jobStoreFileID):
        s3_key_exists(s3_resource=self.s3_resource,
                      bucket=self.bucket_name,
                      key=f'{self.file_key_prefix}{jobStoreFileID}')

    def getFileSize(self, jobStoreFileID: str) -> int:
        """Do we need both getFileSize and getSize???"""
        return self.getSize(url=f's3://{self.bucket_name}/{jobStoreFileID}')

    def getSize(self, url: str) -> int:
        """Do we need both getFileSize and getSize???"""
        try:
            return self._getObjectForUrl(url, existing=True).content_length
        except AWSKeyNotFoundError:
            return 0

    def readFile(self, jobStoreFileID, localFilePath, symlink=False):
        obj = self._getObjectForUrl(f's3://{self.bucket_name}/{jobStoreFileID}', existing=True)

        with AtomicFileCreate(localFilePath) as tmpPath:
            obj.download_file(Filename=tmpPath, ExtraArgs={**self.encryption_args})

        # TODO: checksum and make executable if necessary

        # if not self.config.disableJobStoreChecksumVerification and previously_computed_checksum:
        #     algorithm, expected_checksum = previously_computed_checksum.split('$')
        #     checksum = compute_checksum_for_file(localFilePath, algorithm=algorithm)
        #     if previously_computed_checksum != checksum:
        #         raise ChecksumError(f'Checksum mismatch for file {localFilePath}.  '
        #                             f'Expected: {previously_computed_checksum} Actual: {checksum}')
        # if getattr(jobStoreFileID, 'executable', False):
        #     os.chmod(localFilePath, os.stat(localFilePath).st_mode | stat.S_IXUSR)

    @contextmanager
    def readFileStream(self, jobStoreFileID, encoding=None, errors=None):
        logger.debug("Reading into stream.")
        with download_stream(self.s3_resource,
                             bucket=self.bucket_name,
                             key=f'{self.file_key_prefix}{jobStoreFileID}',
                             extra_args=self.encryption_args,
                             encoding=encoding,
                             errors=errors) as readable:
            yield readable

    @contextmanager
    def readSharedFileStream(self, sharedFileName, encoding=None, errors=None):
        self._requireValidSharedFileName(sharedFileName)
        logger.debug("Reading into stream.")
        with download_stream(self.s3_resource,
                             bucket=self.bucket_name,
                             key=f'{self.shared_key_prefix}{sharedFileName}',
                             extra_args=self.encryption_args,
                             encoding=encoding,
                             errors=errors) as readable:
            yield readable

    def deleteFile(self, file_id):
        self.s3_client.delete_object(Bucket=self.bucket_name, Key=f'{self.file_key_prefix}{file_id}')

    def writeStatsAndLogging(self, log_msg: Union[bytes, str]):
        if isinstance(log_msg, str):
            log_msg = log_msg.encode('utf-8', errors='ignore')
        file_obj = BytesIO(log_msg)

        key_name = f'logs/unread/{uuid.uuid4()}'
        self.s3_client.upload_fileobj(Bucket=self.bucket_name,
                                      Key=key_name,
                                      Fileobj=file_obj,
                                      ExtraArgs=self.encryption_args)

        # use head_object with the SSE headers to access versionId and content_length attributes
        response = self.s3_client.head_object(Bucket=self.bucket_name,
                                              Key=key_name,
                                              **self.encryption_args)
        # checking message length is sufficient; don't checksum log messages
        assert len(log_msg) == response.get('ContentLength')

    def readStatsAndLogging(self, callback, readAll=False):
        """
        Owner ID specifies either logs that have already been read, or not.

        This fetches all referenced logs in the database from s3 as readable objects and runs "callback()" on them.
        """
        prefix = 'logs/' if readAll else 'logs/unread/'
        itemsProcessed = 0
        for log in list_s3_items(bucket=self.bucket.name, prefix=prefix):
            obj = self._getObjectForUrl(f's3://{self.bucket.name}/{log}')
            with download_stream(s3_object=obj) as readable:
                callback(readable)
            if not readAll:
                # move unread logs to read; tag instead?
                pass
            itemsProcessed += 1
        return itemsProcessed

    def getPublicUrl(self, jobStoreFileID: str):
        """Turn s3:// into http:// and put a public-read ACL on it."""
        return create_public_url(self.s3_resource,
                                 bucket=self.bucket_name,
                                 key=f'{self.file_key_prefix}{jobStoreFileID}')

    def getSharedPublicUrl(self, sharedFileName):
        self._requireValidSharedFileName(sharedFileName)
        return create_public_url(self.s3_resource,
                                 bucket=self.bucket_name,
                                 key=f'{self.shared_key_prefix}{sharedFileName}')

    def destroy(self):
        delete_bucket(self.s3_resource, self.bucket_name)

    def check_bucket_region_conflict(self):
        # TODO: Does this matter?
        if bucket_exists(self.s3_resource, self.bucket_name):
            response = self.s3_client.get_bucket_location(Bucket=self.bucket_name)
            if response["LocationConstraint"] != self.region:
                raise ValueError(f'Bucket region conflict.  Bucket already exists in region '
                                 f'"{response["LocationConstraint"]}" but "{self.region}" was specified.')

    def parse_jobstore_identifier(self, jobstore_identifier: str) -> Tuple[str, str]:
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
        return region, bucket_name
