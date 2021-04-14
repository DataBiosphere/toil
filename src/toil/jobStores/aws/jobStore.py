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
import json
import logging
import pickle
import re
import stat
import urllib.error
import urllib.parse
import urllib.request
import uuid
from io import BytesIO
from contextlib import contextmanager
from typing import Optional, Tuple, Union

import boto.sdb
import boto.s3.connection
from typing import Optional
from boto.exception import SDBResponseError
from botocore.exceptions import ClientError

from toil.fileStores import FileID
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             JobStoreExistsException,
                                             NoSuchJobException,
                                             NoSuchJobStoreException)
from toil.jobStores.aws.utils import uploadFile
from toil.jobStores.aws.file_info import AWSFile, AWSFileMetadata
from toil.lib.compatibility import compat_bytes
from toil.lib.ec2 import establish_boto3_session
from toil.lib.aws.dynamodb import (put_item,
                                   delete_item,
                                   get_item,
                                   get_primary_key_items,
                                   table_exists,
                                   create_table,
                                   delete_table)
from toil.lib.aws.s3 import (create_bucket,
                             delete_bucket,
                             bucket_exists,
                             bucket_is_registered_with_toil,
                             copy_s3_to_s3,
                             copy_local_to_s3,
                             boto_args,
                             parse_s3_uri,
                             MultiPartPipe)
from toil.lib.ec2nodes import EC2Regions
from toil.lib.checksum import compute_checksum_for_content, compute_checksum_for_file, ChecksumError
from toil.lib.io import AtomicFileCreate
from toil.lib.retry import retry


DEFAULT_AWS_PART_SIZE = 52428800
logger = logging.getLogger(__name__)


class AWSKeyNotFoundError(Exception):
    pass


class AWSKeyAlreadyExistsError(Exception):
    pass


class AWSJobStore(AbstractJobStore):
    """
    The AWS jobstore is an AWS s3 bucket (to store and track files) and an accompanying dynamodb table (file metadata).

    These two components centralize and track all files for the workflow, including inputs/outputs, logs, and the
    jobs themselves (which are pickled and saved as files).

    The dynamodb table tracks file metadata that may be computationally expensive, like the size and md5sum.
    Because the dynamodb table is kept in sync with the files in s3, it can be used as a proxy to check for
    file existence/updates.

    The AWS jobstore stores 3 things:
        - jobs: these are pickled files that contain the information necessary to run a job when unpickled
        - files: the inputs and outputs of jobs, imported into the jobstore
          * input/output files may be shared or unshared
          * shared files need some kind of lock to avoid races
        - logs: the written log files of jobs that have run

    This class also, importantly, inherits self.config, the options/config set by the user.  When jobs are
    loaded/unpickled, they must re-incorporate this.  This also is the source of truth for how things are encrypted.

    :param int part_size: The size of each individual part used for multipart operations like
           upload and copy, must be >= 5 MiB but large enough to not exceed 10k parts for the
           whole file
    """
    # A dummy job ID under which all shared files are stored
    sharedFileOwnerID = '891f7db6-e4d9-4221-a58e-ab6cc4395f94'

    # A dummy job ID under which all unread stats files are stored
    statsFileOwnerID = 'bfcf5286-4bc7-41ef-a85d-9ab415b69d53'

    # A dummy job ID under which all read stats files are stored
    readStatsFileOwnerID = 'e77fc3aa-d232-4255-ae04-f64ee8eb0bfa'

    def __init__(self, locator: str, part_size: int = DEFAULT_AWS_PART_SIZE):
        super(AWSJobStore, self).__init__()

        self.region, self.bucket_name, self.dynamodb_table_name = self.parse_jobstore_identifier(locator)
        logger.debug(f"Instantiating {self.__class__} for region {self.region} with bucket: '{self.bucket_name}'")
        self.locator = locator
        self.partSize = part_size

        self.sse_key, self.encryption_args = self.set_encryption_from_config()

        # these are either created anew during self.initialize() or loaded using self.resume()
        self.table = None
        self.bucket = None

        boto3_session = establish_boto3_session()
        self.s3_resource = boto3_session.resource('s3', region_name=self.region, **boto_args())
        self.s3_client = self.s3_resource.meta.client

        self._batchedUpdates = []

    def set_encryption_from_config(self):
        if self.config.sseKey:
            with open(self.config.sseKey, 'r') as f:
                sse_key = f.read()
            assert len(self.sse_key) == 32
            return sse_key, {'SSECustomerAlgorithm': 'AES256', 'SSECustomerKey': self.sse_key}
        else:
            return None, {}

    def initialize(self, config):
        """Called when starting a new jobstore with a non-existent bucket and dynamodb table."""
        if bucket_exists(self.bucket_name) or table_exists(self.dynamodb_table_name):
            raise JobStoreExistsException(self.locator)
        self.bucket = create_bucket(self.bucket_name, versioning=True)
        self.table = create_table(self.dynamodb_table_name)
        super(AWSJobStore, self).initialize(config)

    def resume(self):
        """Called when reusing an old jobstore with an existing bucket and dynamodb table."""
        if self.bucket is None:
            self.bucket = bucket_exists(self.bucket_name)
            self.table = table_exists(self.dynamodb_table_name)
        if not self.bucket or not self.table:
            # TODO: allow option here to rebuild db from bucket
            raise NoSuchJobStoreException(self.locator)
        super(AWSJobStore, self).resume()

    def unpickle_job(self, job_id: str):
        """Use a job_id to unpickle and return a job from the jobstore's s3 bucket."""
        with self.readFileStream(job_id) as fh:
            job = pickle.loads(fh.read())
        if job is not None:
            job.assignConfig(self.config)
        return job

    def pickle_job(self, job) -> str:
        """Pickle a job object, save it in the jobstore's s3 bucket, and return its job_id reference."""
        with self.writeFileStream() as (writable, job_id):
            writable.write(pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL))
        return job_id

    @contextmanager
    def batch(self):
        yield
        for jobDescription in self._batchedUpdates:
            self.pickle_job(jobDescription)
        self._batchedUpdates = []

    def assignID(self, jobDescription):
        jobDescription.jobStoreID = str(uuid.uuid4())
        cmd = '<no command>' if jobDescription.command is None else jobDescription.command
        logger.debug(f"Assigning ID to job {jobDescription.jobStoreID} for '{cmd}'")

    def create(self, jobDescription):
        self.pickle_job(jobDescription)
        put_item(table=self.table, hash_key='jobs', sort_key=jobDescription.jobStoreID)
        return jobDescription

    def job_exists(self, job_id: str, check: bool = False):
        """Checks if the job_id is found in s3."""
        if get_item(table=self.table, hash_key='jobs', sort_key=job_id):
            return True
        if check:
            raise NoSuchJobException(job_id)
        return False

    def jobs(self):
        for job_id in get_primary_key_items(table=self.table, key='jobs', return_key='sort_key'):
            yield self.unpickle_job(job_id)

    def load_job(self, job_id: str):
        job = self.unpickle_job(job_id)
        if job is None:
            raise NoSuchJobException(job_id)
        return job

    def update_job(self, jobDescription):
        self.pickle_job(jobDescription)

    def delete_job(self, jobStoreID: str):
        logger.debug("Deleting job %s", jobStoreID)
        associated_files = get_item(table=self.table, hash_key='jobs', sort_key=jobStoreID)
        delete_item(table=self.table, hash_key='jobs', sort_key=jobStoreID)
        logger.debug("Deleting %d file(s) associated with job %s", len(associated_files), jobStoreID)
        for item in associated_files:
            # TODO: Delete each item from dynamodb 'files' table?
            self.s3_client.delete_object({'Bucket': self.bucket.name, 'Key': compat_bytes(item.name)})

    def getEmptyFileStoreID(self, jobStoreID=None, cleanup=False, basename=None):
        new_file_id = str(uuid.uuid4())
        metadata = json.loads({'owner': jobStoreID if cleanup else None,
                               'encrypt': self.sse_key})
        put_item(table=self.table, hash_key='files', sort_key=new_file_id, value=metadata)
        self.s3_client.upload_fileobj(Bucket=self.bucket_name,
                                      Key=new_file_id,
                                      Fileobj=BytesIO(b''),
                                      ExtraArgs=self.encryption_args)

        # use head_object with the SSE headers content_length
        response = self.s3_client.head_object(Bucket=self.bucket_name,
                                              Key=new_file_id,
                                              **self.encryption_args)
        assert 0 == response.get('ContentLength', None)
        return new_file_id

    def _importFile(self, otherCls, url, sharedFileName=None, hardlink=False, symlink=False) -> FileID:
        """
        Upload a file into the s3 bucket jobstore from the source uri and store the file's
        metadata inside of dynamodb.

        This db entry's existence should always be in sync with the file's existence (when one exists,
        so must the other).
        """
        # we are uploading from s3 to s3
        if issubclass(otherCls, AWSJobStore):
            # make sure the object exists and provide access to s3_blob.content_length
            s3_blob = self._getObjectForUrl(url, existing=True)

            file_id = str(uuid.uuid4()) if not sharedFileName else self._sharedFileID(sharedFileName)
            owner_id = s3_blob.key if not sharedFileName else self.sharedFileOwnerID

            # stow file's metadata in db
            metadata = json.loads(dict(file_id=file_id, owner_id=owner_id, sse_key_path=self.sse_key))
            put_item(table=self.table, hash_key='files', sort_key=file_id, value=metadata)

            # upload the uri to our s3 jobstore bucket
            # , sse_key=self.sse_key
            copy_s3_to_s3(src_bucket=s3_blob.bucket_name, src_key=s3_blob.key,
                          dst_bucket=self.bucket_name, dst_key=file_id)
            return FileID(fileStoreID=file_id, size=s3_blob.content_length) if sharedFileName is None else None
        else:
            return super(AWSJobStore, self)._importFile(otherCls, url, sharedFileName=sharedFileName)

    def _exportFile(self, otherCls, jobStoreFileID, url) -> None:
        if issubclass(otherCls, AWSJobStore):
            s3_blob = self._getObjectForUrl(url)
            # fail if db fetch does not exist
            # needed?
            get_item(table=self.table, hash_key='files', sort_key=jobStoreFileID)
            # upload the uri to our s3 jobstore bucket
            # , sse_key=self.sse_key
            copy_s3_to_s3(src_bucket=self.bucket_name, src_key=jobStoreFileID,
                          dst_bucket=s3_blob.bucket_name, dst_key=s3_blob.key)
        else:
            super(AWSJobStore, self)._defaultExportFile(otherCls, jobStoreFileID, url)

    @classmethod
    def _readFromUrl(cls, url, writable):
        srcObj = cls._getObjectForUrl(url, existing=True)
        srcObj.download_fileobj(writable)
        executable = False
        return srcObj.content_length, executable

    def _writeToUrl(self, readable, url, executable=False):
        dstObj = self._getObjectForUrl(url)
        uploadFile(readable=readable,
                   resource=self.s3_resource,
                   bucketName=dstObj.bucket_name,
                   fileID=dstObj.key,
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
        return url.scheme.lower() == 's3'

    def create_metadata(self, file_id: str, file: Optional[str] = None, owner_id: Optional[str] = None):
        """Crunch metadata for a local file, or an empty template if no file provided."""
        checksum = compute_checksum_for_file(file) if file else None
        file_size = os.stat(file).st_size if file else None
        return dict(checksum=checksum,
                    file_size=file_size,
                    encryption=self.sse_key,
                    owner_id=owner_id,
                    file_id=file_id)

    def writeFile(self, localFilePath, jobStoreID=None, cleanup=False):
        owner_id = jobStoreID if cleanup else None
        file_id = str(uuid.uuid4())
        copy_local_to_s3(local_file_path=localFilePath, dst_bucket=self.bucket_name, dst_key=file_id)
        metadata = json.dumps(self.create_metadata(file=localFilePath, file_id=file_id, owner_id=owner_id))
        put_item(table=self.table, hash_key='files', sort_key=file_id, value=metadata)
        return file_id

    @contextmanager
    def writeFileStream(self, jobStoreID=None, cleanup=False, basename=None, encoding=None, errors=None):
        owner_id = jobStoreID if cleanup else None
        file_id = str(uuid.uuid4())
        metadata = json.dumps(self.create_metadata(file=None, file_id=file_id, owner_id=owner_id))

        pipe = MultiPartPipe(encoding=encoding, errors=errors)
        with pipe as writable:
            yield writable, file_id
        put_item(table=self.table, hash_key='files', sort_key=file_id, value=metadata)

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None, encoding=None, errors=None):
        self._requireValidSharedFileName(sharedFileName)
        info = self.FileInfo.loadOrCreate(jobStoreFileID=self._sharedFileID(sharedFileName),
                                          ownerID=str(self.sharedFileOwnerID),
                                          encrypted=isProtected)
        with info.uploadStream(encoding=encoding, errors=errors) as writable:
            yield writable
        info.save()

    def updateFile(self, jobStoreFileID, localFilePath):
        # fetch database entry containing metadata for the file
        metadata = get_item(table=self.table, hash_key='files', sort_key=jobStoreFileID)
        # upload the local file to s3; verify checksums match
        copy_local_to_s3(localFilePath, dst_bucket=self.bucket_name, dst_key=metadata['key_name'])
        # update the database entry

    @contextmanager
    def updateFileStream(self, jobStoreFileID, encoding=None, errors=None):
        info = self.FileInfo.loadOrFail(jobStoreFileID)
        with info.uploadStream(encoding=encoding, errors=errors) as writable:
            yield writable
        info.save()

    def fileExists(self, jobStoreFileID):
        return AWSFile.exists(jobStoreFileID)

    def getFileSize(self, jobStoreFileID: str) -> int:
        try:
            return self._getObjectForUrl(f's3://{self.bucket_name}/{jobStoreFileID}', existing=True).content_length
        except AWSKeyNotFoundError:
            return 0

    def readFile(self, jobStoreFileID, localFilePath, symlink=False):
        obj = self._getObjectForUrl(f's3://{self.bucket_name}/{jobStoreFileID}', existing=True)
        metadata = get_item(table=self.table, hash_key='files', sort_key=jobStoreFileID)

        with AtomicFileCreate(localFilePath) as tmpPath:
            obj.download_file(Filename=tmpPath, ExtraArgs={**self.encryption_args})

        previously_computed_checksum = metadata.get('checksum')
        if not self.config.disableJobStoreChecksumVerification and previously_computed_checksum:
            algorithm, expected_checksum = previously_computed_checksum.split('$')
            checksum = compute_checksum_for_file(localFilePath, algorithm=algorithm)
            if previously_computed_checksum != checksum:
                raise ChecksumError(f'Checksum mismatch for file {localFilePath}.  Expected: {self.checksum} Actual: {checksum}')
        if getattr(jobStoreFileID, 'executable', False):
            os.chmod(localFilePath, os.stat(localFilePath).st_mode | stat.S_IXUSR)

    @contextmanager
    def readFileStream(self, jobStoreFileID, encoding=None, errors=None):
        info = self.FileInfo.loadOrFail(jobStoreFileID)
        logger.debug("Reading %r into stream.", info)
        with info.downloadStream(encoding=encoding, errors=errors) as readable:
            yield readable

    @contextmanager
    def readSharedFileStream(self, sharedFileName, encoding=None, errors=None):
        self._requireValidSharedFileName(sharedFileName)
        jobStoreFileID = self._sharedFileID(sharedFileName)
        info = self.FileInfo.loadOrFail(jobStoreFileID, customName=sharedFileName)
        logger.debug("Reading %r for shared file %r into stream.", info, sharedFileName)
        with info.downloadStream(encoding=encoding, errors=errors) as readable:
            yield readable

    def deleteFile(self, file_id):
        info = AWSFile.load(file_id)
        if info:
            info.delete()

    def writeStatsAndLogging(self, log_msg: Union[bytes, str]):
        if isinstance(log_msg, str):
            log_msg = log_msg.encode('utf-8', errors='ignore')
        file_obj = BytesIO(log_msg)

        info = AWSFile.create(self.statsFileOwnerID)
        info.checksum = compute_checksum_for_content(file_obj)

        self.s3_client.upload_fileobj(Bucket=self.bucket_name,
                                      Key=compat_bytes(info.fileID),
                                      Fileobj=BytesIO(log_msg),
                                      ExtraArgs=self.encryption_args)

        # use head_object with the SSE headers to access versionId and content_length attributes
        response = self.s3_client.head_object(Bucket=self.bucket_name,
                                               Key=compat_bytes(info.fileID),
                                               **self.encryption_args)
        # TODO: raise if no ContentLength?
        assert len(log_msg) == response.get('ContentLength', None)
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

    def _readStatsAndLogging(self, callback, ownerId=None):
        for item in get_primary_key_items(table=self.table, key='logs'):
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
        delete_bucket(self.bucket_name)
        delete_table(self.dynamodb_table_name)

    def parse_jobstore_identifier(self, jobstore_identifier: str) -> Tuple[str, str, str]:
        region, jobstore_name = jobstore_identifier.split(':')

        bucket_name = dynamodb_table_name = f'{jobstore_name}--toil'

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
            response = self.s3_client.get_bucket_location(Bucket=bucket_name)
            if response["LocationConstraint"] != region:
                raise ValueError(f'Bucket region conflict.  Bucket already exists in region '
                                 f'"{response["LocationConstraint"]}" but "{region}" was specified '
                                 f'in the jobstore_identifier: {jobstore_identifier}')
        return region, bucket_name, dynamodb_table_name
