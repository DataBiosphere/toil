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
from toil.lib.compatibility import compat_bytes
from toil.lib.aws.credentials import resource
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
                             MultiPartPipe,
                             generate_presigned_url,
                             list_s3_items,
                             uploadFile,
                             download_stream)
from toil.lib.conversions import modify_url
from toil.lib.ec2nodes import EC2Regions
from toil.lib.checksum import compute_checksum_for_file, ChecksumError
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

    The dynamodb table tracks file metadata, like the size and md5sum, to verify file integrity when moving around.
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

    :param int part_size: The size of each individual part used for multipart operations like upload and copy,
                          must be >= 5 MiB but large enough to not exceed 10k parts for the whole file.
    """
    # A dummy job ID under which all shared files are stored
    sharedFileOwnerID = '891f7db6-e4d9-4221-a58e-ab6cc4395f94'

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

        self.s3_resource = resource('s3', region_name=self.region, **boto_args())
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
        self.bucket = create_bucket(self.bucket_name)
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
            self.create(jobDescription)
        self._batchedUpdates = []

    def assign_job_id(self, jobDescription):
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
        metadata = self.create_metadata(file=None, file_id=new_file_id, owner_id=jobStoreID if cleanup else None)
        put_item(table=self.table, hash_key='files', sort_key=new_file_id, value=json.dumps(metadata))
        self.s3_client.upload_fileobj(Bucket=self.bucket_name,
                                      Key=new_file_id,
                                      Fileobj=BytesIO(b''),
                                      ExtraArgs=self.encryption_args)

        # use head_object with the SSE headers to fetch content_length
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
            metadata = dict(file_id=file_id, owner_id=owner_id, sse_key_path=self.sse_key)
            put_item(table=self.table, hash_key='files', sort_key=file_id, value=json.dumps(metadata))

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
                   s3_resource=self.s3_resource,
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

    def create_metadata(self, file_id: str, file: Optional[str] = None, owner_id: Optional[str] = None, encrypted: Optional[bool] = None):
        """Crunch metadata for a local file, or an empty template if no file provided."""
        checksum = compute_checksum_for_file(file) if file else None
        file_size = os.stat(file).st_size if file else None
        return dict(checksum=checksum,
                    file_size=file_size,
                    encryption=bool(self.sse_key) if encrypted is None else encrypted,
                    owner_id=owner_id,
                    file_id=file_id)

    def writeFile(self, localFilePath, jobStoreID=None, cleanup=False):
        owner_id = jobStoreID if cleanup else None
        file_id = str(uuid.uuid4())
        copy_local_to_s3(local_file_path=localFilePath, dst_bucket=self.bucket_name, dst_key=file_id)
        metadata = self.create_metadata(file=localFilePath, file_id=file_id, owner_id=owner_id)
        put_item(table=self.table, hash_key='files', sort_key=file_id, value=json.dumps(metadata))
        return file_id

    @contextmanager
    def writeFileStream(self, jobStoreID=None, cleanup=False, basename=None, encoding=None, errors=None):
        owner_id = jobStoreID if cleanup else None
        file_id = str(uuid.uuid4())
        metadata = self.create_metadata(file=None, file_id=file_id, owner_id=owner_id)

        pipe = MultiPartPipe(encoding=encoding,
                             errors=errors,
                             part_size=self.partSize,
                             s3_client=self.s3_client,
                             bucket_name=self.bucket_name,
                             file_id=file_id,
                             encryption_args=self.encryption_args)
        with pipe as writable:
            yield writable, file_id
        put_item(table=self.table, hash_key='files', sort_key=file_id, value=json.dumps(metadata))

    # determine isProtected from presence of sse_path?  why is this an input here?
    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None, encoding=None, errors=None):
        self._requireValidSharedFileName(sharedFileName)
        file_id = self._sharedFileID(sharedFileName)
        metadata = self.create_metadata(file=None,
                                        file_id=file_id,
                                        owner_id=str(self.sharedFileOwnerID),
                                        encrypted=isProtected)
        pipe = MultiPartPipe(encoding=encoding,
                             errors=errors,
                             part_size=self.partSize,
                             s3_client=self.s3_client,
                             bucket_name=self.bucket_name,
                             file_id=file_id,
                             encryption_args=self.encryption_args)
        with pipe as writable:
            yield writable
        put_item(table=self.table, hash_key='files', sort_key=file_id, value=json.dumps(metadata))

    def updateFile(self, jobStoreFileID, localFilePath):
        # fetch database entry containing metadata for the file
        metadata = get_item(table=self.table, hash_key='files', sort_key=jobStoreFileID)
        # upload the local file to s3; verify checksums match
        copy_local_to_s3(localFilePath, dst_bucket=self.bucket_name, dst_key=metadata['key_name'])
        # update the database entry

    @contextmanager
    def updateFileStream(self, jobStoreFileID, encoding=None, errors=None):
        metadata = get_item(table=self.table, hash_key='files', sort_key=jobStoreFileID)
        pipe = MultiPartPipe(encoding=encoding,
                             errors=errors,
                             part_size=self.partSize,
                             s3_client=self.s3_client,
                             bucket_name=self.bucket_name,
                             file_id=jobStoreFileID,
                             encryption_args=self.encryption_args)
        with pipe as writable:
            yield writable

    def fileExists(self, jobStoreFileID):
        try:
            self._getObjectForUrl(f's3://{self.bucket_name}/{jobStoreFileID}', existing=True)
            return True
        except AWSKeyNotFoundError:
            return False

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
                raise ChecksumError(f'Checksum mismatch for file {localFilePath}.  '
                                    f'Expected: {previously_computed_checksum} Actual: {checksum}')
        if getattr(jobStoreFileID, 'executable', False):
            os.chmod(localFilePath, os.stat(localFilePath).st_mode | stat.S_IXUSR)

    @contextmanager
    def readFileStream(self, jobStoreFileID, encoding=None, errors=None):
        metadata = get_item(table=self.table, hash_key='files', sort_key=jobStoreFileID)
        obj = self._getObjectForUrl(f'{self.bucket_name}/{jobStoreFileID}')
        logger.debug("Reading into stream.")
        with download_stream(s3_object=obj,
                             checksum_to_verify=metadata['checksum'],
                             extra_args=self.encryption_args,
                             encoding=encoding,
                             errors=errors) as readable:
            yield readable

    @contextmanager
    def readSharedFileStream(self, sharedFileName, encoding=None, errors=None):
        self._requireValidSharedFileName(sharedFileName)
        jobStoreFileID = self._sharedFileID(sharedFileName)
        metadata = get_item(table=self.table, hash_key='files', sort_key=jobStoreFileID)
        obj = self._getObjectForUrl(f'{self.bucket_name}/{jobStoreFileID}')
        with download_stream(s3_object=obj,
                             checksum_to_verify=metadata['checksum'],
                             extra_args=self.encryption_args,
                             encoding=encoding,
                             errors=errors) as readable:
            yield readable

    def deleteFile(self, file_id):
        # delete metadata reference in database
        delete_item(table=self.table, hash_key='files', sort_key=file_id)
        # delete actual file in bucket
        self.s3_client.delete_object(Bucket=self.bucket.name, Key=file_id)

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

        We then return the database items in case we want to use the information to transition unread items to read items.
        """
        prefix = 'logs/' if readAll else 'logs/unread/'
        itemsProcessed = 0
        for log in list_s3_items(bucket=self.bucket.name, prefix=prefix):
            obj = self._getObjectForUrl(f'{self.bucket.name}/{log}')
            with download_stream(s3_object=obj) as readable:
                callback(readable)
            if not readAll:
                # move unread logs to read; tag instead?
                pass
            itemsProcessed += 1
        return itemsProcessed

    # TODO: Make this retry more specific?
    #  example: https://github.com/DataBiosphere/toil/issues/3378
    @retry()
    def getPublicUrl(self, jobStoreFileID: str):
        """Turn s3:// into http:// and put a public-read ACL on it."""
        metadata = get_item(table=self.table, hash_key='files', sort_key=jobStoreFileID)
        self.bucket.Object(jobStoreFileID).Acl().put(ACL='public-read')
        url = generate_presigned_url(bucket=self.bucket.name,
                                     key_name=jobStoreFileID,
                                     expiration=self.publicUrlExpiration.total_seconds())
        # boto doesn't properly remove the x-amz-security-token parameter when
        # query_auth is False when using an IAM role (see issue #2043). Including the
        # x-amz-security-token parameter without the access key results in a 403,
        # even if the resource is public, so we need to remove it.
        # TODO: verify that this is still the case
        url = modify_url(url, remove=['x-amz-security-token', 'AWSAccessKeyId', 'Signature'])
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
