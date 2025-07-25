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

Reasons for this
 - DRY.
 - All retries are on their individual boto functions, instead of here.
 - Simple clear functions => simple clear unit tests (ideally).

Variables defining part size, parallelization, and other constants should live in toil.lib.aws.config.
"""
import os
import json
import logging
import pickle
import re
import stat
import uuid
import datetime

from io import BytesIO
from contextlib import contextmanager
from urllib.parse import ParseResult, urlparse
from typing import (
    ContextManager,
    IO,
    TYPE_CHECKING,
    Optional,
    Union,
    cast,
    Tuple,
    Callable,
    Dict,
    Any,
    Iterator,
    Literal,
    overload
)

# This file can't be imported if the AWS modules are not available.
from botocore.exceptions import ClientError

from toil.fileStores import FileID
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             JobStoreExistsException,
                                             NoSuchJobException,
                                             NoSuchJobStoreException)
from toil.lib.aws.s3 import (
    create_s3_bucket,
    delete_s3_bucket,
    bucket_exists,
    copy_s3_to_s3,
    copy_local_to_s3,
    copy_s3_to_local,
    parse_s3_uri,
    MultiPartPipe,
    list_s3_items,
    upload_to_s3,
    download_stream,
    s3_key_exists,
    head_s3_object,
    get_s3_object,
    put_s3_object,
    create_public_url,
    AWSKeyNotFoundError,
)
from toil.lib.aws.utils import get_object_for_url, list_objects_for_url
from toil.common import Config
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.lib.ec2nodes import EC2Regions
from toil.lib.retry import get_error_status
from toil.version import version
from toil.lib.aws.session import establish_boto3_session
from toil.job import JobDescription, Job
from toil.lib.url import URLAccess


DEFAULT_AWS_PART_SIZE = 52428800
logger = logging.getLogger(__name__)


class AWSJobStore(AbstractJobStore, URLAccess):
    """
    The AWS jobstore can be thought of as an AWS s3 bucket, with functions to
    centralize, store, and track files for the workflow.

    The AWS jobstore stores 4 things:
        
    1. Jobs: These are pickled as files, and contain the information necessary to run a job when unpickled.
       A job's file is deleted when finished, and its absence means it completed.
    
    2. Files: The inputs and outputs of jobs.  Each file is written in s3 with the file pattern:
       "files/{uuid4}/0/{original_filename}" or "files/{uuid4}/1/{original_filename}", where the file prefix
       "files/{uuid4}" should only point to one file.
       where 0 or 1 represent its executability (1 == executable).
    3. Logs: The written log files of jobs that have run, plus the log file for the main Toil process.
    
    4. Shared Files: Files with himan=-readable names, used by Toil itself or Python workflows.
       These include:
       
       * environment.pickle   (environment variables)
       
       * config.pickle        (user options)
       
       * pid.log              (process ID of the workflow; when it finishes, the workflow either succeeded/failed)
       * userScript           (hot deployment;  this is the job module)
       
       * rootJobReturnValue   (workflow succeeded or not)

    NOTES
     - The AWS jobstore does not use a database (directly, at least) currently.  We can get away with this because:
       
       1. AWS s3 has strong consistency.
       
       2. s3's filter/query speed is pretty good.
       
       However, there may be reasons in the future to provide users with a database:
       
       * s3 throttling has limits (3,500/5,000 requests (TODO: per
         second?); something like dynamodb supports 100,000+ requests).
       
       * Access and filtering would be sped up, though how much faster this would be needs testing.
       
       ALSO NOTE: The caching filestore uses a local (per node) database with a very similar structure that maybe
       could be synced up with this.

     - TODO: Etags are s3's native checksum, so use that for file integrity checking since it's free when fetching
       object headers from s3.  Using an md5sum in addition to this would work well with the current filestore.
       WARNING: Etag values differ for the same file when the part size changes, so part size should always
       be Set In Stone, unless we hit s3's 10,000 part limit, and we need to account for that.

     - This class fills in self.config only when initialized/restarted; it is None upon class instantiation.  These
       are the options/config set by the user.  When jobs are loaded/unpickled, they must re-incorporate this.
       The config.sse_key is the source of truth for bucket encryption and a clear error should be raised if
       restarting a bucket with a different encryption key set than it was initialized with.

     - TODO: In general, job stores should log the version of Toil they were
       initialized with and warn the user if restarting with a different
       version.
    """
    def __init__(self, locator: str, partSize: int = DEFAULT_AWS_PART_SIZE) -> None:
        super(AWSJobStore, self).__init__(locator)
        # TODO: parsing of user options seems like it should be done outside of this class;
        #  pass in only the bucket name and region?
        self.region, self.bucket_name = parse_jobstore_identifier(locator)
        boto3_session = establish_boto3_session(region_name=self.region)
        self.s3_resource = boto3_session.resource("s3")
        self.s3_client = boto3_session.client("s3")
        logger.info(f"Instantiating {self.__class__} with region: {self.region}")
        self.part_size = DEFAULT_AWS_PART_SIZE  # don't let users set the part size; it will throw off etag values

        # created anew during self.initialize() or loaded using self.resume()
        self.bucket = None

        # pickled job files named with uuid4
        self.job_key_prefix = 'jobs/'
        # job-file associations; these are empty files mimicking a db w/naming convention: job_uuid4.file_uuid4
        #
        # TODO: a many-to-many system is implemented, but a simpler one-to-many
        # system could be used, because each file should belong to at most one
        # job. This should be changed to a hierarchical layout.
        self.job_associations_key_prefix = 'job-associations/'
        # input/output files named with uuid4
        self.content_key_prefix = 'files/'
        # these are special files, like 'environment.pickle'; place them in root
        self.shared_key_prefix = ''
        # read and unread; named with uuid4
        self.logs_key_prefix = 'logs/'

        # encryption is not set until self.initialize() or self.resume() are called
        self.sse_key: Optional[str] = None
        self.encryption_args: Dict[str, Any] = {}

        ###################################### CREATE/DESTROY JOBSTORE ######################################

    def initialize(self, config: Config) -> None:
        """
        Called when starting a new jobstore with a non-existent bucket.

        Create bucket, raise if it already exists.
        Set options from config.  Set sse key from that.
        """
        logger.debug(f"Instantiating {self.__class__} for region {self.region} with bucket: '{self.bucket_name}'")
        self.configure_encryption(config.sseKey)
        if bucket_exists(self.s3_resource, self.bucket_name):
            raise JobStoreExistsException(self.locator, 'aws')
        self.bucket = create_s3_bucket(self.s3_resource, self.bucket_name, region=self.region)  # type: ignore
        super(AWSJobStore, self).initialize(config)

    def resume(self, sse_key_path: Optional[str] = None) -> None:
        """Called when reusing an old jobstore with an existing bucket.  Raise if the bucket doesn't exist."""
        super(AWSJobStore, self).resume(sse_key_path)  # this sets self.config to not be None and configures encryption
        if not bucket_exists(self.s3_resource, self.bucket_name):
            raise NoSuchJobStoreException(self.locator, 'aws')

    def destroy(self) -> None:
        delete_s3_bucket(self.s3_resource, self.bucket_name)

        ###################################### BUCKET UTIL API ######################################

    def write_to_bucket(
            self,
            identifier: str,
            prefix: str,
            data: Optional[Union[bytes, str, Dict[str, Any]]],
            bucket: Optional[str] = None,
            encrypted: bool = True
    ) -> None:
        """Use for small files.  Does not parallelize or use multipart."""
        # only used if exporting to a URL
        encryption_args = {} if not encrypted else self.encryption_args
        bucket = bucket or self.bucket_name

        if isinstance(data, dict):
            data = json.dumps(data).encode('utf-8')
        elif isinstance(data, str):
            data = data.encode('utf-8')
        elif data is None:
            data = b''

        assert isinstance(data, bytes)
        put_s3_object(s3_resource=self.s3_resource,
                      bucket=bucket,
                      key=f'{prefix}{identifier}',
                      body=data,
                      extra_args=encryption_args)

    def read_from_bucket(self, identifier: str, prefix: str) -> bytes:
        """Use for small files.  Does not parallelize or use multipart."""
        try:
            return get_s3_object(s3_resource=self.s3_resource,
                                 bucket=self.bucket_name,
                                 key=f'{prefix}{identifier}',
                                 extra_args=self.encryption_args)['Body'].read()
        except self.s3_client.exceptions.NoSuchKey:
            if prefix == self.job_key_prefix:
                raise NoSuchJobException(identifier)
            elif prefix == self.content_key_prefix:
                raise NoSuchFileException(identifier)
            else:
                raise
        except ClientError as e:
            if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                if prefix == self.job_key_prefix:
                    raise NoSuchJobException(identifier)
                elif prefix == self.content_key_prefix:
                    raise NoSuchFileException(identifier)
                else:
                    raise
            else:
                raise

        ###################################### JOBS API ######################################

    def assign_job_id(self, jobDescription: JobDescription) -> None:
        jobDescription.jobStoreID = str(uuid.uuid4())
        logger.debug("Assigning Job ID %s", jobDescription.jobStoreID)

    def create_job(self, jobDescription: JobDescription) -> JobDescription:
        """Pickle a jobDescription object and write it to the jobstore as a file."""
        self.write_to_bucket(identifier=str(jobDescription.jobStoreID),
                             prefix=self.job_key_prefix,
                             data=pickle.dumps(jobDescription, protocol=pickle.HIGHEST_PROTOCOL))
        return jobDescription

    def job_exists(self, job_id: str, check: bool = False) -> bool:
        """
        Checks if the job_id is found in s3.

        :param check: If True, raise an exception instead of returning false
            when a job does not exist.
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=f'{self.job_key_prefix}{job_id}', **self.encryption_args)
            return True
        except ClientError as e:
            if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                if check:
                    raise NoSuchJobException(job_id)
            else:
                raise
        except self.s3_client.exceptions.NoSuchKey:
            if check:
                raise NoSuchJobException(job_id)
            else:
                raise
        return False

    def jobs(self) -> Iterator[JobDescription]:
        for result in list_s3_items(self.s3_resource, bucket=self.bucket_name, prefix=self.job_key_prefix):
            try:
                job_id = result['Key'][len(self.job_key_prefix):]  # strip self.job_key_prefix
                yield self.load_job(job_id)
            except NoSuchJobException:
                # job may have been deleted between showing up in the list and getting loaded
                pass

    def load_job(self, job_id: str) -> JobDescription:
        """Use a job_id to get a job from the jobstore's s3 bucket, unpickle, and return it."""
        try:
            job = pickle.loads(self.read_from_bucket(identifier=job_id, prefix=self.job_key_prefix))
        except NoSuchJobException:
            raise

        if not isinstance(job, JobDescription):
            raise RuntimeError(
                f"While trying to load a JobDescription for {job_id}, got a {type(job)} instead!",
            )

        # Now we know it's the right type
        job.assignConfig(self.config)
        return job

    def update_job(self, jobDescription: JobDescription) -> None:
        self.create_job(jobDescription)

    def delete_job(self, job_id: str) -> None:
        logger.debug("Deleting job %s", job_id)

        # delete the actual job file
        self.s3_client.delete_object(Bucket=self.bucket_name, Key=f'{self.job_key_prefix}{job_id}')

        # delete any files marked as associated with the job
        job_file_associations_to_delete = []
        for associated_job_file in list_s3_items(self.s3_resource,
                                                 bucket=self.bucket_name,
                                                 prefix=f'{self.job_associations_key_prefix}{job_id}'):
            job_file_associations_to_delete.append(associated_job_file['Key'])
            file_id = associated_job_file['Key'].split('.')[-1]
            self.delete_file(file_id)

        # delete the job-file association references (these are empty files the simply connect jobs to files)
        for job_file_association in job_file_associations_to_delete:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=f'{job_file_association}')

    def associate_job_with_file(self, job_id: str, file_id: str) -> None:
        # associate this job with this file; the file will be deleted when the job is
        self.write_to_bucket(identifier=f'{job_id}.{file_id}', prefix=self.job_associations_key_prefix, data=None)

        ###################################### FILES API ######################################

    def write_file(self, local_path: str, job_id: Optional[str] = None, cleanup: bool = False) -> FileID:
        """
        Write a local file into the jobstore and return a file_id referencing it.

        :param job_id:
            If job_id AND cleanup are supplied, associate this file with that job.  When the job is deleted, the
            file will be deleted as well.

        :param cleanup:
            If job_id AND cleanup are supplied, associate this file with that job.  When the job is deleted, the
            file will be deleted as well.
            TODO: we don't need cleanup; remove it and only use job_id
        """
        # TODO: etag = compute_checksum_for_file(local_path, algorithm='etag')[len('etag$'):]
        file_id = str(uuid.uuid4())  # mint a new file_id
        file_attributes = os.stat(local_path)
        size = file_attributes.st_size
        executable = file_attributes.st_mode & stat.S_IXUSR != 0

        if job_id and cleanup:
            # associate this job with this file; then the file reference will be deleted when the job is
            self.associate_job_with_file(job_id, file_id)

        copy_local_to_s3(
            s3_resource=self.s3_resource,
            local_file_path=local_path,
            dst_bucket=self.bucket_name,
            dst_key=f'{file_id}/{1 if executable else 0}/{os.path.basename(local_path)}',
            extra_args=self.encryption_args
        )
        return FileID(file_id, size, executable)

    def find_s3_key_from_file_id(self, file_id: str) -> str:
        """This finds an s3 key for which file_id is the prefix, and which already exists."""
        prefix = f'{self.content_key_prefix}{file_id}'
        s3_keys = [s3_item for s3_item in list_s3_items(self.s3_resource, bucket=self.bucket_name, prefix=prefix)]
        if len(s3_keys) == 0:
            raise NoSuchFileException(f'File ID: {file_id} not found!')
        if len(s3_keys) > 1:
            # There can be only one.
            raise RuntimeError(f'File ID: {file_id} should be unique, but includes: {s3_keys}')
        return s3_keys[0]['Key']

    @contextmanager
    def write_file_stream(
        self,
        job_id: Optional[str] = None,
        cleanup: bool = False,
        basename: Optional[str] = None,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Iterator[tuple[IO[bytes], str]]:
        file_id = str(uuid.uuid4())
        if job_id and cleanup:
            self.associate_job_with_file(job_id, file_id)
        pipe = MultiPartPipe(part_size=self.part_size,
                             s3_resource=self.s3_resource,
                             bucket_name=self.bucket_name,
                             file_id=f'{self.content_key_prefix}{file_id}/0/{str(basename)}',
                             encryption_args=self.encryption_args,
                             encoding=encoding,
                             errors=errors)
        with pipe as writable:
            yield writable, file_id

    @contextmanager
    def update_file_stream(
            self,
            file_id: str,
            encoding: Optional[str] = None,
            errors: Optional[str] = None
    ) -> Iterator[IO[Any]]:
        pipe = MultiPartPipe(part_size=self.part_size,
                             s3_resource=self.s3_resource,
                             bucket_name=self.bucket_name,
                             file_id=self.find_s3_key_from_file_id(file_id),
                             encryption_args=self.encryption_args,
                             encoding=encoding,
                             errors=errors)
        with pipe as writable:
            yield writable

    @contextmanager
    def write_shared_file_stream(
        self,
        shared_file_name: str,
        encrypted: Optional[bool] = None,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Iterator[IO[bytes]]:
        pipe = MultiPartPipe(part_size=self.part_size,
                             s3_resource=self.s3_resource,
                             bucket_name=self.bucket_name,
                             file_id=f'{self.shared_key_prefix}{shared_file_name}',
                             encryption_args=self.encryption_args,
                             encoding=encoding,
                             errors=errors)
        with pipe as writable:
            yield writable

    def update_file(self, file_id: str, local_path: str) -> None:
        copy_local_to_s3(
            s3_resource=self.s3_resource,
            local_file_path=local_path,
            dst_bucket=self.bucket_name,
            dst_key=self.find_s3_key_from_file_id(file_id),
            extra_args=self.encryption_args
        )

    def file_exists(self, file_id: str) -> bool:
        try:
            # This throws if the file doesn't exist.
            self.find_s3_key_from_file_id(file_id)
        except NoSuchFileException:
            # It didn't exist
            return False
        return True

    def get_file_size(self, file_id: str) -> int:
        """Do we need both get_file_size and _get_size???"""
        full_s3_key = self.find_s3_key_from_file_id(file_id)
        return self._get_size(url=urlparse(f's3://{self.bucket_name}/{full_s3_key}')) or 0

    @classmethod
    def _get_size(cls, url: ParseResult) -> Optional[int]:
        """Do we need both get_file_size and _get_size???"""
        try:
            return get_object_for_url(url, existing=True).content_length
        except (AWSKeyNotFoundError, NoSuchFileException):
            return 0

    def read_file(self, file_id: str, local_path: str, symlink: bool = False) -> None:
        full_s3_key = self.find_s3_key_from_file_id(file_id)
        executable = int(full_s3_key.split('/')[1])  # 0 or 1
        try:
            copy_s3_to_local(
                s3_resource=self.s3_resource,
                local_file_path=local_path,
                src_bucket=self.bucket_name,
                src_key=full_s3_key,
                extra_args=self.encryption_args
            )
            if executable:
                os.chmod(local_path, os.stat(local_path).st_mode | stat.S_IXUSR)
        except self.s3_client.exceptions.NoSuchKey:
            raise NoSuchFileException(file_id)
        except ClientError as e:
            if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                raise NoSuchFileException(file_id)

    @contextmanager  # type: ignore
    def read_file_stream(  # type: ignore
        self,
        file_id: Union[FileID, str],
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Union[ContextManager[IO[bytes]], ContextManager[IO[str]]]:
        full_s3_key = self.find_s3_key_from_file_id(file_id)
        try:
            with download_stream(self.s3_resource,
                                 bucket=self.bucket_name,
                                 key=full_s3_key,
                                 extra_args=self.encryption_args,
                                 encoding=encoding,
                                 errors=errors) as readable:
                yield readable
        except self.s3_client.exceptions.NoSuchKey:
            raise NoSuchFileException(file_id)
        except ClientError as e:
            if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                raise NoSuchFileException(file_id)
            else:
                raise

    @overload
    @contextmanager
    def read_shared_file_stream(
        self,
        shared_file_name: str,
        encoding: str,
        errors: Optional[str] = None,
    ) -> Iterator[IO[str]]: ...

    @overload
    @contextmanager
    def read_shared_file_stream(
        self,
        shared_file_name: str,
        encoding: Literal[None] = None,
        errors: Optional[str] = None,
    ) -> Iterator[IO[bytes]]: ...

    @contextmanager
    def read_shared_file_stream(
        self,
        shared_file_name: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
    ) -> Iterator[Union[IO[bytes], IO[str]]]:
        self._requireValidSharedFileName(shared_file_name)
        if not s3_key_exists(s3_resource=self.s3_resource,
                             bucket=self.bucket_name,
                             key=f'{self.shared_key_prefix}{shared_file_name}',
                             extra_args=self.encryption_args):
            # TRAVIS=true TOIL_OWNER_TAG="shared" /home/quokka/git/toil/v3nv/bin/python -m pytest --durations=0 --log-level DEBUG --log-cli-level INFO -r s /home/quokka/git/toil/src/toil/test/jobStores/jobStoreTest.py::EncryptedAWSJobStoreTest::testJobDeletions
            # throw NoSuchFileException in download_stream
            raise NoSuchFileException(f's3://{self.bucket_name}/{self.shared_key_prefix}{shared_file_name}')

        try:
            with download_stream(self.s3_resource,
                                 bucket=self.bucket_name,
                                 key=f'{self.shared_key_prefix}{shared_file_name}',
                                 encoding=encoding,
                                 errors=errors,
                                 extra_args=self.encryption_args) as readable:
                yield readable
        except self.s3_client.exceptions.NoSuchKey:
            raise NoSuchFileException(shared_file_name)
        except ClientError as e:
            if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                raise NoSuchFileException(shared_file_name)

    def delete_file(self, file_id: str) -> None:
        try:
            full_s3_key = self.find_s3_key_from_file_id(file_id)
        except NoSuchFileException:
            # The file is gone. That's great, we're idempotent.
            return
        self.s3_client.delete_object(Bucket=self.bucket_name, Key=full_s3_key)

        ###################################### URI API ######################################

    def _import_file(
        self,
        otherCls: type[URLAccess],
        uri: ParseResult,
        shared_file_name: Optional[str] = None,
        hardlink: bool = False,
        symlink: bool = True,
    ) -> Optional[FileID]:
        """
        Upload a file into the s3 bucket jobstore from the source uri.

        This db entry's existence should always be in sync with the file's existence (when one exists,
        so must the other).
        """
        # we are copying from s3 to s3
        if isinstance(otherCls, AWSJobStore):
            src_bucket_name, src_key_name = parse_s3_uri(uri.geturl())
            response = head_s3_object(self.s3_resource, bucket=src_bucket_name, key=src_key_name, check=True)
            content_length = response['ContentLength']  # e.g. 65536

            file_id = str(uuid.uuid4())
            if shared_file_name:
                dst_key = f'{self.shared_key_prefix}{shared_file_name}'
            else:
                # cannot determine exec bit from foreign s3 so default to False
                dst_key = f'{self.content_key_prefix}{file_id}/0/{src_key_name.split("/")[-1]}'

            copy_s3_to_s3(
                s3_resource=self.s3_resource,
                src_bucket=src_bucket_name,
                src_key=src_key_name,
                dst_bucket=self.bucket_name,
                dst_key=dst_key,
                extra_args=self.encryption_args
            )
            # TODO: verify etag after copying here?

            return FileID(file_id, content_length) if not shared_file_name else None
        else:
            return super(AWSJobStore, self)._import_file(
                otherCls=otherCls,
                uri=uri,
                shared_file_name=shared_file_name,
                hardlink=hardlink,
                symlink=symlink
            )

    def _export_file(
        self, otherCls: type[URLAccess], jobStoreFileID: FileID, url: ParseResult
    ) -> None:
        """Export a file_id in the jobstore to the url."""
        if isinstance(otherCls, AWSJobStore):
            src_full_s3_key = self.find_s3_key_from_file_id(jobStoreFileID)
            dst_bucket_name, dst_key_name = parse_s3_uri(url.geturl())
            copy_s3_to_s3(
                s3_resource=self.s3_resource,
                src_bucket=self.bucket_name,
                src_key=src_full_s3_key,
                dst_bucket=dst_bucket_name,
                dst_key=dst_key_name,
                extra_args=self.encryption_args
            )
        else:
            super(AWSJobStore, self)._default_export_file(otherCls, jobStoreFileID, url)

    @classmethod
    def _read_from_url(
        cls, url: ParseResult, writable: Union[IO[bytes], IO[str]]
    ) -> tuple[int, bool]:
        src_obj = get_object_for_url(url, existing=True)
        src_obj.download_fileobj(writable)
        executable = False
        return src_obj.content_length, executable

    @classmethod
    def _write_to_url(
        cls,
        readable: Union[IO[bytes], IO[str]],
        url: ParseResult,
        executable: bool = False,
    ) -> None:
        dst_obj = get_object_for_url(url)
        upload_to_s3(readable=readable,
                     s3_resource=establish_boto3_session().resource("s3"),
                     bucket=dst_obj.bucket_name,
                     key=dst_obj.key)

    @classmethod
    def _url_exists(cls, url: ParseResult) -> bool:
        try:
            get_object_for_url(url, existing=True)
            return True
        except FileNotFoundError:
            # Not a file
            # Might be a directory.
            return cls._get_is_directory(url)

    @classmethod
    def _open_url(cls, url: ParseResult) -> IO[bytes]:
        try:
            src_obj = get_object_for_url(url, existing=True, anonymous=True)
            response = src_obj.get()
        except Exception as e:
            if isinstance(e, PermissionError) or (isinstance(e, ClientError) and get_error_status(e) == 403):
                # The object setup or the download does not have permission. Try again with a login.
                src_obj = get_object_for_url(url, existing=True)
                response = src_obj.get()
            else:
                raise
        # We should get back a response with a stream in 'Body'
        if "Body" not in response:
            raise RuntimeError(f"Could not fetch body stream for {url}")
        return response["Body"]  # type: ignore

    @classmethod
    def _list_url(cls, url: ParseResult) -> list[str]:
        return list_objects_for_url(url)

    @classmethod
    def _supports_url(cls, url: ParseResult, export: bool = False) -> bool:
        # TODO: export seems unused
        return url.scheme.lower() == 's3'

    def get_public_url(self, file_id: str) -> str:  # type: ignore
        """Turn s3:// into http:// and put a public-read ACL on it."""
        try:
            return create_public_url(self.s3_resource,
                                     bucket=self.bucket_name,
                                     key=f'{self.content_key_prefix}{file_id}')
        except self.s3_client.exceptions.NoSuchKey:
            raise NoSuchFileException(file_id)
        except ClientError as e:
            if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                raise NoSuchFileException(file_id)

    def get_shared_public_url(self, file_id: str) -> str:  # type: ignore
        """Turn s3:// into http:// and put a public-read ACL on it."""
        # since this is only for a few files like "config.pickle"... why and what is this used for?
        self._requireValidSharedFileName(file_id)
        try:
            return create_public_url(self.s3_resource,
                                     bucket=self.bucket_name,
                                     key=f'{self.shared_key_prefix}{file_id}')
        except self.s3_client.exceptions.NoSuchKey:
            raise NoSuchFileException(file_id)
        except ClientError as e:
            if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                raise NoSuchFileException(file_id)

    @classmethod
    def _get_is_directory(cls, url: ParseResult) -> bool:
        # We consider it a directory if anything is in it.
        # TODO: Can we just get the first item and not the whole list?
        return len(list_objects_for_url(url)) > 0

    def get_empty_file_store_id(
        self,
        job_id: Optional[str] = None,
        cleanup: bool = False,
        basename: Optional[str] = None,
    ) -> FileID:
        """Create an empty file in s3 and return a file_id referencing it."""
        file_id = str(uuid.uuid4())
        self.write_to_bucket(
            identifier=f'{file_id}/0/{basename}',
            prefix=self.content_key_prefix,
            data=None,
            bucket=self.bucket_name
        )
        if job_id and cleanup:
            self.associate_job_with_file(job_id, file_id)
        return FileID(fileStoreID=file_id, size=0, executable=False)

        ###################################### LOGGING API ######################################

    def write_logs(self, log_msg: Union[bytes, str]) -> None:
        if isinstance(log_msg, str):
            log_msg = log_msg.encode('utf-8', errors='ignore')
        file_obj = BytesIO(log_msg)

        key_name = f'{self.logs_key_prefix}{datetime.datetime.now()}{str(uuid.uuid4())}'.replace(' ', '_')
        self.s3_client.upload_fileobj(Bucket=self.bucket_name,
                                      Key=key_name,
                                      ExtraArgs=self.encryption_args,
                                      Fileobj=file_obj)

    def read_logs(self, callback: Callable[..., Any], read_all: bool = False) -> int:
        """
        This fetches all referenced logs in the database from s3 as readable objects
        and runs "callback()" on them.
        """
        items_processed = 0
        read_log_marker = self.read_from_bucket(identifier='most_recently_read_log.marker',
                                                prefix=self.shared_key_prefix).decode('utf-8')
        startafter = None if read_log_marker == '0' or read_all else read_log_marker
        for result in list_s3_items(self.s3_resource, bucket=self.bucket_name, prefix=self.logs_key_prefix, startafter=startafter):
            if result['Key'] > read_log_marker or read_all:
                read_log_marker = result['Key']
                with download_stream(self.s3_resource,
                                     bucket=self.bucket_name,
                                     key=result['Key'],
                                     extra_args=self.encryption_args) as readable:
                    callback(readable)
                items_processed += 1
        self.write_to_bucket(identifier='most_recently_read_log.marker',
                             prefix=self.shared_key_prefix,
                             data=read_log_marker)
        return items_processed

    def configure_encryption(self, sse_key_path: Optional[str] = None) -> None:
        if sse_key_path:
            with open(sse_key_path, 'r') as f:
                sse_key = f.read()
            if not len(sse_key) == 32:  # TODO: regex
                raise ValueError(f'Check that {sse_key_path} is the path to a real SSE key.  '
                                 f'(Key length {len(sse_key)} != 32)')
            self.sse_key = sse_key
            self.encryption_args = {'SSECustomerAlgorithm': 'AES256', 'SSECustomerKey': sse_key}


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
    return region, bucket_name
