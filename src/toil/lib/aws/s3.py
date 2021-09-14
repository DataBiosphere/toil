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
import hashlib
import itertools
import urllib.parse
import math
import logging
import sys
import os

from io import BytesIO
from typing import Tuple, Optional, Union
from datetime import timedelta
from contextlib import contextmanager
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from toil.lib.conversions import modify_url, MB, MIB, TB
from toil.lib.pipes import WritablePipe, ReadablePipe, HashingPipe
from toil.lib.retry import ErrorCondition
from toil.lib.retry import retry
from toil.lib.aws.credentials import resource

try:
    from boto.exception import BotoServerError
    from mypy_boto3_s3 import S3ServiceResource
    from mypy_boto3_s3.literals import BucketLocationConstraintType
    from mypy_boto3_s3.service_resource import Bucket
except ImportError:
    BotoServerError = None  # type: ignore
    # AWS/boto extra is not installed

logger = logging.getLogger(__name__)

Bucket = resource('s3').Bucket  # only declared for mypy typing
logger = logging.getLogger(__name__)

# AWS Defined Limits
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
AWS_MAX_MULTIPART_COUNT = 10000
AWS_MAX_CHUNK_SIZE = 5 * TB
AWS_MIN_CHUNK_SIZE = 5 * MB
# Note: There is no minimum size limit on the last part of a multipart upload.

# The chunk size we chose arbitrarily, but it must be consistent for etags
DEFAULT_AWS_CHUNK_SIZE = 128 * MIB
assert AWS_MAX_CHUNK_SIZE > DEFAULT_AWS_CHUNK_SIZE > AWS_MIN_CHUNK_SIZE


def get_s3_multipart_chunk_size(file_size: int) -> int:
    if file_size >= AWS_MAX_CHUNK_SIZE * AWS_MAX_MULTIPART_COUNT:
        return AWS_MAX_CHUNK_SIZE
    elif file_size <= DEFAULT_AWS_CHUNK_SIZE * AWS_MAX_MULTIPART_COUNT:
        return DEFAULT_AWS_CHUNK_SIZE
    else:
        return math.ceil(file_size / AWS_MAX_MULTIPART_COUNT)


class NoSuchFileException(Exception):
    pass


class AWSKeyNotFoundError(Exception):
    pass


class AWSKeyAlreadyExistsError(Exception):
    pass


class AWSBadEncryptionKeyError(Exception):
    pass


# # TODO: Determine specific retries
# @retry()
def create_bucket(s3_resource: S3ServiceResource, bucket: str) -> Bucket:
    """
    Create an AWS S3 bucket, using the given Boto3 S3 resource, with the
    given name, in the S3 resource's region.

    Supports the us-east-1 region, where bucket creation is special.

    *ALL* S3 bucket creation should use this function.
    """
    s3_client = s3_resource.meta.client
    logger.info(f"Creating AWS bucket {bucket} in region {s3_client.meta.region_name}")
    if s3_client.meta.region_name == "us-east-1":  # see https://github.com/boto/boto3/issues/125
        s3_client.create_bucket(Bucket=bucket)
    else:
        s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": s3_client.meta.region_name},
        )
    waiter = s3_client.get_waiter('bucket_exists')
    waiter.wait(Bucket=bucket)
    owner_tag = os.environ.get('TOIL_OWNER_TAG')
    if owner_tag:
        bucket_tagging = s3_resource.BucketTagging(bucket)
        bucket_tagging.put(Tagging={'TagSet': [{'Key': 'Owner', 'Value': owner_tag}]})
    logger.debug(f"Successfully created new bucket '{bucket}'")
    return s3_resource.Bucket(bucket)


# # TODO: Determine specific retries
@retry(errors=[BotoServerError])
def delete_bucket(s3_resource: S3ServiceResource, bucket: str) -> None:
    s3_client = s3_resource.meta.client
    bucket_obj = s3_resource.Bucket(bucket)
    try:
        uploads = s3_client.list_multipart_uploads(Bucket=bucket).get('Uploads') or list()
        for u in uploads:
            s3_client.abort_multipart_upload(Bucket=bucket, Key=u["Key"], UploadId=u["UploadId"])
        bucket_obj.objects.all().delete()
        bucket_obj.object_versions.delete()
        bucket_obj.delete()
    except s3_client.exceptions.NoSuchBucket:
        logger.info(f"Bucket already deleted (NoSuchBucket): '{bucket}'")
        print(f"Bucket already deleted (NoSuchBucket): '{bucket}'")
    except ClientError as e:
        if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 404:
            raise
        logger.info(f"Bucket already deleted (404): '{bucket}'")
        print(f"Bucket already deleted (404): '{bucket}'")
    else:
        logger.info(f"Successfully deleted bucket: '{bucket}'")
        print(f"Successfully deleted bucket: '{bucket}'")


# TODO: Determine specific retries
@retry(errors=[BotoServerError])
def bucket_exists(s3_resource, bucket: str) -> Union[bool, Bucket]:
    s3_client = s3_resource.meta.client
    try:
        s3_client.head_bucket(Bucket=bucket)
        return s3_resource.Bucket(bucket)
    except ClientError as e:
        error_code = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if error_code == 404:
            return False
        else:
            raise


# TODO: Determine specific retries
@retry(errors=[BotoServerError])
def copy_s3_to_s3(s3_resource, src_bucket, src_key, dst_bucket, dst_key, extra_args: Optional[dict] = None):
    if not extra_args:
        source = {'Bucket': src_bucket, 'Key': src_key}
        # Note: this may have errors if using sse-c because of
        # a bug with encryption using copy_object and copy (which uses copy_object for files <5GB):
        # https://github.com/aws/aws-cli/issues/6012
        # this will only happen if we attempt to copy a file previously encrypted with sse-c
        # copying an unencrypted file and encrypting it as sse-c seems to work fine though
        kwargs = dict(CopySource=source, Bucket=dst_bucket, Key=dst_key, ExtraArgs=extra_args)
        s3_resource.meta.client.copy(**kwargs)
    else:
        pass


# TODO: Determine specific retries
@retry(errors=[BotoServerError])
def copy_local_to_s3(s3_resource, local_file_path, dst_bucket, dst_key, extra_args: Optional[dict] = None):
    s3_client = s3_resource.meta.client
    s3_client.upload_file(local_file_path, dst_bucket, dst_key, ExtraArgs=extra_args)


class MultiPartPipe(WritablePipe):
    def __init__(self, part_size, s3_client, bucket_name, file_id, encryption_args, encoding, errors):
        super(MultiPartPipe, self).__init__()
        self.encoding = encoding
        self.errors = errors
        self.part_size = part_size
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.file_id = file_id
        self.encryption_args = encryption_args

    def readFrom(self, readable):
        # Get the first block of data we want to put
        buf = readable.read(self.part_size)
        assert isinstance(buf, bytes)

        # We will compute a checksum
        hasher = hashlib.sha1()
        hasher.update(buf)

        # low-level clients are thread safe
        response = self.s3_client.create_multipart_upload(Bucket=self.bucket_name,
                                                          Key=self.file_id,
                                                          **self.encryption_args)
        upload_id = response['UploadId']
        parts = []
        try:
            for part_num in itertools.count():
                logger.debug(f'[{upload_id}] Uploading part %d of %d bytes', part_num + 1, len(buf))
                # TODO: include the Content-MD5 header:
                #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.complete_multipart_upload
                part = self.s3_client.upload_part(Bucket=self.bucket_name,
                                                  Key=self.file_id,
                                                  PartNumber=part_num + 1,
                                                  UploadId=upload_id,
                                                  Body=BytesIO(buf),
                                                  **self.encryption_args)
                parts.append({"PartNumber": part_num + 1, "ETag": part["ETag"]})

                # Get the next block of data we want to put
                buf = readable.read(self.part_size)
                if len(buf) == 0:
                    # Don't allow any part other than the very first to be empty.
                    break
                hasher.update(buf)
        except:
            self.s3_client.abort_multipart_upload(Bucket=self.bucket_name,
                                                  Key=self.file_id,
                                                  UploadId=upload_id)
        else:
            # Save the checksum
            checksum = f'sha1${hasher.hexdigest()}'
            response = self.s3_client.complete_multipart_upload(Bucket=self.bucket_name,
                                                                Key=self.file_id,
                                                                UploadId=upload_id,
                                                                MultipartUpload={"Parts": parts})
            logger.debug(f'[{upload_id}] Upload complete...')


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    # does not support s3/gs: https://docs.python.org/3/library/urllib.parse.html
    # use regex instead?
    if isinstance(uri, str):
        uri = urllib.parse.urlparse(uri)
    if uri.scheme.lower() != 's3':
        raise ValueError(f'Invalid schema.  Expecting s3 prefix, not: {uri}')
    # bucket_name, key_name = uri[len('s3://'):].split('/', 1)
    bucket_name, key_name = uri.netloc.strip('/'), uri.path.strip('/')
    return bucket_name, key_name


def list_s3_items(s3_resource, bucket, prefix, startafter=None):
    s3_client = s3_resource.meta.client
    paginator = s3_client.get_paginator('list_objects_v2')
    kwargs = dict(Bucket=bucket, Prefix=prefix)
    if startafter:
        kwargs['StartAfter'] = startafter
    for page in paginator.paginate(**kwargs):
        for key in page.get('Contents', []):
            yield key


@retry(errors=[ErrorCondition(error=ClientError, error_codes=[404, 500, 502, 503, 504])])
def upload_to_s3(readable,
                 s3_resource,
                 bucket: str,
                 key: str,
                 extra_args: Optional[dict] = None):
    """
    Upload a readable object to s3, using multipart uploading if applicable.

    :param readable: a readable stream or a local file path to upload to s3
    :param S3.Resource resource: boto3 resource
    :param str bucket: name of the bucket to upload to
    :param str key: the name of the file to upload to
    :param dict extra_args: http headers to use when uploading - generally used for encryption purposes
    :param int partSize: max size of each part in the multipart upload, in bytes
    :return: version of the newly uploaded file
    """
    if extra_args is None:
        extra_args = {}

    s3_client = s3_resource.meta.client
    config = TransferConfig(
        multipart_threshold=DEFAULT_AWS_CHUNK_SIZE,
        multipart_chunksize=DEFAULT_AWS_CHUNK_SIZE,
        use_threads=True
    )
    logger.debug("Uploading %s", key)
    # these methods use multipart if necessary
    if isinstance(readable, str):
        s3_client.upload_file(Filename=readable,
                              Bucket=bucket,
                              Key=key,
                              ExtraArgs=extra_args,
                              Config=config)
    else:
        s3_client.upload_fileobj(Fileobj=readable,
                                 Bucket=bucket,
                                 Key=key,
                                 ExtraArgs=extra_args,
                                 Config=config)

    object_summary = s3_resource.ObjectSummary(bucket, key)
    object_summary.wait_until_exists(**extra_args)


@contextmanager
def download_stream(s3_resource, bucket: str, key: str, checksum_to_verify: Optional[str] = None,
                    extra_args: Optional[dict] = None, encoding=None, errors=None):
    """Context manager that gives out a download stream to download data."""
    bucket = s3_resource.Bucket(bucket)

    class DownloadPipe(ReadablePipe):
        def writeTo(self, writable):
            kwargs = dict(Key=key, Fileobj=writable, ExtraArgs=extra_args)
            if not extra_args:
                del kwargs['ExtraArgs']
            bucket.download_fileobj(**kwargs)

    try:
        if checksum_to_verify:
            with DownloadPipe(encoding=encoding, errors=errors) as readable:
                # Interpose a pipe to check the hash
                with HashingPipe(readable, encoding=encoding, errors=errors) as verified:
                    yield verified
        else:
            # Readable end of pipe produces text mode output if encoding specified
            with DownloadPipe(encoding=encoding, errors=errors) as readable:
                # No true checksum available, so don't hash
                yield readable
    except s3_resource.meta.client.exceptions.NoSuchKey:
        raise AWSKeyNotFoundError(f"Key '{key}' does not exist in bucket '{bucket}'.")
    except ClientError as e:
        if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
            raise AWSKeyNotFoundError(f"Key '{key}' does not exist in bucket '{bucket}'.")
        elif e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 400 and \
                e.response.get('Error', {}).get('Message') == 'Bad Request' and \
                e.operation_name == 'HeadObject':
            # An error occurred (400) when calling the HeadObject operation: Bad Request
            raise AWSBadEncryptionKeyError('Your AWS encryption key is most likely configured incorrectly '
                                           '(HeadObject operation: Bad Request).')
        raise


def download_fileobject(s3_resource, bucket: Bucket, key: str, fileobj, extra_args: Optional[dict] = None):
    try:
        bucket.download_fileobj(Key=key, Fileobj=fileobj, ExtraArgs=extra_args)
    except s3_resource.meta.client.exceptions.NoSuchKey:
        raise AWSKeyNotFoundError(f"Key '{key}' does not exist in bucket '{bucket}'.")
    except ClientError as e:
        if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
            raise AWSKeyNotFoundError(f"Key '{key}' does not exist in bucket '{bucket}'.")
        elif e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 400 and \
                e.response.get('Error', {}).get('Message') == 'Bad Request' and \
                e.operation_name == 'HeadObject':
            # An error occurred (400) when calling the HeadObject operation: Bad Request
            raise AWSBadEncryptionKeyError('Your AWS encryption key is most likely configured incorrectly '
                                           '(HeadObject operation: Bad Request).')
        raise


def s3_key_exists(s3_resource, bucket: str, key: str, check: bool = False, extra_args: dict = None):
    """Return True if the s3 obect exists, and False if not.  Will error if encryption args are incorrect."""
    extra_args = extra_args or {}
    s3_client = s3_resource.meta.client
    try:
        s3_client.head_object(Bucket=bucket, Key=key, **extra_args)
        return True
    except s3_client.exceptions.NoSuchKey:
        if check:
            raise AWSKeyNotFoundError(f"Key '{key}' does not exist in bucket '{bucket}'.")
        return False
    except ClientError as e:
        if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
            if check:
                raise AWSKeyNotFoundError(f"Key '{key}' does not exist in bucket '{bucket}'.")
            return False
        elif e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 400 and \
                e.response.get('Error', {}).get('Message') == 'Bad Request' and \
                e.operation_name == 'HeadObject':
            # An error occurred (400) when calling the HeadObject operation: Bad Request
            raise AWSBadEncryptionKeyError('Your AWS encryption key is most likely configured incorrectly '
                                           '(HeadObject operation: Bad Request).')
        else:
            raise


def head_s3_object(s3_resource, bucket: str, key: str, check=False, extra_args: dict = None):
    s3_client = s3_resource.meta.client
    extra_args = extra_args or {}
    try:
        return s3_client.head_object(Bucket=bucket, Key=key, **extra_args)
    except s3_client.exceptions.NoSuchKey:
        if check:
            raise NoSuchFileException(f"File '{key}' not found in AWS jobstore bucket: '{bucket}'")


def get_s3_object(s3_resource, bucket: str, key: str, extra_args: dict = None):
    if extra_args is None:
        extra_args = dict()
    s3_client = s3_resource.meta.client
    return s3_client.get_object(Bucket=bucket, Key=key, **extra_args)


def put_s3_object(s3_resource, bucket: str, key: str, body: Optional[bytes], extra_args: dict = None):
    if extra_args is None:
        extra_args = dict()
    s3_client = s3_resource.meta.client
    return s3_client.put_object(Bucket=bucket, Key=key, Body=body, **extra_args)


def generate_presigned_url(s3_resource, bucket: str, key_name: str, expiration: int) -> Tuple[str, str]:
    s3_client = s3_resource.meta.client
    return s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': key_name},
        ExpiresIn=expiration)


def create_public_url(s3_resource, bucket: str, key: str):
    bucket_obj = Bucket(bucket)
    bucket_obj.Object(key).Acl().put(ACL='public-read')  # TODO: do we need to generate a signed url after doing this?
    url = generate_presigned_url(s3_resource=s3_resource,
                                 bucket=bucket,
                                 key_name=key,
                                 # One year should be sufficient to finish any pipeline ;-)
                                 expiration=int(timedelta(days=365).total_seconds()))
    # boto doesn't properly remove the x-amz-security-token parameter when
    # query_auth is False when using an IAM role (see issue #2043). Including the
    # x-amz-security-token parameter without the access key results in a 403,
    # even if the resource is public, so we need to remove it.
    # TODO: verify that this is still the case
    return modify_url(url, remove=['x-amz-security-token', 'AWSAccessKeyId', 'Signature'])


def get_s3_bucket_region(s3_resource, bucket: str):
    s3_client = s3_resource.meta.client
    # AWS returns None for the default of 'us-east-1'
    return s3_client.get_bucket_location(Bucket=bucket).get('LocationConstraint', None) or 'us-east-1'


# def update_aws_content_type(s3_client, bucket, key, content_type):
#     blob = resources.s3.Bucket(bucket).Object(key)
#     size = blob.content_length
#     part_size = get_s3_multipart_chunk_size(size)
#     if size <= part_size:
#         s3_client.copy_object(Bucket=bucket,
#                               Key=key,
#                               CopySource=dict(Bucket=bucket, Key=key),
#                               MetadataDirective="REPLACE")
#     else:
#         resp = s3_client.create_multipart_upload(Bucket=bucket,
#                                                  Key=key)
#         upload_id = resp['UploadId']
#         multipart_upload = dict(Parts=list())
#         for i in range(math.ceil(size / part_size)):
#             start_range = i * part_size
#             end_range = (i + 1) * part_size - 1
#             if end_range >= size:
#                 end_range = size - 1
#             resp = s3_client.upload_part_copy(CopySource=dict(Bucket=bucket, Key=key),
#                                               Bucket=bucket,
#                                               Key=key,
#                                               CopySourceRange=f"bytes={start_range}-{end_range}",
#                                               PartNumber=i + 1,
#                                               UploadId=upload_id)
#             multipart_upload['Parts'].append(dict(ETag=resp['CopyPartResult']['ETag'], PartNumber=i + 1))
#         s3_client.complete_multipart_upload(Bucket=bucket,
#                                             Key=key,
#                                             MultipartUpload=multipart_upload,
#                                             UploadId=upload_id)
