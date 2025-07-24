# Copyright (C) 2015-2023 Regents of the University of California
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
import logging

from io import BytesIO
from datetime import timedelta
from contextlib import contextmanager
from typing import (Any,
                    Dict,
                    List,
                    Optional,
                    Union,
                    Tuple,
                    cast,
                    Literal,
                    Iterator,
                    TYPE_CHECKING,
                    IO)

try:
    from botocore.exceptions import ClientError
    from boto.exception import BotoServerError, S3ResponseError
except ImportError:
    BotoServerError, S3ResponseError, ClientError = Exception, Exception, Exception  # type: ignore

from toil.lib.retry import retry, get_error_status, ErrorCondition
from toil.lib.misc import printq
from toil.lib.aws import AWSServerErrors, session, build_tag_dict_from_env
from toil.lib.aws.utils import enable_public_objects, flatten_tags
from toil.lib.conversions import modify_url, MB, MIB, TB
from toil.lib.pipes import WritablePipe, ReadablePipe, HashingPipe

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client, S3ServiceResource
    from mypy_boto3_s3.literals import BucketLocationConstraintType
    from mypy_boto3_s3.service_resource import Bucket, Object
    from mypy_boto3_s3.type_defs import ListMultipartUploadsOutputTypeDef, HeadObjectOutputTypeDef, GetObjectOutputTypeDef, PutObjectOutputTypeDef, ObjectTypeDef


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


class AWSKeyNotFoundError(Exception):
    pass


class AWSKeyAlreadyExistsError(Exception):
    pass


class AWSBadEncryptionKeyError(Exception):
    pass


@retry(errors=[BotoServerError, S3ResponseError, ClientError])
def create_s3_bucket(
    s3_resource: "S3ServiceResource",
    bucket_name: str,
    region: Union["BucketLocationConstraintType", Literal["us-east-1"]],
    tags: Optional[Dict[str, str]] = None,
    public: bool = True
) -> "Bucket":
    """
    Create an AWS S3 bucket, using the given Boto3 S3 session, with the
    given name, in the given region.

    Supports the us-east-1 region, where bucket creation is special.

    *ALL* S3 bucket creation should use this function.
    """
    logger.info("Creating bucket '%s' in region %s.", bucket_name, region)
    if region == "us-east-1":  # see https://github.com/boto/boto3/issues/125
        bucket = s3_resource.create_bucket(Bucket=bucket_name)
    else:
        bucket = s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
    # wait until the bucket exists before adding tags
    bucket.wait_until_exists()

    tags = build_tag_dict_from_env() if tags is None else tags
    bucket_tagging = s3_resource.BucketTagging(bucket_name)
    bucket_tagging.put(Tagging={'TagSet': flatten_tags(tags)})  # type: ignore

    # enabling public objects is the historical default
    if public:
        enable_public_objects(bucket_name)

    return bucket


@retry(errors=[BotoServerError, S3ResponseError, ClientError])
def delete_s3_bucket(
    s3_resource: "S3ServiceResource",
    bucket_name: str,
    quiet: bool = True
) -> None:
    """
    Delete the bucket with 'bucket_name'.

    Note: 'quiet' is False when used for a clean up utility script (contrib/admin/cleanup_aws_resources.py)
         that prints progress rather than logging.  Logging should be used for all other internal Toil usage.
    """
    assert isinstance(bucket_name, str), f'{bucket_name} is not a string ({type(bucket_name)}).'
    logger.debug("Deleting bucket '%s'.", bucket_name)
    printq(f'\n * Deleting s3 bucket: {bucket_name}\n\n', quiet)

    s3_client = s3_resource.meta.client

    try:
        for u in s3_client.list_multipart_uploads(Bucket=bucket_name).get('Uploads', []):
            s3_client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=u["Key"],
                UploadId=u["UploadId"]
            )

        paginator = s3_client.get_paginator('list_object_versions')
        for response in paginator.paginate(Bucket=bucket_name):
            # Versions and delete markers can both go in here to be deleted.
            # They both have Key and VersionId, but there's no shared base type
            # defined for them in the stubs to express that. See
            # <https://github.com/vemel/mypy_boto3_builder/issues/123>. So we
            # have to do gymnastics to get them into the same list.
            to_delete: List[Dict[str, Any]] = cast(List[Dict[str, Any]], response.get('Versions', [])) + \
                                              cast(List[Dict[str, Any]], response.get('DeleteMarkers', []))
            for entry in to_delete:
                printq(f"    Deleting {entry['Key']} version {entry['VersionId']}", quiet)
                s3_client.delete_object(
                    Bucket=bucket_name,
                    Key=entry['Key'],
                    VersionId=entry['VersionId']
                )
        bucket = s3_resource.Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.object_versions.delete()
        bucket.delete()
        printq(f'\n * Deleted s3 bucket successfully: {bucket_name}\n\n', quiet)
        logger.debug("Deleted s3 bucket successfully '%s'.", bucket_name)
    except s3_client.exceptions.NoSuchBucket:
        printq(f'\n * S3 bucket no longer exists: {bucket_name}\n\n', quiet)
        logger.debug("S3 bucket no longer exists '%s'.", bucket_name)
    except ClientError as e:
        if get_error_status(e) != 404:
            raise
        printq(f'\n * S3 bucket no longer exists: {bucket_name}\n\n', quiet)
        logger.debug("S3 bucket no longer exists '%s'.", bucket_name)


@retry(errors=[BotoServerError])
def bucket_exists(s3_resource: "S3ServiceResource", bucket: str) -> Union[bool, "Bucket"]:
    s3_client = s3_resource.meta.client
    try:
        s3_client.head_bucket(Bucket=bucket)
        return s3_resource.Bucket(bucket)
    except (ClientError, s3_client.exceptions.NoSuchBucket) as e:
        error_code = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if error_code == 404:
            return False
        else:
            raise


@retry(errors=[AWSServerErrors])
def head_s3_object(bucket: str, key: str, header: Dict[str, Any], region: Optional[str] = None) -> "HeadObjectOutputTypeDef":
    """
    Attempt to HEAD an s3 object and return its response.

    :param bucket: AWS bucket name
    :param key:  AWS Key name for the s3 object
    :param header: Headers to include (mostly for encryption).
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html
    :param region: Region that we want to look for the bucket in
    """
    s3_client = session.client("s3", region_name=region)
    return s3_client.head_object(Bucket=bucket, Key=key, **header)


@retry(errors=[AWSServerErrors])
def list_multipart_uploads(bucket: str, region: str, prefix: str, max_uploads: int = 1) -> "ListMultipartUploadsOutputTypeDef":
    s3_client = session.client("s3", region_name=region)
    return s3_client.list_multipart_uploads(Bucket=bucket, MaxUploads=max_uploads, Prefix=prefix)


@retry(errors=[BotoServerError])
def copy_s3_to_s3(s3_resource: "S3ServiceResource", src_bucket: str, src_key: str, dst_bucket: str, dst_key: str, extra_args: Optional[Dict[Any, Any]] = None) -> None:
    source = {'Bucket': src_bucket, 'Key': src_key}
    # Note: this may have errors if using sse-c because of
    # a bug with encryption using copy_object and copy (which uses copy_object for files <5GB):
    # https://github.com/aws/aws-cli/issues/6012
    # this will only happen if we attempt to copy a file previously encrypted with sse-c
    # copying an unencrypted file and encrypting it as sse-c seems to work fine though
    kwargs = dict(CopySource=source, Bucket=dst_bucket, Key=dst_key, ExtraArgs=extra_args)
    s3_resource.meta.client.copy(**kwargs)  # type: ignore


# TODO: Determine specific retries
@retry(errors=[BotoServerError])
def copy_local_to_s3(
        s3_resource: "S3ServiceResource",
        local_file_path: str,
        dst_bucket: str,
        dst_key: str,
        extra_args: Optional[Dict[Any, Any]] = None
) -> None:
    s3_client = s3_resource.meta.client
    s3_client.upload_file(local_file_path, dst_bucket, dst_key, ExtraArgs=extra_args)


# TODO: Determine specific retries
@retry(errors=[BotoServerError])
def copy_s3_to_local(
        s3_resource: "S3ServiceResource",
        local_file_path: str,
        src_bucket: str,
        src_key: str,
        extra_args: Optional[Dict[Any, Any]] = None
) -> None:
    s3_client = s3_resource.meta.client
    s3_client.download_file(src_bucket, src_key, local_file_path, ExtraArgs=extra_args)


class MultiPartPipe(WritablePipe):
    def __init__(self, part_size: int, s3_resource: "S3ServiceResource", bucket_name: str, file_id: str, encryption_args: Optional[Dict[Any, Any]], encoding: Optional[str] = None, errors: Optional[str] = None) -> None:
        super(MultiPartPipe, self).__init__()
        self.encoding = encoding
        self.errors = errors
        self.part_size = part_size
        self.s3_client = s3_resource.meta.client
        self.bucket_name = bucket_name
        self.file_id = file_id
        self.encryption_args = encryption_args or dict()

    def readFrom(self, readable: IO[Any]) -> None:
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
            response = self.s3_client.complete_multipart_upload(Bucket=self.bucket_name,  # type: ignore
                                                                Key=self.file_id,
                                                                UploadId=upload_id,
                                                                MultipartUpload={"Parts": parts})  # type: ignore
            logger.debug(f'[{upload_id}] Upload complete...')


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    # does not support s3/gs: https://docs.python.org/3/library/urllib.parse.html
    # use regex instead?
    uri = urllib.parse.urlparse(uri)  # type: ignore
    if uri.scheme.lower() != 's3':  # type: ignore
        raise ValueError(f'Invalid schema.  Expecting s3 prefix, not: {uri}')
    bucket_name, key_name = uri.netloc.strip('/'), uri.path.strip('/')  # type: ignore
    return bucket_name, key_name


def list_s3_items(s3_resource: "S3ServiceResource", bucket: str, prefix: str, startafter: Optional[str] = None) -> Iterator["ObjectTypeDef"]:
    s3_client = s3_resource.meta.client
    paginator = s3_client.get_paginator('list_objects_v2')
    kwargs = dict(Bucket=bucket, Prefix=prefix)
    if startafter:
        kwargs['StartAfter'] = startafter
    for page in paginator.paginate(**kwargs):  # type: ignore
        for key in page.get('Contents', []):
            yield key


@retry(errors=[ErrorCondition(error=ClientError, error_codes=[404, 500, 502, 503, 504])])
def upload_to_s3(readable: IO[Any],
                 s3_resource: "S3ServiceResource",
                 bucket: str,
                 key: str,
                 extra_args: Optional[Dict[Any, Any]] = None) -> None:
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
    from boto3.s3.transfer import TransferConfig
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
def download_stream(s3_resource: "S3ServiceResource", bucket: str, key: str, checksum_to_verify: Optional[str] = None,
                    extra_args: Optional[Dict[Any, Any]] = None, encoding: Optional[str] = None, errors: Optional[str] = None) -> Iterator[IO[Any]]:
    """Context manager that gives out a download stream to download data."""
    bucket_obj: Bucket = s3_resource.Bucket(bucket)

    class DownloadPipe(ReadablePipe):
        def writeTo(self, writable: IO[Any]) -> None:
            kwargs = dict(Key=key, Fileobj=writable, ExtraArgs=extra_args)
            if not extra_args:
                del kwargs['ExtraArgs']
            bucket_obj.download_fileobj(**kwargs)  # type: ignore

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


def download_fileobject(s3_resource: "S3ServiceResource", bucket: "Bucket", key: str, fileobj: BytesIO, extra_args: Optional[Dict[Any, Any]] = None) -> None:
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


def s3_key_exists(s3_resource: "S3ServiceResource", bucket: str, key: str, check: bool = False, extra_args: Optional[Dict[Any, Any]] = None) -> bool:
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


def get_s3_object(s3_resource: "S3ServiceResource", bucket: str, key: str, extra_args: Optional[Dict[Any, Any]] = None) -> "GetObjectOutputTypeDef":
    if extra_args is None:
        extra_args = dict()
    s3_client = s3_resource.meta.client
    return s3_client.get_object(Bucket=bucket, Key=key, **extra_args)


def put_s3_object(s3_resource: "S3ServiceResource", bucket: str, key: str, body: Union[str, bytes], extra_args: Optional[Dict[Any, Any]] = None) -> "PutObjectOutputTypeDef":
    if extra_args is None:
        extra_args = dict()
    s3_client = s3_resource.meta.client
    return s3_client.put_object(Bucket=bucket, Key=key, Body=body, **extra_args)


def generate_presigned_url(s3_resource: "S3ServiceResource", bucket: str, key_name: str, expiration: int) -> str:
    s3_client = s3_resource.meta.client
    return s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': key_name},
        ExpiresIn=expiration)


def create_public_url(s3_resource: "S3ServiceResource", bucket: str, key: str) -> str:
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


def get_s3_bucket_region(s3_resource: "S3ServiceResource", bucket: str) -> str:
    s3_client = s3_resource.meta.client
    # AWS returns None for the default of 'us-east-1'
    return s3_client.get_bucket_location(Bucket=bucket).get('LocationConstraint', None) or 'us-east-1'
