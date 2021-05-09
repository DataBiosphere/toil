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
import logging
import hashlib
import itertools
import os
from io import BytesIO
from typing import Optional, Tuple, Union
from datetime import timedelta
import os

from contextlib import contextmanager
from typing import Optional
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from toil.lib.aws.credentials import client, resource
from toil.lib.conversions import modify_url
from toil.lib.pipes import WritablePipe
from toil.lib.compatibility import compat_bytes
from toil.lib.pipes import ReadablePipe, HashingPipe
from toil.lib.retry import retry, ErrorCondition

Bucket = resource('s3').Bucket  # only declared for mypy typing
logger = logging.getLogger(__name__)


class AWSKeyNotFoundError(Exception):
    pass


class AWSKeyAlreadyExistsError(Exception):
    pass


# TODO: Determine specific retries
@retry()
def create_bucket(s3_resource, bucket: str) -> Bucket:
    s3_client = s3_resource.meta.client
    logger.info(f"Creating AWS bucket {bucket} in region {s3_client.meta.region_name}")
    bucket_obj = s3_client.create_bucket(Bucket=bucket,
                                         CreateBucketConfiguration={'LocationConstraint': s3_client.meta.region_name})
    waiter = s3_client.get_waiter('bucket_exists')
    waiter.wait(Bucket=bucket)
    owner_tag = os.environ.get('TOIL_OWNER_TAG')
    if owner_tag:
        bucket_tagging = s3_resource.BucketTagging(bucket)
        bucket_tagging.put(Tagging={'TagSet': [{'Key': 'Owner', 'Value': owner_tag}]})
    logger.debug(f"Successfully created new bucket '{bucket}'")
    return bucket_obj


# TODO: Determine specific retries
@retry()
def delete_bucket(s3_resource, bucket: str) -> None:
    logger.debug(f"Deleting AWS bucket: {bucket}")
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
        pass
    except ClientError as e:
        if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 404:
            raise
    logger.debug(f"Successfully deleted bucket '{bucket}'")


# TODO: Determine specific retries
@retry()
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
@retry()
def copy_s3_to_s3(s3_resource, src_bucket, src_key, dst_bucket, dst_key):
    source = {'Bucket': src_bucket, 'Key': src_key}
    dest = s3_resource.Bucket(dst_bucket)
    dest.copy(source, dst_key)


# TODO: Determine specific retries
@retry()
def copy_local_to_s3(s3_resource, local_file_path, dst_bucket, dst_key):
    s3_client = s3_resource.meta.client
    s3_client.upload_file(local_file_path, dst_bucket, dst_key)


# TODO: Determine specific retries
@retry()
def bucket_versioning_enabled(s3_resource, bucket: str):
    versionings = dict(Enabled=True, Disabled=False, Suspended=None)
    status = s3_resource.BucketVersioning(bucket).status
    return versionings.get(status) if status else False


class MultiPartPipe(WritablePipe):
    def __init__(self, part_size, s3_client, bucket_name, file_id, encryption_args, encoding, errors):
        self.encoding = encoding
        self.errors = errors
        self.part_size = part_size
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.file_id = file_id
        self.encryption_args = encryption_args
        super(MultiPartPipe, self).__init__()

    def readFrom(self, readable):
        # Get the first block of data we want to put
        buf = readable.read(self.part_size)
        assert isinstance(buf, bytes)

        # We will compute a checksum
        hasher = hashlib.sha1()
        hasher.update(buf)

        # low-level clients are thread safe
        upload = self.s3_client.create_multipart_upload(Bucket=self.bucket_name,
                                                        Key=self.file_id,
                                                        **self.encryption_args)
        upload_id = upload['UploadId']
        parts = []
        try:
            for part_num in itertools.count():
                logger.debug('Uploading part %d of %d bytes', part_num + 1, len(buf))
                # TODO: include the Content-MD5 header:
                #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.complete_multipart_upload
                part = self.s3_client.upload_part(Bucket=self.bucket_name,
                                                  Key=compat_bytes(self.file_id),
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
            logger.debug('Attempting to complete upload...')
            response = self.s3_client.complete_multipart_upload(Bucket=self.bucket_name,
                                                                Key=self.file_id,
                                                                UploadId=upload_id,
                                                                MultipartUpload={"Parts": parts})


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith('s3://'):
        raise ValueError(f'Invalid schema.  Expecting s3 prefix, not: {uri}')
    bucket_name, key_name = uri[len('s3://'):].split('/', 1)
    return bucket_name, key_name


def boto_args():
    host = os.environ.get('TOIL_S3_HOST', None)
    port = os.environ.get('TOIL_S3_PORT', None)
    protocol = 'https'
    if os.environ.get('TOIL_S3_USE_SSL', True) == 'False':
        protocol = 'http'
    if host:
        return {'endpoint_url': f'{protocol}://{host}' + f':{port}' if port else ''}
    return {}


def list_s3_items(s3_resource, bucket, prefix):
    s3_client = s3_resource.meta.client
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield page['Contents']


@retry(errors=[ErrorCondition(error=ClientError, error_codes=[404, 500, 502, 503, 504])])
def upload_to_s3(readable,
                 s3_resource,
                 bucket: str,
                 key: str,
                 headerArgs: Optional[dict] = None,
                 partSize: int = 50 << 20):
    """
    Upload a readable object to s3, using multipart uploading if applicable.

    :param readable: a readable stream or a file path to upload to s3
    :param S3.Resource resource: boto3 resource
    :param str bucket: name of the bucket to upload to
    :param str key: the name of the file to upload to
    :param dict headerArgs: http headers to use when uploading - generally used for encryption purposes
    :param int partSize: max size of each part in the multipart upload, in bytes
    :return: version of the newly uploaded file
    """
    if headerArgs is None:
        headerArgs = {}

    s3_client = s3_resource.meta.client
    config = TransferConfig(
        multipart_threshold=partSize,
        multipart_chunksize=partSize,
        use_threads=True
    )
    logger.debug("Uploading %s", key)
    if isinstance(readable, str):
        s3_client.upload_file(Filename=readable,
                              Bucket=bucket,
                              Key=key,
                              ExtraArgs=headerArgs,
                              Config=config)
    else:
        s3_client.upload_fileobj(Fileobj=readable,
                                 Bucket=bucket,
                                 Key=key,
                                 ExtraArgs=headerArgs,
                                 Config=config)

        # Wait until the object exists before calling head_object
        object_summary = s3_resource.ObjectSummary(bucket, key)
        object_summary.wait_until_exists(**headerArgs)


@contextmanager
def download_stream(s3_resource, bucket: str, key: str, checksum_to_verify: Optional[str] = None, extra_args: Optional[dict] = None, encoding=None, errors=None):
    """Context manager that gives out a download stream to download data."""
    bucket = s3_resource.Bucket(bucket)

    class DownloadPipe(ReadablePipe):
        def writeTo(self, writable):
            bucket.download_fileobj(Key=key, Fileobj=writable, ExtraArgs=extra_args)

    with DownloadPipe(encoding=encoding, errors=errors) as readable:
        yield readable


# @contextmanager
# def downloadStream(self, verifyChecksum=True, encoding=None, errors=None):
#     """
#     Context manager that gives out a download stream to download data.
#     """
#     class DownloadPipe(ReadablePipe):
#         def writeTo(self, writable):
#             if info.content is not None:
#                 writable.write(info.content)
#             elif info.version:
#                 headerArgs = info._s3EncryptionArgs()
#                 obj = info.outer.filesBucket.Object(compat_bytes(info.fileID))
#                 obj.download_fileobj(writable, ExtraArgs={'VersionId': info.version, **headerArgs})
#             else:
#                 assert False
#
#     class HashingPipe(ReadableTransformingPipe):
#         """
#         Class which checksums all the data read through it. If it
#         reaches EOF and the checksum isn't correct, raises
#         ChecksumError.
#         Assumes info actually has a checksum.
#         """
#
#         def transform(self, readable, writable):
#             hasher = info._start_checksum(to_match=info.checksum)
#             contents = readable.read(1024 * 1024)
#             while contents != b'':
#                 info._update_checksum(hasher, contents)
#                 try:
#                     writable.write(contents)
#                 except BrokenPipeError:
#                     # Read was stopped early by user code.
#                     # Can't check the checksum.
#                     return
#                 contents = readable.read(1024 * 1024)
#             # We reached EOF in the input.
#             # Finish checksumming and verify.
#             info._finish_checksum(hasher)
#             # Now stop so EOF happens in the output.
#
#     if verifyChecksum and self.checksum:
#         with DownloadPipe() as readable:
#             # Interpose a pipe to check the hash
#             with HashingPipe(readable, encoding=encoding, errors=errors) as verified:
#                 yield verified
#     else:
#         # Readable end of pipe produces text mode output if encoding specified
#         with DownloadPipe(encoding=encoding, errors=errors) as readable:
#             # No true checksum available, so don't hash
#             yield readable

# TODO: test below
# def multipart_parallel_upload(
#         s3_resource: typing.Any,
#         bucket: str,
#         key: str,
#         src_file_handle: typing.BinaryIO,
#         *,
#         part_size: int,
#         content_type: str=None,
#         metadata: dict=None,
#         parallelization_factor=8) -> typing.Sequence[dict]:
#     """
#     Upload a file object to s3 in parallel.
#     """
#     s3_client = s3_resource.meta.client
#     kwargs: dict = dict()
#     if content_type is not None:
#         kwargs['ContentType'] = content_type
#     if metadata is not None:
#         kwargs['Metadata'] = metadata
#     upload_id = s3_client.create_multipart_upload(Bucket=bucket, Key=key, **kwargs)['UploadId']
#
#     def _copy_part(data, part_number):
#         resp = s3_client.upload_part(
#             Body=data,
#             Bucket=bucket,
#             Key=key,
#             PartNumber=part_number,
#             UploadId=upload_id,
#         )
#         return resp['ETag']
#
#     def _chunks():
#         while True:
#             data = src_file_handle.read(part_size)
#             if not data:
#                 break
#             yield data
#
#     with ThreadPoolExecutor(max_workers=parallelization_factor) as e:
#         futures = {e.submit(_copy_part, data, part_number): part_number
#                    for part_number, data in enumerate(_chunks(), start=1)}
#         parts = [dict(ETag=f.result(), PartNumber=futures[f]) for f in as_completed(futures)]
#         parts.sort(key=lambda p: p['PartNumber'])
#     s3_client.complete_multipart_upload(
#         Bucket=bucket,
#         Key=key,
#         MultipartUpload=dict(Parts=parts),
#         UploadId=upload_id,
#     )
#     return parts
#
#
# def upload_file_to_s3(s3_resource, bucket: str, key: str, data: bytes):
#     part_size = 16 * 1024 * 1024
#     if len(data) > part_size:
#         with BytesIO(data) as fh:
#             multipart_parallel_upload(
#                 s3_resource,
#                 bucket,
#                 key,
#                 fh,
#                 part_size=part_size,
#                 parallelization_factor=20
#             )
#     else:
#         blobstore.upload_file_handle(bucket, key, BytesIO(data))


def s3_key_exists(s3_resource, bucket: str, key: str, check: bool = False):
    s3_client = s3_resource.meta.client
    try:
        return s3_client.head_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.NoSuchKey:
        if check:
            raise AWSKeyNotFoundError(f"Key '{key}' does not exist in bucket '{bucket}'.")
        else:
            return False


def get_s3_object(s3_resource, bucket: str, key: str):
    s3_client = s3_resource.meta.client
    return s3_client.get_object(Bucket=bucket, Key=key)


def generate_presigned_url(s3_resource, bucket: str, key_name: str, expiration: int) -> Tuple[str, str]:
    s3_client = s3_resource.meta.client
    return s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': key_name},
        ExpiresIn=expiration)


def create_public_url(s3_resource, bucket: str, key: str):
    bucket = Bucket(bucket)
    bucket.Object(key).Acl().put(ACL='public-read')  # TODO: do we need to generate a presigned url after doing this?
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
