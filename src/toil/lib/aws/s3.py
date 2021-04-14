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

from contextlib import contextmanager
from typing import Optional
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from toil.lib.ec2 import establish_boto3_session
from toil.lib.pipes import WritablePipe
from toil.lib.compatibility import compat_bytes
from toil.lib.pipes import ReadablePipe, HashingPipe
from toil.lib.retry import retry, ErrorCondition

boto3_session = establish_boto3_session()
s3_boto3_resource = boto3_session.resource('s3')
s3_boto3_client = boto3_session.client('s3')
logger = logging.getLogger(__name__)


# TODO: Determine specific retries
@retry()
def create_bucket(bucket: str, region: Optional[str] = None) -> s3_boto3_resource.Bucket:
    logger.debug(f"Creating AWS bucket: {bucket}")
    if region is None:
        s3_client = boto3_session.client('s3')
        bucket = s3_client.create_bucket(Bucket=bucket)
    else:
        s3_client = boto3_session.client('s3', region_name=region)
        bucket = s3_client.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': region})
    bucket.wait_until_exists()
    logger.debug(f"Successfully created new bucket '{bucket.name}'")
    return bucket


# TODO: Determine specific retries
@retry()
def delete_bucket(bucket: str) -> None:
    logger.debug(f"Deleting AWS bucket: {bucket}")
    bucket = s3_boto3_resource.Bucket(bucket)
    try:
        uploads = s3_boto3_client.list_multipart_uploads(Bucket=bucket.name).get('Uploads') or list()
        for u in uploads:
            s3_boto3_client.abort_multipart_upload(Bucket=bucket.name, Key=u["Key"], UploadId=u["UploadId"])

        bucket.objects.all().delete()
        bucket.object_versions.delete()
        bucket.delete()
    except s3_boto3_client.exceptions.NoSuchBucket:
        pass
    except ClientError as e:
        if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 404:
            raise
    logger.debug(f"Successfully deleted bucket '{bucket.name}'")


# TODO: Determine specific retries
@retry()
def bucket_exists(bucket: str) -> Union[bool, s3_boto3_resource.Bucket]:
    try:
        s3_boto3_client.head_bucket(Bucket=bucket)
        return s3_boto3_resource.Bucket(bucket)
    except ClientError as e:
        error_code = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if error_code == 404:
            return False
        else:
            raise


# TODO: Determine specific retries
@retry()
def copy_s3_to_s3(src_bucket, src_key, dst_bucket, dst_key):
    source = {'Bucket': src_bucket, 'Key': src_key}
    dest = s3_boto3_resource.Bucket(dst_bucket)
    dest.copy(source, dst_key)


# TODO: Determine specific retries
@retry()
def copy_local_to_s3(local_file_path, dst_bucket, dst_key):
    s3_boto3_client.upload_file(local_file_path, dst_bucket, dst_key)


# TODO: Determine specific retries
@retry()
def bucket_versioning_enabled(bucket: str):
    versionings = dict(Enabled=True, Disabled=False, Suspended=None)
    status = s3_boto3_resource.BucketVersioning(bucket).status
    return versionings.get(status) if status else False


# TODO: Determine specific retries
@retry()
def bucket_is_registered_with_toil(bucket: str) -> Union[bool, s3_boto3_resource.Bucket]:
    return False


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


def generate_presigned_url(bucket: str, key_name: str, expiration: int) -> Tuple[str, str]:
    return s3_boto3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': key_name},
        ExpiresIn=expiration)


def boto_args():
    host = os.environ.get('TOIL_S3_HOST', None)
    port = os.environ.get('TOIL_S3_PORT', None)
    protocol = 'https'
    if os.environ.get('TOIL_S3_USE_SSL', True) == 'False':
        protocol = 'http'
    if host:
        return {'endpoint_url': f'{protocol}://{host}' + f':{port}' if port else ''}
    return {}


def list_s3_items(bucket, prefix):
    paginator = s3_boto3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield page['Contents']


@retry(errors=[ErrorCondition(error=ClientError, error_codes=[404, 500, 502, 503, 504])])
def uploadFile(readable,
               resource,
               bucketName: str,
               fileID: str,
               headerArgs: Optional[dict] = None,
               partSize: int = 50 << 20):
    """
    Upload a readable object to s3, using multipart uploading if applicable.

    :param readable: a readable stream or a file path to upload to s3
    :param S3.Resource resource: boto3 resource
    :param str bucketName: name of the bucket to upload to
    :param str fileID: the name of the file to upload to
    :param dict headerArgs: http headers to use when uploading - generally used for encryption purposes
    :param int partSize: max size of each part in the multipart upload, in bytes
    :return: version of the newly uploaded file
    """
    if headerArgs is None:
        headerArgs = {}

    client = resource.meta.client
    config = TransferConfig(
        multipart_threshold=partSize,
        multipart_chunksize=partSize,
        use_threads=True
    )
    logger.debug("Uploading %s", fileID)
    if isinstance(readable, str):
        client.upload_file(Filename=readable,
                           Bucket=bucketName,
                           Key=fileID,
                           ExtraArgs=headerArgs,
                           Config=config)
    else:
        client.upload_fileobj(Fileobj=readable,
                              Bucket=bucketName,
                              Key=fileID,
                              ExtraArgs=headerArgs,
                              Config=config)

        # Wait until the object exists before calling head_object
        object_summary = resource.ObjectSummary(bucketName, fileID)
        object_summary.wait_until_exists(**headerArgs)



@contextmanager
def download_stream(s3_object, checksum_to_verify: Optional[str] = None, extra_args: Optional[dict] = None, encoding=None, errors=None):
    """Context manager that gives out a download stream to download data."""
    class DownloadPipe(ReadablePipe):
        def writeTo(self, writable):
            s3_object.download_fileobj(writable, ExtraArgs=extra_args)

    if checksum_to_verify:
        with DownloadPipe() as readable:
            # Interpose a pipe to check the hash
            with HashingPipe(readable,
                             encoding=encoding,
                             errors=errors,
                             checksum_to_verify=checksum_to_verify) as verified:
                yield verified
    else:
        # Readable end of pipe produces text mode output if encoding specified
        with DownloadPipe(encoding=encoding, errors=errors) as readable:
            # No true checksum available, so don't hash
            yield readable
