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
import os
from typing import Optional

from boto.exception import (
    BotoServerError,
    SDBResponseError,
    S3ResponseError
)
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from toil.lib.compatibility import compat_bytes
from toil.lib.retry import retry, ErrorCondition

logger = logging.getLogger(__name__)


def fileSizeAndTime(localFilePath):
    file_stat = os.stat(localFilePath)
    return file_stat.st_size, file_stat.st_mtime


@retry(errors=[ErrorCondition(
    error=ClientError,
    error_codes=[404, 500, 502, 503, 504]
)])
def uploadFromPath(localFilePath: str,
                   resource,
                   bucketName: str,
                   fileID: str,
                   headerArgs: Optional[dict] = None,
                   partSize: int = 50 << 20):
    """
    Uploads a file to s3, using multipart uploading if applicable

    :param str localFilePath: Path of the file to upload to s3
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
    file_size, file_time = fileSizeAndTime(localFilePath)

    version = uploadFile(localFilePath, resource, bucketName, fileID, headerArgs, partSize)
    info = client.head_object(Bucket=bucketName, Key=compat_bytes(fileID), VersionId=version, **headerArgs)
    size = info.get('ContentLength')

    assert size == file_size

    # Make reasonably sure that the file wasn't touched during the upload
    assert fileSizeAndTime(localFilePath) == (file_size, file_time)
    return version


@retry(errors=[ErrorCondition(
    error=ClientError,
    error_codes=[404, 500, 502, 503, 504]
)])
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
                           Key=compat_bytes(fileID),
                           ExtraArgs=headerArgs,
                           Config=config)
    else:
        client.upload_fileobj(Fileobj=readable,
                              Bucket=bucketName,
                              Key=compat_bytes(fileID),
                              ExtraArgs=headerArgs,
                              Config=config)

        # Wait until the object exists before calling head_object
        object_summary = resource.ObjectSummary(bucketName, compat_bytes(fileID))
        object_summary.wait_until_exists(**headerArgs)

    info = client.head_object(Bucket=bucketName, Key=compat_bytes(fileID), **headerArgs)
    return info.get('VersionId', None)


@retry(errors=[ErrorCondition(
    error=ClientError,
    error_codes=[404, 500, 502, 503, 504]
)])
def copyKeyMultipart(resource,
                     srcBucketName: str,
                     srcKeyName: str,
                     srcKeyVersion: str,
                     dstBucketName: str,
                     dstKeyName: str,
                     sseAlgorithm: Optional[str] = None,
                     sseKey: Optional[str] = None,
                     copySourceSseAlgorithm: Optional[str] = None,
                     copySourceSseKey: Optional[str] = None):
    """
    Copies a key from a source key to a destination key in multiple parts. Note that if the
    destination key exists it will be overwritten implicitly, and if it does not exist a new
    key will be created. If the destination bucket does not exist an error will be raised.

    :param S3.Resource resource: boto3 resource
    :param str srcBucketName: The name of the bucket to be copied from.
    :param str srcKeyName: The name of the key to be copied from.
    :param str srcKeyVersion: The version of the key to be copied from.
    :param str dstBucketName: The name of the destination bucket for the copy.
    :param str dstKeyName: The name of the destination key that will be created or overwritten.
    :param str sseAlgorithm: Server-side encryption algorithm for the destination.
    :param str sseKey: Server-side encryption key for the destination.
    :param str copySourceSseAlgorithm: Server-side encryption algorithm for the source.
    :param str copySourceSseKey: Server-side encryption key for the source.

    :rtype: str
    :return: The version of the copied file (or None if versioning is not enabled for dstBucket).
    """
    dstBucket = resource.Bucket(compat_bytes(dstBucketName))
    dstObject = dstBucket.Object(compat_bytes(dstKeyName))
    copySource = {'Bucket': compat_bytes(srcBucketName), 'Key': compat_bytes(srcKeyName)}
    if srcKeyVersion is not None:
        copySource['VersionId'] = compat_bytes(srcKeyVersion)

    # The boto3 functions don't allow passing parameters as None to
    # indicate they weren't provided. So we have to do a bit of work
    # to ensure we only provide the parameters when they are actually
    # required.
    destEncryptionArgs = {}
    if sseKey is not None:
        destEncryptionArgs.update({'SSECustomerAlgorithm': sseAlgorithm,
                                   'SSECustomerKey': sseKey})
    copyEncryptionArgs = {}
    if copySourceSseKey is not None:
        copyEncryptionArgs.update({'CopySourceSSECustomerAlgorithm': copySourceSseAlgorithm,
                                   'CopySourceSSECustomerKey': copySourceSseKey})
    copyEncryptionArgs.update(destEncryptionArgs)

    dstObject.copy(copySource, ExtraArgs=copyEncryptionArgs)

    # Wait until the object exists before calling head_object
    object_summary = resource.ObjectSummary(dstObject.bucket_name, dstObject.key)
    object_summary.wait_until_exists(**destEncryptionArgs)

    # Unfortunately, boto3's managed copy doesn't return the version
    # that it actually copied to. So we have to check immediately
    # after, leaving open the possibility that it may have been
    # modified again in the few seconds since the copy finished. There
    # isn't much we can do about it.
    info = resource.meta.client.head_object(Bucket=dstObject.bucket_name,
                                            Key=dstObject.key,
                                            **destEncryptionArgs)
    return info.get('VersionId', None)


# https://github.com/boto/botocore/blob/49f87350d54f55b687969ec8bf204df785975077/botocore/retries/standard.py#L316
THROTTLED_ERROR_CODES = [
        'Throttling',
        'ThrottlingException',
        'ThrottledException',
        'RequestThrottledException',
        'TooManyRequestsException',
        'ProvisionedThroughputExceededException',
        'TransactionInProgressException',
        'RequestLimitExceeded',
        'BandwidthLimitExceeded',
        'LimitExceededException',
        'RequestThrottled',
        'SlowDown',
        'PriorRequestNotComplete',
        'EC2ThrottledException',
]


# TODO: Replace with: @retry and ErrorCondition
def retryable_s3_errors(e):
    return    ((isinstance(e, socket.error) and e.errno in (errno.ECONNRESET, 104))
            or (isinstance(e, BotoServerError) and e.status in (429, 500))
            or (isinstance(e, BotoServerError) and e.code in THROTTLED_ERROR_CODES)
            # boto3 errors
            or (isinstance(e, S3ResponseError) and e.error_code in THROTTLED_ERROR_CODES)
            or (isinstance(e, ClientError) and 'BucketNotEmpty' in str(e))
            or (isinstance(e, ClientError) and e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 409 and 'try again' in str(e))
            or (isinstance(e, ClientError) and e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') in (404, 429, 500, 502, 503, 504)))


def retry_s3(delays=(0, 1, 1, 4, 16, 64), timeout=300, predicate=retryable_s3_errors):
    return old_retry(delays=delays, timeout=timeout, predicate=predicate)


def region_to_bucket_location(region):
    return '' if region == 'us-east-1' else region


def bucket_location_to_region(location):
    return 'us-east-1' if location == '' else location
