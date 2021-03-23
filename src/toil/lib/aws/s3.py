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
import errno
import logging
import socket
from io import BytesIO

import botocore.client
from botocore.exceptions import ClientError
from boto.exception import BotoServerError, S3ResponseError
from toil.lib.retry import old_retry, retry, ErrorCondition

logger = logging.getLogger(__name__)

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
            or (isinstance(e, BotoServerError) and e.status in (429, 500, 502, 503, 504))
            or (isinstance(e, BotoServerError) and e.code in THROTTLED_ERROR_CODES)
            # boto3 errors
            or (isinstance(e, S3ResponseError) and e.error_code in THROTTLED_ERROR_CODES)
            or (isinstance(e, ClientError) and 'BucketNotEmpty' in str(e))
            or (e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 409 and 'try again' in str(e))
            or (e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') in (404, 429, 500, 502, 503, 504)))


# TODO: Replace with: @retry and ErrorCondition
def retry_s3(delays=(0, 1, 1, 4, 16, 64), timeout=300, predicate=retryable_s3_errors):
    return old_retry(delays=delays, timeout=timeout, predicate=predicate)


# TODO: Eliminate all "client" args and always fetch from a centralized source
#  This may involve creating a more permanent config file containing the region and other details


def create_multipart_upload(client: botocore.client.BaseClient, bucket: str, key: str, extra_args: dict) -> dict:
    logger.debug('Starting multipart upload')
    for attempt in retry_s3():
        with attempt:
            # low-level clients are thread safe
            response = client.create_multipart_upload(Bucket=bucket, Key=key, **extra_args)
            # uploadId = upload['UploadId']
            return response


def upload_part(client: botocore.client.BaseClient, bucket: str, key: str, part_num: int, upload_id: str, body: bytes, extra_args: dict):
    # TODO: include the Content-MD5 header:
    #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.complete_multipart_upload
    logger.debug('Uploading part %d of %d bytes', part_num, len(body))
    for attempt in retry_s3():
        with attempt:
            response = client.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=part_num,
                UploadId=upload_id,
                Body=BytesIO(body),
                **extra_args)
            return response


def upload_fileobj(client: botocore.client.BaseClient, bucket: str, key: str, fileobj: bytes, extra_args: dict):
    for attempt in retry_s3():
        with attempt:
            response = client.upload_fileobj(
                Bucket=bucket,
                Key=key,
                Fileobj=BytesIO(fileobj),
                ExtraArgs=extra_args)
            return response


def abort_multipart_upload(client: botocore.client.BaseClient, bucket: str, key: str, upload_id: str):
    for attempt in retry_s3():
        with attempt:
            response = client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            return response


def complete_multipart_upload(client: botocore.client.BaseClient, bucket: str, key: str, upload_id: str, parts: list):
    """Parts looks like: [{"PartNumber": part_num, "ETag": part["ETag"]}]"""
    for attempt in retry_s3():
        with attempt:
            response = client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts})

            logger.debug('Completed upload object of type %s: %s', str(type(response)), repr(response))
            return response


def list_multipart_uploads(client: botocore.client.BaseClient, bucket: str):
    for attempt in retry_s3():
        with attempt:
            response = client.list_multipart_uploads(Bucket=bucket)
            return response


def head_object(client: botocore.client.BaseClient, bucket: str, key: str, extra_args: dict, check_version: bool):
    for attempt in retry_s3(predicate=lambda e: retryable_s3_errors(e) or isinstance(e, AssertionError)):
        with attempt:
            response = client.head_object(Bucket=bucket, Key=key, **extra_args)
            if check_version:
                assert response.get('VersionId', None) is not None
            return response
