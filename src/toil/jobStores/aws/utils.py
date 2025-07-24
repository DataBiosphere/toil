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
import base64
import bz2
import logging
import os
import types
from ssl import SSLError
from typing import TYPE_CHECKING, IO, Optional, cast, Any

from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from botocore.exceptions import ClientError

from toil.lib.aws import AWSServerErrors, session
from toil.lib.aws.utils import connection_error, get_bucket_region
from toil.lib.compatibility import compat_bytes
from toil.lib.retry import (
    DEFAULT_DELAYS,
    DEFAULT_TIMEOUT,
    get_error_code,
    get_error_message,
    get_error_status,
    old_retry,
    retry,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client, S3ServiceResource
    from mypy_boto3_s3.type_defs import CopySourceTypeDef

logger = logging.getLogger(__name__)

# Make one botocore Config object for setting up S3 to talk to the exact region
# we told it to talk to, so that we don't keep making new objects that look
# like distinct memoization keys. We need to set the addressing style to path
# so that we talk to region-specific DNS names and not per-bucket DNS names. We
# also need to set a special flag to make sure we don't use the generic
# s3.amazonaws.com for us-east-1, or else we might not actually end up talking
# to us-east-1 when a bucket is there.
DIAL_SPECIFIC_REGION_CONFIG = Config(
    s3={"addressing_style": "path", "us_east_1_regional_endpoint": "regional"}
)


def fileSizeAndTime(localFilePath: str) -> tuple[int, float]:
    file_stat = os.stat(localFilePath)
    return file_stat.st_size, file_stat.st_mtime


# TODO: This function is unused.
@retry(errors=[AWSServerErrors])
def uploadFromPath(
    localFilePath: str,
    resource: "S3ServiceResource",
    bucketName: str,
    fileID: str,
    headerArgs: Optional[dict[str, Any]] = None,
    partSize: int = 50 << 20,
) -> Optional[str]:
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

    version = uploadFile(
        localFilePath, resource, bucketName, fileID, headerArgs, partSize
    )

    # Only pass along version if we got one.
    version_args: dict[str, Any] = {"VersionId": version} if version is not None else {}

    info = client.head_object(
        Bucket=bucketName, Key=compat_bytes(fileID), **version_args, **headerArgs
    )
    size = info.get("ContentLength")

    assert size == file_size

    # Make reasonably sure that the file wasn't touched during the upload
    assert fileSizeAndTime(localFilePath) == (file_size, file_time)
    return version


@retry(errors=[AWSServerErrors])
def uploadFile(
    readable: IO[bytes],
    resource: "S3ServiceResource",
    bucketName: str,
    fileID: str,
    headerArgs: Optional[dict[str, Any]] = None,
    partSize: int = 50 << 20,
) -> Optional[str]:
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
        multipart_threshold=partSize, multipart_chunksize=partSize, use_threads=True
    )
    if isinstance(readable, str):
        client.upload_file(
            Filename=readable,
            Bucket=bucketName,
            Key=compat_bytes(fileID),
            ExtraArgs=headerArgs,
            Config=config,
        )
    else:
        client.upload_fileobj(
            Fileobj=readable,
            Bucket=bucketName,
            Key=compat_bytes(fileID),
            ExtraArgs=headerArgs,
            Config=config,
        )

        # Wait until the object exists before calling head_object
        object_summary = resource.ObjectSummary(bucketName, compat_bytes(fileID))
        object_summary.wait_until_exists(**headerArgs)

    info = client.head_object(Bucket=bucketName, Key=compat_bytes(fileID), **headerArgs)
    return info.get("VersionId", None)


class ServerSideCopyProhibitedError(RuntimeError):
    """
    Raised when AWS refuses to perform a server-side copy between S3 keys, and
    insists that you pay to download and upload the data yourself instead.
    """


@retry(errors=[AWSServerErrors])
def copyKeyMultipart(
    resource: "S3ServiceResource",
    srcBucketName: str,
    srcKeyName: str,
    srcKeyVersion: str,
    dstBucketName: str,
    dstKeyName: str,
    sseAlgorithm: Optional[str] = None,
    sseKey: Optional[str] = None,
    copySourceSseAlgorithm: Optional[str] = None,
    copySourceSseKey: Optional[str] = None,
) -> Optional[str]:
    """
    Copies a key from a source key to a destination key in multiple parts. Note that if the
    destination key exists it will be overwritten implicitly, and if it does not exist a new
    key will be created. If the destination bucket does not exist an error will be raised.

    This function will always do a fast, server-side copy, at least
    until/unless <https://github.com/boto/boto3/issues/3270> is fixed. In some
    situations, a fast, server-side copy is not actually possible. For example,
    when residing in an AWS VPC with an S3 VPC Endpoint configured, copying
    from a bucket in another region to a bucket in your own region cannot be
    performed server-side. This is because the VPC Endpoint S3 API servers
    refuse to perform server-side copies between regions, the source region's
    API servers refuse to initiate the copy and refer you to the destination
    bucket's region's API servers, and the VPC routing tables are configured to
    redirect all access to the current region's S3 API servers to the S3
    Endpoint API servers instead.

    If a fast server-side copy is not actually possible, a
    ServerSideCopyProhibitedError will be raised.

    :param resource: boto3 resource
    :param str srcBucketName: The name of the bucket to be copied from.
    :param str srcKeyName: The name of the key to be copied from.
    :param str srcKeyVersion: The version of the key to be copied from.
    :param str dstBucketName: The name of the destination bucket for the copy.
    :param str dstKeyName: The name of the destination key that will be created or overwritten.
    :param str sseAlgorithm: Server-side encryption algorithm for the destination.
    :param str sseKey: Server-side encryption key for the destination.
    :param str copySourceSseAlgorithm: Server-side encryption algorithm for the source.
    :param str copySourceSseKey: Server-side encryption key for the source.

    :return: The version of the copied file (or None if versioning is not enabled for dstBucket).
    """
    dstBucket = resource.Bucket(compat_bytes(dstBucketName))
    dstObject = dstBucket.Object(compat_bytes(dstKeyName))
    copySource: "CopySourceTypeDef" = {
        "Bucket": compat_bytes(srcBucketName),
        "Key": compat_bytes(srcKeyName),
    }
    if srcKeyVersion is not None:
        copySource["VersionId"] = compat_bytes(srcKeyVersion)

    # Get a client to the source region, which may not be the same as the one
    # this resource is connected to. We should probably talk to it for source
    # object metadata. And we really want it to talk to the source region and
    # not wherever the bucket virtual hostnames go.
    source_region = get_bucket_region(srcBucketName)
    source_client = session.client(
        "s3", region_name=source_region, config=DIAL_SPECIFIC_REGION_CONFIG
    )

    # The boto3 functions don't allow passing parameters as None to
    # indicate they weren't provided. So we have to do a bit of work
    # to ensure we only provide the parameters when they are actually
    # required.
    destEncryptionArgs: dict[str, Any] = {}
    if sseKey is not None:
        destEncryptionArgs.update(
            {"SSECustomerAlgorithm": sseAlgorithm, "SSECustomerKey": sseKey}
        )
    copyEncryptionArgs: dict[str, Any] = {}
    if copySourceSseKey is not None:
        copyEncryptionArgs.update(
            {
                "CopySourceSSECustomerAlgorithm": copySourceSseAlgorithm,
                "CopySourceSSECustomerKey": copySourceSseKey,
            }
        )
    copyEncryptionArgs.update(destEncryptionArgs)

    try:
        # Kick off a server-side copy operation
        dstObject.copy(
            copySource, SourceClient=source_client, ExtraArgs=copyEncryptionArgs
        )
    except ClientError as e:
        if get_error_code(e) == "AccessDenied" and "cross-region" in get_error_message(
            e
        ):
            # We have this problem: <https://aws.amazon.com/premiumsupport/knowledge-center/s3-troubleshoot-copy-between-buckets/#Cross-Region_request_issues_with_VPC_endpoints_for_Amazon_S3>
            # The Internet and AWS docs say that we just can't do a
            # cross-region CopyObject from inside a VPC with an endpoint. The
            # AGC team suggested we try doing the copy by talking to the
            # source region instead, but that does not actually work; if we
            # hack up botocore enough for it to actually send the request to
            # the source region's API servers, they reject it and tell us to
            # talk to the destination region's API servers instead. Which we
            # can't reach.
            logger.error(
                "Amazon is refusing to perform a server-side copy of %s: %s",
                copySource,
                e,
            )
            raise ServerSideCopyProhibitedError()
        else:
            # Some other ClientError happened
            raise

    # Wait until the object exists before calling head_object
    object_summary = resource.ObjectSummary(dstObject.bucket_name, dstObject.key)
    object_summary.wait_until_exists(**destEncryptionArgs)

    # Unfortunately, boto3's managed copy doesn't return the version
    # that it actually copied to. So we have to check immediately
    # after, leaving open the possibility that it may have been
    # modified again in the few seconds since the copy finished. There
    # isn't much we can do about it.
    info = resource.meta.client.head_object(
        Bucket=dstObject.bucket_name, Key=dstObject.key, **destEncryptionArgs
    )
    return info.get("VersionId", None)


def retryable_ssl_error(e: BaseException) -> bool:
    # https://github.com/BD2KGenomics/toil/issues/978
    return isinstance(e, SSLError) and e.reason == "DECRYPTION_FAILED_OR_BAD_RECORD_MAC"
