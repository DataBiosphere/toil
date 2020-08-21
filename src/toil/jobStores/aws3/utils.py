# Copyright (C) 2015-2016 Regents of the University of California
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

from __future__ import absolute_import

import logging

from boto3.s3.transfer import TransferConfig

from toil.jobStores.aws.utils import fileSizeAndTime
from toil.lib.compatibility import compat_bytes, compat_oldstr

log = logging.getLogger(__name__)


def uploadFromPath(localFilePath, resource, bucketName, fileID, args=None, partSize=50 << 20):
    """
    Uploads a file to s3, using multipart uploading if applicable

    :param str localFilePath: Path of the file to upload to s3
    :param S3.Resource resource: boto3 resource
    :param str bucketName: name of the bucket to upload to
    :param str fileID: the name of the file to upload to
    :param dict args: http headers to use when uploading - generally used for encryption purposes
    :param int partSize: max size of each part in the multipart upload, in bytes

    :return: version of the newly uploaded file
    """
    if args is None:
        args = {}

    client = resource.meta.client
    file_size, file_time = fileSizeAndTime(localFilePath)
    if file_size <= partSize:
        obj = resource.Object(bucketName, key=compat_bytes(fileID))
        obj.put(Body=open(localFilePath, 'rb'), **args)
        version = obj.version_id
    else:
        version = chunkedFileUpload(localFilePath, client, bucketName, fileID, args, partSize)

    size = client.head_object(Bucket=bucketName,
                              Key=compat_bytes(fileID), VersionId=version)['ContentLength']
    assert size == file_size

    # Make reasonably sure that the file wasn't touched during the upload
    assert fileSizeAndTime(localFilePath) == (file_size, file_time)

    return version


def chunkedFileUpload(readable, client, bucketName, fileID, args=None, partSize=50 << 20):
    """
    Upload a readable object to s3 using multipart upload.

    :param readable: a readable stream or a file path to upload to s3
    :param S3.Client client: boto3 client
    :param str bucketName: name of the bucket to upload to
    :param str fileID: the name of the file to upload to
    :param dict args: http headers to use when uploading - generally used for encryption purposes
    :param int partSize: max size of each part in the multipart upload, in bytes

    :return: version of the newly uploaded file
    """
    if args is None:
        args = {}

    config = TransferConfig(
        multipart_threshold=partSize,
        multipart_chunksize=partSize,
        use_threads=True
    )
    if isinstance(readable, str):
        client.upload_file(FileName=readable, Bucket=bucketName, Key=compat_bytes(fileID),
                           ExtraArgs=args, Config=config)
    else:
        client.upload_fileobj(Fileobj=readable, Bucket=bucketName, Key=compat_bytes(fileID),
                              ExtraArgs=args, Config=config)

    version = client.head_object(Bucket=bucketName, Key=compat_bytes(fileID), **args)['VersionId']
    return version


def copyKeyMultipart(resource, srcBucketName, srcKeyName, srcKeyVersion, dstBucketName, dstKeyName,
                     dstEncryptionArgs=None, srcEncryptionArgs=None):
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

    :param dict dstEncryptionArgs: SSE headers for the destination
    :param dict srcEncryptionArgs: SSE headers for the source

    :rtype: str
    :return: The version of the copied file (or None if versioning is not enabled for dstBucket).
    """
    dstBucket = resource.Bucket(compat_oldstr(dstBucketName))
    dstObject = dstBucket.Object(compat_oldstr(dstKeyName))
    copySource = {'Bucket': compat_oldstr(srcBucketName), 'Key': compat_oldstr(srcKeyName)}
    if srcKeyVersion is not None:
        copySource['VersionId'] = compat_oldstr(srcKeyVersion)

    dstObject.copy(copySource, ExtraArgs=srcEncryptionArgs)

    # Wait until the object exists before calling head_object
    object_summary = resource.ObjectSummary(dstObject.bucket_name, dstObject.key)
    object_summary.wait_until_exists(**dstEncryptionArgs)

    # Unfortunately, boto3's managed copy doesn't return the version
    # that it actually copied to. So we have to check immediately
    # after, leaving open the possibility that it may have been
    # modified again in the few seconds since the copy finished. There
    # isn't much we can do about it.
    info = resource.meta.client.head_object(Bucket=dstObject.bucket_name, Key=dstObject.key,
                                            **dstEncryptionArgs)
    return info.get('VersionId', None)
