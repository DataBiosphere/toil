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
from toil.lib.compatibility import compat_bytes

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
