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

import boto3

from toil.jobStores.aws.utils import fileSizeAndTime
from toil.lib.compatibility import compat_bytes


def uploadFromPath(localFilePath, partSize, bucket, fileID, headers):
    """
    Uploads a file to s3, using multipart uploading if applicable

    :param str localFilePath: Path of the file to upload to s3
    :param int partSize: max size of each part in the multipart upload, in bytes
    :param S3.Bucket bucket: the s3 bucket to upload to. *Note this is a boto3 Bucket instance.*
    :param str fileID: the name of the file to upload to
    :param headers: http headers to use when uploading - generally used for encryption purposes
    :return: version of the newly uploaded file
    """
    sseAlgorithm = headers and headers.get('x-amz-server-side-encryption-customer-algorithm')
    sseKey = headers and headers.get('x-amz-server-side-encryption-customer-key')

    # see toil.jobStores.aws.utils.copyKeyMultipart
    args = {}
    if sseAlgorithm:
        args.update({'SSECustomerAlgorithm': sseAlgorithm, 'SSECustomerKey': sseKey})

    file_size, file_time = fileSizeAndTime(localFilePath)
    if file_size <= partSize:
        obj = bucket.Object(key=compat_bytes(fileID))
        # for attempt in retry_s3():
        #     with attempt:
        # if necessary we can also use obj.upload_file for multipart upload
        obj.put(Body=open(localFilePath, 'rb'), **args)
        version = obj.version_id
    else:
        with open(localFilePath, 'rb') as f:
            # version = chunkedFileUpload(f, bucket, fileID, file_size, headers, partSize)
            version = ''
            # [!!] TODO: To be implemented
            pass

    size = boto3.client('s3').head_object(Bucket=bucket.name,
                                          Key=compat_bytes(fileID), VersionId=version)['ContentLength']
    assert size == file_size

    # Make reasonably sure that the file wasn't touched during the upload
    assert fileSizeAndTime(localFilePath) == (file_size, file_time)

    return version


def chunkedFileUpload(readable, bucket, fileID, file_size, headers=None, partSize=50 << 20):
    pass


class S3KeyWrapper:
    """
    *Temporary* Not part of the Toil API! This class will be deleted once usages of boto.s3.key.Key
    are converted to S3.Object. Only the methods and attributes used in Toil are implemented.
    """
    def __init__(self, obj):
        """
        :param obj: S3.Object
        """
        self.obj = obj

    # Attributes mapping:
    # -----------------------------------
    # boto.s3.key.Key       =>  S3.Object
    # size                  =>  content_length
    # name                  =>  key
    # version_id            =>  version_id
    # get_contents_as_string()      =>  get()
    # set_contents_from_string()    =>  put()
    # get_contents_to_file()        =>  get()
    # bucket                        =>  n/A
    #       name            =>  bucket_name
    #       connection      =>  n/A

    @property
    def size(self):
        return self.obj.content_length

    @property
    def name(self):
        return self.obj.key

    @property
    def version_id(self):
        return self.obj.version_id

    @property
    def bucket(self):
        return BucketWrapper(self.obj.bucket_name)

    def get_contents_as_string(self):
        return self.obj.get()['Body'].read()

    def set_contents_from_string(self, contents):
        pass

    def get_contents_to_file(self, fp):
        pass


class BucketWrapper:
    def __init__(self, bucket_name):
        self.bucketName = bucket_name

    @property
    def name(self):
        return self.bucketName

    @property
    def connection(self):
        return self.ConnectionWrapper()

    class ConnectionWrapper:
        def close(self):
            pass
