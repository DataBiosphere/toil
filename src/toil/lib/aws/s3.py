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
import os
import logging
import pickle
import hashlib
import re
import reprlib
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager
from io import BytesIO
from typing import Optional, Tuple, Union

import boto.sdb
from boto.exception import SDBResponseError
from botocore.exceptions import ClientError

from toil.fileStores import FileID
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             JobStoreExistsException,
                                             NoSuchJobException,
                                             NoSuchJobStoreException)
from toil.jobStores.aws.utils import (SDBHelper,
                                      bucket_location_to_region,
                                      uploadFile,
                                      region_to_bucket_location)
from toil.lib.checksum import compute_checksum_for_content
from toil.lib.compatibility import compat_bytes
from toil.lib.ec2 import establish_boto3_session
from toil.lib.pipes import WritablePipe
from toil.lib.ec2nodes import EC2Regions
from toil.lib.retry import retry

boto3_session = establish_boto3_session()
s3_boto3_resource = boto3_session.resource('s3')
s3_boto3_client = boto3_session.client('s3')
logger = logging.getLogger(__name__)


# TODO: Determine specific retries
@retry()
def create_bucket(bucket: str, region: Optional[str] = None, versioning: bool = False) -> s3_boto3_resource.Bucket:
    logger.debug(f"Creating AWS bucket: {bucket}")
    if region is None:
        s3_client = boto3_session.client('s3')
        bucket = s3_client.create_bucket(Bucket=bucket)
    else:
        s3_client = boto3_session.client('s3', region_name=region)
        bucket = s3_client.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': region})
    bucket.wait_until_exists()

    if versioning:
        bucket.Versioning().enable()
        # Now wait until versioning is actually on. Some uploads
        # would come back with no versions; maybe they were
        # happening too fast and this setting isn't sufficiently consistent?
        time.sleep(1)
        while not bucket_versioning_enabled(bucket.name):
            logger.debug(f"Waiting for versioning activation on bucket '{bucket.name}'...")
            time.sleep(1)
    logger.debug(f"Successfully created new bucket '{bucket.name}' with versioning '{versioning}'")
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
def bucket_versioning_enabled(self, bucket: str):
    versionings = dict(Enabled=True, Disabled=False, Suspended=None)
    status = self.s3_resource.BucketVersioning(bucket).status
    return versionings.get(status) if status else False


# TODO: Determine specific retries
@retry()
def bucket_is_registered_with_toil(bucket: str) -> Union[bool, s3_boto3_resource.Bucket]:
    return False


class MultiPartPipe(WritablePipe):
    def readFrom(self, readable):
        # Get the first block of data we want to put
        buf = readable.read(store.partSize)
        assert isinstance(buf, bytes)

        # We will compute a checksum
        hasher = hashlib.sha1()
        hasher.update(buf)

        client = store.s3_client
        bucket_name = store.filesBucket.name
        headerArgs = info._s3EncryptionArgs()

        # low-level clients are thread safe
        upload = client.create_multipart_upload(Bucket=bucket_name,
                                                Key=compat_bytes(info.fileID),
                                                **headerArgs)
        uploadId = upload['UploadId']
        parts = []
        try:
            for part_num in itertools.count():
                logger.debug('Uploading part %d of %d bytes', part_num + 1, len(buf))
                # TODO: include the Content-MD5 header:
                #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.complete_multipart_upload
                part = client.upload_part(Bucket=bucket_name,
                                          Key=compat_bytes(info.fileID),
                                          PartNumber=part_num + 1,
                                          UploadId=uploadId,
                                          Body=BytesIO(buf),
                                          **headerArgs)
                parts.append({"PartNumber": part_num + 1, "ETag": part["ETag"]})

                # Get the next block of data we want to put
                buf = readable.read(info.outer.partSize)
                assert isinstance(buf, bytes)
                if len(buf) == 0:
                    # Don't allow any part other than the very first to be empty.
                    break
                hasher.update(buf)
        except:
            client.abort_multipart_upload(Bucket=bucket_name,
                                          Key=compat_bytes(info.fileID),
                                          UploadId=uploadId)
        else:
            # Save the checksum
            checksum = info._finish_checksum(hasher)

            logger.debug('Attempting to complete upload...')
            completed = client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=compat_bytes(info.fileID),
                UploadId=uploadId,
                MultipartUpload={"Parts": parts})
