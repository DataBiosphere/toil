# Copyright (C) 2015-2024 Regents of the University of California
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
from typing import Dict, Any, Optional, List

from mypy_boto3_s3.type_defs import ListMultipartUploadsOutputTypeDef, HeadObjectOutputTypeDef, DeleteObjectOutputTypeDef

from toil.lib.aws import session, AWSServerErrors
from toil.lib.retry import retry

logger = logging.getLogger(__name__)


@retry(errors=[AWSServerErrors])
def head_s3_object(bucket: str, key: str, header: Dict[str, Any], region: Optional[str] = None) -> HeadObjectOutputTypeDef:
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
def delete_s3_object(bucket: str, key: str, version: Optional[str], region: Optional[str] = None) -> DeleteObjectOutputTypeDef:
    """
    Attempt to DELETE an s3 object and return its response.

    :param bucket: AWS bucket name
    :param key:  AWS Key name for the s3 object
    :param version: The object's version ID, if it exists
    :param region: Region that we want to look for the bucket in
    """
    s3_client = session.client("s3", region_name=region)
    if version:
        return s3_client.delete_object(Bucket=bucket, Key=key, VersionId=version)
    else:
        return s3_client.delete_object(Bucket=bucket, Key=key)


@retry(errors=[AWSServerErrors])
def head_s3_bucket(bucket: str, region: Optional[str] = None) -> HeadBucketOutputTypeDef:
    """
    Attempt to HEAD an s3 bucket and return its response.

    :param bucket: AWS bucket name
    :param region: Region that we want to look for the bucket in
    """
    s3_client = session.client("s3", region_name=region)
    return s3_client.head_bucket(Bucket=bucket)


def list_multipart_uploads(bucket: str, region: str, prefix: str, max_uploads: int = 1) -> ListMultipartUploadsOutputTypeDef:
    s3_client = session.client("s3", region_name=region)
    return s3_client.list_multipart_uploads(Bucket=bucket, MaxUploads=max_uploads, Prefix=prefix)
