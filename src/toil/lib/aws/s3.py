# Copyright (C) 2015-2023 Regents of the University of California
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
import sys

from typing import (Dict,
                    Optional,
                    Union)

from toil.lib.retry import retry
from toil.lib.aws import tags_from_env
from toil.lib.aws.utils import enable_public_objects, flatten_tags

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

try:
    from boto.exception import BotoServerError, S3ResponseError
    from botocore.exceptions import ClientError
    from mypy_boto3_iam import IAMClient, IAMServiceResource
    from mypy_boto3_s3 import S3Client, S3ServiceResource
    from mypy_boto3_s3.literals import BucketLocationConstraintType
    from mypy_boto3_s3.service_resource import Bucket, Object
    from mypy_boto3_sdb import SimpleDBClient
except ImportError:
    BotoServerError = Exception  # type: ignore
    S3ResponseError = Exception  # type: ignore
    ClientError = Exception  # type: ignore
    # AWS/boto extra is not installed


logger = logging.getLogger(__name__)


@retry(errors=[BotoServerError, S3ResponseError, ClientError])
def create_s3_bucket(
    s3_resource: "S3ServiceResource",
    bucket_name: str,
    region: Union["BucketLocationConstraintType", Literal["us-east-1"]],
    tags: Optional[Dict[str]] = None,
    public: bool = True
) -> "Bucket":
    """
    Create an AWS S3 bucket, using the given Boto3 S3 session, with the
    given name, in the given region.

    Supports the us-east-1 region, where bucket creation is special.

    *ALL* S3 bucket creation should use this function.
    """
    logger.debug("Creating bucket '%s' in region %s.", bucket_name, region)
    if region == "us-east-1":  # see https://github.com/boto/boto3/issues/125
        bucket = s3_resource.create_bucket(Bucket=bucket_name)
    else:
        bucket = s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
    # wait until the bucket exists before adding tags
    bucket.wait_until_exists()

    tags = tags_from_env() if tags is None else tags
    bucket_tagging = s3_resource.BucketTagging(bucket_name)
    bucket_tagging.put(Tagging={'TagSet': flatten_tags(tags)})

    # enabling public objects is the historical default
    if public:
        enable_public_objects(bucket_name)

    return bucket
