# Copyright (C) 2021 Michael R. Crusoe
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
import uuid
from typing import Optional

from toil.jobStores.aws.jobStore import AWSJobStore
from toil.lib.aws.session import establish_boto3_session
from toil.lib.aws.utils import create_s3_bucket, get_bucket_region
from toil.test import ToilTest, needs_aws_s3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


@needs_aws_s3
class S3Test(ToilTest):
    """Confirm the workarounds for us-east-1."""

    from mypy_boto3_s3 import S3ServiceResource
    from mypy_boto3_s3.service_resource import Bucket

    s3_resource: Optional[S3ServiceResource]
    bucket: Optional[Bucket]

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        session = establish_boto3_session(region_name="us-east-1")
        cls.s3_resource = session.resource("s3", region_name="us-east-1")
        cls.bucket = None

    def test_create_bucket(self) -> None:
        """Test bucket creation for us-east-1."""
        bucket_name = f"toil-s3test-{uuid.uuid4()}"
        assert self.s3_resource
        S3Test.bucket = create_s3_bucket(self.s3_resource, bucket_name, "us-east-1")
        S3Test.bucket.wait_until_exists()
        owner_tag = os.environ.get("TOIL_OWNER_TAG")
        if owner_tag:
            bucket_tagging = self.s3_resource.BucketTagging(bucket_name)
            bucket_tagging.put(
                Tagging={"TagSet": [{"Key": "Owner", "Value": owner_tag}]}
            )
        self.assertEqual(get_bucket_region(bucket_name), "us-east-1")

        # Make sure all the bucket location getting strategies work on a bucket we created
        self.assertEqual(get_bucket_region(bucket_name, only_strategies = {1}), "us-east-1")
        self.assertEqual(get_bucket_region(bucket_name, only_strategies = {2}), "us-east-1")
        self.assertEqual(get_bucket_region(bucket_name, only_strategies = {3}), "us-east-1")

    def test_get_bucket_location_public_bucket(self) -> None:
        """
        Test getting buket location for a bucket we don't own.
        """

        bucket_name = 'spacenet-dataset'
        # This bucket happens to live in us-east-1
        self.assertEqual(get_bucket_region(bucket_name), "us-east-1")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.bucket:
            AWSJobStore._delete_bucket(cls.bucket)
        super().tearDownClass()
