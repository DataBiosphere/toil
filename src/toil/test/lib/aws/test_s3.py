# Copyright (C) 2021 Michael R. Crusoe
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
import uuid
from typing import Optional

from toil.lib.aws.s3 import create_bucket, delete_bucket
from toil.lib.ec2 import establish_boto3_session
from toil.lib.aws.s3 import get_s3_bucket_region
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
        super(S3Test, cls).setUpClass()
        session = establish_boto3_session(region_name="us-east-1")
        cls.s3_resource = session.resource("s3", region_name="us-east-1")
        cls.bucket = None

    def test_create_bucket(self) -> None:
        """Test bucket creation for us-east-1."""
        bucket_name = f"toil-s3test-{uuid.uuid4()}"
        assert self.s3_resource
        S3Test.bucket = create_bucket(self.s3_resource, bucket_name)
        S3Test.bucket.wait_until_exists()
        self.assertTrue(get_s3_bucket_region(self.s3_resource, bucket_name), "us-east-1")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.bucket:
            delete_bucket(cls.s3_resource, cls.bucket.name)
        super().tearDownClass()
