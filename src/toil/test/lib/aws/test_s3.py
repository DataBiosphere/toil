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

from typing import Optional, List

from toil.lib.aws.s3 import create_bucket, delete_bucket, get_s3_bucket_region, bucket_exists
from toil.lib.aws.credentials import resource
from toil.test import ToilTest, needs_aws_s3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


@needs_aws_s3
class S3Test(ToilTest):
    """Confirm the workarounds for us-east-1."""

    from mypy_boto3_s3 import S3ServiceResource

    s3_resource: Optional[S3ServiceResource]
    buckets: List[str]

    @classmethod
    def setUpClass(cls) -> None:
        super(S3Test, cls).setUpClass()
        cls.s3_resource = None
        cls.buckets = []

    def test_create_delete_bucket(self) -> None:
        """Test bucket creation and deletion for us-west-2 and us-east-1 (which is special)."""
        for region in ['us-west-2', 'us-east-1']:
            self.s3_resource = resource("s3", region_name=region)
            bucket = f"toil-s3test-{uuid.uuid4()}-{region}"
            self.buckets.append(bucket)

            with self.subTest(f'Test bucket creation in {region}.'):
                create_bucket(self.s3_resource, bucket)
                self.assertTrue(get_s3_bucket_region(self.s3_resource, bucket), region)

            with self.subTest(f'Test bucket deletion in {region}.'):
                delete_bucket(self.s3_resource, bucket)
                self.assertFalse(bucket_exists(self.s3_resource, bucket))

    @classmethod
    def tearDownClass(cls) -> None:
        for bucket in cls.buckets:
            delete_bucket(resource('s3'), bucket)
        super().tearDownClass()
