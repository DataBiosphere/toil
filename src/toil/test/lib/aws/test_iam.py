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
from toil.lib.aws.utils import create_s3_bucket
from toil.lib.ec2 import establish_boto3_session
from toil.test import ToilTest, needs_aws_s3
from toil.lib.aws import iam

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)



class IAMTest(ToilTest):
    """Confirm the workarounds for us-east-1."""

    from mypy_boto3_s3 import S3ServiceResource
    from mypy_boto3_s3.service_resource import Bucket

    s3_resource: Optional[S3ServiceResource]
    bucket: Optional[Bucket]
    '''
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        session = establish_boto3_session(region_name="us-east-1")
        cls.s3_resource = session.resource("s3", region_name="us-east-1")
        cls.bucket = None
    '''
    def test_permissions_iam(self):
        launch_tester = {'*': ['ec2:*', 'iam:*', 's3:*', 'sdb:*']}

        iam.check_policy_warnings(launch_tester)

    def test_negative_permissions_iam(self):
        launch_tester = {'*': ['ec2:*',  's3:*', 'sdb:*']}
        try:
            iam.check_policy_warnings(launch_tester)
        except RuntimeError:
            pass

    def test_allowed_actions(self):
        iam.get_allowed_actions()