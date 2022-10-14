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
import os
import uuid
from typing import Optional

import pytest

from toil.jobStores.aws.jobStore import AWSJobStore
from toil.lib.aws import iam
from toil.lib.aws.utils import create_s3_bucket
from toil.lib.ec2 import establish_boto3_session
from toil.test import ToilTest, needs_aws_s3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)




class IAMTest(ToilTest):
    """Check that given permissions and associated functions perform correctly"""

    def test_permissions_iam(self):
        granted_perms = {'*': {'Action': ['ec2:*', 'iam:*', 's3:*', 'sdb:*'], 'NotAction': []}}
        assert iam.policy_permissions_allow(granted_perms, iam.CLUSTER_LAUNCHING_PERMISSIONS) is True
        granted_perms = {'*': {'Action': [], 'NotAction': ['s3:*']}}
        assert iam.policy_permissions_allow(granted_perms, iam.CLUSTER_LAUNCHING_PERMISSIONS) is True

    def test_negative_permissions_iam(self):
        granted_perms = {'*': {'Action': ['ec2:*', 's3:*', 'sdb:*'], 'NotAction': []}}
        assert iam.policy_permissions_allow(granted_perms, iam.CLUSTER_LAUNCHING_PERMISSIONS) is False
        granted_perms = {'*': {'Action': [], 'NotAction': ['iam:*', 'ec2:*']}}
        assert iam.policy_permissions_allow(granted_perms, iam.CLUSTER_LAUNCHING_PERMISSIONS) is False

    def test_wildcard_handling(self):
        assert iam.permission_matches_any("iam:CreateRole", ['iam:Create**']) is True
        assert iam.permission_matches_any("iam:GetUser", ['iam:???????']) is True
        assert iam.permission_matches_any("iam:ListRoleTags", ['iam:*?*Tags']) is True
        assert iam.permission_matches_any("iam:*", ["*"]) is True
        assert iam.permission_matches_any("ec2:*", ['iam:*']) is False

