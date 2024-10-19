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
import json
import logging
from uuid import uuid4

import boto3
from moto import mock_aws

from toil.lib.aws import iam
from toil.test import ToilTest

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class IAMTest(ToilTest):
    """Check that given permissions and associated functions perform correctly"""

    def test_permissions_iam(self):
        granted_perms = {
            "*": {"Action": ["ec2:*", "iam:*", "s3:*", "sdb:*"], "NotAction": []}
        }
        assert (
            iam.policy_permissions_allow(
                granted_perms, iam.CLUSTER_LAUNCHING_PERMISSIONS
            )
            is True
        )
        granted_perms = {"*": {"Action": [], "NotAction": ["s3:*"]}}
        assert (
            iam.policy_permissions_allow(
                granted_perms, iam.CLUSTER_LAUNCHING_PERMISSIONS
            )
            is True
        )

    def test_negative_permissions_iam(self):
        granted_perms = {"*": {"Action": ["ec2:*", "s3:*", "sdb:*"], "NotAction": []}}
        assert (
            iam.policy_permissions_allow(
                granted_perms, iam.CLUSTER_LAUNCHING_PERMISSIONS
            )
            is False
        )
        granted_perms = {"*": {"Action": [], "NotAction": ["iam:*", "ec2:*"]}}
        assert (
            iam.policy_permissions_allow(
                granted_perms, iam.CLUSTER_LAUNCHING_PERMISSIONS
            )
            is False
        )

    def test_wildcard_handling(self):
        assert iam.permission_matches_any("iam:CreateRole", ["iam:Create**"]) is True
        assert iam.permission_matches_any("iam:GetUser", ["iam:???????"]) is True
        assert iam.permission_matches_any("iam:ListRoleTags", ["iam:*?*Tags"]) is True
        assert iam.permission_matches_any("iam:*", ["*"]) is True
        assert iam.permission_matches_any("ec2:*", ["iam:*"]) is False

    @mock_aws
    def test_get_policy_permissions(self):
        mock_iam = boto3.client("iam")

        # username that moto pretends we have from client.get_user()
        user_name = "default_user"
        mock_iam.create_user(UserName=user_name)

        group_name = "default_group"
        mock_iam.create_group(GroupName=group_name)

        mock_iam.add_user_to_group(GroupName=group_name, UserName=user_name)

        policy_response = mock_iam.create_policy(
            PolicyName="test_iam_createrole",
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",  # represents version language
                    "Statement": [
                        {"Effect": "Allow", "Action": "iam:CreateRole", "Resource": "*"}
                    ],
                }
            ),
        )
        # attached user policy
        mock_iam.attach_user_policy(
            UserName=user_name, PolicyArn=policy_response["Policy"]["Arn"]
        )

        # inline user policy
        mock_iam.put_user_policy(
            UserName=user_name,
            PolicyName="test_iam_createinstanceprofile",
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",  # represents version language
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "iam:CreateInstanceProfile",
                            "Resource": "*",
                        }
                    ],
                }
            ),
        )

        # group policies
        policy_response = mock_iam.create_policy(
            PolicyName="test_iam_taginstanceprofile",
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",  # represents version language
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "iam:TagInstanceProfile",
                            "Resource": "*",
                        }
                    ],
                }
            ),
        )
        # attached group policy
        mock_iam.attach_group_policy(
            GroupName=group_name, PolicyArn=policy_response["Policy"]["Arn"]
        )

        # inline group policy
        mock_iam.put_group_policy(
            GroupName=group_name,
            PolicyName="test_iam_deleterole",
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",  # represents version language
                    "Statement": [
                        {"Effect": "Allow", "Action": "iam:DeleteRole", "Resource": "*"}
                    ],
                }
            ),
        )

        actions_collection = iam.get_policy_permissions("us-west-2")

        actions_set = set(actions_collection["*"]["Action"])
        notactions_set = set(actions_collection["*"]["NotAction"])

        expected_actions = {
            "iam:CreateRole",
            "iam:CreateInstanceProfile",
            "iam:TagInstanceProfile",
            "iam:DeleteRole",
        }
        assert actions_set == expected_actions
        assert notactions_set == set()

    def test_create_delete_iam_role(self):
        region = "us-west-2"
        role_name = f'test{str(uuid4()).replace("-", "")}'
        with self.subTest("Create role w/policies."):
            ec2_role_policy_document = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": ["ec2.amazonaws.com"]},
                            "Action": ["sts:AssumeRole"],
                        }
                    ],
                }
            )
            policy = dict(
                s3_deny=dict(
                    Version="2012-10-17",
                    Statement=[dict(Effect="Deny", Resource="*", Action="s3:*")],
                )
            )
            iam.create_iam_role(
                role_name=role_name,
                assume_role_policy_document=ec2_role_policy_document,
                policies=policy,
                region=region,
            )
        with self.subTest("Delete role w/policies."):
            iam.delete_iam_role(role_name=role_name, region=region)
