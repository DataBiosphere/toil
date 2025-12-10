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

from unittest import mock

from toil.lib.aws.ami import (
    aws_marketplace_flatcar_ami_search,
    feed_flatcar_ami_release,
    flatcar_release_feed_ami,
    get_flatcar_ami,
    ReleaseFeedUnavailableError
)
from toil.test import ToilTest, needs_aws_ec2, needs_online

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


@needs_online
class FlatcarFeedTest(ToilTest):
    """Test accessing the Flatcar AMI release feed, independent of the AWS API"""

    # Note that we need to support getting no AMI back, because sometimes the
    # Flatcar feeds are just down, and we can't fail CI at those times.

    def test_parse_archive_feed(self):
        """Make sure we can get a Flatcar release from the Internet Archive."""
        ami = flatcar_release_feed_ami("us-west-2", "amd64", "archive")
        if ami is not None:
            self.assertEqual(len(ami), len("ami-02b46c73fed689d1c"))
            self.assertTrue(ami.startswith("ami-"))

    def test_parse_beta_feed(self):
        """Make sure we can get a Flatcar release from the beta channel."""
        ami = flatcar_release_feed_ami("us-west-2", "amd64", "beta")
        if ami is not None:
            self.assertEqual(len(ami), len("ami-02b46c73fed689d1c"))
            self.assertTrue(ami.startswith("ami-"))

    def test_parse_stable_feed(self):
        """Make sure we can get a Flatcar release from the stable channel."""
        ami = flatcar_release_feed_ami("us-west-2", "amd64", "stable")
        if ami is not None:
            self.assertEqual(len(ami), len("ami-02b46c73fed689d1c"))
            self.assertTrue(ami.startswith("ami-"))


@needs_aws_ec2
class AMITest(ToilTest):
    @classmethod
    def setUpClass(cls):
        from toil.lib.aws.session import establish_boto3_session

        session = establish_boto3_session(region_name="us-west-2")
        cls.ec2_client = session.client("ec2")

    def test_fetch_flatcar(self):
        with self.subTest("Test flatcar AMI from user is prioritized."):
            with mock.patch.dict(os.environ, {"TOIL_AWS_AMI": "overridden"}):
                ami = get_flatcar_ami(self.ec2_client)
                self.assertEqual(ami, "overridden")

        with self.subTest("Test flatcar AMI returns an AMI-looking AMI."):
            try:
                ami = get_flatcar_ami(self.ec2_client)
                self.assertEqual(len(ami), len("ami-02b46c73fed689d1c"))
                self.assertTrue(ami.startswith("ami-"))
            except ReleaseFeedUnavailableError:
                # Ignore any remote systems being down.
                pass

        with self.subTest(
            "Test feed_flatcar_ami_release() returns an AMI-looking AMI."
        ):
            ami = feed_flatcar_ami_release(self.ec2_client, source="archive")
            self.assertTrue(ami is None or len(ami) == len("ami-02b46c73fed689d1c"))
            self.assertTrue(ami is None or ami.startswith("ami-"))

        with self.subTest(
            "Test aws_marketplace_flatcar_ami_search() returns an AMI-looking AMI."
        ):
            ami = aws_marketplace_flatcar_ami_search(self.ec2_client)
            self.assertTrue(ami is None or len(ami), len("ami-02b46c73fed689d1c"))
            self.assertTrue(ami is None or ami.startswith("ami-"))

    def test_fetch_arm_flatcar(self):
        """Test flatcar AMI finder architecture parameter."""
        try:
            ami = get_flatcar_ami(self.ec2_client, architecture="arm64")
            self.assertTrue(ami.startswith("ami-"))
        except ReleaseFeedUnavailableError:
            # Ignore any remote systems being down.
            pass
