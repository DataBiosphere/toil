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
import pytest

from toil.lib.aws.ami import (aws_marketplace_flatcar_ami_search,
                              get_flatcar_ami,
                              feed_flatcar_ami_release,
                              flatcar_release_feed_amis)
from toil.lib.aws.session import establish_boto3_session
from toil.test import ToilTest, needs_aws_ec2

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class FlatcarFeedTest(ToilTest):
    """Test accessing the FLatcar AMI release feed, independent of the AWS API"""
    
    def test_parse_archive_feed(self):
        """Make sure we can get a Flatcar release from the Internet Archive."""
        amis = list(flatcar_release_feed_amis('us-west-2', 'amd64', 'archive'))
        self.assertTrue(len(amis) > 0)
        for ami in amis:
            self.assertEqual(len(ami), len('ami-02b46c73fed689d1c'))
            self.assertTrue(ami.startswith('ami-'))
            
    def test_parse_beta_feed(self):
        """Make sure we can get a Flatcar release from the beta channel."""
        amis = list(flatcar_release_feed_amis('us-west-2', 'amd64', 'beta'))
        self.assertTrue(len(amis) > 0)
        for ami in amis:
            self.assertEqual(len(ami), len('ami-02b46c73fed689d1c'))
            self.assertTrue(ami.startswith('ami-'))
    
    # TODO: This will fail until https://github.com/flatcar/Flatcar/issues/962 is fixed
    @pytest.mark.xfail
    def test_parse_stable_feed(self):
        """Make sure we can get a Flatcar release from the stable channel."""
        amis = list(flatcar_release_feed_amis('us-west-2', 'amd64', 'stable'))
        self.assertTrue(len(amis) > 0)
        for ami in amis:
            self.assertEqual(len(ami), len('ami-02b46c73fed689d1c'))
            self.assertTrue(ami.startswith('ami-'))
            
    def test_bypass_stable_feed(self):
        """Make sure we can either get or safely not get a Flatcar release from the stable channel."""
        list(flatcar_release_feed_amis('us-west-2', 'amd64', 'stable'))
        # Ifd we get here we safely managed to iterate everything.

@needs_aws_ec2
class AMITest(ToilTest):
    @classmethod
    def setUpClass(cls):
        session = establish_boto3_session(region_name='us-west-2')
        cls.ec2_client = session.client('ec2')

    def test_fetch_flatcar(self):
        with self.subTest('Test flatcar AMI from user is prioritized.'):
            os.environ['TOIL_AWS_AMI'] = 'overridden'
            ami = get_flatcar_ami(self.ec2_client)
            self.assertEqual(ami, 'overridden')
            del os.environ['TOIL_AWS_AMI']

        with self.subTest('Test flatcar AMI returns an AMI-looking AMI.'):
            ami = get_flatcar_ami(self.ec2_client)
            self.assertEqual(len(ami), len('ami-02b46c73fed689d1c'))
            self.assertTrue(ami.startswith('ami-'))

        with self.subTest('Test feed_flatcar_ami_release() returns an AMI-looking AMI.'):
            ami = feed_flatcar_ami_release(self.ec2_client, source='archive')
            self.assertTrue(ami is None or len(ami) == len('ami-02b46c73fed689d1c'))
            self.assertTrue(ami is None or ami.startswith('ami-'))

        with self.subTest('Test aws_marketplace_flatcar_ami_search() returns an AMI-looking AMI.'):
            ami = aws_marketplace_flatcar_ami_search(self.ec2_client)
            self.assertEqual(len(ami), len('ami-02b46c73fed689d1c'))
            self.assertTrue(ami.startswith('ami-'))

    # TODO: This will fail until https://github.com/flatcar/Flatcar/issues/962 is fixed
    @pytest.mark.xfail
    def test_fetch_arm_flatcar(self):
        """Test flatcar AMI finder architecture parameter."""
        amis = set()
        for arch in ['amd64', 'arm64']:
            ami = get_flatcar_ami(self.ec2_client, architecture=arch)
            self.assertTrue(ami.startswith('ami-'))
            amis.add(ami)
        self.assertTrue(len(amis) == 2)
