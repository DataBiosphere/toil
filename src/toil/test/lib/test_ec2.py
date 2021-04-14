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

from toil.lib.aws.credentials import establish_boto3_session
from toil.lib.aws.ami import get_flatcar_ami, official_flatcar_ami_release, aws_marketplace_flatcar_ami_search
from toil.test import ToilTest, needs_aws_ec2

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


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

        with self.subTest('Test official_flatcar_ami_release() returns an AMI-looking AMI.'):
            ami = official_flatcar_ami_release(self.ec2_client)
            self.assertTrue(ami is None or len(ami) == len('ami-02b46c73fed689d1c'))
            self.assertTrue(ami is None or ami.startswith('ami-'))

        with self.subTest('Test aws_marketplace_flatcar_ami_search() returns an AMI-looking AMI.'):
            ami = aws_marketplace_flatcar_ami_search(self.ec2_client)
            self.assertEqual(len(ami), len('ami-02b46c73fed689d1c'))
            self.assertTrue(ami.startswith('ami-'))
