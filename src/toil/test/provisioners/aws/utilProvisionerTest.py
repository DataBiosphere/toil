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
import subprocess
import tempfile
import time
from abc import abstractmethod
from inspect import getsource
from textwrap import dedent
from uuid import uuid4

import boto.ec2
import pytest

from toil.lib.aws import zone_to_region
from toil.provisioners import cluster_factory
from toil.provisioners.aws import get_best_aws_zone
from toil.provisioners.aws.awsProvisioner import AWSProvisioner
from toil.test import (
    ToilTest,
    integrative,
    needs_aws_ec2,
    needs_fetchable_appliance,
    slow,
    timeLimit,
)
from toil.version import exactPython

#from toil.provisioners.aws.awsProvisioner import _CLUSTER_LAUNCHING_PERMISSIONS
log = logging.getLogger(__name__)


@pytest.mark.timeout(1800)
class AWSProvisionerBenchTest(ToilTest):
    """
    Tests for the AWS provisioner that don't actually provision anything.
    """
    def main(self):
        provisioner = AWSProvisioner('fakename', 'mesos', "us-west-2a", 10000, None, None)
        raise RuntimeError("Just crash this pls")
        provisioner.check_policy_warnings()



