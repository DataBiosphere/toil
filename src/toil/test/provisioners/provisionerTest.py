# Copyright (C) 2015 UCSC Computational Genomics Lab
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
import unittest

from unittest.mock import patch, Mock
from toil.provisioners import clusterFactory
import toil.provisioners
from toil.provisioners.abstractProvisioner import AbstractProvisioner

from toil.test import needs_azure, integrative, ToilTest, needs_appliance, timeLimit, slow

log = logging.getLogger(__name__)


class DummyProvisioner(AbstractProvisioner):

    def readClusterSettings(self):
        pass

    def launchCluster(self, leaderNodeType, leaderStorage, owner, **kwargs):
        pass

    def addNodes(self, nodeType, numNodes, preemptable, spotBid=None):
        pass

    def terminateNodes(self, nodes):
        pass

    def getLeader(self):
        pass

    def getProvisionedWorkers(self, nodeType, preemptable):
        pass

    def getNodeShape(self, nodeType=None, preemptable=False):
        pass

    def destroyCluster(self):
        pass


class ProvisionerTest(ToilTest):

    @patch('toil.provisioners.aws.awsProvisioner.AWSProvisioner', Mock())
    def test_clusteFactory_should_return_AWSProvisioner_instance_when_aws(self):
        actual = clusterFactory('aws')
        toil.provisioners.aws.awsProvisioner.AWSProvisioner.assert_called_once()

    @patch('toil.provisioners.gceProvisioner.GCEProvisioner', Mock())
    def test_clusteFactory_should_return_GCEProvisioner_instance_when_gce(self):
        actual = clusterFactory('gce')
        toil.provisioners.gceProvisioner.GCEProvisioner.assert_called_once()

    @patch('toil.provisioners.azure.azureProvisioner.AzureProvisioner', Mock())
    def test_clusteFactory_should_return_AzureProvisioner_instance_when_azure(self):
        actual = clusterFactory('azure')
        toil.provisioners.azure.azureProvisioner.AzureProvisioner.assert_called_once()

    @patch('toil.provisioners.aws.awsProvisioner.AWSProvisioner', Mock())
    def test_clusteFactory_should_return_AWSProvisioner_instance_when_awsFQN(self):
        actual = clusterFactory('toil.provisioners.aws.awsProvisioner.AWSProvisioner')
        toil.provisioners.aws.awsProvisioner.AWSProvisioner.assert_called_once()

    def test_clusteFactory_should_return_DummyProvisioner_when_FQN(self):
        actual = clusterFactory('toil.test.provisioners.provisionerTest.DummyProvisioner')
        self.assertTrue(isinstance(actual, toil.test.provisioners.provisionerTest.DummyProvisioner))


if __name__ == '__main__':
    unittest.main()
