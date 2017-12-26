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
from builtins import next
from builtins import str
from builtins import range
import logging
import os
import subprocess
from abc import abstractmethod
from inspect import getsource
from textwrap import dedent

import time

import pytest
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.exception import EC2ResponseError
from toil.lib.ec2 import wait_instances_running


from toil.provisioners.aws.awsProvisioner import AWSProvisioner

from uuid import uuid4


from toil.test import needs_aws, needs_google, integrative, ToilTest, needs_appliance, timeLimit, slow

log = logging.getLogger(__name__)


@integrative
@needs_appliance
@slow
@pytest.mark.timeout(1200)
class AbstractProvisionerTest(ToilTest):

    def __init__(self, methodName):
        super(AbstractProvisionerTest, self).__init__(methodName=methodName)
        self.numWorkers = ['2']
        self.numSamples = 2
        self.spotBid = 0.15
        self.zone = None
        self.launchArgs = []
        self.static = False

    def sshUtil(self, command):
        baseCommand = ['toil', 'ssh-cluster', '--insecure', '-p=%s' % self.provisionerType]
        if self.zone is not None:
            baseCommand += ['--zone=%s' % self.zone]
        callCommand = baseCommand + [self.clusterName] + command
        subprocess.check_call(callCommand)

    def rsyncUtil(self, src, dest):
        baseCommand = ['toil', 'rsync-cluster', '--insecure', '-p=%s' % self.provisionerType]
        if self.zone is not None:
            baseCommand += ['--zone=%s' % self.zone]
        callCommand = baseCommand + [self.clusterName] + [src, dest]
        subprocess.check_call(callCommand)

    def destroyClusterUtil(self):
        callCommand = ['toil', 'destroy-cluster', '-p=%s' % self.provisionerType]
        if self.zone is not None:
            callCommand += ['--zone=%s' % self.zone]
        callCommand += [self.clusterName]
        subprocess.check_call(callCommand)

    def createClusterUtil(self):
        callCommand = ['toil', 'launch-cluster', '-p=%s' % self.provisionerType, '--keyPairName=%s' % self.keyName,
                       '--leaderNodeType=%s' % self.leaderInstanceType]
        if self.zone is not None:
            callCommand += ['--zone=%s' % self.zone]
        callCommand += self.launchArgs
        callCommand += [self.clusterName]
        subprocess.check_call(callCommand)

    def cleanJobStoreUtil(self):
        callCommand = ['toil', 'clean', self.jobStore]
        subprocess.check_call(callCommand)

    def setUp(self):
        super(AbstractProvisionerTest, self).setUp()

    def tearDown(self):
        super(AbstractProvisionerTest, self).tearDown()
        self.destroyClusterUtil()
        self.cleanJobStoreUtil()

    @abstractmethod
    def _getScript(self):
        """
        Download the test script needed by the inheriting unit test class.
        """
        raise NotImplementedError()

    @abstractmethod
    def preTest(self):
        raise NotImplementedError()

    @abstractmethod
    def postTest(self):
        raise NotImplementedError()

    @abstractmethod
    def _runScript(self, toilOptions):
        """
        Modify the provided Toil options to suit the test Toil script, then run the script with
        those arguments.

        :param toilOptions: List of Toil command line arguments. This list may need to be
               modified to suit the test script's requirements.
        """
        raise NotImplementedError()

    def _test(self, preemptableJobs=False):
        """
        Does the work of the testing. Many features' test are thrown in here is no particular
        order
        """
        self.createClusterUtil()

        self.preTest()

        # --never-download prevents silent upgrades to pip, wheel and setuptools
        venv_command = ['virtualenv', '--system-site-packages', '--never-download',
                        '/home/venv']
        self.sshUtil(venv_command)

        upgrade_command = ['/home/venv/bin/pip', 'install', 'setuptools==28.7.1']
        self.sshUtil(upgrade_command)

        yaml_command = ['/home/venv/bin/pip', 'install', 'pyyaml==3.12']
        self.sshUtil(yaml_command)

        self._getScript()

        toilOptions = [self.jobStore,
                       '--batchSystem=mesos',
                       '--workDir=/var/lib/toil',
                       '--clean=always',
                       '--retryCount=2',
                       '--clusterStats=/home/',
                       '--logDebug',
                       '--logFile=/home/sort.log',
                       '--provisioner=%s' % self.provisionerType]

        if not self.static:
            toilOptions.extend(['--nodeTypes=' + ",".join(self.instanceTypes),
                                '--maxNodes=%s' % ",".join(self.numWorkers)])
        if preemptableJobs:
            toilOptions.extend(['--defaultPreemptable'])

        self._runScript(toilOptions)

        # TODO: replace this with an rysync to get json file and do the test locally?
        # This would confirm that is running and the test successful.
        assert self.isRunning()
        checkStatsCommand = ['/home/venv/bin/python', '-c',
                             'import json; import os; '
                             'json.load(open("/home/" + [f for f in os.listdir("/home/") '
                                                   'if f.endswith(".json")].pop()))'
                             ]
        self.sshUtil(checkStatsCommand)



        self.postTest()

        # TODO - turn on
        # should this be a check if everything is cleaned up
        #assert not self.isRunning()


@pytest.mark.timeout(1200)
@integrative
class AutoscaleTest(AbstractProvisionerTest):

    def __init__(self, name):
        super(AutoscaleTest, self).__init__(name)
        self.clusterName = 'provisioner-test-' + str(uuid4())
        self.requestedLeaderStorage = 80
        self.launchArgs += ['--leaderStorage=%s' % self.requestedLeaderStorage]

    def _getScript(self):
        fileToSort = os.path.join(os.getcwd(), str(uuid4()))
        with open(fileToSort, 'w') as f:
            # Fixme: making this file larger causes the test to hang
            f.write('01234567890123456789012345678901')
        self.rsyncUtil(os.path.join(self._projectRootPath(), 'src/toil/test/sort/sort.py'), ':/home/sort.py')
        self.rsyncUtil(fileToSort, ':/home/sortFile')
        os.unlink(fileToSort)

    def _runScript(self, toilOptions):
        runCommand = ['/home/venv/bin/python', '/home/sort.py', '--fileToSort=/home/sortFile', '--sseKey=/home/sortFile']
        runCommand.extend(toilOptions)
        self.sshUtil(runCommand)

@needs_aws
class AWSAutoscaleTest(AutoscaleTest):

    def __init__(self, name):
        super(AWSAutoscaleTest, self).__init__(name)
        self.jobStore = 'aws:%s:autoscale-%s' % (self.awsRegion(), uuid4())
        self.keyName = os.getenv('TOIL_AWS_KEYNAME')
        self.leaderInstanceType = 't2.medium'
        self.instanceTypes = ["m3.large"]
        self.provisionerType = 'aws'
        self.clusterName = 'aws-provisioner-test-' + str(uuid4())

    def getRootVolID(self):
        """        :return: volumeID
        """
        from boto.ec2.blockdevicemapping import BlockDeviceType
        rootBlockDevice = self.leader.block_device_mapping["/dev/xvda"]
        assert isinstance(rootBlockDevice, BlockDeviceType)
        volumeID = rootBlockDevice.volume_id

        # extra test to check that EBS volume is build with adequate size.
        rootVolume = self.ctx.ec2.get_all_volumes(volume_ids=[volumeID])[0]
        # test that the leader is given adequate storage
        self.assertGreaterEqual(rootVolume.size, self.requestedLeaderStorage)
        return volumeID

    def preTest(self):
        from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        # get the leader so we know the IP address - we don't need to wait since create cluster
        # already insures the leader is running
        self.leader = AWSProvisioner._getLeader(wait=False, clusterName=self.clusterName)
        self.ctx = AWSProvisioner._buildContext(self.clusterName)

        if self.static:
            nodes = AWSProvisioner._getNodesInCluster(self.ctx, self.clusterName, both=True)
            nodes.sort(key=lambda x: x.launch_time)
            # assuming that leader is first
            workers = nodes[1:]
            # test that two worker nodes were created
            self.assertEqual(2, len(workers))
            # test that workers have expected storage size
            # just use the first worker
            worker = workers[0]
            worker = next(wait_instances_running(self.ctx.ec2, [worker]))
            rootBlockDevice = worker.block_device_mapping["/dev/xvda"]
            self.assertTrue(isinstance(rootBlockDevice, BlockDeviceType))
            rootVolume = self.ctx.ec2.get_all_volumes(volume_ids=[rootBlockDevice.volume_id])[0]
            self.assertGreaterEqual(rootVolume.size, self.requestedNodeStorage)

    def postTest(self):
        from boto.exception import EC2ResponseError
        volumeID = self.getRootVolID()
        AWSProvisioner.destroyCluster(self.clusterName)
        self.leader.update()
        for attempt in range(6):
            # https://github.com/BD2KGenomics/toil/issues/1567
            # retry this for up to 1 minute until the volume disappears
            try:
                self.ctx.ec2.get_all_volumes(volume_ids=[volumeID])
                time.sleep(10)
            except EC2ResponseError as e:
                if e.status == 400 and 'InvalidVolume.NotFound' in e.code:
                    break
                else:
                    raise
        else:
            self.fail('Volume with ID %s was not cleaned up properly' % volumeID)

    def isRunning(self):
        roles = list(self.ctx.local_roles())
        assert len(roles) < 2
        return len(roles) == 1

    def testAutoScale(self):
        self._test()

    def testSpotAutoScale(self):
        self.instanceTypes = ["m3.large:%f" % self.spotBid]
        self._test(preemptableJobs=True)



class AWSStaticAutoscaleTest(AWSAutoscaleTest):
    """
    Runs the tests on a statically provisioned cluster with autoscaling enabled.
    """
    def __init__(self, name):
        super(AWSStaticAutoscaleTest, self).__init__(name)
        self.requestedNodeStorage = 20
        self.static = True
        self.launchArgs += ['--nodeTypes', ",".join(self.instanceTypes), '-w', ",".join(self.numWorkers),
                            '--nodeStorage', str(self.requestedLeaderStorage)]


# TODO: require google service account, and keyname
@needs_google
class GoogleAutoscaleTest(AutoscaleTest):

    def __init__(self, name):
        super(GoogleAutoscaleTest, self).__init__(name)
        self.jobStore = 'aws:%s:autoscale-%s' % (self.awsRegion(), uuid4())
        self.leaderInstanceType = 'n1-standard-1'
        self.instanceTypes = ["n1-standard-2"]
        self.provisionerType = 'gce'


        #TODO: set these (no need for zone with SA)
        self.keyName = os.getenv('TOIL_KEYNAME')
        self.boto = '/Users/ejacox/.boto'
        self.zone = 'us-west1-a'
        # TODO: this needs to be unique (e.g. add a uuid)
        self.clusterName = 'gce-provisioner-test'
        self.launchArgs += ['--boto=%s' % self.boto]

    def preTest(self):
        pass

    def postTest(self):
        #TODO: check storage size? - or move to test body
        pass

    # TODO: add this
    def isRunning(self):
        return True

    def testAutoScale(self):
        self._test()

    # TODO: pre-emptable test


class GoogleStaticAutoscaleTest(GoogleAutoscaleTest):
    """
    Runs the tests on a statically provisioned cluster with autoscaling enabled.
    """
    def __init__(self, name):
        super(GoogleStaticAutoscaleTest, self).__init__(name)
        self.requestedNodeStorage = 20
        self.static = True
        self.launchArgs += ['--nodeTypes', ",".join(self.instanceTypes), '-w', ",".join(self.numWorkers),
                            '--nodeStorage', str(self.requestedLeaderStorage)]
