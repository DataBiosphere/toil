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
import pipes
import subprocess

from uuid import uuid4
from toil.test import needs_aws, integrative, ToilTest


log = logging.getLogger(__name__)


@needs_aws
@integrative
class AWSProvisionerTest(ToilTest):


    def sshUtil(self, command):
        baseCommand = ['toil', 'ssh-cluster', '-p=aws', self.clusterName]
        callCommand = baseCommand + command
        subprocess.check_call(callCommand)

    def destroyClusterUtil(self):
        callCommand = ['toil', 'destroy-cluster', '-p=aws', self.clusterName]
        subprocess.check_call(callCommand)

    def createClusterUtil(self):
        callCommand = ['toil', 'launch-cluster', '-p=aws', '--keyPairName=%s' % self.keyName,
                       '--nodeType=%s' % self.instanceType, self.clusterName]
        subprocess.check_call(callCommand)

    def __init__(self, methodName='AWSprovisioner'):
        super(AWSProvisionerTest, self).__init__(methodName=methodName)
        self.instanceType = 'm3.large'
        self.keyName = 'jenkins@jenkins-master'
        self.clusterName = 'aws-provisioner-test-' + str(uuid4())
        self.toilScripts = '2.1.0a1.dev654'#'2.1.0a1.dev455'
        self.numWorkers = 10
        self.numSamples = 10
        self.spotBid = '0.15'

    def setUp(self):
        super(AWSProvisionerTest, self).setUp()
        self.jobStore = 'aws:%s:toil-it-%s' % (self.awsRegion(), uuid4())

    def tearDown(self):
        self.destroyClusterUtil()

    def _test(self, spotInstances=False):
        from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        self.createClusterUtil()
        # get the leader so we know the IP address - we don't need to wait since create cluster
        # already insures the leader is running
        leader = AWSProvisioner._getLeader(wait=False, clusterName=self.clusterName)

        # --never-download prevents silent upgrades to pip, wheel and setuptools
        self.sshUtil(['virtualenv', '--system-site-packages', '--never-download', '/home/venv'])
        self.sshUtil(['/home/venv/bin/pip', 'install', 'setuptools==28.7.1'])
        self.sshUtil(['/home/venv/bin/pip', 'install', 'pyyaml==3.12'])
        # install toil scripts
        self.sshUtil(['/home/venv/bin/pip', 'install', 'toil-scripts==%s' % self.toilScripts])
        # install curl
        self.sshUtil(['sudo', 'apt-get', '-y', 'install', 'curl'])

        toilOptions = ['--batchSystem=mesos',
                       '--workDir=/var/lib/toil',
                       '--mesosMaster=%s:5050' % leader.private_ip_address,
                       '--clean=always',
                       '--retryCount=0']

        toilOptions.extend(['--provisioner=aws',
                            '--nodeType=' + self.instanceType,
                            '--maxNodes=%s' % self.numWorkers,
                            '--logDebug'])
        if spotInstances:
            toilOptions.extend([
                '--preemptableNodeType=%s:%s' % (self.instanceType, self.spotBid),
                # The RNASeq pipeline does not specify a preemptability requirement so we
                # need to specify a default, otherwise jobs would never get scheduled.
                '--defaultPreemptable',
                '--maxPreemptableNodes=%s' % self.numWorkers])

        toilOptions = ' '.join(toilOptions)

        runCommand = ['bash', '-c',
                      'PATH=/home/venv/bin/:$PATH '
                      'TOIL_SCRIPTS_TEST_NUM_SAMPLES='+str(self.numSamples)+
                      ' TOIL_SCRIPTS_TEST_TOIL_OPTIONS=' + pipes.quote(toilOptions) +
                      ' TOIL_SCRIPTS_TEST_JOBSTORE=' + self.jobStore +
                      ' /home/venv/bin/python -m unittest -v' +
                      ' toil_scripts.rnaseq_cgl.test.test_rnaseq_cgl.RNASeqCGLTest.test_manifest']

        self.sshUtil(runCommand)

    @integrative
    @needs_aws
    def testAutoScale(self):
        self._test(spotInstances=False)

    @integrative
    @needs_aws
    def testSpotAutoScale(self):
        self._test(spotInstances=True)
