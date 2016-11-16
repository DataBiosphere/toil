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

from uuid import uuid4
from toil.test import needs_aws, integrative, ToilTest, needs_appliance

log = logging.getLogger(__name__)


@needs_aws
@integrative
@needs_appliance
class AWSProvisionerTest(ToilTest):

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
        from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        AWSProvisioner.destroyCluster(self.clusterName)

    def _test(self, spotInstances=False):
        from toil.provisioners.aws.awsProvisioner import AWSProvisioner

        leader = AWSProvisioner.launchCluster(instanceType=self.instanceType, keyName=self.keyName,
                                              clusterName=self.clusterName)

        # --never-download prevents silent upgrades to pip, wheel and setuptools
        venv_command = 'virtualenv --system-site-packages --never-download /home/venv'
        AWSProvisioner._sshAppliance(leader.ip_address, command=venv_command)

        upgrade_command = '/home/venv/bin/pip install setuptools==28.7.1'
        AWSProvisioner._sshAppliance(leader.ip_address, command=upgrade_command)

        yaml_command = '/home/venv/bin/pip install pyyaml==3.12'
        AWSProvisioner._sshAppliance(leader.ip_address, command=yaml_command)

        # install toil scripts
        install_command = ('/home/venv/bin/pip install toil-scripts==%s' % self.toilScripts)
        AWSProvisioner._sshAppliance(leader.ip_address, command=install_command)

        # install curl
        install_command = 'sudo apt-get -y install curl'
        AWSProvisioner._sshAppliance(leader.ip_address, command=install_command)

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

        runCommand = 'bash -c \\"export PATH=/home/venv/bin/:$PATH;export TOIL_SCRIPTS_TEST_NUM_SAMPLES=%i; export TOIL_SCRIPTS_TEST_TOIL_OPTIONS=' + pipes.quote(toilOptions) + \
                     '; export TOIL_SCRIPTS_TEST_JOBSTORE=' + self.jobStore + \
                     '; /home/venv/bin/python -m unittest -v' + \
                     ' toil_scripts.rnaseq_cgl.test.test_rnaseq_cgl.RNASeqCGLTest.test_manifest\\"'

        runCommand %= self.numSamples

        AWSProvisioner._sshAppliance(leader.ip_address, runCommand)

    @integrative
    @needs_aws
    def testAutoScale(self):
        self._test(spotInstances=False)

    @integrative
    @needs_aws
    def testSpotAutoScale(self):
        self._test(spotInstances=True)
