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

from cgcloud.lib.context import Context

from toil.test import needs_aws, integrative, ToilTest, needs_appliance

log = logging.getLogger(__name__)


@needs_aws
@integrative
@needs_appliance
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
        self.numWorkers = 2
        self.numSamples = 2
        self.spotBid = '0.15'

    def setUp(self):
        super(AWSProvisionerTest, self).setUp()
        self.jobStore = 'aws:%s:toil-it-%s' % (self.awsRegion(), uuid4())

    def tearDown(self):
        self.destroyClusterUtil()

    def getMatchingRoles(self, clusterName):
        from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        ctx = AWSProvisioner._buildContext(clusterName)
        roles = list(ctx.local_roles())
        return roles

    def _test(self, spotInstances=False):
        from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        self.createClusterUtil()
        # get the leader so we know the IP address - we don't need to wait since create cluster
        # already insures the leader is running
        leader = AWSProvisioner._getLeader(wait=False, clusterName=self.clusterName)

        assert len(self.getMatchingRoles(self.clusterName)) == 1
        # --never-download prevents silent upgrades to pip, wheel and setuptools
        venv_command = ['virtualenv', '--system-site-packages', '--never-download',
                        '/home/venv']
        self.sshUtil(venv_command)

        upgrade_command = ['/home/venv/bin/pip', 'install', 'setuptools==28.7.1']
        self.sshUtil(upgrade_command)

        yaml_command = ['/home/venv/bin/pip', 'install', 'pyyaml==3.12']
        self.sshUtil(yaml_command)

        # install toil scripts
        install_command = ['/home/venv/bin/pip', 'install', 'toil-scripts==%s' % self.toilScripts]
        self.sshUtil(install_command)

        toilOptions = ['--batchSystem=mesos',
                       '--workDir=/var/lib/toil',
                       '--mesosMaster=%s:5050' % leader.private_ip_address,
                       '--clean=always',
                       '--retryCount=2',
                       '--clusterStats=/home/']

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

        # TOIL_AWS_NODE_DEBUG prevents the provisioner from killing nodes that
        # fail a status check. This allows for easier debugging of
        # https://github.com/BD2KGenomics/toil/issues/1141
        runCommand = ['bash', '-c',
                      'PATH=/home/venv/bin/:$PATH '
                      'TOIL_AWS_NODE_DEBUG=True '
                      'TOIL_SCRIPTS_TEST_NUM_SAMPLES='+str(self.numSamples)+
                      ' TOIL_SCRIPTS_TEST_TOIL_OPTIONS=' + pipes.quote(toilOptions) +
                      ' TOIL_SCRIPTS_TEST_JOBSTORE=' + self.jobStore +
                      ' /home/venv/bin/python -m unittest -v' +
                      ' toil_scripts.rnaseq_cgl.test.test_rnaseq_cgl.RNASeqCGLTest.test_manifest']

        self.sshUtil(runCommand)
        assert len(self.getMatchingRoles(self.clusterName)) == 1

        checkStatsCommand = ['/home/venv/bin/python', '-c',
                             'import json; import os; '
                             'json.load(open("/home/" + [f for f in os.listdir("/home/") '
                                                   'if f.endswith(".json")].pop()))'
                             ]

        self.sshUtil(checkStatsCommand)

        AWSProvisioner.destroyCluster(self.clusterName)
        assert len(self.getMatchingRoles(self.clusterName)) == 0

    @integrative
    @needs_aws
    def testAutoScale(self):
        self._test(spotInstances=False)

    @integrative
    @needs_aws
    def testSpotAutoScale(self):
        self._test(spotInstances=True)
