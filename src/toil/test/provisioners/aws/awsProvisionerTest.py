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
import os
import subprocess
from abc import abstractmethod
from inspect import getsource
from textwrap import dedent
from toil.provisioners.aws.awsProvisioner import AWSProvisioner

from uuid import uuid4


from toil.test import needs_aws, integrative, ToilTest, needs_appliance, timeLimit

log = logging.getLogger(__name__)


@needs_aws
@integrative
@needs_appliance
class AbstractAWSAutoscaleTest(ToilTest):

    def sshUtil(self, command):
        baseCommand = ['toil', 'ssh-cluster', '-p=aws', self.clusterName]
        callCommand = baseCommand + command
        subprocess.check_call(callCommand)

    def rsyncUtil(self, src, dest):
        baseCommand = ['toil', 'rsync-cluster', '-p=aws', self.clusterName]
        callCommand = baseCommand + [src, dest]
        subprocess.check_call(callCommand)

    def destroyClusterUtil(self):
        callCommand = ['toil', 'destroy-cluster', '-p=aws', self.clusterName]
        subprocess.check_call(callCommand)

    def createClusterUtil(self):
        callCommand = ['toil', 'launch-cluster', '-p=aws', '--keyPairName=%s' % self.keyName,
                       '--nodeType=%s' % self.instanceType, self.clusterName]
        subprocess.check_call(callCommand)

    def cleanJobStoreUtil(self):
        callCommand = ['toil', 'clean', self.jobStore]
        subprocess.check_call(callCommand)

    def __init__(self, methodName):
        super(AbstractAWSAutoscaleTest, self).__init__(methodName=methodName)
        self.instanceType = 'm3.large'
        self.keyName = 'jenkins@jenkins-master'
        self.clusterName = 'aws-provisioner-test-' + str(uuid4())
        self.numWorkers = 2
        self.numSamples = 2
        self.spotBid = '0.15'

    def setUp(self):
        super(AbstractAWSAutoscaleTest, self).setUp()

    def tearDown(self):
        super(AbstractAWSAutoscaleTest, self).tearDown()
        self.destroyClusterUtil()
        self.cleanJobStoreUtil()

    def getMatchingRoles(self, clusterName):
        from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        ctx = AWSProvisioner._buildContext(clusterName)
        roles = list(ctx.local_roles())
        return roles

    @abstractmethod
    def _getScript(self):
        """
        Download the test script needed by the inheriting unit test class.
        """
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

    def _test(self, spotInstances=False, fulfillableBid=True):
        if not fulfillableBid:
            self.spotBid = '0.01'
        from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        self.createClusterUtil()
        # get the leader so we know the IP address - we don't need to wait since create cluster
        # already insures the leader is running
        self.leader = AWSProvisioner._getLeader(wait=False, clusterName=self.clusterName)

        assert len(self.getMatchingRoles(self.clusterName)) == 1
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
                       '--mesosMaster=%s:5050' % self.leader.private_ip_address,
                       '--clean=always',
                       '--retryCount=2',
                       '--clusterStats=/home/',
                       '--logDebug',
                       '--provisioner=aws']

        if spotInstances:
            toilOptions.extend([
                '--preemptableNodeType=%s:%s' % (self.instanceType, self.spotBid),
                # The RNASeq pipeline does not specify a preemptability requirement so we
                # need to specify a default, otherwise jobs would never get scheduled.
                '--defaultPreemptable',
                '--maxPreemptableNodes=%s' % self.numWorkers])
        else:
            toilOptions.extend(['--nodeType=' + self.instanceType,
                                '--maxNodes=%s' % self.numWorkers])

        self._runScript(toilOptions)

        assert len(self.getMatchingRoles(self.clusterName)) == 1

        checkStatsCommand = ['/home/venv/bin/python', '-c',
                             'import json; import os; '
                             'json.load(open("/home/" + [f for f in os.listdir("/home/") '
                                                   'if f.endswith(".json")].pop()))'
                             ]

        self.sshUtil(checkStatsCommand)

        AWSProvisioner.destroyCluster(self.clusterName)
        assert len(self.getMatchingRoles(self.clusterName)) == 0


class AWSAutoscaleTest(AbstractAWSAutoscaleTest):

    def __init__(self, name):
        super(AWSAutoscaleTest, self).__init__(name)
        self.clusterName = 'provisioner-test-' + str(uuid4())

    def setUp(self):
        super(AWSAutoscaleTest, self).setUp()
        self.jobStore = 'aws:%s:autoscale-%s' % (self.awsRegion(), uuid4())

    def _getScript(self):
        sseKeyFile = os.path.join(os.getcwd(), 'keyFile')
        with open(sseKeyFile, 'w') as f:
            f.write('01234567890123456789012345678901')
        self.rsyncUtil(os.path.join(self._projectRootPath(), 'src/toil/test/sort/sort.py'), ':/home/sort.py')
        self.rsyncUtil(sseKeyFile, ':/home/keyFile')
        os.unlink(sseKeyFile)

    def _runScript(self, toilOptions):
        runCommand = ['/home/venv/bin/python', '/home/sort.py', '--fileToSort=/home/keyFile']
        runCommand.extend(toilOptions)
        runCommand.append('--sseKey=/home/keyFile')
        self.sshUtil(runCommand)

    @integrative
    @needs_aws
    def testAutoScale(self):
        self._test(spotInstances=False)

    @integrative
    @needs_aws
    def testSpotAutoScale(self):
        self._test(spotInstances=True)


class AWSRestartTest(AbstractAWSAutoscaleTest):
    """
    This test insures autoscaling works on a restarted Toil run
    """

    def __init__(self, name):
        super(AWSRestartTest, self).__init__(name)
        self.clusterName = 'restart-test-' + str(uuid4())

    def setUp(self):
        super(AWSRestartTest, self).setUp()
        self.instanceType = 't2.micro'
        self.scriptName = "/home/restartScript.py"
        self.jobStore = 'aws:%s:restart-%s' % (self.awsRegion(), uuid4())

    def _getScript(self):
        def restartScript():
            from toil.job import Job
            import argparse
            import os

            def f0(job):
                if 'FAIL' in os.environ:
                    raise RuntimeError('failed on purpose')

            if __name__ == '__main__':
                parser = argparse.ArgumentParser()
                Job.Runner.addToilOptions(parser)
                options = parser.parse_args()
                rootJob = Job.wrapJobFn(f0, cores=0.5, memory='50 M', disk='50 M')
                Job.Runner.startToil(rootJob, options)

        script = dedent('\n'.join(getsource(restartScript).split('\n')[1:]))
        # use appliance ssh method instead of sshutil so we can specify input param
        AWSProvisioner._sshAppliance(self.leader.ip_address, 'tee', self.scriptName, input=script)

    def _runScript(self, toilOptions):
        # clean = onSuccess
        disallowedOptions = ['--clean=always', '--retryCount=2']
        newOptions = [option for option in toilOptions if option not in disallowedOptions]
        try:
            # include a default memory - on restart the minimum memory requirement is the default, usually 2 GB
            command = ['/home/venv/bin/python', self.scriptName, '-e', 'FAIL=true', '--defaultMemory=50000000']
            command.extend(newOptions)
            self.sshUtil(command)
        except subprocess.CalledProcessError:
            pass
        else:
            self.fail('Command succeeded when we expected failure')
        with timeLimit(600):
            command = ['/home/venv/bin/python', self.scriptName, '--restart', '--defaultMemory=50000000']
            command.extend(toilOptions)
            self.sshUtil(command)

    @integrative
    def testAutoScaledCluster(self):
        self._test()


class PremptableDeficitCompensationTest(AbstractAWSAutoscaleTest):

    def __init__(self, name):
        super(PremptableDeficitCompensationTest, self).__init__(name)
        self.clusterName = 'deficit-test-' + str(uuid4())

    def setUp(self):
        super(PremptableDeficitCompensationTest, self).setUp()
        self.instanceType = 'm3.large' # instance needs to be available on the spot market
        self.jobStore = 'aws:%s:deficit-%s' % (self.awsRegion(), uuid4())

    def test(self):
        self._test(spotInstances=True, fulfillableBid=False)

    def _getScript(self):
        def userScript():
            from toil.job import Job
            from toil.common import Toil

            # Because this is the only job in the pipeline and because it is preemptable,
            # there will be no non-preemptable jobs. The non-preemptable scaler will therefore
            # not request any nodes initially. And since we made it impossible for the
            # preemptable scaler to allocate any nodes (using an abnormally low spot bid),
            # we will observe a deficit of preemptable nodes that the non-preemptable scaler will
            # compensate for by spinning up non-preemptable nodes instead.
            #
            def job(job, disk='10M', cores=1, memory='10M', preemptable=True):
                pass

            if __name__ == '__main__':
                options = Job.Runner.getDefaultArgumentParser().parse_args()
                with Toil(options) as toil:
                    if toil.config.restart:
                        toil.restart()
                    else:
                        toil.start(Job.wrapJobFn(job))

        script = dedent('\n'.join(getsource(userScript).split('\n')[1:]))
        # use appliance ssh method instead of sshutil so we can specify input param
        AWSProvisioner._sshAppliance(self.leader.ip_address, 'tee', '/home/userScript.py', input=script)

    def _runScript(self, toilOptions):
        toilOptions.extend([
            '--preemptableCompensation=1.0'])
        command = ['/home/venv/bin/python', '/home/userScript.py']
        command.extend(toilOptions)
        self.sshUtil(command)
