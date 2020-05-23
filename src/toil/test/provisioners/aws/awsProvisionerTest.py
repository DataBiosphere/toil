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
import time
from abc import abstractmethod
from inspect import getsource
from textwrap import dedent
from uuid import uuid4

import pytest
from builtins import next
from builtins import range
from builtins import str

import subprocess
from toil.provisioners import clusterFactory
from toil.provisioners.aws.awsProvisioner import AWSProvisioner
from toil.version import exactPython
from toil.test import needs_aws_ec2, integrative, ToilTest, needs_appliance, timeLimit, slow

log = logging.getLogger(__name__)

class AWSProvisionerBenchTest(ToilTest):
    """
    Tests for the AWS provisioner that don't actually provision anything.
    """
    
    def testAMIFinding(self):
        for zone in ['us-west-2a', 'eu-central-1a', 'sa-east-1b']:
            provisioner = AWSProvisioner('fakename', zone, 10000, None)
            ami = provisioner._discoverAMI()
            # Make sure we got an AMI and it looks plausible
            assert(ami.startswith('ami-'))

@needs_aws_ec2
@needs_appliance
@slow
@integrative
class AbstractAWSAutoscaleTest(ToilTest):
    def __init__(self, methodName):
        super(AbstractAWSAutoscaleTest, self).__init__(methodName=methodName)
        self.keyName = os.environ.get('TOIL_AWS_KEYNAME', 'id_rsa')
        self.instanceTypes = ["m3.large"]
        self.clusterName = 'aws-provisioner-test-' + str(uuid4())
        self.numWorkers = ['2']
        self.numSamples = 2
        self.spotBid = 0.15

    def setUp(self):
        super(AbstractAWSAutoscaleTest, self).setUp()

    def tearDown(self):
        super(AbstractAWSAutoscaleTest, self).tearDown()
        subprocess.check_call(['toil', 'destroy-cluster', '-p=aws', self.clusterName])
        subprocess.check_call(['toil', 'clean', self.jobStore])

    def sshUtil(self, command):
        cmd = ['toil', 'ssh-cluster', '--insecure', '-p=aws', self.clusterName] + command
        log.debug("Running %s.", str(cmd))
        p = subprocess.Popen(cmd, stderr=-1, stdout=-1)
        o, e = p.communicate()
        log.debug('\n\nSTDOUT: ' + o.decode("utf-8"))
        log.debug('\n\nSTDERR: ' + e.decode("utf-8"))

    def rsyncUtil(self, src, dest):
        subprocess.check_call(['toil', 'rsync-cluster', '--insecure', '-p=aws', self.clusterName] + [src, dest])

    def createClusterUtil(self, args=None):
        args = [] if args is None else args
        subprocess.check_call(['toil', 'launch-cluster', '-p=aws', '-z=us-west-2a', f'--keyPairName={self.keyName}',
                               '--leaderNodeType=t2.medium', self.clusterName] + args)

    def getMatchingRoles(self):
        return list(self.cluster._ctx.local_roles())

    def launchCluster(self):
        self.createClusterUtil()

    def getRootVolID(self):
        instances = self.cluster._getNodesInCluster(nodeType=None, both=True)
        instances.sort(key=lambda x: x.launch_time)
        leader = instances[0]  # assume leader was launched first

        from boto.ec2.blockdevicemapping import BlockDeviceType
        rootBlockDevice = leader.block_device_mapping["/dev/xvda"]
        assert isinstance(rootBlockDevice, BlockDeviceType)
        return rootBlockDevice.volume_id

    @abstractmethod
    def _getScript(self):
        """Download the test script needed by the inheriting unit test class."""
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
        """Does the work of the testing.  Many features' tests are thrown in here in no particular order."""
        self.launchCluster()
        # get the leader so we know the IP address - we don't need to wait since create cluster
        # already insures the leader is running
        self.cluster = clusterFactory(provisioner='aws', clusterName=self.clusterName)
        self.leader = self.cluster.getLeader()
        self.sshUtil(['mkdir', '-p', self.scriptDir])  # hot deploy doesn't seem permitted to work in normal /tmp or /home

        assert len(self.getMatchingRoles()) == 1
        # --never-download prevents silent upgrades to pip, wheel and setuptools
        venv_command = ['virtualenv', '--system-site-packages', '--python', exactPython, '--never-download', '/home/venv']
        self.sshUtil(venv_command)

        upgrade_command = ['/home/venv/bin/pip', 'install', 'setuptools==28.7.1', 'pyyaml==3.12']
        self.sshUtil(upgrade_command)

        self._getScript()

        toilOptions = [self.jobStore,
                       '--batchSystem=mesos',
                       '--workDir=/var/lib/toil',
                       '--clean=always',
                       '--retryCount=2',
                       '--clusterStats=/tmp/t/',
                       '--logDebug',
                       '--logFile=/tmp/t/sort.log',
                       '--provisioner=aws']

        toilOptions.extend(['--nodeTypes=' + ",".join(self.instanceTypes),
                            '--maxNodes=' + ",".join(self.numWorkers)])
        if preemptableJobs:
            toilOptions.extend(['--defaultPreemptable'])

        self._runScript(toilOptions)

        assert len(self.getMatchingRoles()) == 1

        # check stats
        self.sshUtil(['/home/venv/bin/python', '-c', 'import json; import os; '
                      'json.load(open("/home/" + [f for f in os.listdir("/tmp/t/") if f.endswith(".json")].pop()))'])

        from boto.exception import EC2ResponseError
        volumeID = self.getRootVolID()
        self.cluster.destroyCluster()
        for attempt in range(6):
            # https://github.com/BD2KGenomics/toil/issues/1567
            # retry this for up to 1 minute until the volume disappears
            try:
                self.cluster._ctx.ec2.get_all_volumes(volume_ids=[volumeID])
                time.sleep(10)
            except EC2ResponseError as e:
                if e.status == 400 and 'InvalidVolume.NotFound' in e.code:
                    break
                else:
                    raise
        else:
            self.fail('Volume with ID %s was not cleaned up properly' % volumeID)

        assert len(self.getMatchingRoles()) == 0


@integrative
@pytest.mark.timeout(1800)
class AWSAutoscaleTest(AbstractAWSAutoscaleTest):
    def __init__(self, name):
        super(AWSAutoscaleTest, self).__init__(name)
        self.clusterName = 'provisioner-test-' + str(uuid4())
        self.requestedLeaderStorage = 80

    def setUp(self):
        super(AWSAutoscaleTest, self).setUp()
        self.jobStore = 'aws:%s:autoscale-%s' % (self.awsRegion(), uuid4())

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

    def launchCluster(self):
        # add arguments to test that we can specify leader storage
        self.createClusterUtil(args=['--leaderStorage', str(self.requestedLeaderStorage)])

    def getRootVolID(self):
        """
        Adds in test to check that EBS volume is build with adequate size.
        Otherwise is functionally equivalent to parent.
        :return: volumeID
        """
        volumeID = super(AWSAutoscaleTest, self).getRootVolID()
        rootVolume = self.cluster._ctx.ec2.get_all_volumes(volume_ids=[volumeID])[0]
        # test that the leader is given adequate storage
        self.assertGreaterEqual(rootVolume.size, self.requestedLeaderStorage)
        return volumeID

    @integrative
    @needs_aws_ec2
    def testAutoScale(self):
        self.instanceTypes = ["m3.large"]
        self.numWorkers = ['2']
        self._test()

    @integrative
    @needs_aws_ec2
    def testSpotAutoScale(self):
        self.instanceTypes = ["m3.large:%f" % self.spotBid]
        self.numWorkers = ['2']
        self._test(preemptableJobs=True)


@integrative
@pytest.mark.timeout(1200)
class AWSStaticAutoscaleTest(AWSAutoscaleTest):
    """Runs the tests on a statically provisioned cluster with autoscaling enabled."""
    def __init__(self, name):
        super(AWSStaticAutoscaleTest, self).__init__(name)
        self.requestedNodeStorage = 20

    def launchCluster(self):
        from toil.lib.ec2 import wait_instances_running
        from boto.ec2.blockdevicemapping import BlockDeviceType
        self.createClusterUtil(args=['--leaderStorage', str(self.requestedLeaderStorage),
                                     '--nodeTypes', ",".join(self.instanceTypes), '-w', ",".join(self.numWorkers), '--nodeStorage', str(self.requestedLeaderStorage)])

        self.cluster = clusterFactory(provisioner='aws', clusterName=self.clusterName)
        nodes = self.cluster._getNodesInCluster(both=True)
        nodes.sort(key=lambda x: x.launch_time)
        # assuming that leader is first
        workers = nodes[1:]
        # test that two worker nodes were created
        self.assertEqual(2, len(workers))
        # test that workers have expected storage size
        # just use the first worker
        worker = workers[0]
        worker = next(wait_instances_running(self.cluster._ctx.ec2, [worker]))
        rootBlockDevice = worker.block_device_mapping["/dev/xvda"]
        self.assertTrue(isinstance(rootBlockDevice, BlockDeviceType))
        rootVolume = self.cluster._ctx.ec2.get_all_volumes(volume_ids=[rootBlockDevice.volume_id])[0]
        self.assertGreaterEqual(rootVolume.size, self.requestedNodeStorage)

    def _runScript(self, toilOptions):
        runCommand = ['/home/venv/bin/python', '/home/sort.py', '--fileToSort=/home/sortFile']
        runCommand.extend(toilOptions)
        self.sshUtil(runCommand)


@integrative
@pytest.mark.timeout(1200)
class AWSAutoscaleTestMultipleNodeTypes(AbstractAWSAutoscaleTest):
    def __init__(self, name):
        super(AWSAutoscaleTestMultipleNodeTypes, self).__init__(name)
        self.clusterName = 'provisioner-test-' + str(uuid4())

    def setUp(self):
        super(AWSAutoscaleTestMultipleNodeTypes, self).setUp()
        self.jobStore = 'aws:%s:autoscale-%s' % (self.awsRegion(), uuid4())

    def _getScript(self):
        sseKeyFile = os.path.join(os.getcwd(), 'keyFile')
        with open(sseKeyFile, 'w') as f:
            f.write('01234567890123456789012345678901')
        self.rsyncUtil(os.path.join(self._projectRootPath(), 'src/toil/test/sort/sort.py'), ':/home/sort.py')
        self.rsyncUtil(sseKeyFile, ':/home/keyFile')
        os.unlink(sseKeyFile)

    def _runScript(self, toilOptions):
        #Set memory requirements so that sort jobs can be run
        # on small instances, but merge jobs must be run on large
        # instances
        runCommand = ['/home/venv/bin/python', '/home/sort.py', '--fileToSort=/home/s3am/bin/asadmin', '--sortMemory=0.6G', '--mergeMemory=3.0G']
        runCommand.extend(toilOptions)
        runCommand.append('--sseKey=/home/keyFile')
        self.sshUtil(runCommand)

    @integrative
    @needs_aws_ec2
    def testAutoScale(self):
        self.instanceTypes = ["t2.small", "m3.large"]
        self.numWorkers = ['2', '1']
        self._test()


@integrative
@pytest.mark.timeout(1200)
class AWSRestartTest(AbstractAWSAutoscaleTest):
    """This test insures autoscaling works on a restarted Toil run."""
    def __init__(self, name):
        super(AWSRestartTest, self).__init__(name)
        self.clusterName = 'restart-test-' + str(uuid4())

    def setUp(self):
        super(AWSRestartTest, self).setUp()
        self.instanceTypes = ['t2.small']
        self.numWorkers = ['1']
        self.scriptDir = '/tmp/t/'
        self.scriptName = self.scriptDir + 'restartScript.py'
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
        tempfile_path = '/tmp/temp-or-ary.txt'
        with open(tempfile_path, 'w') as f:
            # use appliance ssh method instead of sshutil so we can specify input param
            f.write(script)
        cluster = clusterFactory(provisioner='aws', clusterName=self.clusterName)
        leader = cluster.getLeader()
        self.sshUtil(['mkdir', '-p', self.scriptDir])  # hot deploy doesn't seem permitted to work in normal /tmp or /home
        leader.injectFile(tempfile_path, self.scriptName, 'toil_leader')
        if os.path.exists(tempfile_path):
            os.remove(tempfile_path)

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

    def testAutoScaledCluster(self):
        self._test()


@integrative
@pytest.mark.timeout(1200)
class PreemptableDeficitCompensationTest(AbstractAWSAutoscaleTest):
    def __init__(self, name):
        super(PreemptableDeficitCompensationTest, self).__init__(name)
        self.clusterName = 'deficit-test-' + str(uuid4())

    def setUp(self):
        super(PreemptableDeficitCompensationTest, self).setUp()
        self.instanceTypes = ['m3.large:0.01', "m3.large"]  # instance needs to be available on the spot market
        self.numWorkers = ['1', '1']
        self.jobStore = 'aws:%s:deficit-%s' % (self.awsRegion(), uuid4())

    def test(self):
        self._test(preemptableJobs=True)

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
        cluster = clusterFactory(provisioner='aws', clusterName=self.clusterName)
        leader = cluster.getLeader()
        leader.sshAppliance('tee', '/home/userScript.py', input=script)

    def _runScript(self, toilOptions):
        toilOptions.extend(['--preemptableCompensation=1.0'])
        command = ['/home/venv/bin/python', '/home/userScript.py']
        command.extend(toilOptions)
        self.sshUtil(command)

