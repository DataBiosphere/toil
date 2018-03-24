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

import pytest
from uuid import uuid4


from toil.test import needs_azure, integrative, ToilTest, needs_appliance, timeLimit, slow

log = logging.getLogger(__name__)


@needs_azure
@integrative
@needs_appliance
@slow
class AbstractAzureAutoscaleTest(ToilTest):

    def sshUtil(self, command):
        baseCommand = ['toil', 'ssh-cluster', '--insecure', '-p=azure', self.clusterName]
        callCommand = baseCommand + command
        subprocess.check_call(callCommand)

    def rsyncUtil(self, src, dest):
        baseCommand = ['toil', 'rsync-cluster', '--insecure', '-p=azure', self.clusterName]
        callCommand = baseCommand + [src, dest]
        subprocess.check_call(callCommand)

    def destroyClusterUtil(self):
        callCommand = ['toil', 'destroy-cluster', '-p=azure', self.clusterName]
        subprocess.check_call(callCommand)

    def createClusterUtil(self, args=None):
        if args is None:
            args = []
        callCommand = ['toil', 'launch-cluster', self.clusterName, '-p=azure', '--keyPairName=%s' % self.sshKeyName,
                       '--leaderNodeType=%s' % self.leaderInstanceType, '--zone=%s' % self.azureZone]
        if self.publicKeyFile:
            callCommand += ['--publicKeyFile=%s' % self.publicKeyFile]
        callCommand = callCommand + args if args else callCommand
        log.info("createClusterUtil: %s" % ' '.join(callCommand))
        subprocess.check_call(callCommand)

    def cleanJobStoreUtil(self):
        callCommand = ['toil', 'clean', self.jobStore]
        subprocess.check_call(callCommand)

    def __init__(self, methodName):
        super(AbstractAzureAutoscaleTest, self).__init__(methodName=methodName)
        self.keyName = os.getenv('TOIL_AZURE_KEYNAME')
        self.publicKeyFile = os.getenv('PUBLIC_KEY_FILE')
        self.sshKeyName = os.getenv('TOIL_SSH_KEYNAME')
        self.azureZone = os.getenv('TOIL_AZURE_ZONE')

        self.leaderInstanceType = 'Standard_A2_v2'
        self.instanceTypes = ["Standard_A4_v2"]
        self.numWorkers = ['2']
        self.numSamples = 2

    def setUp(self):
        super(AbstractAzureAutoscaleTest, self).setUp()

    def tearDown(self):
        super(AbstractAzureAutoscaleTest, self).tearDown()
        self.destroyClusterUtil()
        self.cleanJobStoreUtil()

    def launchCluster(self):
        self.createClusterUtil()

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

    def _test(self, preemptableJobs=False):
        """
        Does the work of the testing. Many features' test are thrown in here is no particular
        order
        """
        self.launchCluster()

        # TODO: Add a check of leader and node storage size if set.

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
                       '--provisioner=azure']

        toilOptions.extend(['--nodeTypes=' + ",".join(self.instanceTypes),
                            '--maxNodes=%s' % ",".join(self.numWorkers)])

        self._runScript(toilOptions)

        checkStatsCommand = ['/home/venv/bin/python', '-c',
                             'import json; import os; '
                             'json.load(open("/home/" + [f for f in os.listdir("/home/") '
                                                   'if f.endswith(".json")].pop()))'
                             ]

        self.sshUtil(checkStatsCommand)


        # TODO: Add a check to make sure everything is cleaned up.



@pytest.mark.timeout(3000)
class AzureAutoscaleTest(AbstractAzureAutoscaleTest):

    def __init__(self, name):
        super(AzureAutoscaleTest, self).__init__(name)
        self.clusterName = 'provisioner-test-' + bytes(uuid4())

    def setUp(self):
        super(AzureAutoscaleTest, self).setUp()
        self.jobStore = 'azure:%s:autoscale-%s' % (self.keyName, str(uuid4()).replace('-','')[:24])

    def _getScript(self):
        # TODO: Isn't this the key file?
        fileToSort = os.path.join(os.getcwd(), str(uuid4()))
        with open(fileToSort, 'w') as f:
            # Fixme: making this file larger causes the test to hang
            f.write('01234567890123456789012345678901')
        self.rsyncUtil(os.path.join(self._projectRootPath(), 'src/toil/test/sort/sort.py'), ':/home/sort.py')
        self.rsyncUtil(fileToSort, ':/home/sortFile')
        os.unlink(fileToSort)

    def _runScript(self, toilOptions):
        runCommand = ['/home/venv/bin/python', '/home/sort.py', '--fileToSort=/home/sortFile']
        #, '--sseKey=/home/sortFile']
        runCommand.extend(toilOptions)
        self.sshUtil(runCommand)

    def launchCluster(self):
        # add arguments to test that we can specify leader storage
        self.createClusterUtil()

    # TODO: aren't these checks inherited?
    @integrative
    @needs_azure
    def testAutoScale(self):
        self.instanceTypes = ["Standard_A4_v2"]
        self.numWorkers = ['2']
        self._test()


@pytest.mark.timeout(3000)
class AzureStaticAutoscaleTest(AzureAutoscaleTest):
    """
    Runs the tests on a statically provisioned cluster with autoscaling enabled.
    """
    def __init__(self, name):
        super(AzureStaticAutoscaleTest, self).__init__(name)

    def launchCluster(self):
        self.createClusterUtil(args=['--nodeTypes', ",".join(self.instanceTypes), '-w', ",".join(self.numWorkers)])

    def _runScript(self, toilOptions):
        runCommand = ['/home/venv/bin/python', '/home/sort.py', '--fileToSort=/home/sortFile']
        runCommand.extend(toilOptions)
        self.sshUtil(runCommand)

@pytest.mark.timeout(3000)
class AzureAutoscaleTestMultipleNodeTypes(AbstractAzureAutoscaleTest):

    def __init__(self, name):
        super(AzureAutoscaleTestMultipleNodeTypes, self).__init__(name)
        self.clusterName = 'provisioner-test-' + bytes(uuid4())

    def setUp(self):
        super(AzureAutoscaleTestMultipleNodeTypes, self).setUp()
        self.jobStore = 'azure:%s:autoscale-%s' % (self.keyName, str(uuid4()).replace('-','')[:24])

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
        runCommand = ['/home/venv/bin/python', '/home/sort.py', '--fileToSort=/home/s3am/bin/asadmin', '--sortMemory=1.0G', '--mergeMemory=3.0G']
        runCommand.extend(toilOptions)
        #runCommand.append('--sseKey=/home/keyFile')
        self.sshUtil(runCommand)

    @integrative
    @needs_azure
    def testAutoScale(self):
        self.instanceTypes = ["Standard_A4_v2", "Standard_D3_v2"]
        self.numWorkers = ['2','1']
        self._test()

@pytest.mark.timeout(3300)
class AzureRestartTest(AbstractAzureAutoscaleTest):
    """
    This test insures autoscaling works on a restarted Toil run
    """

    def __init__(self, name):
        super(AzureRestartTest, self).__init__(name)
        self.clusterName = 'restart-test-' + bytes(uuid4())

    def setUp(self):
        super(AzureRestartTest, self).setUp()
        self.instanceTypes = ['Standard_A4_v2']
        self.numWorkers = ['1']
        self.scriptName = "/home/restartScript.py"
        self.jobStore = 'azure:%s:restart-%s' % (self.keyName, str(uuid4()).replace('-','')[:24])

    def _getScript(self):
        self.rsyncUtil(os.path.join(self._projectRootPath(), 'src/toil/test/provisioners/restartScript.py'),
                        ':'+self.scriptName)


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
        with timeLimit(1200):
            command = ['/home/venv/bin/python', self.scriptName, '--restart', '--defaultMemory=50000000']
            command.extend(toilOptions)
            self.sshUtil(command)

    @integrative
    def testAutoScaledCluster(self):
        self._test()
