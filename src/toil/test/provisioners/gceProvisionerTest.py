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
from abc import abstractmethod
from uuid import uuid4

import pytest

from toil.test import (
    ToilTest,
    get_data,
    integrative,
    needs_fetchable_appliance,
    needs_google_project,
    needs_google_storage,
    slow,
    timeLimit,
)
from toil.version import exactPython

log = logging.getLogger(__name__)


@needs_google_project
@needs_google_storage
@integrative
@needs_fetchable_appliance
@slow
class AbstractGCEAutoscaleTest(ToilTest):
    projectID = os.getenv("TOIL_GOOGLE_PROJECTID")

    def sshUtil(self, command):
        baseCommand = ["toil", "ssh-cluster", "--insecure", "-p=gce", self.clusterName]
        callCommand = baseCommand + command
        subprocess.check_call(callCommand)

    def rsyncUtil(self, src, dest):
        baseCommand = [
            "toil",
            "rsync-cluster",
            "--insecure",
            "-p=gce",
            self.clusterName,
        ]
        callCommand = baseCommand + [src, dest]
        subprocess.check_call(callCommand)

    def destroyClusterUtil(self):
        callCommand = ["toil", "destroy-cluster", "-p=gce", self.clusterName]
        subprocess.check_call(callCommand)

    def createClusterUtil(self, args=None):
        if args is None:
            args = []
        callCommand = [
            "toil",
            "launch-cluster",
            self.clusterName,
            "-p=gce",
            "--keyPairName=%s" % self.keyName,
            "--leaderNodeType=%s" % self.leaderInstanceType,
            "--zone=%s" % self.googleZone,
        ]
        if self.botoDir is not None:
            callCommand += ["--boto=%s" % self.botoDir]
        callCommand = callCommand + args if args else callCommand
        log.info("createClusterUtil: %s" % "".join(callCommand))
        subprocess.check_call(callCommand)

    def cleanJobStoreUtil(self):
        callCommand = ["toil", "clean", self.jobStore]
        subprocess.check_call(callCommand)

    def __init__(self, methodName):
        super().__init__(methodName=methodName)
        # TODO: add TOIL_GOOGLE_KEYNAME to needs_google_project or ssh with SA account
        self.keyName = os.getenv("TOIL_GOOGLE_KEYNAME")
        # TODO: remove this when switching to google jobstore
        self.botoDir = os.getenv("TOIL_BOTO_DIR")
        # TODO: get this from SA account or add an environment variable
        self.googleZone = "us-west1-a"

        self.leaderInstanceType = "n1-standard-1"
        self.instanceTypes = ["n1-standard-2"]
        self.numWorkers = ["2"]
        self.numSamples = 2
        self.spotBid = 0.15

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.destroyClusterUtil()
        self.cleanJobStoreUtil()

    # def getMatchingRoles(self, clusterName):
    #    ctx = AWSProvisioner._buildContext(clusterName)
    #    roles = list(ctx.local_roles())
    #    return roles

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

    def _test(self, preemptibleJobs=False):
        """
        Does the work of the testing. Many features' test are thrown in here is no particular
        order
        """
        self.launchCluster()

        # TODO: What is the point of this test?
        # assert len(self.getMatchingRoles(self.clusterName)) == 1

        # TODO: Add a check of leader and node storage size if set.

        # --never-download prevents silent upgrades to pip, wheel and setuptools
        venv_command = [
            "virtualenv",
            "--system-site-packages",
            "--never-download",
            "--python",
            exactPython,
            "/home/venv",
        ]
        self.sshUtil(venv_command)

        self._getScript()

        toilOptions = [
            self.jobStore,
            "--batchSystem=mesos",
            "--workDir=/var/lib/toil",
            "--clean=always",
            "--retryCount=2",
            "--clusterStats=/home/",
            "--logDebug",
            "--logFile=/home/sort.log",
            "--provisioner=gce",
        ]

        toilOptions.extend(
            [
                "--nodeTypes=" + ",".join(self.instanceTypes),
                "--maxNodes=%s" % ",".join(self.numWorkers),
            ]
        )
        if preemptibleJobs:
            toilOptions.extend(["--defaultPreemptible"])

        self._runScript(toilOptions)

        # TODO: Does this just check if it is still running?
        # assert len(self.getMatchingRoles(self.clusterName)) == 1

        checkStatsCommand = [
            "/home/venv/bin/python",
            "-c",
            "import json; import os; "
            'json.load(open("/home/" + [f for f in os.listdir("/home/") '
            'if f.endswith(".json")].pop()))',
        ]

        self.sshUtil(checkStatsCommand)

        # TODO: Add a check to make sure everything is cleaned up.


@pytest.mark.timeout(1600)
class GCEAutoscaleTest(AbstractGCEAutoscaleTest):

    def __init__(self, name):
        super().__init__(name)
        self.clusterName = "provisioner-test-" + str(uuid4())
        self.requestedLeaderStorage = 80

    def setUp(self):
        super().setUp()
        self.jobStore = f"google:{self.projectID}:autoscale-{uuid4()}"

    def _getScript(self):
        # TODO: Isn't this the key file?
        fileToSort = os.path.join(os.getcwd(), str(uuid4()))
        with open(fileToSort, "w") as f:
            # Fixme: making this file larger causes the test to hang
            f.write("01234567890123456789012345678901")
        self.rsyncUtil(
            get_data("test/sort/sort.py"),
            ":/home/sort.py",
        )
        self.rsyncUtil(fileToSort, ":/home/sortFile")
        os.unlink(fileToSort)

    def _runScript(self, toilOptions):
        runCommand = [
            "/home/venv/bin/python",
            "/home/sort.py",
            "--fileToSort=/home/sortFile",
        ]
        #'--sseKey=/home/sortFile']
        runCommand.extend(toilOptions)
        log.info("_runScript: %s" % "".join(runCommand))
        self.sshUtil(runCommand)

    def launchCluster(self):
        # add arguments to test that we can specify leader storage
        self.createClusterUtil(
            args=["--leaderStorage", str(self.requestedLeaderStorage)]
        )

    # TODO: aren't these checks inherited?
    @integrative
    @needs_google_project
    @needs_google_storage
    def testAutoScale(self):
        self.instanceTypes = ["n1-standard-2"]
        self.numWorkers = ["2"]
        self._test()

    @integrative
    @needs_google_project
    @needs_google_storage
    def testSpotAutoScale(self):
        self.instanceTypes = ["n1-standard-2:%f" % self.spotBid]
        # Some spot workers have a stopped state after being started, strangely.
        # This could be the natural preemption process, but it seems too rapid.
        self.numWorkers = ["3"]  # Try 3 to account for a stopped node.
        self._test(preemptibleJobs=True)


@pytest.mark.timeout(1600)
class GCEStaticAutoscaleTest(GCEAutoscaleTest):
    """
    Runs the tests on a statically provisioned cluster with autoscaling enabled.
    """

    def __init__(self, name):
        super().__init__(name)
        self.requestedNodeStorage = 20

    def launchCluster(self):
        self.createClusterUtil(
            args=[
                "--leaderStorage",
                str(self.requestedLeaderStorage),
                "--nodeTypes",
                ",".join(self.instanceTypes),
                "-w",
                ",".join(self.numWorkers),
                "--nodeStorage",
                str(self.requestedLeaderStorage),
            ]
        )

        # TODO: check the number of workers and their storage
        # nodes = AWSProvisioner._getNodesInCluster(ctx, self.clusterName, both=True)
        # nodes.sort(key=lambda x: x.launch_time)
        # assuming that leader is first
        # workers = nodes[1:]
        # test that two worker nodes were created
        # self.assertEqual(2, len(workers))
        # test that workers have expected storage size
        # just use the first worker
        # worker = workers[0]
        # worker = next(wait_instances_running(ctx.ec2, [worker]))
        # rootBlockDevice = worker.block_device_mapping["/dev/xvda"]
        # self.assertTrue(isinstance(rootBlockDevice, BlockDeviceType))
        # rootVolume = ctx.ec2.get_all_volumes(volume_ids=[rootBlockDevice.volume_id])[0]
        # self.assertGreaterEqual(rootVolume.size, self.requestedNodeStorage)

    def _runScript(self, toilOptions):
        runCommand = [
            "/home/venv/bin/python",
            "/home/sort.py",
            "--fileToSort=/home/sortFile",
        ]
        runCommand.extend(toilOptions)
        log.info("_runScript: %s" % "".join(runCommand))
        self.sshUtil(runCommand)


@pytest.mark.timeout(1800)
class GCEAutoscaleTestMultipleNodeTypes(AbstractGCEAutoscaleTest):

    def __init__(self, name):
        super().__init__(name)
        self.clusterName = "provisioner-test-" + str(uuid4())

    def setUp(self):
        super().setUp()
        self.jobStore = f"google:{self.projectID}:multinode-{uuid4()}"

    def _getScript(self):
        sseKeyFile = os.path.join(os.getcwd(), "keyFile")
        with open(sseKeyFile, "w") as f:
            f.write("01234567890123456789012345678901")
        self.rsyncUtil(
            get_data("test/sort/sort.py"),
            ":/home/sort.py",
        )
        self.rsyncUtil(sseKeyFile, ":/home/keyFile")
        os.unlink(sseKeyFile)

    def _runScript(self, toilOptions):
        # Set memory requirements so that sort jobs can be run
        # on small instances, but merge jobs must be run on large
        # instances
        runCommand = [
            "/home/venv/bin/python",
            "/home/sort.py",
            "--fileToSort=/home/s3am/bin/asadmin",
            "--sortMemory=0.6G",
            "--mergeMemory=3.0G",
        ]
        runCommand.extend(toilOptions)
        # runCommand.append('--sseKey=/home/keyFile')
        log.info("_runScript: %s" % "".join(runCommand))
        self.sshUtil(runCommand)

    @integrative
    @needs_google_project
    @needs_google_storage
    def testAutoScale(self):
        self.instanceTypes = ["n1-standard-2", "n1-standard-4"]
        self.numWorkers = ["2", "1"]
        self._test()


@pytest.mark.timeout(1800)
class GCERestartTest(AbstractGCEAutoscaleTest):
    """
    This test insures autoscaling works on a restarted Toil run
    """

    def __init__(self, name):
        super().__init__(name)
        self.clusterName = "restart-test-" + str(uuid4())

    def setUp(self):
        super().setUp()
        self.instanceTypes = ["n1-standard-1"]
        self.numWorkers = ["1"]
        self.scriptName = "/home/restartScript.py"
        # TODO: replace this with a google job store
        zone = "us-west-2"
        self.jobStore = f"google:{self.projectID}:restart-{uuid4()}"

    def _getScript(self):
        self.rsyncUtil(
            get_data("test/provisioners/restartScript.py"),
            ":" + self.scriptName,
        )

    def _runScript(self, toilOptions):
        # clean = onSuccess
        disallowedOptions = ["--clean=always", "--retryCount=2"]
        newOptions = [
            option for option in toilOptions if option not in disallowedOptions
        ]
        try:
            # include a default memory - on restart the minimum memory requirement is the default, usually 2 GB
            command = [
                "/home/venv/bin/python",
                self.scriptName,
                "-e",
                "FAIL=true",
                "--defaultMemory=50000000",
            ]
            command.extend(newOptions)
            self.sshUtil(command)
        except subprocess.CalledProcessError:
            pass
        else:
            self.fail("Command succeeded when we expected failure")
        with timeLimit(1200):
            command = [
                "/home/venv/bin/python",
                self.scriptName,
                "--restart",
                "--defaultMemory=50000000",
            ]
            command.extend(toilOptions)
            self.sshUtil(command)

    @integrative
    def testAutoScaledCluster(self):
        self._test()
