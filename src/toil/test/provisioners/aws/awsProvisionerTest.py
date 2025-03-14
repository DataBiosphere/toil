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
from typing import TYPE_CHECKING, Optional
from uuid import uuid4

import pytest

from toil.provisioners import cluster_factory
from toil.provisioners.aws.awsProvisioner import AWSProvisioner
from toil.test import (
    ToilTest,
    get_data,
    integrative,
    needs_aws_ec2,
    needs_fetchable_appliance,
    needs_mesos,
    slow,
    timeLimit,
)
from toil.test.provisioners.clusterTest import AbstractClusterTest
from toil.version import exactPython

if TYPE_CHECKING:
    from mypy_boto3_ec2 import EC2Client
    from mypy_boto3_ec2.type_defs import (
        DescribeVolumesResultTypeDef,
        EbsInstanceBlockDeviceTypeDef,
        FilterTypeDef,
        InstanceBlockDeviceMappingTypeDef,
        InstanceTypeDef,
        VolumeTypeDef,
    )


log = logging.getLogger(__name__)


class AWSProvisionerBenchTest(ToilTest):
    """
    Tests for the AWS provisioner that don't actually provision anything.
    """

    # Needs to talk to EC2 for image discovery
    @needs_aws_ec2
    def test_AMI_finding(self):
        for zone in ["us-west-2a", "eu-central-1a", "sa-east-1b"]:
            provisioner = AWSProvisioner(
                "fakename", "mesos", zone, 10000, None, None, enable_fuse=False
            )
            ami = provisioner._discoverAMI()
            # Make sure we got an AMI and it looks plausible
            assert ami.startswith("ami-")

    @needs_aws_ec2
    def test_read_write_global_files(self):
        """
        Make sure the `_write_file_to_cloud()` and `_read_file_from_cloud()`
        functions of the AWS provisioner work as intended.
        """
        provisioner = AWSProvisioner(
            f"aws-provisioner-test-{uuid4()}",
            "mesos",
            "us-west-2a",
            50,
            None,
            None,
            enable_fuse=False,
        )
        key = "config/test.txt"
        contents = b"Hello, this is a test."

        try:
            url = provisioner._write_file_to_cloud(key, contents=contents)
            self.assertTrue(url.startswith("s3://"))

            self.assertEqual(contents, provisioner._read_file_from_cloud(key))
        finally:
            # the cluster was never launched, but we need to clean up the s3 bucket
            provisioner.destroyCluster()


@needs_aws_ec2
@needs_fetchable_appliance
@slow
@integrative
class AbstractAWSAutoscaleTest(AbstractClusterTest):
    def __init__(self, methodName):
        super().__init__(methodName=methodName)
        self.instanceTypes = ["m5a.large"]
        self.clusterName = "aws-provisioner-test-" + str(uuid4())
        self.numWorkers = ["2"]
        self.numSamples = 2
        self.spotBid = 0.15
        # We can't dump our user script right in /tmp or /home, because hot
        # deploy refuses to zip up those whole directories. So we make sure to
        # have a subdirectory to upload the script to.
        self.scriptDir = "/tmp/t"
        # Where should we put our virtualenv?
        self.venvDir = "/tmp/venv"
        # Where should we put our data to work on?
        # Must exist in the Toil container; the leader will try to rsync to it
        # (for the SSE key) and not create it.
        self.dataDir = "/tmp"
        # What filename should we use for our script (without path)?
        # Can be changed by derived tests.
        self.scriptName = "test_script.py"

    def script(self):
        """
        Return the full path to the user script on the leader.
        """
        return os.path.join(self.scriptDir, self.scriptName)

    def data(self, filename):
        """
        Return the full path to the data file with the given name on the leader.
        """
        return os.path.join(self.dataDir, filename)

    def rsyncUtil(self, src, dest):
        subprocess.check_call(
            [
                "toil",
                "rsync-cluster",
                "--insecure",
                "-p=aws",
                "-z",
                self.zone,
                self.clusterName,
            ]
            + [src, dest]
        )

    def getRootVolID(self) -> str:
        instances: list["InstanceTypeDef"] = self.cluster._get_nodes_in_cluster_boto3()
        instances.sort(key=lambda x: x.get("LaunchTime"))
        leader: "InstanceTypeDef" = instances[0]  # assume leader was launched first

        bdm: Optional[list["InstanceBlockDeviceMappingTypeDef"]] = leader.get(
            "BlockDeviceMappings"
        )
        assert bdm is not None
        root_block_device: Optional["EbsInstanceBlockDeviceTypeDef"] = None
        for device in bdm:
            if device["DeviceName"] == "/dev/xvda":
                root_block_device = device["Ebs"]
        assert (
            root_block_device is not None
        )  # There should be a device named "/dev/xvda"
        assert root_block_device.get("VolumeId") is not None
        return root_block_device["VolumeId"]

    @abstractmethod
    def _getScript(self):
        """Download the test script needed by the inheriting unit test class."""
        raise NotImplementedError()

    def putScript(self, content: str):
        """
        Helper method for _getScript to inject a script file at the configured script path, from text.
        """
        cluster = cluster_factory(
            provisioner="aws", zone=self.zone, clusterName=self.clusterName
        )
        leader = cluster.getLeader()

        self.sshUtil(["mkdir", "-p", self.scriptDir])

        with tempfile.NamedTemporaryFile(mode="w") as t:
            # use appliance ssh method instead of sshutil so we can specify input param
            t.write(content)
            # This works to make writes visible on non-Windows
            t.flush()
            leader.injectFile(t.name, self.script(), "toil_leader")

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
        """Does the work of the testing.  Many features' tests are thrown in here in no particular order."""
        # Make a cluster
        self.launchCluster()
        # get the leader so we know the IP address - we don't need to wait since create cluster
        # already insures the leader is running
        self.cluster = cluster_factory(
            provisioner="aws", zone=self.zone, clusterName=self.clusterName
        )
        self.leader = self.cluster.getLeader()
        self.sshUtil(["mkdir", "-p", self.scriptDir])
        self.sshUtil(["mkdir", "-p", self.dataDir])

        assert len(self.cluster._getRoleNames()) == 1
        # --never-download prevents silent upgrades to pip, wheel and setuptools
        venv_command = [
            "virtualenv",
            "--system-site-packages",
            "--python",
            exactPython,
            "--never-download",
            self.venvDir,
        ]
        self.sshUtil(venv_command)

        log.info("Set up script...")
        self._getScript()

        toilOptions = [
            self.jobStore,
            "--workDir=/var/lib/toil",
            "--clean=always",
            "--retryCount=2",
            "--logDebug",
            "--logFile=" + os.path.join(self.scriptDir, "sort.log"),
        ]

        if preemptibleJobs:
            toilOptions.extend(["--defaultPreemptible"])

        log.info("Run script...")
        self._runScript(toilOptions)

        assert len(self.cluster._getRoleNames()) == 1

        volumeID = self.getRootVolID()
        self.cluster.destroyCluster()
        boto3_ec2: "EC2Client" = self.aws.client(region=self.region, service_name="ec2")
        volume_filter: "FilterTypeDef" = {"Name": "volume-id", "Values": [volumeID]}
        volumes: Optional[list["VolumeTypeDef"]] = None
        for attempt in range(6):
            # https://github.com/BD2KGenomics/toil/issues/1567
            # retry this for up to 1 minute until the volume disappears
            volumes = boto3_ec2.describe_volumes(Filters=[volume_filter])["Volumes"]
            if len(volumes) == 0:
                # None are left, so they have been properly deleted
                break
            time.sleep(10)
        if volumes is None or len(volumes) > 0:
            self.fail("Volume with ID %s was not cleaned up properly" % volumeID)

        assert len(self.cluster._getRoleNames()) == 0


@integrative
@needs_mesos
@pytest.mark.timeout(1800)
class AWSAutoscaleTest(AbstractAWSAutoscaleTest):
    def __init__(self, name):
        super().__init__(name)
        self.clusterName = "provisioner-test-" + str(uuid4())
        self.requestedLeaderStorage = 80
        self.scriptName = "sort.py"

    def setUp(self):
        super().setUp()
        self.jobStore = f"aws:{self.awsRegion()}:autoscale-{uuid4()}"

    def _getScript(self):
        fileToSort = os.path.join(os.getcwd(), str(uuid4()))
        with open(fileToSort, "w") as f:
            # Fixme: making this file larger causes the test to hang
            f.write("01234567890123456789012345678901")
        self.rsyncUtil(
            get_data("test/sort/sort.py"),
            ":" + self.script(),
        )
        self.rsyncUtil(fileToSort, ":" + self.data("sortFile"))
        os.unlink(fileToSort)

    def _runScript(self, toilOptions):
        toilOptions.extend(
            [
                "--provisioner=aws",
                "--batchSystem=mesos",
                "--nodeTypes=" + ",".join(self.instanceTypes),
                "--maxNodes=" + ",".join(self.numWorkers),
            ]
        )
        runCommand = [
            self.python(),
            self.script(),
            "--fileToSort=" + self.data("sortFile"),
            "--sseKey=" + self.data("sortFile"),
        ]
        runCommand.extend(toilOptions)
        self.sshUtil(runCommand)

    def launchCluster(self):
        # add arguments to test that we can specify leader storage
        self.createClusterUtil(
            args=["--leaderStorage", str(self.requestedLeaderStorage)]
        )

    def getRootVolID(self) -> str:
        """
        Adds in test to check that EBS volume is build with adequate size.
        Otherwise is functionally equivalent to parent.
        :return: volumeID
        """
        volumeID = super().getRootVolID()
        boto3_ec2: "EC2Client" = self.aws.client(region=self.region, service_name="ec2")
        volume_filter: "FilterTypeDef" = {"Name": "volume-id", "Values": [volumeID]}
        volumes: "DescribeVolumesResultTypeDef" = boto3_ec2.describe_volumes(
            Filters=[volume_filter]
        )
        root_volume: "VolumeTypeDef" = volumes["Volumes"][0]  # should be first
        # test that the leader is given adequate storage
        self.assertGreaterEqual(root_volume["Size"], self.requestedLeaderStorage)
        return volumeID

    @integrative
    @needs_aws_ec2
    def testAutoScale(self):
        self.instanceTypes = ["m5a.large"]
        self.numWorkers = ["2"]
        self._test()

    @integrative
    @needs_aws_ec2
    def testSpotAutoScale(self):
        self.instanceTypes = ["m5a.large:%f" % self.spotBid]
        self.numWorkers = ["2"]
        self._test(preemptibleJobs=True)

    @integrative
    @needs_aws_ec2
    def testSpotAutoScaleBalancingTypes(self):
        self.instanceTypes = ["m5.large/m5a.large:%f" % self.spotBid]
        self.numWorkers = ["2"]
        self._test(preemptibleJobs=True)


@integrative
@needs_mesos
@pytest.mark.timeout(2400)
class AWSStaticAutoscaleTest(AWSAutoscaleTest):
    """Runs the tests on a statically provisioned cluster with autoscaling enabled."""

    def __init__(self, name):
        super().__init__(name)
        self.requestedNodeStorage = 20

    def launchCluster(self):
        from toil.lib.ec2 import wait_instances_running

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

        self.cluster = cluster_factory(
            provisioner="aws", zone=self.zone, clusterName=self.clusterName
        )
        # We need to wait a little bit here because the workers might not be
        # visible to EC2 read requests immediately after the create returns,
        # which is the last thing that starting the cluster does.
        time.sleep(10)
        nodes: list["InstanceTypeDef"] = self.cluster._get_nodes_in_cluster_boto3()
        nodes.sort(key=lambda x: x.get("LaunchTime"))
        # assuming that leader is first
        workers = nodes[1:]
        # test that two worker nodes were created
        self.assertEqual(2, len(workers))
        # test that workers have expected storage size
        # just use the first worker
        worker = workers[0]
        boto3_ec2: "EC2Client" = self.aws.client(region=self.region, service_name="ec2")

        worker: "InstanceTypeDef" = next(wait_instances_running(boto3_ec2, [worker]))

        bdm: Optional[list["InstanceBlockDeviceMappingTypeDef"]] = worker.get(
            "BlockDeviceMappings"
        )
        assert bdm is not None
        root_block_device: Optional["EbsInstanceBlockDeviceTypeDef"] = None
        for device in bdm:
            if device["DeviceName"] == "/dev/xvda":
                root_block_device = device["Ebs"]
        assert root_block_device is not None
        assert (
            root_block_device.get("VolumeId") is not None
        )  # TypedDicts cannot have runtime type checks

        volume_filter: "FilterTypeDef" = {
            "Name": "volume-id",
            "Values": [root_block_device["VolumeId"]],
        }
        root_volume: "VolumeTypeDef" = boto3_ec2.describe_volumes(
            Filters=[volume_filter]
        )["Volumes"][
            0
        ]  # should be first
        self.assertGreaterEqual(root_volume.get("Size"), self.requestedNodeStorage)

    def _runScript(self, toilOptions):
        # Autoscale even though we have static nodes
        toilOptions.extend(
            [
                "--provisioner=aws",
                "--batchSystem=mesos",
                "--nodeTypes=" + ",".join(self.instanceTypes),
                "--maxNodes=" + ",".join(self.numWorkers),
            ]
        )
        runCommand = [
            self.python(),
            self.script(),
            "--fileToSort=" + self.data("sortFile"),
        ]
        runCommand.extend(toilOptions)
        self.sshUtil(runCommand)


@integrative
@pytest.mark.timeout(1200)
class AWSManagedAutoscaleTest(AWSAutoscaleTest):
    """Runs the tests on a self-scaling Kubernetes cluster."""

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
                "--workers",
                ",".join([f"0-{c}" for c in self.numWorkers]),
                "--nodeStorage",
                str(self.requestedLeaderStorage),
                "--clusterType",
                "kubernetes",
            ]
        )

        self.cluster = cluster_factory(
            provisioner="aws", zone=self.zone, clusterName=self.clusterName
        )

    def _runScript(self, toilOptions):
        # Don't use the provisioner, and use Kubernetes instead of Mesos
        toilOptions.extend(["--batchSystem=kubernetes"])
        runCommand = [
            self.python(),
            self.script(),
            "--fileToSort=" + self.data("sortFile"),
        ]
        runCommand.extend(toilOptions)
        self.sshUtil(runCommand)


@integrative
@needs_mesos
@pytest.mark.timeout(1200)
class AWSAutoscaleTestMultipleNodeTypes(AbstractAWSAutoscaleTest):
    def __init__(self, name):
        super().__init__(name)
        self.clusterName = "provisioner-test-" + str(uuid4())

    def setUp(self):
        super().setUp()
        self.jobStore = f"aws:{self.awsRegion()}:autoscale-{uuid4()}"

    def _getScript(self):
        sseKeyFile = os.path.join(os.getcwd(), "keyFile")
        with open(sseKeyFile, "w") as f:
            f.write("01234567890123456789012345678901")
        self.rsyncUtil(
            get_data("test/sort/sort.py"),
            ":" + self.script(),
        )
        self.rsyncUtil(sseKeyFile, ":" + self.data("keyFile"))
        os.unlink(sseKeyFile)

    def _runScript(self, toilOptions):
        # Set memory requirements so that sort jobs can be run
        # on small instances, but merge jobs must be run on large
        # instances
        toilOptions.extend(
            [
                "--provisioner=aws",
                "--batchSystem=mesos",
                "--nodeTypes=" + ",".join(self.instanceTypes),
                "--maxNodes=" + ",".join(self.numWorkers),
            ]
        )
        runCommand = [
            self.python(),
            self.script(),
            "--fileToSort=/home/s3am/bin/asadmin",
            "--sortMemory=0.6G",
            "--mergeMemory=3.0G",
        ]
        runCommand.extend(toilOptions)
        runCommand.append("--sseKey=" + self.data("keyFile"))
        self.sshUtil(runCommand)

    @integrative
    @needs_aws_ec2
    def testAutoScale(self):
        self.instanceTypes = ["t2.small", "m5a.large"]
        self.numWorkers = ["2", "1"]
        self._test()


@integrative
@needs_mesos
@pytest.mark.timeout(1200)
class AWSRestartTest(AbstractAWSAutoscaleTest):
    """This test insures autoscaling works on a restarted Toil run."""

    def __init__(self, name):
        super().__init__(name)
        self.clusterName = "restart-test-" + str(uuid4())
        self.scriptName = "restartScript.py"

    def setUp(self):
        super().setUp()
        self.instanceTypes = ["t2.small"]
        self.numWorkers = ["1"]
        self.jobStore = f"aws:{self.awsRegion()}:restart-{uuid4()}"

    def _getScript(self):
        def restartScript():
            import os

            from configargparse import ArgumentParser

            from toil.job import Job

            def f0(job):
                if "FAIL" in os.environ:
                    raise RuntimeError("failed on purpose")

            if __name__ == "__main__":
                parser = ArgumentParser()
                Job.Runner.addToilOptions(parser)
                options = parser.parse_args()
                rootJob = Job.wrapJobFn(f0, cores=0.5, memory="50 M", disk="50 M")
                Job.Runner.startToil(rootJob, options)

        script = dedent("\n".join(getsource(restartScript).split("\n")[1:]))
        self.putScript(script)

    def _runScript(self, toilOptions):
        # Use the provisioner in the workflow
        toilOptions.extend(
            [
                "--provisioner=aws",
                "--batchSystem=mesos",
                "--nodeTypes=" + ",".join(self.instanceTypes),
                "--maxNodes=" + ",".join(self.numWorkers),
            ]
        )
        # clean = onSuccess
        disallowedOptions = ["--clean=always", "--retryCount=2"]
        newOptions = [
            option for option in toilOptions if option not in disallowedOptions
        ]
        try:
            # include a default memory - on restart the minimum memory requirement is the default, usually 2 GB
            command = [
                self.python(),
                self.script(),
                "--setEnv",
                "FAIL=true",
                "--defaultMemory=50000000",
            ]
            command.extend(newOptions)
            self.sshUtil(command)
        except subprocess.CalledProcessError:
            pass
        else:
            self.fail("Command succeeded when we expected failure")
        with timeLimit(600):
            command = [
                self.python(),
                self.script(),
                "--restart",
                "--defaultMemory=50000000",
            ]
            command.extend(toilOptions)
            self.sshUtil(command)

    def testAutoScaledCluster(self):
        self._test()


@integrative
@needs_mesos
@pytest.mark.timeout(1200)
class PreemptibleDeficitCompensationTest(AbstractAWSAutoscaleTest):
    def __init__(self, name):
        super().__init__(name)
        self.clusterName = "deficit-test-" + str(uuid4())
        self.scriptName = "userScript.py"

    def setUp(self):
        super().setUp()
        self.instanceTypes = [
            "m5a.large:0.01",
            "m5a.large",
        ]  # instance needs to be available on the spot market
        self.numWorkers = ["1", "1"]
        self.jobStore = f"aws:{self.awsRegion()}:deficit-{uuid4()}"

    def test(self):
        self._test(preemptibleJobs=True)

    def _getScript(self):
        def userScript():
            from toil.common import Toil
            from toil.job import Job

            # Because this is the only job in the pipeline and because it is preemptible,
            # there will be no non-preemptible jobs. The non-preemptible scaler will therefore
            # not request any nodes initially. And since we made it impossible for the
            # preemptible scaler to allocate any nodes (using an abnormally low spot bid),
            # we will observe a deficit of preemptible nodes that the non-preemptible scaler will
            # compensate for by spinning up non-preemptible nodes instead.
            #
            def job(job, disk="10M", cores=1, memory="10M", preemptible=True):
                pass

            if __name__ == "__main__":
                options = Job.Runner.getDefaultArgumentParser().parse_args()
                with Toil(options) as toil:
                    if toil.config.restart:
                        toil.restart()
                    else:
                        toil.start(Job.wrapJobFn(job))

        script = dedent("\n".join(getsource(userScript).split("\n")[1:]))
        self.putScript(script)

    def _runScript(self, toilOptions):
        toilOptions.extend(
            [
                "--provisioner=aws",
                "--batchSystem=mesos",
                "--nodeTypes=" + ",".join(self.instanceTypes),
                "--maxNodes=" + ",".join(self.numWorkers),
            ]
        )
        toilOptions.extend(["--preemptibleCompensation=1.0"])
        command = [self.python(), self.script()]
        command.extend(toilOptions)
        self.sshUtil(command)
