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
import time
from typing import Optional
from uuid import uuid4

from toil.lib.aws import zone_to_region
from toil.lib.aws.session import AWSConnectionManager
from toil.lib.retry import retry
from toil.provisioners import cluster_factory
from toil.provisioners.aws import get_best_aws_zone
from toil.test import (
    ToilTest,
    needs_aws_ec2,
    needs_env_var,
    needs_fetchable_appliance,
    slow,
)

log = logging.getLogger(__name__)


@needs_aws_ec2
@needs_fetchable_appliance
class AbstractClusterTest(ToilTest):
    def __init__(self, methodName: str) -> None:
        super().__init__(methodName=methodName)
        self.keyName = os.getenv("TOIL_AWS_KEYNAME", "id_rsa").strip()
        self.clusterName = f"aws-provisioner-test-{uuid4()}"
        self.leaderNodeType = "t2.medium"
        self.clusterType = "mesos"
        self.zone = get_best_aws_zone()
        assert (
            self.zone is not None
        ), "Could not determine AWS availability zone to test in; is TOIL_AWS_ZONE set?"
        self.region = zone_to_region(self.zone)

        # Get connection to AWS
        self.aws = AWSConnectionManager()

        # Where should we put our virtualenv?
        self.venvDir = "/tmp/venv"

    def python(self) -> str:
        """
        Return the full path to the venv Python on the leader.
        """
        return os.path.join(self.venvDir, "bin/python")

    def pip(self) -> str:
        """
        Return the full path to the venv pip on the leader.
        """
        return os.path.join(self.venvDir, "bin/pip")

    def destroyCluster(self) -> None:
        """
        Destroy the cluster we built, if it exists.

        Succeeds if the cluster does not currently exist.
        """
        subprocess.check_call(
            ["toil", "destroy-cluster", "-p=aws", "-z", self.zone, self.clusterName]
        )

    def setUp(self) -> None:
        """
        Set up for the test.
        Must be overridden to call this method and set self.jobStore.
        """
        super().setUp()
        # Make sure that destroy works before we create any clusters.
        # If this fails, no tests will run.
        self.destroyCluster()

    def tearDown(self) -> None:
        # Note that teardown will run even if the test crashes.
        super().tearDown()
        self.destroyCluster()
        subprocess.check_call(["toil", "clean", self.jobStore])

    def sshUtil(self, command: list[str]) -> None:
        """
        Run the given command on the cluster.
        Raise subprocess.CalledProcessError if it fails.
        """

        cmd = [
            "toil",
            "ssh-cluster",
            "--insecure",
            "-p=aws",
            "-z",
            self.zone,
            self.clusterName,
        ] + command
        log.info("Running %s.", str(cmd))
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        # Put in non-blocking mode. See https://stackoverflow.com/a/59291466
        os.set_blocking(p.stdout.fileno(), False)
        os.set_blocking(p.stderr.fileno(), False)

        out_buffer = b""
        err_buffer = b""

        loops_since_line = 0

        running = True
        while running:
            # While the process is running, see if it stopped
            running = p.poll() is None

            # Also collect its output
            out_data = p.stdout.read()
            if out_data:
                out_buffer += out_data

            while out_buffer.find(b"\n") != -1:
                # And log every full line
                cut = out_buffer.find(b"\n")
                log.info(
                    "STDOUT: %s", out_buffer[0:cut].decode("utf-8", errors="ignore")
                )
                loops_since_line = 0
                out_buffer = out_buffer[cut + 1 :]

            # Same for the error
            err_data = p.stderr.read()
            if err_data:
                err_buffer += err_data

            while err_buffer.find(b"\n") != -1:
                cut = err_buffer.find(b"\n")
                log.info(
                    "STDERR: %s", err_buffer[0:cut].decode("utf-8", errors="ignore")
                )
                loops_since_line = 0
                err_buffer = err_buffer[cut + 1 :]

            loops_since_line += 1
            if loops_since_line > 60:
                log.debug("...waiting...")
                loops_since_line = 0

            time.sleep(1)

        # At the end, log the last lines
        if out_buffer:
            log.info("STDOUT: %s", out_buffer.decode("utf-8", errors="ignore"))
        if err_buffer:
            log.info("STDERR: %s", err_buffer.decode("utf-8", errors="ignore"))

        if p.returncode != 0:
            # It failed
            log.error("Failed to run %s.", str(cmd))
            raise subprocess.CalledProcessError(p.returncode, " ".join(cmd))

    @retry(errors=[subprocess.CalledProcessError], intervals=[1, 1])
    def rsync_util(self, from_file: str, to_file: str) -> None:
        """
        Transfer a file to/from the cluster.

        The cluster-side path should have a ':' in front of it.
        """
        cmd = [
            "toil",
            "rsync-cluster",
            "--insecure",
            "-p=aws",
            "-z",
            self.zone,
            self.clusterName,
            from_file,
            to_file,
        ]
        log.info("Running %s.", str(cmd))
        subprocess.check_call(cmd)

    @retry(errors=[subprocess.CalledProcessError], intervals=[1, 1])
    def createClusterUtil(self, args: Optional[list[str]] = None) -> None:
        args = [] if args is None else args

        command = [
            "toil",
            "launch-cluster",
            "-p=aws",
            "-z",
            self.zone,
            f"--keyPairName={self.keyName}",
            f"--leaderNodeType={self.leaderNodeType}",
            f"--clusterType={self.clusterType}",
            "--logDebug",
            self.clusterName,
        ] + args

        log.debug("Launching cluster: %s", command)

        # Try creating the cluster
        subprocess.check_call(command)
        # If we fail, tearDown will destroy the cluster.

    def launchCluster(self) -> None:
        self.createClusterUtil()


@needs_aws_ec2
@needs_fetchable_appliance
@slow
class CWLOnARMTest(AbstractClusterTest):
    """Run the CWL 1.2 conformance tests on ARM specifically."""

    def __init__(self, methodName: str) -> None:
        super().__init__(methodName=methodName)
        self.clusterName = f"cwl-test-{uuid4()}"
        self.leaderNodeType = "t4g.2xlarge"
        self.clusterType = "kubernetes"
        # We need to be running in a directory which Flatcar and the Toil Appliance both have
        self.cwl_test_dir = "/tmp/toil/cwlTests"

    def setUp(self) -> None:
        super().setUp()
        self.jobStore = f"aws:{self.awsRegion()}:cluster-{uuid4()}"

    @needs_env_var("CI_COMMIT_SHA", "a git commit sha")
    def test_cwl_on_arm(self) -> None:
        # Make a cluster
        self.launchCluster()
        # get the leader so we know the IP address - we don't need to wait since create cluster
        # already ensures the leader is running
        self.cluster = cluster_factory(
            provisioner="aws", zone=self.zone, clusterName=self.clusterName
        )
        self.leader = self.cluster.getLeader()

        commit = os.environ["CI_COMMIT_SHA"]
        self.sshUtil(
            [
                "bash",
                "-c",
                f"mkdir -p {self.cwl_test_dir} && cd {self.cwl_test_dir} && git clone https://github.com/DataBiosphere/toil.git",
            ]
        )

        # We use CI_COMMIT_SHA to retrieve the Toil version needed to run the CWL tests
        self.sshUtil(
            ["bash", "-c", f"cd {self.cwl_test_dir}/toil && git checkout {commit}"]
        )

        # --never-download prevents silent upgrades to pip, wheel and setuptools
        self.sshUtil(
            [
                "bash",
                "-c",
                f"virtualenv --system-site-packages --never-download {self.venvDir}",
            ]
        )
        self.sshUtil(
            [
                "bash",
                "-c",
                f". .{self.venvDir}/bin/activate && cd {self.cwl_test_dir}/toil && make prepare && make develop extras=[all]",
            ]
        )

        # Runs the CWLv12Test on an ARM instance
        self.sshUtil(
            [
                "bash",
                "-c",
                f". .{self.venvDir}/bin/activate && cd {self.cwl_test_dir}/toil && pytest --log-cli-level DEBUG -r s src/toil/test/cwl/cwlTest.py::CWLv12Test::test_run_conformance",
            ]
        )

        # We know if it succeeds it should save a junit XML for us to read.
        # Bring it back to be an artifact.
        self.rsync_util(
            f":{self.cwl_test_dir}/toil/conformance-1.2.junit.xml",
            os.path.join(self._projectRootPath(), "arm-conformance-1.2.junit.xml"),
        )
