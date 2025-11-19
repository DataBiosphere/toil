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
import builtins
import logging
import os
import re
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any, cast

import pytest

import toil
from toil import resolveEntryPoint
from toil.common import Config, Toil
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import Job
from toil.lib.bioio import system
from toil.test import get_data
from toil.test import pintegrative as integrative
from toil.test import pneeds_aws_ec2 as needs_aws_ec2
from toil.test import pneeds_cwl as needs_cwl
from toil.test import pneeds_docker as needs_docker
from toil.test import pneeds_rsync3 as needs_rsync3
from toil.test import pslow as slow
from toil.test.sort.sort import makeFileToSort
from toil.utils.toilStats import get_stats, process_data
from toil.utils.toilStatus import ToilStatus
from toil.version import python

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def unsortedFile(tmp_path: Path) -> Generator[Path]:
    try:
        tempFile = tmp_path / "lines"
        makeFileToSort(str(tempFile), 1000, 10)
        yield tempFile
    finally:
        pass  # no cleanup needed


@pytest.fixture(scope="function")
def correctSort(unsortedFile: Path) -> list[str]:
    with unsortedFile.open() as fileHandle:
        lines = fileHandle.readlines()
        lines.sort()
        return lines


class TestUtils:
    """
    Tests the utilities that toil ships with, e.g. stats and status, in conjunction with restart
    functionality.
    """

    N: int = 1000

    @property
    def toilMain(self) -> str:
        return resolveEntryPoint("toil")

    def cleanCommand(self, jobstore: Path) -> list[str]:
        return [self.toilMain, "clean", str(jobstore)]

    def statsCommand(self, jobstore: Path) -> list[str]:
        return [self.toilMain, "stats", str(jobstore), "--pretty"]

    def statusCommand(
        self, jobstore: Path, failIfNotComplete: bool = False
    ) -> list[str]:
        commandTokens = [self.toilMain, "status", str(jobstore)]
        if failIfNotComplete:
            commandTokens.append("--failIfNotComplete")
        return commandTokens

    def test_config_functionality(self, tmp_path: Path) -> None:
        """Ensure that creating and reading back the config file works"""
        config_file = tmp_path / "config.yaml"
        config_command = [self.toilMain, "config", str(config_file)]
        # make sure the command `toil config file_path` works
        try:
            subprocess.check_call(config_command)
        except subprocess.CalledProcessError:
            pytest.fail("The toil config utility failed!")

        parser = Job.Runner.getDefaultArgumentParser()
        # make sure that toil can read from the generated config file
        try:
            parser.parse_args(
                [str(tmp_path / "random_jobstore"), "--config", str(config_file)]
            )
            with config_file.open() as cm:
                payload = cm.read()
                expected = "workDir batchSystem symlinkImports defaultMemory retryCount"
                assert all(
                    re.search(rf"^#*{ param }:", payload, re.MULTILINE)
                    for param in expected.split(" ")
                ), f"Generated config contains { expected }"
        except SystemExit:
            pytest.fail("Failed to parse the default generated config file!")

    @needs_rsync3
    @pytest.mark.timeout(1200)
    @needs_aws_ec2
    @integrative
    @pytest.mark.rsync
    @pytest.mark.online
    @pytest.mark.aws_s3
    @pytest.mark.integrative
    @slow
    @pytest.mark.slow
    def testAWSProvisionerUtils(self) -> None:
        """
        Runs a number of the cluster utilities in sequence.

        Launches a cluster with custom tags.
        Verifies the tags exist.
        ssh's into the cluster.
        Does some weird string comparisons.
        Makes certain that TOIL_WORKDIR is set as expected in the ssh'ed cluster.
        Rsyncs a file and verifies it exists on the leader.
        Destroys the cluster.

        :return:
        """
        # TODO: Run these for the other clouds.
        clusterName = f"cluster-utils-test{uuid.uuid4()}"
        keyName = os.getenv("TOIL_AWS_KEYNAME", "id_rsa").strip()
        expected_owner = os.getenv("TOIL_OWNER_TAG", keyName)

        try:
            from toil.provisioners.aws.awsProvisioner import AWSProvisioner

            aws_provisioner = AWSProvisioner.__module__
            logger.debug(f"Found AWSProvisioner: {aws_provisioner}.")

            # launch master with an assortment of custom tags
            system(
                [
                    self.toilMain,
                    "launch-cluster",
                    "--clusterType",
                    "mesos",
                    "-t",
                    "key1=value1",
                    "-t",
                    "key2=value2",
                    "--tag",
                    "key3=value3",
                    "--leaderNodeType=t2.medium",
                    "--keyPairName=" + keyName,
                    clusterName,
                    "--provisioner=aws",
                    "--zone=us-west-2a",
                    "--logLevel=DEBUG",
                ]
            )

            cluster = toil.provisioners.cluster_factory(
                provisioner="aws", zone="us-west-2a", clusterName=clusterName
            )
            leader = cluster.getLeader()

            # check that the leader carries the appropriate tags
            tags = {
                "key1": "value1",
                "key2": "value2",
                "key3": "value3",
                "Name": clusterName,
                "Owner": expected_owner,
            }
            for key in tags:
                assert cast(dict[str, str], leader.tags).get(key) == tags[key]
        finally:
            system(
                [
                    self.toilMain,
                    "destroy-cluster",
                    "--zone=us-west-2a",
                    "--provisioner=aws",
                    clusterName,
                ]
            )

    @slow
    def testUtilsSort(
        self, tmp_path: Path, unsortedFile: Path, correctSort: list[str]
    ) -> None:
        """
        Tests the status and stats commands of the toil command line utility using the
        sort example with the --restart flag.
        """
        jobstore = tmp_path / "jobstore"
        outputFile = tmp_path / "someSortedStuff.txt"
        # Get the sort command to run
        toilCommand = [
            sys.executable,
            "-m",
            toil.test.sort.sort.__name__,
            str(jobstore),
            "--logLevel=DEBUG",
            "--fileToSort",
            str(unsortedFile),
            "--outputFile",
            str(outputFile),
            "--N",
            str(self.N),
            "--stats",
            "--retryCount=2",
            "--badWorker=0.5",
            "--badWorkerFailInterval=0.05",
        ]
        # Try restarting it to check that a JobStoreException is thrown
        with pytest.raises(subprocess.CalledProcessError):
            system(toilCommand + ["--restart"])
        # Check that trying to run it in restart mode does not create the jobStore
        assert not jobstore.exists()

        # Status command
        # Run the script for the first time
        try:
            system(toilCommand)
            finished = True
        except (
            subprocess.CalledProcessError
        ):  # This happens when the script fails due to having unfinished jobs
            system(self.statusCommand(jobstore))
            with pytest.raises(subprocess.CalledProcessError):
                system(self.statusCommand(jobstore, failIfNotComplete=True))
            finished = False
        assert jobstore.exists()

        # Try running it without restart and check an exception is thrown
        with pytest.raises(subprocess.CalledProcessError):
            system(toilCommand)

        # Now restart it until done
        totalTrys = 1
        while not finished:
            try:
                system(toilCommand + ["--restart"])
                finished = True
            except (
                subprocess.CalledProcessError
            ):  # This happens when the script fails due to having unfinished jobs
                system(self.statusCommand(jobstore))
                with pytest.raises(subprocess.CalledProcessError):
                    system(self.statusCommand(jobstore, failIfNotComplete=True))
                if totalTrys > 16:
                    pytest.fail("Exceeded a reasonable number of restarts")
                totalTrys += 1

        # Check the toil status command does not issue an exception
        system(self.statusCommand(jobstore))

        # Check we can run 'toil stats'
        system(self.statsCommand(jobstore))

        # Check the file is properly sorted
        with outputFile.open() as fileHandle:
            l2 = fileHandle.readlines()
            assert correctSort == l2

        # Check we can run 'toil clean'
        system(self.cleanCommand(jobstore))

    @slow
    @pytest.mark.slow
    def testUtilsStatsSort(
        self, tmp_path: Path, unsortedFile: Path, correctSort: list[str]
    ) -> None:
        """
        Tests the stats commands on a complete run of the stats test.
        """
        jobstore = tmp_path / "jobstore"
        outputFile = tmp_path / "someSortedStuff.txt"
        # Get the sort command to run
        toilCommand = [
            sys.executable,
            "-m",
            toil.test.sort.sort.__name__,
            str(jobstore),
            "--logLevel=DEBUG",
            "--fileToSort",
            str(unsortedFile),
            "--outputFile",
            str(outputFile),
            "--N",
            str(self.N),
            "--stats",
            "--retryCount=99",
            "--badWorker=0.5",
            "--badWorkerFailInterval=0.01",
        ]

        # Run the script for the first time
        system(toilCommand)
        assert jobstore.exists()

        # Check we can run 'toil stats'
        system(self.statsCommand(jobstore))

        # Check the file is properly sorted
        with outputFile.open() as fileHandle:
            l2 = fileHandle.readlines()
            assert correctSort == l2

    def testUnicodeSupport(self, tmp_path: Path) -> None:
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.clean = "always"
        options.logLevel = "debug"
        Job.Runner.startToil(Job.wrapFn(printUnicodeCharacter), options)

    @slow
    @pytest.mark.slow
    def testMultipleJobsPerWorkerStats(self, tmp_path: Path) -> None:
        """
        Tests case where multiple jobs are run on 1 worker to ensure that all jobs report back their data
        """
        options = Job.Runner.getDefaultOptions(tmp_path / "jobstore")
        options.clean = "never"
        options.stats = True
        Job.Runner.startToil(RunTwoJobsPerWorker(), options)
        config = Config()
        config.setOptions(options)
        jobStore = Toil.resumeJobStore(config.jobStore)
        stats = get_stats(jobStore)
        collatedStats = process_data(jobStore.config, stats)
        assert (
            len(collatedStats.job_types) == 2  # type: ignore[attr-defined]
        ), "Some jobs are not represented in the stats."

    def check_status(
        self,
        jobstore: Path,
        status: str,
        status_fn: Callable[[str], str],
        process: subprocess.Popen[Any] | None = None,
        seconds: int = 20,
    ) -> None:
        time_elapsed = 0.0
        has_stopped = process.poll() is not None if process else False
        current_status = status_fn(str(jobstore))
        while current_status != status:
            if has_stopped:
                # If the process has stopped and the stratus is wrong, it will never be right.
                assert process is not None
                assert (
                    current_status == status
                ), f"Process returned {process.returncode} without status reaching {status}; stuck at {current_status}"
            logger.debug(
                "Workflow is %s; waiting for %s (%s/%s elapsed)",
                current_status,
                status,
                time_elapsed,
                seconds,
            )
            time.sleep(0.5)
            time_elapsed += 0.5
            has_stopped = process.poll() is not None if process else False
            current_status = status_fn(str(jobstore))
            if time_elapsed > seconds:
                assert (
                    current_status == status
                ), "Waited {seconds} seconds without status reaching {status}; stuck at {current_status}"

    def testGetPIDStatus(self, tmp_path: Path, unsortedFile: Path) -> None:
        """Test that ToilStatus.getPIDStatus() behaves as expected."""
        jobstore = tmp_path / "jobstore"
        outputFile = tmp_path / "someSortedStuff.txt"
        wf = subprocess.Popen(
            [
                python,
                "-m",
                "toil.test.sort.sort",
                jobstore.as_uri(),
                f"--fileToSort={unsortedFile}",
                f"--outputFile={outputFile}",
                "--clean=never",
            ]
        )
        self.check_status(
            jobstore,
            "RUNNING",
            status_fn=ToilStatus.getPIDStatus,
            process=wf,
            seconds=60,
        )
        wf.wait()
        self.check_status(
            jobstore,
            "COMPLETED",
            status_fn=ToilStatus.getPIDStatus,
            process=wf,
            seconds=60,
        )

        # TODO: we need to reach into the FileJobStore's files and delete this
        #  shared file. We assume we know its internal layout.
        os.remove(jobstore / "files/shared/pid.log")
        self.check_status(
            jobstore,
            "QUEUED",
            status_fn=ToilStatus.getPIDStatus,
            process=wf,
            seconds=60,
        )

    def testGetStatusFailedToilWF(self, tmp_path: Path, unsortedFile: Path) -> None:
        """
        Test that ToilStatus.getStatus() behaves as expected with a failing Toil workflow.
        While this workflow could be called by importing and evoking its main function, doing so would remove the
        opportunity to test the 'RUNNING' functionality of getStatus().
        """
        # --badWorker is set to force failure.
        jobstore = tmp_path / "jobstore"
        outputFile = tmp_path / "someSortedStuff.txt"
        wf = subprocess.Popen(
            [
                python,
                "-m",
                "toil.test.sort.sort",
                jobstore.as_uri(),
                f"--fileToSort={unsortedFile}",
                f"--outputFile={outputFile}",
                "--clean=never",
                "--badWorker=1",
            ]
        )
        self.check_status(
            jobstore, "RUNNING", status_fn=ToilStatus.getStatus, process=wf, seconds=60
        )
        wf.wait()
        self.check_status(
            jobstore, "ERROR", status_fn=ToilStatus.getStatus, process=wf, seconds=60
        )

    @needs_cwl
    @needs_docker
    @pytest.mark.cwl
    @pytest.mark.docker
    @pytest.mark.online
    def testGetStatusFailedCWLWF(self, tmp_path: Path) -> None:
        """Test that ToilStatus.getStatus() behaves as expected with a failing CWL workflow."""
        jobstore = tmp_path / "jobstore"
        with get_data("test/cwl/sorttool.cwl") as cwl_file:
            with get_data("test/cwl/whale.txt") as input_file:
                # --badWorker is set to force failure.
                cmd = [
                    "toil-cwl-runner",
                    "--logDebug",
                    "--jobStore",
                    str(jobstore),
                    "--clean=never",
                    "--badWorker=1",
                    str(cwl_file),
                    "--reverse",
                    "--input",
                    str(input_file),
                    f"--outdir={tmp_path}",
                ]
                logger.info("Run command: %s", " ".join(cmd))
                wf = subprocess.Popen(cmd)
                self.check_status(
                    jobstore,
                    "RUNNING",
                    status_fn=ToilStatus.getStatus,
                    process=wf,
                    seconds=60,
                )
                wf.wait()
                self.check_status(
                    jobstore,
                    "ERROR",
                    status_fn=ToilStatus.getStatus,
                    process=wf,
                    seconds=60,
                )

    @needs_cwl
    @needs_docker
    @pytest.mark.cwl
    @pytest.mark.docker
    @pytest.mark.online
    def testGetStatusSuccessfulCWLWF(self, tmp_path: Path) -> None:
        """Test that ToilStatus.getStatus() behaves as expected with a successful CWL workflow."""
        jobstore = tmp_path / "jobstore"
        with get_data("test/cwl/sorttool.cwl") as cwl_file:
            with get_data("test/cwl/whale.txt") as input_file:
                cmd = [
                    "toil-cwl-runner",
                    "--jobStore",
                    str(jobstore),
                    "--clean=never",
                    str(cwl_file),
                    "--reverse",
                    "--input",
                    str(input_file),
                    f"--outdir={tmp_path}",
                ]
                wf = subprocess.Popen(cmd)
                self.check_status(
                    jobstore,
                    "RUNNING",
                    status_fn=ToilStatus.getStatus,
                    process=wf,
                    seconds=60,
                )
                wf.wait()
                self.check_status(
                    jobstore,
                    "COMPLETED",
                    status_fn=ToilStatus.getStatus,
                    process=wf,
                    seconds=60,
                )

    @needs_cwl
    @pytest.mark.cwl
    def testPrintJobLog(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that ToilStatus.printJobLog() reads the log from a failed command without error."""
        jobstore = tmp_path / "jobstore"
        print_args: list[str] = []

        def fake_print(*args: Any, **kwargs: Any) -> None:
            print_args.extend(args)

        # Run a workflow that will always fail
        with get_data("test/cwl/alwaysfails.cwl") as cwl_file:
            cmd = [
                "toil-cwl-runner",
                "--logDebug",
                "--jobStore",
                str(jobstore),
                "--clean=never",
                f"--outdir={tmp_path/'outdir'}",
                str(cwl_file),
                "--message",
                "Testing",
            ]
            logger.info("Run command: %s", " ".join(cmd))
            wf = subprocess.Popen(cmd)
            wf.wait()
            # print log and check output
            status = ToilStatus(str(jobstore))
            with monkeypatch.context() as m:
                m.setattr(builtins, "print", fake_print)
                status.printJobLog()

        # Make sure it printed some kind of complaint about the missing command.
        assert "invalidcommand" in print_args[0]

    @pytest.mark.timeout(1200)
    def testRestartAttribute(self, tmp_path: Path) -> None:
        """
        Test that the job store is only destroyed when we observe a successful workflow run.
        The following simulates a failing workflow that attempts to resume without restart().
        In this case, the job store should not be destroyed until restart() is called.
        """
        jobstore = tmp_path / "jobstore"
        # Run a workflow that will always fail
        cmd = [
            python,
            "-m",
            "toil.test.sort.restart_sort",
            jobstore.as_uri(),
            "--badWorker=1",
            "--logDebug",
        ]
        subprocess.run(cmd)

        restart_cmd = [
            python,
            "-m",
            "toil.test.sort.restart_sort",
            jobstore.as_uri(),
            "--badWorker=0",
            "--logDebug",
            "--restart",
        ]
        subprocess.run(restart_cmd)

        # Check the job store exists after restart attempt
        assert jobstore.exists()

        successful_cmd = [
            python,
            "-m",
            "toil.test.sort.sort",
            "--logDebug",
            jobstore.as_uri(),
            "--restart",
        ]
        subprocess.run(successful_cmd)

        # Check the job store is destroyed after calling restart()
        assert not jobstore.exists()


def printUnicodeCharacter() -> None:
    # We want to get a unicode character to stdout but we can't print it directly because of
    # Python encoding issues. To work around this we print in a separate Python process. See
    # http://stackoverflow.com/questions/492483/setting-the-correct-encoding-when-piping-stdout-in-python
    subprocess.check_call([sys.executable, "-c", "print('\\xc3\\xbc')"])


class RunTwoJobsPerWorker(Job):
    """
    Runs child job with same resources as self in an attempt to chain the jobs on the same worker
    """

    def run(self, fileStore: AbstractFileStore) -> None:
        self.addChildFn(printUnicodeCharacter)
