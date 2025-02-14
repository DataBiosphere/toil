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
import re
import shutil
import subprocess
import sys
import time
import uuid
from unittest.mock import patch

import pytest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import toil
from toil import resolveEntryPoint
from toil.common import Config, Toil
from toil.job import Job
from toil.lib.bioio import system
from toil.test import (
    ToilTest,
    get_data,
    get_temp_file,
    integrative,
    needs_aws_ec2,
    needs_cwl,
    needs_docker,
    needs_rsync3,
    slow,
)
from toil.test.sort.sortTest import makeFileToSort
from toil.utils.toilStats import get_stats, process_data
from toil.utils.toilStatus import ToilStatus
from toil.version import python

logger = logging.getLogger(__name__)


class UtilsTest(ToilTest):
    """
    Tests the utilities that toil ships with, e.g. stats and status, in conjunction with restart
    functionality.
    """

    def setUp(self):
        super().setUp()
        self.tempDir = self._createTempDir()
        self.tempFile = get_temp_file(rootDir=self.tempDir)
        self.outputFile = get_temp_file(rootDir=self.tempDir)
        self.outputFile = "someSortedStuff.txt"
        self.toilDir = os.path.join(self.tempDir, "jobstore")
        self.assertFalse(os.path.exists(self.toilDir))
        self.lines = 1000
        self.lineLen = 10
        self.N = 1000
        makeFileToSort(self.tempFile, self.lines, self.lineLen)
        # First make our own sorted version
        with open(self.tempFile) as fileHandle:
            self.correctSort = fileHandle.readlines()
            self.correctSort.sort()

        self.sort_workflow_cmd = [
            python,
            "-m",
            "toil.test.sort.sort",
            f"file:{self.toilDir}",
            f"--fileToSort={self.tempFile}",
            f"--outputFile={self.outputFile}",
            "--clean=never",
        ]

        self.restart_sort_workflow_cmd = [
            python,
            "-m",
            "toil.test.sort.restart_sort",
            f"file:{self.toilDir}",
        ]

    def tearDown(self):
        if os.path.exists(self.tempDir):
            shutil.rmtree(self.tempDir)
        if os.path.exists(self.toilDir):
            shutil.rmtree(self.toilDir)

        for f in [
            self.tempFile,
            self.outputFile,
            os.path.join(self.tempDir, "output.txt"),
        ]:
            if os.path.exists(f):
                os.remove(f)

        ToilTest.tearDown(self)

    @property
    def toilMain(self):
        return resolveEntryPoint("toil")

    @property
    def cleanCommand(self):
        return [self.toilMain, "clean", self.toilDir]

    @property
    def statsCommand(self):
        return [self.toilMain, "stats", self.toilDir, "--pretty"]

    def statusCommand(self, failIfNotComplete=False):
        commandTokens = [self.toilMain, "status", self.toilDir]
        if failIfNotComplete:
            commandTokens.append("--failIfNotComplete")
        return commandTokens

    def test_config_functionality(self):
        """Ensure that creating and reading back the config file works"""
        config_file = os.path.abspath("config.yaml")
        config_command = [self.toilMain, "config", config_file]
        # make sure the command `toil config file_path` works
        try:
            subprocess.check_call(config_command)
        except subprocess.CalledProcessError:
            self.fail("The toil config utility failed!")

        parser = Job.Runner.getDefaultArgumentParser()
        # make sure that toil can read from the generated config file
        try:
            parser.parse_args(["random_jobstore", "--config", config_file])
            with open(config_file) as cm:
                payload = cm.read()
                expected = "workDir batchSystem symlinkImports defaultMemory retryCount"
                assert all(
                    re.search(rf"^#*{ param }:", payload, re.MULTILINE)
                    for param in expected.split(" ")
                ), f"Generated config contains { expected }"
        except SystemExit:
            self.fail("Failed to parse the default generated config file!")
        finally:
            os.remove(config_file)

    @needs_rsync3
    @pytest.mark.timeout(1200)
    @needs_aws_ec2
    @integrative
    @slow
    def testAWSProvisionerUtils(self):
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
        keyName = os.getenv("TOIL_AWS_KEYNAME").strip() or "id_rsa"
        expected_owner = os.getenv("TOIL_OWNER_TAG") or keyName

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
                self.assertEqual(leader.tags.get(key), tags[key])
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
    def testUtilsSort(self):
        """
        Tests the status and stats commands of the toil command line utility using the
        sort example with the --restart flag.
        """
        # Get the sort command to run
        toilCommand = [
            sys.executable,
            "-m",
            toil.test.sort.sort.__name__,
            self.toilDir,
            "--logLevel=DEBUG",
            "--fileToSort",
            self.tempFile,
            "--outputFile",
            self.outputFile,
            "--N",
            str(self.N),
            "--stats",
            "--retryCount=2",
            "--badWorker=0.5",
            "--badWorkerFailInterval=0.05",
        ]
        # Try restarting it to check that a JobStoreException is thrown
        self.assertRaises(
            subprocess.CalledProcessError, system, toilCommand + ["--restart"]
        )
        # Check that trying to run it in restart mode does not create the jobStore
        self.assertFalse(os.path.exists(self.toilDir))

        # Status command
        # Run the script for the first time
        try:
            system(toilCommand)
            finished = True
        except (
            subprocess.CalledProcessError
        ):  # This happens when the script fails due to having unfinished jobs
            system(self.statusCommand())
            self.assertRaises(
                subprocess.CalledProcessError,
                system,
                self.statusCommand(failIfNotComplete=True),
            )
            finished = False
        self.assertTrue(os.path.exists(self.toilDir))

        # Try running it without restart and check an exception is thrown
        self.assertRaises(subprocess.CalledProcessError, system, toilCommand)

        # Now restart it until done
        totalTrys = 1
        while not finished:
            try:
                system(toilCommand + ["--restart"])
                finished = True
            except (
                subprocess.CalledProcessError
            ):  # This happens when the script fails due to having unfinished jobs
                system(self.statusCommand())
                self.assertRaises(
                    subprocess.CalledProcessError,
                    system,
                    self.statusCommand(failIfNotComplete=True),
                )
                if totalTrys > 16:
                    self.fail()  # Exceeded a reasonable number of restarts
                totalTrys += 1

        # Check the toil status command does not issue an exception
        system(self.statusCommand())

        # Check we can run 'toil stats'
        system(self.statsCommand)

        # Check the file is properly sorted
        with open(self.outputFile) as fileHandle:
            l2 = fileHandle.readlines()
            self.assertEqual(self.correctSort, l2)

        # Delete output file before next step
        os.remove(self.outputFile)

        # Check we can run 'toil clean'
        system(self.cleanCommand)

    @slow
    def testUtilsStatsSort(self):
        """
        Tests the stats commands on a complete run of the stats test.
        """
        # Get the sort command to run
        toilCommand = [
            sys.executable,
            "-m",
            toil.test.sort.sort.__name__,
            self.toilDir,
            "--logLevel=DEBUG",
            "--fileToSort",
            self.tempFile,
            "--outputFile",
            self.outputFile,
            "--N",
            str(self.N),
            "--stats",
            "--retryCount=99",
            "--badWorker=0.5",
            "--badWorkerFailInterval=0.01",
        ]

        # Run the script for the first time
        system(toilCommand)
        self.assertTrue(os.path.exists(self.toilDir))

        # Check we can run 'toil stats'
        system(self.statsCommand)

        # Check the file is properly sorted
        with open(self.outputFile) as fileHandle:
            l2 = fileHandle.readlines()
            self.assertEqual(self.correctSort, l2)

        # Delete output file
        os.remove(self.outputFile)

    def testUnicodeSupport(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.clean = "always"
        options.logLevel = "debug"
        Job.Runner.startToil(Job.wrapFn(printUnicodeCharacter), options)

    @slow
    def testMultipleJobsPerWorkerStats(self):
        """
        Tests case where multiple jobs are run on 1 worker to ensure that all jobs report back their data
        """
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.clean = "never"
        options.stats = True
        Job.Runner.startToil(RunTwoJobsPerWorker(), options)
        config = Config()
        config.setOptions(options)
        jobStore = Toil.resumeJobStore(config.jobStore)
        stats = get_stats(jobStore)
        collatedStats = process_data(jobStore.config, stats)
        self.assertTrue(
            len(collatedStats.job_types) == 2,
            "Some jobs are not represented in the stats.",
        )

    def check_status(self, status, status_fn, process=None, seconds=20):
        time_elapsed = 0.0
        has_stopped = process.poll() is not None if process else False
        current_status = status_fn(self.toilDir)
        while current_status != status:
            if has_stopped:
                # If the process has stopped and the stratus is wrong, it will never be right.
                self.assertEqual(
                    current_status,
                    status,
                    f"Process returned {process.returncode} without status reaching {status}; stuck at {current_status}",
                )
            logger.debug(
                "Workflow is %s; waiting for %s (%s/%s elapsed)",
                current_status,
                status,
                time_elapsed,
                seconds
            )
            time.sleep(0.5)
            time_elapsed += 0.5
            has_stopped = process.poll() is not None if process else False
            current_status = status_fn(self.toilDir)
            if time_elapsed > seconds:
                self.assertEqual(
                    current_status,
                    status,
                    f"Waited {seconds} seconds without status reaching {status}; stuck at {current_status}",
                )

    def testGetPIDStatus(self):
        """Test that ToilStatus.getPIDStatus() behaves as expected."""
        wf = subprocess.Popen(self.sort_workflow_cmd)
        self.check_status("RUNNING", status_fn=ToilStatus.getPIDStatus, process=wf, seconds=60)
        wf.wait()
        self.check_status("COMPLETED", status_fn=ToilStatus.getPIDStatus, process=wf, seconds=60)

        # TODO: we need to reach into the FileJobStore's files and delete this
        #  shared file. We assume we know its internal layout.
        os.remove(os.path.join(self.toilDir, "files/shared/pid.log"))
        self.check_status("QUEUED", status_fn=ToilStatus.getPIDStatus, process=wf, seconds=60)

    def testGetStatusFailedToilWF(self):
        """
        Test that ToilStatus.getStatus() behaves as expected with a failing Toil workflow.
        While this workflow could be called by importing and evoking its main function, doing so would remove the
        opportunity to test the 'RUNNING' functionality of getStatus().
        """
        # --badWorker is set to force failure.
        wf = subprocess.Popen(self.sort_workflow_cmd + ["--badWorker=1"])
        self.check_status("RUNNING", status_fn=ToilStatus.getStatus, process=wf, seconds=60)
        wf.wait()
        self.check_status("ERROR", status_fn=ToilStatus.getStatus, process=wf, seconds=60)

    @needs_cwl
    @needs_docker
    def testGetStatusFailedCWLWF(self):
        """Test that ToilStatus.getStatus() behaves as expected with a failing CWL workflow."""
        # --badWorker is set to force failure.
        cmd = [
            "toil-cwl-runner",
            "--logDebug",
            "--jobStore",
            self.toilDir,
            "--clean=never",
            "--badWorker=1",
            get_data("test/cwl/sorttool.cwl"),
            "--reverse",
            "--input",
            get_data("test/cwl/whale.txt"),
            f"--outdir={self.tempDir}",
        ]
        logger.info("Run command: %s", " ".join(cmd))
        wf = subprocess.Popen(cmd)
        self.check_status("RUNNING", status_fn=ToilStatus.getStatus, process=wf, seconds=60)
        wf.wait()
        self.check_status("ERROR", status_fn=ToilStatus.getStatus, process=wf, seconds=60)

    @needs_cwl
    @needs_docker
    def testGetStatusSuccessfulCWLWF(self):
        """Test that ToilStatus.getStatus() behaves as expected with a successful CWL workflow."""
        cmd = [
            "toil-cwl-runner",
            "--jobStore",
            self.toilDir,
            "--clean=never",
            get_data("test/cwl/sorttool.cwl"),
            "--reverse",
            "--input",
            get_data("test/cwl/whale.txt"),
            f"--outdir={self.tempDir}",
        ]
        wf = subprocess.Popen(cmd)
        self.check_status("RUNNING", status_fn=ToilStatus.getStatus, process=wf, seconds=60)
        wf.wait()
        self.check_status("COMPLETED", status_fn=ToilStatus.getStatus, process=wf, seconds=60)

    @needs_cwl
    @patch("builtins.print")
    def testPrintJobLog(self, mock_print):
        """Test that ToilStatus.printJobLog() reads the log from a failed command without error."""
        # Run a workflow that will always fail
        cmd = [
            "toil-cwl-runner",
            "--logDebug",
            "--jobStore",
            self.toilDir,
            "--clean=never",
            get_data("test/cwl/alwaysfails.cwl"),
            "--message",
            "Testing",
        ]
        logger.info("Run command: %s", " ".join(cmd))
        wf = subprocess.Popen(cmd)
        wf.wait()
        # print log and check output
        status = ToilStatus(self.toilDir)
        status.printJobLog()

        # Make sure it printed some kind of complaint about the missing command.
        args, kwargs = mock_print.call_args
        self.assertIn("invalidcommand", args[0])

    @pytest.mark.timeout(1200)
    def testRestartAttribute(self):
        """
        Test that the job store is only destroyed when we observe a successful workflow run.
        The following simulates a failing workflow that attempts to resume without restart().
        In this case, the job store should not be destroyed until restart() is called.
        """
        # Run a workflow that will always fail
        cmd = self.restart_sort_workflow_cmd + ["--badWorker=1", "--logDebug"]
        subprocess.run(cmd)

        restart_cmd = self.restart_sort_workflow_cmd + [
            "--badWorker=0",
            "--logDebug",
            "--restart",
        ]
        subprocess.run(restart_cmd)

        # Check the job store exists after restart attempt
        self.assertTrue(os.path.exists(self.toilDir))

        successful_cmd = [
            python,
            "-m",
            "toil.test.sort.sort",
            "--logDebug",
            "file:" + self.toilDir,
            "--restart",
        ]
        subprocess.run(successful_cmd)

        # Check the job store is destroyed after calling restart()
        self.assertFalse(os.path.exists(self.toilDir))


def printUnicodeCharacter():
    # We want to get a unicode character to stdout but we can't print it directly because of
    # Python encoding issues. To work around this we print in a separate Python process. See
    # http://stackoverflow.com/questions/492483/setting-the-correct-encoding-when-piping-stdout-in-python
    subprocess.check_call([sys.executable, "-c", "print('\\xc3\\xbc')"])


class RunTwoJobsPerWorker(Job):
    """
    Runs child job with same resources as self in an attempt to chain the jobs on the same worker
    """

    def __init__(self):
        Job.__init__(self)

    def run(self, fileStore):
        self.addChildFn(printUnicodeCharacter)
