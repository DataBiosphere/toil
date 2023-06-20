# Copyright (C) 2015-2021 Regents of the University of California
# Copyright (C) 2015 Curoverse, Inc
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
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import unittest
import uuid
import zipfile
from functools import partial
from io import StringIO
from pathlib import Path
from typing import Dict, List, MutableMapping, Optional, Union
from unittest.mock import Mock, call
from urllib.request import urlretrieve

import pytest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from toil.cwl.utils import (download_structure,
                            visit_cwl_class_and_reduce,
                            visit_top_cwl_class)
from toil.exceptions import FailedJobsException
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.lib.aws import zone_to_region
from toil.lib.threading import cpu_count
from toil.provisioners import cluster_factory
from toil.provisioners.aws import get_best_aws_zone
from toil.test import (ToilTest,
                       needs_aws_ec2,
                       needs_aws_s3,
                       needs_cwl,
                       needs_docker,
                       needs_docker_cuda,
                       needs_env_var,
                       needs_fetchable_appliance,
                       needs_gridengine,
                       needs_kubernetes,
                       needs_local_cuda,
                       needs_lsf,
                       needs_mesos,
                       needs_parasol,
                       needs_slurm,
                       needs_torque,
                       needs_wes_server,
                       slow)
from toil.test.provisioners.aws.awsProvisionerTest import \
    AbstractAWSAutoscaleTest
from toil.test.provisioners.clusterTest import AbstractClusterTest

log = logging.getLogger(__name__)
CONFORMANCE_TEST_TIMEOUT = 3600


def run_conformance_tests(
    workDir: str,
    yml: str,
    runner: Optional[str] = None,
    caching: bool = False,
    batchSystem: str = None,
    selected_tests: str = None,
    selected_tags: str = None,
    skipped_tests: str = None,
    extra_args: Optional[List[str]] = None,
    must_support_all_features: bool = False,
    junit_file: Optional[str] = None,
):
    """
    Run the CWL conformance tests.

    :param workDir: Directory to run tests in.

    :param yml: CWL test list YML to run tests from.

    :param runner: If set, use this cwl runner instead of the default toil-cwl-runner.

    :param caching: If True, use Toil file store caching.

    :param batchSystem: If set, use this batch system instead of the default single_machine.

    :param selected_tests: If set, use this description of test numbers to run (comma-separated numbers or ranges)

    :param selected_tags: As an alternative to selected_tests, run tests with the given tags.

    :param skipped_tests: Comma-separated string labels of tests to skip.

    :param extra_args: Provide these extra arguments to runner for each test.

    :param must_support_all_features: If set, fail if some CWL optional features are unsupported.

    :param junit_file: JUnit XML file to write test info to.
    """
    try:
        if runner is None:
            runner = "toil-cwl-runner"
        cmd = [
            "cwltest",
            f"--tool={runner}",
            f"--test={yml}",
            "--timeout=2400",
            f"--basedir={workDir}",
        ]
        if selected_tests:
            cmd.append(f"-n={selected_tests}")
        if selected_tags:
            cmd.append(f"--tags={selected_tags}")
        if skipped_tests:
            cmd.append(f"-S{skipped_tests}")
        if junit_file:
            # Capture output for JUnit
            cmd.append("--junit-verbose")
            cmd.append(f"--junit-xml={junit_file}")
        else:
            # Otherwise dump all output to our output stream
            cmd.append("--verbose")

        args_passed_directly_to_runner = [
            "--clean=always",
            "--logDebug",
            "--statusWait=10",
            "--retryCount=2",
        ]

        args_passed_directly_to_runner.append(f"--caching={caching}")

        if extra_args:
            args_passed_directly_to_runner += extra_args

        if "SINGULARITY_DOCKER_HUB_MIRROR" in os.environ:
            args_passed_directly_to_runner.append(
                "--setEnv=SINGULARITY_DOCKER_HUB_MIRROR"
            )

        job_store_override = None

        if batchSystem == "kubernetes":
            # Run tests in parallel on Kubernetes.
            # We can throw a bunch at it at once and let Kubernetes schedule.
            # But we still want a local core for each.
            parallel_tests = max(min(cpu_count(), 8), 1)
        else:
            # Run tests in parallel on the local machine. Don't run too many
            # tests at once; we want at least a couple cores for each.
            parallel_tests = max(int(cpu_count() / 2), 1)
        cmd.append(f"-j{parallel_tests}")

        if batchSystem:
            args_passed_directly_to_runner.append(f"--batchSystem={batchSystem}")
        cmd.extend(["--"] + args_passed_directly_to_runner)

        log.info("Running: '%s'", "' '".join(cmd))
        try:
            output = subprocess.check_output(cmd, cwd=workDir, stderr=subprocess.STDOUT)
        finally:
            if job_store_override:
                # Clean up the job store we used for all the tests, if it is still there.
                subprocess.run(["toil", "clean", job_store_override])

    except subprocess.CalledProcessError as e:
        only_unsupported = False
        # check output -- if we failed but only have unsupported features, we're okay
        p = re.compile(
            r"(?P<failures>\d+) failures, (?P<unsupported>\d+) unsupported features"
        )

        error_log = e.output.decode("utf-8")
        for line in error_log.split("\n"):
            m = p.search(line)
            if m:
                if int(m.group("failures")) == 0 and int(m.group("unsupported")) > 0:
                    only_unsupported = True
                    break
        if (not only_unsupported) or must_support_all_features:
            print(error_log)
            raise e


@needs_cwl
class CWLWorkflowTest(ToilTest):
    """
    CWL tests included in Toil that don't involve the whole CWL conformance
    test suite. Tests Toil-specific functions like URL types supported for
    inputs.
    """

    def setUp(self):
        """Runs anew before each test to create farm fresh temp dirs."""
        self.outDir = f"/tmp/toil-cwl-test-{str(uuid.uuid4())}"
        os.makedirs(self.outDir)
        self.rootDir = self._projectRootPath()

    def tearDown(self):
        """Clean up outputs."""
        if os.path.exists(self.outDir):
            shutil.rmtree(self.outDir)
        unittest.TestCase.tearDown(self)

    def _tester(self, cwlfile, jobfile, expect, main_args=[], out_name="output"):
        from toil.cwl import cwltoil

        st = StringIO()
        main_args = main_args[:]
        main_args.extend(
            [
                "--outdir",
                self.outDir,
                os.path.join(self.rootDir, cwlfile),
                os.path.join(self.rootDir, jobfile),
            ]
        )
        cwltoil.main(main_args, stdout=st)
        out = json.loads(st.getvalue())
        out.get(out_name, {}).pop("http://commonwl.org/cwltool#generation", None)
        out.get(out_name, {}).pop("nameext", None)
        out.get(out_name, {}).pop("nameroot", None)
        self.assertEqual(out, expect)

    def _debug_worker_tester(self, cwlfile, jobfile, expect):
        from toil.cwl import cwltoil

        st = StringIO()
        cwltoil.main(
            [
                "--debugWorker",
                "--outdir",
                self.outDir,
                os.path.join(self.rootDir, cwlfile),
                os.path.join(self.rootDir, jobfile),
            ],
            stdout=st,
        )
        out = json.loads(st.getvalue())
        out["output"].pop("http://commonwl.org/cwltool#generation", None)
        out["output"].pop("nameext", None)
        out["output"].pop("nameroot", None)
        self.assertEqual(out, expect)

    def revsort(self, cwl_filename, tester_fn):
        tester_fn(
            "src/toil/test/cwl/" + cwl_filename,
            "src/toil/test/cwl/revsort-job.json",
            self._expected_revsort_output(self.outDir),
        )

    def revsort_no_checksum(self, cwl_filename, tester_fn):
        tester_fn(
            "src/toil/test/cwl/" + cwl_filename,
            "src/toil/test/cwl/revsort-job.json",
            self._expected_revsort_nochecksum_output(self.outDir),
        )

    def download(self, inputs, tester_fn):
        input_location = os.path.join("src/toil/test/cwl", inputs)
        tester_fn(
            "src/toil/test/cwl/download.cwl",
            input_location,
            self._expected_download_output(self.outDir),
        )

    def load_contents(self, inputs, tester_fn):
        input_location = os.path.join("src/toil/test/cwl", inputs)
        tester_fn(
            "src/toil/test/cwl/load_contents.cwl",
            input_location,
            self._expected_load_contents_output(self.outDir),
        )

    def download_directory(self, inputs, tester_fn):
        input_location = os.path.join("src/toil/test/cwl", inputs)
        tester_fn(
            "src/toil/test/cwl/download_directory.cwl",
            input_location,
            self._expected_download_output(self.outDir),
        )

    def download_subdirectory(self, inputs, tester_fn):
        input_location = os.path.join("src/toil/test/cwl", inputs)
        tester_fn(
            "src/toil/test/cwl/download_subdirectory.cwl",
            input_location,
            self._expected_download_output(self.outDir),
        )

    def test_mpi(self):
        from toil.cwl import cwltoil

        stdout = StringIO()
        main_args = [
            "--outdir",
            self.outDir,
            "--enable-dev",
            "--enable-ext",
            "--mpi-config-file",
            os.path.join(self.rootDir, "src/toil/test/cwl/mock_mpi/fake_mpi.yml"),
            os.path.join(self.rootDir, "src/toil/test/cwl/mpi_simple.cwl"),
        ]
        path = os.environ["PATH"]
        os.environ["PATH"] = f"{path}:{self.rootDir}/src/toil/test/cwl/mock_mpi/"
        cwltoil.main(main_args, stdout=stdout)
        os.environ["PATH"] = path
        out = json.loads(stdout.getvalue())
        with open(out.get("pids", {}).get("location")[len("file://") :]) as f:
            two_pids = [int(i) for i in f.read().split()]
        self.assertEqual(len(two_pids), 2)
        self.assertTrue(isinstance(two_pids[0], int))
        self.assertTrue(isinstance(two_pids[1], int))

    @needs_aws_s3
    def test_s3_as_secondary_file(self):
        from toil.cwl import cwltoil

        stdout = StringIO()
        main_args = [
            "--outdir",
            self.outDir,
            os.path.join(self.rootDir, "src/toil/test/cwl/s3_secondary_file.cwl"),
            os.path.join(self.rootDir, "src/toil/test/cwl/s3_secondary_file.json"),
        ]
        cwltoil.main(main_args, stdout=stdout)
        out = json.loads(stdout.getvalue())
        self.assertEqual(
            out["output"]["checksum"], "sha1$d14dd02e354918b4776b941d154c18ebc15b9b38"
        )
        self.assertEqual(out["output"]["size"], 24)
        with open(out["output"]["location"][len("file://") :]) as f:
            self.assertEqual(f.read().strip(), "When is s4 coming out?")

    def test_run_revsort(self):
        self.revsort("revsort.cwl", self._tester)

    def test_run_revsort_nochecksum(self):
        self.revsort_no_checksum(
            "revsort.cwl", partial(self._tester, main_args=["--no-compute-checksum"])
        )

    def test_run_revsort2(self):
        self.revsort("revsort2.cwl", self._tester)

    def test_run_revsort_debug_worker(self):
        self.revsort("revsort.cwl", self._debug_worker_tester)

    def test_run_colon_output(self):
        self._tester(
            "src/toil/test/cwl/colon_test_output.cwl",
            "src/toil/test/cwl/colon_test_output_job.yaml",
            self._expected_colon_output(self.outDir),
            out_name="result",
        )

    @needs_aws_s3
    def test_download_s3(self):
        self.download("download_s3.json", self._tester)

    def test_download_http(self):
        self.download("download_http.json", self._tester)

    def test_download_https(self):
        self.download("download_https.json", self._tester)

    def test_download_file(self):
        self.download("download_file.json", self._tester)

    @needs_aws_s3
    def test_download_directory_s3(self):
        self.download_directory("download_directory_s3.json", self._tester)

    def test_download_directory_file(self):
        self.download_directory("download_directory_file.json", self._tester)

    @needs_aws_s3
    def test_download_subdirectory_s3(self):
        self.download_subdirectory("download_subdirectory_s3.json", self._tester)

    def test_download_subdirectory_file(self):
        self.download_subdirectory("download_subdirectory_file.json", self._tester)

    # We also want to make sure we can run a bare tool with loadContents on the inputs, which requires accessing the input data early in the leader.

    @needs_aws_s3
    def test_load_contents_s3(self):
        self.load_contents("download_s3.json", self._tester)

    def test_load_contents_http(self):
        self.load_contents("download_http.json", self._tester)

    def test_load_contents_https(self):
        self.load_contents("download_https.json", self._tester)

    def test_load_contents_file(self):
        self.load_contents("download_file.json", self._tester)

    @slow
    def test_bioconda(self):
        self._tester(
            "src/toil/test/cwl/seqtk_seq.cwl",
            "src/toil/test/cwl/seqtk_seq_job.json",
            self._expected_seqtk_output(self.outDir),
            main_args=["--beta-conda-dependencies"],
            out_name="output1",
        )

    @needs_docker
    def test_biocontainers(self):
        self._tester(
            "src/toil/test/cwl/seqtk_seq.cwl",
            "src/toil/test/cwl/seqtk_seq_job.json",
            self._expected_seqtk_output(self.outDir),
            main_args=["--beta-use-biocontainers"],
            out_name="output1",
        )

    @needs_docker
    @needs_docker_cuda
    @needs_local_cuda
    def test_cuda(self):
        self._tester(
            "src/toil/test/cwl/nvidia_smi.cwl",
            "src/toil/test/cwl/empty.json",
            {},
            out_name="result",
        )

    @slow
    def test_restart(self):
        """
        Enable restarts with toil-cwl-runner -- run failing test, re-run correct test.
        Only implemented for single machine.
        """
        log.info("Running CWL Test Restart.  Expecting failure, then success.")
        from toil.cwl import cwltoil
        from toil.jobStores.abstractJobStore import NoSuchJobStoreException

        outDir = self._createTempDir()
        cwlDir = os.path.join(self._projectRootPath(), "src", "toil", "test", "cwl")
        cmd = [
            "--outdir",
            outDir,
            "--jobStore",
            os.path.join(outDir, "jobStore"),
            "--no-container",
            os.path.join(cwlDir, "revsort.cwl"),
            os.path.join(cwlDir, "revsort-job.json"),
        ]

        # create a fake rev bin that actually points to the "date" binary
        cal_path = [
            d
            for d in os.environ["PATH"].split(":")
            if os.path.exists(os.path.join(d, "date"))
        ][-1]
        os.symlink(os.path.join(cal_path, "date"), f'{os.path.join(outDir, "rev")}')

        def path_with_bogus_rev():
            # append to the front of the PATH so that we check there first
            return f"{outDir}:" + os.environ["PATH"]

        orig_path = os.environ["PATH"]
        # Force a failure by trying to use an incorrect version of `rev` from the PATH
        os.environ["PATH"] = path_with_bogus_rev()
        try:
            cwltoil.main(cmd)
            self.fail("Expected problem job with incorrect PATH did not fail")
        except FailedJobsException:
            pass
        # Finish the job with a correct PATH
        os.environ["PATH"] = orig_path
        cwltoil.main(["--restart"] + cmd)
        # Should fail because previous job completed successfully
        try:
            cwltoil.main(["--restart"] + cmd)
            self.fail("Restart with missing directory did not fail")
        except NoSuchJobStoreException:
            pass

    @needs_aws_s3
    def test_streamable(self):
        """
        Test that a file with 'streamable'=True is a named pipe.
        This is a CWL1.2 feature.
        """
        cwlfile = "src/toil/test/cwl/stream.cwl"
        jobfile = "src/toil/test/cwl/stream.json"
        out_name = "output"
        jobstore = f"--jobStore=aws:us-west-1:toil-stream-{uuid.uuid4()}"
        from toil.cwl import cwltoil

        st = StringIO()
        args = [
            "--outdir",
            self.outDir,
            jobstore,
            os.path.join(self.rootDir, cwlfile),
            os.path.join(self.rootDir, jobfile),
        ]
        cwltoil.main(args, stdout=st)
        out = json.loads(st.getvalue())
        out[out_name].pop("http://commonwl.org/cwltool#generation", None)
        out[out_name].pop("nameext", None)
        out[out_name].pop("nameroot", None)
        self.assertEqual(out, self._expected_streaming_output(self.outDir))
        with open(out[out_name]["location"][len("file://") :]) as f:
            self.assertEqual(f.read().strip(), "When is s4 coming out?")

    @staticmethod
    def _expected_seqtk_output(outDir):
        loc = "file://" + os.path.join(outDir, "out")
        return {
            "output1": {
                "location": loc,
                "checksum": "sha1$322e001e5a99f19abdce9f02ad0f02a17b5066c2",
                "basename": "out",
                "class": "File",
                "size": 150,
            }
        }

    @staticmethod
    def _expected_revsort_output(outDir):
        loc = "file://" + os.path.join(outDir, "output.txt")
        return {
            "output": {
                "location": loc,
                "basename": "output.txt",
                "size": 1111,
                "class": "File",
                "checksum": "sha1$b9214658cc453331b62c2282b772a5c063dbd284",
            }
        }

    @staticmethod
    def _expected_revsort_nochecksum_output(outDir):
        loc = "file://" + os.path.join(outDir, "output.txt")
        return {
            "output": {
                "location": loc,
                "basename": "output.txt",
                "size": 1111,
                "class": "File",
            }
        }

    @staticmethod
    def _expected_download_output(outDir):
        loc = "file://" + os.path.join(outDir, "output.txt")
        return {
            "output": {
                "location": loc,
                "basename": "output.txt",
                "size": 0,
                "class": "File",
                "checksum": "sha1$da39a3ee5e6b4b0d3255bfef95601890afd80709",
            }
        }

    @classmethod
    def _expected_load_contents_output(cls, out_dir):
        """
        Generate the putput we expect from load_contents.cwl, when sending
        output files to the given directory.
        """
        expected = cls._expected_download_output(out_dir)
        expected["length"] = 146
        return expected

    @staticmethod
    def _expected_colon_output(outDir):
        loc = "file://" + os.path.join(outDir, "A%3AGln2Cys_result")
        return {
            "result": {
                "location": loc,
                "basename": "A:Gln2Cys_result",
                "class": "Directory",
                "listing": [
                    {
                        "class": "File",
                        "location": f"{loc}/whale.txt",
                        "basename": "whale.txt",
                        "checksum": "sha1$327fc7aedf4f6b69a42a7c8b808dc5a7aff61376",
                        "size": 1111,
                        "nameroot": "whale",
                        "nameext": ".txt",
                    }
                ],
            }
        }

    def _expected_streaming_output(self, outDir):
        loc = "file://" + os.path.join(outDir, "output.txt")
        return {
            "output": {
                "location": loc,
                "basename": "output.txt",
                "size": 24,
                "class": "File",
                "checksum": "sha1$d14dd02e354918b4776b941d154c18ebc15b9b38",
            }
        }


@needs_cwl
class CWLv10Test(ToilTest):
    """
    Run the CWL 1.0 conformance tests in various environments.
    """

    def setUp(self):
        """Runs anew before each test to create farm fresh temp dirs."""
        self.outDir = f"/tmp/toil-cwl-test-{str(uuid.uuid4())}"
        os.makedirs(self.outDir)
        self.rootDir = self._projectRootPath()
        self.cwlSpec = os.path.join(self.rootDir, "src/toil/test/cwl/spec")
        self.workDir = os.path.join(self.cwlSpec, "v1.0")
        # The latest cwl git commit hash from https://github.com/common-workflow-language/common-workflow-language.
        # Update it to get the latest tests.
        testhash = "6a955874ade22080b8ef962b4e0d6e408112c1ef"  # Date:   Tue Dec 16 2020 8:43pm PST
        url = (
            "https://github.com/common-workflow-language/common-workflow-language/archive/%s.zip"
            % testhash
        )
        if not os.path.exists(self.cwlSpec):
            urlretrieve(url, "spec.zip")
            with zipfile.ZipFile("spec.zip", "r") as z:
                z.extractall()
            shutil.move("common-workflow-language-%s" % testhash, self.cwlSpec)
            os.remove("spec.zip")

    def tearDown(self):
        """Clean up outputs."""
        if os.path.exists(self.outDir):
            shutil.rmtree(self.outDir)
        unittest.TestCase.tearDown(self)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_caching(self):
        self.test_run_conformance(caching=True)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance(
        self, batchSystem=None, caching=False, selected_tests=None
    ):
        run_conformance_tests(
            workDir=self.workDir,
            yml="conformance_test_v1.0.yaml",
            caching=caching,
            batchSystem=batchSystem,
            selected_tests=selected_tests,
        )

    @slow
    @needs_lsf
    @unittest.skip
    def test_lsf_cwl_conformance(self, **kwargs):
        return self.test_run_conformance(batchSystem="lsf", **kwargs)

    @slow
    @needs_slurm
    @unittest.skip
    def test_slurm_cwl_conformance(self, **kwargs):
        return self.test_run_conformance(batchSystem="slurm", **kwargs)

    @slow
    @needs_torque
    @unittest.skip
    def test_torque_cwl_conformance(self, **kwargs):
        return self.test_run_conformance(batchSystem="torque", **kwargs)

    @slow
    @needs_gridengine
    @unittest.skip
    def test_gridengine_cwl_conformance(self, **kwargs):
        return self.test_run_conformance(batchSystem="grid_engine", **kwargs)

    @slow
    @needs_mesos
    @unittest.skip
    def test_mesos_cwl_conformance(self, **kwargs):
        return self.test_run_conformance(batchSystem="mesos", **kwargs)

    @slow
    @needs_parasol
    @unittest.skip
    def test_parasol_cwl_conformance(self, **kwargs):
        return self.test_run_conformance(batchSystem="parasol", **kwargs)

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance(self, **kwargs):
        return self.test_run_conformance(
            batchSystem="kubernetes",
            extra_args=["--retryCount=3"],
            # This test doesn't work with
            # Singularity; see
            # https://github.com/common-workflow-language/cwltool/blob/7094ede917c2d5b16d11f9231fe0c05260b51be6/conformance-test.sh#L99-L117
            skipped_tests="docker_entrypoint",
            **kwargs,
        )

    @slow
    @needs_lsf
    @unittest.skip
    def test_lsf_cwl_conformance_with_caching(self):
        return self.test_lsf_cwl_conformance(caching=True)

    @slow
    @needs_slurm
    @unittest.skip
    def test_slurm_cwl_conformance_with_caching(self):
        return self.test_slurm_cwl_conformance(caching=True)

    @slow
    @needs_torque
    @unittest.skip
    def test_torque_cwl_conformance_with_caching(self):
        return self.test_torque_cwl_conformance(caching=True)

    @slow
    @needs_gridengine
    @unittest.skip
    def test_gridengine_cwl_conformance_with_caching(self):
        return self.test_gridengine_cwl_conformance(caching=True)

    @slow
    @needs_mesos
    @unittest.skip
    def test_mesos_cwl_conformance_with_caching(self):
        return self.test_mesos_cwl_conformance(caching=True)

    @slow
    @needs_parasol
    @unittest.skip
    def test_parasol_cwl_conformance_with_caching(self):
        return self.test_parasol_cwl_conformance(caching=True)

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance_with_caching(self):
        return self.test_kubernetes_cwl_conformance(caching=True)


@needs_cwl
class CWLv11Test(ToilTest):
    """
    Run the CWL 1.1 conformance tests in various environments.
    """

    @classmethod
    def setUpClass(cls):
        """Runs anew before each test."""
        cls.rootDir = cls._projectRootPath()
        cls.cwlSpec = os.path.join(cls.rootDir, "src/toil/test/cwl/spec_v11")
        cls.test_yaml = os.path.join(cls.cwlSpec, "conformance_tests.yaml")
        # TODO: Use a commit zip in case someone decides to rewrite master's history?
        url = "https://github.com/common-workflow-language/cwl-v1.1.git"
        commit = "664835e83eb5e57eee18a04ce7b05fb9d70d77b7"
        p = subprocess.Popen(
            f"git clone {url} {cls.cwlSpec} && cd {cls.cwlSpec} && git checkout {commit}",
            shell=True,
        )
        p.communicate()

    def tearDown(self):
        """Clean up outputs."""
        unittest.TestCase.tearDown(self)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance(self, **kwargs):
        run_conformance_tests(workDir=self.cwlSpec, yml=self.test_yaml, **kwargs)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_caching(self):
        self.test_run_conformance(caching=True)

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance(self, **kwargs):
        return self.test_run_conformance(
            batchSystem="kubernetes",
            extra_args=["--retryCount=3"],
            # These tests don't work with
            # Singularity; see
            # https://github.com/common-workflow-language/cwltool/blob/7094ede917c2d5b16d11f9231fe0c05260b51be6/conformance-test.sh#L99-L117
            skipped_tests="docker_entrypoint,stdin_shorcut",
            **kwargs,
        )

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance_with_caching(self):
        return self.test_kubernetes_cwl_conformance(caching=True)


@needs_cwl
class CWLv12Test(ToilTest):
    """
    Run the CWL 1.2 conformance tests in various environments.
    """

    @classmethod
    def setUpClass(cls):
        """Runs anew before each test."""
        cls.rootDir = cls._projectRootPath()
        cls.cwlSpec = os.path.join(cls.rootDir, "src/toil/test/cwl/spec_v12")
        cls.test_yaml = os.path.join(cls.cwlSpec, "conformance_tests.yaml")
        # TODO: Use a commit zip in case someone decides to rewrite master's history?
        url = "https://github.com/common-workflow-language/cwl-v1.2.git"
        commit = "8c3fd9d9f0209a51c5efacb1c7bc02a1164688d6"
        p = subprocess.Popen(
            f"git clone {url} {cls.cwlSpec} && cd {cls.cwlSpec} && git checkout {commit}",
            shell=True,
        )
        p.communicate()

    def tearDown(self):
        """Clean up outputs."""
        unittest.TestCase.tearDown(self)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance(self, **kwargs):
        run_conformance_tests(workDir=self.cwlSpec, yml=self.test_yaml, **kwargs)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_caching(self):
        self.test_run_conformance(caching=True)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_in_place_update(self):
        """
        Make sure that with --bypass-file-store we properly support in place
        update on a single node, and that this doesn't break any other
        features.
        """
        self.test_run_conformance(
            extra_args=["--bypass-file-store"], must_support_all_features=True
        )

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance(self, **kwargs):
        if "junit_file" not in kwargs:
            kwargs["junit_file"] = os.path.join(
                self.rootDir, "kubernetes-conformance.junit.xml"
            )
        return self.test_run_conformance(
            batchSystem="kubernetes",
            extra_args=["--retryCount=3"],
            # This test doesn't work with
            # Singularity; see
            # https://github.com/common-workflow-language/cwltool/blob/7094ede917c2d5b16d11f9231fe0c05260b51be6/conformance-test.sh#L99-L117
            # and
            # https://github.com/common-workflow-language/cwltool/issues/1441#issuecomment-826747975
            skipped_tests="docker_entrypoint",
            **kwargs,
        )

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance_with_caching(self):
        return self.test_kubernetes_cwl_conformance(
            caching=True,
            junit_file=os.path.join(
                self.rootDir, "kubernetes-caching-conformance.junit.xml"
            ),
        )

    @slow
    @needs_wes_server
    def test_wes_server_cwl_conformance(self):
        """
        Run the CWL conformance tests via WES. TOIL_WES_ENDPOINT must be
        specified. If the WES server requires authentication, set TOIL_WES_USER
        and TOIL_WES_PASSWORD.

        To run manually:

        TOIL_WES_ENDPOINT=http://localhost:8080 \
        TOIL_WES_USER=test \
        TOIL_WES_PASSWORD=password \
        python -m pytest src/toil/test/cwl/cwlTest.py::CWLv12Test::test_wes_server_cwl_conformance -vv --log-level INFO --log-cli-level INFO
        """
        endpoint = os.environ.get("TOIL_WES_ENDPOINT")
        extra_args = [f"--wes_endpoint={endpoint}"]

        # These are the ones that currently fail:
        #   - 310: mixed_version_v10_wf
        #   - 311: mixed_version_v11_wf
        #   - 312: mixed_version_v12_wf

        # Main issues:
        # 1. `cwltool --print-deps` doesn't seem to include secondary files from the default
        #     e.g.: https://github.com/common-workflow-language/cwl-v1.2/blob/1.2.1_proposed/tests/mixed-versions/wf-v10.cwl#L4-L10

        return self.test_run_conformance(
            runner="toil-wes-cwl-runner",
            selected_tests="1-309,313-337",
            extra_args=extra_args,
        )


@needs_aws_ec2
@needs_fetchable_appliance
@slow
class CWLOnARMTest(AbstractClusterTest):
    """
    Run the CWL 1.2 conformance tests on ARM specifically.
    """

    def __init__(self, methodName):
        super().__init__(methodName=methodName)
        self.clusterName = "cwl-test-" + str(uuid.uuid4())
        self.leaderNodeType = "t4g.2xlarge"
        self.clusterType = "kubernetes"
        # We need to be running in a directory which Flatcar and the Toil Appliance both have
        self.cwl_test_dir = "/tmp/toil/cwlTests"

    def setUp(self):
        super().setUp()
        self.jobStore = f"aws:{self.awsRegion()}:cluster-{uuid.uuid4()}"

    @needs_env_var("CI_COMMIT_SHA", "a git commit sha")
    def test_cwl_on_arm(self):
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


@needs_cwl
@pytest.mark.cwl_small_log_dir
def test_workflow_echo_string_scatter_stderr_log_dir(tmp_path: Path):
    log_dir = tmp_path / "cwl-logs"
    job_store = "test_workflow_echo_string_scatter_stderr_log_dir"
    toil = "toil-cwl-runner"
    jobstore = f"--jobStore={job_store}"
    option_1 = "--strict-memory-limit"
    option_2 = "--force-docker-pull"
    option_3 = "--clean=always"
    option_4 = f"--log-dir={log_dir}"
    cwl = os.path.join(
        os.path.dirname(__file__), "echo_string_scatter_capture_stdout.cwl"
    )
    cmd = [toil, jobstore, option_1, option_2, option_3, option_4, cwl]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    outputs = json.loads(stdout)
    out_list = outputs["list_out"]
    assert len(out_list) == 2, f"outList shoud have two file elements {out_list}"
    out_base = outputs["list_out"][0]
    # This is a test on the scatter functionality and stdout.
    # Each value of scatter should generate a separate file in the output.
    for index, file in enumerate(out_list):
        if index > 0:
            new_file_loc = out_base["location"] + f"_{index + 1}"
        else:
            new_file_loc = out_base["location"]
        assert (
            new_file_loc == file["location"]
        ), f"Toil should have detected conflicts for these stdout files {new_file_loc} and {file}"

    assert b"Finished toil run successfully" in stderr
    assert p.returncode == 0

    assert log_dir.exists()
    scatter_0 = log_dir / "echo-test-scatter.0.scatter"
    scatter_1 = log_dir / "echo-test-scatter.1.scatter"
    list_0 = log_dir / "echo-test-scatter.0.list"
    list_1 = log_dir / "echo-test-scatter.1.list"
    assert scatter_0.exists()
    assert scatter_1.exists()
    assert list_0.exists()
    assert list_1.exists()


@needs_cwl
@pytest.mark.cwl_small_log_dir
def test_log_dir_echo_no_output(tmp_path: Path) -> None:
    log_dir = tmp_path / "cwl-logs"
    job_store = "test_log_dir_echo_no_output"
    toil = "toil-cwl-runner"
    jobstore = f"--jobStore={job_store}"
    option_1 = "--strict-memory-limit"
    option_2 = "--force-docker-pull"
    option_3 = "--clean=always"
    option_4 = f"--log-dir={log_dir}"
    cwl = os.path.join(os.path.dirname(__file__), "echo-stdout-log-dir.cwl")
    cmd = [toil, jobstore, option_1, option_2, option_3, option_4, cwl]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    tmp_path = log_dir

    assert log_dir.exists()
    assert len(list(tmp_path.iterdir())) == 1

    subdir = next(tmp_path.iterdir())
    assert subdir.name == "echo"
    assert subdir.is_dir()
    assert len(list(subdir.iterdir())) == 1
    result = next(subdir.iterdir())
    assert result.name == "out.txt"
    output = open(result).read()
    assert "hello" in output


@needs_cwl
@pytest.mark.cwl_small_log_dir
def test_log_dir_echo_stderr(tmp_path: Path) -> None:
    log_dir = tmp_path / "cwl-logs"

    job_store = "test_log_dir_echo_stderr"
    toil = "toil-cwl-runner"
    jobstore = f"--jobStore={job_store}"
    option_1 = "--strict-memory-limit"
    option_2 = "--force-docker-pull"
    option_3 = "--clean=always"
    option_4 = f"--log-dir={log_dir}"
    cwl = os.path.join(os.path.dirname(__file__), "echo-stderr.cwl")
    cmd = [toil, jobstore, option_1, option_2, option_3, option_4, cwl]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    tmp_path = log_dir

    assert len(list(tmp_path.iterdir())) == 1

    subdir = next(tmp_path.iterdir())
    assert subdir.name == "echo-stderr.cwl"
    assert subdir.is_dir()
    assert len(list(subdir.iterdir())) == 1
    result = next(subdir.iterdir())
    assert result.name == "out.txt"
    output = open(result).read()
    assert output == "hello\n"


@needs_cwl
@pytest.mark.cwl_small_log_dir
def test_filename_conflict_resolution(tmp_path: Path):
    out_dir = tmp_path / "cwl-out-dir"
    toil = "toil-cwl-runner"
    options = [
        f"--outdir={out_dir}",
        "--clean=always",
    ]
    cwl = os.path.join(
        os.path.dirname(__file__), "test_filename_conflict_resolution.cwl"
    )
    input = os.path.join(
        os.path.dirname(__file__), "test_filename_conflict_resolution.ms"
    )
    cwl_inputs = ["--msin", input]
    cmd = [toil] + options + [cwl] + cwl_inputs
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    assert b"Finished toil run successfully" in stderr
    assert p.returncode == 0

@needs_cwl
@needs_docker
@pytest.mark.cwl_small_log_dir
def test_filename_conflict_detection(tmp_path: Path):
    """
    Make sure we don't just stage files over each other when using a container.
    """
    out_dir = tmp_path / "cwl-out-dir"
    toil = "toil-cwl-runner"
    options = [
        f"--outdir={out_dir}",
        "--clean=always",
    ]
    cwl = os.path.join(
        os.path.dirname(__file__), "test_filename_conflict_detection.cwl"
    )
    cmd = [toil] + options + [cwl]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    assert b"File staging conflict" in stderr
    assert p.returncode != 0

@needs_cwl
@needs_docker
@pytest.mark.cwl_small_log_dir
def test_filename_conflict_detection_at_root(tmp_path: Path):
    """
    Make sure we don't just stage files over each other.

    Specifically, when using a container and the files are at the root of the work dir.
    """
    out_dir = tmp_path / "cwl-out-dir"
    toil = "toil-cwl-runner"
    options = [
        f"--outdir={out_dir}",
        "--clean=always",
    ]
    cwl = os.path.join(
        os.path.dirname(__file__), "test_filename_conflict_detection_at_root.cwl"
    )
    cmd = [toil] + options + [cwl]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    assert b"File staging conflict" in stderr
    assert p.returncode != 0


@needs_cwl
@pytest.mark.cwl_small
def test_pick_value_with_one_null_value(caplog):
    """
    Make sure toil-cwl-runner does not false log a warning when pickValue is
    used but outputSource only contains one null value. See: #3991.
    """
    from toil.cwl import cwltoil

    cwl_file = os.path.join(os.path.dirname(__file__), "conditional_wf.cwl")
    job_file = os.path.join(os.path.dirname(__file__), "conditional_wf.yaml")
    args = [cwl_file, job_file]

    with caplog.at_level(logging.WARNING, logger="toil.cwl.cwltoil"):
        cwltoil.main(args)
        for line in caplog.messages:
            assert "You had a conditional step that did not run, but you did not use pickValue to handle the skipped input." not in line


@needs_cwl
@pytest.mark.cwl_small
def test_usage_message():
    """
    This is purely to ensure a (more) helpful error message is printed if a user does
    not order their positional args correctly [cwl, cwl-job (json/yml/yaml), jobstore].
    """
    toil = "toil-cwl-runner"
    cwl = "test/cwl/revsort.cwl"
    cwl_job_json = "test/cwl/revsort-job.json"
    jobstore = "delete-test-toil"
    random_option_1 = "--logInfo"
    random_option_2 = "--disableChaining"
    cmd_wrong_ordering_1 = [
        toil,
        cwl,
        cwl_job_json,
        jobstore,
        random_option_1,
        random_option_2,
    ]
    cmd_wrong_ordering_2 = [
        toil,
        cwl,
        jobstore,
        random_option_1,
        random_option_2,
        cwl_job_json,
    ]
    cmd_wrong_ordering_3 = [
        toil,
        jobstore,
        random_option_1,
        random_option_2,
        cwl,
        cwl_job_json,
    ]

    for cmd in [cmd_wrong_ordering_1, cmd_wrong_ordering_2, cmd_wrong_ordering_3]:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        assert (
            b"Usage: toil-cwl-runner [options] example.cwl example-job.yaml" in stderr
        )
        assert (
            b"All positional arguments [cwl, yml_or_json] "
            b"must always be specified last for toil-cwl-runner." in stderr
        )


@needs_cwl
@pytest.mark.cwl_small
def test_workflow_echo_string():
    toil = "toil-cwl-runner"
    jobstore = f"--jobStore=file:explicit-local-jobstore-{uuid.uuid4()}"
    option_1 = "--strict-memory-limit"
    option_2 = "--force-docker-pull"
    option_3 = "--clean=always"
    cwl = os.path.join(os.path.dirname(__file__), "echo_string.cwl")
    cmd = [toil, jobstore, option_1, option_2, option_3, cwl]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    assert stdout == b"{}", f"Got wrong output: {stdout}\nWith error: {stderr}"
    assert b"Finished toil run successfully" in stderr
    assert p.returncode == 0


@needs_cwl
@pytest.mark.cwl_small
def test_workflow_echo_string_scatter_capture_stdout():
    toil = "toil-cwl-runner"
    jobstore = f"--jobStore=file:explicit-local-jobstore-{uuid.uuid4()}"
    option_1 = "--strict-memory-limit"
    option_2 = "--force-docker-pull"
    option_3 = "--clean=always"
    cwl = os.path.join(
        os.path.dirname(__file__), "echo_string_scatter_capture_stdout.cwl"
    )
    cmd = [toil, jobstore, option_1, option_2, option_3, cwl]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    outputs = json.loads(stdout)
    out_list = outputs["list_out"]
    assert len(out_list) == 2, f"outList shoud have two file elements {out_list}"
    out_base = outputs["list_out"][0]
    # This is a test on the scatter functionality and stdout.
    # Each value of scatter should generate a separate file in the output.
    for index, file in enumerate(out_list):
        if index > 0:
            new_file_loc = out_base["location"] + f"_{index + 1}"
        else:
            new_file_loc = out_base["location"]
        assert (
            new_file_loc == file["location"]
        ), f"Toil should have detected conflicts for these stdout files {new_file_loc} and {file}"

    assert b"Finished toil run successfully" in stderr
    assert p.returncode == 0


@needs_cwl
@pytest.mark.cwl_small
def test_visit_top_cwl_class():
    structure = {
        "class": "Directory",
        "listing": [
            {
                "class": "Directory",
                "listing": [
                    {"class": "File"},
                    {
                        "class": "File",
                        "secondaryFiles": [
                            {"class": "Directory"},
                            {"class": "File"},
                            {"cruft"},
                        ],
                    },
                ],
            },
            {"some garbage": "yep"},
            [],
            None,
        ],
    }

    counter = 0

    def increment(thing: Dict) -> None:
        """
        Make sure we are at something CWL object like, and count it.
        """
        assert "class" in thing
        nonlocal counter
        counter += 1

    # We should stop at the root when looking for a Directory
    visit_top_cwl_class(structure, ("Directory",), increment)
    assert counter == 1

    # We should see the top-level files when looking for a file
    counter = 0
    visit_top_cwl_class(structure, ("File",), increment)
    assert counter == 2

    # When looking for a file or a directory we should stop at the first match to either.
    counter = 0
    visit_top_cwl_class(structure, ("File", "Directory"), increment)
    assert counter == 1


@needs_cwl
@pytest.mark.cwl_small
def test_visit_cwl_class_and_reduce():
    structure = {
        "class": "Directory",
        "listing": [
            {
                "class": "Directory",
                "listing": [
                    {"class": "File"},
                    {
                        "class": "File",
                        "secondaryFiles": [
                            {"class": "Directory"},
                            {"class": "File"},
                            {"cruft"},
                        ],
                    },
                ],
            },
            {"some garbage": "yep"},
            [],
            None,
        ],
    }

    down_count = 0

    def op_down(thing: MutableMapping) -> int:
        """
        Grab the ID of the thing we are at, and count what we visit going
        down.
        """
        nonlocal down_count
        down_count += 1
        return id(thing)

    up_count = 0
    up_child_count = 0

    def op_up(thing: MutableMapping, down_value: int, child_results: List[str]) -> str:
        """
        Check the down return value and the up return values, and count
        what we visit going up and what child relationships we have.
        """
        nonlocal up_child_count
        nonlocal up_count
        assert down_value == id(thing)
        for res in child_results:
            assert res == "Sentinel value!"
            up_child_count += 1
        up_count += 1
        return "Sentinel value!"

    visit_cwl_class_and_reduce(structure, ("Directory",), op_down, op_up)
    assert down_count == 3
    assert up_count == 3
    # Only 2 child relationships
    assert up_child_count == 2


@needs_cwl
@pytest.mark.cwl_small
def test_download_structure(tmp_path) -> None:
    """
    Make sure that download_structure makes the right calls to what it thinks is the file store.
    """

    # Define what we would download
    fid1 = FileID("afile", 10, False)
    fid2 = FileID("adifferentfile", 1000, True)

    # And what directory structure it would be in
    structure = {
        "dir1": {
            "dir2": {
                "f1": "toilfile:" + fid1.pack(),
                "f1again": "toilfile:" + fid1.pack(),
                "dir2sub": {},
            },
            "dir3": {},
        },
        "anotherfile": "toilfile:" + fid2.pack(),
    }

    # Say where to put it on the filesystem
    to_dir = str(tmp_path)

    # Make a fake file store
    file_store = Mock(AbstractFileStore)

    # These will be populated.
    # TODO: This cache seems unused. Remove it?
    # This maps filesystem path to CWL URI
    index = {}
    # This maps CWL URI to filesystem path
    existing = {}

    # Do the download
    download_structure(file_store, index, existing, structure, to_dir)

    # Check the results
    # 3 files should be made
    assert len(index) == 3
    # From 2 unique URIs
    assert len(existing) == 2

    # Make sure that the index contents (path to URI) are correct
    assert os.path.join(to_dir, "dir1/dir2/f1") in index
    assert os.path.join(to_dir, "dir1/dir2/f1again") in index
    assert os.path.join(to_dir, "anotherfile") in index
    assert (
        index[os.path.join(to_dir, "dir1/dir2/f1")] == structure["dir1"]["dir2"]["f1"]
    )
    assert (
        index[os.path.join(to_dir, "dir1/dir2/f1again")]
        == structure["dir1"]["dir2"]["f1again"]
    )
    assert index[os.path.join(to_dir, "anotherfile")] == structure["anotherfile"]

    # And the existing contents (URI to path)
    assert "toilfile:" + fid1.pack() in existing
    assert "toilfile:" + fid2.pack() in existing
    assert existing["toilfile:" + fid1.pack()] in [
        os.path.join(to_dir, "dir1/dir2/f1"),
        os.path.join(to_dir, "dir1/dir2/f1again"),
    ]
    assert existing["toilfile:" + fid2.pack()] == os.path.join(to_dir, "anotherfile")

    # The directory structure should be created for real
    assert os.path.isdir(os.path.join(to_dir, "dir1")) is True
    assert os.path.isdir(os.path.join(to_dir, "dir1/dir2")) is True
    assert os.path.isdir(os.path.join(to_dir, "dir1/dir2/dir2sub")) is True
    assert os.path.isdir(os.path.join(to_dir, "dir1/dir3")) is True

    # The file store should have been asked to do the download
    file_store.readGlobalFile.assert_has_calls(
        [
            call(fid1, os.path.join(to_dir, "dir1/dir2/f1"), symlink=True),
            call(fid1, os.path.join(to_dir, "dir1/dir2/f1again"), symlink=True),
            call(fid2, os.path.join(to_dir, "anotherfile"), symlink=True),
        ],
        any_order=True,
    )
