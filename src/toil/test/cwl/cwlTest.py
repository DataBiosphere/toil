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
import stat
import subprocess
import sys
import unittest
import uuid
import zipfile
from functools import partial
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional, cast
from unittest.mock import Mock, call
from urllib.request import urlretrieve

if TYPE_CHECKING:
    from cwltool.utils import CWLObjectType

import pytest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from schema_salad.exceptions import ValidationException

from toil.cwl.utils import (
    DirectoryStructure,
    download_structure,
    visit_cwl_class_and_reduce,
    visit_top_cwl_class,
)
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.lib.threading import cpu_count
from toil.test import (
    ToilTest,
    needs_aws_s3,
    needs_cwl,
    needs_docker,
    needs_docker_cuda,
    needs_gridengine,
    needs_kubernetes,
    needs_local_cuda,
    needs_lsf,
    needs_mesos,
    needs_online,
    needs_singularity_or_docker,
    needs_slurm,
    needs_torque,
    needs_wes_server,
    slow,
)

log = logging.getLogger(__name__)
CONFORMANCE_TEST_TIMEOUT = 10000


def run_conformance_tests(
    workDir: str,
    yml: str,
    runner: Optional[str] = None,
    caching: bool = False,
    batchSystem: Optional[str] = None,
    selected_tests: Optional[str] = None,
    selected_tags: Optional[str] = None,
    skipped_tests: Optional[str] = None,
    extra_args: Optional[list[str]] = None,
    must_support_all_features: bool = False,
    junit_file: Optional[str] = None,
) -> None:
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
            "--relax-path-checks",
            # Defaults to 20s but we can't start hundreds of nodejs processes that fast on our CI potatoes
            "--eval-timeout=600",
            f"--caching={caching}",
        ]

        if extra_args:
            args_passed_directly_to_runner += extra_args

        if "SINGULARITY_DOCKER_HUB_MIRROR" in os.environ:
            args_passed_directly_to_runner.append(
                "--setEnv=SINGULARITY_DOCKER_HUB_MIRROR"
            )

        if batchSystem is None or batchSystem == "single_machine":
            # Make sure we can run on small machines
            args_passed_directly_to_runner.append("--scale=0.1")

        job_store_override = None

        if batchSystem == "kubernetes":
            # Run tests in parallel on Kubernetes.
            # We can throw a bunch at it at once and let Kubernetes schedule.
            # But we still want a local core for each.
            parallel_tests = max(min(cpu_count(), 8), 1)
        else:
            # Run tests in parallel on the local machine. Don't run too many
            # tests at once; we want at least a couple cores for each.
            # But we need to have at least a few going in parallel or we risk hitting our timeout.
            parallel_tests = max(int(cpu_count() / 2), 4)
        cmd.append(f"-j{parallel_tests}")

        if batchSystem:
            args_passed_directly_to_runner.append(f"--batchSystem={batchSystem}")
        cmd.extend(["--"] + args_passed_directly_to_runner)

        log.info("Running: '%s'", "' '".join(cmd))
        output_lines: list[str] = []
        try:
            child = subprocess.Popen(
                cmd, cwd=workDir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )

            if child.stdout is not None:
                for line_bytes in child.stdout:
                    # Pass through all the logs
                    line_text = line_bytes.decode("utf-8", errors="replace").rstrip()
                    output_lines.append(line_text)
                    log.info(line_text)

            # Once it's done writing, amke sure it succeeded.
            child.wait()
            log.info("CWL tests finished with exit code %s", child.returncode)
            if child.returncode != 0:
                # Act like check_output and raise an error.
                raise subprocess.CalledProcessError(child.returncode, " ".join(cmd))
        finally:
            if job_store_override:
                # Clean up the job store we used for all the tests, if it is still there.
                subprocess.run(["toil", "clean", job_store_override])

    except subprocess.CalledProcessError as e:
        log.info("CWL test runner return code was unsuccessful")
        only_unsupported = False
        # check output -- if we failed but only have unsupported features, we're okay
        p = re.compile(
            r"(?P<failures>\d+) failures, (?P<unsupported>\d+) unsupported features"
        )

        for line_text in output_lines:
            m = p.search(line_text)
            if m:
                if int(m.group("failures")) == 0 and int(m.group("unsupported")) > 0:
                    only_unsupported = True
                    break
        if (not only_unsupported) or must_support_all_features:
            log.error(
                "CWL tests gave unacceptable output:\n%s", "\n".join(output_lines)
            )
            raise e
        log.info("Unsuccessful return code is OK")


TesterFuncType = Callable[[str, str, "CWLObjectType"], None]


@needs_cwl
class CWLWorkflowTest(ToilTest):
    """
    CWL tests included in Toil that don't involve the whole CWL conformance
    test suite. Tests Toil-specific functions like URL types supported for
    inputs.
    """

    def setUp(self) -> None:
        """Runs anew before each test to create farm fresh temp dirs."""
        self.outDir = f"/tmp/toil-cwl-test-{str(uuid.uuid4())}"
        os.makedirs(self.outDir)
        self.rootDir = self._projectRootPath()
        self.jobStoreDir = f"./jobstore-{str(uuid.uuid4())}"

    def tearDown(self) -> None:
        """Clean up outputs."""
        if os.path.exists(self.outDir):
            shutil.rmtree(self.outDir)
        if os.path.exists(self.jobStoreDir):
            shutil.rmtree(self.jobStoreDir)
        unittest.TestCase.tearDown(self)

    def test_cwl_cmdline_input(self) -> None:
        """
        Test that running a CWL workflow with inputs specified on the command line passes.
        """
        from toil.cwl import cwltoil

        cwlfile = "src/toil/test/cwl/conditional_wf.cwl"
        args = [cwlfile, "--message", "str", "--sleep", "2"]
        st = StringIO()
        # If the workflow runs, it must have had options
        cwltoil.main(args, stdout=st)

    def _tester(
        self,
        cwlfile: str,
        jobfile: str,
        expect: "CWLObjectType",
        main_args: list[str] = [],
        out_name: str = "output",
        output_here: bool = False,
    ) -> None:
        from toil.cwl import cwltoil

        st = StringIO()
        main_args = main_args[:]
        if not output_here:
            # Don't just dump output in the working directory.
            main_args.extend(["--logDebug", "--outdir", self.outDir])
        main_args.extend(
            [
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

        for k, v in expect.items():
            if (
                isinstance(v, dict)
                and "class" in v
                and v["class"] == "File"
                and "path" in v
            ):
                # This is a top-level output file.
                # None of our output files should be executable.
                self.assertTrue(os.path.exists(v["path"]))
                self.assertFalse(os.stat(v["path"]).st_mode & stat.S_IXUSR)

    def _debug_worker_tester(
        self, cwlfile: str, jobfile: str, expect: "CWLObjectType"
    ) -> None:
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

    def revsort(self, cwl_filename: str, tester_fn: TesterFuncType) -> None:
        tester_fn(
            "src/toil/test/cwl/" + cwl_filename,
            "src/toil/test/cwl/revsort-job.json",
            self._expected_revsort_output(self.outDir),
        )

    def revsort_no_checksum(self, cwl_filename: str, tester_fn: TesterFuncType) -> None:
        tester_fn(
            "src/toil/test/cwl/" + cwl_filename,
            "src/toil/test/cwl/revsort-job.json",
            self._expected_revsort_nochecksum_output(self.outDir),
        )

    def download(self, inputs: str, tester_fn: TesterFuncType) -> None:
        input_location = os.path.join("src/toil/test/cwl", inputs)
        tester_fn(
            "src/toil/test/cwl/download.cwl",
            input_location,
            self._expected_download_output(self.outDir),
        )

    def load_contents(self, inputs: str, tester_fn: TesterFuncType) -> None:
        input_location = os.path.join("src/toil/test/cwl", inputs)
        tester_fn(
            "src/toil/test/cwl/load_contents.cwl",
            input_location,
            self._expected_load_contents_output(self.outDir),
        )

    def download_directory(self, inputs: str, tester_fn: TesterFuncType) -> None:
        input_location = os.path.join("src/toil/test/cwl", inputs)
        tester_fn(
            "src/toil/test/cwl/download_directory.cwl",
            input_location,
            self._expected_download_output(self.outDir),
        )

    def download_subdirectory(self, inputs: str, tester_fn: TesterFuncType) -> None:
        input_location = os.path.join("src/toil/test/cwl", inputs)
        tester_fn(
            "src/toil/test/cwl/download_subdirectory.cwl",
            input_location,
            self._expected_download_output(self.outDir),
        )

    def test_mpi(self) -> None:
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
    def test_s3_as_secondary_file(self) -> None:
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

    def test_run_revsort(self) -> None:
        self.revsort("revsort.cwl", self._tester)

    def test_run_revsort_nochecksum(self) -> None:
        self.revsort_no_checksum(
            "revsort.cwl", partial(self._tester, main_args=["--no-compute-checksum"])
        )

    def test_run_revsort_no_container(self) -> None:
        self.revsort(
            "revsort.cwl", partial(self._tester, main_args=["--no-container"])
        )

    def test_run_revsort2(self) -> None:
        self.revsort("revsort2.cwl", self._tester)

    def test_run_revsort_debug_worker(self) -> None:
        self.revsort("revsort.cwl", self._debug_worker_tester)

    def test_run_colon_output(self) -> None:
        self._tester(
            "src/toil/test/cwl/colon_test_output.cwl",
            "src/toil/test/cwl/colon_test_output_job.yaml",
            self._expected_colon_output(self.outDir),
            out_name="result",
        )
    
    @pytest.mark.integrative
    @needs_singularity_or_docker
    def test_run_dockstore_trs(self) -> None:
        from toil.cwl import cwltoil

        stdout = StringIO()
        main_args = [
            "--outdir",
            self.outDir,
            "#workflow/github.com/dockstore-testing/md5sum-checker",
            "https://raw.githubusercontent.com/dockstore-testing/md5sum-checker/refs/heads/master/md5sum/md5sum-input-cwl.json"
        ]
        cwltoil.main(main_args, stdout=stdout)
        out = json.loads(stdout.getvalue())
        with open(out.get("output_file", {}).get("location")[len("file://") :]) as f:
            computed_hash = f.read().strip()
        self.assertEqual(computed_hash, "00579a00e3e7fa0674428ac7049423e2")

    def test_glob_dir_bypass_file_store(self) -> None:
        self.maxDiff = 1000
        try:
            # We need to output to the current directory to make sure that
            # works.
            self._tester(
                "src/toil/test/cwl/glob_dir.cwl",
                "src/toil/test/cwl/empty.json",
                self._expected_glob_dir_output(os.getcwd()),
                main_args=["--bypass-file-store"],
                output_here=True,
            )
        finally:
            # Clean up anything we made in the current directory.
            try:
                shutil.rmtree(os.path.join(os.getcwd(), "shouldmake"))
            except FileNotFoundError:
                pass

    def test_required_input_condition_protection(self) -> None:
        # This doesn't run containerized
        self._tester(
            "src/toil/test/cwl/not_run_required_input.cwl",
            "src/toil/test/cwl/empty.json",
            {},
        )

    @needs_slurm
    def test_slurm_node_memory(self) -> None:
        pass

        # Run the workflow. This will either finish quickly and tell us the
        # memory we got, or take a long time because it requested a whole
        # node's worth of memory and no nodes are free right now. We need to
        # support both.

        # And if we run out of time we need to stop the workflow gracefully and
        # cancel the Slurm jobs.

        main_args = [
            f"--jobStore={self.jobStoreDir}",
            # Avoid racing to toil kill before the jobstore is removed
            "--clean=never",
            "--batchSystem=slurm",
            "--no-cwl-default-ram",
            "--slurmDefaultAllMem=True",
            "--outdir",
            self.outDir,
            os.path.join(self.rootDir, "src/toil/test/cwl/measure_default_memory.cwl"),
        ]
        try:
            log.debug("Start test workflow")
            child = subprocess.Popen(
                ["toil-cwl-runner"] + main_args, stdout=subprocess.PIPE
            )
            output, _ = child.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            # The job didn't finish quickly; presumably waiting for a full node.
            # Stop the workflow
            log.debug("Workflow might be waiting for a full node. Stop it.")
            subprocess.check_call(["toil", "kill", self.jobStoreDir])
            # Wait another little bit for it to clean up, making sure to collect output in case it is blocked on writing
            child.communicate(timeout=20)
            # Kill it off in case it is still running
            child.kill()
            # Reap it
            child.wait()
            # The test passes
        else:
            out = json.loads(output)
            log.debug("Workflow output: %s", out)
            memory_string = out["memory"]
            log.debug("Observed memory: %s", memory_string)
            # If there's no memory limit enforced, Slurm will return "unlimited".
            # Set result to something sensible.
            if memory_string.strip() == "unlimited":
                result = 4 * 1024 * 1024
            else:
                result = int(memory_string)
            # We should see more than the CWL default or the Toil default, assuming Slurm nodes of reasonable size (3 GiB).
            self.assertGreater(result, 3 * 1024 * 1024)

    @needs_aws_s3
    def test_download_s3(self) -> None:
        self.download("download_s3.json", self._tester)

    def test_download_http(self) -> None:
        self.download("download_http.json", self._tester)

    def test_download_https(self) -> None:
        self.download("download_https.json", self._tester)

    def test_download_https_reference(self) -> None:
        self.download(
            "download_https.json",
            partial(self._tester, main_args=["--reference-inputs"]),
        )

    def test_download_file(self) -> None:
        self.download("download_file.json", self._tester)

    @needs_aws_s3
    def test_download_directory_s3(self) -> None:
        self.download_directory("download_directory_s3.json", self._tester)

    @needs_aws_s3
    def test_download_directory_s3_reference(self) -> None:
        self.download_directory(
            "download_directory_s3.json",
            partial(self._tester, main_args=["--reference-inputs"]),
        )

    def test_download_directory_file(self) -> None:
        self.download_directory("download_directory_file.json", self._tester)

    @needs_aws_s3
    def test_download_subdirectory_s3(self) -> None:
        self.download_subdirectory("download_subdirectory_s3.json", self._tester)

    def test_download_subdirectory_file(self) -> None:
        self.download_subdirectory("download_subdirectory_file.json", self._tester)

    # We also want to make sure we can run a bare tool with loadContents on the inputs, which requires accessing the input data early in the leader.

    @needs_aws_s3
    def test_load_contents_s3(self) -> None:
        self.load_contents("download_s3.json", self._tester)

    def test_load_contents_http(self) -> None:
        self.load_contents("download_http.json", self._tester)

    def test_load_contents_https(self) -> None:
        self.load_contents("download_https.json", self._tester)

    def test_load_contents_file(self) -> None:
        self.load_contents("download_file.json", self._tester)

    @slow
    @pytest.mark.integrative
    @unittest.skip("Fails too often due to remote service")
    def test_bioconda(self) -> None:
        self._tester(
            "src/toil/test/cwl/seqtk_seq.cwl",
            "src/toil/test/cwl/seqtk_seq_job.json",
            self._expected_seqtk_output(self.outDir),
            main_args=["--beta-conda-dependencies"],
            out_name="output1",
        )

    @needs_docker
    def test_default_args(self) -> None:
        self._tester(
            "src/toil/test/cwl/seqtk_seq.cwl",
            "src/toil/test/cwl/seqtk_seq_job.json",
            self._expected_seqtk_output(self.outDir),
            main_args=[
                "--default-container",
                "quay.io/biocontainers/seqtk:1.4--he4a0461_1",
            ],
            out_name="output1",
        )

    @needs_docker
    @pytest.mark.integrative
    @unittest.skip("Fails too often due to remote service")
    def test_biocontainers(self) -> None:
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
    def test_cuda(self) -> None:
        self._tester(
            "src/toil/test/cwl/nvidia_smi.cwl",
            "src/toil/test/cwl/empty.json",
            {},
            out_name="result",
        )

    @slow
    def test_restart(self) -> None:
        """
        Enable restarts with toil-cwl-runner -- run failing test, re-run correct test.
        Only implemented for single machine.
        """
        log.info("Running CWL Test Restart.  Expecting failure, then success.")
        from toil.cwl import cwltoil

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

        def path_with_bogus_rev() -> str:
            # append to the front of the PATH so that we check there first
            return f"{outDir}:" + os.environ["PATH"]

        orig_path = os.environ["PATH"]
        # Force a failure by trying to use an incorrect version of `rev` from the PATH
        os.environ["PATH"] = path_with_bogus_rev()
        try:
            subprocess.check_output(
                ["toil-cwl-runner"] + cmd,
                env=os.environ.copy(),
                stderr=subprocess.STDOUT,
            )
            self.fail("Expected problem job with incorrect PATH did not fail")
        except subprocess.CalledProcessError:
            pass
        # Finish the job with a correct PATH
        os.environ["PATH"] = orig_path
        cmd.insert(0, "--restart")
        cwltoil.main(cmd)
        # Should fail because previous job completed successfully
        try:
            subprocess.check_output(
                ["toil-cwl-runner"] + cmd,
                env=os.environ.copy(),
                stderr=subprocess.STDOUT,
            )
            self.fail("Restart with missing directory did not fail")
        except subprocess.CalledProcessError:
            pass

    def test_caching(self) -> None:
        log.info("Running CWL caching test.")
        from toil.cwl import cwltoil

        outDir = self._createTempDir()
        cacheDir = self._createTempDir()

        cwlDir = os.path.join(self._projectRootPath(), "src", "toil", "test", "cwl")
        log_path = os.path.join(outDir, "log")
        cmd = [
            "--outdir",
            outDir,
            "--jobStore",
            os.path.join(outDir, "jobStore"),
            "--clean=always",
            "--no-container",
            "--cachedir",
            cacheDir,
            os.path.join(cwlDir, "revsort.cwl"),
            os.path.join(cwlDir, "revsort-job.json"),
        ]
        st = StringIO()
        ret = cwltoil.main(cmd, stdout=st)
        assert ret == 0
        # cwltool hashes certain steps into directories, ensure it exists
        # since cwltool caches per task and revsort has 2 cwl tasks, there should be 2 directories and 2 status files
        assert (len(os.listdir(cacheDir)) == 4)

        # Rerun the workflow to ensure there is a cache hit and that we don't rerun the tools
        st = StringIO()
        cmd = [
                  "--writeLogsFromAllJobs=True",
                  "--writeLogs",
                  log_path
              ] + cmd
        ret = cwltoil.main(cmd, stdout=st)
        assert ret == 0

        # Ensure all of the worker logs are using their cached outputs
        for file in os.listdir(log_path):
            assert "Using cached output" in open(os.path.join(log_path, file), encoding="utf-8").read()



    @needs_aws_s3
    def test_streamable(self, extra_args: Optional[list[str]] = None) -> None:
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
            "--logDebug",
            "--outdir",
            self.outDir,
            jobstore,
            os.path.join(self.rootDir, cwlfile),
            os.path.join(self.rootDir, jobfile),
        ]
        if extra_args:
            args = extra_args + args
        log.info("Run CWL run: %s", " ".join(args))
        cwltoil.main(args, stdout=st)
        out = json.loads(st.getvalue())
        out[out_name].pop("http://commonwl.org/cwltool#generation", None)
        out[out_name].pop("nameext", None)
        out[out_name].pop("nameroot", None)
        self.assertEqual(out, self._expected_streaming_output(self.outDir))
        with open(out[out_name]["location"][len("file://") :]) as f:
            self.assertEqual(f.read().strip(), "When is s4 coming out?")

    @needs_aws_s3
    def test_streamable_reference(self) -> None:
        """
        Test that a streamable file is a stream even when passed around by URI.
        """
        self.test_streamable(extra_args=["--reference-inputs"])

    def test_preemptible(self) -> None:
        """
        Tests that the http://arvados.org/cwl#UsePreemptible extension is supported.
        """
        cwlfile = "src/toil/test/cwl/preemptible.cwl"
        jobfile = "src/toil/test/cwl/empty.json"
        out_name = "output"
        from toil.cwl import cwltoil

        st = StringIO()
        args = [
            "--outdir",
            self.outDir,
            os.path.join(self.rootDir, cwlfile),
            os.path.join(self.rootDir, jobfile),
        ]
        cwltoil.main(args, stdout=st)
        out = json.loads(st.getvalue())
        out[out_name].pop("http://commonwl.org/cwltool#generation", None)
        out[out_name].pop("nameext", None)
        out[out_name].pop("nameroot", None)
        with open(out[out_name]["location"][len("file://") :]) as f:
            self.assertEqual(f.read().strip(), "hello")

    def test_preemptible_expression(self) -> None:
        """
        Tests that the http://arvados.org/cwl#UsePreemptible extension is validated.
        """
        cwlfile = "src/toil/test/cwl/preemptible_expression.cwl"
        jobfile = "src/toil/test/cwl/preemptible_expression.json"
        from toil.cwl import cwltoil

        st = StringIO()
        args = [
            "--outdir",
            self.outDir,
            os.path.join(self.rootDir, cwlfile),
            os.path.join(self.rootDir, jobfile),
        ]
        try:
            cwltoil.main(args, stdout=st)
            raise RuntimeError("Did not raise correct exception")
        except ValidationException as e:
            # Make sure we chastise the user appropriately.
            assert "expressions are not allowed" in str(e)

    @staticmethod
    def _expected_seqtk_output(outDir: str) -> "CWLObjectType":
        path = os.path.join(outDir, "out")
        loc = "file://" + path
        return {
            "output1": {
                "location": loc,
                "path": path,
                "checksum": "sha1$322e001e5a99f19abdce9f02ad0f02a17b5066c2",
                "basename": "out",
                "class": "File",
                "size": 150,
            }
        }

    @staticmethod
    def _expected_revsort_output(outDir: str) -> "CWLObjectType":
        path = os.path.join(outDir, "output.txt")
        loc = "file://" + path
        return {
            "output": {
                "location": loc,
                "path": path,
                "basename": "output.txt",
                "size": 1111,
                "class": "File",
                "checksum": "sha1$b9214658cc453331b62c2282b772a5c063dbd284",
            }
        }

    @staticmethod
    def _expected_revsort_nochecksum_output(outDir: str) -> "CWLObjectType":
        path = os.path.join(outDir, "output.txt")
        loc = "file://" + path
        return {
            "output": {
                "location": loc,
                "path": path,
                "basename": "output.txt",
                "size": 1111,
                "class": "File",
            }
        }

    @staticmethod
    def _expected_download_output(outDir: str) -> "CWLObjectType":
        path = os.path.join(outDir, "output.txt")
        loc = "file://" + path
        return {
            "output": {
                "location": loc,
                "basename": "output.txt",
                "size": 0,
                "class": "File",
                "checksum": "sha1$da39a3ee5e6b4b0d3255bfef95601890afd80709",
                "path": path,
            }
        }

    @staticmethod
    def _expected_glob_dir_output(out_dir: str) -> "CWLObjectType":
        dir_path = os.path.join(out_dir, "shouldmake")
        dir_loc = "file://" + dir_path
        file_path = os.path.join(dir_path, "test.txt")
        file_loc = os.path.join(dir_loc, "test.txt")
        return {
            "shouldmake": {
                "location": dir_loc,
                "path": dir_path,
                "basename": "shouldmake",
                "nameroot": "shouldmake",
                "nameext": "",
                "class": "Directory",
                "listing": [
                    {
                        "class": "File",
                        "location": file_loc,
                        "path": file_path,
                        "basename": "test.txt",
                        "checksum": "sha1$da39a3ee5e6b4b0d3255bfef95601890afd80709",
                        "size": 0,
                        "nameroot": "test",
                        "nameext": ".txt",
                    }
                ],
            }
        }

    @classmethod
    def _expected_load_contents_output(cls, out_dir: str) -> "CWLObjectType":
        """
        Generate the putput we expect from load_contents.cwl, when sending
        output files to the given directory.
        """
        expected = cls._expected_download_output(out_dir)
        expected["length"] = 146
        return expected

    @staticmethod
    def _expected_colon_output(outDir: str) -> "CWLObjectType":
        path = os.path.join(outDir, "A:Gln2Cys_result")
        loc = "file://" + os.path.join(outDir, "A%3AGln2Cys_result")
        return {
            "result": {
                "location": loc,
                "path": path,
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
                        "path": f"{path}/whale.txt",
                    }
                ],
            }
        }

    def _expected_streaming_output(self, outDir: str) -> "CWLObjectType":
        path = os.path.join(outDir, "output.txt")
        loc = "file://" + path
        return {
            "output": {
                "location": loc,
                "path": path,
                "basename": "output.txt",
                "size": 24,
                "class": "File",
                "checksum": "sha1$d14dd02e354918b4776b941d154c18ebc15b9b38",
            }
        }


@needs_cwl
@needs_online
class CWLv10Test(ToilTest):
    """
    Run the CWL 1.0 conformance tests in various environments.
    """

    def setUp(self) -> None:
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

    def tearDown(self) -> None:
        """Clean up outputs."""
        if os.path.exists(self.outDir):
            shutil.rmtree(self.outDir)
        unittest.TestCase.tearDown(self)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_caching(self) -> None:
        self.test_run_conformance(caching=True)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance(
        self,
        batchSystem: Optional[str] = None,
        caching: bool = False,
        selected_tests: Optional[str] = None,
        skipped_tests: Optional[str] = None,
        extra_args: Optional[list[str]] = None,
    ) -> None:
        run_conformance_tests(
            workDir=self.workDir,
            yml="conformance_test_v1.0.yaml",
            caching=caching,
            batchSystem=batchSystem,
            selected_tests=selected_tests,
            skipped_tests=skipped_tests,
            extra_args=extra_args,
        )

    @slow
    @needs_lsf
    @unittest.skip("Not run")
    def test_lsf_cwl_conformance(self, caching: bool = False) -> None:
        self.test_run_conformance(batchSystem="lsf", caching=caching)

    @slow
    @needs_slurm
    @unittest.skip("Not run")
    def test_slurm_cwl_conformance(self, caching: bool = False) -> None:
        self.test_run_conformance(batchSystem="slurm", caching=caching)

    @slow
    @needs_torque
    @unittest.skip("Not run")
    def test_torque_cwl_conformance(self, caching: bool = False) -> None:
        self.test_run_conformance(batchSystem="torque", caching=caching)

    @slow
    @needs_gridengine
    @unittest.skip("Not run")
    def test_gridengine_cwl_conformance(self, caching: bool = False) -> None:
        self.test_run_conformance(batchSystem="grid_engine", caching=caching)

    @slow
    @needs_mesos
    @unittest.skip("Not run")
    def test_mesos_cwl_conformance(self, caching: bool = False) -> None:
        self.test_run_conformance(batchSystem="mesos", caching=caching)

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance(self, caching: bool = False) -> None:
        self.test_run_conformance(
            caching=caching,
            batchSystem="kubernetes",
            extra_args=["--retryCount=3"],
            # This test doesn't work with
            # Singularity; see
            # https://github.com/common-workflow-language/cwltool/blob/7094ede917c2d5b16d11f9231fe0c05260b51be6/conformance-test.sh#L99-L117
            skipped_tests="docker_entrypoint",
        )

    @slow
    @needs_lsf
    @unittest.skip("Not run")
    def test_lsf_cwl_conformance_with_caching(self) -> None:
        self.test_lsf_cwl_conformance(caching=True)

    @slow
    @needs_slurm
    @unittest.skip("Not run")
    def test_slurm_cwl_conformance_with_caching(self) -> None:
        self.test_slurm_cwl_conformance(caching=True)

    @slow
    @needs_torque
    @unittest.skip("Not run")
    def test_torque_cwl_conformance_with_caching(self) -> None:
        self.test_torque_cwl_conformance(caching=True)

    @slow
    @needs_gridengine
    @unittest.skip("Not run")
    def test_gridengine_cwl_conformance_with_caching(self) -> None:
        self.test_gridengine_cwl_conformance(caching=True)

    @slow
    @needs_mesos
    @unittest.skip("Not run")
    def test_mesos_cwl_conformance_with_caching(self) -> None:
        self.test_mesos_cwl_conformance(caching=True)

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance_with_caching(self) -> None:
        self.test_kubernetes_cwl_conformance(caching=True)


@needs_cwl
@needs_online
class CWLv11Test(ToilTest):
    """
    Run the CWL 1.1 conformance tests in various environments.
    """

    rootDir: str
    cwlSpec: str
    test_yaml: str

    @classmethod
    def setUpClass(cls) -> None:
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

    def tearDown(self) -> None:
        """Clean up outputs."""
        unittest.TestCase.tearDown(self)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance(
        self,
        caching: bool = False,
        batchSystem: Optional[str] = None,
        skipped_tests: Optional[str] = None,
        extra_args: Optional[list[str]] = None,
    ) -> None:
        run_conformance_tests(
            workDir=self.cwlSpec,
            yml=self.test_yaml,
            caching=caching,
            batchSystem=batchSystem,
            skipped_tests=skipped_tests,
            extra_args=extra_args,
        )

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_caching(self) -> None:
        self.test_run_conformance(caching=True)

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance(self, caching: bool = False) -> None:
        self.test_run_conformance(
            batchSystem="kubernetes",
            extra_args=["--retryCount=3"],
            # These tests don't work with
            # Singularity; see
            # https://github.com/common-workflow-language/cwltool/blob/7094ede917c2d5b16d11f9231fe0c05260b51be6/conformance-test.sh#L99-L117
            skipped_tests="docker_entrypoint,stdin_shorcut",
            caching=caching,
        )

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance_with_caching(self) -> None:
        self.test_kubernetes_cwl_conformance(caching=True)


@needs_cwl
@needs_online
class CWLv12Test(ToilTest):
    """
    Run the CWL 1.2 conformance tests in various environments.
    """

    rootDir: str
    cwlSpec: str
    test_yaml: str

    @classmethod
    def setUpClass(cls) -> None:
        """Runs anew before each test."""
        cls.rootDir = cls._projectRootPath()
        cls.cwlSpec = os.path.join(cls.rootDir, "src/toil/test/cwl/spec_v12")
        cls.test_yaml = os.path.join(cls.cwlSpec, "conformance_tests.yaml")
        # TODO: Use a commit zip in case someone decides to rewrite master's history?
        url = "https://github.com/common-workflow-language/cwl-v1.2.git"
        commit = "0d538a0dbc5518f3c6083ce4571926f65cb84f76"
        p = subprocess.Popen(
            f"git clone {url} {cls.cwlSpec} && cd {cls.cwlSpec} && git checkout {commit}",
            shell=True,
        )
        p.communicate()

    def tearDown(self) -> None:
        """Clean up outputs."""
        unittest.TestCase.tearDown(self)

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance(
        self,
        runner: Optional[str] = None,
        caching: bool = False,
        batchSystem: Optional[str] = None,
        selected_tests: Optional[str] = None,
        skipped_tests: Optional[str] = None,
        extra_args: Optional[list[str]] = None,
        must_support_all_features: bool = False,
        junit_file: Optional[str] = None,
    ) -> None:
        if junit_file is None:
            junit_file = os.path.join(self.rootDir, "conformance-1.2.junit.xml")
        run_conformance_tests(
            workDir=self.cwlSpec,
            yml=self.test_yaml,
            runner=runner,
            caching=caching,
            batchSystem=batchSystem,
            selected_tests=selected_tests,
            skipped_tests=skipped_tests,
            extra_args=extra_args,
            must_support_all_features=must_support_all_features,
            junit_file=junit_file,
        )
    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_caching(self) -> None:
        self.test_run_conformance(
            caching=True,
            junit_file=os.path.join(self.rootDir, "caching-conformance-1.2.junit.xml"),
        )

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_task_caching(self) -> None:
        self.test_run_conformance(
            junit_file=os.path.join(self.rootDir, "task-caching-conformance-1.2.junit.xml"),
            extra_args=["--cachedir", self._createTempDir("task_cache")]
        )

    @slow
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_in_place_update(self) -> None:
        """
        Make sure that with --bypass-file-store we properly support in place
        update on a single node, and that this doesn't break any other
        features.
        """
        self.test_run_conformance(
            extra_args=["--bypass-file-store"],
            must_support_all_features=True,
            junit_file=os.path.join(
                self.rootDir, "in-place-update-conformance-1.2.junit.xml"
            ),
        )

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance(
        self, caching: bool = False, junit_file: Optional[str] = None
    ) -> None:
        if junit_file is None:
            junit_file = os.path.join(
                self.rootDir, "kubernetes-conformance-1.2.junit.xml"
            )
        self.test_run_conformance(
            caching=caching,
            batchSystem="kubernetes",
            extra_args=["--retryCount=3"],
            # This test doesn't work with
            # Singularity; see
            # https://github.com/common-workflow-language/cwltool/blob/7094ede917c2d5b16d11f9231fe0c05260b51be6/conformance-test.sh#L99-L117
            # and
            # https://github.com/common-workflow-language/cwltool/issues/1441#issuecomment-826747975
            skipped_tests="docker_entrypoint",
            junit_file=junit_file,
        )

    @slow
    @needs_kubernetes
    def test_kubernetes_cwl_conformance_with_caching(self) -> None:
        self.test_kubernetes_cwl_conformance(
            caching=True,
            junit_file=os.path.join(
                self.rootDir, "kubernetes-caching-conformance-1.2.junit.xml"
            ),
        )

    @slow
    @needs_wes_server
    def test_wes_server_cwl_conformance(self) -> None:
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

        self.test_run_conformance(
            runner="toil-wes-cwl-runner",
            selected_tests="1-309,313-337",
            extra_args=extra_args,
        )


@needs_cwl
@pytest.mark.cwl_small_log_dir
def test_workflow_echo_string_scatter_stderr_log_dir(tmp_path: Path) -> None:
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


# TODO: It's not clear how this test tests filename conflict resolution; it
# seems like it runs a python script to copy some files and makes sure the
# workflow doesn't fail.
@needs_cwl
@pytest.mark.cwl_small_log_dir
def test_filename_conflict_resolution(tmp_path: Path) -> None:
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
@pytest.mark.cwl_small_log_dir
def test_filename_conflict_resolution_3_or_more(tmp_path: Path) -> None:
    out_dir = tmp_path / "cwl-out-dir"
    toil = "toil-cwl-runner"
    options = [
        f"--outdir={out_dir}",
        "--clean=always",
    ]
    cwl = os.path.join(os.path.dirname(__file__), "scatter_duplicate_outputs.cwl")
    cmd = [toil] + options + [cwl]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    assert b"Finished toil run successfully" in stderr
    assert p.returncode == 0
    assert (
        len(os.listdir(out_dir)) == 9
    ), "All 9 files made by the scatter should be in the directory"


@needs_cwl
@needs_docker
@pytest.mark.cwl_small_log_dir
def test_filename_conflict_detection(tmp_path: Path) -> None:
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
def test_filename_conflict_detection_at_root(tmp_path: Path) -> None:
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
def test_pick_value_with_one_null_value(caplog: pytest.LogCaptureFixture) -> None:
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
            assert (
                "You had a conditional step that did not run, but you did not use pickValue to handle the skipped input."
                not in line
            )


@needs_cwl
@pytest.mark.cwl_small
def test_workflow_echo_string() -> None:
    toil = "toil-cwl-runner"
    jobstore = f"--jobStore=file:explicit-local-jobstore-{uuid.uuid4()}"
    option_1 = "--strict-memory-limit"
    option_2 = "--force-docker-pull"
    option_3 = "--clean=always"
    cwl = os.path.join(os.path.dirname(__file__), "echo_string.cwl")
    cmd = [toil, jobstore, option_1, option_2, option_3, cwl]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    stdout2 = stdout.decode("utf-8")
    stderr2 = stderr.decode("utf-8")
    assert (
        stdout2.strip() == "{}"
    ), f"Got wrong output: {stdout2}\nWith error: {stderr2}"
    assert "Finished toil run successfully" in stderr2
    assert p.returncode == 0


@needs_cwl
@pytest.mark.cwl_small
def test_workflow_echo_string_scatter_capture_stdout() -> None:
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
    log.debug("Workflow standard output: %s", stdout)
    assert len(stdout) > 0
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
def test_visit_top_cwl_class() -> None:
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

    def increment(thing: "CWLObjectType") -> None:
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
def test_visit_cwl_class_and_reduce() -> None:
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

    def op_down(thing: "CWLObjectType") -> int:
        """
        Grab the ID of the thing we are at, and count what we visit going
        down.
        """
        nonlocal down_count
        down_count += 1
        return id(thing)

    up_count = 0
    up_child_count = 0

    def op_up(thing: "CWLObjectType", down_value: int, child_results: list[str]) -> str:
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
def test_download_structure(tmp_path: Path) -> None:
    """
    Make sure that download_structure makes the right calls to what it thinks is the file store.
    """

    # Define what we would download
    fid1 = FileID("afile", 10, False)
    fid2 = FileID("adifferentfile", 1000, True)

    # And what directory structure it would be in
    structure: DirectoryStructure = {
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
    index: dict[str, str] = {}
    # This maps CWL URI to filesystem path
    existing: dict[str, str] = {}

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
        index[os.path.join(to_dir, "dir1/dir2/f1")]
        == cast(
            DirectoryStructure, cast(DirectoryStructure, structure["dir1"])["dir2"]
        )["f1"]
    )
    assert (
        index[os.path.join(to_dir, "dir1/dir2/f1again")]
        == cast(
            DirectoryStructure, cast(DirectoryStructure, structure["dir1"])["dir2"]
        )["f1again"]
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
            call(fid1, os.path.join(to_dir, "dir1/dir2/f1"), symlink=False),
            call(fid1, os.path.join(to_dir, "dir1/dir2/f1again"), symlink=False),
            call(fid2, os.path.join(to_dir, "anotherfile"), symlink=False),
        ],
        any_order=True,
    )


@needs_cwl
@pytest.mark.timeout(300)
def test_import_on_workers() -> None:
    args = [
        "src/toil/test/cwl/download.cwl",
        "src/toil/test/cwl/download_file.json",
        "--runImportsOnWorkers",
        "--importWorkersDisk=10MiB",
        "--realTimeLogging=True",
        "--logLevel=INFO",
        "--logColors=False",
    ]
    from toil.cwl import cwltoil

    detector = ImportWorkersMessageHandler()

    # Set up a log message detector to the root logger
    logging.getLogger().addHandler(detector)

    cwltoil.main(args)

    assert detector.detected is True


# StreamHandler is generic, _typeshed doesn't exist at runtime, do a bit of typing trickery, see https://github.com/python/typeshed/issues/5680
if TYPE_CHECKING:
    from _typeshed import SupportsWrite

    _stream_handler = logging.StreamHandler[SupportsWrite[str]]
else:
    _stream_handler = logging.StreamHandler


class ImportWorkersMessageHandler(_stream_handler):
    """
    Detect the import workers log message and set a flag.
    """

    def __init__(self) -> None:
        self.detected = False  # Have we seen the message we want?

        super().__init__(sys.stderr)

    def emit(self, record: logging.LogRecord) -> None:
        if (record.msg % record.args).startswith(
            "Issued job 'CWLImportJob' CWLImportJob"
        ):
            self.detected = True
