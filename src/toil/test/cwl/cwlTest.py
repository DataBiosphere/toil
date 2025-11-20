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
from collections.abc import Generator
import json
import logging
import os
import re
import shutil
import stat
import subprocess
import sys
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
    remove_redundant_mounts
)
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import WorkerImportJob
from toil.lib.threading import cpu_count
from toil.test import (
    get_data,
)
from toil.test import (
    pslow as slow,
    pneeds_docker as needs_docker,
    pneeds_cwl as needs_cwl,
    pneeds_aws_s3 as needs_aws_s3,
    pneeds_docker_cuda as needs_docker_cuda,
    pneeds_gridengine as needs_gridengine,
    pneeds_kubernetes as needs_kubernetes,
    pneeds_local_cuda as needs_local_cuda,
    pneeds_lsf as needs_lsf,
    pneeds_mesos as needs_mesos,
    pneeds_online as needs_online,
    pneeds_slurm as needs_slurm,
    pneeds_torque as needs_torque,
    pneeds_wes_server as needs_wes_server,
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


# This is a type for a function that runs toil-cwl-runner and checks the
# result. See TestCWLWorkflow._tester and TestCWLWorkflow._debug_worker_tester
# for implementations.
TesterFuncType = Callable[[Path, Path, "CWLObjectType", Path], None]


@needs_cwl
@pytest.mark.cwl
class TestCWLWorkflow:
    """
    CWL tests included in Toil that don't involve the whole CWL conformance
    test suite. Tests Toil-specific functions like URL types supported for
    inputs.
    """

    def test_cwl_cmdline_input(self) -> None:
        """
        Test that running a CWL workflow with inputs specified on the command line passes.
        """
        from toil.cwl import cwltoil

        with get_data("test/cwl/conditional_wf.cwl") as cwlfile:
            args = [str(cwlfile), "--message", "str", "--sleep", "2"]
            st = StringIO()
            # If the workflow runs, it must have had options
            cwltoil.main(args, stdout=st)

    def _tester(
        self,
        cwlfile: Path,
        jobfile: Path,
        expect: "CWLObjectType",
        outdir: Path,
        out_name: str = "output",
        main_args: Optional[list[str]] = None,
    ) -> None:
        """
        Helper function that runs a CWL workflow and checks the result.

        Implements TesterFuncType, plus a few additional parameters.

        :param cwlfile: CWL workflow file path to run.
        :param jobfile: Path to the input definition for the workflow run.
        :param expect: Expected result of the workflow as a deserialized CWL
            object. Should have one key per workflow output field. If output
            files are expected from the workflow, they need to have their
            absolute paths on disk under outdir already filled in.
        :param outdir: Path to a directory to put the workflow's output files
            in.
        :param out_name: Name of the JSON key where the workflow's outputs can
            be found in the output JSON from Toil.
        :param main_args: Additional arguments to pass to toil-cwl-runner. This
            is not part of TesterFuncType; you can use partial() to fill this
            in and stamp out a TesterFuncType that runs toil-cwl-runner with
            various closed-over arguments.
        """
    

        from toil.cwl import cwltoil

        st = StringIO()
        real_main_args = main_args or []
        real_main_args.extend(
            [
                "--logDebug",
                "--outdir",
                str(outdir),
                str(cwlfile),
                str(jobfile),
            ]
        )
        cwltoil.main(real_main_args, stdout=st)
        out = json.loads(st.getvalue())
        out.get(out_name, {}).pop("http://commonwl.org/cwltool#generation", None)
        out.get(out_name, {}).pop("nameext", None)
        out.get(out_name, {}).pop("nameroot", None)
        assert out == expect

        for k, v in expect.items():
            if (
                isinstance(v, dict)
                and "class" in v
                and v["class"] == "File"
                and "path" in v
            ):
                # This is a top-level output file.
                # None of our output files should be executable.
                assert os.path.exists(v["path"]) is True
                assert (os.stat(v["path"]).st_mode & stat.S_IXUSR) == 0

    def _debug_worker_tester(
        self, cwlfile: Path, jobfile: Path, expect: "CWLObjectType", outdir: Path
    ) -> None:
        """
        Helper function that runs a CWL workflow with --debugWorker and checks
        the result.

        Implements TesterFuncType directly.

        :param cwlfile: CWL workflow file path to run.
        :param jobfile: Path to the input definition for the workflow run.
        :param expect: Expected result of the workflow as a deserialized CWL
            object. Should have one key per workflow output field. If output
            files are expected from the workflow, they need to have their
            absolute paths on disk under outdir already filled in.
        :param outdir: Path to a directory to put the workflow's output files
            in.
        """
        from toil.cwl import cwltoil

        st = StringIO()
        cwltoil.main(
            [
                "--debugWorker",
                "--outdir",
                str(outdir),
                str(cwlfile),
                str(jobfile),
            ],
            stdout=st,
        )
        out = json.loads(st.getvalue())
        out["output"].pop("http://commonwl.org/cwltool#generation", None)
        out["output"].pop("nameext", None)
        out["output"].pop("nameroot", None)
        assert out == expect

    def revsort(
        self, cwl_filename: str, tester_fn: TesterFuncType, out_dir: Path
    ) -> None:
        with get_data(f"test/cwl/{cwl_filename}") as cwl_file:
            with get_data("test/cwl/revsort-job.json") as job_file:
                tester_fn(
                    cwl_file, job_file, self._expected_revsort_output(out_dir), out_dir
                )

    def revsort_no_checksum(
        self, cwl_filename: str, tester_fn: TesterFuncType, out_dir: Path
    ) -> None:
        with get_data(f"test/cwl/{cwl_filename}") as cwl_file:
            with get_data("test/cwl/revsort-job.json") as job_file:
                tester_fn(
                    cwl_file,
                    job_file,
                    self._expected_revsort_nochecksum_output(out_dir),
                    out_dir,
                )

    def download(self, inputs: str, tester_fn: TesterFuncType, out_dir: Path) -> None:
        """
        Run a generic download test with a tester function and check the result.

        Ther test is the download.cwl workflow.

        The result has to match _expected_download_output on the output
        directory, so it must contain an empty "output.txt" file.

        :param inputs: Relative path to the inputs file within the Toil source
            tree's src/toil/test/cwl directory.
        :param tester_fn: The tester function to use to run the workflow and
            check the result.
        :param out_dir: Path to the output directory to save the workflow
            output in.
        """
        with get_data(f"test/cwl/{inputs}") as input_location:
            with get_data("test/cwl/download.cwl") as cwl_file:
                tester_fn(
                    cwl_file,
                    input_location,
                    self._expected_download_output(out_dir),
                    out_dir,
                )

    def load_contents(
        self, inputs: str, tester_fn: TesterFuncType, out_dir: Path
    ) -> None:
        with get_data(f"test/cwl/{inputs}") as input_location:
            with get_data("test/cwl/load_contents.cwl") as cwl_file:
                tester_fn(
                    cwl_file,
                    input_location,
                    self._expected_load_contents_output(out_dir),
                    out_dir,
                )

    def download_directory(
        self, inputs: str, tester_fn: TesterFuncType, out_dir: Path
    ) -> None:
        with get_data(f"test/cwl/{inputs}") as input_location:
            with get_data("test/cwl/download_directory.cwl") as cwl_file:
                tester_fn(
                    cwl_file,
                    input_location,
                    self._expected_download_output(out_dir),
                    out_dir,
                )

    def download_subdirectory(
        self, inputs: str, tester_fn: TesterFuncType, out_dir: Path
    ) -> None:
        with get_data(f"test/cwl/{inputs}") as input_location:
            with get_data("test/cwl/download_subdirectory.cwl") as cwl_file:
                tester_fn(
                    cwl_file,
                    input_location,
                    self._expected_download_output(out_dir),
                    out_dir,
                )

    def test_mpi(self, tmp_path: Path) -> None:
        from toil.cwl import cwltoil

        stdout = StringIO()
        with get_data("test/cwl/mock_mpi/fake_mpi.yml") as mpi_config_file:
            with get_data("test/cwl/mpi_simple.cwl") as cwl_file:
                with get_data("test/cwl/mock_mpi/fake_mpi_run.py") as fake_mpi_run:
                    main_args = [
                        "--logDebug",
                        "--outdir",
                        str(tmp_path),
                        "--enable-dev",
                        "--enable-ext",
                        "--mpi-config-file",
                        str(mpi_config_file),
                        str(cwl_file),
                    ]
                    path = os.environ["PATH"]
                    os.environ["PATH"] = f"{path}:{fake_mpi_run.parent}"
                    cwltoil.main(main_args, stdout=stdout)
                    os.environ["PATH"] = path
                    stdout_text = stdout.getvalue()
                    assert "pids" in stdout_text
                    out = json.loads(stdout_text)
                    with open(
                        out.get("pids", {}).get("location")[len("file://") :]
                    ) as f:
                        two_pids = [int(i) for i in f.read().split()]
                    assert len(two_pids) == 2
                    assert isinstance(two_pids[0], int)
                    assert isinstance(two_pids[1], int)

    @needs_aws_s3
    @pytest.mark.aws_s3
    @pytest.mark.online
    def test_s3_as_secondary_file(self, tmp_path: Path) -> None:
        from toil.cwl import cwltoil

        stdout = StringIO()
        with get_data("test/cwl/s3_secondary_file.cwl") as cwl_file:
            with get_data("test/cwl/s3_secondary_file.json") as inputs_file:
                main_args = ["--outdir", str(tmp_path), str(cwl_file), str(inputs_file)]
                cwltoil.main(main_args, stdout=stdout)
                out = json.loads(stdout.getvalue())
                assert (
                    out["output"]["checksum"]
                    == "sha1$d14dd02e354918b4776b941d154c18ebc15b9b38"
                )

                assert out["output"]["size"] == 24
                with open(out["output"]["location"][len("file://") :]) as f:
                    assert f.read().strip() == "When is s4 coming out?"

    @needs_docker
    @pytest.mark.docker
    @pytest.mark.online
    def test_run_revsort(self, tmp_path: Path) -> None:
        self.revsort("revsort.cwl", self._tester, tmp_path)

    @needs_docker
    @pytest.mark.docker
    @pytest.mark.online
    def test_run_revsort_nochecksum(self, tmp_path: Path) -> None:
        self.revsort_no_checksum(
            "revsort.cwl",
            partial(self._tester, main_args=["--no-compute-checksum"]),
            tmp_path,
        )

    def test_run_revsort_no_container(self, tmp_path: Path) -> None:
        self.revsort(
            "revsort.cwl", partial(self._tester, main_args=["--no-container"]), tmp_path
        )

    @needs_docker
    @pytest.mark.docker
    @pytest.mark.online
    def test_run_revsort2(self, tmp_path: Path) -> None:
        self.revsort("revsort2.cwl", self._tester, tmp_path)

    @needs_docker
    @pytest.mark.docker
    @pytest.mark.online
    def test_run_revsort_debug_worker(self, tmp_path: Path) -> None:
        self.revsort("revsort.cwl", self._debug_worker_tester, tmp_path)

    @needs_docker
    @pytest.mark.docker
    @pytest.mark.online
    def test_run_colon_output(self, tmp_path: Path) -> None:
        with get_data("test/cwl/colon_test_output.cwl") as cwl_file:
            with get_data("test/cwl/colon_test_output_job.yaml") as inputs_file:
                self._tester(
                    cwl_file,
                    inputs_file,
                    self._expected_colon_output(tmp_path),
                    tmp_path,
                    out_name="result",
                )

    @pytest.mark.integrative
    @needs_docker
    @pytest.mark.docker
    @pytest.mark.online
    def test_run_dockstore_trs(self, tmp_path: Path) -> None:
        from toil.cwl import cwltoil

        stdout = StringIO()
        main_args = [
            "--outdir",
            str(tmp_path),
            "#workflow/github.com/dockstore-testing/md5sum-checker:master",
            "https://raw.githubusercontent.com/dockstore-testing/md5sum-checker/refs/heads/master/md5sum/md5sum-input-cwl.json",
        ]
        cwltoil.main(main_args, stdout=stdout)
        out = json.loads(stdout.getvalue())
        with open(out.get("output_file", {}).get("location")[len("file://") :]) as f:
            computed_hash = f.read().strip()
        assert computed_hash == "00579a00e3e7fa0674428ac7049423e2"

    def test_glob_dir_bypass_file_store(self, tmp_path: Path) -> None:
        self.maxDiff = 1000
        with get_data("test/cwl/glob_dir.cwl") as cwl_file:
            with get_data("test/cwl/empty.json") as inputs_file:
                self._tester(
                    cwl_file,
                    inputs_file,
                    self._expected_glob_dir_output(tmp_path),
                    tmp_path,
                    main_args=["--bypass-file-store"],
                )

    def test_required_input_condition_protection(self, tmp_path: Path) -> None:
        # This doesn't run containerized
        with get_data("test/cwl/not_run_required_input.cwl") as cwl_file:
            with get_data("test/cwl/empty.json") as inputs_file:
                self._tester(cwl_file, inputs_file, {}, tmp_path)

    @needs_slurm
    @pytest.mark.slurm
    def test_slurm_node_memory(self, tmp_path: Path) -> None:
        pass

        # Run the workflow. This will either finish quickly and tell us the
        # memory we got, or take a long time because it requested a whole
        # node's worth of memory and no nodes are free right now. We need to
        # support both.

        # And if we run out of time we need to stop the workflow gracefully and
        # cancel the Slurm jobs.

        try:
            with get_data("test/cwl/measure_default_memory.cwl") as cwl_file:
                main_args = [
                    f"--jobStore={str(tmp_path / 'jobStoreDir')}",
                    # Avoid racing to toil kill before the jobstore is removed
                    "--clean=never",
                    "--batchSystem=slurm",
                    "--no-cwl-default-ram",
                    "--slurmDefaultAllMem=True",
                    "--outdir",
                    str(tmp_path / "outdir"),
                    str(cwl_file),
                ]
                log.debug("Start test workflow")
                child = subprocess.Popen(
                    ["toil-cwl-runner"] + main_args, stdout=subprocess.PIPE
                )
                output, _ = child.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            # The job didn't finish quickly; presumably waiting for a full node.
            # Stop the workflow
            log.debug("Workflow might be waiting for a full node. Stop it.")
            subprocess.check_call(["toil", "kill", str(tmp_path / "jobStoreDir")])
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
            assert result > (3 * 1024 * 1024)

    @needs_aws_s3
    @pytest.mark.aws_s3
    @pytest.mark.online
    def test_download_s3(self, tmp_path: Path) -> None:
        self.download("download_s3.json", self._tester, tmp_path)

    def test_download_http(self, tmp_path: Path) -> None:
        self.download("download_http.json", self._tester, tmp_path)

    def test_download_https(self, tmp_path: Path) -> None:
        self.download("download_https.json", self._tester, tmp_path)

    def test_download_https_reference(self, tmp_path: Path) -> None:
        self.download(
            "download_https.json",
            partial(self._tester, main_args=["--reference-inputs"]),
            tmp_path,
        )

    def test_download_file(self, tmp_path: Path) -> None:
        self.download("download_file.json", self._tester, tmp_path)

    def test_download_file_worker_import(self, tmp_path: Path) -> None:
        self.download(
            "download_file.json",
            partial(self._tester, main_args=["--run-imports-on-workers"]),
            tmp_path
        )

    def test_download_file_uri(self, tmp_path: Path) -> None:
        self.download("download_file_uri.json", self._tester, tmp_path)

    def test_download_file_uri_worker_import(self, tmp_path: Path) -> None:
        self.download(
            "download_file_uri.json",
            partial(self._tester, main_args=["--run-imports-on-workers"]),
            tmp_path
        )

    def test_download_file_uri_no_hostname(self, tmp_path: Path) -> None:
        """
        Test if CWL handles file: URIs without even empty hostnames.
        """
        # We can in fact ship an absolute file URI to an empty file if we
        # assume /dev/null is available. So we can still use the helpers.
        self.download(
            "download_file_uri_no_hostname.json",
            self._tester,
            tmp_path
        )

    def test_download_file_uri_no_hostname_worker_import(self, tmp_path: Path) -> None:
        """
        Test if CWL handles file: URIs without even empty hostnames, with worker import.
        """
        self.download(
            "download_file_uri_no_hostname.json",
            partial(self._tester, main_args=["--run-imports-on-workers"]),
            tmp_path
        )

    @needs_aws_s3
    @pytest.mark.aws_s3
    @pytest.mark.online
    def test_download_directory_s3(self, tmp_path: Path) -> None:
        self.download_directory("download_directory_s3.json", self._tester, tmp_path)

    @needs_aws_s3
    @pytest.mark.aws_s3
    @pytest.mark.online
    def test_download_directory_s3_reference(self, tmp_path: Path) -> None:
        self.download_directory(
            "download_directory_s3.json",
            partial(self._tester, main_args=["--reference-inputs"]),
            tmp_path,
        )

    def test_download_directory_file(self, tmp_path: Path) -> None:
        self.download_directory("download_directory_file.json", self._tester, tmp_path)

    @needs_aws_s3
    @pytest.mark.aws_s3
    @pytest.mark.online
    def test_download_subdirectory_s3(self, tmp_path: Path) -> None:
        self.download_subdirectory(
            "download_subdirectory_s3.json", self._tester, tmp_path
        )

    def test_download_subdirectory_file(self, tmp_path: Path) -> None:
        self.download_subdirectory(
            "download_subdirectory_file.json", self._tester, tmp_path
        )

    # We also want to make sure we can run a bare tool with loadContents on the inputs, which requires accessing the input data early in the leader.

    @needs_aws_s3
    @pytest.mark.aws_s3
    @pytest.mark.online
    def test_load_contents_s3(self, tmp_path: Path) -> None:
        self.load_contents("download_s3.json", self._tester, tmp_path)

    def test_load_contents_http(self, tmp_path: Path) -> None:
        self.load_contents("download_http.json", self._tester, tmp_path)

    def test_load_contents_https(self, tmp_path: Path) -> None:
        self.load_contents("download_https.json", self._tester, tmp_path)

    def test_load_contents_file(self, tmp_path: Path) -> None:
        self.load_contents("download_file.json", self._tester, tmp_path)

    @slow
    @pytest.mark.integrative
    @pytest.mark.slow
    @pytest.mark.skip("Fails too often due to remote service")
    def test_bioconda(self, tmp_path: Path) -> None:
        with get_data("test/cwl/seqtk_seq.cwl") as cwl_file:
            with get_data("test/cwl/seqtk_seq_job.json") as inputs_file:
                self._tester(
                    cwl_file,
                    inputs_file,
                    self._expected_seqtk_output(tmp_path),
                    tmp_path,
                    main_args=["--beta-conda-dependencies"],
                    out_name="output1",
                )

    @needs_docker
    @pytest.mark.docker
    @pytest.mark.online
    def test_default_args(self, tmp_path: Path) -> None:
        with get_data("test/cwl/seqtk_seq.cwl") as cwl_file:
            with get_data("test/cwl/seqtk_seq_job.json") as inputs_file:
                self._tester(
                    cwl_file,
                    inputs_file,
                    self._expected_seqtk_output(tmp_path),
                    tmp_path,
                    main_args=[
                        "--default-container",
                        "quay.io/biocontainers/seqtk:1.4--he4a0461_1",
                    ],
                    out_name="output1",
                )

    @needs_docker
    @pytest.mark.docker
    @pytest.mark.integrative
    @pytest.mark.online
    @pytest.mark.skip(reason="Fails too often due to remote service")
    def test_biocontainers(self, tmp_path: Path) -> None:
        with get_data("test/cwl/seqtk_seq.cwl") as cwl_file:
            with get_data("test/cwl/seqtk_seq_job.json") as inputs_file:
                self._tester(
                    cwl_file,
                    inputs_file,
                    self._expected_seqtk_output(tmp_path),
                    tmp_path,
                    main_args=["--beta-use-biocontainers"],
                    out_name="output1",
                )

    @needs_docker
    @needs_docker_cuda
    @needs_local_cuda
    @pytest.mark.docker
    @pytest.mark.online
    @pytest.mark.docker_cuda
    @pytest.mark.local_cuda
    def test_cuda(self, tmp_path: Path) -> None:
        with get_data("test/cwl/nvidia_smi.cwl") as cwl_file:
            with get_data("test/cwl/empty.json") as inputs_file:
                self._tester(
                    cwl_file,
                    inputs_file,
                    {},
                    tmp_path,
                    out_name="result",
                )

    @slow
    @pytest.mark.slow
    def test_restart(self, tmp_path: Path) -> None:
        """
        Enable restarts with toil-cwl-runner -- run failing test, re-run correct test.
        Only implemented for single machine.
        """
        log.info("Running CWL Test Restart.  Expecting failure, then success.")
        from toil.cwl import cwltoil

        outDir = tmp_path / "outDir"
        outDir.mkdir()
        jobStore = tmp_path / "jobStore"
        with get_data("test/cwl/revsort.cwl") as cwl_file:
            with get_data("test/cwl/revsort-job.json") as job_file:
                cmd = [
                    "--outdir",
                    str(outDir),
                    "--jobStore",
                    str(jobStore),
                    "--no-container",
                    str(cwl_file),
                    str(job_file),
                ]

                # create a fake rev bin that actually points to the "date" binary
                cal_path = [
                    d
                    for d in os.environ["PATH"].split(":")
                    if os.path.exists(os.path.join(d, "date"))
                ][-1]
                os.symlink(
                    os.path.realpath(os.path.join(cal_path, "date")), outDir / "rev"
                )

                def path_with_bogus_rev() -> str:
                    # append to the front of the PATH so that we check there first
                    return f"{str(outDir)}:" + os.environ["PATH"]

                orig_path = os.environ["PATH"]
                # Force a failure by trying to use an incorrect version of `rev` from the PATH
                os.environ["PATH"] = path_with_bogus_rev()
                try:
                    subprocess.check_output(
                        ["toil-cwl-runner"] + cmd,
                        env=os.environ.copy(),
                        stderr=subprocess.STDOUT,
                    )
                    pytest.fail("Expected problem job with incorrect PATH did not fail")
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
                    pytest.fail("Restart with missing directory did not fail")
                except subprocess.CalledProcessError:
                    pass

    def test_caching(self, tmp_path: Path) -> None:
        log.info("Running CWL caching test.")
        from toil.cwl import cwltoil

        outDir = tmp_path / "outDir"
        cacheDir = tmp_path / "cacheDir"
        log_path = outDir / "log"

        with get_data("test/cwl/revsort.cwl") as cwl_file:
            with get_data("test/cwl/revsort-job.json") as job_file:
                cmd = [
                    "--outdir",
                    str(outDir),
                    "--jobStore",
                    str(tmp_path / "jobStore"),
                    "--clean=always",
                    "--no-container",
                    "--cachedir",
                    str(cacheDir),
                    str(cwl_file),
                    str(job_file),
                ]
                st = StringIO()
                ret = cwltoil.main(cmd, stdout=st)
                assert ret == 0
                # cwltool hashes certain steps into directories, ensure it exists
                # since cwltool caches per task and revsort has 2 cwl tasks, there should be 2 directories and 2 status files
                assert sum(1 for _ in cacheDir.iterdir()) == 4

                # Rerun the workflow to ensure there is a cache hit and that we don't rerun the tools
                st = StringIO()
                cmd = [
                    "--writeLogsFromAllJobs=True",
                    "--writeLogs",
                    str(log_path),
                ] + cmd
                ret = cwltoil.main(cmd, stdout=st)
                assert ret == 0

                # Ensure all of the worker logs are using their cached outputs
                for file in log_path.iterdir():
                    assert "Using cached output" in file.read_text(encoding="utf-8")

    @needs_aws_s3
    @pytest.mark.aws_s3
    @pytest.mark.online
    def test_streamable(
        self, tmp_path: Path, extra_args: Optional[list[str]] = None
    ) -> None:
        """
        Test that a file with 'streamable'=True is a named pipe.
        This is a CWL1.2 feature.
        """
        st = StringIO()
        outDir = tmp_path / "outDir"
        with get_data("test/cwl/stream.cwl") as cwlfile:
            with get_data("test/cwl/stream.json") as jobfile:
                out_name = "output"
                jobstore = f"--jobStore=aws:us-west-1:toil-stream-{uuid.uuid4()}"
                from toil.cwl import cwltoil

                args = [
                    "--logDebug",
                    "--outdir",
                    str(outDir),
                    jobstore,
                    str(cwlfile),
                    str(jobfile),
                ]
                if extra_args:
                    args = extra_args + args
                log.info("Run CWL run: %s", " ".join(args))
                cwltoil.main(args, stdout=st)
        out = json.loads(st.getvalue())
        out[out_name].pop("http://commonwl.org/cwltool#generation", None)
        out[out_name].pop("nameext", None)
        out[out_name].pop("nameroot", None)
        assert out == self._expected_streaming_output(outDir)
        with open(out[out_name]["location"][len("file://") :]) as f:
            assert f.read().strip() == "When is s4 coming out?"

    @needs_aws_s3
    @pytest.mark.aws_s3
    @pytest.mark.online
    def test_streamable_reference(self, tmp_path: Path) -> None:
        """
        Test that a streamable file is a stream even when passed around by URI.
        """
        self.test_streamable(tmp_path=tmp_path, extra_args=["--reference-inputs"])

    def test_preemptible(self, tmp_path: Path) -> None:
        """
        Tests that the http://arvados.org/cwl#UsePreemptible extension is supported.
        """
        from toil.cwl import cwltoil

        st = StringIO()
        out_name = "output"
        with get_data("test/cwl/preemptible.cwl") as cwlfile:
            with get_data("test/cwl/empty.json") as jobfile:
                args = [
                    "--outdir",
                    str(tmp_path / "outDir"),
                    str(cwlfile),
                    str(jobfile),
                ]
                cwltoil.main(args, stdout=st)
        out = json.loads(st.getvalue())
        out[out_name].pop("http://commonwl.org/cwltool#generation", None)
        out[out_name].pop("nameext", None)
        out[out_name].pop("nameroot", None)
        with open(out[out_name]["location"][len("file://") :]) as f:
            assert f.read().strip() == "hello"

    def test_preemptible_expression(self, tmp_path: Path) -> None:
        """
        Tests that the http://arvados.org/cwl#UsePreemptible extension is validated.
        """
        from toil.cwl import cwltoil

        st = StringIO()
        with get_data("test/cwl/preemptible_expression.cwl") as cwlfile:
            with get_data("test/cwl/preemptible_expression.json") as jobfile:
                args = [
                    "--outdir",
                    str(tmp_path),
                    str(cwlfile),
                    str(jobfile),
                ]
                with pytest.raises(
                    ValidationException, match=re.escape("expressions are not allowed")
                ):
                    cwltoil.main(args, stdout=st)

    @staticmethod
    def _expected_seqtk_output(outDir: Path) -> "CWLObjectType":
        path = outDir / "out"
        return {
            "output1": {
                "location": path.as_uri(),
                "path": str(path),
                "checksum": "sha1$322e001e5a99f19abdce9f02ad0f02a17b5066c2",
                "basename": "out",
                "class": "File",
                "size": 150,
            }
        }

    @staticmethod
    def _expected_revsort_output(outDir: Path) -> "CWLObjectType":
        path = outDir / "output.txt"
        return {
            "output": {
                "location": path.as_uri(),
                "path": str(path),
                "basename": "output.txt",
                "size": 1111,
                "class": "File",
                "checksum": "sha1$b9214658cc453331b62c2282b772a5c063dbd284",
            }
        }

    @staticmethod
    def _expected_revsort_nochecksum_output(outDir: Path) -> "CWLObjectType":
        path = outDir / "output.txt"
        return {
            "output": {
                "location": path.as_uri(),
                "path": str(path),
                "basename": "output.txt",
                "size": 1111,
                "class": "File",
            }
        }

    @staticmethod
    def _expected_download_output(outDir: Path) -> "CWLObjectType":
        path = outDir / "output.txt"
        return {
            "output": {
                "location": path.as_uri(),
                "basename": "output.txt",
                "size": 0,
                "class": "File",
                "checksum": "sha1$da39a3ee5e6b4b0d3255bfef95601890afd80709",
                "path": str(path),
            }
        }

    @staticmethod
    def _expected_glob_dir_output(out_dir: Path) -> "CWLObjectType":
        dir_path = out_dir / "shouldmake"
        dir_loc = dir_path.as_uri()
        file_path = dir_path / "test.txt"
        file_loc = file_path.as_uri()
        return {
            "shouldmake": {
                "location": dir_loc,
                "path": str(dir_path),
                "basename": "shouldmake",
                "nameroot": "shouldmake",
                "nameext": "",
                "class": "Directory",
                "listing": [
                    {
                        "class": "File",
                        "location": file_loc,
                        "path": str(file_path),
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
    def _expected_load_contents_output(cls, out_dir: Path) -> "CWLObjectType":
        """
        Generate the putput we expect from load_contents.cwl, when sending
        output files to the given directory.
        """
        expected = cls._expected_download_output(out_dir)
        expected["length"] = 146
        return expected

    @staticmethod
    def _expected_colon_output(outDir: Path) -> "CWLObjectType":
        path = outDir / "A:Gln2Cys_result"
        loc = "file://" + os.path.join(
            outDir, "A%3AGln2Cys_result"
        )  # not using .as_uri to ensure the expected escaping
        return {
            "result": {
                "location": loc,
                "path": str(path),
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

    def _expected_streaming_output(self, outDir: Path) -> "CWLObjectType":
        path = outDir / "output.txt"
        return {
            "output": {
                "location": path.as_uri(),
                "path": str(path),
                "basename": "output.txt",
                "size": 24,
                "class": "File",
                "checksum": "sha1$d14dd02e354918b4776b941d154c18ebc15b9b38",
            }
        }

    @needs_docker
    @pytest.mark.docker
    @pytest.mark.online
    def test_missing_import(self, tmp_path: Path) -> None:
        with get_data("test/cwl/revsort.cwl") as cwl_file:
            with get_data("test/cwl/revsort-job-missing.json") as inputs_file:
                cmd = [
                    "toil-cwl-runner",
                    f"--outdir={str(tmp_path)}",
                    "--clean=always",
                    str(cwl_file),
                    str(inputs_file),
                ]
                p = subprocess.run(cmd, capture_output=True, text=True)
                # Make sure that the missing file is mentioned in the log so the user knows
                assert p.returncode == 1, p.stderr
                assert "missing.txt" in p.stderr

    @needs_aws_s3
    @pytest.mark.aws_s3
    @pytest.mark.online
    def test_optional_secondary_files_exists(self, tmp_path: Path) -> None:
        from toil.cwl import cwltoil

        with get_data("test/cwl/optional-file.cwl") as cwlfile:
            with get_data("test/cwl/optional-file-exists.json") as jobfile:
                args = [str(cwlfile), str(jobfile), f"--outdir={str(tmp_path)}"]
                ret = cwltoil.main(args)
                assert ret == 0
                assert (tmp_path / "wdl_templates_old.zip").exists()

    @needs_aws_s3
    @pytest.mark.aws_s3
    @pytest.mark.online
    def test_optional_secondary_files_missing(self, tmp_path: Path) -> None:
        from toil.cwl import cwltoil

        with get_data("test/cwl/optional-file.cwl") as cwlfile:
            with get_data("test/cwl/optional-file-missing.json") as jobfile:
                args = [str(cwlfile), str(jobfile), f"--outdir={str(tmp_path)}"]
                ret = cwltoil.main(args)
                assert ret == 0
                assert not (tmp_path / "hello_old.zip").exists()


@pytest.fixture(scope="function")
def cwl_v1_0_spec(tmp_path: Path) -> Generator[Path]:
    # The latest cwl git commit hash from https://github.com/common-workflow-language/common-workflow-language.
    # Update it to get the latest tests.
    testhash = (
        "6a955874ade22080b8ef962b4e0d6e408112c1ef"  # Date:   Tue Dec 16 2020 8:43pm PST
    )
    url = (
        "https://github.com/common-workflow-language/common-workflow-language/archive/%s.zip"
        % testhash
    )
    urlretrieve(url, "spec.zip")
    with zipfile.ZipFile("spec.zip", "r") as z:
        z.extractall()
    shutil.move("common-workflow-language-%s" % testhash, str(tmp_path))
    os.remove("spec.zip")
    try:
        yield tmp_path / ("common-workflow-language-%s" % testhash)
    finally:
        pass  # no cleanup

@pytest.mark.integrative
@pytest.mark.conformance
@needs_cwl
@needs_online
@pytest.mark.cwl
@pytest.mark.online
class TestCWLv10Conformance:
    """
    Run the CWL 1.0 conformance tests in various environments.
    """

    @slow
    @needs_docker
    @pytest.mark.slow
    @pytest.mark.docker
    @pytest.mark.online
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_caching(self, cwl_v1_0_spec: Path) -> None:
        self.test_run_conformance(cwl_v1_0_spec, caching=True)

    @slow
    @needs_docker
    @pytest.mark.slow
    @pytest.mark.docker
    @pytest.mark.online
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance(
        self,
        cwl_v1_0_spec: Path,
        batchSystem: Optional[str] = None,
        caching: bool = False,
        selected_tests: Optional[str] = None,
        skipped_tests: Optional[str] = None,
        extra_args: Optional[list[str]] = None,
    ) -> None:
        run_conformance_tests(
            workDir=str(cwl_v1_0_spec / "v1.0"),
            yml=str(cwl_v1_0_spec / "v1.0" / "conformance_test_v1.0.yaml"),
            caching=caching,
            batchSystem=batchSystem,
            selected_tests=selected_tests,
            skipped_tests=skipped_tests,
            extra_args=extra_args,
        )

    @slow
    @needs_lsf
    @pytest.mark.slow
    @pytest.mark.lsf
    @pytest.mark.skip("Not run")
    def test_lsf_cwl_conformance(
        self, cwl_v1_0_spec: Path, caching: bool = False
    ) -> None:
        self.test_run_conformance(cwl_v1_0_spec, batchSystem="lsf", caching=caching)

    @slow
    @needs_slurm
    @pytest.mark.slow
    @pytest.mark.slurm
    @pytest.mark.skip("Not run")
    def test_slurm_cwl_conformance(
        self, cwl_v1_0_spec: Path, caching: bool = False
    ) -> None:
        self.test_run_conformance(cwl_v1_0_spec, batchSystem="slurm", caching=caching)

    @slow
    @needs_torque
    @pytest.mark.slow
    @pytest.mark.torque
    @pytest.mark.skip("Not run")
    def test_torque_cwl_conformance(
        self, cwl_v1_0_spec: Path, caching: bool = False
    ) -> None:
        self.test_run_conformance(cwl_v1_0_spec, batchSystem="torque", caching=caching)

    @slow
    @needs_gridengine
    @pytest.mark.slow
    @pytest.mark.gridengine
    @pytest.mark.skip("Not run")
    def test_gridengine_cwl_conformance(
        self, cwl_v1_0_spec: Path, caching: bool = False
    ) -> None:
        self.test_run_conformance(
            cwl_v1_0_spec, batchSystem="grid_engine", caching=caching
        )

    @slow
    @needs_mesos
    @pytest.mark.slow
    @pytest.mark.mesos
    @pytest.mark.skip("Not run")
    def test_mesos_cwl_conformance(
        self, cwl_v1_0_spec: Path, caching: bool = False
    ) -> None:
        self.test_run_conformance(cwl_v1_0_spec, batchSystem="mesos", caching=caching)

    @slow
    @needs_kubernetes
    @pytest.mark.slow
    @pytest.mark.kubernetes
    @pytest.mark.online
    def test_kubernetes_cwl_conformance(
        self, cwl_v1_0_spec: Path, caching: bool = False
    ) -> None:
        self.test_run_conformance(
            cwl_v1_0_spec,
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
    @pytest.mark.slow
    @pytest.mark.lsf
    @pytest.mark.skip(reason="Not run")
    def test_lsf_cwl_conformance_with_caching(self, cwl_v1_0_spec: Path) -> None:
        self.test_lsf_cwl_conformance(cwl_v1_0_spec, caching=True)

    @slow
    @needs_slurm
    @pytest.mark.slow
    @pytest.mark.slurm
    @pytest.mark.skip(reason="Not run")
    def test_slurm_cwl_conformance_with_caching(self, cwl_v1_0_spec: Path) -> None:
        self.test_slurm_cwl_conformance(cwl_v1_0_spec, caching=True)

    @slow
    @needs_torque
    @pytest.mark.slow
    @pytest.mark.torque
    @pytest.mark.skip(reason="Not run")
    def test_torque_cwl_conformance_with_caching(self, cwl_v1_0_spec: Path) -> None:
        self.test_torque_cwl_conformance(cwl_v1_0_spec, caching=True)

    @slow
    @needs_gridengine
    @pytest.mark.slow
    @pytest.mark.gridengine
    @pytest.mark.skip(reason="Not run")
    def test_gridengine_cwl_conformance_with_caching(self, cwl_v1_0_spec: Path) -> None:
        self.test_gridengine_cwl_conformance(cwl_v1_0_spec, caching=True)

    @slow
    @needs_mesos
    @pytest.mark.slow
    @pytest.mark.mesos
    @pytest.mark.skip(reason="Not run")
    def test_mesos_cwl_conformance_with_caching(self, cwl_v1_0_spec: Path) -> None:
        self.test_mesos_cwl_conformance(cwl_v1_0_spec, caching=True)

    @slow
    @needs_kubernetes
    @pytest.mark.slow
    @pytest.mark.kubernetes
    @pytest.mark.online
    def test_kubernetes_cwl_conformance_with_caching(self, cwl_v1_0_spec: Path) -> None:
        self.test_kubernetes_cwl_conformance(cwl_v1_0_spec, caching=True)


@pytest.fixture(scope="function")
def cwl_v1_1_spec(tmp_path: Path) -> Generator[Path]:
    # The latest cwl git commit hash from https://github.com/common-workflow-language/cwl-v1.1
    # Update it to get the latest tests.
    testhash = "664835e83eb5e57eee18a04ce7b05fb9d70d77b7"
    url = (
        "https://github.com/common-workflow-language/cwl-v1.1/archive/%s.zip" % testhash
    )
    urlretrieve(url, "spec.zip")
    with zipfile.ZipFile("spec.zip", "r") as z:
        z.extractall()
    shutil.move("cwl-v1.1-%s" % testhash, str(tmp_path))
    os.remove("spec.zip")
    try:
        yield tmp_path / ("cwl-v1.1-%s" % testhash)
    finally:
        pass  # no cleanup


@pytest.mark.integrative
@pytest.mark.conformance
@needs_cwl
@needs_online
@pytest.mark.cwl
@pytest.mark.online
class TestCWLv11Conformance:
    """
    Run the CWL 1.1 conformance tests in various environments.
    """

    @slow
    @needs_docker
    @pytest.mark.slow
    @pytest.mark.docker
    @pytest.mark.online
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance(
        self,
        cwl_v1_1_spec: Path,
        caching: bool = False,
        batchSystem: Optional[str] = None,
        skipped_tests: Optional[str] = None,
        extra_args: Optional[list[str]] = None,
    ) -> None:
        run_conformance_tests(
            workDir=str(cwl_v1_1_spec),
            yml=str(cwl_v1_1_spec / "conformance_tests.yaml"),
            caching=caching,
            batchSystem=batchSystem,
            skipped_tests=skipped_tests,
            extra_args=extra_args,
        )

    @slow
    @needs_docker
    @pytest.mark.slow
    @pytest.mark.docker
    @pytest.mark.online
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_caching(self, cwl_v1_1_spec: Path) -> None:
        self.test_run_conformance(cwl_v1_1_spec, caching=True)

    @slow
    @needs_kubernetes
    @pytest.mark.slow
    @pytest.mark.kubernetes
    @pytest.mark.online
    def test_kubernetes_cwl_conformance(
        self, cwl_v1_1_spec: Path, caching: bool = False
    ) -> None:
        self.test_run_conformance(
            cwl_v1_1_spec,
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
    @pytest.mark.slow
    @pytest.mark.kubernetes
    @pytest.mark.online
    def test_kubernetes_cwl_conformance_with_caching(self, cwl_v1_1_spec: Path) -> None:
        self.test_kubernetes_cwl_conformance(cwl_v1_1_spec, caching=True)


@pytest.fixture(scope="function")
def cwl_v1_2_spec(tmp_path: Path) -> Generator[Path]:
    # The latest cwl git commit hash from https://github.com/common-workflow-language/cwl-v1.2
    # Update it to get the latest tests.
    testhash = "0d538a0dbc5518f3c6083ce4571926f65cb84f76"
    url = (
        "https://github.com/common-workflow-language/cwl-v1.2/archive/%s.zip" % testhash
    )
    urlretrieve(url, "spec.zip")
    with zipfile.ZipFile("spec.zip", "r") as z:
        z.extractall()
    shutil.move("cwl-v1.2-%s" % testhash, str(tmp_path))
    os.remove("spec.zip")
    try:
        yield tmp_path / ("cwl-v1.2-%s" % testhash)
    finally:
        pass  # no cleanup


@pytest.mark.integrative
@pytest.mark.conformance
@needs_cwl
@needs_online
@pytest.mark.cwl
@pytest.mark.online
class TestCWLv12Conformance:
    """
    Run the CWL 1.2 conformance tests in various environments.
    """

    @slow
    @needs_docker
    @pytest.mark.slow
    @pytest.mark.docker
    @pytest.mark.online
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance(
        self,
        cwl_v1_2_spec: Path,
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
            junit_file = os.path.abspath("conformance-1.2.junit.xml")
        run_conformance_tests(
            workDir=str(cwl_v1_2_spec),
            yml=str(cwl_v1_2_spec / "conformance_tests.yaml"),
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
    @needs_docker
    @pytest.mark.slow
    @pytest.mark.docker
    @pytest.mark.online
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_caching(self, cwl_v1_2_spec: Path) -> None:
        self.test_run_conformance(
            cwl_v1_2_spec,
            caching=True,
            junit_file=os.path.abspath("caching-conformance-1.2.junit.xml"),
        )

    @slow
    @needs_docker
    @pytest.mark.slow
    @pytest.mark.docker
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_task_caching(
        self, cwl_v1_2_spec: Path, tmp_path: Path
    ) -> None:
        self.test_run_conformance(
            cwl_v1_2_spec,
            junit_file=os.path.abspath("task-caching-conformance-1.2.junit.xml"),
            extra_args=["--cachedir", str(tmp_path / "task_cache")],
        )

    @slow
    @needs_docker
    @pytest.mark.slow
    @pytest.mark.docker
    @pytest.mark.timeout(CONFORMANCE_TEST_TIMEOUT)
    def test_run_conformance_with_in_place_update(self, cwl_v1_2_spec: Path) -> None:
        """
        Make sure that with --bypass-file-store we properly support in place
        update on a single node, and that this doesn't break any other
        features.
        """
        self.test_run_conformance(
            cwl_v1_2_spec,
            extra_args=["--bypass-file-store"],
            must_support_all_features=True,
            junit_file=os.path.abspath("in-place-update-conformance-1.2.junit.xml"),
        )

    @slow
    @needs_kubernetes
    @pytest.mark.slow
    @pytest.mark.kubernetes
    @pytest.mark.online
    def test_kubernetes_cwl_conformance(
        self,
        cwl_v1_2_spec: Path,
        caching: bool = False,
        junit_file: Optional[str] = None,
    ) -> None:
        if junit_file is None:
            junit_file = os.path.abspath("kubernetes-conformance-1.2.junit.xml")
        self.test_run_conformance(
            cwl_v1_2_spec,
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
    @pytest.mark.slow
    @pytest.mark.kubernetes
    @pytest.mark.online
    def test_kubernetes_cwl_conformance_with_caching(self, cwl_v1_2_spec: Path) -> None:
        self.test_kubernetes_cwl_conformance(
            cwl_v1_2_spec,
            caching=True,
            junit_file=os.path.abspath("kubernetes-caching-conformance-1.2.junit.xml"),
        )

    @slow
    @needs_wes_server
    @pytest.mark.slow
    @pytest.mark.wes_server
    @pytest.mark.online
    def test_wes_server_cwl_conformance(self, cwl_v1_2_spec: Path) -> None:
        """
        Run the CWL conformance tests via WES. TOIL_WES_ENDPOINT must be
        specified. If the WES server requires authentication, set TOIL_WES_USER
        and TOIL_WES_PASSWORD.

        To run manually:

        TOIL_WES_ENDPOINT=http://localhost:8080 \
        TOIL_WES_USER=test \
        TOIL_WES_PASSWORD=password \
        python -m pytest src/toil/test/cwl/cwlTest.py::TestCWLv12Conformance::test_wes_server_cwl_conformance -vv --log-level INFO --log-cli-level INFO
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
            cwl_v1_2_spec,
            runner="toil-wes-cwl-runner",
            selected_tests="1-309,313-337",
            extra_args=extra_args,
        )


@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small_log_dir
def test_workflow_echo_string_scatter_stderr_log_dir(tmp_path: Path) -> None:
    log_dir = tmp_path / "cwl-logs"
    with get_data("test/cwl/echo_string_scatter_capture_stdout.cwl") as cwl_file:
        cmd = [
            "toil-cwl-runner",
            f"--jobStore={tmp_path / 'jobstore'}",
            "--strict-memory-limit",
            f"--log-dir={log_dir}",
            str(cwl_file),
        ]
        p = subprocess.run(cmd, capture_output=True, text=True)
        outputs = json.loads(p.stdout)
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

        assert "Finished toil run successfully" in p.stderr
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
@pytest.mark.cwl
@pytest.mark.cwl_small_log_dir
def test_log_dir_echo_no_output(tmp_path: Path) -> None:
    log_dir = tmp_path / "cwl-logs"
    job_store = tmp_path / "test_log_dir_echo_no_output"
    with get_data("test/cwl/echo-stdout-log-dir.cwl") as cwl_file:
        cmd = [
            "toil-cwl-runner",
            f"--jobStore={job_store}",
            "--strict-memory-limit",
            f"--log-dir={str(log_dir)}",
            str(cwl_file),
        ]
        subprocess.run(cmd)

        assert log_dir.exists()
        assert sum(1 for _ in log_dir.iterdir()) == 1

        subdir = next(log_dir.iterdir())
        assert subdir.name == "echo"
        assert subdir.is_dir()
        assert sum(1 for _ in subdir.iterdir()) == 1
        result = next(subdir.iterdir())
        assert result.name == "out.txt"
        assert "hello" in result.read_text()


@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small_log_dir
def test_log_dir_echo_stderr(tmp_path: Path) -> None:
    log_dir = tmp_path / "cwl-logs"
    log_dir.mkdir()
    with get_data("test/cwl/echo-stderr.cwl") as cwl_file:
        cmd = [
            "toil-cwl-runner",
            f"--jobStore={str(tmp_path / 'test_log_dir_echo_stderr')}",
            "--strict-memory-limit",
            "--force-docker-pull",
            "--clean=always",
            f"--log-dir={str(log_dir)}",
            str(cwl_file),
        ]
        subprocess.run(cmd)
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
@pytest.mark.cwl
@pytest.mark.cwl_small_log_dir
def test_filename_conflict_resolution(tmp_path: Path) -> None:
    with get_data("test/cwl/test_filename_conflict_resolution.cwl") as cwl_file:
        with get_data("test/cwl/test_filename_conflict_resolution.ms") as msin:
            cmd = [
                "toil-cwl-runner",
                f"--outdir={tmp_path}",
                str(cwl_file),
                "--msin",
                str(msin),
            ]
            p = subprocess.run(cmd, capture_output=True, text=True)
            assert "Finished toil run successfully" in p.stderr
            assert p.returncode == 0


@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small_log_dir
def test_filename_conflict_resolution_3_or_more(tmp_path: Path) -> None:
    with get_data("test/cwl/scatter_duplicate_outputs.cwl") as cwl_file:
        cmd = ["toil-cwl-runner", f"--outdir={tmp_path}", str(cwl_file)]
        p = subprocess.run(cmd, capture_output=True, text=True)
        assert "Finished toil run successfully" in p.stderr
        assert p.returncode == 0
        assert (
            sum(1 for _ in tmp_path.iterdir()) == 9
        ), f"All 9 files made by the scatter should be in the directory: {tmp_path}"


@needs_cwl
@needs_docker
@pytest.mark.cwl
@pytest.mark.docker
@pytest.mark.cwl_small_log_dir
def test_filename_conflict_detection(tmp_path: Path) -> None:
    """
    Make sure we don't just stage files over each other when using a container.
    """
    with get_data("test/cwl/test_filename_conflict_detection.cwl") as cwl_file:
        cmd = ["toil-cwl-runner", f"--outdir={tmp_path}", str(cwl_file)]
        p = subprocess.run(cmd, capture_output=True, text=True)
        assert "File staging conflict" in p.stderr
        assert p.returncode != 0


@needs_cwl
@needs_docker
@pytest.mark.cwl
@pytest.mark.docker
@pytest.mark.cwl_small_log_dir
def test_filename_conflict_detection_at_root(tmp_path: Path) -> None:
    """
    Make sure we don't just stage files over each other.

    Specifically, when using a container and the files are at the root of the work dir.
    """
    with get_data("test/cwl/test_filename_conflict_detection_at_root.cwl") as cwl_file:
        cmd = ["toil-cwl-runner", f"--outdir={tmp_path}", str(cwl_file)]
        p = subprocess.run(cmd, capture_output=True, text=True)
        assert "File staging conflict" in p.stderr
        assert p.returncode != 0


@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small
def test_pick_value_with_one_null_value(
    caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    """
    Make sure toil-cwl-runner does not false log a warning when pickValue is
    used but outputSource only contains one null value. See: #3991.
    """
    from toil.cwl import cwltoil

    with get_data("test/cwl/conditional_wf.cwl") as cwl_file:
        with get_data("test/cwl/conditional_wf.yaml") as job_file:
            with caplog.at_level(logging.WARNING, logger="toil.cwl.cwltoil"):
                cwltoil.main([f"--outdir={tmp_path}", str(cwl_file), str(job_file)])
                for line in caplog.messages:
                    assert (
                        "You had a conditional step that did not run, but you did not use pickValue to handle the skipped input."
                        not in line
                    )


@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small
def test_workflow_echo_string(tmp_path: Path) -> None:
    with get_data("test/cwl/echo_string.cwl") as cwl_file:
        cmd = [
            "toil-cwl-runner",
            f"--jobStore=file:{tmp_path / 'jobstore'}",
            "--strict-memory-limit",
            str(cwl_file),
        ]
        p = subprocess.run(cmd, capture_output=True, text=True)
        assert (
            p.stdout.strip() == "{}"
        ), f"Got wrong output: {p.stdout}\nWith error: {p.stderr}"
        assert "Finished toil run successfully" in p.stderr
        assert p.returncode == 0


@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small
def test_workflow_echo_string_scatter_capture_stdout(tmp_path: Path) -> None:
    with get_data("test/cwl/echo_string_scatter_capture_stdout.cwl") as cwl_file:
        cmd = [
            "toil-cwl-runner",
            f"--jobStore=file:{tmp_path / 'jobStore'}",
            "--strict-memory-limit",
            str(cwl_file),
        ]
        p = subprocess.run(cmd, capture_output=True, text=True)
        assert len(p.stdout) > 0
        outputs = json.loads(p.stdout)
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

        assert "Finished toil run successfully" in p.stderr
        assert p.returncode == 0


@needs_cwl
@pytest.mark.cwl
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
@pytest.mark.cwl
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
@pytest.mark.cwl
@pytest.mark.cwl_small
def test_trim_mounts_op_nonredundant() -> None:
    """
    Make sure we don't remove all non-duplicate listings
    """
    s: CWLObjectType = {"class": "Directory", "basename": "directory", "listing": [{"class": "File", "basename": "file", "contents": "hello world"}]}
    remove_redundant_mounts(s)

    # nothing should have been removed
    assert isinstance(s['listing'], list)
    assert len(s['listing']) == 1

@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small
def test_trim_mounts_op_redundant() -> None:
    """
    Make sure we remove all duplicate listings
    """
    s: CWLObjectType = {
        "class": "Directory",
        "location": "file:///home/heaucques/Documents/toil/test_dir",
        "basename": "test_dir",
        "listing": [
            {
                "class": "Directory",
                "location": "file:///home/heaucques/Documents/toil/test_dir/nested_dir",
                "basename": "nested_dir",
                "listing": [],
                "path": "/home/heaucques/Documents/toil/test_dir/nested_dir"
            },
            {
                "class": "File",
                "location": "file:///home/heaucques/Documents/toil/test_dir/test_file",
                "basename": "test_file",
                "size": 0,
                "nameroot": "test_file",
                "nameext": "",
                "path": "/home/heaucques/Documents/toil/test_dir/test_file",
                "checksum": "sha1$da39a3ee5e6b4b0d3255bfef95601890afd80709"
            }
        ],
        "path": "/home/heaucques/Documents/toil/test_dir"
    }
    remove_redundant_mounts(s)

    # everything should have been removed
    assert isinstance(s['listing'], list)
    assert len(s['listing']) == 0

@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small
def test_trim_mounts_op_partially_redundant() -> None:
    """
    Make sure we remove only the redundant listings in the CWL object and leave nonredundant listings intact
    """
    s: CWLObjectType = {
        "class": "Directory",
        "location": "file:///home/heaucques/Documents/toil/test_dir",
        "basename": "test_dir",
        "listing": [
            {
                "class": "Directory",
                "location": "file:///home/heaucques/Documents/thing",
                "basename": "thing2",
                "listing": [],
                "path": "/home/heaucques/Documents/toil/thing2"
            },
            {
                "class": "File",
                "location": "file:///home/heaucques/Documents/toil/test_dir/test_file",
                "basename": "test_file",
                "size": 0,
                "nameroot": "test_file",
                "nameext": "",
                "path": "/home/heaucques/Documents/toil/test_dir/test_file",
                "checksum": "sha1$da39a3ee5e6b4b0d3255bfef95601890afd80709"
            }
        ],
        "path": "/home/heaucques/Documents/toil/test_dir"
    }
    remove_redundant_mounts(s)

    # everything except the nested directory should be removed
    assert isinstance(s['listing'], list)
    assert len(s['listing']) == 1

@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small
def test_trim_mounts_op_mixed_urls_and_paths() -> None:
    """
    Ensure we remove redundant listings in certain edge cases
    """
    # Edge cases around encoding:
    # Ensure URL decoded file URIs match the bare path equivalent. Both of these paths should have the same shared directory
    s: CWLObjectType = {"class": "Directory", "basename": "123", "location": "file:///tmp/%25/123", "listing": [{"class": "File", "path": "/tmp/%/123/456", "basename": "456"}]}
    remove_redundant_mounts(s)
    assert isinstance(s['listing'], list)
    assert len(s['listing']) == 0

@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small
def test_trim_mounts_op_decodable_paths() -> None:
    """"""
    # Ensure path names don't get unnecessarily decoded
    s: CWLObjectType = {"class": "Directory", "basename": "dir", "path": "/tmp/cat%2Ftag/dir", "listing": [{"class": "File", "path": "/tmp/cat/tag/dir/file", "basename": "file"}]}
    remove_redundant_mounts(s)
    assert isinstance(s['listing'], list)
    assert len(s['listing']) == 1

@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small
def test_trim_mounts_op_multiple_encodings() -> None:
    # Ensure differently encoded URLs are properly decoded
    s: CWLObjectType = {"class": "Directory", "basename": "dir", "location": "file:///tmp/cat%2Ftag/dir", "listing": [{"class": "File", "location": "file:///tmp/cat%2ftag/dir/file", "basename": "file"}]}
    remove_redundant_mounts(s)
    assert isinstance(s['listing'], list)
    assert len(s['listing']) == 0




@needs_cwl
@pytest.mark.cwl
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
    to_dir = tmp_path

    # Make a fake file store
    file_store = Mock(AbstractFileStore)

    # These will be populated.
    # TODO: This cache seems unused. Remove it?
    # This maps filesystem path to CWL URI
    index: dict[str, str] = {}
    # This maps CWL URI to filesystem path
    existing: dict[str, str] = {}

    # Do the download
    download_structure(file_store, index, existing, structure, str(to_dir))

    # Check the results
    # 3 files should be made
    assert len(index) == 3
    # From 2 unique URIs
    assert len(existing) == 2

    # Make sure that the index contents (path to URI) are correct
    assert str(to_dir / "dir1/dir2/f1") in index
    assert str(to_dir / "dir1/dir2/f1again") in index
    assert str(to_dir / "anotherfile") in index
    assert (
        index[str(to_dir / "dir1/dir2/f1")]
        == cast(
            DirectoryStructure, cast(DirectoryStructure, structure["dir1"])["dir2"]
        )["f1"]
    )
    assert (
        index[str(to_dir / "dir1/dir2/f1again")]
        == cast(
            DirectoryStructure, cast(DirectoryStructure, structure["dir1"])["dir2"]
        )["f1again"]
    )
    assert index[str(to_dir / "anotherfile")] == structure["anotherfile"]

    # And the existing contents (URI to path)
    assert "toilfile:" + fid1.pack() in existing
    assert "toilfile:" + fid2.pack() in existing
    assert existing["toilfile:" + fid1.pack()] in [
        str(to_dir / "dir1/dir2/f1"),
        str(to_dir / "dir1/dir2/f1again"),
    ]
    assert existing["toilfile:" + fid2.pack()] == str(to_dir / "anotherfile")

    # The directory structure should be created for real
    assert (to_dir / "dir1").is_dir()
    assert (to_dir / "dir1/dir2").is_dir()
    assert (to_dir / "dir1/dir2/dir2sub").is_dir()
    assert (to_dir / "dir1/dir3").is_dir()

    # The file store should have been asked to do the download
    file_store.readGlobalFile.assert_has_calls(
        [
            call(fid1, str(to_dir / "dir1/dir2/f1"), symlink=False),
            call(fid1, str(to_dir / "dir1/dir2/f1again"), symlink=False),
            call(fid2, str(to_dir / "anotherfile"), symlink=False),
        ],
        any_order=True,
    )


@needs_cwl
@pytest.mark.cwl
@pytest.mark.timeout(300)
def test_import_on_workers() -> None:
    from toil.cwl import cwltoil

    detector = ImportWorkersMessageHandler()

    # Set up a log message detector to the root logger
    logging.getLogger().addHandler(detector)

    with get_data("test/cwl/download.cwl") as cwl_file:
        with get_data("test/cwl/directory/directory/file.txt") as file_path:
            # To make sure we see every job issued with a leader log message
            # that we can then detect for the test, we need to turn off
            # chaining.
            args = [
                "--runImportsOnWorkers",
                "--importWorkersDisk=10MiB",
                "--realTimeLogging=True",
                "--logLevel=INFO",
                "--logColors=False",
                "--disableChaining=True",
                str(cwl_file),
                "--input",
                str(file_path),
            ]
            cwltoil.main(args)

        assert detector.detected is True

@needs_cwl
@pytest.mark.cwl
@pytest.mark.cwl_small
def test_missing_tmpdir_and_tmp_outdir(tmp_path: Path) -> None:
    """
    tmpdir_prefix and tmp_outdir_prefix do not need to exist prior to running the workflow
    """
    tmpdir_prefix = os.path.join(tmp_path, "tmpdir/blah")
    tmp_outdir_prefix = os.path.join(tmp_path, "tmp_outdir/blah")

    assert not os.path.exists(os.path.dirname(tmpdir_prefix))
    assert not os.path.exists(os.path.dirname(tmp_outdir_prefix))
    with get_data("test/cwl/echo_string.cwl") as cwl_file:
        cmd = [
            "toil-cwl-runner",
            f"--jobStore=file:{tmp_path / 'jobstore'}",
            "--strict-memory-limit",
            f'--tmpdir-prefix={tmpdir_prefix}',
            f'--tmp-outdir-prefix={tmp_outdir_prefix}',
            str(cwl_file),
        ]
        p = subprocess.run(cmd)
        assert p.returncode == 0

# StreamHandler is generic, _typeshed doesn't exist at runtime, do a bit of typing trickery, see https://github.com/python/typeshed/issues/5680
if TYPE_CHECKING:
    from _typeshed import SupportsWrite

    _stream_handler = logging.StreamHandler[SupportsWrite[str]]
else:
    _stream_handler = logging.StreamHandler


class ImportWorkersMessageHandler(_stream_handler):
    """
    Detect whether any WorkerImportJob jobs ran during a workflow.
    """

    def __init__(self) -> None:
        self.detected = False  # Have we seen the message we want?

        super().__init__(sys.stderr)

    def emit(self, record: logging.LogRecord) -> None:
        # We get the job name from the class since we already started failing
        # this test once due to it being renamed.
        try:
            formatted = record.getMessage()
        except TypeError as e:
            # The log message has the wrong number of items for its fields.
            # Complain in a way we could figure out.
            raise RuntimeError(
                f"Log message {record.msg} has wrong number of "
                f"fields in {record.args}"
            ) from e
        if formatted.startswith(
            f"Issued job '{WorkerImportJob.__name__}'"
        ):
            self.detected = True
