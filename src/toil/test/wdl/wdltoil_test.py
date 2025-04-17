import json
import logging
import os
import re
import shutil
import string
import subprocess
import unittest
from collections.abc import Generator
from pathlib import Path
from typing import Any, Optional, Union, cast
from unittest.mock import patch
from uuid import uuid4

import pytest
import WDL.Error
import WDL.Expr

from toil.fileStores import FileID
from toil.test import (
    ToilTest,
    get_data,
    needs_docker,
    needs_docker_cuda,
    needs_google_storage,
    needs_singularity_or_docker,
    needs_wdl,
    slow,
)
from toil.version import exactPython
from toil.wdl.wdltoil import (
    WDLSectionJob,
    WDLWorkflowGraph,
    parse_disks,
    remove_common_leading_whitespace,
)

logger = logging.getLogger(__name__)


WDL_CONFORMANCE_TEST_REPO = "https://github.com/DataBiosphere/wdl-conformance-tests.git"
WDL_CONFORMANCE_TEST_COMMIT = "baf44bcc7e6f6927540adf77d91b26a5558ae4b7"
# These tests are known to require things not implemented by
# Toil and will not be run in CI.
WDL_CONFORMANCE_TESTS_UNSUPPORTED_BY_TOIL = [
    16,  # Basic object test (deprecated and removed in 1.1); MiniWDL and toil-wdl-runner do not support Objects, so this will fail if ran by them
    21,  # Parser: expression placeholders in strings in conditional expressions in 1.0, Cromwell style; Fails with MiniWDL and toil-wdl-runner
    64,  # Legacy test for as_map_as_input; It looks like MiniWDL does not have the function as_map()
    77,  # Test that array cannot coerce to a string. WDL 1.1 does not allow compound types to coerce into a string. This should return a TypeError.
]
WDL_UNIT_TESTS_UNSUPPORTED_BY_TOIL = [
    14,  # test_object, Objects are not supported
    19,  # map_to_struct, miniwdl cannot coerce map to struct, https://github.com/chanzuckerberg/miniwdl/issues/712
    52,  # relative_and_absolute, needs root to run
    58,  # test_gpu, needs gpu to run, else warning
    59,  # will be fixed in #5001
    66,  # This needs way too many resources (and actually doesn't work?), see https://github.com/DataBiosphere/wdl-conformance-tests/blob/2d617b703a33791f75f30a9db43c3740a499cd89/README_UNIT.md?plain=1#L8
    67,  # same as above
    68,  # Bug, see #https://github.com/DataBiosphere/toil/issues/4993
    69,  # Same as 68
    87,  # MiniWDL does not handle metacharacters properly when running regex, https://github.com/chanzuckerberg/miniwdl/issues/709
    97,  # miniwdl bug, see https://github.com/chanzuckerberg/miniwdl/issues/701
    105,  # miniwdl (and toil) bug, unserializable json is serialized, see https://github.com/chanzuckerberg/miniwdl/issues/702
    107,  # object not supported
    108,  # object not supported
    109,  # object not supported
    110,  # object not supported
    120,  # miniwdl bug, see https://github.com/chanzuckerberg/miniwdl/issues/699
    131,  # miniwdl bug, evalerror, see https://github.com/chanzuckerberg/miniwdl/issues/700
    134,  # same as 131
    144,  # miniwdl and toil bug
]


@pytest.fixture(scope="function")
def wdl_conformance_test_repo(tmp_path: Path) -> Generator[Path]:
    try:
        p = subprocess.Popen(
            f"git clone {WDL_CONFORMANCE_TEST_REPO} {str(tmp_path)} && cd {str(tmp_path)} && git checkout {WDL_CONFORMANCE_TEST_COMMIT}",
            shell=True,
        )

        p.communicate()

        if p.returncode > 0:
            raise RuntimeError("Could not clone WDL conformance tests")
        yield tmp_path
    finally:
        pass  # no cleanup needed


class TestWDLConformance:
    """
    WDL conformance tests for Toil.
    """

    def check(self, p: "subprocess.CompletedProcess[bytes]") -> None:
        """
        Make sure a call completed or explain why it failed.
        """

        if p.returncode != 0:
            logger.error(
                "Failed process standard output: %s",
                p.stdout.decode("utf-8", errors="replace"),
            )
            logger.error(
                "Failed process standard error: %s",
                p.stderr.decode("utf-8", errors="replace"),
            )

        p.check_returncode()

    @slow
    def test_unit_tests_v11(self, wdl_conformance_test_repo: Path) -> None:
        # There are still some bugs with the WDL spec, use a fixed version until
        # See comments of https://github.com/openwdl/wdl/pull/669
        os.chdir(wdl_conformance_test_repo)
        repo_url = "https://github.com/stxue1/wdl.git"
        repo_branch = "wdl-1.1.3-fixes"
        commands1 = [
            exactPython,
            "setup_unit_tests.py",
            "-v",
            "1.1",
            "--extra-patch-data",
            "unit_tests_patch_data.yaml",
            "--repo",
            repo_url,
            "--branch",
            repo_branch,
            "--force-pull",
        ]
        p1 = subprocess.run(commands1, capture_output=True)
        self.check(p1)
        commands2 = [
            exactPython,
            "run_unit.py",
            "-r",
            "toil-wdl-runner",
            "-v",
            "1.1",
            "--progress",
            "--exclude-numbers",
            ",".join([str(t) for t in WDL_UNIT_TESTS_UNSUPPORTED_BY_TOIL]),
        ]
        p2 = subprocess.run(commands2, capture_output=True)
        self.check(p2)

    # estimated running time: 10 minutes
    @slow
    def test_conformance_tests_v10(self, wdl_conformance_test_repo: Path) -> None:
        os.chdir(wdl_conformance_test_repo)
        commands = [
            exactPython,
            "run.py",
            "--runner",
            "toil-wdl-runner",
            "--conformance-file",
            "conformance.yaml",
            "-v",
            "1.0",
        ]
        if WDL_CONFORMANCE_TESTS_UNSUPPORTED_BY_TOIL:
            commands.append("--exclude-numbers")
            commands.append(
                ",".join([str(t) for t in WDL_CONFORMANCE_TESTS_UNSUPPORTED_BY_TOIL])
            )
        p = subprocess.run(commands, capture_output=True)

        self.check(p)

    # estimated running time: 10 minutes
    @slow
    def test_conformance_tests_v11(self, wdl_conformance_test_repo: Path) -> None:
        os.chdir(wdl_conformance_test_repo)
        commands = [
            exactPython,
            "run.py",
            "--runner",
            "toil-wdl-runner",
            "--conformance-file",
            "conformance.yaml",
            "-v",
            "1.1",
        ]
        if WDL_CONFORMANCE_TESTS_UNSUPPORTED_BY_TOIL:
            commands.append("--exclude-numbers")
            commands.append(
                ",".join([str(t) for t in WDL_CONFORMANCE_TESTS_UNSUPPORTED_BY_TOIL])
            )
        p = subprocess.run(commands, capture_output=True)

        self.check(p)

    @slow
    def test_conformance_tests_integration(
        self, wdl_conformance_test_repo: Path
    ) -> None:
        os.chdir(wdl_conformance_test_repo)
        commands = [
            exactPython,
            "run.py",
            "--runner",
            "toil-wdl-runner",
            "-v",
            "1.0",
            "--conformance-file",
            "integration.yaml",
            "--id",
            "encode,tut01,tut02,tut03,tut04",
        ]
        p = subprocess.run(
            commands,
            capture_output=True,
        )

        self.check(p)


class TestWDL:
    """Tests for Toil's MiniWDL-based implementation."""

    base_command = [exactPython, "-m", "toil.wdl.wdltoil"]

    # We inherit a testMD5sum but it is going to need Singularity or Docker
    # now. And also needs to have a WDL 1.0+ WDL file. So we replace it.
    @needs_singularity_or_docker
    def test_MD5sum(self, tmp_path: Path) -> None:
        """Test if Toil produces the same outputs as known good outputs for WDL's
        GATK tutorial #1."""
        with get_data("test/wdl/md5sum/md5sum.1.0.wdl") as wdl:
            with get_data("test/wdl/md5sum/md5sum.json") as json_file:
                result_json = subprocess.check_output(
                    self.base_command
                    + [
                        str(wdl),
                        str(json_file),
                        "-o",
                        str(tmp_path),
                        "--logDebug",
                        "--retryCount=0",
                    ]
                )
                result = json.loads(result_json)

                assert "ga4ghMd5.value" in result
                assert isinstance(result["ga4ghMd5.value"], str)
                assert os.path.exists(result["ga4ghMd5.value"])
                assert os.path.basename(result["ga4ghMd5.value"]) == "md5sum.txt"

    def test_url_to_file(self, tmp_path: Path) -> None:
        """
        Test if web URL strings can be coerced to usable Files.
        """
        with get_data("test/wdl/testfiles/url_to_file.wdl") as wdl:
            result_json = subprocess.check_output(
                self.base_command
                + [str(wdl), "-o", str(tmp_path), "--logInfo", "--retryCount=0"]
            )
            result = json.loads(result_json)

            assert "url_to_file.first_line" in result
            assert isinstance(result["url_to_file.first_line"], str)
            assert result["url_to_file.first_line"] == "chr1\t248387328"

    def test_string_file_coercion(self, tmp_path: Path) -> None:
        """
        Test if input Files can be coerced to string and back.
        """
        with get_data("test/wdl/testfiles/string_file_coercion.wdl") as wdl:
            with get_data("test/wdl/testfiles/string_file_coercion.json") as json_file:
                result_json = subprocess.check_output(
                    self.base_command
                    + [
                        str(wdl),
                        str(json_file),
                        "-o",
                        str(tmp_path),
                        "--logInfo",
                        "--retryCount=0"
                    ]
                )
                result = json.loads(result_json)

                assert "StringFileCoercion.output_file" in result

    @needs_docker
    def test_wait(self, tmp_path: Path) -> None:
        """
        Test if Bash "wait" works in WDL scripts.
        """
        with get_data("test/wdl/testfiles/wait.wdl") as wdl:
            result_json = subprocess.check_output(
                self.base_command
                + [
                    str(wdl),
                    "-o",
                    str(tmp_path),
                    "--logInfo",
                    "--retryCount=0",
                    "--wdlContainer=docker",
                ]
            )
            result = json.loads(result_json)

            assert "wait.result" in result
            assert isinstance(result["wait.result"], str)
            assert result["wait.result"] == "waited"

    @needs_singularity_or_docker
    def test_workflow_file_deletion(self, tmp_path: Path) -> None:
        """
        Test if Toil can delete non-output outputs at the end of a workflow.
        """
        # Keep a job store around to inspect for files.
        (tmp_path / "jobStore").mkdir()
        job_store = tmp_path / "jobStore" / "tree"

        # Make a working directory to run in
        work_dir = tmp_path / "workDir"
        work_dir.mkdir()
        # Make the file that will be imported from a string in the workflow
        referenced_file = work_dir / "localfile.txt"
        with referenced_file.open("w") as f:
            f.write("This file is imported by local path in the workflow")
        # Make the file to pass as input
        sent_in_file = work_dir / "sent_in.txt"
        with sent_in_file.open("w") as f:
            f.write("This file is sent in as input")

        with get_data("test/wdl/testfiles/drop_files.wdl") as wdl:
            result_json = subprocess.check_output(
                self.base_command
                + [
                    str(wdl),
                    "-o",
                    str(tmp_path / "output"),
                    "--jobStore",
                    job_store,
                    "--clean=never",
                    "--logInfo",
                    "--retryCount=0",
                    '--input={"file_in": "' + str(sent_in_file) + '"}',
                ],
                cwd=work_dir,
            )
            result = json.loads(result_json)

            # Get all the file values in the job store.
            all_file_values = set()
            for directory, _, files in os.walk(
                job_store
            ):  # can't switch to job_store.walk() until Python 3.12 is the minimum version
                for filename in files:
                    with (Path(directory) / filename).open(
                        encoding="utf-8", errors="replace"
                    ) as f:
                        all_file_values.add(f.read().rstrip())

            # Make sure the files with the right contents are in the job store and
            # the files with the wrong contents aren't anymore.
            #
            # This assumes no top-level cleanup in the main driver script.

            # These are all created inside the workflow and not output
            assert (
                "This file is imported by local path in the workflow"
                not in all_file_values
            )
            assert "This file is consumed by a task call" not in all_file_values
            assert (
                "This file is created in a task inputs section" not in all_file_values
            )
            assert "This file is created in a runtime section" not in all_file_values
            assert "This task file is not used" not in all_file_values
            assert "This file should be discarded" not in all_file_values
            assert "This file is dropped by a subworkflow" not in all_file_values
            assert "This file gets stored in a variable" not in all_file_values
            assert "This file never gets stored in a variable" not in all_file_values

            # These are created inside the workflow and output
            assert "3" in all_file_values
            assert "This file is collected as a task output twice" in all_file_values
            assert "This file should be kept" in all_file_values
            assert "This file is kept by a subworkflow" in all_file_values

            # These are sent into the workflow from the enclosing environment and
            # should not be deleted.
            assert "This file is sent in as input" in all_file_values

            # Make sure we didn't somehow delete the file sent as input
            assert sent_in_file.exists()

    @needs_singularity_or_docker
    def test_all_call_outputs(self, tmp_path: Path) -> None:
        """
        Test if Toil can collect all call outputs from a workflow that doesn't expose them.
        """
        with get_data("test/wdl/testfiles/not_enough_outputs.wdl") as wdl:
            # With no flag we don't include the call outputs
            result_json = subprocess.check_output(
                self.base_command
                + [str(wdl), "-o", str(tmp_path), "--logInfo", "--retryCount=0"]
            )
            result = json.loads(result_json)

            assert "wf.only_result" in result
            assert "wf.do_math.square" not in result
            assert "wf.do_math.cube" not in result
            assert "wf.should_never_output" not in result

            # With flag off we don't include the call outputs
            result_json = subprocess.check_output(
                self.base_command
                + [
                    str(wdl),
                    "-o",
                    str(tmp_path),
                    "--logInfo",
                    "--retryCount=0",
                    "--allCallOutputs=false",
                ]
            )
            result = json.loads(result_json)

            assert "wf.only_result" in result
            assert "wf.do_math.square" not in result
            assert "wf.do_math.cube" not in result
            assert "wf.should_never_output" not in result

            # With flag on we do include the call outputs
            result_json = subprocess.check_output(
                self.base_command
                + [
                    str(wdl),
                    "-o",
                    str(tmp_path),
                    "--logInfo",
                    "--retryCount=0",
                    "--allCallOutputs=on",
                ]
            )
            result = json.loads(result_json)

            assert "wf.only_result" in result
            assert "wf.do_math.square" in result
            assert "wf.do_math.cube" in result
            assert "wf.should_never_output" not in result

    @needs_singularity_or_docker
    def test_croo_detection(self, tmp_path: Path) -> None:
        """
        Test if Toil can detect and do something sensible with Cromwell Output Organizer workflows.
        """
        with get_data("test/wdl/testfiles/croo.wdl") as wdl:
            # With no flag we should include all task outputs
            result_json = subprocess.check_output(
                self.base_command
                + [str(wdl), "-o", str(tmp_path), "--logInfo", "--retryCount=0"]
            )
            result = json.loads(result_json)

            assert "wf.only_result" in result
            assert "wf.do_math.square" in result
            assert "wf.do_math.cube" in result
            assert "wf.should_never_output" not in result

            # With flag off we obey the WDL spec even if we're suspicious
            result_json = subprocess.check_output(
                self.base_command
                + [
                    str(wdl),
                    "-o",
                    str(tmp_path),
                    "--logInfo",
                    "--retryCount=0",
                    "--allCallOutputs=off",
                ]
            )
            result = json.loads(result_json)

            assert "wf.only_result" in result
            assert "wf.do_math.square" not in result
            assert "wf.do_math.cube" not in result
            assert "wf.should_never_output" not in result

    @needs_singularity_or_docker
    def test_caching(self, tmp_path: Path) -> None:
        """
        Test if Toil can cache task runs.
        """
        with get_data("test/wdl/testfiles/random.wdl") as wdl:
            cachedir = tmp_path / "cache"
            cachedir.mkdir()
            caching_env = dict(os.environ)
            caching_env["MINIWDL__CALL_CACHE__GET"] = "true"
            caching_env["MINIWDL__CALL_CACHE__PUT"] = "true"
            caching_env["MINIWDL__CALL_CACHE__DIR"] = str(cachedir)

            result_json = subprocess.check_output(
                self.base_command
                + [
                    str(wdl),
                    "-o",
                    str(tmp_path / "out1"),
                    "--logInfo",
                    "--retryCount=0",
                    '--inputs={"random.task_1_input": 1, "random.task_2_input": 1}',
                ],
                env=caching_env,
            )
            result_initial = json.loads(result_json)

            assert "random.value_seen" in result_initial
            assert "random.value_written" in result_initial

            result_json = subprocess.check_output(
                self.base_command
                + [
                    str(wdl),
                    "-o",
                    str(tmp_path / "out2"),
                    "--logInfo",
                    "--retryCount=0",
                    '--inputs={"random.task_1_input": 1, "random.task_2_input": 1}',
                ],
                env=caching_env,
            )
            result_cached = json.loads(result_json)

            assert "random.value_seen" in result_cached
            assert "random.value_written" in result_cached

            assert (
                result_cached["random.value_seen"]
                == result_initial["random.value_seen"]
            )
            assert (
                result_cached["random.value_written"]
                == result_initial["random.value_written"]
            )

            result_json = subprocess.check_output(
                self.base_command
                + [
                    str(wdl),
                    "-o",
                    str(tmp_path / "out3"),
                    "--logInfo",
                    "--retryCount=0",
                    '--inputs={"random.task_1_input": 2, "random.task_2_input": 1}',
                ],
                env=caching_env,
            )
            result_not_cached = json.loads(result_json)

            assert "random.value_seen" in result_not_cached
            assert "random.value_written" in result_not_cached

            assert (
                result_not_cached["random.value_seen"]
                != result_initial["random.value_seen"]
            )
            assert (
                result_not_cached["random.value_written"]
                != result_initial["random.value_written"]
            )

            result_json = subprocess.check_output(
                self.base_command
                + [
                    str(wdl),
                    "-o",
                    str(tmp_path / "out4"),
                    "--logInfo",
                    "--retryCount=0",
                    '--inputs={"random.task_1_input": 1, "random.task_2_input": 2}',
                ],
                env=caching_env,
            )
            result_part_cached = json.loads(result_json)

            assert "random.value_seen" in result_part_cached
            assert "random.value_written" in result_part_cached

            assert (
                result_part_cached["random.value_seen"]
                == result_initial["random.value_seen"]
            )
            assert (
                result_part_cached["random.value_written"]
                != result_initial["random.value_written"]
            )
            assert (
                result_part_cached["random.value_written"]
                != result_not_cached["random.value_written"]
            )

    def test_url_to_optional_file(self, tmp_path: Path) -> None:
        """
        Test if missing and error-producing URLs are handled correctly for optional File? values.
        """
        with get_data("test/wdl/testfiles/url_to_optional_file.wdl") as wdl:

            def run_for_code(code: int) -> dict[str, Any]:
                """
                Run a workflow coercing URL to File? where the URL returns the given status code.

                Return the parsed output.
                """
                logger.info("Test optional file with HTTP code %s", code)
                json_value = '{"url_to_optional_file.http_code": %d}' % code
                result_json = subprocess.check_output(
                    self.base_command
                    + [
                        str(wdl),
                        json_value,
                        "-o",
                        str(tmp_path),
                        "--logInfo",
                        "--retryCount=0",
                    ]
                )
                result = json.loads(result_json)
                return cast(dict[str, Any], json.loads(result_json))

            # Check files that exist
            result = run_for_code(200)
            assert "url_to_optional_file.out_file" in result
            assert result["url_to_optional_file.out_file"] is not None

            for code in (404, 410):
                # Check files that definitely don't
                result = run_for_code(code)
                assert "url_to_optional_file.out_file" in result
                assert result["url_to_optional_file.out_file"] is None

            for code in (402, 418, 500, 502):
                # Check that cases where the server refuses to say if the file
                # exists stop the workflow.
                with pytest.raises(subprocess.CalledProcessError):
                    run_for_code(code)

    def test_missing_output_directory(self, tmp_path: Path) -> None:
        """
        Test if Toil can run a WDL workflow into a new directory.
        """
        with get_data("test/wdl/md5sum/md5sum.1.0.wdl") as wdl:
            with get_data("test/wdl/md5sum/md5sum.json") as json_file:
                subprocess.check_call(
                    self.base_command
                    + [
                        str(wdl),
                        str(json_file),
                        "-o",
                        str(tmp_path / "does" / "not" / "exist"),
                        "--logDebug",
                        "--retryCount=0",
                    ]
                )

    @needs_singularity_or_docker
    def test_miniwdl_self_test(
        self, tmp_path: Path, extra_args: Optional[list[str]] = None
    ) -> None:
        """Test if the MiniWDL self test runs and produces the expected output."""
        with get_data("test/wdl/miniwdl_self_test/self_test.wdl") as wdl_file:
            with get_data("test/wdl/miniwdl_self_test/inputs.json") as json_file:

                result_json = subprocess.check_output(
                    self.base_command
                    + [
                        str(wdl_file),
                        str(json_file),
                        "--logDebug",
                        "-o",
                        str(tmp_path),
                        "--outputDialect",
                        "miniwdl",
                    ]
                    + (extra_args or [])
                )
                result = json.loads(result_json)

                # Expect MiniWDL-style output with a designated "dir"

                assert "dir" in result
                assert isinstance(result["dir"], str)
                out_dir = result["dir"]

                assert "outputs" in result
                assert isinstance(result["outputs"], dict)
                outputs = result["outputs"]

                assert "hello_caller.message_files" in outputs
                assert isinstance(outputs["hello_caller.message_files"], list)
                assert len(outputs["hello_caller.message_files"]) == 2
                for item in outputs["hello_caller.message_files"]:
                    # All the files should be strings in the "out" directory
                    assert isinstance(item, str), "File output must be a string"
                    assert item.startswith(
                        out_dir
                    ), "File output must be in the output directory"

                    # Look at the filename within that directory
                    name_in_out_dir = item[len(out_dir) :]

                    # Ity should contain the job name of "hello", so they are human-readable.
                    assert (
                        "hello" in name_in_out_dir
                    ), f"File output {name_in_out_dir} should have the originating task name in it"

                    # And it should not contain non-human-readable content.
                    #
                    # We use a threshold number of digits as a proxy for this, but
                    # don't try and get around this by just rolling other random
                    # strings; we want these outputs to be human-readable!!!
                    digit_count = len(
                        [c for c in name_in_out_dir if c in string.digits]
                    )
                    assert (
                        digit_count < 3
                    ), f"File output {name_in_out_dir} has {digit_count} digits, which is too many to be plausibly human-readable"

                assert "hello_caller.messages" in outputs
                assert outputs["hello_caller.messages"] == [
                    "Hello, Alyssa P. Hacker!",
                    "Hello, Ben Bitdiddle!",
                ]

    @needs_singularity_or_docker
    def test_miniwdl_self_test_by_reference(self, tmp_path: Path) -> None:
        """
        Test if the MiniWDL self test works when passing input files by URL reference.
        """
        self.test_miniwdl_self_test(
            tmp_path=tmp_path, extra_args=["--referenceInputs=True"]
        )

    @pytest.mark.integrative
    @needs_singularity_or_docker
    def test_dockstore_trs(
        self, tmp_path: Path, extra_args: Optional[list[str]] = None
    ) -> None:
        wdl_file = "#workflow/github.com/dockstore/bcc2020-training/HelloWorld:master"
        # Needs an input but doesn't provide a good one.
        json_input = json.dumps(
            {
                "hello_world.hello.myName": "https://raw.githubusercontent.com/dockstore/bcc2020-training/refs/heads/master/wdl-training/exercise1/name.txt"
            }
        )

        result_json = subprocess.check_output(
            self.base_command
            + [
                wdl_file,
                json_input,
                "--logDebug",
                "-o",
                str(tmp_path),
                "--outputDialect",
                "miniwdl",
            ]
            + (extra_args or [])
        )
        result = json.loads(result_json)

        with open(result.get("outputs", {}).get("hello_world.helloFile")) as f:
            result_text = f.read().strip()

        assert result_text == "Hello World!\nMy name is potato."

    # TODO: Should this move to the TRS/Dockstore tests file?
    @pytest.mark.integrative
    @needs_singularity_or_docker
    def test_dockstore_metrics_publication(
        self, tmp_path: Path, extra_args: Optional[list[str]] = None
    ) -> None:
        wdl_file = "#workflow/github.com/dockstore/bcc2020-training/HelloWorld:master"
        # Needs an input but doesn't provide a good one.
        json_input = json.dumps(
            {
                "hello_world.hello.myName": "https://raw.githubusercontent.com/dockstore/bcc2020-training/refs/heads/master/wdl-training/exercise1/name.txt"
            }
        )

        env = dict(os.environ)
        # Set credentials we got permission to publish from the Dockstore team,
        # and work on the staging Dockstore.
        env["TOIL_TRS_ROOT"] = "https://staging.dockstore.org"
        env["TOIL_DOCKSTORE_TOKEN"] = "99cf5578ebe94b194d7864630a86258fa3d6cedcc17d757b5dd49e64ee3b68c3"
        # Enable history for when <https://github.com/DataBiosphere/toil/pull/5258> merges
        env["TOIL_HISTORY"] = "True"

        output_log = subprocess.check_output(
            self.base_command
            + [
                wdl_file,
                json_input,
                "--logDebug",
                "-o",
                str(tmp_path),
                "--outputDialect",
                "miniwdl",
                "--publishWorkflowMetrics=current",
            ]
            + (extra_args or []),
            stderr=subprocess.STDOUT,
            env=env,
        )

        assert b'Workflow metrics were accepted by Dockstore.' in output_log

    @slow
    @needs_docker_cuda
    def test_giraffe_deepvariant(self, tmp_path: Path) -> None:
        """Test if Giraffe and GPU DeepVariant run. This could take 25 minutes."""
        # TODO: enable test if nvidia-container-runtime and Singularity are installed but Docker isn't.

        json_dir = tmp_path / "json"
        json_dir.mkdir()
        base_uri = "https://raw.githubusercontent.com/vgteam/vg_wdl/65dd739aae765f5c4dedd14f2e42d5a263f9267a"

        wdl_file = f"{base_uri}/workflows/giraffe_and_deepvariant.wdl"
        json_file = json_dir / "inputs.json"
        with json_file.open("w") as fp:
            # Write some inputs. We need to override the example inputs to use a GPU container, but that means we need absolute input URLs.
            json.dump(
                {
                    "GiraffeDeepVariant.INPUT_READ_FILE_1": f"{base_uri}/tests/small_sim_graph/reads_1.fastq.gz",
                    "GiraffeDeepVariant.INPUT_READ_FILE_2": f"{base_uri}/tests/small_sim_graph/reads_2.fastq.gz",
                    "GiraffeDeepVariant.XG_FILE": f"{base_uri}/tests/small_sim_graph/graph.xg",
                    "GiraffeDeepVariant.SAMPLE_NAME": "s0",
                    "GiraffeDeepVariant.GBWT_FILE": f"{base_uri}/tests/small_sim_graph/graph.gbwt",
                    "GiraffeDeepVariant.GGBWT_FILE": f"{base_uri}/tests/small_sim_graph/graph.gg",
                    "GiraffeDeepVariant.MIN_FILE": f"{base_uri}/tests/small_sim_graph/graph.min",
                    "GiraffeDeepVariant.DIST_FILE": f"{base_uri}/tests/small_sim_graph/graph.dist",
                    "GiraffeDeepVariant.OUTPUT_GAF": True,
                    "GiraffeDeepVariant.runDeepVariantCallVariants.in_dv_gpu_container": "google/deepvariant:1.3.0-gpu",
                },
                fp,
            )

        result_json = subprocess.check_output(
            self.base_command
            + [
                wdl_file,
                json_file,
                "-o",
                str(tmp_path / "out"),
                "--outputDialect",
                "miniwdl",
            ]
        )
        result = json.loads(result_json)

        # Expect MiniWDL-style output with a designated "dir"
        assert "dir" in result
        assert isinstance(result["dir"], str)
        out_dir = result["dir"]

        assert "outputs" in result
        assert isinstance(result["outputs"], dict)
        outputs = result["outputs"]

        # Expect a VCF file to have been written
        assert "GiraffeDeepVariant.output_vcf" in outputs
        assert isinstance(outputs["GiraffeDeepVariant.output_vcf"], str)
        assert os.path.exists(outputs["GiraffeDeepVariant.output_vcf"])

    @slow
    @needs_singularity_or_docker
    def test_giraffe(self, tmp_path: Path) -> None:
        """Test if Giraffe runs. This could take 12 minutes. Also we scale it down but it still demands lots of memory."""
        # TODO: enable test if nvidia-container-runtime and Singularity are installed but Docker isn't.
        # TODO: Reduce memory requests with custom/smaller inputs.
        # TODO: Skip if node lacks enough memory.

        base_uri = "https://raw.githubusercontent.com/vgteam/vg_wdl/65dd739aae765f5c4dedd14f2e42d5a263f9267a"
        wdl_file = f"{base_uri}/workflows/giraffe.wdl"
        json_file = f"{base_uri}/params/giraffe.json"

        result_json = subprocess.check_output(
            self.base_command
            + [
                wdl_file,
                json_file,
                "-o",
                str(tmp_path),
                "--outputDialect",
                "miniwdl",
                "--scale",
                "0.1",
                "--logDebug",
            ]
        )
        result = json.loads(result_json)

        # Expect MiniWDL-style output with a designated "dir"
        assert "dir" in result
        assert isinstance(result["dir"], str)
        out_dir = result["dir"]

        assert "outputs" in result
        assert isinstance(result["outputs"], dict)
        outputs = result["outputs"]

        # Expect a BAM file to have been written
        assert "Giraffe.output_bam" in outputs
        assert isinstance(outputs["Giraffe.output_bam"], str)
        assert os.path.exists(outputs["Giraffe.output_bam"])

    @needs_singularity_or_docker
    @needs_google_storage
    def test_gs_uri(self, tmp_path: Path) -> None:
        """Test if Toil can access Google Storage URIs."""
        with get_data("test/wdl/md5sum/md5sum.1.0.wdl") as wdl:
            with get_data("test/wdl/md5sum/md5sum-gs.json") as json_file:
                result_json = subprocess.check_output(
                    self.base_command
                    + [str(wdl), str(json_file), "-o", str(tmp_path), "--logDebug"]
                )
                result = json.loads(result_json)

                assert "ga4ghMd5.value" in result
                assert isinstance(result["ga4ghMd5.value"], str)
                assert os.path.exists(result["ga4ghMd5.value"])
                assert os.path.basename(result["ga4ghMd5.value"]) == "md5sum.txt"

    def test_check(self, tmp_path: Path) -> None:
        """Test that Toil's lint check works"""
        with get_data("test/wdl/lint_error.wdl") as wdl:
            out = subprocess.check_output(
                self.base_command + [str(wdl), "-o", str(tmp_path), "--logInfo"], stderr=subprocess.STDOUT)

            assert b'UnnecessaryQuantifier' in out

            p = subprocess.Popen(
                self.base_command + [wdl, "--strict=True", "--logCritical"], stderr=subprocess.PIPE)
            # Not actually a test assert; we need this to teach MyPy that we
            # get an stderr when we pass stderr=subprocess.PIPE.
            assert p.stderr is not None
            stderr = p.stderr.read()
            p.wait()
            assert p.returncode == 2
            assert b'Workflow did not pass linting in strict mode' in stderr

class TestWDLToilBench(unittest.TestCase):
    """Tests for Toil's MiniWDL-based implementation that don't run workflows."""

    def test_coalesce(self) -> None:
        """
        Test if WDLSectionJob can coalesce WDL decls.

        White box test; will need to be changed or removed if the WDL interpreter changes.
        """

        # Set up data structures for our fake workflow graph to pull from.
        # This has all decl-type nodes
        all_decls: set[str] = set()
        # And this has all transitive dependencies for all nodes.
        all_deps: dict[str, set[str]] = {}

        def mock_is_decl(self: Any, node_id: str) -> bool:
            """
            Replacement function to determine if a node is a decl or not.
            """
            return node_id in all_decls

        def mock_get_transitive_dependencies(self: Any, node_id: str) -> set[str]:
            """
            Replacement function to get all the transitive dependencies of a node.
            """
            return all_deps[node_id]

        # These are the only two methods coalesce_nodes calls, so we can
        # replace them to ensure our graph is used without actually needing to
        # make any WDL objects for it.
        #
        # If that changes, the test will need to change! Maybe then it will be
        # worth extracting a base type for this interface.
        with patch.object(WDLWorkflowGraph, "is_decl", mock_is_decl):
            with patch.object(
                WDLWorkflowGraph,
                "get_transitive_dependencies",
                mock_get_transitive_dependencies,
            ):
                with self.subTest(msg="Two unrelated decls can coalesce"):
                    # Set up two unrelated decls
                    all_decls = {"decl1", "decl2"}
                    all_deps = {"decl1": set(), "decl2": set()}

                    result = WDLSectionJob.coalesce_nodes(
                        ["decl1", "decl2"], WDLWorkflowGraph([])
                    )

                    # Make sure they coalesced
                    assert len(result) == 1
                    assert "decl1" in result[0]
                    assert "decl2" in result[0]

                with self.subTest(msg="A decl will not coalesce with a non-decl"):
                    all_decls = {"decl"}
                    all_deps = {"decl": set(), "nondecl": set()}

                    result = WDLSectionJob.coalesce_nodes(
                        ["decl", "nondecl"], WDLWorkflowGraph([])
                    )

                    assert len(result) == 2
                    assert len(result[0]) == 1
                    assert len(result[1]) == 1

                with self.subTest(
                    msg="Two adjacent decls with a common dependency can coalesce"
                ):
                    all_decls = {"decl1", "decl2"}
                    all_deps = {"decl1": {"base"}, "decl2": {"base"}, "base": set()}

                    result = WDLSectionJob.coalesce_nodes(
                        ["base", "decl1", "decl2"], WDLWorkflowGraph([])
                    )

                    assert len(result) == 2
                    assert "base" in result[0]
                    assert "decl1" in result[1]
                    assert "decl2" in result[1]

                with self.subTest(
                    msg="Two adjacent decls with different dependencies will not coalesce"
                ):
                    all_decls = {"decl1", "decl2"}
                    all_deps = {"decl1": {"base"}, "decl2": set(), "base": set()}

                    result = WDLSectionJob.coalesce_nodes(
                        ["base", "decl1", "decl2"], WDLWorkflowGraph([])
                    )

                    assert len(result) == 3
                    assert "base" in result[0]

                with self.subTest(
                    msg="Two adjacent decls with different successors will coalesce"
                ):
                    all_decls = {"decl1", "decl2"}
                    all_deps = {"decl1": set(), "decl2": set(), "successor": {"decl2"}}

                    result = WDLSectionJob.coalesce_nodes(
                        ["decl1", "decl2", "successor"], WDLWorkflowGraph([])
                    )

                    assert len(result) == 2
                    assert "decl1" in result[0]
                    assert "decl2" in result[0]
                    assert "successor" in result[1]

    def make_string_expr(self, to_parse: str) -> WDL.Expr.String:
        """
        Parse pseudo-WDL for testing whitespace removal.
        """

        pos = WDL.Error.SourcePosition("nowhere", "nowhere", 0, 0, 0, 0)

        parts: list[Union[str, WDL.Expr.Placeholder]] = re.split("(~{[^}]*})", to_parse)
        for i in range(1, len(parts), 2):
            parts[i] = WDL.Expr.Placeholder(pos, {}, WDL.Expr.Null(pos))

        return WDL.Expr.String(pos, parts)

    def test_remove_common_leading_whitespace(self) -> None:
        """
        Make sure leading whitespace removal works properly.
        """

        # For a single line, we remove its leading whitespace
        expr = self.make_string_expr(" a ~{b} c")
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == "a "
        assert trimmed.parts[2] == " c"

        # Whitespace removed isn't affected by totally blank lines
        expr = self.make_string_expr("    \n\n    a\n    ~{stuff}\n    b\n\n")
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == "\n\na\n"
        assert trimmed.parts[2] == "\nb\n\n"

        # Unless blank toleration is off
        expr = self.make_string_expr("    \n\n    a\n    ~{stuff}\n    b\n\n")
        trimmed = remove_common_leading_whitespace(expr, tolerate_blanks=False)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == "    \n\n    a\n    "
        assert trimmed.parts[2] == "\n    b\n\n"

        # Whitespace is still removed if the first line doesn't have it before the newline
        expr = self.make_string_expr("\n    a\n    ~{stuff}\n    b\n")
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == "\na\n"
        assert trimmed.parts[2] == "\nb\n"

        # Whitespace is not removed if actual content is dedented
        expr = self.make_string_expr("    \n\n    a\n    ~{stuff}\nuhoh\n    b\n\n")
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == "    \n\n    a\n    "
        assert trimmed.parts[2] == "\nuhoh\n    b\n\n"

        # Unless dedents are tolerated
        expr = self.make_string_expr("    \n\n    a\n    ~{stuff}\nuhoh\n    b\n\n")
        trimmed = remove_common_leading_whitespace(expr, tolerate_dedents=True)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == "\n\na\n"
        assert trimmed.parts[2] == "\nuhoh\nb\n\n"

        # Whitespace is still removed if all-whitespace lines have less of it
        expr = self.make_string_expr("\n    a\n    ~{stuff}\n  \n    b\n")
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == "\na\n"
        assert trimmed.parts[2] == "\n\nb\n"

        # Unless all-whitespace lines are not tolerated
        expr = self.make_string_expr("\n    a\n    ~{stuff}\n  \n    b\n")
        trimmed = remove_common_leading_whitespace(expr, tolerate_all_whitespace=False)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == "\n  a\n  "
        assert trimmed.parts[2] == "\n\n  b\n"

        # When mixed tabs and spaces are detected, nothing is changed.
        expr = self.make_string_expr("\n    a\n\t~{stuff}\n    b\n")
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == "\n    a\n\t"
        assert trimmed.parts[2] == "\n    b\n"

        # When mixed tabs and spaces are not in the prefix, whitespace is removed.
        expr = self.make_string_expr("\n\ta\n\t~{stuff} \n\tb\n")
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == "\na\n"
        assert trimmed.parts[2] == " \nb\n"

        # An empty string works
        expr = self.make_string_expr("")
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 1
        assert trimmed.parts[0] == ""

        # A string of only whitespace is preserved as an all-whitespece line
        expr = self.make_string_expr("\t\t\t")
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 1
        assert trimmed.parts[0] == "\t\t\t"

        # A string of only whitespace is trimmed when all-whitespace lines are not tolerated
        expr = self.make_string_expr("\t\t\t")
        trimmed = remove_common_leading_whitespace(expr, tolerate_all_whitespace=False)
        assert len(trimmed.parts) == 1
        assert trimmed.parts[0] == ""

        # An empty expression works
        expr = WDL.Expr.String(
            WDL.Error.SourcePosition("nowhere", "nowhere", 0, 0, 0, 0), []
        )
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 0

        # An expression of only placeholders works
        expr = self.make_string_expr("~{AAA}")
        trimmed = remove_common_leading_whitespace(expr)
        assert len(trimmed.parts) == 3
        assert trimmed.parts[0] == ""
        assert trimmed.parts[2] == ""

        # The command flag is preserved
        expr = self.make_string_expr(" a ~{b} c")
        trimmed = remove_common_leading_whitespace(expr)
        assert trimmed.command == False
        expr.command = True
        trimmed = remove_common_leading_whitespace(expr)
        assert trimmed.command == True

    def test_choose_human_readable_directory(self) -> None:
        """
        Test to make sure that we pick sensible but non-colliding directories to put files in.
        """

        from toil.wdl.wdltoil import (
            DirectoryNamingStateDict,
            choose_human_readable_directory,
        )

        state: DirectoryNamingStateDict = {}

        # The first time we should get  apath with the task name and without the ID
        first_chosen = choose_human_readable_directory(
            "root", "taskname", "111-222-333", state
        )
        assert first_chosen.startswith("root")
        assert "taskname" in first_chosen
        assert "111-222-333" not in first_chosen

        # If we use the same ID we should get the same result
        same_id = choose_human_readable_directory(
            "root", "taskname", "111-222-333", state
        )
        assert same_id == first_chosen

        # If we use a different ID we should get a different result still obeying the constraints
        diff_id = choose_human_readable_directory(
            "root", "taskname", "222-333-444", state
        )
        assert diff_id != first_chosen
        assert diff_id.startswith("root")
        assert "taskname" in diff_id
        assert "222-333-444" not in diff_id

    def test_uri_packing(self) -> None:
        """
        Test to make sure Toil URI packing brings through the required information.
        """

        from toil.wdl.wdltoil import pack_toil_uri, unpack_toil_uri

        # Set up a file
        file_id = FileID("fileXYZ", 123, True)
        task_path = "the_wf.the_task"
        dir_id = uuid4()
        file_basename = "thefile.txt"

        # Pack and unpack it
        uri = pack_toil_uri(file_id, task_path, dir_id, file_basename)
        unpacked = unpack_toil_uri(uri)

        # Make sure we got what we put in
        assert unpacked[0] == file_id
        assert unpacked[0].size == file_id.size
        assert unpacked[0].executable == file_id.executable

        assert unpacked[1] == task_path

        # TODO: We don't make the UUIDs back into UUID objects
        assert unpacked[2] == str(dir_id)

        assert unpacked[3] == file_basename

    def test_disk_parse(self) -> None:
        """
        Test to make sure the disk parsing is correct
        """
        # Test cromwell compatibility
        spec = "local-disk 5 SSD"
        specified_mount_point, part_size, part_suffix = parse_disks(spec, spec)
        assert specified_mount_point is None
        assert part_size == 5
        assert part_suffix == "GB"

        # Test spec conformance
        # https://github.com/openwdl/wdl/blob/e43e042104b728df1f1ad6e6145945d2b32331a6/SPEC.md?plain=1#L5072-L5082
        spec = "10"
        specified_mount_point, part_size, part_suffix = parse_disks(spec, spec)
        assert specified_mount_point is None
        assert part_size == 10
        assert part_suffix == "GiB"  # WDL spec default

        spec = "1 MB"
        specified_mount_point, part_size, part_suffix = parse_disks(spec, spec)
        assert specified_mount_point is None
        assert part_size == 1
        assert part_suffix == "MB"

        spec = "MOUNT_POINT 3"
        specified_mount_point, part_size, part_suffix = parse_disks(spec, spec)
        assert specified_mount_point == "MOUNT_POINT"
        assert part_size == 3
        assert part_suffix == "GiB"

        spec = "MOUNT_POINT 2 MB"
        specified_mount_point, part_size, part_suffix = parse_disks(spec, spec)
        assert specified_mount_point == "MOUNT_POINT"
        assert part_size == 2
        assert part_suffix == "MB"
