import json
import os
import re
import shutil
import subprocess
import unittest
from uuid import uuid4
from typing import Optional

from unittest.mock import patch
from typing import Any, Dict, List, Set

import logging
import pytest

from toil.provisioners import cluster_factory
from toil.test import (ToilTest,
                       needs_docker_cuda,
                       needs_google_storage,
                       needs_singularity_or_docker,
                       needs_wdl,
                       slow, integrative)
from toil.version import exactPython
from toil.wdl.wdltoil import WDLSectionJob, WDLWorkflowGraph, remove_common_leading_whitespace

import WDL.Expr
import WDL.Error

logger = logging.getLogger(__name__)

@needs_wdl
class BaseWDLTest(ToilTest):
    """Base test class for WDL tests."""

    def setUp(self) -> None:
        """Runs anew before each test to create farm fresh temp dirs."""
        self.output_dir = os.path.join('/tmp/', 'toil-wdl-test-' + str(uuid4()))
        os.makedirs(self.output_dir)

    def tearDown(self) -> None:
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)


WDL_CONFORMANCE_TEST_REPO = "https://github.com/DataBiosphere/wdl-conformance-tests.git"
WDL_CONFORMANCE_TEST_COMMIT = "01401a46bc0e60240fb2b69af4b978d0a5bd8fc8"
# These tests are known to require things not implemented by
# Toil and will not be run in CI.
WDL_CONFORMANCE_TESTS_UNSUPPORTED_BY_TOIL= [
    16, # Basic object test (deprecated and removed in 1.1); MiniWDL and toil-wdl-runner do not support Objects, so this will fail if ran by them
    21, # Parser: expression placeholders in strings in conditional expressions in 1.0, Cromwell style; Fails with MiniWDL and toil-wdl-runner
    64, # Legacy test for as_map_as_input; It looks like MiniWDL does not have the function as_map()
    77, # Test that array cannot coerce to a string. WDL 1.1 does not allow compound types to coerce into a string. This should return a TypeError.
]

class WDLConformanceTests(BaseWDLTest):
    """
    WDL conformance tests for Toil.
    """
    wdl_dir = "wdl-conformance-tests"

    @classmethod
    def setUpClass(cls) -> None:

        p = subprocess.Popen(
            f"git clone {WDL_CONFORMANCE_TEST_REPO} {cls.wdl_dir} && cd {cls.wdl_dir} && git checkout {WDL_CONFORMANCE_TEST_COMMIT}",
            shell=True,
        )

        p.communicate()

        if p.returncode > 0:
            raise RuntimeError("Could not clone WDL conformance tests")

        os.chdir(cls.wdl_dir)

        cls.base_command = [exactPython, "run.py", "--runner", "toil-wdl-runner"]

    def check(self, p: subprocess.CompletedProcess) -> None:
        """
        Make sure a call completed or explain why it failed.
        """

        if p.returncode != 0:
            logger.error("Failed process standard output: %s", p.stdout.decode('utf-8', errors='replace'))
            logger.error("Failed process standard error: %s", p.stderr.decode('utf-8', errors='replace'))

        p.check_returncode()

    # estimated running time: 10 minutes
    @slow
    def test_conformance_tests_v10(self):
        command = self.base_command + ["-v", "1.0"]
        if WDL_CONFORMANCE_TESTS_UNSUPPORTED_BY_TOIL:
            command.append("--exclude-numbers")
            command.append(",".join([str(t) for t in WDL_CONFORMANCE_TESTS_UNSUPPORTED_BY_TOIL]))
        p = subprocess.run(command, capture_output=True)

        self.check(p)

    # estimated running time: 10 minutes
    @slow
    def test_conformance_tests_v11(self):
        command = self.base_command + ["-v", "1.1"]
        if WDL_CONFORMANCE_TESTS_UNSUPPORTED_BY_TOIL:
            command.append("--exclude-numbers")
            command.append(",".join([str(t) for t in WDL_CONFORMANCE_TESTS_UNSUPPORTED_BY_TOIL]))
        p = subprocess.run(command, capture_output=True)

        self.check(p)

    @slow
    def test_conformance_tests_integration(self):
        ids_to_run = "encode,tut01,tut02,tut03,tut04"
        p = subprocess.run(self.base_command + ["-v", "1.0", "--conformance-file", "integration.yaml", "--id", ids_to_run], capture_output=True)

        self.check(p)

    @classmethod
    def tearDownClass(cls) -> None:
        upper_dir = os.path.dirname(os.getcwd())
        os.chdir(upper_dir)
        shutil.rmtree("wdl-conformance-tests")


class WDLTests(BaseWDLTest):
    """Tests for Toil's MiniWDL-based implementation."""

    @classmethod
    def setUpClass(cls) -> None:
        """Runs once for all tests."""
        cls.base_command = [exactPython, '-m', 'toil.wdl.wdltoil']

    # We inherit a testMD5sum but it is going to need Singularity or Docker
    # now. And also needs to have a WDL 1.0+ WDL file. So we replace it.
    @needs_singularity_or_docker
    def test_MD5sum(self):
        """Test if Toil produces the same outputs as known good outputs for WDL's
        GATK tutorial #1."""
        wdl = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.1.0.wdl')
        json_file = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.json')

        result_json = subprocess.check_output(
            self.base_command + [wdl, json_file, '-o', self.output_dir, '--logDebug', '--retryCount=0'])
        result = json.loads(result_json)

        assert 'ga4ghMd5.value' in result
        assert isinstance(result['ga4ghMd5.value'], str)
        assert os.path.exists(result['ga4ghMd5.value'])
        assert os.path.basename(result['ga4ghMd5.value']) == 'md5sum.txt'

    def test_missing_output_directory(self):
        """
        Test if Toil can run a WDL workflow into a new directory.
        """
        wdl = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.1.0.wdl')
        json_file = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.json')
        subprocess.check_call(self.base_command + [wdl, json_file, '-o', os.path.join(self.output_dir, "does", "not", "exist"), '--logDebug', '--retryCount=0'])

    @needs_singularity_or_docker
    def test_miniwdl_self_test(self, extra_args: Optional[List[str]] = None) -> None:
        """Test if the MiniWDL self test runs and produces the expected output."""
        wdl_file = os.path.abspath('src/toil/test/wdl/miniwdl_self_test/self_test.wdl')
        json_file = os.path.abspath('src/toil/test/wdl/miniwdl_self_test/inputs.json')

        result_json = subprocess.check_output(
            self.base_command + [wdl_file, json_file, '--logDebug', '-o', self.output_dir, '--outputDialect',
                                 'miniwdl'] + (extra_args or []))
        result = json.loads(result_json)

        # Expect MiniWDL-style output with a designated "dir"

        assert 'dir' in result
        assert isinstance(result['dir'], str)
        out_dir = result['dir']

        assert 'outputs' in result
        assert isinstance(result['outputs'], dict)
        outputs = result['outputs']

        assert 'hello_caller.message_files' in outputs
        assert isinstance(outputs['hello_caller.message_files'], list)
        assert len(outputs['hello_caller.message_files']) == 2
        for item in outputs['hello_caller.message_files']:
            # All the files should be strings in the "out" directory
            assert isinstance(item, str)
            assert item.startswith(out_dir)

        assert 'hello_caller.messages' in outputs
        assert outputs['hello_caller.messages'] == ["Hello, Alyssa P. Hacker!", "Hello, Ben Bitdiddle!"]

    @needs_singularity_or_docker
    def test_miniwdl_self_test_by_reference(self) -> None:
        """
        Test if the MiniWDL self test works when passing input files by URL reference.
        """
        self.test_miniwdl_self_test(extra_args=["--referenceInputs=True"])

    @slow
    @needs_docker_cuda
    def test_giraffe_deepvariant(self):
        """Test if Giraffe and GPU DeepVariant run. This could take 25 minutes."""
        # TODO: enable test if nvidia-container-runtime and Singularity are installed but Docker isn't.

        json_dir = self._createTempDir()
        base_uri = 'https://raw.githubusercontent.com/vgteam/vg_wdl/65dd739aae765f5c4dedd14f2e42d5a263f9267a'

        wdl_file = f"{base_uri}/workflows/giraffe_and_deepvariant.wdl"
        json_file = os.path.abspath(os.path.join(json_dir, 'inputs.json'))
        with open(json_file, 'w') as fp:
            # Write some inputs. We need to override the example inputs to use a GPU container, but that means we need absolute input URLs.
            json.dump(fp, {
                "GiraffeDeepVariant.INPUT_READ_FILE_1": f"{base_uri}/tests/small_sim_graph/reads_1.fastq.gz",
                "GiraffeDeepVariant.INPUT_READ_FILE_2": f"{base_uri}/tests/small_sim_graph/reads_2.fastq.gz",
                "GiraffeDeepVariant.XG_FILE": f"{base_uri}/tests/small_sim_graph/graph.xg",
                "GiraffeDeepVariant.SAMPLE_NAME": "s0",
                "GiraffeDeepVariant.GBWT_FILE": f"{base_uri}/tests/small_sim_graph/graph.gbwt",
                "GiraffeDeepVariant.GGBWT_FILE": f"{base_uri}/tests/small_sim_graph/graph.gg",
                "GiraffeDeepVariant.MIN_FILE": f"{base_uri}/tests/small_sim_graph/graph.min",
                "GiraffeDeepVariant.DIST_FILE": f"{base_uri}/tests/small_sim_graph/graph.dist",
                "GiraffeDeepVariant.OUTPUT_GAF": True,
                "GiraffeDeepVariant.runDeepVariantCallVariants.in_dv_gpu_container": "google/deepvariant:1.3.0-gpu"
            })

        result_json = subprocess.check_output(
            self.base_command + [wdl_file, json_file, '-o', self.output_dir, '--outputDialect', 'miniwdl'])
        result = json.loads(result_json)

        # Expect MiniWDL-style output with a designated "dir"
        assert 'dir' in result
        assert isinstance(result['dir'], str)
        out_dir = result['dir']

        assert 'outputs' in result
        assert isinstance(result['outputs'], dict)
        outputs = result['outputs']

        # Expect a VCF file to have been written
        assert 'GiraffeDeepVariant.output_vcf' in outputs
        assert isinstance(outputs['GiraffeDeepVariant.output_vcf'], str)
        assert os.path.exists(outputs['GiraffeDeepVariant.output_vcf'])

    @slow
    @needs_singularity_or_docker
    def test_giraffe(self):
        """Test if Giraffe runs. This could take 12 minutes. Also we scale it down but it still demands lots of memory."""
        # TODO: enable test if nvidia-container-runtime and Singularity are installed but Docker isn't.

        json_dir = self._createTempDir()
        base_uri = 'https://raw.githubusercontent.com/vgteam/vg_wdl/65dd739aae765f5c4dedd14f2e42d5a263f9267a'
        wdl_file = f"{base_uri}/workflows/giraffe.wdl"
        json_file = f"{base_uri}/params/giraffe.json"

        result_json = subprocess.check_output(
            self.base_command + [wdl_file, json_file, '-o', self.output_dir, '--outputDialect', 'miniwdl', '--scale',
                                 '0.1'])
        result = json.loads(result_json)

        # Expect MiniWDL-style output with a designated "dir"
        assert 'dir' in result
        assert isinstance(result['dir'], str)
        out_dir = result['dir']

        assert 'outputs' in result
        assert isinstance(result['outputs'], dict)
        outputs = result['outputs']

        # Expect a BAM file to have been written
        assert 'Giraffe.output_bam' in outputs
        assert isinstance(outputs['Giraffe.output_bam'], str)
        assert os.path.exists(outputs['Giraffe.output_bam'])

    @needs_singularity_or_docker
    @needs_google_storage
    def test_gs_uri(self):
        """Test if Toil can access Google Storage URIs."""
        wdl = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.1.0.wdl')
        json_file = os.path.abspath('src/toil/test/wdl/md5sum/md5sum-gs.json')

        result_json = subprocess.check_output(self.base_command + [wdl, json_file, '-o', self.output_dir, '--logDebug'])
        result = json.loads(result_json)

        assert 'ga4ghMd5.value' in result
        assert isinstance(result['ga4ghMd5.value'], str)
        assert os.path.exists(result['ga4ghMd5.value'])
        assert os.path.basename(result['ga4ghMd5.value']) == 'md5sum.txt'


class WDLToilBenchTests(ToilTest):
    """Tests for Toil's MiniWDL-based implementation that don't run workflows."""

    def test_coalesce(self):
        """
        Test if WDLSectionJob can coalesce WDL decls.

        White box test; will need to be changed or removed if the WDL interpreter changes.
        """

        # Set up data structures for our fake workflow graph to pull from.
        # This has all decl-type nodes
        all_decls: Set[str] = set()
        # And this has all transitive dependencies for all nodes.
        all_deps: Dict[str, Set[str]] = {}

        def mock_is_decl(self: Any, node_id: str) -> bool:
            """
            Replacement function to determine if a node is a decl or not.
            """
            return node_id in all_decls

        def mock_get_transitive_dependencies(self: Any, node_id: str) -> Set[str]:
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
        with patch.object(WDLWorkflowGraph, 'is_decl', mock_is_decl):
            with patch.object(WDLWorkflowGraph, 'get_transitive_dependencies', mock_get_transitive_dependencies):
                with self.subTest(msg="Two unrelated decls can coalesce"):
                    # Set up two unrelated decls
                    all_decls = {"decl1", "decl2"}
                    all_deps = {
                        "decl1": set(),
                        "decl2": set()
                    }

                    result = WDLSectionJob.coalesce_nodes(["decl1", "decl2"], WDLWorkflowGraph([]))

                    # Make sure they coalesced
                    assert len(result) == 1
                    assert "decl1" in result[0]
                    assert "decl2" in result[0]

                with self.subTest(msg="A decl will not coalesce with a non-decl"):
                    all_decls = {"decl"}
                    all_deps = {
                        "decl": set(),
                        "nondecl": set()
                    }

                    result = WDLSectionJob.coalesce_nodes(["decl", "nondecl"], WDLWorkflowGraph([]))

                    assert len(result) == 2
                    assert len(result[0]) == 1
                    assert len(result[1]) == 1

                with self.subTest(msg="Two adjacent decls with a common dependency can coalesce"):
                    all_decls = {"decl1", "decl2"}
                    all_deps = {
                        "decl1": {"base"},
                        "decl2": {"base"},
                        "base": set()
                    }

                    result = WDLSectionJob.coalesce_nodes(["base", "decl1", "decl2"], WDLWorkflowGraph([]))

                    assert len(result) == 2
                    assert "base" in result[0]
                    assert "decl1" in result[1]
                    assert "decl2" in result[1]

                with self.subTest(msg="Two adjacent decls with different dependencies will not coalesce"):
                    all_decls = {"decl1", "decl2"}
                    all_deps = {
                        "decl1": {"base"},
                        "decl2": set(),
                        "base": set()
                    }

                    result = WDLSectionJob.coalesce_nodes(["base", "decl1", "decl2"], WDLWorkflowGraph([]))

                    assert len(result) == 3
                    assert "base" in result[0]

                with self.subTest(msg="Two adjacent decls with different successors will coalesce"):
                    all_decls = {"decl1", "decl2"}
                    all_deps = {
                        "decl1": set(),
                        "decl2": set(),
                        "successor": {"decl2"}
                    }

                    result = WDLSectionJob.coalesce_nodes(["decl1", "decl2", "successor"], WDLWorkflowGraph([]))

                    assert len(result) == 2
                    assert "decl1" in result[0]
                    assert "decl2" in result[0]
                    assert "successor" in result[1]


    def make_string_expr(self, to_parse: str) -> WDL.Expr.String:
        """
        Parse pseudo-WDL for testing whitespace removal.
        """

        pos = WDL.Error.SourcePosition("nowhere", "nowhere", 0, 0, 0, 0)

        parts: List[Union[str, WDL.Expr.Placeholder]] = re.split("(~{[^}]*})", to_parse)
        for i in range(1, len(parts), 2):
            parts[i] = WDL.Expr.Placeholder(pos, {}, WDL.Expr.Null(pos))

        return WDL.Expr.String(pos, parts)

    def test_remove_common_leading_whitespace(self):
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
        expr = WDL.Expr.String(WDL.Error.SourcePosition("nowhere", "nowhere", 0, 0, 0, 0), [])
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




if __name__ == "__main__":
    unittest.main()  # run all tests
