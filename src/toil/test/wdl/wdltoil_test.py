import json
import os
import shutil
import subprocess
import unittest
import uuid
import zipfile
from urllib.request import urlretrieve

import pytest

from toil.test import ToilTest, needs_docker, needs_docker_cuda, needs_google_storage, needs_java, needs_singularity_or_docker, slow
from toil.version import exactPython
# Don't import the test case directly or pytest will test it again.
import toil.test.wdl.toilwdlTest


class ToilConformanceTests(toil.test.wdl.toilwdlTest.BaseToilWdlTest):
    """
    New WDL conformance tests for Toil
    """
    wdl_dir = "wdl-conformance-tests"
    @classmethod
    def setUpClass(cls) -> None:

        url = "https://github.com/DataBiosphere/wdl-conformance-tests.git"
        commit = "032fb99a1458d456b6d5f17d27928469ec1a1c68"

        p = subprocess.Popen(
            f"git clone {url} {cls.wdl_dir} && cd {cls.wdl_dir} && git checkout {commit}",
            shell=True,
        )

        p.communicate()

        if p.returncode > 0:
            raise RuntimeError

        os.chdir(cls.wdl_dir)

        cls.base_command = [exactPython, "run.py", "--runner", "toil-wdl-runner"]

    # estimated running time: 2 minutes
    @slow
    def test_conformance_tests_v10(self):
        tests_to_run = "0,1,5-7,9-15,17,22-24,26,28-30,32-40,53,57-59,62,67-69"
        p = subprocess.run(self.base_command + ["-v", "1.0", "-n", tests_to_run], capture_output=True)

        if p.returncode != 0:
            print(p.stdout)

        p.check_returncode()

    # estimated running time: 2 minutes
    @slow
    def test_conformance_tests_v11(self):
        tests_to_run = "2-11,13-15,17-20,22-24,26,29,30,32-40,53,57-59,62,67-69"
        p = subprocess.run(self.base_command + ["-v", "1.1", "-n", tests_to_run], capture_output=True)

        if p.returncode != 0:
            print(p.stdout)

        p.check_returncode()

    @classmethod
    def tearDownClass(cls) -> None:
        upper_dir = os.path.dirname(os.getcwd())
        os.chdir(upper_dir)
        shutil.rmtree("wdl-conformance-tests")


class WdlToilTest(toil.test.wdl.toilwdlTest.ToilWdlTest):
    """
    Version of the old Toil WDL tests that tests the new MiniWDL-based implementation.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """Runs once for all tests."""
        cls.base_command = [exactPython, '-m', 'toil.wdl.wdltoil']

    # We inherit a testMD5sum but it is going to need Singularity and not
    # Docker now. And also needs to have a WDL 1.0+ WDL file. So we replace it.
    @needs_singularity_or_docker
    def testMD5sum(self):
        """Test if Toil produces the same outputs as known good outputs for WDL's
        GATK tutorial #1."""
        wdl = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.1.0.wdl')
        json_file = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.json')

        result_json = subprocess.check_output(self.base_command + [wdl, json_file, '-o', self.output_dir, '--logDebug'])
        result = json.loads(result_json)

        assert 'ga4ghMd5.value' in result
        assert isinstance(result['ga4ghMd5.value'], str)
        assert os.path.exists(result['ga4ghMd5.value'])
        assert os.path.basename(result['ga4ghMd5.value']) == 'md5sum.txt'

    def test_empty_file_path(self):
        """Test if empty File type inputs are protected against"""
        wdl = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.1.0.wdl')
        json_file = os.path.abspath('src/toil/test/wdl/md5sum/empty_file.json')

        p = subprocess.Popen(self.base_command + [wdl, json_file, '-o', self.output_dir, '--logDebug'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        retval = p.wait()

        assert retval != 0
        assert b'Could not find' in stderr

    @needs_singularity_or_docker
    def test_miniwdl_self_test(self):
        """Test if the MiniWDL self test runs and produces the expected output."""
        wdl_file = os.path.abspath('src/toil/test/wdl/miniwdl_self_test/self_test.wdl')
        json_file = os.path.abspath('src/toil/test/wdl/miniwdl_self_test/inputs.json')

        result_json = subprocess.check_output(self.base_command + [wdl_file, json_file, '-o', self.output_dir, '--outputDialect', 'miniwdl'])
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
            # All the files should be strings in the "out" direcotry
            assert isinstance(item, str)
            assert item.startswith(out_dir)

        assert 'hello_caller.messages' in outputs
        assert outputs['hello_caller.messages'] == ["Hello, Alyssa P. Hacker!", "Hello, Ben Bitdiddle!"]

    @slow
    @needs_docker_cuda
    def test_giraffe_deepvariant(self):
        """Test if Giraffe and CPU DeepVariant run. This could take 25 minutes."""
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

        result_json = subprocess.check_output(self.base_command + [wdl_file, json_file, '-o', self.output_dir, '--outputDialect', 'miniwdl'])
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
        """Test if Giraffe runs. This could take 12 minutes. Also we scale it down."""
        # TODO: enable test if nvidia-container-runtime and Singularity are installed but Docker isn't.

        json_dir = self._createTempDir()
        base_uri = 'https://raw.githubusercontent.com/vgteam/vg_wdl/65dd739aae765f5c4dedd14f2e42d5a263f9267a'
        wdl_file = f"{base_uri}/workflows/giraffe.wdl"
        json_file = f"{base_uri}/params/giraffe.json"

        result_json = subprocess.check_output(self.base_command + [wdl_file, json_file, '-o', self.output_dir, '--outputDialect', 'miniwdl', '--scale', '0.1'])
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

    def test_empty_file_path(self):
        """Test if empty File type inputs are protected against"""
        wdl = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.1.0.wdl')
        json_file = os.path.abspath('src/toil/test/wdl/md5sum/empty_file.json')

        p = subprocess.Popen(self.base_command + [wdl, json_file, '-o', self.output_dir, '--logDebug'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        retval = p.wait()

        assert retval != 0
        assert b'Could not find' in stderr

if __name__ == "__main__":
    unittest.main()  # run all tests
