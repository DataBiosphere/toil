import os
import shutil
import subprocess
import tempfile
from typing import List
import unittest
import uuid
import zipfile
from urllib.request import urlretrieve

from toil.test import ToilTest, needs_docker, needs_java, slow
from toil.version import exactPython
from toil.wdl.utils import get_analyzer
from toil.wdl.wdl_functions import (basename,
                                    glob,
                                    parse_cores,
                                    parse_disk,
                                    parse_memory,
                                    process_infile,
                                    read_csv,
                                    read_tsv,
                                    select_first,
                                    size)


class BaseToilWdlTest(ToilTest):
    """Base test class for WDL tests"""
    
    def setUp(self) -> None:
        """Runs anew before each test to create farm fresh temp dirs."""
        self.output_dir = os.path.join('/tmp/', 'toil-wdl-test-' + str(uuid.uuid4()))
        os.makedirs(self.output_dir)
        
    def tearDown(self) -> None:
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
    
    @classmethod
    def setUpClass(cls) -> None:
        """Runs once for all tests."""
        super(BaseToilWdlTest, cls).setUpClass()
        cls.base_command = [exactPython, os.path.abspath("src/toil/wdl/toilwdl.py")]

class ToilWdlTest(BaseToilWdlTest):
    """
    General tests for Toil WDL
    """
    
    @needs_docker
    def testMD5sum(self):
        """Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #1."""
        wdl = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.wdl')
        inputfile = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.input')
        json = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.json')

        subprocess.check_call(self.base_command + [wdl, json, '-o', self.output_dir, '--logDebug'])
        md5sum_output = os.path.join(self.output_dir, 'md5sum.txt')
        assert os.path.exists(md5sum_output)
        os.unlink(md5sum_output)

class ToilWDLLibraryTest(BaseToilWdlTest):
    """
    Test class for WDL standard functions.
    """

    # estimated run time <1 sec
    def testFn_SelectFirst(self):
        """Test the wdl built-in functional equivalent of 'select_first()',
        which returns the first value in a list that is not None."""
        assert select_first(['somestring', 'anotherstring', None, '', 1]) == 'somestring'
        assert select_first([None, '', 1, 'somestring']) == 1
        assert select_first([2, 1, '', 'somestring', None, '']) == 2
        assert select_first(['', 2, 1, 'somestring', None, '']) == 2

    # estimated run time <1 sec
    def testFn_Size(self) -> None:
        """Test the wdl built-in functional equivalent of 'size()',
        which returns a file's size based on the path."""
        from toil.common import Toil
        from toil.job import Job
        from toil.wdl.wdl_types import WDLFile
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.clean = 'always'
        with Toil(options) as toil:
            small = process_infile(WDLFile(file_path=os.path.abspath('src/toil/test/wdl/testfiles/vocab.wdl')), toil)
            small_file = size(small)
            assert small_file >= 1800, small_file

    # estimated run time <1 sec
    def testFn_Basename(self):
        assert basename('/home/quokka/git/delete/toil/src/toil/wdl/toilwdl.py', '.py') == 'toilwdl'
        assert basename('/home/quokka/git/delete/toil/src/toil/wdl/toilwdl.py') == 'toilwdl.py'
        assert basename('toilwdl.py', '.py') == 'toilwdl'
        assert basename('toilwdl.py') == 'toilwdl.py'

    # estimated run time <1 sec
    def testFn_Glob(self):
        """Test the wdl built-in functional equivalent of 'glob()',
        which finds all files with a pattern in a directory."""
        vocab_location = glob('vocab.wdl', os.path.abspath('src/toil'))
        assert vocab_location == [os.path.abspath('src/toil/test/wdl/testfiles/vocab.wdl')], str(vocab_location)
        wdl_locations = glob('wdl_*.py', os.path.abspath('src/toil'))
        wdl_that_should_exist = [os.path.abspath('src/toil/wdl/wdl_analysis.py'),
                                 os.path.abspath('src/toil/wdl/wdl_synthesis.py'),
                                 os.path.abspath('src/toil/wdl/wdl_types.py'),
                                 os.path.abspath('src/toil/wdl/wdl_functions.py')]
        # make sure the files match the expected files
        for location in wdl_that_should_exist:
            assert location in wdl_locations, f'{str(location)} not in {str(wdl_locations)}!'
        # make sure the same number of files were found as expected
        assert len(wdl_that_should_exist) == len(wdl_locations), f'{str(len(wdl_locations))} != {str(len(wdl_that_should_exist))}'

    # estimated run time <1 sec
    def testFn_ParseMemory(self):
        """Test the wdl built-in functional equivalent of 'parse_memory()',
        which parses a specified memory input to an int output.

        The input can be a string or an int or a float and may include units
        such as 'Gb' or 'mib' as a separate argument."""
        assert parse_memory(2147483648) == 2147483648, str(parse_memory(2147483648))
        assert parse_memory('2147483648') == 2147483648, str(parse_memory(2147483648))
        assert parse_memory('2GB') == 2000000000, str(parse_memory('2GB'))
        assert parse_memory('2GiB') == 2147483648, str(parse_memory('2GiB'))
        assert parse_memory('1 GB') == 1000000000, str(parse_memory('1 GB'))
        assert parse_memory('1 GiB') == 1073741824, str(parse_memory('1 GiB'))

    # estimated run time <1 sec
    def testFn_ParseCores(self):
        """Test the wdl built-in functional equivalent of 'parse_cores()',
        which parses a specified disk input to an int output.

        The input can be a string or an int."""
        assert parse_cores(1) == 1
        assert parse_cores('1') == 1

    # estimated run time <1 sec
    def testFn_ParseDisk(self):
        """Test the wdl built-in functional equivalent of 'parse_disk()',
        which parses a specified disk input to an int output.

        The input can be a string or an int or a float and may include units
        such as 'Gb' or 'mib' as a separate argument.

        The minimum returned value is 2147483648 bytes."""
        # check minimum returned value
        assert parse_disk('1') == 2147483648, str(parse_disk('1'))
        assert parse_disk(1) == 2147483648, str(parse_disk(1))

        assert parse_disk(2200000001) == 2200000001, str(parse_disk(2200000001))
        assert parse_disk('2200000001') == 2200000001, str(parse_disk('2200000001'))
        assert parse_disk('/mnt/my_mnt 3 SSD, /mnt/my_mnt2 500 HDD') == 503000000000, str(parse_disk('/mnt/my_mnt 3 SSD, /mnt/my_mnt2 500 HDD'))
        assert parse_disk('local-disk 10 SSD') == 10000000000, str(parse_disk('local-disk 10 SSD'))
        assert parse_disk('/mnt/ 10 HDD') == 10000000000, str(parse_disk('/mnt/ 10 HDD'))
        assert parse_disk('/mnt/ 1000 HDD') == 1000000000000, str(parse_disk('/mnt/ 1000 HDD'))

    # estimated run time <1 sec
    def testPrimitives(self):
        """Test if toilwdl correctly interprets some basic declarations."""
        wdl = os.path.abspath('src/toil/test/wdl/testfiles/vocab.wdl')

        # TODO: test for all version.
        aWDL = get_analyzer(wdl)
        aWDL.analyze()

        no_declaration = ['bool1', 'int1', 'float1', 'file1', 'string1']
        collection_counter = []
        for key, declaration in aWDL.workflows_dictionary['vocabulary'].items():
            if not key.startswith('declaration'):
                continue

            name, var_type, var_expr = declaration

            if name in no_declaration:
                collection_counter.append(name)
                assert not var_expr

            if name == 'bool2':
                collection_counter.append(name)
                assert var_expr == 'True', var_expr
                assert var_type == 'Boolean', var_type
            if name == 'int2':
                collection_counter.append(name)
                assert var_expr == '1', var_expr
                assert var_type == 'Int', var_type
            if name == 'float2':
                collection_counter.append(name)
                assert var_expr == '1.1', var_expr
                assert var_type == 'Float', var_type
            if name == 'file2':
                collection_counter.append(name)
                assert var_expr == "'src/toil/test/wdl/test.tsv'", var_expr
                assert var_type == 'File', var_type
            if name == 'string2':
                collection_counter.append(name)
                assert var_expr == "'x'", var_expr
                assert var_type == 'String', var_type
        assert collection_counter == ['bool1', 'int1', 'float1', 'file1', 'string1',
                                      'bool2', 'int2', 'float2', 'file2', 'string2']
                                      
    # estimated run time <1 sec
    def testCSV(self):
        default_csv_output = [['1', '2', '3'],
                              ['4', '5', '6'],
                              ['7', '8', '9']]
        csv_array = read_csv(os.path.abspath('src/toil/test/wdl/test.csv'))
        assert csv_array == default_csv_output

    # estimated run time <1 sec
    def testTSV(self):
        default_tsv_output = [['1', '2', '3'],
                              ['4', '5', '6'],
                              ['7', '8', '9']]
        tsv_array = read_tsv(os.path.abspath('src/toil/test/wdl/test.tsv'))
        assert tsv_array == default_tsv_output

class ToilWdlIntegrationTest(BaseToilWdlTest):
    """Test class for WDL tests that need extra workflows and data downloaded"""

    gatk_data: str
    gatk_data_dir: str
    encode_data: str
    encode_data_dir: str
    wdl_data: str
    wdl_data_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        """Runs once for all tests."""
        super(ToilWdlIntegrationTest, cls).setUpClass() 

        cls.test_directory = os.path.abspath("src/toil/test/wdl/")

        cls.encode_data = os.path.join(cls.test_directory, "ENCODE_data.zip")
        cls.encode_data_dir = os.path.join(cls.test_directory, "ENCODE_data")

        cls.wdl_data = os.path.join(cls.test_directory, "wdl_templates.zip")
        cls.wdl_data_dir = os.path.join(cls.test_directory, "wdl_templates")

        cls.gatk_data = os.path.join(cls.test_directory, "GATK_data.zip")
        cls.gatk_data_dir = os.path.join(cls.test_directory, "GATK_data")

        cls.fetch_and_unzip_from_s3(filename='ENCODE_data.zip',
                                    data=cls.encode_data,
                                    data_dir=cls.encode_data_dir)

        cls.fetch_and_unzip_from_s3(filename='wdl_templates.zip',
                                    data=cls.wdl_data,
                                    data_dir=cls.wdl_data_dir)

        cls.fetch_and_unzip_from_s3(filename='GATK_data.zip',
                                    data=cls.gatk_data,
                                    data_dir=cls.gatk_data_dir)

    @classmethod
    def tearDownClass(cls) -> None:
        """We generate a lot of cruft."""
        data_dirs = [cls.gatk_data_dir, cls.wdl_data_dir, cls.encode_data_dir]
        data_zips = [cls.gatk_data, cls.wdl_data, cls.encode_data]
        encode_outputs = ['ENCFF000VOL_chr21.fq.gz',
                          'ENCFF000VOL_chr21.raw.srt.bam',
                          'ENCFF000VOL_chr21.raw.srt.bam.flagstat.qc',
                          'ENCFF000VOL_chr21.raw.srt.dup.qc',
                          'ENCFF000VOL_chr21.raw.srt.filt.nodup.srt.final.bam',
                          'ENCFF000VOL_chr21.raw.srt.filt.nodup.srt.final.bam.bai',
                          'ENCFF000VOL_chr21.raw.srt.filt.nodup.srt.final.filt.nodup.sample.15.SE.tagAlign.gz',
                          'ENCFF000VOL_chr21.raw.srt.filt.nodup.srt.final.filt.nodup.sample.15.SE.tagAlign.gz.cc.plot.pdf',
                          'ENCFF000VOL_chr21.raw.srt.filt.nodup.srt.final.filt.nodup.sample.15.SE.tagAlign.gz.cc.qc',
                          'ENCFF000VOL_chr21.raw.srt.filt.nodup.srt.final.flagstat.qc',
                          'ENCFF000VOL_chr21.raw.srt.filt.nodup.srt.final.pbc.qc',
                          'ENCFF000VOL_chr21.raw.srt.filt.nodup.srt.final.SE.tagAlign.gz',
                          'ENCFF000VOL_chr21.sai',
                          'test.txt',
                          'filter_qc.json',
                          'filter_qc.log',
                          'GRCh38_chr21_bwa.tar.gz',
                          'mapping.json',
                          'mapping.log',
                          'post_mapping.json',
                          'post_mapping.log',
                          'wdl-stats.log',
                          'xcor.json',
                          'xcor.log',
                          'toilwdl_compiled.pyc',
                          'toilwdl_compiled.py',
                          'post_processing.log',
                          'md5.log']
        for cleanup in data_dirs + data_zips + encode_outputs:
            if os.path.isdir(cleanup):
                shutil.rmtree(cleanup)
            elif os.path.exists(cleanup):
                os.remove(cleanup)
        super(ToilWdlIntegrationTest, cls).tearDownClass()

    # estimated run time 27 sec
    @slow
    @needs_java
    def testTut01(self):
        """Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #1."""
        wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl")
        json = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json")
        ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/output/")

        subprocess.check_call(self.base_command + [wdl, json, '-o', self.output_dir])

        compare_runs(self.output_dir, ref_dir)

    # estimated run time 28 sec
    @slow
    @needs_java
    def testTut02(self):
        """Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #2."""
        wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t02/simpleVariantSelection.wdl")
        json = os.path.abspath("src/toil/test/wdl/wdl_templates/t02/simpleVariantSelection_inputs.json")
        ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t02/output/")

        subprocess.check_call(self.base_command + [wdl, json, '-o', self.output_dir])

        compare_runs(self.output_dir, ref_dir)

    # estimated run time 60 sec
    @slow
    @needs_java
    def testTut03(self):
        """Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #3."""
        wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t03/simpleVariantDiscovery.wdl")
        json = os.path.abspath("src/toil/test/wdl/wdl_templates/t03/simpleVariantDiscovery_inputs.json")
        ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t03/output/")

        subprocess.check_call(self.base_command + [wdl, json, '-o', self.output_dir])

        compare_runs(self.output_dir, ref_dir)

    # estimated run time 175 sec
    @slow
    @needs_java
    @unittest.skip('broken; see: https://github.com/DataBiosphere/toil/issues/3339')
    def testTut04(self):
        """Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #4."""
        wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/jointCallingGenotypes.wdl")
        json = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/jointCallingGenotypes_inputs.json")
        ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/output/")

        subprocess.check_call(self.base_command + [wdl, json, '-o', self.output_dir])

        compare_runs(self.output_dir, ref_dir)

    # estimated run time 80 sec
    @slow
    @needs_docker
    def testENCODE(self):
        """Test if toilwdl produces the same outputs as known good outputs for
        a short ENCODE run."""
        wdl = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testENCODE/encode_mapping_workflow.wdl")
        json = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testENCODE/encode_mapping_workflow.wdl.json")
        ref_dir = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testENCODE/output/")

        subprocess.check_call(
            self.base_command + [wdl, json, '--docker_user=None', '--out_dir', self.output_dir])

        compare_runs(self.output_dir, ref_dir)

    # estimated run time 2 sec
    def testPipe(self):
        """Test basic bash input functionality with a pipe."""
        wdl = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testPipe/call.wdl")
        json = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testPipe/call.json")
        ref_dir = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testPipe/output/")

        subprocess.check_call(
            self.base_command + [wdl, json, '--out_dir', self.output_dir])

        compare_runs(self.output_dir, ref_dir)

    # estimated run time <1 sec
    def testJSON(self):
        default_json_dict_output = {
            'helloHaplotypeCaller.haplotypeCaller.RefIndex': '"src/toil/test/wdl/GATK_data/ref/human_g1k_b37_20.fasta.fai"',
            'helloHaplotypeCaller.haplotypeCaller.sampleName': '"WDL_tut1_output"',
            'helloHaplotypeCaller.haplotypeCaller.inputBAM': '"src/toil/test/wdl/GATK_data/inputs/NA12878_wgs_20.bam"',
            'helloHaplotypeCaller.haplotypeCaller.bamIndex': '"src/toil/test/wdl/GATK_data/inputs/NA12878_wgs_20.bai"',
            'helloHaplotypeCaller.haplotypeCaller.GATK': '"src/toil/test/wdl/GATK_data/gatk-package-4.1.9.0-local.jar"',
            'helloHaplotypeCaller.haplotypeCaller.RefDict': '"src/toil/test/wdl/GATK_data/ref/human_g1k_b37_20.dict"',
            'helloHaplotypeCaller.haplotypeCaller.RefFasta': '"src/toil/test/wdl/GATK_data/ref/human_g1k_b37_20.fasta"'}

        from toil.wdl.utils import dict_from_JSON
        json_dict = dict_from_JSON("src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json")
        assert json_dict == default_json_dict_output, (
                str(json_dict) + '\nAssertionError: ' + str(default_json_dict_output))

    # estimated run time <1 sec
    def test_size_large(self) -> None:
        """Test the wdl built-in functional equivalent of 'size()',
        which returns a file's size based on the path, on a large file."""
        from toil.common import Toil
        from toil.job import Job
        from toil.wdl.wdl_types import WDLFile
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.clean = 'always'
        with Toil(options) as toil:
            large = process_infile(WDLFile(file_path=self.encode_data), toil)
            larger_file = size(large)
            larger_file_in_mb = size(large, 'mb')
            assert larger_file >= 70000000, larger_file
            assert larger_file_in_mb >= 70, larger_file_in_mb

    @classmethod
    def fetch_and_unzip_from_s3(cls, filename, data, data_dir):
        if not os.path.exists(data):
            s3_loc = os.path.join('http://toil-datasets.s3.amazonaws.com/', filename)
            urlretrieve(s3_loc, data)
        # extract the compressed data if not already extracted
        if not os.path.exists(data_dir):
            with zipfile.ZipFile(data, 'r') as zip_ref:
                zip_ref.extractall(cls.test_directory)


def compare_runs(output_dir, ref_dir):
    """
    Takes two directories and compares all of the files between those two
    directories, asserting that they match.

    - Ignores outputs.txt, which contains a list of the outputs in the folder.
    - Compares line by line, unless the file is a .vcf file.
    - Ignores potentially date-stamped comments (lines starting with '#').
    - Ignores quality scores in .vcf files and only checks that they found
      the same variants.  This is due to assumed small observed rounding
      differences between systems.

    :param ref_dir: The first directory to compare (with output_dir).
    :param output_dir: The second directory to compare (with ref_dir).
    """
    reference_output_files = os.listdir(ref_dir)
    for file in reference_output_files:
        if file not in ('outputs.txt', '__pycache__'):
            test_output_files = os.listdir(output_dir)
            filepath = os.path.join(ref_dir, file)
            with open(filepath) as default_file:
                good_data = []
                for line in default_file:
                    if not line.startswith('#'):
                        good_data.append(line)
                for test_file in test_output_files:
                    if file == test_file:
                        test_filepath = os.path.join(output_dir, file)
                        if file.endswith(".vcf"):
                            compare_vcf_files(filepath1=filepath,
                                              filepath2=test_filepath)
                        else:
                            with open(test_filepath) as test_file:
                                test_data = []
                                for line in test_file:
                                    if not line.startswith('#'):
                                        test_data.append(line)
                            assert good_data == test_data, "File does not match: %r" % file


def compare_vcf_files(filepath1, filepath2):
    """
    Asserts that two .vcf files contain the same variant findings.

    - Ignores potentially date-stamped comments (lines starting with '#').
    - Ignores quality scores in .vcf files and only checks that they found
      the same variants.  This is due to assumed small observed rounding
      differences between systems.

    VCF File Column Contents:
    1: #CHROM
    2: POS
    3: ID
    4: REF
    5: ALT
    6: QUAL
    7: FILTER
    8: INFO

    :param filepath1: First .vcf file to compare.
    :param filepath2: Second .vcf file to compare.
    """
    with open(filepath1) as default_file:
        good_data = []
        for line in default_file:
            line = line.strip()
            if not line.startswith('#'):
                good_data.append(line.split('\t'))

    with open(filepath2) as test_file:
        test_data = []
        for line in test_file:
            line = line.strip()
            if not line.startswith('#'):
                test_data.append(line.split('\t'))

    for i in range(len(test_data)):
        if test_data[i] != good_data[i]:
            for j in range(len(test_data[i])):
                # Only compare chromosome, position, ID, reference, and alts.
                # Quality score may vary (<1%) between systems because of
                # (assumed) rounding differences.  Same for the "info" sect.
                if j < 5:
                    if j == 4:
                        if test_data[i][j].startswith('*,'):
                            test_data[i][j] = test_data[i][j][2:]
                        if good_data[i][j].startswith('*,'):
                            good_data[i][j] = good_data[i][j][2:]
                    assert test_data[i][j] == good_data[i][j], f"\nInconsistent VCFs: {filepath1} != {filepath2}\n" \
                                                               f" - {test_data[i][j]} != {good_data[i][j]}\n" \
                                                               f" - Line: {i} Column: {j}"


if __name__ == "__main__":
    unittest.main()  # run all tests
