from __future__ import absolute_import
from six import iteritems
import unittest
import os
import subprocess
from toil.version import exactPython
import toil.wdl.wdl_parser as wdl_parser
from toil.wdl.wdl_analysis import AnalyzeWDL
from toil.wdl.wdl_synthesis import SynthesizeWDL
from toil.wdl.wdl_functions import generate_docker_bashscript_file
from toil.wdl.wdl_functions import select_first
from toil.wdl.wdl_functions import size
from toil.wdl.wdl_functions import glob
from toil.wdl.wdl_functions import process_and_read_file
from toil.wdl.wdl_functions import process_infile
from toil.wdl.wdl_functions import process_outfile
from toil.wdl.wdl_functions import abspath_file
from toil.wdl.wdl_functions import combine_dicts
from toil.wdl.wdl_functions import parse_memory
from toil.wdl.wdl_functions import parse_cores
from toil.wdl.wdl_functions import parse_disk
from toil.wdl.wdl_functions import defined
from toil.wdl.wdl_functions import read_tsv
from toil.wdl.wdl_functions import read_csv
from toil.wdl.wdl_functions import basename
from toil.test import ToilTest, slow, needs_docker
from toil import urlretrieve
import zipfile
import shutil
import uuid


class ToilWdlIntegrationTest(ToilTest):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        """Runs anew before each test to create farm fresh temp dirs."""
        self.output_dir = os.path.join('/tmp/', 'toil-wdl-test-' + str(uuid.uuid4()))
        os.makedirs(self.output_dir)

    @classmethod
    def setUpClass(cls):
        """Runs once for all tests."""
        cls.program = os.path.abspath("src/toil/wdl/toilwdl.py")

        cls.test_directory = os.path.abspath("src/toil/test/wdl/")

        cls.encode_data = os.path.join(cls.test_directory, "ENCODE_data.zip")
        cls.encode_data_dir = os.path.join(cls.test_directory, "ENCODE_data")

        cls.wdl_data = os.path.join(cls.test_directory, "wdl_templates.zip")
        cls.wdl_data_dir = os.path.join(cls.test_directory, "wdl_templates")

        cls.gatk_data = os.path.join(cls.test_directory, "GATK_data.zip")
        cls.gatk_data_dir = os.path.join(cls.test_directory, "GATK_data")

        # GATK tests will not run on jenkins b/c GATK.jar needs Java 7
        # and jenkins only has Java 6 (12-16-2017).
        # Set this to true to run the GATK integration tests locally.
        cls.manual_integration_tests = False

        # Delete the test datasets after running the tests.
        # Jenkins requires this to not error on "untracked files".
        # Set to true if running tests locally and you don't want to
        # redownload the data each time you run the test.
        cls.jenkins = True

        cls.fetch_and_unzip_from_s3(filename='ENCODE_data.zip',
                                    data=cls.encode_data,
                                    data_dir=cls.encode_data_dir)

        cls.fetch_and_unzip_from_s3(filename='wdl_templates.zip',
                                    data=cls.wdl_data,
                                    data_dir=cls.wdl_data_dir)

        # these tests require Java 7 (GATK.jar); jenkins has Java 6 and so must
        # be run manually as integration tests (12.16.2017)
        if cls.manual_integration_tests:
            cls.fetch_and_unzip_from_s3(filename='GATK_data.zip',
                                        data=cls.gatk_data,
                                        data_dir=cls.gatk_data_dir)

    def tearDown(self):
        """Clean up outputs."""
        remove_outputs(self.output_dir)

        jobstores = ['./toilWorkflowRun', '/mnt/ephemeral/workspace/toil-pull-requests/toilWorkflowRun']
        for jobstore in jobstores:
            if os.path.exists(jobstore):
                shutil.rmtree(jobstore)

        unittest.TestCase.tearDown(self)

    @classmethod
    def tearDownClass(cls):
        """Clean up (shared) inputs."""
        if cls.jenkins:
            if os.path.exists(cls.gatk_data):
                os.remove(cls.gatk_data)
            if os.path.exists(cls.gatk_data_dir):
                shutil.rmtree(cls.gatk_data_dir)

            if os.path.exists(cls.wdl_data):
                os.remove(cls.wdl_data)
            if os.path.exists(cls.wdl_data_dir):
                shutil.rmtree(cls.wdl_data_dir)

            if os.path.exists(cls.encode_data):
                os.remove(cls.encode_data)
            if os.path.exists(cls.encode_data_dir):
                shutil.rmtree(cls.encode_data_dir)

    @needs_docker
    def testMD5sum(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #1.'''
        wdl = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.wdl')
        inputfile = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.input')
        json = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.json')

        subprocess.check_call([exactPython, self.program, wdl, json, '-o', self.output_dir])
        md5sum_output = os.path.join(self.output_dir, 'md5sum.txt')
        assert os.path.exists(md5sum_output)
        os.unlink(md5sum_output)

    # estimated run time <1 sec
    def testFn_SelectFirst(self):
        """Test the wdl built-in functional equivalent of 'select_first()',
        which returns the first value in a list that is not None."""
        assert select_first(['somestring', 'anotherstring', None, '', 1]) == 'somestring'
        assert select_first([None, '', 1, 'somestring']) == 1
        assert select_first([2, 1, '', 'somestring', None, '']) == 2
        assert select_first(['', 2, 1, 'somestring', None, '']) == 2

    # estimated run time <1 sec
    def testFn_Size(self):
        """Test the wdl built-in functional equivalent of 'size()',
        which returns a file's size based on the path."""
        from toil.job import Job
        from toil.common import Toil
        options = Job.Runner.getDefaultOptions('./toilWorkflowRun')
        options.clean = 'always'
        with Toil(options) as toil:
            small = process_infile(os.path.abspath('src/toil/test/wdl/testfiles/vocab.wdl'), toil)
            small_file = size(small)
            large = process_infile(self.encode_data, toil)
            larger_file = size(large)
            larger_file_in_mb = size(large, 'mb')
            assert small_file >= 1800, small_file
            assert larger_file >= 70000000, larger_file
            assert larger_file_in_mb >= 70, larger_file_in_mb

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
                                 os.path.abspath('src/toil/wdl/wdl_functions.py'),
                                 os.path.abspath('src/toil/wdl/wdl_parser.py')]
        # make sure the files match the expected files
        for location in wdl_that_should_exist:
            assert location in wdl_locations, '{} not in {}!'.format(str(location), str(wdl_locations))
        # make sure the same number of files were found as expected
        assert len(wdl_that_should_exist) == len(wdl_locations), '{} != {}'.format(str(len(wdl_locations)), str(len(wdl_that_should_exist)))

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
        '''Test if toilwdl correctly interprets some basic declarations.'''
        wdl = os.path.abspath('src/toil/test/wdl/testfiles/vocab.wdl')
        json = os.path.abspath('src/toil/test/wdl/testfiles/vocab.json')

        aWDL = AnalyzeWDL(wdl, json, self.output_dir)
        with open(wdl, 'r') as wdl:
            wdl_string = wdl.read()
            ast = wdl_parser.parse(wdl_string).ast()
            aWDL.create_tasks_dict(ast)
            aWDL.create_workflows_dict(ast)

        no_declaration = ['bool1', 'int1', 'float1', 'file1', 'string1']
        collection_counter = []
        for name, declaration in iteritems(aWDL.workflows_dictionary['vocabulary']['wf_declarations']):

            if name in no_declaration:
                collection_counter.append(name)
                assert not declaration['value']

            if name == 'bool2':
                collection_counter.append(name)
                assert declaration['value'] == 'True', declaration['value']
                assert declaration['type'] == 'Boolean', declaration['type']
            if name == 'int2':
                collection_counter.append(name)
                assert declaration['value'] == '1', declaration['value']
                assert declaration['type'] == 'Int', declaration['type']
            if name == 'float2':
                collection_counter.append(name)
                assert declaration['value'] == '1.1', declaration['value']
                assert declaration['type'] == 'Float', declaration['type']
            if name == 'file2':
                collection_counter.append(name)
                assert declaration['value'] == "'src/toil/test/wdl/test.tsv'", declaration['value']
                assert declaration['type'] == 'File', declaration['type']
            if name == 'string2':
                collection_counter.append(name)
                assert declaration['value'] == "'x'", declaration['value']
                assert declaration['type'] == 'String', declaration['type']
        assert collection_counter == ['bool1', 'int1', 'float1', 'file1', 'string1',
                                      'bool2', 'int2', 'float2', 'file2', 'string2']

    # estimated run time 27 sec
    @slow
    def testTut01(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #1.'''
        if self.manual_integration_tests:
            wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl")
            json = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json")
            ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/output/")

            subprocess.check_call([exactPython, self.program, wdl, json, '-o', self.output_dir])

            compare_runs(self.output_dir, ref_dir)

    # estimated run time 28 sec
    @slow
    def testTut02(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #2.'''
        if self.manual_integration_tests:
            wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t02/simpleVariantSelection.wdl")
            json = os.path.abspath("src/toil/test/wdl/wdl_templates/t02/simpleVariantSelection_inputs.json")
            ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t02/output/")

            subprocess.check_call([exactPython, self.program, wdl, json, '-o', self.output_dir])

            compare_runs(self.output_dir, ref_dir)

    # estimated run time 60 sec
    @slow
    def testTut03(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #3.'''
        if self.manual_integration_tests:
            wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t03/simpleVariantDiscovery.wdl")
            json = os.path.abspath("src/toil/test/wdl/wdl_templates/t03/simpleVariantDiscovery_inputs.json")
            ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t03/output/")

            subprocess.check_call([exactPython, self.program, wdl, json, '-o', self.output_dir])

            compare_runs(self.output_dir, ref_dir)

    # estimated run time 175 sec
    @slow
    def testTut04(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #4.'''
        if self.manual_integration_tests:
            wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/jointCallingGenotypes.wdl")
            json = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/jointCallingGenotypes_inputs.json")
            ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/output/")

            subprocess.check_call([exactPython, self.program, wdl, json, '-o', self.output_dir])

            compare_runs(self.output_dir, ref_dir)

    # estimated run time 80 sec
    @slow
    @needs_docker
    def testENCODE(self):
        '''Test if toilwdl produces the same outputs as known good outputs for
        a short ENCODE run.'''
        wdl = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testENCODE/encode_mapping_workflow.wdl")
        json = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testENCODE/encode_mapping_workflow.wdl.json")
        ref_dir = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testENCODE/output/")

        subprocess.check_call(
            [exactPython, self.program, wdl, json, '--docker_user=None', '--out_dir', self.output_dir])

        compare_runs(self.output_dir, ref_dir)

    # estimated run time 2 sec
    def testPipe(self):
        '''Test basic bash input functionality with a pipe.'''
        wdl = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testPipe/call.wdl")
        json = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testPipe/call.json")
        ref_dir = os.path.abspath(
            "src/toil/test/wdl/wdl_templates/testPipe/output/")

        subprocess.check_call(
            [exactPython, self.program, wdl, json, '--out_dir', self.output_dir])

        compare_runs(self.output_dir, ref_dir)

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

    # estimated run time <1 sec
    def testJSON(self):
        default_json_dict_output = {
            u'helloHaplotypeCaller.haplotypeCaller.RefIndex': u'"src/toil/test/wdl/GATK_data/ref/human_g1k_b37_20.fasta.fai"',
            u'helloHaplotypeCaller.haplotypeCaller.sampleName': u'"WDL_tut1_output"',
            u'helloHaplotypeCaller.haplotypeCaller.inputBAM': u'"src/toil/test/wdl/GATK_data/inputs/NA12878_wgs_20.bam"',
            u'helloHaplotypeCaller.haplotypeCaller.bamIndex': u'"src/toil/test/wdl/GATK_data/inputs/NA12878_wgs_20.bai"',
            u'helloHaplotypeCaller.haplotypeCaller.GATK': u'"src/toil/test/wdl/GATK_data/GenomeAnalysisTK.jar"',
            u'helloHaplotypeCaller.haplotypeCaller.RefDict': u'"src/toil/test/wdl/GATK_data/ref/human_g1k_b37_20.dict"',
            u'helloHaplotypeCaller.haplotypeCaller.RefFasta': u'"src/toil/test/wdl/GATK_data/ref/human_g1k_b37_20.fasta"'}

        t = AnalyzeWDL(
            "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl",
            "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json",
            self.output_dir)

        json_dict = t.dict_from_JSON("src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json")
        assert json_dict == default_json_dict_output, (str(json_dict) + '\nAssertionError: ' + str(default_json_dict_output))

    @classmethod
    def fetch_and_unzip_from_s3(cls, filename, data, data_dir):
        if not os.path.exists(data):
            s3_loc = os.path.join('http://toil-datasets.s3.amazonaws.com/', filename)
            urlretrieve(s3_loc, data)
        # extract the compressed data if not already extracted
        if not os.path.exists(data_dir):
            with zipfile.ZipFile(data, 'r') as zip_ref:
                zip_ref.extractall(cls.test_directory)


def remove_outputs(output_dir):
    '''Remove the outputs generated by various unittests.

    These are created in the current working directory, which on jenkins is the
    main toil folder.  They must be removed so that jenkins does not think we
    have untracked files in our github repo.'''
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
                      'toilwdl_compiled.py']
    other_log_outputs = ['post_processing.log',
                         'md5.log']
    outputs = encode_outputs + other_log_outputs
    for output in outputs:
        output = os.path.abspath(output)
        if os.path.exists(output):
            os.remove(output)
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)


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
        if file != 'outputs.txt':
            test_output_files = os.listdir(output_dir)
            filepath = os.path.join(ref_dir, file)
            with open(filepath, 'r') as default_file:
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
                            with open(test_filepath, 'r') as test_file:
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
    with open(filepath1, 'r') as default_file:
        good_data = []
        for line in default_file:
            line = line.strip()
            if not line.startswith('#'):
                good_data.append(line.split('\t'))

    with open(filepath2, 'r') as test_file:
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
                    assert test_data[i][j] == good_data[i][j], "File does not match: %r" % filepath1


if __name__ == "__main__":
    unittest.main()  # run all tests
