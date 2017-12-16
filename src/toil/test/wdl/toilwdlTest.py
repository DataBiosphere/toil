from __future__ import absolute_import
import unittest
import os
import subprocess
from toil.wdl.toilwdl import ToilWDL
from toil.test import ToilTest, slow
import zipfile

class ToilWdlIntegrationTest(ToilTest):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        """
        Initial set up of variables for the test.
        """
        self.program = os.path.abspath("src/toil/wdl/toilwdl.py")

        self.test_directory = os.path.abspath("src/toil/test/wdl/")
        self.output_dir = self._createTempDir(purpose='tempDir')

        ############# FETCH AND EXTRACT ENCODE DATASETS FROM S3#################
        self.encode_data = os.path.join(self.test_directory, "ENCODE_data.zip")
        self.encode_data_dir = os.path.join(self.test_directory, "ENCODE_data")
        # download the data from s3 if not already present
        if not os.path.exists(self.encode_data):
            encode_s3_loc = 'http://toil-datasets.s3.amazonaws.com/ENCODE_data.zip'
            fetch_encode_from_s3_cmd = ["wget", "-P", self.test_directory, encode_s3_loc]
            subprocess.check_call(fetch_encode_from_s3_cmd)
        # extract the compressed data if not already extracted
        if not os.path.exists(self.encode_data_dir):
            with zipfile.ZipFile(self.encode_data, 'r') as zip_ref:
                zip_ref.extractall(self.test_directory)

        ############# FETCH AND EXTRACT WDL TEMPLATES FROM S3###################
        self.wdl_data = os.path.join(self.test_directory, "wdl_templates.zip")
        self.wdl_data_dir = os.path.join(self.test_directory, "wdl_templates")
        # download the data from s3 if not already present
        if not os.path.exists(self.wdl_data):
            wdl_s3_loc = 'http://toil-datasets.s3.amazonaws.com/wdl_templates.zip'
            fetch_wdldata_from_s3_cmd = ["wget", "-P", self.test_directory, wdl_s3_loc]
            subprocess.check_call(fetch_wdldata_from_s3_cmd)
        # extract the compressed data if not already extracted
        if not os.path.exists(self.wdl_data_dir):
            with zipfile.ZipFile(self.wdl_data, 'r') as zip_ref:
                zip_ref.extractall(self.test_directory)

    def tearDown(self):
        """Default tearDown for unittest."""
        unittest.TestCase.tearDown(self)

    # estimated run time 80 sec
    @slow
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
            ['python', self.program, wdl, json, '--out_dir', self.output_dir])

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
            ['python', self.program, wdl, json, '--out_dir', self.output_dir])

        compare_runs(self.output_dir, ref_dir)

    # estimated run time <1 sec
    def testCSV(self):
        default_csv_output = [['1', '2', '3'], 
                              ['4', '5', '6'], 
                              ['7', '8', '9']]
        t = ToilWDL(os.path.abspath(
                "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl"),
            os.path.abspath(
                "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json"),
            self.output_dir)
        csv_array = t.create_csv_array('src/toil/test/wdl/test.csv')
        assert csv_array == default_csv_output

    # estimated run time <1 sec
    def testTSV(self):
        default_tsv_output = [['1', '2', '3'], 
                              ['4', '5', '6'], 
                              ['7', '8', '9']]
        t = ToilWDL(os.path.abspath(
                "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl"),
            os.path.abspath(
                "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json"),
            self.output_dir)
        tsv_array = t.create_tsv_array('src/toil/test/wdl/test.tsv')
        assert tsv_array == default_tsv_output

    # estimated run time <1 sec
    def testJSON(self):
        default_json_dict_output = {
            u'RefIndex': u'src/toil/test/wdl/GATK_data/ref/human_g1k_b37_20.fasta.fai',
            u'sampleName': u'WDL_tut1_output',
            u'inputBAM': u'src/toil/test/wdl/GATK_data/inputs/NA12878_wgs_20.bam',
            u'bamIndex': u'src/toil/test/wdl/GATK_data/inputs/NA12878_wgs_20.bai',
            u'GATK': u'src/toil/test/wdl/GATK_data/GenomeAnalysisTK.jar',
            u'RefDict': u'src/toil/test/wdl/GATK_data/ref/human_g1k_b37_20.dict',
            u'RefFasta': u'src/toil/test/wdl/GATK_data/ref/human_g1k_b37_20.fasta'}

        t = ToilWDL(
            "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl",
            "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json",
            self.output_dir)

        json_dict = t.dict_from_JSON(
            "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json")
        assert json_dict == default_json_dict_output

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