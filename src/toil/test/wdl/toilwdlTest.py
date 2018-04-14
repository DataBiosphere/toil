from __future__ import absolute_import
import unittest
import os
from toil import subprocess
from toil.wdl.wdl_analysis import AnalyzeWDL
from toil.wdl.wdl_synthesis import SynthesizeWDL, recursive_glob, generate_docker_bashscript_file, heredoc_wdl
from toil.test import ToilTest, slow
import zipfile
import shutil

class ToilWdlIntegrationTest(ToilTest):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        """
        Initial set up of variables for the test.
        """
        self.program = os.path.abspath("src/toil/wdl/toilwdl.py")

        self.test_directory = os.path.abspath("src/toil/test/wdl/")
        self.output_dir = self._createTempDir(purpose='tempDir')

        self.encode_data = os.path.join(self.test_directory, "ENCODE_data.zip")
        self.encode_data_dir = os.path.join(self.test_directory, "ENCODE_data")

        self.wdl_data = os.path.join(self.test_directory, "wdl_templates.zip")
        self.wdl_data_dir = os.path.join(self.test_directory, "wdl_templates")

        self.gatk_data = os.path.join(self.test_directory, "GATK_data.zip")
        self.gatk_data_dir = os.path.join(self.test_directory, "GATK_data")

        # GATK tests will not run on jenkins b/c GATK.jar needs Java 7
        # and jenkins only has Java 6 (12-16-2017).
        # Set this to true to run the GATK integration tests locally.
        self.manual_integration_tests = False

        # Delete the test datasets after running the tests.
        # Jenkins requires this to not error on "untracked files".
        # Set to true if running tests locally and you don't want to
        # redownload the data each time you run the test.
        self.jenkins = True

        self.fetch_and_unzip_from_s3(filename='ENCODE_data.zip',
                                     data=self.encode_data,
                                     data_dir=self.encode_data_dir)

        self.fetch_and_unzip_from_s3(filename='wdl_templates.zip',
                                     data=self.wdl_data,
                                     data_dir=self.wdl_data_dir)

        # these tests require Java 7 (GATK.jar); jenkins has Java 6 and so must
        # be run manually as integration tests (12.16.2017)
        if self.manual_integration_tests:
            self.fetch_and_unzip_from_s3(filename='GATK_data.zip',
                                         data=self.gatk_data,
                                         data_dir=self.gatk_data_dir)

    def tearDown(self):
        """Default tearDown for unittest."""

        # automatically delete the test files
        # especially for jenkins checking if
        # there are untracked files in the repo
        if self.jenkins:
            if os.path.exists(self.gatk_data):
                os.remove(self.gatk_data)
            if os.path.exists(self.gatk_data_dir):
                shutil.rmtree(self.gatk_data_dir)

            if os.path.exists(self.wdl_data):
                os.remove(self.wdl_data)
            if os.path.exists(self.wdl_data_dir):
                shutil.rmtree(self.wdl_data_dir)

            if os.path.exists(self.encode_data):
                os.remove(self.encode_data)
            if os.path.exists(self.encode_data_dir):
                shutil.rmtree(self.encode_data_dir)

        remove_encode_output()

        unittest.TestCase.tearDown(self)

    # estimated run time 7 sec; not actually slow, but this will break on travis because it does not have
    # docker installed so we tag this so that it runs on jenkins instead.
    @slow
    def testMD5sum(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #1.'''
        wdl = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.wdl')
        inputfile = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.input')
        json = os.path.abspath('src/toil/test/wdl/md5sum/md5sum.json')

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.output_dir])
        md5sum_output = os.path.join(self.output_dir, 'md5sum.txt')
        assert os.path.exists(md5sum_output)
        os.unlink(md5sum_output)

    # estimated run time 27 sec
    @slow
    def testTut01(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #1.'''
        if self.manual_integration_tests:
            wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl")
            json = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json")
            ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/output/")

            subprocess.check_call(['python', self.program, wdl, json, '-o', self.output_dir])

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

            subprocess.check_call(['python', self.program, wdl, json, '-o', self.output_dir])

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

            subprocess.check_call(['python', self.program, wdl, json, '-o', self.output_dir])

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

            subprocess.check_call(['python', self.program, wdl, json, '-o', self.output_dir])

            compare_runs(self.output_dir, ref_dir)

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
        t = AnalyzeWDL(os.path.abspath(
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
        t = AnalyzeWDL(os.path.abspath(
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

        t = AnalyzeWDL(
            "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl",
            "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json",
            self.output_dir)

        json_dict = t.dict_from_JSON(
            "src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json")
        assert json_dict == default_json_dict_output

    def fetch_and_unzip_from_s3(self, filename, data, data_dir):
        if not os.path.exists(data):
            s3_loc = os.path.join('http://toil-datasets.s3.amazonaws.com/', filename)
            fetch_from_s3_cmd = ["wget", "-P", self.test_directory, s3_loc]
            subprocess.check_call(fetch_from_s3_cmd)
        # extract the compressed data if not already extracted
        if not os.path.exists(data_dir):
            with zipfile.ZipFile(data, 'r') as zip_ref:
                zip_ref.extractall(self.test_directory)

def remove_encode_output():
    '''Remove the outputs generated by the ENCODE unittest.

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
                      'xcor.log']
    for output in encode_outputs:
        output = os.path.abspath(output)
        if os.path.exists(output):
            os.remove(output)


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
