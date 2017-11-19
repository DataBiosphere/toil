from __future__ import absolute_import
import unittest
import os
import subprocess
from toil.test import slow
from toil.wdl.toilwdl import ToilWDL
import zipfile
import time

class TestCase(unittest.TestCase):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        """
        Initial set up of variables for the test.
        """

        self.program = 'src/toil/wdl/toilwdl.py'
        self.tutorial_test_output_dir = 'src/toil/wdl/toil_outputs'
        self.output_file = "src/toil/wdl/toilwdl_compiled.py"

        if not os.path.exists(self.tutorial_test_output_dir):
            try:
                os.makedirs(self.tutorial_test_output_dir)
            except:
                raise OSError(
                    'Could not create directory.  Insufficient permissions or disk space most likely.')

        with zipfile.ZipFile('src/toil/wdl/ENCODE_data.zip', 'r') as zip_ref:
            zip_ref.extractall('src/toil/wdl/')
        with zipfile.ZipFile('src/toil/wdl/GATK_data.zip', 'r') as zip_ref:
            zip_ref.extractall('src/toil/wdl/')
        with zipfile.ZipFile('src/toil/wdl/toil_templates.zip', 'r') as zip_ref:
            zip_ref.extractall('src/toil/wdl/')
        with zipfile.ZipFile('src/toil/wdl/wdl_templates.zip', 'r') as zip_ref:
            zip_ref.extractall('src/toil/wdl/')

    def tearDown(self):
        """Default tearDown for unittest."""
        unittest.TestCase.tearDown(self)

    # # estimated run time 27 sec
    @slow
    def testTut01(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #1.'''
        wdl = "src/toil/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl"
        json = "src/toil/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json"
        tutorial_good_output_dir = 'src/toil/wdl/wdl_templates/t01/output/'

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.tutorial_test_output_dir])
        subprocess.check_call(['python', self.output_file])

        default_output_files = os.listdir(tutorial_good_output_dir)
        test_output_files = os.listdir(self.tutorial_test_output_dir)

        for file in default_output_files:
            if file != 'outputs.txt':
                filepath = os.path.join(tutorial_good_output_dir, file)
                with open(filepath, 'r') as default_file:
                    good_data = []
                    for line in default_file:
                        if not line.startswith('#'):
                            good_data.append(line)

                    for test_file in test_output_files:
                        if file == test_file:
                            test_filepath = os.path.join(self.tutorial_test_output_dir, file)
                            with open(test_filepath, 'r') as test_file:
                                test_data = []
                                for line in test_file:
                                    if not line.startswith('#'):
                                        test_data.append(line)

                assert good_data == test_data

    # estimated run time 28 sec
    @slow
    def testTut02(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #2.'''
        wdl = "src/toil/wdl/wdl_templates/t02/simpleVariantSelection.wdl"
        json = "src/toil/wdl/wdl_templates/t02/simpleVariantSelection_inputs.json"
        tutorial_good_output_dir = 'src/toil/wdl/wdl_templates/t02/output/'

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.tutorial_test_output_dir])
        subprocess.check_call(['python', self.output_file])

        default_output_files = os.listdir(tutorial_good_output_dir)
        test_output_files = os.listdir(self.tutorial_test_output_dir)

        for file in default_output_files:
            if file != 'outputs.txt':
                filepath = os.path.join(tutorial_good_output_dir, file)
                with open(filepath, 'r') as default_file:
                    good_data = []
                    for line in default_file:
                        if not line.startswith('#'):
                            good_data.append(line)

                    for test_file in test_output_files:
                        if file == test_file:
                            test_filepath = os.path.join(self.tutorial_test_output_dir, file)
                            with open(test_filepath, 'r') as test_file:
                                test_data = []
                                for line in test_file:
                                    if not line.startswith('#'):
                                        test_data.append(line)

                assert good_data == test_data

    # estimated run time 60 sec
    @slow
    def testTut03(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #3.'''
        wdl = "src/toil/wdl/wdl_templates/t03/simpleVariantDiscovery.wdl"
        json = "src/toil/wdl/wdl_templates/t03/simpleVariantDiscovery_inputs.json"
        tutorial_good_output_dir = 'src/toil/wdl/wdl_templates/t03/output/'

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.tutorial_test_output_dir])
        subprocess.check_call(['python', self.output_file])

        default_output_files = os.listdir(tutorial_good_output_dir)
        test_output_files = os.listdir(self.tutorial_test_output_dir)

        for file in default_output_files:
            if file != 'outputs.txt':
                filepath = os.path.join(tutorial_good_output_dir, file)
                with open(filepath, 'r') as default_file:
                    good_data = []
                    for line in default_file:
                        if not line.startswith('#'):
                            good_data.append(line)

                    for test_file in test_output_files:
                        if file == test_file:
                            test_filepath = os.path.join(self.tutorial_test_output_dir, file)
                            with open(test_filepath, 'r') as test_file:
                                test_data = []
                                for line in test_file:
                                    if not line.startswith('#'):
                                        test_data.append(line)

                assert good_data == test_data

    # estimated run time 175 sec
    @slow
    def testTut04(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #4.'''
        wdl = "src/toil/wdl/wdl_templates/t04/jointCallingGenotypes.wdl"
        json = "src/toil/wdl/wdl_templates/t04/jointCallingGenotypes_inputs.json"
        tutorial_good_output_dir = 'src/toil/wdl/wdl_templates/t04/output/'

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.tutorial_test_output_dir])
        subprocess.check_call(['python', self.output_file])

        default_output_files = os.listdir(tutorial_good_output_dir)
        test_output_files = os.listdir(self.tutorial_test_output_dir)

        for file in default_output_files:
            if file != 'outputs.txt':
                filepath = os.path.join(tutorial_good_output_dir, file)
                with open(filepath, 'r') as default_file:
                    good_data = []
                    for line in default_file:
                        if not line.startswith('#'):
                            good_data.append(line)

                    for test_file in test_output_files:
                        if file == test_file:
                            test_filepath = os.path.join(self.tutorial_test_output_dir, file)
                            with open(test_filepath, 'r') as test_file:
                                test_data = []
                                for line in test_file:
                                    if not line.startswith('#'):
                                        test_data.append(line)

                assert good_data == test_data

    # estimated run time 80 sec
    @slow
    def testENCODE(self):
        '''Test if toilwdl produces the same outputs as known good outputs for
        a short ENCODE run.'''
        wdl = "src/toil/wdl/wdl_templates/testENCODE/encode_mapping_workflow.wdl"
        json = "src/toil/wdl/wdl_templates/testENCODE/encode_mapping_workflow.wdl.json"
        tutorial_good_output_dir = 'src/toil/wdl/wdl_templates/testENCODE/output/'

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.tutorial_test_output_dir])
        subprocess.check_call(['python', self.output_file])

        default_output_files = os.listdir(tutorial_good_output_dir)
        test_output_files = os.listdir(self.tutorial_test_output_dir)

        for file in default_output_files:
            if (file != 'outputs.txt') and (os.path.isfile(file)):
                filepath = os.path.join(tutorial_good_output_dir, file)
                with open(filepath, 'r') as default_file:
                    good_data = []
                    for line in default_file:
                        if not line.startswith('#'):
                            good_data.append(line)

                    for test_file in test_output_files:
                        if file == test_file:
                            test_filepath = os.path.join(self.tutorial_test_output_dir, file)
                            with open(test_filepath, 'r') as test_file:
                                test_data = []
                                for line in test_file:
                                    if not line.startswith('#'):
                                        test_data.append(line)

                assert good_data == test_data

    # estimated run time 2 sec
    def testPipe(self):
        '''Test basic bash input functionality with a pipe.'''
        wdl = "src/toil/wdl/wdl_templates/testPipe/call.wdl"
        json = "src/toil/wdl/wdl_templates/testPipe/call.json"
        tutorial_good_output_dir = 'src/toil/wdl/wdl_templates/testPipe/output/'

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.tutorial_test_output_dir])
        subprocess.check_call(['python', self.output_file])

        default_output_files = os.listdir(tutorial_good_output_dir)
        test_output_files = os.listdir(self.tutorial_test_output_dir)

        for file in default_output_files:
            if file != 'outputs.txt':
                filepath = os.path.join(tutorial_good_output_dir, file)
                with open(filepath, 'r') as default_file:
                    good_data = []
                    for line in default_file:
                        if not line.startswith('#'):
                            good_data.append(line)

                    for test_file in test_output_files:
                        if file == test_file:
                            test_filepath = os.path.join(self.tutorial_test_output_dir, file)
                            with open(test_filepath, 'r') as test_file:
                                test_data = []
                                for line in test_file:
                                    if not line.startswith('#'):
                                        test_data.append(line)

                assert good_data == test_data

    # estimated run time <1 sec
    def testCSV(self):
        default_csv_output = [['1', '2', '3'], ['4', '5', '6'], ['7', '8', '9']]
        t = ToilWDL("src/toil/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl",
                    "src/toil/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json",
                    "src/toil/wdl/toil_outputs")
        csv_array = t.create_csv_array('src/toil/wdl/test.csv')
        assert csv_array == default_csv_output

    # estimated run time <1 sec
    def testTSV(self):
        default_tsv_output = [['1', '2', '3'], ['4', '5', '6'], ['7', '8', '9']]
        t = ToilWDL("src/toil/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl",
                    "src/toil/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json",
                    "src/toil/wdl/toil_outputs")
        tsv_array = t.create_tsv_array('src/toil/wdl/test.tsv')
        assert tsv_array == default_tsv_output

    # estimated run time <1 sec
    def testJSON(self):
        default_json_dict_output = {u'RefIndex': u'GATK_data/ref/human_g1k_b37_20.fasta.fai',
                                    u'sampleName': u'WDL_tut1_output',
                                    u'inputBAM': u'GATK_data/inputs/NA12878_wgs_20.bam',
                                    u'bamIndex': u'GATK_data/inputs/NA12878_wgs_20.bai',
                                    u'GATK': u'GATK_data/GenomeAnalysisTK.jar',
                                    u'RefDict': u'GATK_data/ref/human_g1k_b37_20.dict',
                                    u'RefFasta': u'GATK_data/ref/human_g1k_b37_20.fasta'}
        t = ToilWDL("src/toil/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl",
                    "src/toil/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json",
                    "src/toil/wdl/toil_outputs")
        json_dict = t.dict_from_JSON("src/toil/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json")
        assert json_dict == default_json_dict_output

if __name__ == "__main__":
    unittest.main() # run all tests