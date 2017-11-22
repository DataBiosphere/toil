from __future__ import absolute_import
import unittest
import os
import subprocess
from toil.test import slow
import zipfile

#######################################
# Note: GATK.jar requires java 7      #
# jenkins has only java 6 (11-22-2017)#
#######################################

class TestCase(unittest.TestCase):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        """
        Initial set up of variables for the test.
        """

        self.program = os.path.abspath("src/toil/wdl/toilwdl.py")
        self.output_dir = os.path.abspath("src/toil/wdl/wdl_working/")
        self.output_file = os.path.abspath("toilwdl_compiled.py")

        self.test_directory = os.path.abspath("src/toil/test/wdl/")

        self.gatk_data = os.path.join(self.test_directory, "GATK_data.zip")
        self.wdl_data = os.path.join(self.test_directory, "wdl_templates.zip")

        if not os.path.exists(self.output_dir):
            try:
                os.makedirs(self.output_dir)
            except:
                raise OSError(
                    "Could not create directory.  "
                    "Insufficient permissions or disk space most likely.")

        with zipfile.ZipFile(self.gatk_data, 'r') as zip_ref:
            zip_ref.extractall(self.test_directory)
        with zipfile.ZipFile(self.wdl_data, 'r') as zip_ref:
            zip_ref.extractall(self.test_directory)

    def tearDown(self):
        """Default tearDown for unittest."""
        unittest.TestCase.tearDown(self)

    # estimated run time 27 sec
    @slow
    def testTut01(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #1.'''
        wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller.wdl")
        json = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/helloHaplotypeCaller_inputs.json")
        ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t01/output/")

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.output_dir])

        self.compare_runs(ref_dir)

    # estimated run time 28 sec
    @slow
    def testTut02(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #2.'''
        wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t02/simpleVariantSelection.wdl")
        json = os.path.abspath("src/toil/test/wdl/wdl_templates/t02/simpleVariantSelection_inputs.json")
        ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t02/output/")

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.output_dir])

        self.compare_runs(ref_dir)

    # estimated run time 60 sec
    @slow
    def testTut03(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #3.'''
        wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t03/simpleVariantDiscovery.wdl")
        json = os.path.abspath("src/toil/test/wdl/wdl_templates/t03/simpleVariantDiscovery_inputs.json")
        ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t03/output/")

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.output_dir])

        self.compare_runs(ref_dir)

    # estimated run time 175 sec
    @slow
    def testTut04(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #4.'''
        wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/jointCallingGenotypes.wdl")
        json = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/jointCallingGenotypes_inputs.json")
        ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/output/")

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.output_dir])

        self.compare_runs(ref_dir)

    def compare_runs(self, ref_dir):
        """

        :param ref_dir:
        :return:
        """
        reference_output_files = os.listdir(ref_dir)
        for file in reference_output_files:
            if file != 'outputs.txt':
                test_output_files = os.listdir(self.output_dir)
                filepath = os.path.join(ref_dir, file)
                with open(filepath, 'r') as default_file:
                    good_data = []
                    for line in default_file:
                        if not line.startswith('#'):
                            good_data.append(line)
                    for test_file in test_output_files:
                        if file == test_file:
                            test_filepath = os.path.join(self.output_dir, file)
                            print(file)
                            if file.endswith(".vcf"):
                                self.compare_vcf_files(filepath1=filepath, filepath2=test_filepath)
                            else:
                                with open(test_filepath, 'r') as test_file:
                                    test_data = []
                                    for line in test_file:
                                        if not line.startswith('#'):
                                            test_data.append(line)
                                assert good_data == test_data, "File does not match: %r" % file

    def compare_vcf_files(self, filepath1, filepath2):
        """

        :param filepath1:
        :param filepath2:
        :return:
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
    unittest.main() # run all tests