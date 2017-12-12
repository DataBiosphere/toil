from __future__ import absolute_import

import unittest
import os
import subprocess
import zipfile

from toil.test.wdl.toilwdlTest import compare_runs
from toil.test import ToilTest, slow

#######################################
# Note: GATK.jar requires java 7      #
# jenkins has only java 6 (11-22-2017)#
#######################################

class ToilWdlIntegrationTest(ToilTest):
    """A set of test cases for toilwdl.py"""

    def setUp(self):
        """
        Initial set up of variables for the test.
        """

        # TODO: Setup ftp to host and pull files from; test_directory == tempDir
        self.program = os.path.abspath("src/toil/wdl/toilwdl.py")

        self.test_directory = os.path.abspath("src/toil/test/wdl/")
        self.output_dir = self._createTempDir(purpose='tempDir')

        self.gatk_data = os.path.join(self.test_directory, "GATK_data.zip")
        self.wdl_data = os.path.join(self.test_directory, "wdl_templates.zip")

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

        compare_runs(self.output_dir, ref_dir)

    # estimated run time 28 sec
    @slow
    def testTut02(self):
        '''Test if toilwdl produces the same outputs as known good outputs for WDL's
        GATK tutorial #2.'''
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
        wdl = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/jointCallingGenotypes.wdl")
        json = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/jointCallingGenotypes_inputs.json")
        ref_dir = os.path.abspath("src/toil/test/wdl/wdl_templates/t04/output/")

        subprocess.check_call(['python', self.program, wdl, json, '-o', self.output_dir])

        compare_runs(self.output_dir, ref_dir)

if __name__ == "__main__":
    unittest.main() # run all tests