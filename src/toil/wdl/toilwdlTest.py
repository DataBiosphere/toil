import unittest
import os


# from toilwdl import ToilWDL  # Test Module
import toilwdl
import subprocess

class TestCase(unittest.TestCase):
    """A set of test cases for fence_class.py"""

    def setUp(self):
        """
        Initial set up of variables for the test.  Three test files are
        provided (must be in the same folder).
        """

        self.program = './toilwdl.py'
        self.tutorial_test_output_dir = './toil_outputs/'
        self.output_file = "./toilwdl_compiled.py"

    def tearDown(self):
        """Default tearDown for unittest."""
        unittest.TestCase.tearDown(self)

    # def testTut01(self):
    #     '''Test if toilwdl produces the same outputs as known good outputs for WDL's
    #     GATK tutorial #1.'''
    #     wdl = "./wdl_templates/t01/helloHaplotypeCaller.wdl"
    #     json = "./wdl_templates/t01/helloHaplotypeCaller_inputs.json"
    #     tutorial_good_output_dir = './wdl_templates/t01/output/'
    #
    #     subprocess.check_call(['python', self.program, wdl, json])
    #     subprocess.check_call(['python', 'toilwdl_compiled.py'])
    #
    #     default_output_files = os.listdir(tutorial_good_output_dir)
    #     test_output_files = os.listdir(self.tutorial_test_output_dir)
    #
    #     for file in default_output_files:
    #         if file != 'outputs.txt':
    #             filepath = tutorial_good_output_dir + file
    #             with open(filepath, 'r') as default_file:
    #                 good_data = []
    #                 for line in default_file:
    #                     if not line.startswith('#'):
    #                         good_data.append(line)
    #
    #                 for test_file in test_output_files:
    #                     if file == test_file:
    #                         test_filepath = self.tutorial_test_output_dir + test_file
    #                         with open(test_filepath, 'r') as test_file:
    #                             test_data = []
    #                             for line in test_file:
    #                                 if not line.startswith('#'):
    #                                     test_data.append(line)
    #
    #             assert good_data == test_data
    #
    # def testTut02(self):
    #     '''Test if toilwdl produces the same outputs as known good outputs for WDL's
    #     GATK tutorial #2.'''
    #     wdl = "./wdl_templates/t02/simpleVariantSelection.wdl"
    #     json = "./wdl_templates/t02/simpleVariantSelection_inputs.json"
    #     tutorial_good_output_dir = './wdl_templates/t02/output/'
    #
    #     subprocess.check_call(['python', self.program, wdl, json])
    #     subprocess.check_call(['python', 'toilwdl_compiled.py'])
    #
    #     default_output_files = os.listdir(tutorial_good_output_dir)
    #     test_output_files = os.listdir(self.tutorial_test_output_dir)
    #
    #     for file in default_output_files:
    #         if file != 'outputs.txt':
    #             filepath = tutorial_good_output_dir + file
    #             with open(filepath, 'r') as default_file:
    #                 good_data = []
    #                 for line in default_file:
    #                     if not line.startswith('#'):
    #                         good_data.append(line)
    #
    #                 for test_file in test_output_files:
    #                     if file == test_file:
    #                         test_filepath = self.tutorial_test_output_dir + test_file
    #                         with open(test_filepath, 'r') as test_file:
    #                             test_data = []
    #                             for line in test_file:
    #                                 if not line.startswith('#'):
    #                                     test_data.append(line)
    #
    #             assert good_data == test_data
    #
    # def testTut03(self):
    #     '''Test if toilwdl produces the same outputs as known good outputs for WDL's
    #     GATK tutorial #3.'''
    #     wdl = "./wdl_templates/t03/simpleVariantDiscovery.wdl"
    #     json = "./wdl_templates/t03/simpleVariantDiscovery_inputs.json"
    #     tutorial_good_output_dir = './wdl_templates/t03/output/'
    #
    #     subprocess.check_call(['python', self.program, wdl, json])
    #     subprocess.check_call(['python', 'toilwdl_compiled.py'])
    #
    #     default_output_files = os.listdir(tutorial_good_output_dir)
    #     test_output_files = os.listdir(self.tutorial_test_output_dir)
    #
    #     for file in default_output_files:
    #         if file != 'outputs.txt':
    #             filepath = tutorial_good_output_dir + file
    #             with open(filepath, 'r') as default_file:
    #                 good_data = []
    #                 for line in default_file:
    #                     if not line.startswith('#'):
    #                         good_data.append(line)
    #
    #                 for test_file in test_output_files:
    #                     if file == test_file:
    #                         test_filepath = self.tutorial_test_output_dir + test_file
    #                         with open(test_filepath, 'r') as test_file:
    #                             test_data = []
    #                             for line in test_file:
    #                                 if not line.startswith('#'):
    #                                     test_data.append(line)
    #
    #             assert good_data == test_data
    #
    # def testTut04(self):
    #     '''Test if toilwdl produces the same outputs as known good outputs for WDL's
    #     GATK tutorial #4.'''
    #     wdl = "./wdl_templates/t04/jointCallingGenotypes.wdl"
    #     json = "./wdl_templates/t04/jointCallingGenotypes_inputs.json"
    #     tutorial_good_output_dir = './wdl_templates/t04/output/'
    #
    #     subprocess.check_call(['python', self.program, wdl, json])
    #     subprocess.check_call(['python', 'toilwdl_compiled.py'])
    #
    #     default_output_files = os.listdir(tutorial_good_output_dir)
    #     test_output_files = os.listdir(self.tutorial_test_output_dir)
    #
    #     for file in default_output_files:
    #         if file != 'outputs.txt':
    #             filepath = tutorial_good_output_dir + file
    #             with open(filepath, 'r') as default_file:
    #                 good_data = []
    #                 for line in default_file:
    #                     if not line.startswith('#'):
    #                         good_data.append(line)
    #
    #                 for test_file in test_output_files:
    #                     if file == test_file:
    #                         test_filepath = self.tutorial_test_output_dir + test_file
    #                         with open(test_filepath, 'r') as test_file:
    #                             test_data = []
    #                             for line in test_file:
    #                                 if not line.startswith('#'):
    #                                     test_data.append(line)
    #
    #             assert good_data == test_data

    def testPipe(self):
        '''Test basic bash input functionality with a pipe.'''
        wdl = "./wdl_templates/testPipe/call.wdl"
        json = "./wdl_templates/testPipe/call.json"
        tutorial_good_output_dir = './wdl_templates/testPipe/output/'

        subprocess.check_call(['python', self.program, wdl, json])
        subprocess.check_call(['python', 'toilwdl_compiled.py'])

        default_output_files = os.listdir(tutorial_good_output_dir)
        test_output_files = os.listdir(self.tutorial_test_output_dir)

        for file in default_output_files:
            if file != 'outputs.txt':
                filepath = tutorial_good_output_dir + file
                with open(filepath, 'r') as default_file:
                    good_data = []
                    for line in default_file:
                        if not line.startswith('#'):
                            good_data.append(line)

                    for test_file in test_output_files:
                        if file == test_file:
                            test_filepath = self.tutorial_test_output_dir + test_file
                            with open(test_filepath, 'r') as test_file:
                                test_data = []
                                for line in test_file:
                                    if not line.startswith('#'):
                                        test_data.append(line)

                assert good_data == test_data

if __name__ == "__main__":
    unittest.main() # run all tests