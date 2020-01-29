from __future__ import absolute_import
import unittest
import os
import re
import sys
import shutil

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from toil import subprocess
from toil.test import ToilTest
from toil.test import needs_cwl
from toil.version import python


class ToilDocumentationTest(ToilTest):
    """Tests for scripts in the toil tutorials."""
    @classmethod
    def setUpClass(cls):
        cls.directory = os.path.dirname(os.path.abspath(__file__))

    def tearDown(self):
        # src/toil/test/docs/scripts/cwlExampleFiles/sample_1_output.txt
        output_files = ["sample_1_output.txt", "sample_2_output.txt", "sample_3_output.txt"]
        for output in output_files:
            output_file = os.path.join(self.directory, 'scripts/cwlExampleFiles', output)
            if os.path.exists(output_file):
                os.remove(output_file)

        jobstores = ['./toilWorkflowRun', '/mnt/ephemeral/workspace/toil-pull-requests/toilWorkflowRun']
        for jobstore in jobstores:
            if os.path.exists(jobstore):
                shutil.rmtree(jobstore)

        unittest.TestCase.tearDown(self)

    """Just check the exit code"""
    def checkExitCode(self, script):
        program = os.path.join(self.directory, "scripts", script)
        process = subprocess.Popen([python, program, "file:my-jobstore", "--clean=always"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        assert process.returncode == 0, stderr
        if isinstance(stdout, bytes):
            return stdout.decode('utf-8') + ' ' + stderr.decode('utf-8')
        return stdout + ' ' + stderr

    """Check the exit code and the output"""
    def checkExpectedOut(self, script, expectedOutput):
        outerr = self.checkExitCode(script)

        # Check that the expected output is there
        index = outerr.find(expectedOutput)
        self.assertGreater(index, -1, "Expected:\n{}\nOutput:\n{}".format(expectedOutput, outerr))

    """Check the exit code and look for a pattern"""
    def checkExpectedPattern(self, script, expectedPattern):
        outerr = self.checkExitCode(script)

        # Check that the expected output pattern is there
        pattern = re.compile(expectedPattern, re.DOTALL)
        n = re.search(pattern, outerr)
        self.assertNotEqual(n, None, "Pattern:\n{}\nOutput:\n{}".format(expectedPattern, outerr))

    @needs_cwl
    def testCwlexample(self):
        self.checkExitCode("tutorial_cwlexample.py")

    def testDiscoverfiles(self):
        self.checkExitCode("tutorial_discoverfiles.py")

    def testDynamic(self):
        self.checkExitCode("tutorial_dynamic.py")

    def testEncapsulation(self):
        self.checkExitCode("tutorial_encapsulation.py")

    def testEncapsulation2(self):
        self.checkExitCode("tutorial_encapsulation2.py")

    def testHelloworld(self):
        self.checkExpectedOut("tutorial_helloworld.py", "Hello, world!, here's a message: You did it!\n")

    def testInvokeworkflow(self):
        self.checkExpectedOut("tutorial_invokeworkflow.py", "Hello, world!, here's a message: Woot\n")

    def testInvokeworkflow2(self):
        self.checkExpectedOut("tutorial_invokeworkflow2.py", "Hello, world!, I have a message: Woot!\n")

    def testJobFunctions(self):
        self.checkExpectedOut("tutorial_jobfunctions.py", "Hello world, I have a message: Woot!\n")

    def testManaging(self):
        self.checkExitCode("tutorial_managing.py")

    def testManaging2(self):
        self.checkExitCode("tutorial_managing2.py")

    def testMultiplejobs(self):
        self.checkExpectedPattern("tutorial_multiplejobs.py", "Hello world, I have a message: first.*Hello world, I have a message: "
                                         "second or third.*Hello world, I have a message: second or third.*Hello world,"
                                         " I have a message: last")

    def testMultiplejobs2(self):
        self.checkExpectedPattern("tutorial_multiplejobs2.py", "Hello world, I have a message: first.*Hello world, I have a message: "
                                         "second or third.*Hello world, I have a message: second or third.*Hello world,"
                                         " I have a message: last")

    def testMultiplejobs3(self):
        self.checkExpectedPattern("tutorial_multiplejobs3.py", "Hello world, I have a message: first.*Hello world, I have a message: "
                                         "second or third.*Hello world, I have a message: second or third.*Hello world,"
                                         " I have a message: last")

    def testPromises2(self):
        self.checkExpectedOut("tutorial_promises2.py", "['00000', '00001', '00010', '00011', '00100', '00101', '00110', '00111', '01000',"
                      " '01001', '01010', '01011', '01100', '01101', '01110', '01111', '10000', '10001', "
                      "'10010', '10011', '10100', '10101', '10110', '10111', '11000', '11001', '11010', "
                      "'11011', '11100', '11101', '11110', '11111']")

    def testQuickstart(self):
        self.checkExpectedOut("tutorial_quickstart.py", "Hello, world!, here's a message: Woot\n")

    def testRequirements(self):
        self.checkExitCode("tutorial_requirements.py")

    def testArguments(self):
        self.checkExpectedOut("tutorial_arguments.py", "Hello, world!, here's a message: Woot")

    """@needs_docker  # timing out; likely need to update docker on toil
    def testDocker(self):
        self.checkExitCode("tutorial_docker.py")"""

    def testPromises(self):
        self.checkExpectedPattern("tutorial_promises.py", "i is: 1.*i is: 2.*i is: 3")

    def testServices(self):
        self.checkExitCode("tutorial_services.py")

    """Needs cromwell jar file to run
    def testWdlexample(self):
       self.checkExitCode("tutorial_wdlexample.py")"""


if __name__ == "__main__":
    unittest.main()
