import os
import re
import shutil
import subprocess
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from toil.test import ToilTest, needs_cwl, needs_docker, travis_test
from toil.version import python


class ToilDocumentationTest(ToilTest):
    """Tests for scripts in the toil tutorials."""
    @classmethod
    def setUpClass(cls):
        super(ToilTest, cls).setUpClass()
        cls.directory = os.path.dirname(os.path.abspath(__file__))

    def tearDown(self):
        super(ToilTest, self).tearDown()
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
        if isinstance(stdout, bytes):
            stdout = stdout.decode('utf-8')
            stderr = stderr.decode('utf-8')
        if not process.returncode == 0:
            raise RuntimeError(stderr)
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

    @travis_test
    def testDiscoverfiles(self):
        self.checkExitCode("tutorial_discoverfiles.py")

    @travis_test
    def testDynamic(self):
        self.checkExitCode("tutorial_dynamic.py")

    @travis_test
    def testEncapsulation(self):
        self.checkExitCode("tutorial_encapsulation.py")

    @travis_test
    def testEncapsulation2(self):
        self.checkExitCode("tutorial_encapsulation2.py")

    @travis_test
    def testHelloworld(self):
        self.checkExpectedOut("tutorial_helloworld.py", "Hello, world!, here's a message: You did it!\n")

    @travis_test
    def testInvokeworkflow(self):
        self.checkExpectedOut("tutorial_invokeworkflow.py", "Hello, world!, here's a message: Woot\n")

    @travis_test
    def testInvokeworkflow2(self):
        self.checkExpectedOut("tutorial_invokeworkflow2.py", "Hello, world!, I have a message: Woot!\n")

    @travis_test
    def testJobFunctions(self):
        self.checkExpectedOut("tutorial_jobfunctions.py", "Hello world, I have a message: Woot!\n")

    @travis_test
    def testManaging(self):
        self.checkExitCode("tutorial_managing.py")

    @travis_test
    def testManaging2(self):
        self.checkExitCode("tutorial_managing2.py")

    @travis_test
    def testMultiplejobs(self):
        self.checkExpectedPattern("tutorial_multiplejobs.py",
                                  "Hello world, I have a message: first.*Hello world, I have a message: "
                                  "second or third.*Hello world, I have a message: second or third.*Hello world,"
                                  " I have a message: last")

    @travis_test
    def testMultiplejobs2(self):
        self.checkExpectedPattern("tutorial_multiplejobs2.py",
                                  "Hello world, I have a message: first.*Hello world, I have a message: "
                                  "second or third.*Hello world, I have a message: second or third.*Hello world,"
                                  " I have a message: last")

    @travis_test
    def testMultiplejobs3(self):
        self.checkExpectedPattern("tutorial_multiplejobs3.py",
                                  "Hello world, I have a message: first.*Hello world, I have a message: "
                                  "second or third.*Hello world, I have a message: second or third.*Hello world,"
                                  " I have a message: last")

    @travis_test
    def testPromises2(self):
        self.checkExpectedOut("tutorial_promises2.py",
                              "['00000', '00001', '00010', '00011', '00100', '00101', '00110', '00111',"
                              " '01000', '01001', '01010', '01011', '01100', '01101', '01110', '01111',"
                              " '10000', '10001', '10010', '10011', '10100', '10101', '10110', '10111',"
                              " '11000', '11001', '11010', '11011', '11100', '11101', '11110', '11111']")

    @travis_test
    def testQuickstart(self):
        self.checkExpectedOut("tutorial_quickstart.py", "Hello, world!, here's a message: Woot\n")

    @travis_test
    def testRequirements(self):
        self.checkExitCode("tutorial_requirements.py")

    @travis_test
    def testArguments(self):
        self.checkExpectedOut("tutorial_arguments.py", "Hello, world!, here's a message: Woot")

    @needs_docker
    def testDocker(self):
        self.checkExitCode("tutorial_docker.py")

    @travis_test
    def testPromises(self):
        self.checkExpectedPattern("tutorial_promises.py", "i is: 1.*i is: 2.*i is: 3")

    @travis_test
    def testServices(self):
        self.checkExitCode("tutorial_services.py")

    @unittest.skip('Needs cromwell jar file to run.')
    def testWdlexample(self):
        self.checkExitCode("tutorial_wdlexample.py")


if __name__ == "__main__":
    unittest.main()
