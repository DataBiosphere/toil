from __future__ import absolute_import
import unittest
import os
import re
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from toil import subprocess
from toil.test import ToilTest
from toil.test import needs_cwl
from toil.test import needs_docker
from toil.lib.docker import apiDockerCall

class ToilDocumentationTest(ToilTest):
    """Tests for scripts in the toil tutorials"""

    def tearDown(self):
        output_files = ["sample_1_output.txt", "sample_2_output.txt", "sample_3_output.txt"]
        for output in output_files:
            output_dir = os.path.abspath("scripts/cwlExampleFiles")
            if os.path.exists(os.path.abspath(os.path.join(output_dir, output))):
                print("!!!!!!!!!!")
                os.remove(os.path.abspath(os.path.join(output_dir, output)))

        unittest.TestCase.tearDown(self)

    """Check that the exit code is 0"""
    def checkExitCode(self, p):
        out = subprocess.call(["python", p, "file:my-jobstore", "--clean=always"])
        self.assertEqual(out, 0, out)

    """Just check the exit code"""
    def runTest1(self, script):
        program = os.path.abspath("scripts/" + script)
        self.checkExitCode(program)

    """Check the exit code and the output"""
    def runTest2(self, script, expectedOutput):
        program = os.path.abspath("scripts/" + script)
        self.checkExitCode(program)

        # Check that the expected output is there
        out = subprocess.check_output(['python', program, 'file:my-jobstore', '--logOff', '--clean=always'])
        index = out.find(expectedOutput)
        self.assertGreater(index, -1, index)

    """Check the exit code and look for a pattern"""
    def runTest3(self, script, expectedPattern):
        program = os.path.abspath("scripts/" + script)
        self.checkExitCode(program)

        # Check that the expected outputs are somewhere in the log messages
        out = subprocess.check_output(['python', program, 'file:my-jobstore', '--clean=always'])
        pattern = re.compile(expectedPattern, re.DOTALL)
        n = re.search(pattern, out)
        self.assertNotEqual(n, None, n)

    @needs_cwl
    def testCwlexample(self):
        self.runTest1("tutorial_cwlexample.py")

    def testDiscoverfiles(self):
        self.runTest1("tutorial_discoverfiles.py")

    def testDynamic(self):
        self.runTest1("tutorial_dynamic.py")

    def testEncapsulation(self):
        self.runTest1("tutorial_encapsulation.py")

    def testEncapsulation2(self):
        self.runTest1("tutorial_encapsulation2.py")

    def testHelloworld(self):
        self.runTest2("tutorial_helloworld.py", "Hello, world!, here's a message: You did it!\n")

    def testInvokeworkflow(self):
        self.runTest2("tutorial_invokeworkflow.py", "Hello, world!, here's a message: Woot\n")

    def testInvokeworkflow2(self):
        self.runTest2("tutorial_invokeworkflow2.py", "Hello, world!, I have a message: Woot!\n")

    def testJobFunctions(self):
        self.runTest2("tutorial_jobfunctions.py", "Hello world, I have a message: Woot!\n")

    def testManaging(self):
        self.runTest1("tutorial_managing.py")

    def testManaging2(self):
        self.runTest1("tutorial_managing2.py")

    def testMultiplejobs(self):
        self.runTest3("tutorial_multiplejobs.py", "Hello world, I have a message: first.*Hello world, I have a message: "
                                         "second or third.*Hello world, I have a message: second or third.*Hello world,"
                                         " I have a message: last")

    def testMultiplejobs2(self):
        self.runTest3("tutorial_multiplejobs2.py", "Hello world, I have a message: first.*Hello world, I have a message: "
                                         "second or third.*Hello world, I have a message: second or third.*Hello world,"
                                         " I have a message: last")

    def testMultiplejobs3(self):
        self.runTest3("tutorial_multiplejobs3.py", "Hello world, I have a message: first.*Hello world, I have a message: "
                                         "second or third.*Hello world, I have a message: second or third.*Hello world,"
                                         " I have a message: last")

    def testPromises2(self):
        self.runTest2("tutorial_promises2.py", "['00000', '00001', '00010', '00011', '00100', '00101', '00110', '00111', '01000',"
                      " '01001', '01010', '01011', '01100', '01101', '01110', '01111', '10000', '10001', "
                      "'10010', '10011', '10100', '10101', '10110', '10111', '11000', '11001', '11010', "
                      "'11011', '11100', '11101', '11110', '11111']")

    def testQuickstart(self):
        self.runTest2("tutorial_quickstart.py", "Hello, world!, here's a message: Woot\n")

    def testRequirements(self):
        self.runTest1("tutorial_requirements.py")

    def testArguments(self):
        self.runTest2("tutorial_arguments.py", "Hello, world!, here's a message: Woot")

    @needs_docker
    def testDocker(self):
        self.runTest1("tutorial_docker.py")

    def testPromises(self):
        self.runTest3("tutorial_promises.py", "i is: 1.*i is: 2.*i is: 3")

    def testServices(self):
        self.runTest1("tutorial_services.py")

    """Needs cromwell jar file to run
    def testWdlexample(self):
       self.runTest1("tutorial_wdlexample.py")"""

if __name__ == "__main__":
    unittest.main()


