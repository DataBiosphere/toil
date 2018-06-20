import logging
import signal
import time
import uuid
from threading import Thread
from subprocess import CalledProcessError

import os
from pwd import getpwuid
from bd2k.util.files import mkdir_p
from toil.job import Job
from toil.leader import FailedJobsException
from toil.lib.singularity import singularityCall, singularityCheckOutput
from toil.test import ToilTest

_log = logging.getLogger(__name__)


class SingularityTest(ToilTest):
    """
    Tests singularityCall and ensures no containers are left around.


    When running tests you may optionally set the TOIL_TEST_TEMP environment variable to the path
    of a directory where you want temporary test files be placed. The directory will be created
    if it doesn't exist. The path may be relative in which case it will be assumed to be relative
    to the project root. If TOIL_TEST_TEMP is not defined, temporary files and directories will
    be created in the system's default location for such files and any temporary files or
    directories left over from tests will be removed automatically removed during tear down.
    Otherwise, left-over files will not be removed.
    """
    def setUp(self):
        self.tempDir = self._createTempDir(purpose='tempDir')


    def testSingularityPipeChain(self, caching=True):
        """
        Test for piping API for singularityCall().  Using this API (activated when list of
        argument lists is given as parameters), commands a piped together into a chain
        ex:  parameters=[ ['printf', 'x\n y\n'], ['wc', '-l'] ] should execute:
        printf 'x\n y\n' | wc -l
        """
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir, 'jobstore'))
        options.logLevel = 'INFO'
        options.workDir = self.tempDir
        options.clean = 'always'
        if not caching:
            options.disableCaching = True
        A = Job.wrapJobFn(_testSingularityPipeChainFn)
        rv = Job.Runner.startToil(A, options)
        assert rv.strip() == '2'

    def testSingularityPipeChainErrorDetection(self, caching=True):
        """
        By default, executing cmd1 | cmd2 | ... | cmdN, will only return an error
        if cmdN fails.  This can lead to all manor of errors being silently missed.
        This tests to make sure that the piping API for singularityCall() throws an exception
        if non-last commands in the chain fail.
        """
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir, 'jobstore'))
        options.logLevel = 'INFO'
        options.workDir = self.tempDir
        options.clean = 'always'
        if not caching:
            options.disableCaching = True
        A = Job.wrapJobFn(_testSingularityPipeChainErrorFn)
        rv = Job.Runner.startToil(A, options)
        assert rv == True

    def testSingularityPermissions(self, caching=True):
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir, 'jobstore'))
        options.logLevel = 'INFO'
        options.workDir = self.tempDir
        options.clean = 'always'
        if not caching:
            options.disableCaching = True
        A = Job.wrapJobFn(_testSingularityPermissions)
        Job.Runner.startToil(A, options)

    def testSingularityPermissionsNonCaching(self):
        self.testSingularityPermissions(caching=False)

    def testNonCachingSingularityChain(self):
        self.testSingularityPipeChain(caching=False)

    def testNonCachingSingularityChainErrorDetection(self):
        self.testSingularityPipeChainErrorDetection(caching=False)


def _testSingularityPipeChainFn(job):
    """
    Return the result of simple pipe chain.  Should be 2
    """
    parameters = [ ['printf', 'x\n y\n'], ['wc', '-l'] ]
    return singularityCheckOutput(job, tool='docker://quay.io/ucsc_cgl/spooky_test', parameters=parameters)

def _testSingularityPipeChainErrorFn(job):
    """
    Return True if the command exit 1 | wc -l raises a CalledProcessError when run through 
    the singularity interface
    """
    parameters = [ ['exit', '1'], ['wc', '-l'] ]
    try:
        return singularityCheckOutput(job, tool='docker://quay.io/ucsc_cgl/spooky_test', parameters=parameters)
    except CalledProcessError:
        return True
    return False

def _testSingularityPermissions(job):
    testDir = job.fileStore.getLocalTempDir()
    singularityCall(job, tool='docker://ubuntu', workDir=testDir, parameters=[['touch', '/data/test.txt']])
    outFile = os.path.join(testDir, 'test.txt')
    assert os.path.exists(outFile)
    assert not ownerName(outFile) == "root"


def ownerName(filename):
    """
    Determines a given file's owner
    :param str filename: path to a file
    :return: name of filename's owner
    """
    return getpwuid(os.stat(filename).st_uid).pw_name
