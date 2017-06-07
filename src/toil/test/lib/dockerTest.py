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
from toil.lib.docker import dockerCall, dockerCheckOutput, _containerIsRunning, _dockerKill, STOP, FORGO, RM
from toil.test import ToilTest

_log = logging.getLogger(__name__)


class DockerTest(ToilTest):
    """
    Tests dockerCall and ensures no containers are left around.


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

    def testDockerClean(self, caching=True):
        """
        Run the test container that creates a file in the work dir, and sleeps for 5 minutes.  Ensure
        that the calling job gets SIGKILLed after a minute, leaving behind the spooky/ghost/zombie
        container. Ensure that the container is killed on batch system shutdown (through the defer
        mechanism).
        This inherently also tests _docker
        :returns: None
        """
        # We need to test the behaviour of `defer` with `rm` and `detached`. We do not look at the case
        # where `rm` and `detached` are both True.  This is the truth table for the different
        # combinations at the end of the test. R = Running, X = Does not exist, E = Exists but not
        # running.
        #              None     FORGO     STOP    RM
        #    rm        X         R         X      X
        # detached     R         R         E      X
        #  Neither     R         R         E      X
        assert os.getuid() != 0, "Cannot test this if the user is root."
        data_dir = os.path.join(self.tempDir, 'data')
        work_dir = os.path.join(self.tempDir, 'working')
        test_file = os.path.join(data_dir, 'test.txt')
        mkdir_p(data_dir)
        mkdir_p(work_dir)
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir, 'jobstore'))
        options.logLevel = 'INFO'
        options.workDir = work_dir
        options.clean = 'always'
        if not caching:
            options.disableCaching = True
        for rm in (True, False):
            for detached in (True, False):
                if detached and rm:
                    continue
                for defer in (FORGO, STOP, RM, None):
                    # Not using base64 logic here since it might create a name starting with a `-`.
                    container_name = uuid.uuid4().hex
                    A = Job.wrapJobFn(_testDockerCleanFn, data_dir, detached, rm, defer,
                                      container_name)
                    try:
                        Job.Runner.startToil(A, options)
                    except FailedJobsException:
                        # The file created by spooky_container would remain in the directory, and since
                        # it was created inside the container, it would have had uid and gid == 0 (root)
                        # upon creation. If the defer mechanism worked, it should now be non-zero and we
                        # check for that.
                        file_stats = os.stat(test_file)
                        assert file_stats.st_gid != 0
                        assert file_stats.st_uid != 0
                        if (rm and defer != FORGO) or defer == RM:
                            # These containers should not exist
                            assert _containerIsRunning(container_name) is None, \
                                'Container was not removed.'
                        elif defer == STOP:
                            # These containers should exist but be non-running
                            assert _containerIsRunning(container_name) == False, \
                                'Container was not stopped.'
                        else:
                            # These containers will be running
                            assert _containerIsRunning(container_name) == True, \
                                'Container was not running.'
                    finally:
                        # Prepare for the next test.
                        _dockerKill(container_name, RM)
                        os.remove(test_file)

    def testDockerPipeChain(self, caching=True):
        """
        Test for piping API for dockerCall().  Using this API (activated when list of
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
        A = Job.wrapJobFn(_testDockerPipeChainFn)
        rv = Job.Runner.startToil(A, options)
        assert rv.strip() == '2'

    def testDockerPipeChainErrorDetection(self, caching=True):
        """
        By default, executing cmd1 | cmd2 | ... | cmdN, will only return an error
        if cmdN fails.  This can lead to all manor of errors being silently missed.
        This tests to make sure that the piping API for dockerCall() throws an exception
        if non-last commands in the chain fail.
        """
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir, 'jobstore'))
        options.logLevel = 'INFO'
        options.workDir = self.tempDir
        options.clean = 'always'
        if not caching:
            options.disableCaching = True
        A = Job.wrapJobFn(_testDockerPipeChainErrorFn)
        rv = Job.Runner.startToil(A, options)
        assert rv == True

    def testDockerPermissions(self, caching=True):
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir, 'jobstore'))
        options.logLevel = 'INFO'
        options.workDir = self.tempDir
        options.clean = 'always'
        if not caching:
            options.disableCaching = True
        A = Job.wrapJobFn(_testDockerPermissions)
        Job.Runner.startToil(A, options)

    def testDockerPermissionsNonCaching(self):
        self.testDockerPermissions(caching=False)

    def testNonCachingDockerChain(self):
        self.testDockerPipeChain(caching=False)

    def testNonCachingDockerChainErrorDetection(self):
        self.testDockerPipeChainErrorDetection(caching=False)

    def testNonCachingDockerClean(self):
        self.testDockerClean(caching=False)


def _testDockerCleanFn(job, workDir, detached=None, rm=None, defer=None, containerName=None):
    """
    Test function for test docker_clean.  Runs a container with given flags and then dies leaving
    behind a zombie container
    :param toil.job.Job job: job
    :param workDir: See `work_dir=` in :func:`dockerCall`
    :param bool rm: See `rm=` in :func:`dockerCall`
    :param bool detached: See `detached=` in :func:`dockerCall`
    :param int defer: See `defer=` in :func:`dockerCall`
    :param str containerName: See `container_name=` in :func:`dockerCall`
    :return:
    """
    dockerParameters = ['--log-driver=none', '-v', os.path.abspath(workDir) + ':/data',
                        '--name', containerName]
    if detached:
        dockerParameters.append('-d')
    if rm:
        dockerParameters.append('--rm')

    def killSelf():
        test_file = os.path.join(workDir, 'test.txt')
        # This will kill the worker once we are sure the docker container started
        while not os.path.exists(test_file):
            _log.debug('Waiting on the file created by spooky_container.')
            time.sleep(1)
        # By the time we reach here, we are sure the container is running.
        os.kill(os.getpid(), signal.SIGKILL)  # signal.SIGINT)
    t = Thread(target=killSelf)
    # Make it a daemon thread so that thread failure doesn't hang tests.
    t.daemon = True
    t.start()
    dockerCall(job, tool='quay.io/ucsc_cgl/spooky_test', workDir=workDir, defer=defer, dockerParameters=dockerParameters)


def _testDockerPipeChainFn(job):
    """
    Return the result of simple pipe chain.  Should be 2
    """
    parameters = [ ['printf', 'x\n y\n'], ['wc', '-l'] ]
    return dockerCheckOutput(job, tool='quay.io/ucsc_cgl/spooky_test', parameters=parameters)

def _testDockerPipeChainErrorFn(job):
    """
    Return True if the command exit 1 | wc -l raises a CalledProcessError when run through 
    the docker interface
    """
    parameters = [ ['exit', '1'], ['wc', '-l'] ]
    try:
        return dockerCheckOutput(job, tool='quay.io/ucsc_cgl/spooky_test', parameters=parameters)
    except CalledProcessError:
        return True
    return False

def _testDockerPermissions(job):
    testDir = job.fileStore.getLocalTempDir()
    dockerCall(job, tool='ubuntu', workDir=testDir, parameters=[['touch', '/data/test.txt']])
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
