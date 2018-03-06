# Copyright (C) 2015-2018 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
import logging
import signal
import time
import os
import uuid
import docker
from toil import subprocess
from threading import Thread
from docker.errors import ContainerError

from toil.test import mkdir_p
from toil.job import Job
from toil.leader import FailedJobsException
from toil.test import ToilTest, slow, needs_appliance
from toil.lib import FORGO, STOP, RM
from toil.lib.docker import dockerCall, dockerCheckOutput, apiDockerCall, containerIsRunning, dockerKill

# only needed for subprocessDockerCall tests
from pwd import getpwuid
from bd2k.util.retry import retry
from toil.lib import dockerPredicate


logger = logging.getLogger(__name__)


@needs_appliance
class DockerTest(ToilTest):
    """
    Tests dockerCall and ensures no containers are left around.
    When running tests you may optionally set the TOIL_TEST_TEMP environment
    variable to the path of a directory where you want temporary test files be
    placed. The directory will be created if it doesn't exist. The path may be
    relative in which case it will be assumed to be relative to the project
    root. If TOIL_TEST_TEMP is not defined, temporary files and directories will
    be created in the system's default location for such files and any temporary
    files or directories left over from tests will be removed automatically
    removed during tear down.
    Otherwise, left-over files will not be removed.
    """
    def setUp(self):
        self.tempDir = self._createTempDir(purpose='tempDir')
        self.dockerTestLogLevel = 'INFO'

    def testDockerClean(self,
                        disableCaching=True,
                        detached=True,
                        rm=True,
                        deferParam=None):
        """
        Run the test container that creates a file in the work dir, and sleeps
        for 5 minutes.
        Ensure that the calling job gets SIGKILLed after a minute, leaving
        behind the spooky/ghost/zombie container. Ensure that the container is
        killed on batch system shutdown (through the deferParam mechanism).
        """

        # We need to test the behaviour of `deferParam` with `rm` and
        # `detached`. We do not look at the case where `rm` and `detached` are
        # both True.  This is the truth table for the different combinations at
        # the end of the test. R = Running, X = Does not exist, E = Exists but
        # not running.
        #              None     FORGO     STOP    RM
        #    rm        X         R         X      X
        # detached     R         R         E      X
        #  Neither     R         R         E      X

        data_dir = os.path.join(self.tempDir, 'data')
        working_dir = os.path.join(self.tempDir, 'working')
        test_file = os.path.join(working_dir, 'test.txt')

        mkdir_p(data_dir)
        mkdir_p(working_dir)

        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir,
                                                            'jobstore'))
        options.logLevel = self.dockerTestLogLevel
        options.workDir = working_dir
        options.clean = 'always'
        options.disableCaching = disableCaching

        # No base64 logic since it might create a name starting with a `-`.
        container_name = uuid.uuid4().hex
        A = Job.wrapJobFn(_testDockerCleanFn,
                          working_dir,
                          detached,
                          rm,
                          deferParam,
                          container_name)
        try:
            Job.Runner.startToil(A, options)
        except FailedJobsException:
            # The file created by spooky_container would remain in the directory
            # and since it was created inside the container, it would have had
            # uid and gid == 0 (root) which may cause problems when docker
            # attempts to clean up the jobstore.
            file_stats = os.stat(test_file)
            assert file_stats.st_gid != 0
            assert file_stats.st_uid != 0

            if (rm and (deferParam != FORGO)) or deferParam == RM:
                # These containers should not exist
                assert containerIsRunning(container_name) is None, \
                    'Container was not removed.'

            elif deferParam == STOP:
                # These containers should exist but be non-running
                assert containerIsRunning(container_name) == False, \
                    'Container was not stopped.'

            else:
                # These containers will be running
                assert containerIsRunning(container_name) == True, \
                    'Container was not running.'
        client = docker.from_env(version='auto')
        dockerKill(container_name, client)
        try:
            os.remove(test_file)
        except:
            pass

    def testDockerClean_CRx_FORGO(self):
        self.testDockerClean(disableCaching=True, detached=False, rm=True,
                             deferParam=FORGO)

    def testDockerClean_CRx_STOP(self):
        self.testDockerClean(disableCaching=True, detached=False, rm=True,
                             deferParam=STOP)

    def testDockerClean_CRx_RM(self):
        self.testDockerClean(disableCaching=True, detached=False, rm=True,
                             deferParam=RM)

    @slow
    def testDockerClean_CRx_None(self):
        self.testDockerClean(disableCaching=True, detached=False, rm=True,
                             deferParam=None)

    @slow
    def testDockerClean_CxD_FORGO(self):
        self.testDockerClean(disableCaching=True, detached=True, rm=False,
                             deferParam=FORGO)

    @slow
    def testDockerClean_CxD_STOP(self):
        self.testDockerClean(disableCaching=True, detached=True, rm=False,
                             deferParam=STOP)

    @slow
    def testDockerClean_CxD_RM(self):
        self.testDockerClean(disableCaching=True, detached=True, rm=False,
                             deferParam=RM)

    @slow
    def testDockerClean_CxD_None(self):
        self.testDockerClean(disableCaching=True, detached=True, rm=False,
                             deferParam=None)

    @slow
    def testDockerClean_Cxx_FORGO(self):
        self.testDockerClean(disableCaching=True, detached=False, rm=False,
                             deferParam=FORGO)

    @slow
    def testDockerClean_Cxx_STOP(self):
        self.testDockerClean(disableCaching=True, detached=False, rm=False,
                             deferParam=STOP)

    @slow
    def testDockerClean_Cxx_RM(self):
        self.testDockerClean(disableCaching=True, detached=False, rm=False,
                             deferParam=RM)

    @slow
    def testDockerClean_Cxx_None(self):
        self.testDockerClean(disableCaching=True, detached=False, rm=False,
                             deferParam=None)

    @slow
    def testDockerClean_xRx_FORGO(self):
        self.testDockerClean(disableCaching=False, detached=False, rm=True,
                             deferParam=FORGO)

    @slow
    def testDockerClean_xRx_STOP(self):
        self.testDockerClean(disableCaching=False, detached=False, rm=True,
                             deferParam=STOP)

    @slow
    def testDockerClean_xRx_RM(self):
        self.testDockerClean(disableCaching=False, detached=False, rm=True,
                             deferParam=RM)

    @slow
    def testDockerClean_xRx_None(self):
        self.testDockerClean(disableCaching=False, detached=False, rm=True,
                             deferParam=None)

    @slow
    def testDockerClean_xxD_FORGO(self):
        self.testDockerClean(disableCaching=False, detached=True, rm=False,
                             deferParam=FORGO)

    @slow
    def testDockerClean_xxD_STOP(self):
        self.testDockerClean(disableCaching=False, detached=True, rm=False,
                             deferParam=STOP)

    @slow
    def testDockerClean_xxD_RM(self):
        self.testDockerClean(disableCaching=False, detached=True, rm=False,
                             deferParam=RM)

    @slow
    def testDockerClean_xxD_None(self):
        self.testDockerClean(disableCaching=False, detached=True, rm=False,
                             deferParam=None)

    @slow
    def testDockerClean_xxx_FORGO(self):
        self.testDockerClean(disableCaching=False, detached=False, rm=False,
                             deferParam=FORGO)

    @slow
    def testDockerClean_xxx_STOP(self):
        self.testDockerClean(disableCaching=False, detached=False, rm=False,
                             deferParam=STOP)

    @slow
    def testDockerClean_xxx_RM(self):
        self.testDockerClean(disableCaching=False, detached=False, rm=False,
                             deferParam=RM)

    @slow
    def testDockerClean_xxx_None(self):
        self.testDockerClean(disableCaching=False, detached=False, rm=False,
                             deferParam=None)

    def testDockerPipeChain(self, disableCaching=True):
        """
        Test for piping API for dockerCall().  Using this API (activated when
        list of argument lists is given as parameters), commands a piped
        together into a chain.
        ex:  parameters=[ ['printf', 'x\n y\n'], ['wc', '-l'] ] should execute:
        printf 'x\n y\n' | wc -l
        """
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir,
                                                            'jobstore'))
        options.logLevel = self.dockerTestLogLevel
        options.workDir = self.tempDir
        options.clean = 'always'
        options.caching = disableCaching
        A = Job.wrapJobFn(_testDockerPipeChainFn)
        rv = Job.Runner.startToil(A, options)
        assert rv.strip() == '2'

    def testDockerPipeChainErrorDetection(self, disableCaching=True):
        """
        By default, executing cmd1 | cmd2 | ... | cmdN, will only return an
        error if cmdN fails.  This can lead to all manor of errors being
        silently missed.  This tests to make sure that the piping API for
        dockerCall() throws an exception if non-last commands in the chain fail.
        """
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir,
                                                            'jobstore'))
        options.logLevel = self.dockerTestLogLevel
        options.workDir = self.tempDir
        options.clean = 'always'
        options.caching = disableCaching
        A = Job.wrapJobFn(_testDockerPipeChainErrorFn)
        rv = Job.Runner.startToil(A, options)
        assert rv == True

    def testNonCachingDockerChain(self):
        self.testDockerPipeChain(disableCaching=False)

    def testNonCachingDockerChainErrorDetection(self):
        self.testDockerPipeChainErrorDetection(disableCaching=False)

    ########################################################
    # all tests below this point test subprocessDockerCall #
    ########################################################

    @slow
    def testSubprocessDockerClean(self, caching=True):
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
                    A = Job.wrapJobFn(_testSubprocessDockerCleanFn, data_dir, detached, rm, defer,
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
                            assert containerIsRunning(container_name) is None, \
                                'Container was not removed.'
                        elif defer == STOP:
                            # These containers should exist but be non-running
                            assert containerIsRunning(container_name) == False, \
                                'Container was not stopped.'
                        else:
                            # These containers will be running
                            assert containerIsRunning(container_name) == True, \
                                'Container was not running.'
                    finally:
                        # Prepare for the next test.
                        _dockerKill(container_name, RM)
                        os.remove(test_file)

    def testSubprocessDockerPipeChain(self, caching=True):
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
        A = Job.wrapJobFn(_testSubprocessDockerPipeChainFn)
        rv = Job.Runner.startToil(A, options)
        assert rv.strip() == '2'

    def testSubprocessDockerPipeChainErrorDetection(self, caching=True):
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
        A = Job.wrapJobFn(_testSubprocessDockerPipeChainErrorFn)
        rv = Job.Runner.startToil(A, options)
        assert rv == True

    def testSubprocessDockerPermissions(self, caching=True):
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir, 'jobstore'))
        options.logLevel = 'INFO'
        options.workDir = self.tempDir
        options.clean = 'always'
        if not caching:
            options.disableCaching = True
        A = Job.wrapJobFn(_testSubprocessDockerPermissions)
        Job.Runner.startToil(A, options)

    def testSubprocessDockerPermissionsNonCaching(self):
        self.testSubprocessDockerPermissions(caching=False)

    def testSubprocessNonCachingDockerChain(self):
        self.testSubprocessDockerPipeChain(caching=False)

    def testSubprocessNonCachingDockerChainErrorDetection(self):
        self.testSubprocessDockerPipeChainErrorDetection(caching=False)

    @slow
    def testSubprocessNonCachingDockerClean(self):
        self.testSubprocessDockerClean(caching=False)

def _testDockerCleanFn(job,
                       working_dir,
                       detached=None,
                       rm=None,
                       deferParam=None,
                       containerName=None):
    """
    Test function for test docker_clean.  Runs a container with given flags and
    then dies leaving behind a zombie container.
    :param toil.job.Job job: job
    :param working_dir: See `work_dir=` in :func:`dockerCall`
    :param bool rm: See `rm=` in :func:`dockerCall`
    :param bool detached: See `detached=` in :func:`dockerCall`
    :param int deferParam: See `deferParam=` in :func:`dockerCall`
    :param str containerName: See `container_name=` in :func:`dockerCall`
    """
    def killSelf():
        test_file = os.path.join(working_dir, 'test.txt')
        # Kill the worker once we are sure the docker container is started
        while not os.path.exists(test_file):
            logger.debug('Waiting on the file created by spooky_container.')
            time.sleep(1)
        # By the time we reach here, we are sure the container is running.
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGKILL)

    t = Thread(target=killSelf)
    # Make it a daemon thread so that thread failure doesn't hang tests.
    t.daemon = True
    t.start()
    apiDockerCall(job,
                  image='quay.io/ucsc_cgl/spooky_test',
                  working_dir=working_dir,
                  deferParam=deferParam,
                  containerName=containerName,
                  detach=detached,
                  remove=rm,
                  privileged=True)

def _testDockerPipeChainFn(job):
    """Return the result of a simple pipe chain.  Should be 2."""
    parameters = [ ['printf', 'x\n y\n'], ['wc', '-l'] ]
    return apiDockerCall(job,
                         image='quay.io/ucsc_cgl/spooky_test',
                         parameters=parameters,
                         privileged=True)

def _testDockerPipeChainErrorFn(job):
    """Return True if the command exit 1 | wc -l raises a ContainerError."""
    parameters = [ ['exit', '1'], ['wc', '-l'] ]
    try:
        apiDockerCall(job,
                      image='quay.io/ucsc_cgl/spooky_test',
                      parameters=parameters)
    except ContainerError:
        return True
    return False

def _testSubprocessDockerCleanFn(job, workDir, detached=None, rm=None, defer=None, containerName=None):
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
            logger.debug('Waiting on the file created by spooky_container.')
            time.sleep(1)
        # By the time we reach here, we are sure the container is running.
        os.kill(os.getpid(), signal.SIGKILL)  # signal.SIGINT)
    t = Thread(target=killSelf)
    # Make it a daemon thread so that thread failure doesn't hang tests.
    t.daemon = True
    t.start()
    dockerCall(job, tool='quay.io/ucsc_cgl/spooky_test', workDir=workDir, defer=defer, dockerParameters=dockerParameters)

def _testSubprocessDockerPipeChainFn(job):
    """
    Return the result of simple pipe chain.  Should be 2
    """
    parameters = [ ['printf', 'x\n y\n'], ['wc', '-l'] ]
    return dockerCheckOutput(job, tool='quay.io/ucsc_cgl/spooky_test', parameters=parameters)

def _testSubprocessDockerPipeChainErrorFn(job):
    """
    Return True if the command exit 1 | wc -l raises a CalledProcessError when run through 
    the docker interface
    """
    parameters = [ ['exit', '1'], ['wc', '-l'] ]
    try:
        return dockerCheckOutput(job, tool='quay.io/ucsc_cgl/spooky_test', parameters=parameters)
    except subprocess.CalledProcessError:
        return True
    return False

def _testSubprocessDockerPermissions(job):
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

def _dockerKill(containerName, action):
    """
    Kills the specified container.
    :param str containerName: The name of the container created by docker_call
    :param int action: What action should be taken on the container?  See `defer=` in
           :func:`docker_call`
    """
    running = containerIsRunning(containerName)
    if running is None:
        # This means that the container doesn't exist.  We will see this if the container was run
        # with --rm and has already exited before this call.
        logger.info('The container with name "%s" appears to have already been removed.  Nothing to '
                     'do.', containerName)
    else:
        if action in (None, FORGO):
            logger.info('The container with name %s continues to exist as we were asked to forgo a '
                         'post-job action on it.', containerName)
        else:
            logger.info('The container with name %s exists. Running user-specified defer functions.',
                         containerName)
            if running and action >= STOP:
                logger.info('Stopping container "%s".', containerName)
                for attempt in retry(predicate=dockerPredicate):
                    with attempt:
                        subprocess.check_call(['docker', 'stop', containerName])
            else:
                logger.info('The container "%s" was not found to be running.', containerName)
            if action >= RM:
                # If the container was run with --rm, then stop will most likely remove the
                # container.  We first check if it is running then remove it.
                running = containerIsRunning(containerName)
                if running is not None:
                    logger.info('Removing container "%s".', containerName)
                    for attempt in retry(predicate=dockerPredicate):
                        with attempt:
                            subprocess.check_call(['docker', 'rm', '-f', containerName])
                else:
                    logger.info('The container "%s" was not found on the system.  Nothing to remove.',
                                 containerName)