# Copyright (C) 2015-2020 Regents of the University of California
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
import logging
import signal
import time
import os
import sys
import uuid
import docker
from threading import Thread
from docker.errors import ContainerError

from toil.job import Job
from toil.leader import FailedJobsException
from toil.test import ToilTest, slow, needs_docker
from toil.lib.docker import apiDockerCall, containerIsRunning, dockerKill
from toil.lib.docker import FORGO, STOP, RM


logger = logging.getLogger(__name__)


@needs_docker
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

        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(working_dir, exist_ok=True)

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
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir, 'jobstore'))
        options.logLevel = self.dockerTestLogLevel
        options.workDir = self.tempDir
        options.clean = 'always'
        options.caching = disableCaching
        A = Job.wrapJobFn(_testDockerPipeChainFn)
        rv = Job.Runner.startToil(A, options)
        logger.info('Container pipeline result: %s', repr(rv))
        if sys.version_info >= (3, 0):
            rv = rv.decode('utf-8')
        assert rv.strip() == '2'

    def testDockerPipeChainErrorDetection(self, disableCaching=True):
        """
        By default, executing cmd1 | cmd2 | ... | cmdN, will only return an
        error if cmdN fails.  This can lead to all manor of errors being
        silently missed.  This tests to make sure that the piping API for
        dockerCall() throws an exception if non-last commands in the chain fail.
        """
        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir, 'jobstore'))
        options.logLevel = self.dockerTestLogLevel
        options.workDir = self.tempDir
        options.clean = 'always'
        options.caching = disableCaching
        A = Job.wrapJobFn(_testDockerPipeChainErrorFn)
        rv = Job.Runner.startToil(A, options)
        assert rv is True

    def testNonCachingDockerChain(self):
        self.testDockerPipeChain(disableCaching=False)

    def testNonCachingDockerChainErrorDetection(self):
        self.testDockerPipeChainErrorDetection(disableCaching=False)

    def testDockerLogs(self, stream=False, demux=False):
        """Test for the different log outputs when deatch=False."""

        working_dir = os.path.join(self.tempDir, 'working')
        script_file = os.path.join(working_dir, 'script.sh')
        os.makedirs(working_dir, exist_ok=True)

        options = Job.Runner.getDefaultOptions(os.path.join(self.tempDir, 'jobstore'))
        options.logLevel = self.dockerTestLogLevel
        options.workDir = working_dir
        options.clean = 'always'
        A = Job.wrapJobFn(_testDockerLogsFn,
                          working_dir=working_dir,
                          script_file=script_file,
                          stream=stream,
                          demux=demux)

        try:
            rv = Job.Runner.startToil(A, options)
            assert rv == True
        finally:
            try:
                os.remove(script_file)
            except:
                pass

    def testDockerLogs_Stream(self):
        self.testDockerLogs(stream=True, demux=False)

    def testDockerLogs_Demux(self):
        self.testDockerLogs(stream=False, demux=True)

    def testDockerLogs_Demux_Stream(self):
        self.testDockerLogs(stream=True, demux=True)


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
    parameters = [['printf', 'x\n y\n'], ['wc', '-l']]
    return apiDockerCall(job,
                         image='quay.io/ucsc_cgl/ubuntu:20.04',
                         parameters=parameters,
                         privileged=True)


def _testDockerPipeChainErrorFn(job):
    """Return True if the command exit 1 | wc -l raises a ContainerError."""
    parameters = [['exit', '1'], ['wc', '-l']]
    try:
        apiDockerCall(job,
                      image='quay.io/ucsc_cgl/spooky_test',
                      parameters=parameters)
    except ContainerError:
        return True
    return False


def _testDockerLogsFn(job,
                      working_dir,
                      script_file,
                      stream=False,
                      demux=False):
    """Return True if the test succeeds. Otherwise Exception is raised."""

    # we write a script file because the redirection operator, '>&2', is wrapped
    # in quotes when passed as parameters.
    import textwrap
    bash_script = textwrap.dedent('''
    #!/bin/bash
    echo hello stdout ;
    echo hello stderr >&2 ;
    echo hello stdout ;
    echo hello stderr >&2 ;
    echo hello stdout ;
    echo hello stdout ;
    ''')

    with open(script_file, 'w') as file:
        file.write(bash_script)

    out = apiDockerCall(job,
                        image='quay.io/ucsc_cgl/ubuntu:20.04',
                        working_dir=working_dir,
                        parameters=[script_file],
                        volumes={working_dir: {'bind': working_dir, 'mode': 'rw'}},
                        entrypoint="/bin/bash",
                        stdout=True,
                        stderr=True,
                        stream=stream,
                        demux=demux)

    # we check the output length because order is not guaranteed.
    if stream:
        if demux:
            # a generator with tuples of (stdout, stderr)
            assert hasattr(out, '__iter__')
            for _ in range(6):
                stdout, stderr = next(out)
                if stdout:
                    # len('hello stdout\n') == 13
                    assert len(stdout) == 13
                elif stderr:
                    assert len(stderr) == 13
                else:
                    assert False
        else:
            # a generator with bytes
            assert hasattr(out, '__iter__')
            for _ in range(6):
                assert len(next(out)) == 13
    else:
        if demux:
            # a tuple of (stdout, stderr)
            stdout, stderr = out
            # len('hello stdout\n' * 4) == 52
            assert len(stdout) == 52
            # len('hello stderr\n' * 2) == 26
            assert len(stderr) == 26
        else:
            # a bytes object
            # len('hello stdout\n' * 4 + 'hello stderr\n' * 2) == 78
            assert len(out) == 78

    return True
