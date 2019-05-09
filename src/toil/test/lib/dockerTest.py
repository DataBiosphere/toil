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
from threading import Thread
from docker.errors import ContainerError

from toil import subprocess
from toil.test import mkdir_p
from toil.job import Job
from toil.leader import FailedJobsException
from toil.test import ToilTest, slow, needs_appliance
from toil.lib.docker import apiDockerCall, containerIsRunning, dockerKill
from toil.lib.docker import FORGO, STOP, RM

# only needed for subprocessDockerCall tests
from pwd import getpwuid
from toil.lib.retry import retry


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
