# Copyright (C) 2015-2016 Regents of the University of California
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
from __future__ import print_function

from builtins import str
from builtins import range
from builtins import object
import filecmp
from abc import abstractmethod, ABCMeta
from struct import pack, unpack
from uuid import uuid4

from toil.job import Job
from toil.fileStores.cachingFileStore import IllegalDeletionCacheError, CacheUnbalancedError, CachingFileStore
from toil.test import ToilTest, slow, travis_test
from toil.leader import FailedJobsException
from toil.jobStores.abstractJobStore import NoSuchFileException

import collections
import inspect
import os
import random
import signal
import time
import pytest

# Python 3 compatibility imports
from six.moves import xrange
from future.utils import with_metaclass

# Some tests take too long on the AWS jobstore and are unquitable for CI.  They can be
# be run during manual tests by setting this to False.
testingIsAutomatic = True

class DeferredFunctionTest(with_metaclass(ABCMeta, ToilTest)):
    """
    Test the deferred function system.
    """
    # This determines what job store type to use.
    jobStoreType = 'file'

    def _getTestJobStore(self):
        if self.jobStoreType == 'file':
            return self._getTestJobStorePath()
        elif self.jobStoreType == 'aws':
            return 'aws:%s:cache-tests-%s' % (self.awsRegion(), uuid4())
        elif self.jobStoreType == 'google':
            projectID = os.getenv('TOIL_GOOGLE_PROJECTID')
            return 'google:%s:cache-tests-%s' % (projectID, str(uuid4()))
        else:
            raise RuntimeError('Illegal job store type.')

    def setUp(self):
        super(DeferredFunctionTest, self).setUp()
        testDir = self._createTempDir()
        self.options = Job.Runner.getDefaultOptions(self._getTestJobStore())
        self.options.logLevel = 'INFO'
        self.options.workDir = testDir
        self.options.clean = 'always'
        self.options.logFile = os.path.join(testDir, 'logFile')

    # Tests for the various defer possibilities
    @travis_test
    def testDeferredFunctionRunsWithMethod(self):
        """
        Refer docstring in _testDeferredFunctionRuns.
        Test with Method
        """
        self._testDeferredFunctionRuns(_writeNonLocalFilesMethod)

    @travis_test
    def testDeferredFunctionRunsWithClassMethod(self):
        """
        Refer docstring in _testDeferredFunctionRuns.
        Test with Class Method
        """
        self._testDeferredFunctionRuns(_writeNonLocalFilesClassMethod)

    @travis_test
    def testDeferredFunctionRunsWithLambda(self):
        """
        Refer docstring in _testDeferredFunctionRuns.
        Test with Lambda
        """
        self._testDeferredFunctionRuns(_writeNonLocalFilesLambda)

    def _testDeferredFunctionRuns(self, callableFn):
        """
        Create 2 files. Make a job that writes data to them. Register a deferred function that
        deletes the two files (one passed as an arg, and one as a kwarg) and later assert that
        the files have been deleted.

        :param function callableFn: The function to use in the test.
        :return: None
        """
        workdir = self._createTempDir(purpose='nonLocalDir')
        nonLocalFile1 = os.path.join(workdir, str(uuid4()))
        nonLocalFile2 = os.path.join(workdir, str(uuid4()))
        open(nonLocalFile1, 'w').close()
        open(nonLocalFile2, 'w').close()
        assert os.path.exists(nonLocalFile1)
        assert os.path.exists(nonLocalFile2)
        A = Job.wrapJobFn(callableFn, files=(nonLocalFile1, nonLocalFile2))
        Job.Runner.startToil(A, self.options)
        assert not os.path.exists(nonLocalFile1)
        assert not os.path.exists(nonLocalFile2)

    @slow
    def testDeferredFunctionRunsWithFailures(self):
        """
        Create 2 non local filesto use as flags.  Create a job that registers a function that
        deletes one non local file.  If that file exists, the job SIGKILLs itself. If it doesn't
        exist, the job registers a second deferred function to delete the second non local file
        and exits normally.

        Initially the first file exists, so the job should SIGKILL itself and neither deferred
        function will run (in fact, the second should not even be registered). On the restart,
        the first deferred function should run and the first file should not exist, but the
        second one should.  We assert the presence of the second, then register the second
        deferred function and exit normally.  At the end of the test, neither file should exist.

        Incidentally, this also tests for multiple registered deferred functions, and the case
        where a deferred function fails (since the first file doesn't exist on the retry).
        """
        self.options.retryCount = 1
        workdir = self._createTempDir(purpose='nonLocalDir')
        nonLocalFile1 = os.path.join(workdir, str(uuid4()))
        nonLocalFile2 = os.path.join(workdir, str(uuid4()))
        open(nonLocalFile1, 'w').close()
        open(nonLocalFile2, 'w').close()
        assert os.path.exists(nonLocalFile1)
        assert os.path.exists(nonLocalFile2)
        A = Job.wrapJobFn(_deferredFunctionRunsWithFailuresFn,
                          files=(nonLocalFile1, nonLocalFile2))
        Job.Runner.startToil(A, self.options)
        assert not os.path.exists(nonLocalFile1)
        assert not os.path.exists(nonLocalFile2)

    @slow
    def testNewJobsCanHandleOtherJobDeaths(self):
        """
        Create 2 non-local files and then create 2 jobs. The first job registers a deferred job
        to delete the second non-local file, deletes the first non-local file and then kills
        itself.  The second job waits for the first file to be deleted, then sleeps for a few
        seconds and then spawns a child. the child of the second does nothing. However starting
        it should handle the untimely demise of the first job and run the registered deferred
        function that deletes the first file.  We assert the absence of the two files at the
        end of the run.
        """
        # There can be no retries
        self.options.retryCount = 0
        workdir = self._createTempDir(purpose='nonLocalDir')
        nonLocalFile1 = os.path.join(workdir, str(uuid4()))
        nonLocalFile2 = os.path.join(workdir, str(uuid4()))
        open(nonLocalFile1, 'w').close()
        open(nonLocalFile2, 'w').close()
        assert os.path.exists(nonLocalFile1)
        assert os.path.exists(nonLocalFile2)
        files = [nonLocalFile1, nonLocalFile2]
        root = Job()
        A = Job.wrapJobFn(_testNewJobsCanHandleOtherJobDeaths_A, files=files)
        B = Job.wrapJobFn(_testNewJobsCanHandleOtherJobDeaths_B, files=files)
        C = Job.wrapJobFn(_testNewJobsCanHandleOtherJobDeaths_C, files=files,
                          expectedResult=False)
        root.addChild(A)
        root.addChild(B)
        B.addChild(C)
        try:
            Job.Runner.startToil(root, self.options)
        except FailedJobsException as e:
            pass

    @travis_test
    def testBatchSystemCleanupCanHandleWorkerDeaths(self):
        """
        Create a non-local files. Create a job that registers a deferred job to delete the file
        and then kills itself.

        Assert that the file is missing after the pipeline fails.
        """

        # There can be no retries
        self.options.retryCount = 0
        workdir = self._createTempDir(purpose='nonLocalDir')
        nonLocalFile1 = os.path.join(workdir, str(uuid4()))
        nonLocalFile2 = os.path.join(workdir, str(uuid4()))
        # The first file has to be non zero or meseeks will go into an infinite sleep
        file1 = open(nonLocalFile1, 'w')
        file1.write('test')
        file1.close()
        open(nonLocalFile2, 'w').close()
        assert os.path.exists(nonLocalFile1)
        assert os.path.exists(nonLocalFile2)
        A = Job.wrapJobFn(_testNewJobsCanHandleOtherJobDeaths_A,
                          files=(nonLocalFile1, nonLocalFile2))
        try:
            Job.Runner.startToil(A, self.options)
        except FailedJobsException:
            pass
        assert not os.path.exists(nonLocalFile1)
        assert not os.path.exists(nonLocalFile2)

def _writeNonLocalFilesMethod(job, files):
    """
    Write some data to 2 files.  Pass them to a registered deferred method.

    :param tuple files: the tuple of the two files to work with
    :return: None
    """
    for nlf in files:
        with open(nlf, 'wb') as nonLocalFileHandle:
            nonLocalFileHandle.write(os.urandom(1 * 1024 * 1024))
    job.defer(_deleteMethods._deleteFileMethod, files[0], nlf=files[1])
    return None

def _writeNonLocalFilesClassMethod(job, files):
    """
    Write some data to 2 files.  Pass them to a registered deferred class method.

    :param tuple files: the tuple of the two files to work with
    :return: None
    """
    for nlf in files:
        with open(nlf, 'wb') as nonLocalFileHandle:
            nonLocalFileHandle.write(os.urandom(1 * 1024 * 1024))
    job.defer(_deleteMethods._deleteFileClassMethod, files[0], nlf=files[1])
    return None

def _writeNonLocalFilesLambda(job, files):
    """
    Write some data to 2 files.  Pass them to a registered deferred Lambda.

    :param tuple files: the tuple of the two files to work with
    :return: None
    """
    lmd = lambda x, nlf: [os.remove(x), os.remove(nlf)]
    for nlf in files:
        with open(nlf, 'wb') as nonLocalFileHandle:
            nonLocalFileHandle.write(os.urandom(1 * 1024 * 1024))
    job.defer(lmd, files[0], nlf=files[1])
    return None

def _deferredFunctionRunsWithFailuresFn(job, files):
    """
    Refer testDeferredFunctionRunsWithFailures

    :param tuple files: the tuple of the two files to work with
    :return: None
    """
    job.defer(_deleteFile, files[0])
    if os.path.exists(files[0]):
        os.kill(os.getpid(), signal.SIGKILL)
    else:
        assert os.path.exists(files[1])
        job.defer(_deleteFile, files[1])

def _deleteFile(nonLocalFile, nlf=None):
    """
    Delete nonLocalFile and nlf
    :param str nonLocalFile:
    :param str nlf:
    :return: None
    """
    os.remove(nonLocalFile)
    if nlf is not None:
        os.remove(nlf)

def _testNewJobsCanHandleOtherJobDeaths_A(job, files):
    """
    Defer deletion of files[1], then wait for _testNewJobsCanHandleOtherJobDeaths_B to
    start up, and finally delete files[0] before sigkilling self.

    :param tuple files: the tuple of the two files to work with
    :return: None
    """
    # Write the pid to files[1] such that we can be sure that this process has died before
    # we spawn the next job that will do the cleanup.
    with open(files[1], 'w') as fileHandle:
        fileHandle.write(str(os.getpid()))
    job.defer(_deleteFile, files[1])
    while os.stat(files[0]).st_size == 0:
        time.sleep(0.5)
    os.remove(files[0])
    os.kill(os.getpid(), signal.SIGKILL)

def _testNewJobsCanHandleOtherJobDeaths_B(job, files):
    # Write something to files[0] such that we can be sure that this process has started
    # before _testNewJobsCanHandleOtherJobDeaths_A kills itself.
    with open(files[0], 'w') as fileHandle:
        fileHandle.write(str(os.getpid()))
    while os.path.exists(files[0]):
        time.sleep(0.5)
    # Get the pid of _testNewJobsCanHandleOtherJobDeaths_A and wait for it to truly be dead.
    with open(files[1], 'r') as fileHandle:
        meeseeksPID = int(fileHandle.read())
    while CachingFileStore._pidExists(meeseeksPID):
        time.sleep(0.5)
    # Now that we are convinced that_testNewJobsCanHandleOtherJobDeaths_A has died, we can
    # spawn the next job
    return None

def _testNewJobsCanHandleOtherJobDeaths_C(job, files, expectedResult):
    """
    Asserts whether the files exist or not.

    :param Job job: Job
    :param list files: list of files to test
    :param bool expectedResult: Are we expecting the files to exist or not?
    """
    for testFile in files:
        assert os.path.exists(testFile) is expectedResult


class _deleteMethods(object):
    @staticmethod
    def _deleteFileMethod(nonLocalFile, nlf=None):
        """
        Delete nonLocalFile and nlf

        :return: None
        """
        os.remove(nonLocalFile)
        if nlf is not None:
            os.remove(nlf)

    @classmethod
    def _deleteFileClassMethod(cls, nonLocalFile, nlf=None):
        """
        Delete nonLocalFile and nlf

        :return: None
        """
        os.remove(nonLocalFile)
        if nlf is not None:
            os.remove(nlf)

