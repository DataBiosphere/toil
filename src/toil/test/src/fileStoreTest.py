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

import filecmp
from abc import abstractmethod, ABCMeta
from struct import pack, unpack
from uuid import uuid4

from toil.job import Job
from toil.fileStore import IllegalDeletionCacheError, CachingFileStore
from toil.test import ToilTest, needs_aws, needs_azure, needs_google, experimental
from toil.leader import FailedJobsException
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.fileStore import CacheUnbalancedError

import collections
import inspect
import os
import random
import signal
import time
import unittest

# Python 3 compatibility imports
from six.moves import xrange

# Some tests take too long on the AWS and Azure Job stores and are unquitable for CI.  They can be
# be run during manual tests by setting this to False.
testingIsAutomatic = True


class hidden:
    """
    Hiding the abstract test classes from the Unittest loader so it can be inherited in different
    test suites for the different job stores.
    """
    class AbstractFileStoreTest(ToilTest):
        """
        An abstract base class for testing the various general functions described in
        :class:toil.fileStore.FileStore
        """
        # This is overwritten in the inheriting classs
        jobStoreType = None
        __metaclass__ = ABCMeta

        def _getTestJobStore(self):
            if self.jobStoreType == 'file':
                return self._getTestJobStorePath()
            elif self.jobStoreType == 'aws':
                return 'aws:%s:cache-tests-%s' % (self.awsRegion(), uuid4())
            elif self.jobStoreType == 'azure':
                return 'azure:toiltest:cache-tests-' + str(uuid4())
            elif self.jobStoreType == 'google':
                projectID = 'cgc-05-0006'
                return 'google:' + projectID + ':cache-tests-' + str(uuid4())
            else:
                raise RuntimeError('Illegal job store type.')

        def setUp(self):
            super(hidden.AbstractFileStoreTest, self).setUp()
            testDir = self._createTempDir()
            self.options = Job.Runner.getDefaultOptions(self._getTestJobStore())
            self.options.logLevel = 'INFO'
            self.options.workDir = testDir
            self.options.clean = 'always'
            self.options.logFile = os.path.join(testDir, 'logFile')

        @staticmethod
        def _uselessFunc(job):
            """
            I do nothing.  Don't judge me.
            """
            return None

        # Sanity test
        def testToilIsNotBroken(self):
            """
            Runs a simple DAG to test if if any features other that caching were broken.
            """
            A = Job.wrapJobFn(self._uselessFunc)
            B = Job.wrapJobFn(self._uselessFunc)
            C = Job.wrapJobFn(self._uselessFunc)
            D = Job.wrapJobFn(self._uselessFunc)
            A.addChild(B)
            A.addChild(C)
            B.addChild(D)
            C.addChild(D)
            Job.Runner.startToil(A, self.options)

        # Test filestore operations.  This is a slightly less intense version of the cache specific
        # test `testReturnFileSizes`
        def testFileStoreOperations(self):
            """
            Write a couple of files to the jobstore.  Delete a couple of them.  Read back written
            and locally deleted files.
            """
            workdir = self._createTempDir(purpose='nonLocalDir')
            F = Job.wrapJobFn(self._testFileStoreOperations,
                              nonLocalDir=workdir,
                              numIters=30, disk='2G')
            Job.Runner.startToil(F, self.options)

        @staticmethod
        def _testFileStoreOperations(job, nonLocalDir, numIters=100):
            """
            Aux function for testFileStoreOperations Conduct numIters operations.
            """
            work_dir = job.fileStore.getLocalTempDir()
            writtenFiles = {}  # fsID: (size, isLocal)
            localFileIDs = set()
            # Add one file for the sake of having something in the job store
            writeFileSize = random.randint(0, 30)
            cls = hidden.AbstractNonCachingFileStoreTest
            fsId, _ = cls._writeFileToJobStore(job, isLocalFile=True, nonLocalDir=nonLocalDir,
                                               fileMB=writeFileSize)
            writtenFiles[fsId] = writeFileSize
            localFileIDs.add(writtenFiles.keys()[0])
            i = 0
            while i <= numIters:
                randVal = random.random()
                if randVal < 0.33:  # Write
                    writeFileSize = random.randint(0, 30)
                    isLocalFile = True if random.random() <= 0.5 else False
                    fsID, _ = cls._writeFileToJobStore(job, isLocalFile=isLocalFile,
                                                       nonLocalDir=nonLocalDir,
                                                       fileMB=writeFileSize)
                    writtenFiles[fsID] = writeFileSize
                    localFileIDs.add(fsID)
                else:
                    if len(writtenFiles) == 0:
                        continue
                    else:
                        fsID, rdelFileSize = random.choice(writtenFiles.items())
                        rdelRandVal = random.random()
                    if randVal < 0.66:  # Read
                        mutable = True if random.random() <= 0.5 else False
                        cache = True if random.random() <= 0.5 else False
                        job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, str(uuid4())]),
                                                     cache=cache, mutable=mutable)
                        localFileIDs.add(fsID)
                    else:  # Delete
                        if rdelRandVal <= 0.5:  # Local Delete
                            if fsID not in localFileIDs:
                                continue
                            job.fileStore.deleteLocalFile(fsID)
                        else:  # Global Delete
                            job.fileStore.deleteGlobalFile(fsID)
                            writtenFiles.pop(fsID)
                        if fsID in localFileIDs:
                            localFileIDs.remove(fsID)
                i += 1

        # Tests for the various defer possibilities
        def testDeferredFunctionRunsWithMethod(self):
            """
            Refer docstring in _testDeferredFunctionRuns.
            Test with Method
            """
            self._testDeferredFunctionRuns(self._writeNonLocalFilesMethod)

        def testDeferredFunctionRunsWithClassMethod(self):
            """
            Refer docstring in _testDeferredFunctionRuns.
            Test with Class Method
            """
            self._testDeferredFunctionRuns(self._writeNonLocalFilesClassMethod)

        def testDeferredFunctionRunsWithLambda(self):
            """
            Refer docstring in _testDeferredFunctionRuns.
            Test with Lambda
            """
            self._testDeferredFunctionRuns(self._writeNonLocalFilesLambda)

        def _testDeferredFunctionRuns(self, callableFn):
            """
            Create 2 files. Make a job that writes data to them. Register a deferred function that
            deletes the two files (one passed as an arg, adn one as a kwarg) and later assert that
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

        @staticmethod
        def _writeNonLocalFilesMethod(job, files):
            """
            Write some data to 2 files.  Pass them to a registered deferred method.

            :param tuple files: the tuple of the two files to work with
            :return: None
            """
            for nlf in files:
                with open(nlf, 'w') as nonLocalFileHandle:
                    nonLocalFileHandle.write(os.urandom(1 * 1024 * 1024))
            job.defer(_deleteMethods._deleteFileMethod, files[0], nlf=files[1])
            return None

        @staticmethod
        def _writeNonLocalFilesClassMethod(job, files):
            """
            Write some data to 2 files.  Pass them to a registered deferred class method.

            :param tuple files: the tuple of the two files to work with
            :return: None
            """
            for nlf in files:
                with open(nlf, 'w') as nonLocalFileHandle:
                    nonLocalFileHandle.write(os.urandom(1 * 1024 * 1024))
            job.defer(_deleteMethods._deleteFileClassMethod, files[0], nlf=files[1])
            return None

        @staticmethod
        def _writeNonLocalFilesLambda(job, files):
            """
            Write some data to 2 files.  Pass them to a registered deferred Lambda.

            :param tuple files: the tuple of the two files to work with
            :return: None
            """
            lmd = lambda x, nlf: [os.remove(x), os.remove(nlf)]
            for nlf in files:
                with open(nlf, 'w') as nonLocalFileHandle:
                    nonLocalFileHandle.write(os.urandom(1 * 1024 * 1024))
            job.defer(lmd, files[0], nlf=files[1])
            return None

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
            A = Job.wrapJobFn(self._deferredFunctionRunsWithFailuresFn,
                              files=(nonLocalFile1, nonLocalFile2))
            Job.Runner.startToil(A, self.options)
            assert not os.path.exists(nonLocalFile1)
            assert not os.path.exists(nonLocalFile2)

        @staticmethod
        def _deferredFunctionRunsWithFailuresFn(job, files):
            """
            Refer testDeferredFunctionRunsWithFailures

            :param tuple files: the tuple of the two files to work with
            :return: None
            """
            cls = hidden.AbstractNonCachingFileStoreTest
            job.defer(cls._deleteFile, files[0])
            if os.path.exists(files[0]):
                os.kill(os.getpid(), signal.SIGKILL)
            else:
                assert os.path.exists(files[1])
                job.defer(cls._deleteFile, files[1])

        @staticmethod
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
            A = Job.wrapJobFn(self._testNewJobsCanHandleOtherJobDeaths_A, files=files)
            B = Job.wrapJobFn(self._testNewJobsCanHandleOtherJobDeaths_B, files=files)
            C = Job.wrapJobFn(self._testNewJobsCanHandleOtherJobDeaths_C, files=files,
                              expectedResult=False)
            root.addChild(A)
            root.addChild(B)
            B.addChild(C)
            try:
                Job.Runner.startToil(root, self.options)
            except FailedJobsException as e:
                pass

        @staticmethod
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
            job.defer(hidden.AbstractNonCachingFileStoreTest._deleteFile, files[1])
            while os.stat(files[0]).st_size == 0:
                time.sleep(0.5)
            os.remove(files[0])
            os.kill(os.getpid(), signal.SIGKILL)

        @staticmethod
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

        @staticmethod
        def _testNewJobsCanHandleOtherJobDeaths_C(job, files, expectedResult):
            """
            Asserts whether the files exist or not.

            :param Job job: Job
            :param list files: list of files to test
            :param bool expectedResult: Are we expecting the files to exist or not?
            """
            for testFile in files:
                assert os.path.exists(testFile) is expectedResult

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
            A = Job.wrapJobFn(self._testNewJobsCanHandleOtherJobDeaths_A,
                              files=(nonLocalFile1, nonLocalFile2))
            try:
                Job.Runner.startToil(A, self.options)
            except FailedJobsException:
                pass
            assert not os.path.exists(nonLocalFile1)
            assert not os.path.exists(nonLocalFile2)

        @staticmethod
        def _writeFileToJobStore(job, isLocalFile, nonLocalDir=None, fileMB=1):
            """
            This function creates a file and writes it to the jobstore.

            :param bool isLocalFile: Is the file local(T) or Non-Local(F)?
            :param str nonLocalDir: A dir to write the file to.  If unspecified, a local directory
                                    is created.
            :param int fileMB: Size of the created file in MB
            """
            if isLocalFile:
                work_dir = job.fileStore.getLocalTempDir()
            else:
                assert nonLocalDir is not None
                work_dir = nonLocalDir
            with open(os.path.join(work_dir, str(uuid4())), 'w') as testFile:
                testFile.write(os.urandom(fileMB * 1024 * 1024))

            return job.fileStore.writeGlobalFile(testFile.name), testFile

    class AbstractNonCachingFileStoreTest(AbstractFileStoreTest):
        """
        Abstract tests for the the various functions in :class:toil.fileStore.NonCachingFileStore.
        These tests are general enough that they can also be used for
        :class:toil.fileStore.CachingFileStore.
        """
        __metaclass__ = ABCMeta

        def setUp(self):
            super(hidden.AbstractNonCachingFileStoreTest, self).setUp()
            self.options.disableCaching = True

    class AbstractCachingFileStoreTest(AbstractFileStoreTest):
        """
        Abstract tests for the the various cache-related functions in
        :class:toil.fileStore.CachingFileStore.
        """
        __metaclass__ = ABCMeta

        def setUp(self):
            super(hidden.AbstractCachingFileStoreTest, self).setUp()
            self.options.disableCaching = False

        def testExtremeCacheSetup(self):
            """
            Try to create the cache with bad worker active and then have 10 child jobs try to run in
            the chain.  This tests whether the cache is created properly even when the job crashes
            randomly.
            """
            if testingIsAutomatic and self.jobStoreType != 'file':
                self.skipTest("To save time")
            self.options.retryCount = 20
            self.options.badWorker = 0.5
            self.options.badWorkerFailInterval = 0.1
            for test in xrange(0, 20):
                E = Job.wrapJobFn(self._uselessFunc)
                F = Job.wrapJobFn(self._uselessFunc)
                jobs = {}
                for i in xrange(0, 10):
                    jobs[i] = Job.wrapJobFn(self._uselessFunc)
                    E.addChild(jobs[i])
                    jobs[i].addChild(F)
                Job.Runner.startToil(E, self.options)

        def testCacheLockRace(self):
            """
            Make 3 jobs compete for the same cache lock file.  If they have the lock at the same
            time, the test will fail.  This test abuses the _CacheState class and modifies values in
            the lock file.  DON'T TRY THIS AT HOME.
            """
            A = Job.wrapJobFn(self._setUpLockFile)
            B = Job.wrapJobFn(self._selfishLocker, cores=1)
            C = Job.wrapJobFn(self._selfishLocker, cores=1)
            D = Job.wrapJobFn(self._selfishLocker, cores=1)
            E = Job.wrapJobFn(self._raceTestSuccess)
            A.addChild(B)
            A.addChild(C)
            A.addChild(D)
            B.addChild(E)
            C.addChild(E)
            D.addChild(E)
            Job.Runner.startToil(A, self.options)

        @staticmethod
        def _setUpLockFile(job):
            """
            Set nlink=0 for the cache test
            """
            with job.fileStore.cacheLock():
                cacheInfo = job.fileStore._CacheState._load(job.fileStore.cacheStateFile)
                cacheInfo.nlink = 0
                cacheInfo.write(job.fileStore.cacheStateFile)

        @staticmethod
        def _selfishLocker(job):
            """
            Try to acquire a lock on the lock file.  If 2 threads have the lock concurrently, then
            abort.
            """
            for i in xrange(0, 1000):
                with job.fileStore.cacheLock():
                    cacheInfo = job.fileStore._CacheState._load(job.fileStore.cacheStateFile)
                    cacheInfo.nlink += 1
                    cacheInfo.cached = max(cacheInfo.nlink, cacheInfo.cached)
                    cacheInfo.write(job.fileStore.cacheStateFile)
                time.sleep(0.001)
                with job.fileStore.cacheLock():
                    cacheInfo = job.fileStore._CacheState._load(job.fileStore.cacheStateFile)
                    cacheInfo.nlink -= 1
                    cacheInfo.write(job.fileStore.cacheStateFile)

        @staticmethod
        def _raceTestSuccess(job):
            """
            Assert that the cache test passed successfully.
            """
            with job.fileStore.cacheLock():
                cacheInfo = job.fileStore._CacheState._load(job.fileStore.cacheStateFile)
                # Value of the nlink has to be zero for successful run
                assert cacheInfo.nlink == 0
                assert cacheInfo.cached > 1

        def testCacheEvictionPartialEvict(self):
            """
            Ensure the cache eviction happens as expected.  Two files (20MB and 30MB) are written
            sequentially into the job store in separate jobs.  The cache max is force set to 50MB.
            A Third Job requests 10MB of disk requiring eviction of the 1st file.  Ensure that the
            behavior is as expected.
            """
            self._testValidityOfCacheEvictTest()

            # Explicitly set clean to always so even the failed cases get cleaned (This will
            # overwrite the value set in setUp if it is ever changed in the future)
            self.options.clean = 'always'

            self._testCacheEviction(file1MB=20, file2MB=30, diskRequestMB=10)

        def testCacheEvictionTotalEvict(self):
            """
            Ensure the cache eviction happens as expected.  Two files (20MB and 30MB) are written
            sequentially into the job store in separate jobs.  The cache max is force set to 50MB.
            A Third Job requests 10MB of disk requiring eviction of the 1st file.  Ensure that the
            behavior is as expected.
            """
            self._testValidityOfCacheEvictTest()

            # Explicitly set clean to always so even the failed cases get cleaned (This will
            # overwrite the value set in setUp if it is ever changed in the future)
            self.options.clean = 'always'

            self._testCacheEviction(file1MB=20, file2MB=30, diskRequestMB=30)

        def testCacheEvictionFailCase(self):
            """
            Ensure the cache eviction happens as expected.  Two files (20MB and 30MB) are written
            sequentially into the job store in separate jobs.  The cache max is force set to 50MB.
            A Third Job requests 10MB of disk requiring eviction of the 1st file.  Ensure that the
            behavior is as expected.
            """
            self._testValidityOfCacheEvictTest()

            # Explicitly set clean to always so even the failed cases get cleaned (This will
            # overwrite the value set in setUp if it is ever changed in the future)
            self.options.clean = 'always'

            self._testCacheEviction(file1MB=20, file2MB=30, diskRequestMB=60)

        def _testValidityOfCacheEvictTest(self):
            # If the job store and cache are on the same file system, file sizes are accounted for
            # by the job store and are not reflected in the cache hence this test is redundant.
            if not self.options.jobStore.startswith(('aws', 'azure', 'google')):
                workDirDev = os.stat(self.options.workDir).st_dev
                jobStoreDev = os.stat(os.path.dirname(self.options.jobStore)).st_dev
                if workDirDev == jobStoreDev:
                    self.skipTest('Job store and working directory are on the same filesystem.')

        def _testCacheEviction(self, file1MB, file2MB, diskRequestMB):
            """
            Ensure the cache eviction happens as expected.  Two files (20MB and 30MB) are written
            sequentially into the job store in separate jobs.  The cache max is force set to 50MB.
            A Third Job requests either 10, 30 or 60MB -- requiring eviction of 1 file, both files,
            or results in an error due to lack of space, respectively.  Ensure that the behavior is
            as expected.
            """
            self.options.retryCount = 0
            if diskRequestMB > 50:
                # This can be non int as it will never reach _probeJobReqs
                expectedResult = 'Fail'
            else:
                expectedResult = 50 - file1MB if diskRequestMB <= file1MB else 0
            try:
                A = Job.wrapJobFn(self._writeFileToJobStoreWithAsserts, isLocalFile=True,
                                  fileMB=file1MB)
                # Sleep for 1 second after writing the first file so that their ctimes are
                # guaranteed to be distinct for the purpose of this test.
                B = Job.wrapJobFn(self._sleepy, timeToSleep=1)
                C = Job.wrapJobFn(self._writeFileToJobStoreWithAsserts, isLocalFile=True,
                                  fileMB=file2MB)
                D = Job.wrapJobFn(self._forceModifyCacheLockFile, newTotalMB=50, disk='0M')
                E = Job.wrapJobFn(self._uselessFunc, disk=''.join([str(diskRequestMB), 'M']))
                # Set it to > 2GB such that the cleanup jobs don't die in the non-fail cases
                F = Job.wrapJobFn(self._forceModifyCacheLockFile, newTotalMB=5000, disk='10M')
                G = Job.wrapJobFn(self._probeJobReqs, sigmaJob=100, cached=expectedResult,
                                  disk='100M')
                A.addChild(B)
                B.addChild(C)
                C.addChild(D)
                D.addChild(E)
                E.addChild(F)
                F.addChild(G)
                Job.Runner.startToil(A, self.options)
            except FailedJobsException as err:
                self.assertEqual(err.numberOfFailedJobs, 1)
                with open(self.options.logFile) as f:
                    logContents = f.read()
                if CacheUnbalancedError.message in logContents:
                    self.assertEqual(expectedResult, 'Fail')
                else:
                    self.fail('Toil did not raise the expected AssertionError')

        @staticmethod
        def _writeFileToJobStoreWithAsserts(job, isLocalFile, nonLocalDir=None, fileMB=1):
            """
            This function creates a file and writes it to the jobstore.

            :param bool isLocalFile: Is the file local(T) or Non-Local(F)?
            :param str nonLocalDir: A dir to write the file to.  If unspecified, a local directory
                                    is created.
            :param int fileMB: Size of the created file in MB
            """
            cls = hidden.AbstractNonCachingFileStoreTest
            fsID, testFile = cls._writeFileToJobStore(job, isLocalFile, nonLocalDir, fileMB)
            actual = os.stat(testFile.name).st_nlink
            if isLocalFile:
                # Since the file has been hard linked it should have nlink_count = threshold + 1
                # (local, cached, and possibly job store).
                expected = job.fileStore.nlinkThreshold + 1
                assert actual == expected, 'Should have %i nlinks. Got %i' % (expected, actual)
            else:
                # Since the file hasn't been hard linked it should have nlink_count = 1
                assert actual == 1, 'Should have one nlink. Got %i.' % actual
            return fsID

        @staticmethod
        def _sleepy(job, timeToSleep):
            """
            I'm waiting for prince charming... but only for timeToSleep seconds.

            :param int timeToSleep: Time in seconds
            """
            time.sleep(timeToSleep)

        @staticmethod
        def _forceModifyCacheLockFile(job, newTotalMB):
            """
            This function opens and modifies the cache lock file to reflect a new "total"
            value = newTotalMB and thereby fooling the cache logic into believing only newTotalMB is
            allowed for the run.

            :param int newTotalMB: New value for "total" in the cacheLockFile
            """
            with job.fileStore.cacheLock() as _:
                cacheInfo = job.fileStore._CacheState._load(job.fileStore.cacheStateFile)
                cacheInfo.total = float(newTotalMB * 1024 * 1024)
                cacheInfo.write(job.fileStore.cacheStateFile)

        @staticmethod
        def _probeJobReqs(job, total=None, cached=None, sigmaJob=None):
            """
            Probes the cacheLockFile to ensure the values for total, disk and cache are as expected.
            Can also specify combinations of the requirements if desired.

            :param int total: Expected Total Space available for caching in MB.
            :param int cached: Expected Total size of files in the cache in MB.
            :param int sigmaJob: Expected sum of job requirements in MB.
            """
            valueDict = locals()
            assert (total or cached or sigmaJob)
            with job.fileStore.cacheLock() as x:
                cacheInfo = job.fileStore._CacheState._load(job.fileStore.cacheStateFile)
                for value in ('total', 'cached', 'sigmaJob'):
                    # If the value wasn't provided, it is None and should be ignored
                    if valueDict[value] is None:
                        continue
                    expectedMB = valueDict[value] * 1024 * 1024
                    cacheInfoMB = getattr(cacheInfo, value)
                    assert cacheInfoMB == expectedMB, 'Testing %s: Expected ' % value + \
                                                      '%s but got %s.' % (expectedMB, cacheInfoMB)

        def testAsyncWriteWithCaching(self):
            """
            Ensure the Async Writing of files happens as expected.  The first Job forcefully
            modifies the cache lock file to 1GB. The second asks for 1GB of disk and  writes a 900MB
            file into cache then rewrites it to the job store triggering an async write since the
            two unique jobstore IDs point to the same local file.  Also, the second write is not
            cached since the first was written to cache, and there "isn't enough space" to cache the
            second.  Imediately assert that the second write isn't cached, and is being
            asynchronously written to the job store (through the presence of a harbinger file).
             Attempting to get the file from the jobstore should not fail.
            """
            self.options.retryCount = 0
            self.options.logLevel = 'DEBUG'
            A = Job.wrapJobFn(self._forceModifyCacheLockFile, newTotalMB=1024, disk='1G')
            B = Job.wrapJobFn(self._doubleWriteFileToJobStore, fileMB=850, disk='900M')
            # Set it to > 2GB such that the cleanup jobs don't die.
            C = Job.wrapJobFn(self._readFromJobStoreWithoutAsssertions, fsID=B.rv(), disk='1G')
            D = Job.wrapJobFn(self._forceModifyCacheLockFile, newTotalMB=5000, disk='1G')
            A.addChild(B)
            B.addChild(C)
            C.addChild(D)
            Job.Runner.startToil(A, self.options)

        @staticmethod
        def _doubleWriteFileToJobStore(job, fileMB):
            """
            Write a local file to job store, then write it again.  The second should trigger an
            async write.

            :param job: job
            :param fileMB: File Size
            :return: Job store file ID for second written file
            """
            # Make this take longer so we can test asynchronous writes across jobs/workers.
            oldHarbingerFileRead = job.fileStore.HarbingerFile.read
            def newHarbingerFileRead(self):
                time.sleep(5)
                return oldHarbingerFileRead(self)

            job.fileStore.logToMaster('Double writing a file into job store')
            work_dir = job.fileStore.getLocalTempDir()
            with open(os.path.join(work_dir, str(uuid4())), 'w') as testFile:
                testFile.write(os.urandom(fileMB * 1024 * 1024))

            job.fileStore.writeGlobalFile(testFile.name)
            fsID = job.fileStore.writeGlobalFile(testFile.name)
            hidden.AbstractCachingFileStoreTest._readFromJobStoreWithoutAsssertions(job, fsID)
            # Make this take longer so we can test asynchronous writes across jobs/workers.
            job.fileStore.HarbingerFile.read = newHarbingerFileRead
            return job.fileStore.writeGlobalFile(testFile.name)

        @staticmethod
        def _readFromJobStoreWithoutAsssertions(job, fsID):
            """
            Reads a file from the job store.  That will be all, thank you.

            :param job: job
            :param fsID: Job store file ID for the read file
            :return: None
            """
            job.fileStore.logToMaster('Reading the written file')
            assert not job.fileStore._fileIsCached(fsID)
            assert job.fileStore.HarbingerFile(job.fileStore, fileStoreID=fsID).exists()
            job.fileStore.readGlobalFile(fsID)

        # writeGlobalFile tests
        def testWriteNonLocalFileToJobStore(self):
            """
            Write a file not in localTempDir to the job store.  Such a file should not be cached.
            Ensure the file is not cached.
            """
            workdir = self._createTempDir(purpose='nonLocalDir')
            A = Job.wrapJobFn(self._writeFileToJobStoreWithAsserts, isLocalFile=False,
                              nonLocalDir=workdir)
            Job.Runner.startToil(A, self.options)

        def testWriteLocalFileToJobStore(self):
            """
            Write a file from the localTempDir to the job store.  Such a file will be cached by
            default.  Ensure the file is cached.
            """
            A = Job.wrapJobFn(self._writeFileToJobStoreWithAsserts, isLocalFile=True)
            Job.Runner.startToil(A, self.options)

        # readGlobalFile tests
        def testReadCacheMissFileFromJobStoreWithoutCachingReadFile(self):
            """
            Read a file from the file store that does not have a corresponding cached copy.  Do not
            cache the read file.  Ensure the number of links on the file are appropriate.
            """
            self._testCacheMissFunction(cacheReadFile=False)

        def testReadCacheMissFileFromJobStoreWithCachingReadFile(self):
            """
            Read a file from the file store that does not have a corresponding cached copy.  Cache
            the read file.  Ensure the number of links on the file are appropriate.
            """
            self._testCacheMissFunction(cacheReadFile=True)

        def _testCacheMissFunction(self, cacheReadFile):
            """
            This is the function that actually does what the 2 cache miss functions want.

            :param cacheReadFile: Does the read file need to be cached(T) or not(F)
            """
            workdir = self._createTempDir(purpose='nonLocalDir')
            A = Job.wrapJobFn(self._writeFileToJobStoreWithAsserts, isLocalFile=False,
                              nonLocalDir=workdir)
            B = Job.wrapJobFn(self._readFromJobStore, isCachedFile=False,
                              cacheReadFile=cacheReadFile, fsID=A.rv())
            A.addChild(B)
            Job.Runner.startToil(A, self.options)

        @staticmethod
        def _readFromJobStore(job, isCachedFile, cacheReadFile, fsID, isTest=True):
            """
            Read a file from the filestore.  If the file was cached, ensure it was hard linked
            correctly.  If it wasn't, ensure it was put into cache.

            :param bool isCachedFile: Flag.  Was the read file read from cache(T)? This defines the
             nlink count to be asserted.
            :param bool cacheReadFile: Should the the file that is read be cached(T)?
            :param str fsID: job store file ID
            :param bool isTest: Is this being run as a test(T) or an accessory to another test(F)?

            """
            work_dir = job.fileStore.getLocalTempDir()
            x = job.fileStore.nlinkThreshold
            if isCachedFile:
                outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']),
                                                       mutable=False)
                expected = x + 1
            else:
                if cacheReadFile:
                    outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']),
                                                           cache=True, mutable=False)
                    expected = x + 1
                else:
                    outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']),
                                                           cache=False, mutable=False)
                    expected = x
            if isTest:
                actual = os.stat(outfile).st_nlink
                assert actual == expected, 'Should have %i nlinks. Got %i.' % (expected, actual)
                return None
            else:
                return outfile

        def testReadCachHitFileFromJobStore(self):
            """
            Read a file from the file store that has a corresponding cached copy.  Ensure the number
            of links on the file are appropriate.
            """
            A = Job.wrapJobFn(self._writeFileToJobStoreWithAsserts, isLocalFile=True)
            B = Job.wrapJobFn(self._readFromJobStore, isCachedFile=True, cacheReadFile=None,
                              fsID=A.rv())
            A.addChild(B)
            Job.Runner.startToil(A, self.options)

        def testMultipleJobsReadSameCacheHitGlobalFile(self):
            """
            Write a local file to the job store (hence adding a copy to cache), then have 10 jobs
            read it.  Assert cached file size in the cache lock file never goes up, assert sigma job
            reqs is always
                   (a multiple of job reqs) - (number of files linked to the cachedfile * filesize).
            At the end, assert the cache lock file shows sigma job = 0.
            """
            self._testMultipleJobsReadGlobalFileFunction(cacheHit=True)

        def testMultipleJobsReadSameCacheMissGlobalFile(self):
            """
            Write a non-local file to the job store(hence no cached copy), then have 10 jobs read
            it. Assert cached file size in the cache lock file never goes up, assert sigma job reqs
            is always
                   (a multiple of job reqs) - (number of files linked to the cachedfile * filesize).
            At the end, assert the cache lock file shows sigma job = 0.
            """
            self._testMultipleJobsReadGlobalFileFunction(cacheHit=False)

        def _testMultipleJobsReadGlobalFileFunction(self, cacheHit):
            """
            This function does what the two Multiple File reading tests want to do

            :param bool cacheHit: Is the test for the CacheHit case(T) or cacheMiss case(F)
            """
            dirPurpose = 'tempWriteDir' if cacheHit else 'nonLocalDir'
            workdir = self._createTempDir(purpose=dirPurpose)
            with open(os.path.join(workdir, 'test'), 'w') as x:
                x.write(str(0))
            A = Job.wrapJobFn(self._writeFileToJobStoreWithAsserts, isLocalFile=cacheHit,
                              nonLocalDir=workdir,
                              fileMB=256)
            B = Job.wrapJobFn(self._probeJobReqs, sigmaJob=100, disk='100M')
            jobs = {}
            for i in xrange(0, 10):
                jobs[i] = Job.wrapJobFn(self._multipleFileReader, diskMB=1024, fsID=A.rv(),
                                        maxWriteFile=os.path.abspath(x.name), disk='1G',
                                        memory='10M', cores=1)
                A.addChild(jobs[i])
                jobs[i].addChild(B)
            Job.Runner.startToil(A, self.options)
            with open(x.name, 'r') as y:
                assert int(y.read()) > 2

        @staticmethod
        def _multipleFileReader(job, diskMB, fsID, maxWriteFile):
            """
            Read a file from the job store immutable and explicitly ask to have it in the cache.
            If we are using the File Job Store, assert sum of cached file sizes in the cache lock
            file is zero, else assert it is equal to the read file.
            Also assert the sum job reqs + (number of files linked to the cachedfile * filesize) is
            and integer multiple of the disk requirements provided to this job.

            :param int diskMB: disk requirements provided to the job
            :param str fsID: job store file ID
            :param str maxWriteFile: path to file where the max number of concurrent readers of
                                     cache lock file will be written
            """
            work_dir = job.fileStore.getLocalTempDir()
            outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']), cache=True,
                                                   mutable=False)
            diskMB = diskMB * 1024 * 1024
            with job.fileStore.cacheLock():
                fileStats = os.stat(outfile)
                fileSize = fileStats.st_size
                fileNlinks = fileStats.st_nlink
                with open(maxWriteFile, 'r+') as x:
                    prev_max = int(x.read())
                    x.seek(0)
                    x.truncate()
                    x.write(str(max(prev_max, fileNlinks)))
                cacheInfo = job.fileStore._CacheState._load(job.fileStore.cacheStateFile)
                if cacheInfo.nlink == 2:
                    assert cacheInfo.cached == 0.0  # Since fileJobstore on same filesystem
                else:
                    assert cacheInfo.cached == fileSize
                assert ((cacheInfo.sigmaJob + (fileNlinks - cacheInfo.nlink) * fileSize) %
                        diskMB) == 0.0
            # Sleep so there's no race conditions where a job ends before another can get a hold of
            # the file
            time.sleep(3)

        @staticmethod
        def _writeExportGlobalFile(job):
            fileName = os.path.join(job.fileStore.getLocalTempDir(), 'testfile')
            with open(fileName, 'wb') as f:
                f.write(os.urandom(1024 * 30000)) # 30 Mb
            outputFile = os.path.join(job.fileStore.getLocalTempDir(), 'exportedFile')
            job.fileStore.exportFile(job.fileStore.writeGlobalFile(fileName), 'File://' + outputFile)
            assert filecmp.cmp(fileName, outputFile)

        def testFileStoreExportFile(self):
            # Tests that files written to job store can be immediately exported
            # motivated by https://github.com/BD2KGenomics/toil/issues/1469
            root = Job.wrapJobFn(self._writeExportGlobalFile)
            Job.Runner.startToil(root, self.options)

        # Testing for the return of file sizes to the sigma job pool.
        def testReturnFileSizes(self):
            """
            Write a couple of files to the jobstore.  Delete a couple of them.  Read back written
            and locally deleted files.  Ensure that after every step that the cache state file is
            describing the correct values.
            """
            workdir = self._createTempDir(purpose='nonLocalDir')
            F = Job.wrapJobFn(self._returnFileTestFn,
                              jobDisk=2 * 1024 * 1024 * 1024,
                              initialCachedSize=0,
                              nonLocalDir=workdir,
                              disk='2G')
            Job.Runner.startToil(F, self.options)

        def testReturnFileSizesWithBadWorker(self):
            """
            Write a couple of files to the jobstore.  Delete a couple of them.  Read back written
            and locally deleted files.  Ensure that after every step that the cache state file is
            describing the correct values.
            """
            self.options.retryCount = 20
            self.options.badWorker = 0.5
            self.options.badWorkerFailInterval = 0.1
            workdir = self._createTempDir(purpose='nonLocalDir')
            F = Job.wrapJobFn(self._returnFileTestFn,
                              jobDisk=2 * 1024 * 1024 * 1024,
                              initialCachedSize=0,
                              nonLocalDir=workdir,
                              numIters=30, disk='2G')
            Job.Runner.startToil(F, self.options)

        @staticmethod
        def _returnFileTestFn(job, jobDisk, initialCachedSize, nonLocalDir, numIters=100):
            """
            Aux function for jobCacheTest.testReturnFileSizes Conduct numIters operations and ensure
            the cache state file is tracked appropriately.

            Track the cache calculations even thought they won't be used in filejobstore

            :param float jobDisk: The value of disk passed to this job.
            """
            cached = initialCachedSize
            work_dir = job.fileStore.getLocalTempDir()
            writtenFiles = {}  # fsID: (size, isLocal)
            localFileIDs = collections.defaultdict(list)  # fsid: local/non-local/mutable/immutable
            # Add one file for the sake of having something in the job store
            writeFileSize = random.randint(0, 30)
            jobDisk -= writeFileSize * 1024 * 1024
            cls = hidden.AbstractCachingFileStoreTest
            fsId = cls._writeFileToJobStoreWithAsserts(job, isLocalFile=True, fileMB=writeFileSize)
            writtenFiles[fsId] = writeFileSize
            if job.fileStore._fileIsCached(writtenFiles.keys()[0]):
                cached += writeFileSize * 1024 * 1024
            localFileIDs[writtenFiles.keys()[0]].append('local')
            cls._requirementsConcur(job, jobDisk, cached)
            i = 0
            while i <= numIters:
                randVal = random.random()
                if randVal < 0.33:  # Write
                    writeFileSize = random.randint(0, 30)
                    if random.random() <= 0.5:  # Write a local file
                        fsID = cls._writeFileToJobStoreWithAsserts(job, isLocalFile=True,
                                                                   fileMB=writeFileSize)
                        writtenFiles[fsID] = writeFileSize
                        localFileIDs[fsID].append('local')
                        jobDisk -= writeFileSize * 1024 * 1024
                        if job.fileStore._fileIsCached(fsID):
                            cached += writeFileSize * 1024 * 1024
                    else:  # Write a non-local file
                        fsID = cls._writeFileToJobStoreWithAsserts(job, isLocalFile=False,
                                                                   nonLocalDir=nonLocalDir,
                                                                   fileMB=writeFileSize)
                        writtenFiles[fsID] = writeFileSize
                        localFileIDs[fsID].append('non-local')
                        # No change to the job since there was no caching
                    cls._requirementsConcur(job, jobDisk, cached)
                else:
                    if len(writtenFiles) == 0:
                        continue
                    else:
                        fsID, rdelFileSize = random.choice(writtenFiles.items())
                        rdelRandVal = random.random()
                        fileWasCached = job.fileStore._fileIsCached(fsID)
                    if randVal < 0.66:  # Read
                        if rdelRandVal <= 0.5:  # Read as mutable
                            job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, str(uuid4())]),
                                                         mutable=True)
                            localFileIDs[fsID].append('mutable')
                            # No change because the file wasn't cached
                        else:  # Read as immutable
                            job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, str(uuid4())]),
                                                         mutable=False)
                            localFileIDs[fsID].append('immutable')
                            jobDisk -= rdelFileSize * 1024 * 1024
                        if not fileWasCached:
                            if job.fileStore._fileIsCached(fsID):
                                cached += rdelFileSize * 1024 * 1024
                        cls._requirementsConcur(job, jobDisk, cached)
                    else:  # Delete
                        if rdelRandVal <= 0.5:  # Local Delete
                            if fsID not in localFileIDs.keys():
                                continue
                            job.fileStore.deleteLocalFile(fsID)
                        else:  # Global Delete
                            job.fileStore.deleteGlobalFile(fsID)
                            assert not os.path.exists(job.fileStore.encodedFileID(fsID))
                            writtenFiles.pop(fsID)
                        if fsID in localFileIDs.keys():
                            for lFID in localFileIDs[fsID]:
                                if lFID not in ('non-local', 'mutable'):
                                    jobDisk += rdelFileSize * 1024 * 1024
                            localFileIDs.pop(fsID)
                        if fileWasCached:
                            if not job.fileStore._fileIsCached(fsID):
                                cached -= rdelFileSize * 1024 * 1024
                        cls._requirementsConcur(job, jobDisk, cached)
                i += 1
            return jobDisk, cached

        @staticmethod
        def _requirementsConcur(job, jobDisk, cached):
            """
            Assert the values for job disk and total cached file sizes tracked in the job's cache
            state file is equal to the values we expect.
            """
            with job.fileStore._CacheState.open(job.fileStore) as cacheInfo:
                jobState = cacheInfo.jobState[job.fileStore.jobID]
                # cached should have a value only if the job store is on a different file system
                # than the cache
                if cacheInfo.nlink != 2:
                    assert cacheInfo.cached == cached
                else:
                    assert cacheInfo.cached == 0
            assert jobState['jobReqs'] == jobDisk

        # Testing the resumability of a failed worker
        def testControlledFailedWorkerRetry(self):
            """
            Conduct a couple of job store operations.  Then die.  Ensure that the restarted job is
            tracking values in the cache state file appropriately.
            """
            workdir = self._createTempDir(purpose='nonLocalDir')
            self.options.retryCount = 1
            F = Job.wrapJobFn(self._controlledFailTestFn, jobDisk=2 * 1024 * 1024 * 1024,
                              testDir=workdir,
                              disk='2G')
            G = Job.wrapJobFn(self._probeJobReqs, sigmaJob=100, disk='100M')
            F.addChild(G)
            Job.Runner.startToil(F, self.options)

        @staticmethod
        def _controlledFailTestFn(job, jobDisk, testDir):
            """
            This is the aux function for the controlled failed worker test.  It does a couple of
            cache operations, fails, then checks whether the new worker starts with the expected
            value, and whether it exits with zero for sigmaJob.

            :param float jobDisk: Disk space supplied for this job
            :param str testDir: T3sting directory
            """
            cls = hidden.AbstractCachingFileStoreTest
            if os.path.exists(os.path.join(testDir, 'testfile.test')):
                with open(os.path.join(testDir, 'testfile.test'), 'r') as fH:
                    cached = unpack('d', fH.read())[0]
                cls._requirementsConcur(job, jobDisk, cached)
                cls._returnFileTestFn(job, jobDisk, cached, testDir, 20)
            else:
                modifiedJobReqs, cached = cls._returnFileTestFn(job, jobDisk, 0, testDir, 20)
                with open(os.path.join(testDir, 'testfile.test'), 'w') as fH:
                    fH.write(pack('d', cached))
                os.kill(os.getpid(), signal.SIGKILL)

        def testRemoveLocalMutablyReadFile(self):
            """
            If a mutably read file is deleted by the user, it is ok.
            """
            self._deleteLocallyReadFilesFn(readAsMutable=True)

        def testRemoveLocalImmutablyReadFile(self):
            """
            If an immutably read file is deleted by the user, it is not ok.
            """
            self._deleteLocallyReadFilesFn(readAsMutable=False)

        def _deleteLocallyReadFilesFn(self, readAsMutable):
            self.options.retryCount = 0
            A = Job.wrapJobFn(self._writeFileToJobStoreWithAsserts, isLocalFile=True, memory='10M')
            B = Job.wrapJobFn(self._removeReadFileFn, A.rv(), readAsMutable=readAsMutable,
                              memory='20M')
            A.addChild(B)
            Job.Runner.startToil(A, self.options)

        @staticmethod
        def _removeReadFileFn(job, fileToDelete, readAsMutable):
            """
            Accept a file. Run os.remove on it. Then attempt to delete it locally. This will raise
            an error for files read immutably.
            Then write a new file to the jobstore and try to do the same. This should always raise
            an error

            :param fileToDelete: File written to the job store that is tracked by the cache
            """
            work_dir = job.fileStore.getLocalTempDir()
            # Are we processing the read file or the written file?
            processsingReadFile = True
            # Read in the file
            outfile = job.fileStore.readGlobalFile(fileToDelete, os.path.join(work_dir, 'temp'),
                                                   mutable=readAsMutable)
            tempfile = os.path.join(work_dir, 'tmp.tmp')
            # The first time we run this loop, processsingReadFile is True and fileToDelete is the
            # file read from the job store.  The second time, processsingReadFile is False and
            # fileToDelete is one that was just written in to the job store. Ensure the correct
            # behaviour is seen in both conditions.
            while True:
                os.rename(outfile, tempfile)
                try:
                    job.fileStore.deleteLocalFile(fileToDelete)
                except IllegalDeletionCacheError:
                    job.fileStore.logToMaster('Detected a deleted file %s.' % fileToDelete)
                    os.rename(tempfile, outfile)
                else:
                    # If we are processing the write test, or if we are testing the immutably read
                    # file, we should not reach here.
                    assert processsingReadFile and readAsMutable
                if processsingReadFile:
                    processsingReadFile = False
                    # Write a file
                    with open(os.path.join(work_dir, str(uuid4())), 'w') as testFile:
                        testFile.write(os.urandom(1 * 1024 * 1024))
                    fileToDelete = job.fileStore.writeGlobalFile(testFile.name)
                    outfile = testFile.name
                else:
                    break

        def testDeleteLocalFile(self):
            """
            Test the deletion capabilities of deleteLocalFile
            """
            self.options.retryCount = 0
            workdir = self._createTempDir(purpose='nonLocalDir')
            A = Job.wrapJobFn(self._deleteLocalFileFn, nonLocalDir=workdir)
            Job.Runner.startToil(A, self.options)

        @staticmethod
        def _deleteLocalFileFn(job, nonLocalDir):
            """
            Test deleteLocalFile on a local write, non-local write, read, mutable read, and bogus
            jobstore IDs.
            """
            work_dir = job.fileStore.getLocalTempDir()
            # Write local file
            with open(os.path.join(work_dir, str(uuid4())), 'w') as localFile:
                localFile.write(os.urandom(1 * 1024 * 1024))
            localFsID = job.fileStore.writeGlobalFile(localFile.name)
            # write Non-Local File
            with open(os.path.join(nonLocalDir, str(uuid4())), 'w') as nonLocalFile:
                nonLocalFile.write(os.urandom(1 * 1024 * 1024))
            nonLocalFsID = job.fileStore.writeGlobalFile(nonLocalFile.name)
            # Delete fsid of local file. The file should be deleted
            job.fileStore.deleteLocalFile(localFsID)
            assert not os.path.exists(localFile.name)
            # Delete fsid of non-local file. The file should persist
            job.fileStore.deleteLocalFile(nonLocalFsID)
            assert os.path.exists(nonLocalFile.name)
            # Read back one file and then delete it
            readBackFile1 = job.fileStore.readGlobalFile(localFsID)
            job.fileStore.deleteLocalFile(localFsID)
            assert not os.path.exists(readBackFile1)
            # Read back one file with 2 different names and then delete it. Assert both get deleted
            readBackFile1 = job.fileStore.readGlobalFile(localFsID)
            readBackFile2 = job.fileStore.readGlobalFile(localFsID)
            job.fileStore.deleteLocalFile(localFsID)
            assert not os.path.exists(readBackFile1)
            assert not os.path.exists(readBackFile2)
            # Try to get a bogus FSID
            try:
                job.fileStore.readGlobalFile('bogus')
            except NoSuchFileException:
                pass


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


class NonCachingFileStoreTestWithFileJobStore(hidden.AbstractNonCachingFileStoreTest):
    jobStoreType = 'file'


class CachingFileStoreTestWithFileJobStore(hidden.AbstractCachingFileStoreTest):
    jobStoreType = 'file'


@needs_aws
class NonCachingFileStoreTestWithAwsJobStore(hidden.AbstractNonCachingFileStoreTest):
    jobStoreType = 'aws'


@needs_aws
class CachingFileStoreTestWithAwsJobStore(hidden.AbstractCachingFileStoreTest):
    jobStoreType = 'aws'


@needs_azure
@experimental
class NonCachingFileStoreTestWithAzureJobStore(hidden.AbstractNonCachingFileStoreTest):
    jobStoreType = 'azure'


@needs_azure
@experimental
class CachingFileStoreTestWithAzureJobStore(hidden.AbstractCachingFileStoreTest):
    jobStoreType = 'azure'


@experimental
@needs_google
class NonCachingFileStoreTestWithGoogleJobStore(hidden.AbstractNonCachingFileStoreTest):
    jobStoreType = 'google'


@experimental
@needs_google
class CachingFileStoreTestWithGoogleJobStore(hidden.AbstractCachingFileStoreTest):
    jobStoreType = 'google'


def _exportStaticMethodAsGlobalFunctions(cls):
    """
    Define utility functions because Toil can't pickle static methods. Note that this relies on
    the convention that the first argument of a job function is named 'job'.
    """
    for name, kind, clazz, value in inspect.classify_class_attrs(cls):
        if kind == 'static method':
            method = value.__func__
            args = inspect.getargspec(method).args
            if args and args[0] == 'job':
                globals()[name] = method


_exportStaticMethodAsGlobalFunctions(hidden.AbstractFileStoreTest)
_exportStaticMethodAsGlobalFunctions(hidden.AbstractCachingFileStoreTest)
_exportStaticMethodAsGlobalFunctions(hidden.AbstractNonCachingFileStoreTest)
