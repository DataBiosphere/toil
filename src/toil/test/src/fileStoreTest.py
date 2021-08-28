# Copyright (C) 2015-2021 Regents of the University of California
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
import collections
import datetime
import errno
import filecmp
import inspect
import logging
import os
import stat
import random
import signal
import time
from abc import ABCMeta
from struct import pack, unpack
from uuid import uuid4

import pytest

from toil.common import Toil
from toil.fileStores import FileID
from toil.fileStores.cachingFileStore import (CacheUnbalancedError,
                                              IllegalDeletionCacheError)
from toil.job import Job
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.leader import FailedJobsException
from toil.realtimeLogger import RealtimeLogger
from toil.test import ToilTest, needs_aws_ec2, needs_google, slow, travis_test

# Some tests take too long on the AWS jobstore and are unquitable for CI.  They can be
# be run during manual tests by setting this to False.
testingIsAutomatic = True

logger = logging.getLogger(__name__)


class hidden:
    """
    Hiding the abstract test classes from the Unittest loader so it can be inherited in different
    test suites for the different job stores.
    """
    class AbstractFileStoreTest(ToilTest, metaclass=ABCMeta):
        """
        An abstract base class for testing the various general functions described in
        :class:toil.fileStores.abstractFileStore.AbstractFileStore
        """
        # This is overwritten in the inheriting classs
        jobStoreType = None

        def _getTestJobStore(self):
            if self.jobStoreType == 'file':
                return self._getTestJobStorePath()
            elif self.jobStoreType == 'aws':
                return 'aws:%s:cache-tests-%s' % (self.awsRegion(), str(uuid4()))
            elif self.jobStoreType == 'google':
                projectID = os.getenv('TOIL_GOOGLE_PROJECTID')
                return 'google:%s:cache-tests-%s' % (projectID, str(uuid4()))
            else:
                raise RuntimeError('Illegal job store type.')

        def setUp(self):
            super(hidden.AbstractFileStoreTest, self).setUp()
            testDir = self._createTempDir()
            self.options = Job.Runner.getDefaultOptions(self._getTestJobStore())
            self.options.logLevel = 'DEBUG'
            self.options.realTimeLogging = True
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
        @travis_test
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

        @slow
        def testFileStoreLogging(self):
            """
            Write a couple of files to the jobstore.  Delete a couple of them.  Read back written
            and locally deleted files.
            """

            class WatchingHandler(logging.Handler):
                """
                A logging handler that watches for a certain substring and
                trips a flag if it appears.
                """
                def __init__(self, match: str):
                    super().__init__()
                    self.match = match
                    self.seen = False
                def emit(self, record):
                    if self.match in record.getMessage():
                        self.seen = True

            handler = WatchingHandler("cats.txt")

            logging.getLogger().addHandler(handler)

            F = Job.wrapJobFn(self._accessAndFail,
                              disk='100M')
            try:
                Job.Runner.startToil(F, self.options)
            except FailedJobsException:
                # We expect this.
                pass

            logging.getLogger().removeHandler(handler)

            assert handler.seen, "Downloaded file name not found in logs of failing Toil run"

        @staticmethod
        def _accessAndFail(job):
            with job.fileStore.writeGlobalFileStream() as (stream, fileID):
                stream.write('Cats'.encode('utf-8'))
            localPath = os.path.join(job.fileStore.getLocalTempDir(), 'cats.txt')
            job.fileStore.readGlobalFile(fileID, localPath)
            with job.fileStore.readGlobalFileStream(fileID) as stream2:
                pass
            raise RuntimeError("I do not like this file")


        # Test filestore operations.  This is a slightly less intense version of the cache specific
        # test `testReturnFileSizes`
        @slow
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

            # Fill in the size of the local file we just made
            writtenFiles[fsId] = writeFileSize
            # Remember it actually should be local
            localFileIDs.add(fsId)
            logger.info('Now have local file: %s', fsId)

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
                    if isLocalFile:
                        localFileIDs.add(fsID)
                    logger.info('Wrote %s file of size %d MB: %s', 'local' if isLocalFile else 'non-local', writeFileSize, fsID)
                else:
                    if len(writtenFiles) == 0:
                        continue
                    else:
                        fsID, rdelFileSize = random.choice(list(writtenFiles.items()))
                        rdelRandVal = random.random()
                    if randVal < 0.66:  # Read
                        mutable = True if random.random() <= 0.5 else False
                        cache = True if random.random() <= 0.5 else False
                        job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, str(uuid4())]),
                                                     cache=cache, mutable=mutable)
                        localFileIDs.add(fsID)
                        logger.info('Read %s %s local copy of: %s', 'mutable' if mutable else 'immutable', 'cached' if cache else 'uncached', fsID)
                    else:  # Delete
                        if rdelRandVal <= 0.5:  # Local Delete
                            if fsID not in localFileIDs:
                                # Make sure trying to deleteLocalFile this file fails properly
                                try:
                                    job.fileStore.deleteLocalFile(fsID)
                                except OSError as e:
                                    if e.errno != errno.ENOENT:
                                        # This is supposed to produce an
                                        # ENOENT. If it doesn't something is
                                        # broken.
                                        raise
                                    logger.info('Correctly fail to local-delete non-local file: %s', fsID)
                                else:
                                    assert False, "Was able to delete non-local file {}".format(fsID)
                            else:
                                logger.info('Delete local file: %s', fsID)
                                job.fileStore.deleteLocalFile(fsID)
                        else:  # Global Delete
                            job.fileStore.deleteGlobalFile(fsID)
                            writtenFiles.pop(fsID)
                        if fsID in localFileIDs:
                            localFileIDs.remove(fsID)
                            logger.info('No longer have file: %s', fsID)
                i += 1

        def testWriteReadGlobalFilePermissions(self):
            """
            Ensures that uploaded files preserve their file permissions when they
            are downloaded again. This function checks that a written executable file
            maintains its executability after being read.
            """
            for executable in True, False:
                for disable_caching in True, False:
                    with self.subTest(f'Testing readwrite file permissions\n'
                                      f'[executable: {executable}]\n'
                                      f'[disable_caching: {disable_caching}]\n'):
                        self.options.disableCaching = disable_caching
                        read_write_job = Job.wrapJobFn(self._testWriteReadGlobalFilePermissions, executable=executable)
                        Job.Runner.startToil(read_write_job, self.options)

        @staticmethod
        def _testWriteReadGlobalFilePermissions(job, executable):
            srcFile = job.fileStore.getLocalTempFile()
            with open(srcFile, 'w') as f:
                f.write('Hello')

            if executable:
                os.chmod(srcFile, os.stat(srcFile).st_mode | stat.S_IXUSR)

            # Initial file owner execute permissions
            initialPermissions = os.stat(srcFile).st_mode & stat.S_IXUSR
            fileID = job.fileStore.writeGlobalFile(srcFile)

            for mutable in True, False:
                for symlink in True, False:
                    dstFile = job.fileStore.getLocalTempFileName()
                    job.fileStore.readGlobalFile(fileID, userPath=dstFile, mutable=mutable, symlink=symlink)
                    # Current file owner execute permissions
                    currentPermissions = os.stat(dstFile).st_mode & stat.S_IXUSR
                    assert initialPermissions == currentPermissions, f'{initialPermissions} != {currentPermissions}'

        def testWriteExportFileCompatibility(self):
            """
            Ensures that files created in a job preserve their executable permissions
            when they are exported from the leader.
            """
            for executable in True, False:
                export_file_job = Job.wrapJobFn(self._testWriteExportFileCompatibility, executable=executable)
                with Toil(self.options) as toil:
                    initialPermissions, fileID = toil.start(export_file_job)
                    dstFile = os.path.join(self._createTempDir(), str(uuid4()))
                    toil.exportFile(fileID, 'file://' + dstFile)
                    currentPermissions = os.stat(dstFile).st_mode & stat.S_IXUSR

                    assert initialPermissions == currentPermissions, f'{initialPermissions} != {currentPermissions}'

        @staticmethod
        def _testWriteExportFileCompatibility(job, executable):
            srcFile = job.fileStore.getLocalTempFile()
            with open(srcFile, 'w') as f:
                f.write('Hello')
            if executable:
                os.chmod(srcFile, os.stat(srcFile).st_mode | stat.S_IXUSR)
            initialPermissions = os.stat(srcFile).st_mode & stat.S_IXUSR
            fileID = job.fileStore.writeGlobalFile(srcFile)
            return initialPermissions, fileID

        def testImportReadFileCompatibility(self):
            """
            Ensures that files imported to the leader preserve their executable permissions
            when they are read by the fileStore.
            """
            with Toil(self.options) as toil:
                workDir = self._createTempDir()
                for executable in True, False:
                    srcFile = os.path.join(workDir, str(uuid4()))
                    with open(srcFile, 'w') as f:
                        f.write('Hello')
                    if executable:
                        os.chmod(srcFile, os.stat(srcFile).st_mode | stat.S_IXUSR)
                    initialPermissions = os.stat(srcFile).st_mode & stat.S_IXUSR
                    jobStoreFileID = toil.importFile('file://' + srcFile)
                    for mutable in True,False:
                        for symlink in True, False:
                            with self.subTest(f'Now testing readGlobalFileWith: mutable={mutable} symlink={symlink}'):
                                dstFile = os.path.join(workDir, str(uuid4()))
                                A = Job.wrapJobFn(self._testImportReadFileCompatibility,
                                                  fileID=jobStoreFileID,
                                                  dstFile=dstFile,
                                                  initialPermissions=initialPermissions,
                                                  mutable=mutable,
                                                  symlink=symlink)
                                toil.start(A)

        @staticmethod
        def _testImportReadFileCompatibility(job, fileID, dstFile, initialPermissions, mutable, symlink):
            dstFile = job.fileStore.readGlobalFile(fileID, mutable=mutable, symlink=symlink)
            currentPermissions = os.stat(dstFile).st_mode & stat.S_IXUSR

            assert initialPermissions == currentPermissions

        def testReadWriteFileStreamTextMode(self):
            """
            Checks if text mode is compatibile with file streams.
            """
            with Toil(self.options) as toil:
                A = Job.wrapJobFn(_testReadWriteFileStreamTextMode)
                toil.start(A)

        @staticmethod
        def _testReadWriteFileStreamTextMode(job):
            with job.fileStore.writeGlobalFileStream(encoding='utf-8') as (stream, fileID):
                stream.write('foo')
            job.fileStore.readGlobalFileStream(fileID)
            with job.fileStore.readGlobalFileStream(fileID, encoding='utf-8') as stream2:
                assert 'foo' == stream2.read()

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
            with open(os.path.join(work_dir, str(uuid4())), 'wb') as testFile:
                testFile.write(os.urandom(fileMB * 1024 * 1024))

            return job.fileStore.writeGlobalFile(testFile.name), testFile

    class AbstractNonCachingFileStoreTest(AbstractFileStoreTest, metaclass=ABCMeta):
        """
        Abstract tests for the the various functions in
        :class:toil.fileStores.nonCachingFileStore.NonCachingFileStore. These
        tests are general enough that they can also be used for
        :class:toil.fileStores.CachingFileStore.
        """

        def setUp(self):
            super(hidden.AbstractNonCachingFileStoreTest, self).setUp()
            self.options.disableCaching = True

    class AbstractCachingFileStoreTest(AbstractFileStoreTest, metaclass=ABCMeta):
        """
        Abstract tests for the the various cache-related functions in
        :class:toil.fileStores.cachingFileStore.CachingFileStore.
        """

        def setUp(self):
            super(hidden.AbstractCachingFileStoreTest, self).setUp()
            self.options.disableCaching = False

        @slow
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
            for test in range(0, 20):
                E = Job.wrapJobFn(self._uselessFunc)
                F = Job.wrapJobFn(self._uselessFunc)
                jobs = {}
                for i in range(0, 10):
                    jobs[i] = Job.wrapJobFn(self._uselessFunc)
                    E.addChild(jobs[i])
                    jobs[i].addChild(F)
                Job.Runner.startToil(E, self.options)

        @slow
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

        @slow
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

        @slow
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
            # If the job store and cache are on the same file system, file
            # sizes are accounted for by the job store and are not reflected in
            # the cache hence this test is redundant (caching will be free).
            if not self.options.jobStore.startswith(('aws', 'google')):
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
                D = Job.wrapJobFn(self._adjustCacheLimit, newTotalMB=50, disk='0Mi')
                E = Job.wrapJobFn(self._uselessFunc, disk=''.join([str(diskRequestMB), 'Mi']))
                # Set it to > 2GB such that the cleanup jobs don't die in the non-fail cases
                F = Job.wrapJobFn(self._adjustCacheLimit, newTotalMB=5000, disk='10Mi')
                G = Job.wrapJobFn(self._probeJobReqs, sigmaJob=100, cached=expectedResult,
                                  disk='100Mi')
                A.addChild(B)
                B.addChild(C)
                C.addChild(D)
                D.addChild(E)
                E.addChild(F)
                F.addChild(G)
                Job.Runner.startToil(A, self.options)
            except FailedJobsException as err:
                with open(self.options.logFile) as f:
                    logContents = f.read()
                if CacheUnbalancedError.message in logContents:
                    self.assertEqual(expectedResult, 'Fail')
                else:
                    self.fail('Toil did not raise the expected CacheUnbalancedError but failed for some other reason')

        @staticmethod
        def _writeFileToJobStoreWithAsserts(job, isLocalFile, nonLocalDir=None, fileMB=1, expectAsyncUpload=True):
            """
            This function creates a file and writes it to the jobstore.

            :param bool isLocalFile: Is the file local(T) (i.e. in the file
                                     store managed temp dir) or Non-Local(F)?
                                     Non-local files should not be cached.
            :param str nonLocalDir: A dir to write the file to.  If unspecified, a local directory
                                    is created.
            :param int fileMB: Size of the created file in MB
            :param bool expectAsyncUpload: Whether we expect the upload to hit
                                           the job store later(T) or immediately(F)
            """
            cls = hidden.AbstractNonCachingFileStoreTest
            fsID, testFile = cls._writeFileToJobStore(job, isLocalFile, nonLocalDir, fileMB)
            actual = os.stat(testFile.name).st_nlink

            # If the caching is free, the job store must have hard links to
            # everything the file store has.
            expectJobStoreLink = job.fileStore.cachingIsFree()

            # How many links ought this file to have?
            expected = 1

            if isLocalFile:
                # We expect a hard link into the cache and not a copy
                expected += 1

                if expectJobStoreLink and not expectAsyncUpload:
                    # We also expect a link in the job store
                    expected += 1

            assert actual == expected, 'Should have %d links. Got %d.' % (expected, actual)

            logger.info('Uploaded %s with %d links', fsID, actual)

            if not isLocalFile:
                # Make sure it isn't cached if we don't want it to be
                assert not job.fileStore.fileIsCached(fsID), "File uploaded from non-local-temp directory %s should not be cached" % nonLocalDir

            return fsID

        @staticmethod
        def _sleepy(job, timeToSleep):
            """
            I'm waiting for prince charming... but only for timeToSleep seconds.

            :param int timeToSleep: Time in seconds
            """
            time.sleep(timeToSleep)

        @staticmethod
        def _adjustCacheLimit(job, newTotalMB):
            """
            This function tells the cache to adopt a new "total" value =
            newTotalMB, changing the maximum cache disk space allowed for the
            run.

            :param int newTotalMB: New total cache disk space limit in MB.
            """

            # Convert to bytes and pass on to the actual cache
            job.fileStore.adjustCacheLimit(float(newTotalMB * 1024 * 1024))

        @staticmethod
        def _probeJobReqs(job, total=None, cached=None, sigmaJob=None):
            """
            Probes the cacheLockFile to ensure the values for total, disk and cache are as expected.
            Can also specify combinations of the requirements if desired.

            :param int total: Expected Total Space available for caching in MB.
            :param int cached: Expected Total size of files in the cache in MB.
            :param int sigmaJob: Expected sum of job requirements in MB.
            """

            RealtimeLogger.info('Probing job requirements')

            valueDict = locals()
            assert (total or cached or sigmaJob)

            # Work out which function to call for which value
            toCall = {'total': job.fileStore.getCacheLimit,
                      'cached': job.fileStore.getCacheUsed,
                      'sigmaJob': job.fileStore.getCacheExtraJobSpace}

            for value in ('total', 'cached', 'sigmaJob'):
                # If the value wasn't provided, it is None and should be ignored
                if valueDict[value] is None:
                    continue

                RealtimeLogger.info('Probing cache state: %s', value)

                expectedBytes = valueDict[value] * 1024 * 1024
                cacheInfoBytes = toCall[value]()

                RealtimeLogger.info('Got %d for %s; expected %d', cacheInfoBytes, value, expectedBytes)

                assert cacheInfoBytes == expectedBytes, 'Testing %s: Expected ' % value + \
                                                  '%s but got %s.' % (expectedBytes, cacheInfoBytes)

        @slow
        def testAsyncWriteWithCaching(self):
            """
            Ensure the Async Writing of files happens as expected.  The first Job forcefully
            modifies the cache size to 1GB. The second asks for 1GB of disk and  writes a 900MB
            file into cache then rewrites it to the job store triggering an async write since the
            two unique jobstore IDs point to the same local file.  Also, the second write is not
            cached since the first was written to cache, and there "isn't enough space" to cache the
            second.  Imediately assert that the second write isn't cached, and is being
            asynchronously written to the job store.

            Attempting to get the file from the jobstore should not fail.
            """
            self.options.retryCount = 0
            self.options.logLevel = 'DEBUG'
            A = Job.wrapJobFn(self._adjustCacheLimit, newTotalMB=1024, disk='1G')
            B = Job.wrapJobFn(self._doubleWriteFileToJobStore, fileMB=850, disk='900M')
            C = Job.wrapJobFn(self._readFromJobStoreWithoutAssertions, fsID=B.rv(), disk='1G')
            # Set it to > 2GB such that the cleanup jobs don't die.
            D = Job.wrapJobFn(self._adjustCacheLimit, newTotalMB=5000, disk='1G')
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
            job.fileStore.logToMaster('Double writing a file into job store')
            work_dir = job.fileStore.getLocalTempDir()
            with open(os.path.join(work_dir, str(uuid4())), 'wb') as testFile:
                testFile.write(os.urandom(fileMB * 1024 * 1024))

            job.fileStore.logToMaster('Writing copy 1 and discarding ID')
            job.fileStore.writeGlobalFile(testFile.name)
            job.fileStore.logToMaster('Writing copy 2 and saving ID')
            fsID = job.fileStore.writeGlobalFile(testFile.name)
            job.fileStore.logToMaster('Copy 2 ID: {}'.format(fsID))

            hidden.AbstractCachingFileStoreTest._readFromJobStoreWithoutAssertions(job, fsID)

            job.fileStore.logToMaster('Writing copy 3 and returning ID')
            return job.fileStore.writeGlobalFile(testFile.name)

        @staticmethod
        def _readFromJobStoreWithoutAssertions(job, fsID):
            """
            Reads a file from the job store.  That will be all, thank you.

            :param job: job
            :param fsID: Job store file ID for the read file
            :return: None
            """
            job.fileStore.logToMaster('Reading the written file')
            job.fileStore.readGlobalFile(fsID)

        # writeGlobalFile tests

        @travis_test
        def testWriteNonLocalFileToJobStore(self):
            """
            Write a file not in localTempDir to the job store.  Such a file should not be cached.
            Ensure the file is not cached.
            """
            workdir = self._createTempDir(purpose='nonLocalDir')
            A = Job.wrapJobFn(self._writeFileToJobStoreWithAsserts, isLocalFile=False,
                              nonLocalDir=workdir)
            Job.Runner.startToil(A, self.options)

        @travis_test
        def testWriteLocalFileToJobStore(self):
            """
            Write a file from the localTempDir to the job store.  Such a file will be cached by
            default.  Ensure the file is cached.
            """
            A = Job.wrapJobFn(self._writeFileToJobStoreWithAsserts, isLocalFile=True)
            Job.Runner.startToil(A, self.options)

        # readGlobalFile tests

        @travis_test
        def testReadCacheMissFileFromJobStoreWithoutCachingReadFile(self):
            """
            Read a file from the file store that does not have a corresponding cached copy.  Do not
            cache the read file.  Ensure the number of links on the file are appropriate.
            """
            self._testCacheMissFunction(cacheReadFile=False)

        @travis_test
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

            Note that we may see hard links when we don't expect it based on
            caching, because immutable reads from the FileJobStore can be
            fulfilled by hardlinks. We only do immutable reads.

            :param bool isCachedFile: Flag.  Was the read file read from cache(T)? If so, we look for a hard link.
            :param bool cacheReadFile: Should the the file that is read be cached(T)?
            :param str fsID: job store file ID
            :param bool isTest: Is this being run as a test(T) or an accessory to another test(F)?

            """
            work_dir = job.fileStore.getLocalTempDir()
            wantHardLink = False
            if isCachedFile:
                outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']),
                                                       mutable=False)
                wantHardLink = True
            else:
                if cacheReadFile:
                    outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']),
                                                           cache=True, mutable=False)
                    wantHardLink = True
                else:
                    assert not job.fileStore.fileIsCached(fsID), "File mistakenly cached before read"
                    outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']),
                                                           cache=False, mutable=False)
                    assert not job.fileStore.fileIsCached(fsID), "File mistakenly cached after read"
                    wantHardLink = False
            if isTest:
                actual = os.stat(outfile).st_nlink
                if wantHardLink:
                    assert actual > 1, 'Should have multiple links for file that was %s and %s. Got %i.' % ('cached' if isCachedFile else 'not cached',
                        'saved' if cacheReadFile else 'not saved', actual)
                # We need to accept harf links even if we don't want them,
                # because we may get them straight from the FileJobStore since
                # we asked for immutable reads.
                return None
            else:
                return outfile

        @travis_test
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

        @slow
        def testMultipleJobsReadSameCacheHitGlobalFile(self):
            """
            Write a local file to the job store (hence adding a copy to cache), then have 10 jobs
            read it. Assert cached file size never goes up, assert unused job
            required disk space is always:
                   (a multiple of job reqs) - (number of current file readers * filesize).
            At the end, assert the cache shows unused job-required space = 0.
            """
            self._testMultipleJobsReadGlobalFileFunction(cacheHit=True)

        @slow
        def testMultipleJobsReadSameCacheMissGlobalFile(self):
            """
            Write a non-local file to the job store(hence no cached copy), then have 10 jobs read
            it. Assert cached file size never goes up, assert unused job
            required disk space is always:
                   (a multiple of job reqs) - (number of current file readers * filesize).
            At the end, assert the cache shows unused job-required space = 0.
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
            B = Job.wrapJobFn(self._probeJobReqs, sigmaJob=100, disk='100Mi')
            jobs = {}
            for i in range(0, 10):
                jobs[i] = Job.wrapJobFn(self._multipleFileReader, diskMB=1024, fsID=A.rv(),
                                        maxWriteFile=os.path.abspath(x.name), disk='1Gi',
                                        memory='10Mi', cores=1)
                A.addChild(jobs[i])
                jobs[i].addChild(B)
            Job.Runner.startToil(A, self.options)
            with open(x.name, 'r') as y:
                assert int(y.read()) > 2

        @staticmethod
        def _multipleFileReader(job, diskMB, fsID, maxWriteFile):
            """
            Read a file from the job store immutable and explicitly ask to have it in the cache.
            If caching files is free, assert used cache space is zero, else
            assert it is equal to the read file.
            Also assert the sum job reqs + (number of readers of file * filesize) is
            and integer multiple of the disk requirements provided to this job.

            :param int diskMB: disk requirements provided to the job
            :param str fsID: job store file ID
            :param str maxWriteFile: path to file where the max number of concurrent readers of
                                     file will be written
            """
            work_dir = job.fileStore.getLocalTempDir()
            outfile = job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, 'temp']), cache=True,
                                                   mutable=False)
            diskBytes = diskMB * 1024 * 1024
            fileStats = os.stat(outfile)
            fileSize = fileStats.st_size

            currentReaders = job.fileStore.getFileReaderCount(fsID)

            extraJobSpace = job.fileStore.getCacheExtraJobSpace()

            usedCache = job.fileStore.getCacheUsed()

            logger.info('Extra job space: %s', str(extraJobSpace))
            logger.info('Current file readers: %s', str(currentReaders))
            logger.info('File size: %s', str(fileSize))
            logger.info('Job disk bytes: %s', str(diskBytes))
            logger.info('Used cache: %s', str(usedCache))

            with open(maxWriteFile, 'r+') as x:
                prev_max = int(x.read())
                x.seek(0)
                x.truncate()
                x.write(str(max(prev_max, currentReaders)))
            if job.fileStore.cachingIsFree():
                # No space should be used when caching is free
                assert usedCache == 0.0
            else:
                # The right amount of space should be used otherwise
                assert usedCache == fileSize

            # Make sure that there's no over-usage of job requirements
            assert ((extraJobSpace + currentReaders * fileSize) %
                    diskBytes) == 0.0
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
            if not filecmp.cmp(fileName, outputFile):
                logger.warning('Source file: %s', str(os.stat(fileName)))
                logger.warning('Destination file: %s', str(os.stat(outputFile)))
                raise RuntimeError("File {} did not properly get copied to {}".format(fileName, outputFile))

        @slow
        def testFileStoreExportFile(self):
            # Tests that files written to job store can be immediately exported
            # motivated by https://github.com/BD2KGenomics/toil/issues/1469
            root = Job.wrapJobFn(self._writeExportGlobalFile)
            Job.Runner.startToil(root, self.options)

        # Testing for the return of file sizes to the sigma job pool.
        @slow
        def testReturnFileSizes(self):
            """
            Write a couple of files to the jobstore. Delete a couple of them.
            Read back written and locally deleted files. Ensure that after
            every step that the cache is in a valid state.
            """
            workdir = self._createTempDir(purpose='nonLocalDir')
            F = Job.wrapJobFn(self._returnFileTestFn,
                              jobDisk=2 * 1024 * 1024 * 1024,
                              initialCachedSize=0,
                              nonLocalDir=workdir,
                              disk='2Gi')
            Job.Runner.startToil(F, self.options)

        @slow
        def testReturnFileSizesWithBadWorker(self):
            """
            Write a couple of files to the jobstore. Delete a couple of them.
            Read back written and locally deleted files. Ensure that after
            every step that the cache is in a valid state.
            """
            self.options.retryCount = 20
            self.options.badWorker = 0.5
            self.options.badWorkerFailInterval = 0.1
            workdir = self._createTempDir(purpose='nonLocalDir')
            F = Job.wrapJobFn(self._returnFileTestFn,
                              jobDisk=2 * 1024 * 1024 * 1024,
                              initialCachedSize=0,
                              nonLocalDir=workdir,
                              numIters=30, disk='2Gi')
            Job.Runner.startToil(F, self.options)

        @staticmethod
        def _returnFileTestFn(job, jobDisk, initialCachedSize, nonLocalDir, numIters=100):
            """
            Aux function for jobCacheTest.testReturnFileSizes Conduct numIters operations and ensure
            the cache has the right amount of data in it at all times.

            Track the cache calculations even thought they won't be used in filejobstore

            Assumes nothing is evicted from the cache.

            :param float jobDisk: The value of disk passed to this job.
            """
            cached = initialCachedSize
            RealtimeLogger.info('Expecting %d bytes cached initially', cached)
            work_dir = job.fileStore.getLocalTempDir()
            writtenFiles = {}  # fsID: (size, isLocal)
            # fsid: local/mutable/immutable for all operations that should make local files as tracked by the FileStore
            localFileIDs = collections.defaultdict(list)
            # Add one file for the sake of having something in the job store
            writeFileSize = random.randint(0, 30)
            jobDisk -= writeFileSize * 1024 * 1024
            # We keep jobDisk in sync with the amount of free space the job
            # still has that the file store doesn't know it has used.
            cls = hidden.AbstractCachingFileStoreTest
            fsId = cls._writeFileToJobStoreWithAsserts(job, isLocalFile=True, fileMB=writeFileSize)
            writtenFiles[fsId] = writeFileSize
            if job.fileStore.fileIsCached(list(writtenFiles.keys())[0]):
                cached += writeFileSize * 1024 * 1024
                RealtimeLogger.info('Expecting %d bytes cached because file of %d MB is cached', cached, writeFileSize)
            else:
                RealtimeLogger.info('Expecting %d bytes cached because file of %d MB is not cached', cached, writeFileSize)
            localFileIDs[list(writtenFiles.keys())[0]].append('local')
            RealtimeLogger.info('Checking for %d bytes cached', cached)
            cls._requirementsConcur(job, jobDisk, cached)
            i = 0
            while i <= numIters:
                randVal = random.random()
                if randVal < 0.33:  # Write
                    RealtimeLogger.info('Writing a file')
                    writeFileSize = random.randint(0, 30)
                    if random.random() <= 0.5:  # Write a local file
                        RealtimeLogger.info('Writing a local file of %d MB', writeFileSize)
                        fsID = cls._writeFileToJobStoreWithAsserts(job, isLocalFile=True,
                                                                   fileMB=writeFileSize)
                        RealtimeLogger.info('Wrote local file: %s', fsID)
                        writtenFiles[fsID] = writeFileSize
                        localFileIDs[fsID].append('local')
                        jobDisk -= writeFileSize * 1024 * 1024
                        if job.fileStore.fileIsCached(fsID):
                            cached += writeFileSize * 1024 * 1024
                            RealtimeLogger.info('Expecting %d bytes cached because file of %d MB is cached', cached, writeFileSize)
                        else:
                            RealtimeLogger.info('Expecting %d bytes cached because file of %d MB is not cached', cached, writeFileSize)
                    else:  # Write a non-local file
                        RealtimeLogger.info('Writing a non-local file of %d MB', writeFileSize)
                        fsID = cls._writeFileToJobStoreWithAsserts(job, isLocalFile=False,
                                                                   nonLocalDir=nonLocalDir,
                                                                   fileMB=writeFileSize)
                        RealtimeLogger.info('Wrote non-local file: %s', fsID)
                        writtenFiles[fsID] = writeFileSize
                        # Don't record in localFileIDs because we're not local
                        # No change to the job since there was no caching
                    RealtimeLogger.info('Checking for %d bytes cached', cached)
                    cls._requirementsConcur(job, jobDisk, cached)
                else:
                    if len(writtenFiles) == 0:
                        continue
                    else:
                        fsID, rdelFileSize = random.choice(list(writtenFiles.items()))
                        rdelRandVal = random.random()
                        fileWasCached = job.fileStore.fileIsCached(fsID)
                    if randVal < 0.66:  # Read
                        RealtimeLogger.info('Reading a file with size %d and previous cache status %s: %s', rdelFileSize, str(fileWasCached), fsID)
                        if rdelRandVal <= 0.5:  # Read as mutable, uncached
                            RealtimeLogger.info('Reading as mutable and uncached; should still have %d bytes cached', cached)
                            job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, str(uuid4())]),
                                                         mutable=True, cache=False)
                            localFileIDs[fsID].append('mutable')
                            # No change because the file wasn't cached
                        else:  # Read as immutable
                            RealtimeLogger.info('Reading as immutable and cacheable')
                            job.fileStore.readGlobalFile(fsID, '/'.join([work_dir, str(uuid4())]),
                                                         mutable=False, cache=True)
                            localFileIDs[fsID].append('immutable')
                            jobDisk -= rdelFileSize * 1024 * 1024
                        if not fileWasCached:
                            if job.fileStore.fileIsCached(fsID):
                                RealtimeLogger.info('File was not cached before and is now. Should have %d bytes cached', cached)
                                cached += rdelFileSize * 1024 * 1024
                            else:
                                RealtimeLogger.info('File was not cached before and still is not now. '
                                                    'Should still have %d bytes cached', cached)
                        else:
                            RealtimeLogger.info('File was cached before. Should still have %d bytes cached', cached)
                        cls._requirementsConcur(job, jobDisk, cached)
                    else:  # Delete
                        if rdelRandVal <= 0.5:  # Local Delete
                            if fsID not in list(localFileIDs.keys()):
                                continue
                            RealtimeLogger.info('Deleting a file locally with history %s: %s', localFileIDs[fsID], fsID)
                            job.fileStore.deleteLocalFile(fsID)
                        else:  # Global Delete
                            RealtimeLogger.info('Deleting a file globally: %s', fsID)
                            job.fileStore.deleteGlobalFile(fsID)
                            try:
                                job.fileStore.readGlobalFile(fsID)
                            except FileNotFoundError as err:
                                pass
                            except:
                                raise RuntimeError('Got wrong error type for read of deleted file')
                            else:
                                raise RuntimeError('Able to read deleted file')
                            writtenFiles.pop(fsID)
                        if fsID in list(localFileIDs.keys()):
                            for lFID in localFileIDs[fsID]:
                                if lFID != 'mutable':
                                    jobDisk += rdelFileSize * 1024 * 1024
                            localFileIDs.pop(fsID)
                        if fileWasCached:
                            if not job.fileStore.fileIsCached(fsID):
                                cached -= rdelFileSize * 1024 * 1024
                                RealtimeLogger.info('File was cached before and is not now. Should have %d bytes cached', cached)
                            else:
                                RealtimeLogger.info('File was cached before and still is cached now. '
                                                    'Should still have %d bytes cached', cached)
                        else:
                            RealtimeLogger.info('File was not cached before deletion. Should still have %d bytes cached', cached)
                        cls._requirementsConcur(job, jobDisk, cached)
                i += 1
            return jobDisk, cached

        @staticmethod
        def _requirementsConcur(job, jobDisk, cached):
            """
            Assert the values for job disk and total cached file sizes tracked
            by the file store are equal to the values we expect.
            """

            used = job.fileStore.getCacheUsed()

            if not job.fileStore.cachingIsFree():
                RealtimeLogger.info('Caching is not free; %d bytes are used and %d bytes are expected', used, cached)
                assert used == cached, 'Cache should have %d bytes used, but actually has %d bytes used' % (cached, used)
            else:
                RealtimeLogger.info('Caching is free; %d bytes are used and %d bytes would be expected if caching were not free', used, cached)
                assert used == 0, 'Cache should have nothing in it, but actually has %d bytes used' % used

            jobUnused = job.fileStore.getCacheUnusedJobRequirement()

            assert jobUnused == jobDisk, 'Job should have %d bytes of disk for non-FileStore use but the FileStore reports %d' % (jobDisk, jobUnused)

        # Testing the resumability of a failed worker
        @slow
        def testControlledFailedWorkerRetry(self):
            """
            Conduct a couple of job store operations.  Then die.  Ensure that the restarted job is
            tracking values in the cache state file appropriately.
            """
            workdir = self._createTempDir(purpose='nonLocalDir')
            self.options.retryCount = 1
            jobDiskBytes = 2 * 1024 * 1024 * 1024
            F = Job.wrapJobFn(self._controlledFailTestFn, jobDisk=jobDiskBytes,
                              testDir=workdir,
                              disk=jobDiskBytes)
            G = Job.wrapJobFn(self._probeJobReqs, sigmaJob=100, disk='100Mi')
            F.addChild(G)
            Job.Runner.startToil(F, self.options)

        @staticmethod
        def _controlledFailTestFn(job, jobDisk, testDir):
            """
            This is the aux function for the controlled failed worker test.  It does a couple of
            cache operations, fails, then checks whether the new worker starts with the expected
            value, and whether it computes cache statistics correctly.

            :param float jobDisk: Disk space supplied for this job
            :param str testDir: Testing directory
            """

            # Make sure we actually have the disk size we are supposed to
            job.fileStore.logToMaster('Job is running with %d bytes of disk, %d requested' % (job.disk, jobDisk))
            assert job.disk == jobDisk, 'Job was scheduled with %d bytes but requested %d' % (job.disk, jobDisk)

            cls = hidden.AbstractCachingFileStoreTest
            if os.path.exists(os.path.join(testDir, 'testfile.test')):
                with open(os.path.join(testDir, 'testfile.test'), 'rb') as fH:
                    cached = unpack('d', fH.read())[0]
                RealtimeLogger.info('Loaded expected cache size of %d from testfile.test', cached)
                cls._requirementsConcur(job, jobDisk, cached)
                cls._returnFileTestFn(job, jobDisk, cached, testDir, 20)
            else:
                RealtimeLogger.info('Expecting cache size of 0 because testfile.test is absent')
                modifiedJobReqs, cached = cls._returnFileTestFn(job, jobDisk, 0, testDir, 20)
                with open(os.path.join(testDir, 'testfile.test'), 'wb') as fH:
                    fH.write(pack('d', cached))
                    RealtimeLogger.info('Wrote cache size of %d to testfile.test', cached)
                os.kill(os.getpid(), signal.SIGKILL)

        @slow
        def testRemoveLocalMutablyReadFile(self):
            """
            If a mutably read file is deleted by the user, it is ok.
            """
            self._deleteLocallyReadFilesFn(readAsMutable=True)

        @slow
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
                    with open(os.path.join(work_dir, str(uuid4())), 'wb') as testFile:
                        testFile.write(os.urandom(1 * 1024 * 1024))
                    fileToDelete = job.fileStore.writeGlobalFile(testFile.name)
                    outfile = testFile.name
                else:
                    break

        @travis_test
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
            with open(os.path.join(work_dir, str(uuid4())), 'wb') as localFile:
                localFile.write(os.urandom(1 * 1024 * 1024))
            localFsID = job.fileStore.writeGlobalFile(localFile.name)
            # write Non-Local File
            with open(os.path.join(nonLocalDir, str(uuid4())), 'wb') as nonLocalFile:
                nonLocalFile.write(os.urandom(1 * 1024 * 1024))
            nonLocalFsID = job.fileStore.writeGlobalFile(nonLocalFile.name)
            # Delete fsid of local file. The file should be deleted
            job.fileStore.deleteLocalFile(localFsID)
            assert not os.path.exists(localFile.name)
            # Delete fsid of non-local file. It should fail and the file should persist
            try:
                job.fileStore.deleteLocalFile(nonLocalFsID)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    # This is supposed to produce an
                    # ENOENT. If it doesn't something is
                    # broken.
                    raise
            else:
                assert False, "Error should have been raised"
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
            # Try to get a non-FileID that doesn't exist.
            try:
                job.fileStore.readGlobalFile('bogus')
            except NoSuchFileException:
                # TODO: We would like to require TypeError, but for Cactus
                # support we have to accept non-FileIDs.
                pass
            else:
                raise RuntimeError("Managed to get a file from a non-FileID")
            # Try to get a FileID for something that doesn't exist
            try:
                job.fileStore.readGlobalFile(FileID('bogus', 4096))
            except NoSuchFileException:
                pass
            else:
                raise RuntimeError("Managed to read a non-existent file")

        @travis_test
        def testSimultaneousReadsUncachedStream(self):
            """
            Test many simultaneous read attempts on a file created via a stream
            directly to the job store.
            """
            self.options.retryCount = 0
            self.options.disableChaining = True

            # Make a file
            parent = Job.wrapJobFn(self._createUncachedFileStream)
            # Now make a bunch of children fight over it
            for i in range(30):
                parent.addChildJobFn(self._readFileWithDelay, parent.rv())

            Job.Runner.startToil(parent, self.options)

        @staticmethod
        def _createUncachedFileStream(job):
            """
            Create and return a FileID for a non-cached file written via a stream.
            """

            messageBytes = 'This is a test file\n'.encode('utf-8')

            with job.fileStore.jobStore.writeFileStream() as (out, idString):
                # Write directly to the job store so the caching file store doesn't even see it.
                # TODO: If we ever change how the caching file store does its IDs we will have to change this.
                out.write(messageBytes)

            # Now make a file ID
            fileID = FileID(idString, len(messageBytes))

            return fileID

        @staticmethod
        def _readFileWithDelay(job, fileID, cores=0.1, memory=50 * 1024 * 1024, disk=50 * 1024 * 1024):
            """
            Read a file from the CachingFileStore with a delay imposed on the download.
            Should create contention.

            Has low requirements so we can run a lot of copies at once.
            """

            # Make sure the file store delays
            # Delay needs to be longer than the timeout for sqlite locking in the file store.
            job.fileStore.forceDownloadDelay = 120

            readStart = datetime.datetime.now()
            logger.debug('Begin read at %s', str(readStart))

            localPath = job.fileStore.readGlobalFile(fileID, cache=True, mutable=True)

            readEnd = datetime.datetime.now()
            logger.debug('End read at %s: took %f seconds', str(readEnd), (readEnd - readStart).total_seconds())

            with open(localPath, 'rb') as fh:
                text = fh.read().decode('utf-8').strip()
            logger.debug('Got file contents: %s', text)



class NonCachingFileStoreTestWithFileJobStore(hidden.AbstractNonCachingFileStoreTest):
    jobStoreType = 'file'

@pytest.mark.timeout(1000)
class CachingFileStoreTestWithFileJobStore(hidden.AbstractCachingFileStoreTest):
    jobStoreType = 'file'


@needs_aws_ec2
class NonCachingFileStoreTestWithAwsJobStore(hidden.AbstractNonCachingFileStoreTest):
    jobStoreType = 'aws'


@slow
@needs_aws_ec2
@pytest.mark.timeout(1000)
class CachingFileStoreTestWithAwsJobStore(hidden.AbstractCachingFileStoreTest):
    jobStoreType = 'aws'


@needs_google
class NonCachingFileStoreTestWithGoogleJobStore(hidden.AbstractNonCachingFileStoreTest):
    jobStoreType = 'google'


@slow
@needs_google
@pytest.mark.timeout(1000)
class CachingFileStoreTestWithGoogleJobStore(hidden.AbstractCachingFileStoreTest):
    jobStoreType = 'google'


def _exportStaticMethodAsGlobalFunctions(cls):
    """
    Define utility functions because Toil can't pickle static methods. Note that this relies on
    the convention that the first argument of a job function is named 'job'.
    """
    for name, kind, clazz, value in inspect.classify_class_attrs(cls):
        if kind == 'static method' and name != '__new__':  # __new__ became static in 3.7
            method = value.__func__
            args = inspect.getfullargspec(method).args
            if args and args[0] == 'job':
                globals()[name] = method


_exportStaticMethodAsGlobalFunctions(hidden.AbstractFileStoreTest)
_exportStaticMethodAsGlobalFunctions(hidden.AbstractCachingFileStoreTest)
_exportStaticMethodAsGlobalFunctions(hidden.AbstractNonCachingFileStoreTest)
