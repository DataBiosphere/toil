# Copyright (C) 2015 UCSC Computational Genomics Lab
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
from Queue import Queue
from abc import abstractmethod, ABCMeta
import hashlib
from itertools import chain, islice
import logging
import os
import urllib2
from threading import Thread
import tempfile
import uuid
import shutil
import time
from unittest import skip
from toil.common import Config
from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             NoSuchFileException)
from toil.jobStores.fileJobStore import FileJobStore
from toil.test import ToilTest, needs_aws, needs_azure, needs_encryption

logger = logging.getLogger(__name__)


# TODO: AWSJobStore does not check the existence of jobs before associating files with them

class hidden:
    """
    Hide abstract base class from unittest's test case loader

    http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class#answer-25695512
    """

    class AbstractJobStoreTest(ToilTest):
        __metaclass__ = ABCMeta

        @classmethod
        def setUpClass(cls):
            super(hidden.AbstractJobStoreTest, cls).setUpClass()
            logging.basicConfig(level=logging.DEBUG)
            logging.getLogger('boto').setLevel(logging.CRITICAL)

        def _createConfig(self):
            return Config()

        @abstractmethod
        def _createJobStore(self, config=None):
            """
            :rtype: AbstractJobStore
            """
            raise NotImplementedError()

        def setUp(self):
            super(hidden.AbstractJobStoreTest, self).setUp()
            self.namePrefix = str(uuid.uuid4())
            self.config = self._createConfig()
            self.master = self._createJobStore(self.config)

        def tearDown(self):
            self.master.deleteJobStore()
            super(hidden.AbstractJobStoreTest, self).tearDown()

        def test(self):
            """
            This is a front-to-back test of the "happy" path in a job store, i.e. covering things
            that occur in the dat to day life of a job store. The purist might insist that this be
            split up into several cases and I agree wholeheartedly.
            """
            master = self.master

            # Test initial state
            #
            self.assertFalse(master.exists('foo'))
            self.assertRaises(NoSuchJobException, master.load, 'foo')

            # Create parent job and verify its existence/properties
            #
            jobOnMaster = master.create('master1', 12, 34, 35)
            self.assertTrue(master.exists(jobOnMaster.jobStoreID))
            self.assertEquals(jobOnMaster.command, 'master1')
            self.assertEquals(jobOnMaster.memory, 12)
            self.assertEquals(jobOnMaster.cores, 34)
            self.assertEquals(jobOnMaster.disk, 35)
            self.assertEquals(jobOnMaster.stack, [])
            self.assertEquals(jobOnMaster.predecessorNumber, 0)
            self.assertEquals(jobOnMaster.predecessorsFinished, set())
            self.assertEquals(jobOnMaster.logJobStoreFileID, None)

            # Create a second instance of the job store, simulating a worker ...
            #
            worker = self._createJobStore()
            # ... and load the parent job there.
            jobOnWorker = worker.load(jobOnMaster.jobStoreID)
            self.assertEquals(jobOnMaster, jobOnWorker)

            # Update state on job
            #
            # The following demonstrates the job update pattern, where files to be deleted are
            # referenced in "filesToDelete" array, which is persisted to disk first. If things go
            # wrong during the update, this list of files to delete is used to remove the
            # unneeded files
            jobOnWorker.filesToDelete = ['1', '2']
            worker.update(jobOnWorker)
            # Check jobs to delete persisted
            self.assertEquals(master.load(jobOnWorker.jobStoreID).filesToDelete, ['1', '2'])
            # Create children    
            child1 = worker.create('child1', 23, 45, 46, 1)
            child2 = worker.create('child2', 34, 56, 57, 1)
            # Update parent
            jobOnWorker.stack.append((
                (child1.jobStoreID, 23, 45, 46, 1),
                (child2.jobStoreID, 34, 56, 57, 1)))
            jobOnWorker.filesToDelete = []
            worker.update(jobOnWorker)

            # Check equivalence between master and worker
            #
            self.assertNotEquals(jobOnWorker, jobOnMaster)
            # Reload parent job on master
            jobOnMaster = master.load(jobOnMaster.jobStoreID)
            self.assertEquals(jobOnWorker, jobOnMaster)
            # Load children on master an check equivalence
            self.assertEquals(master.load(child1.jobStoreID), child1)
            self.assertEquals(master.load(child2.jobStoreID), child2)

            # Test changing and persisting job state across multiple jobs
            #
            childJobs = [worker.load(childCommand[0]) for childCommand in jobOnMaster.stack[-1]]
            for childJob in childJobs:
                childJob.logJobStoreFileID = str(uuid.uuid4())
                childJob.remainingRetryCount = 66
                self.assertNotEquals(childJob, master.load(childJob.jobStoreID))
            for childJob in childJobs:
                worker.update(childJob)
            for childJob in childJobs:
                self.assertEquals(master.load(childJob.jobStoreID), childJob)
                self.assertEquals(worker.load(childJob.jobStoreID), childJob)

            # Test job iterator
            #
            self.assertEquals(set(childJobs + [jobOnMaster]), set(worker.jobs()))
            self.assertEquals(set(childJobs + [jobOnMaster]), set(master.jobs()))

            # Test job deletions
            #
            # First delete parent, this should have no effect on the children
            self.assertTrue(master.exists(jobOnMaster.jobStoreID))
            self.assertTrue(worker.exists(jobOnMaster.jobStoreID))
            master.delete(jobOnMaster.jobStoreID)
            self.assertFalse(master.exists(jobOnMaster.jobStoreID))
            self.assertFalse(worker.exists(jobOnMaster.jobStoreID))

            for childJob in childJobs:
                self.assertTrue(master.exists(childJob.jobStoreID))
                self.assertTrue(worker.exists(childJob.jobStoreID))
                master.delete(childJob.jobStoreID)
                self.assertFalse(master.exists(childJob.jobStoreID))
                self.assertFalse(worker.exists(childJob.jobStoreID))
                self.assertRaises(NoSuchJobException, worker.load, childJob.jobStoreID)
                self.assertRaises(NoSuchJobException, master.load, childJob.jobStoreID)

            # Test job iterator now has no jobs
            #
            self.assertEquals(set(), set(worker.jobs()))
            self.assertEquals(set(), set(master.jobs()))

            try:
                with master.readSharedFileStream('missing') as _:
                    pass
                self.fail('Expecting NoSuchFileException')
            except NoSuchFileException:
                pass

            # Test shared files: Write shared file on master, ...
            #
            with master.writeSharedFileStream('foo') as f:
                f.write('bar')
            # ... read that file on worker, ...
            with worker.readSharedFileStream('foo') as f:
                self.assertEquals('bar', f.read())
            # ... and read it again on master.
            with master.readSharedFileStream('foo') as f:
                self.assertEquals('bar', f.read())

            with master.writeSharedFileStream('nonEncrypted', isProtected=False) as f:
                f.write('bar')
            self.assertUrl(master.getSharedPublicUrl('nonEncrypted'))
            self.assertRaises(NoSuchFileException, master.getSharedPublicUrl, 'missing')

            # Test per-job files: Create empty file on master, ...
            #
            # First recreate job
            jobOnMaster = master.create('master1', 12, 34, 35, 'foo')
            fileOne = worker.getEmptyFileStoreID(jobOnMaster.jobStoreID)
            # Check file exists
            self.assertTrue(worker.fileExists(fileOne))
            self.assertTrue(master.fileExists(fileOne))
            # ... write to the file on worker, ...
            with worker.updateFileStream(fileOne) as f:
                f.write('one')
            # ... read the file as a stream on the master, ....
            with master.readFileStream(fileOne) as f:
                self.assertEquals(f.read(), 'one')

            # ... and copy it to a temporary physical file on the master.
            fh, path = tempfile.mkstemp()
            try:
                os.close(fh)
                master.readFile(fileOne, path)
                with open(path, 'r+') as f:
                    self.assertEquals(f.read(), 'one')
                    # Write a different string to the local file ...
                    f.seek(0)
                    f.truncate(0)
                    f.write('two')
                # ... and create a second file from the local file.
                fileTwo = master.writeFile(path, jobOnMaster.jobStoreID)
                with worker.readFileStream(fileTwo) as f:
                    self.assertEquals(f.read(), 'two')
                # Now update the first file from the local file ...
                master.updateFile(fileOne, path)
                with worker.readFileStream(fileOne) as f:
                    self.assertEquals(f.read(), 'two')
            finally:
                os.unlink(path)
            # Create a third file to test the last remaining method.
            with worker.writeFileStream(jobOnMaster.jobStoreID) as (f, fileThree):
                f.write('three')
            with master.readFileStream(fileThree) as f:
                self.assertEquals(f.read(), 'three')
            # Delete a file explicitly but leave files for the implicit deletion through the parent
            worker.deleteFile(fileOne)

            # Check the file is gone
            #
            for store in worker, master:
                self.assertFalse(store.fileExists(fileOne))
                self.assertRaises(NoSuchFileException, store.readFile, fileOne, '')
                try:
                    with store.readFileStream(fileOne) as _:
                        pass
                    self.fail('Expecting NoSuchFileException')
                except NoSuchFileException:
                    pass

            # Test stats and logging
            #
            stats = None

            def callback(f2):
                stats.add(f2.read())

            stats = set()
            self.assertEquals(0, master.readStatsAndLogging(callback))
            self.assertEquals(set(), stats)
            master.writeStatsAndLogging('1')
            self.assertEquals(1, master.readStatsAndLogging(callback))
            self.assertEquals({'1'}, stats)
            self.assertEquals(0, master.readStatsAndLogging(callback))
            master.writeStatsAndLogging('1')
            master.writeStatsAndLogging('2')
            stats = set()
            self.assertEquals(2, master.readStatsAndLogging(callback))
            self.assertEquals({'1', '2'}, stats)
            largeLogEntry = os.urandom(self._largeLogEntrySize())
            stats = set()
            master.writeStatsAndLogging(largeLogEntry)
            self.assertEquals(1, master.readStatsAndLogging(callback))
            self.assertEquals({largeLogEntry}, stats)

            # Delete parent and its associated files
            #
            master.delete(jobOnMaster.jobStoreID)
            self.assertFalse(master.exists(jobOnMaster.jobStoreID))
            # Files should be gone as well. NB: the fooStream() methods return context managers
            self.assertRaises(NoSuchFileException, worker.readFileStream(fileTwo).__enter__)
            self.assertRaises(NoSuchFileException, worker.readFileStream(fileThree).__enter__)

            # TODO: Who deletes the shared files?

        def testFileDeletion(self):
            """
            Intended to cover the batch deletion of items in the AWSJobStore, but it doesn't hurt
            running it on the other job stores.
            """
            master = self.master
            n = self._batchDeletionSize()
            for numFiles in (1, n - 1, n, n + 1, 2 * n):
                job = master.create('1', 2, 3, 4, 0)
                fileIDs = [master.getEmptyFileStoreID(job.jobStoreID) for _ in xrange(0, numFiles)]
                master.delete(job.jobStoreID)
                for fileID in fileIDs:
                    self.assertRaises(NoSuchFileException, master.readFileStream(fileID).__enter__)

        def testMultipartUploads(self):
            """
            This test is meant to cover multi-part uploads in the AWSJobStore but it doesn't hurt
            running it against the other job stores as well.
            """
            # Should not block. On Linux, /dev/random blocks when its running low on entropy
            random_device = '/dev/urandom'
            # http://unix.stackexchange.com/questions/11946/how-big-is-the-pipe-buffer
            bufSize = 65536
            partSize = self._partSize()
            self.assertEquals(partSize % bufSize, 0)
            job = self.master.create('1', 2, 3, 4, 0)

            # Test file/stream ending on part boundary and within a part
            #
            for partsPerFile in (1, 2.33):
                checksum = hashlib.md5()
                checksumQueue = Queue(2)

                # FIXME: Having a separate thread is probably overkill here

                def checksumThreadFn():
                    while True:
                        _buf = checksumQueue.get()
                        if _buf is None:
                            break
                        checksum.update(_buf)

                # Multipart upload from stream
                #
                checksumThread = Thread(target=checksumThreadFn)
                checksumThread.start()
                try:
                    with open(random_device) as readable:
                        with self.master.writeFileStream(job.jobStoreID) as (writable, fileId):
                            for i in range(int(partSize * partsPerFile / bufSize)):
                                buf = readable.read(bufSize)
                                checksumQueue.put(buf)
                                writable.write(buf)
                finally:
                    checksumQueue.put(None)
                    checksumThread.join()
                before = checksum.hexdigest()

                # Verify
                #
                checksum = hashlib.md5()
                with self.master.readFileStream(fileId) as readable:
                    while True:
                        buf = readable.read(bufSize)
                        if not buf:
                            break
                        checksum.update(buf)
                after = checksum.hexdigest()
                self.assertEquals(before, after)

                # Multi-part upload from file
                #
                checksum = hashlib.md5()
                fh, path = tempfile.mkstemp()
                try:
                    with os.fdopen(fh, 'r+') as writable:
                        with open(random_device) as readable:
                            for i in range(int(partSize * partsPerFile / bufSize)):
                                buf = readable.read(bufSize)
                                writable.write(buf)
                                checksum.update(buf)
                    fileId = self.master.writeFile(path, job.jobStoreID)
                finally:
                    os.unlink(path)
                before = checksum.hexdigest()

                # Verify
                #
                checksum = hashlib.md5()
                with self.master.readFileStream(fileId) as readable:
                    while True:
                        buf = readable.read(bufSize)
                        if not buf:
                            break
                        checksum.update(buf)
                after = checksum.hexdigest()
                self.assertEquals(before, after)
            self.master.delete(job.jobStoreID)

        def testZeroLengthFiles(self):
            job = self.master.create('1', 2, 3, 4, 0)
            nullFile = self.master.writeFile('/dev/null', job.jobStoreID)
            with self.master.readFileStream(nullFile) as f:
                self.assertEquals(f.read(), "")
            with self.master.writeFileStream(job.jobStoreID) as (f, nullStream):
                pass
            with self.master.readFileStream(nullStream) as f:
                self.assertEquals(f.read(), "")
            self.master.delete(job.jobStoreID)

        def testLargeFile(self):
            dirPath = self._createTempDir()
            filePath = os.path.join(dirPath, 'large')
            hashIn = hashlib.md5()
            with open(filePath, 'w') as f:
                for i in xrange(0, 10):
                    buf = os.urandom(self._partSize())
                    f.write(buf)
                    hashIn.update(buf)
            job = self.master.create('1', 2, 3, 4, 0)
            jobStoreFileID = self.master.writeFile(filePath, job.jobStoreID)
            os.unlink(filePath)
            self.master.readFile(jobStoreFileID, filePath)
            hashOut = hashlib.md5()
            with open(filePath, 'r') as f:
                while True:
                    buf = f.read(self._partSize())
                    if not buf: break
                    hashOut.update(buf)
            self.assertEqual(hashIn.digest(), hashOut.digest())

        def assertUrl(self, url):
            prefix, path = url.split(':', 1)
            if prefix == 'file':
                self.assertTrue(os.path.exists(path))
            else:
                try:
                    urllib2.urlopen(urllib2.Request(url))
                except:
                    self.fail()
                    
        def testCleanCache(self):
            # Make a bunch of jobs
            master = self.master

            # Create parent job
            rootJob = master.create('rootjob', 12, 34, 35)
            
            # Create a bunch of child jobs
            for i in range(100):
                child = master.create("child%s" % i, 23, 45, 46, 1)
                rootJob.stack.append(((child.jobStoreID, 23, 45, 46, 1),))
            master.update(rootJob)
            
            # See how long it takes to clean with no cache
            noCacheStart = time.time()
            master.clean(rootJob)
            noCacheEnd = time.time()
            
            noCacheTime = noCacheEnd - noCacheStart
            
            # See how long it takes to clean with cache
            jobCache = {jobWrapper.jobStoreID: jobWrapper
                        for jobWrapper in master.jobs()}
            cacheStart = time.time()
            master.clean(rootJob, jobCache=jobCache)
            cacheEnd = time.time()
            
            cacheTime = cacheEnd - cacheStart

            logger.info("Without cache: %f, with cache: %f.", noCacheTime, cacheTime)

            # Running with the cache should be faster.
            self.assertTrue(cacheTime <= noCacheTime)
            
        @skip("too slow") # This takes a long time on the remote JobStores
        def testManyJobs(self):
            # Make sure we can store large numbers of jobs
            
            # Make a bunch of jobs
            master = self.master

            # Create parent job
            rootJob = master.create('rootjob', 12, 34, 35)
            
            # Create a bunch of child jobs
            for i in range(3000):
                child = master.create("child%s" % i, 23, 45, 46, 1)
                rootJob.stack.append(((child.jobStoreID, 23, 45, 46, 1),))
            master.update(rootJob)
            
            # Pull them all back out again
            allJobs = list(master.jobs())
            
            # Make sure we have the right number of jobs
            self.assertEquals(len(allJobs), 3001)

        # Sub-classes may want to override these in order to maximize test coverage

        def _largeLogEntrySize(self):
            return 1 * 1024 * 1024

        def _batchDeletionSize(self):
            return 10

        def _partSize(self):
            return 5 * 1024 * 1024

    class AbstractEncryptedJobStoreTest(AbstractJobStoreTest):
        """
        A mixin test case that creates a job store that encrypts files stored within it
        """
        __metaclass__ = ABCMeta

        def setUp(self):
            self.sseKeyDir = tempfile.mkdtemp()
            self.cseKeyDir = tempfile.mkdtemp()
            super(hidden.AbstractEncryptedJobStoreTest, self).setUp()

        def tearDown(self):
            super(hidden.AbstractEncryptedJobStoreTest, self).tearDown()
            shutil.rmtree(self.sseKeyDir)
            shutil.rmtree(self.cseKeyDir)

        def _createConfig(self):
            config = super(hidden.AbstractEncryptedJobStoreTest, self)._createConfig()
            sseKeyFile = os.path.join(self.sseKeyDir, 'keyFile')
            with open(sseKeyFile, 'w') as f:
                f.write('01234567890123456789012345678901')
            config.sseKey = sseKeyFile
            # config.attrib['sse_key'] = sseKeyFile

            cseKeyFile = os.path.join(self.cseKeyDir, 'keyFile')
            with open(cseKeyFile, 'w') as f:
                f.write("i am a fake key, so don't use me")
            config.cseKey = cseKeyFile
            return config


class FileJobStoreTest(hidden.AbstractJobStoreTest):
    def _createJobStore(self, config=None):
        return FileJobStore(self.namePrefix, config=config)


@needs_aws
class AWSJobStoreTest(hidden.AbstractJobStoreTest):
    testRegion = 'us-west-2'

    def _createJobStore(self, config=None):
        from toil.jobStores.aws.jobStore import AWSJobStore
        partSize = self._partSize()
        for encrypted in (True, False):
            self.assertTrue(AWSJobStore.FileInfo.maxInlinedSize(encrypted) < partSize)
        AWSJobStore.FileInfo.s3PartSize = partSize
        return AWSJobStore.createJobStore(self.testRegion + ':' + self.namePrefix, config=config)

    def testInlinedFiles(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        master = self.master
        for encrypted in (True, False):
            n = AWSJobStore.FileInfo.maxInlinedSize(encrypted)
            sizes = (1, n / 2, n - 1, n, n + 1, 2 * n)
            for size in chain(sizes, islice(reversed(sizes), 1)):
                s = os.urandom(size)
                with master.writeSharedFileStream('foo') as f:
                    f.write(s)
                with master.readSharedFileStream('foo') as f:
                    self.assertEqual(s, f.read())

    def _largeLogEntrySize(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        # So we get into the else branch of reader() in uploadStream(multiPart=False):
        return AWSJobStore.FileInfo.maxBinarySize() * 2

    def _batchDeletionSize(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        return AWSJobStore.itemsPerBatchDelete


@needs_aws
class InvalidAWSJobStoreTest(ToilTest):
    def testInvalidJobStoreName(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        self.assertRaises(ValueError,
                          AWSJobStore.createJobStore,
                          'us-west-2:a--b')
        self.assertRaises(ValueError,
                          AWSJobStore.createJobStore,
                          'us-west-2:' + ('a' * 100))
        self.assertRaises(ValueError,
                          AWSJobStore.createJobStore,
                          'us-west-2:a_b')


@needs_azure
class AzureJobStoreTest(hidden.AbstractJobStoreTest):
    def _createJobStore(self, config=None):
        from toil.jobStores.azureJobStore import AzureJobStore
        return AzureJobStore('toiltest', self.namePrefix, config=config)

    def _partSize(self):
        from toil.jobStores.azureJobStore import AzureJobStore
        return AzureJobStore._maxAzureBlockBytes

    def testLargeJob(self):
        from toil.jobStores.azureJobStore import maxAzureTablePropertySize
        command = os.urandom(maxAzureTablePropertySize * 2)
        job1 = self.master.create(command=command, memory=0, cores=0, disk=0)
        self.assertEqual(job1.command, command)
        job2 = self.master.load(job1.jobStoreID)
        self.assertIsNot(job1, job2)
        self.assertEqual(job2.command, command)


class EncryptedFileJobStoreTest(FileJobStoreTest, hidden.AbstractEncryptedJobStoreTest):
    pass


@needs_aws
@needs_encryption
class EncryptedAWSJobStoreTest(AWSJobStoreTest, hidden.AbstractEncryptedJobStoreTest):
    pass


@needs_azure
@needs_encryption
class EncryptedAzureJobStoreTest(AzureJobStoreTest, hidden.AbstractEncryptedJobStoreTest):
    pass
