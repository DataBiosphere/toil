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

import hashlib
import http
import logging
import os
import shutil
import socketserver
import tempfile
import threading
import time
import urllib.parse as urlparse
import uuid
from abc import ABCMeta, abstractmethod
from io import BytesIO
from itertools import chain, islice
from queue import Queue
from threading import Thread
from typing import Any, Tuple
from urllib.request import Request, urlopen

import pytest
from stubserver import FTPStubServer

from toil.common import Config, Toil
from toil.fileStores import FileID
from toil.job import Job, JobDescription, TemporaryID
from toil.jobStores.abstractJobStore import (NoSuchFileException,
                                             NoSuchJobException)
from toil.jobStores.fileJobStore import FileJobStore
from toil.lib.aws.utils import create_s3_bucket, get_object_for_url
from toil.lib.memoize import memoize
from toil.lib.retry import retry
from toil.statsAndLogging import StatsAndLogging
from toil.test import (ToilTest,
                       make_tests,
                       needs_aws_s3,
                       needs_encryption,
                       needs_google_project,
                       needs_google_storage,
                       slow)

# noinspection PyPackageRequirements
# (installed by `make prepare`)

try:
    from toil.jobStores.googleJobStore import google_retry
except ImportError:
    # Need google_retry decorator even if google is not available, so make one up.
    def google_retry(x):
        return x


logger = logging.getLogger(__name__)


def tearDownModule():
    AbstractJobStoreTest.Test.cleanUpExternalStores()


class AbstractJobStoreTest:
    """
    Hide abstract base class from unittest's test case loader

    http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class#answer-25695512
    """

    class Test(ToilTest, metaclass=ABCMeta):
        @classmethod
        def setUpClass(cls):
            super().setUpClass()
            logging.basicConfig(level=logging.DEBUG)
            logging.getLogger('boto').setLevel(logging.CRITICAL)
            logging.getLogger('boto').setLevel(logging.WARNING)
            logging.getLogger('boto3.resources').setLevel(logging.WARNING)
            logging.getLogger('botocore.auth').setLevel(logging.WARNING)
            logging.getLogger('botocore.hooks').setLevel(logging.WARNING)

        # The use of @memoize ensures that we only have one instance of per class even with the
        # generative import/export tests attempts to instantiate more. This in turn enables us to
        # share the external stores (buckets, blob store containers, local directory, etc.) used
        # for testing import export. While the constructor arguments are included in the
        # memoization key, I have only ever seen one case: ('test', ). The worst that can happen
        # if other values are also used is that there will be more external stores and less sharing
        # of them. They will still all be cleaned-up.
        @classmethod
        @memoize
        def __new__(cls, *args):
            return super().__new__(cls)

        def _createConfig(self):
            return Config()

        @abstractmethod
        def _createJobStore(self):
            """
            :rtype: AbstractJobStore
            """
            raise NotImplementedError()

        def setUp(self):
            super().setUp()
            self.namePrefix = 'jobstore-test-' + str(uuid.uuid4())
            self.config = self._createConfig()

            # Jobstores to be used in testing.
            # jobstore_initialized is created with a particular configuration, as creating by self._createConfig()
            # jobstore_resume_noconfig is created with the resume() method. resume() will look for a previously
            # instantiated jobstore, and initialize the jobstore calling it with the found config. In this case,
            # jobstore_resume_noconfig will be initialized with the config from jobstore_initialized.
            self.jobstore_initialized = self._createJobStore()
            self.jobstore_initialized.initialize(self.config)
            self.jobstore_resumed_noconfig = self._createJobStore()
            self.jobstore_resumed_noconfig.resume()

            # Requirements for jobs to be created.
            self.arbitraryRequirements = {'memory': 1, 'disk': 2, 'cores': 1, 'preemptible': False}
            # Function to make an arbitrary new job
            self.arbitraryJob = lambda: JobDescription(command='command',
                                                       jobName='arbitrary',
                                                       requirements=self.arbitraryRequirements)

            self.parentJobReqs = dict(memory=12, cores=34, disk=35, preemptible=True)
            self.childJobReqs1 = dict(memory=23, cores=45, disk=46, preemptible=True)
            self.childJobReqs2 = dict(memory=34, cores=56, disk=57, preemptible=False)

        def tearDown(self):
            self.jobstore_initialized.destroy()
            self.jobstore_resumed_noconfig.destroy()
            super().tearDown()

        def testInitialState(self):
            """Ensure proper handling of nonexistant files."""
            self.assertFalse(self.jobstore_initialized.job_exists('nonexistantFile'))
            self.assertRaises(NoSuchJobException, self.jobstore_initialized.load_job, 'nonexistantFile')

        def testJobCreation(self):
            """
            Test creation of a job.

            Does the job exist in the jobstore it is supposed to be in?
            Are its attributes what is expected?
            """

            jobstore = self.jobstore_initialized

            # Create a job and verify its existence/properties
            job = JobDescription(command='parent1',
                                 requirements=self.parentJobReqs,
                                 jobName='test1', unitName='onParent')
            self.assertTrue(isinstance(job.jobStoreID, TemporaryID))
            jobstore.assign_job_id(job)
            self.assertFalse(isinstance(job.jobStoreID, TemporaryID))
            created = jobstore.create_job(job)

            self.assertEqual(created, job)

            self.assertTrue(jobstore.job_exists(job.jobStoreID))
            self.assertEqual(job.command, 'parent1')
            self.assertEqual(job.memory, self.parentJobReqs['memory'])
            self.assertEqual(job.cores, self.parentJobReqs['cores'])
            self.assertEqual(job.disk, self.parentJobReqs['disk'])
            self.assertEqual(job.preemptible, self.parentJobReqs['preemptible'])
            self.assertEqual(job.jobName, 'test1')
            self.assertEqual(job.unitName, 'onParent')

        def testConfigEquality(self):
            """
            Ensure that the command line configurations are successfully loaded and stored.

            In setUp() self.jobstore1 is created and initialized. In this test,  after creating newJobStore,
            .resume() will look for a previously instantiated job store and load its config options. This is expected
            to be equal but not the same object.
            """
            newJobStore = self._createJobStore()
            newJobStore.resume()
            self.assertEqual(newJobStore.config, self.config)
            self.assertIsNot(newJobStore.config, self.config)

        def testJobLoadEquality(self):
            """Tests that a job created via one JobStore instance can be loaded from another."""

            # Create a job on the first jobstore.
            jobDesc1 = JobDescription(command='jobstore1',
                                      requirements=self.parentJobReqs,
                                      jobName='test1', unitName='onJS1')
            self.jobstore_initialized.assign_job_id(jobDesc1)
            self.jobstore_initialized.create_job(jobDesc1)

            # Load it from the second jobstore
            jobDesc2 = self.jobstore_resumed_noconfig.load_job(jobDesc1.jobStoreID)

            self.assertEqual(jobDesc1.command, jobDesc2.command)

        def testChildLoadingEquality(self):
            """Test that loading a child job operates as expected."""
            job = JobDescription(command='parent1',
                                 requirements=self.parentJobReqs,
                                 jobName='test1', unitName='onParent')

            childJob = JobDescription(command='child1',
                                      requirements=self.childJobReqs1,
                                      jobName='test2', unitName='onChild1')
            self.jobstore_initialized.assign_job_id(job)
            self.jobstore_initialized.assign_job_id(childJob)
            self.jobstore_initialized.create_job(job)
            self.jobstore_initialized.create_job(childJob)
            job.addChild(childJob.jobStoreID)
            self.jobstore_initialized.update_job(job)

            self.assertEqual(self.jobstore_initialized.load_job(list(job.allSuccessors())[0]).command, childJob.command)

        def testPersistantFilesToDelete(self):
            """
            Make sure that updating a job carries over filesToDelete.

            The following demonstrates the job update pattern, where files to be deleted are referenced in
            "filesToDelete" array, which is persisted to disk first. If things go wrong during the update, this list of
            files to delete is used to remove the unneeded files.
            """

            # Create a job.
            job = JobDescription(command='job1',
                                 requirements=self.parentJobReqs,
                                 jobName='test1', unitName='onJS1')

            self.jobstore_initialized.assign_job_id(job)
            self.jobstore_initialized.create_job(job)
            job.filesToDelete = ['1', '2']
            self.jobstore_initialized.update_job(job)
            self.assertEqual(self.jobstore_initialized.load_job(job.jobStoreID).filesToDelete, ['1', '2'])

        def testUpdateBehavior(self):
            """Tests the proper behavior during updating jobs."""
            jobstore1 = self.jobstore_initialized
            jobstore2 = self.jobstore_resumed_noconfig

            job1 = JobDescription(command='parent1',
                                 requirements=self.parentJobReqs,
                                 jobName='test1', unitName='onParent')

            childJob1 = JobDescription(command='child1',
                                      requirements=self.childJobReqs1,
                                      jobName='test2', unitName='onChild1')

            childJob2 = JobDescription(command='child2',
                                      requirements=self.childJobReqs2,
                                      jobName='test3', unitName='onChild2')

            jobstore1.assign_job_id(job1)
            jobstore1.create_job(job1)
            job2 = jobstore2.load_job(job1.jobStoreID)

            # Create child jobs.
            jobstore2.assign_job_id(childJob1)
            jobstore2.create_job(childJob1)
            jobstore2.assign_job_id(childJob2)
            jobstore2.create_job(childJob2)

            # Add them to job2.
            job2.addChild(childJob1.jobStoreID)
            job2.addChild(childJob2.jobStoreID)
            jobstore2.update_job(job2)

            # Check equivalence between jobstore1 and jobstore2.
            # While job1 and job2 share a jobStoreID, job1 has not been "refreshed" to show the newly added child jobs.
            self.assertNotEqual(sorted(job2.allSuccessors()), sorted(job1.allSuccessors()))

            # Reload parent job on jobstore, "refreshing" the job.
            job1 = jobstore1.load_job(job1.jobStoreID)
            self.assertEqual(sorted(job2.allSuccessors()), sorted(job1.allSuccessors()))

            # Jobs still shouldn't *actually* be equal, even if their contents are the same.
            self.assertNotEqual(job2, job1)

            # Load children on jobstore and check against equivalence
            self.assertNotEqual(jobstore1.load_job(childJob1.jobStoreID), childJob1)
            self.assertNotEqual(jobstore1.load_job(childJob2.jobStoreID), childJob2)

        def testJobDeletions(self):
            """Tests the consequences of deleting jobs."""
            # A local jobstore object for testing.
            jobstore = self.jobstore_initialized
            job = JobDescription(command='job1',
                                 requirements=self.parentJobReqs,
                                 jobName='test1', unitName='onJob')
            # Create job
            jobstore.assign_job_id(job)
            jobstore.create_job(job)

            # Create child Jobs
            child1 = JobDescription(command='child1',
                                    requirements=self.childJobReqs1,
                                    jobName='test2', unitName='onChild1')

            child2 = JobDescription(command='job1',
                                    requirements=self.childJobReqs2,
                                    jobName='test3', unitName='onChild2')

            # Add children to parent.
            jobstore.assign_job_id(child1)
            jobstore.create_job(child1)
            jobstore.assign_job_id(child2)
            jobstore.create_job(child2)
            job.addChild(child1.jobStoreID)
            job.addChild(child2.jobStoreID)
            jobstore.update_job(job)

            # Get it ready to run children
            job.command = None
            jobstore.update_job(job)

            # Go get the children
            childJobs = [jobstore.load_job(childID) for childID in job.nextSuccessors()]

            # Test job iterator - the results of the iterator are effected by eventual
            # consistency. We cannot guarantee all jobs will appear but we can assert that all
            # jobs that show up are a subset of all existing jobs. If we had deleted jobs before
            # this we would have to worry about ghost jobs appearing and this assertion would not
            # be valid
            self.assertTrue({j.jobStoreID for j in (childJobs + [job])} >= {j.jobStoreID for j in jobstore.jobs()})

            # Test job deletions
            # First delete parent, this should have no effect on the children
            self.assertTrue(jobstore.job_exists(job.jobStoreID))
            jobstore.delete_job(job.jobStoreID)
            self.assertFalse(jobstore.job_exists(job.jobStoreID))

            # Check the deletion of children
            for childJob in childJobs:
                self.assertTrue(jobstore.job_exists(childJob.jobStoreID))
                jobstore.delete_job(childJob.jobStoreID)
                self.assertFalse(jobstore.job_exists(childJob.jobStoreID))
                self.assertRaises(NoSuchJobException, jobstore.load_job, childJob.jobStoreID)

            try:
                with jobstore.read_shared_file_stream('missing') as _:
                    pass
                self.fail('Expecting NoSuchFileException')
            except NoSuchFileException:
                pass

        def testSharedFiles(self):
            """Tests the sharing of files."""
            jobstore1 = self.jobstore_initialized
            jobstore2 = self.jobstore_resumed_noconfig

            bar = b'bar'

            with jobstore1.write_shared_file_stream('foo') as f:
                f.write(bar)
            # ... read that file on worker, ...
            with jobstore2.read_shared_file_stream('foo') as f:
                self.assertEqual(bar, f.read())
            # ... and read it again on jobstore1.
            with jobstore1.read_shared_file_stream('foo') as f:
                self.assertEqual(bar, f.read())

            with jobstore1.write_shared_file_stream('nonEncrypted', encrypted=False) as f:
                f.write(bar)
            self.assertUrl(jobstore1.get_shared_public_url('nonEncrypted'))
            self.assertRaises(NoSuchFileException, jobstore1.get_shared_public_url, 'missing')

        def testReadWriteSharedFilesTextMode(self):
            """Checks if text mode is compatible for shared file streams."""
            jobstore1 = self.jobstore_initialized
            jobstore2 = self.jobstore_resumed_noconfig

            bar = 'bar'

            with jobstore1.write_shared_file_stream('foo', encoding='utf-8') as f:
                f.write(bar)

            with jobstore2.read_shared_file_stream('foo', encoding='utf-8') as f:
                self.assertEqual(bar, f.read())

            with jobstore1.read_shared_file_stream('foo', encoding='utf-8') as f:
                self.assertEqual(bar, f.read())

        def testReadWriteFileStreamTextMode(self):
            """Checks if text mode is compatible for file streams."""
            jobstore = self.jobstore_initialized
            job = self.arbitraryJob()
            jobstore.assign_job_id(job)
            jobstore.create_job(job)

            foo = 'foo'
            bar = 'bar'

            with jobstore.write_file_stream(job.jobStoreID, encoding='utf-8') as (f, fileID):
                f.write(foo)

            with jobstore.read_file_stream(fileID, encoding='utf-8') as f:
                self.assertEqual(foo, f.read())

            with jobstore.update_file_stream(fileID, encoding='utf-8') as f:
                f.write(bar)

            with jobstore.read_file_stream(fileID, encoding='utf-8') as f:
                self.assertEqual(bar, f.read())

        def testPerJobFiles(self):
            """Tests the behavior of files on jobs."""
            jobstore1 = self.jobstore_initialized
            jobstore2 = self.jobstore_resumed_noconfig

            # Create jobNodeOnJS1
            jobOnJobStore1 = JobDescription(command='job1',
                                            requirements=self.parentJobReqs,
                                            jobName='test1', unitName='onJobStore1')

            # First recreate job
            jobstore1.assign_job_id(jobOnJobStore1)
            jobstore1.create_job(jobOnJobStore1)
            fileOne = jobstore2.get_empty_file_store_id(jobOnJobStore1.jobStoreID, cleanup=True)
            # Check file exists
            self.assertTrue(jobstore2.file_exists(fileOne))
            self.assertTrue(jobstore1.file_exists(fileOne))
            one = b'one'
            two = b'two'
            three = b'three'
            # ... write to the file on jobstore2, ...
            with jobstore2.update_file_stream(fileOne) as f:
                f.write(one)
            # ... read the file as a stream on the jobstore1, ....
            with jobstore1.read_file_stream(fileOne) as f:
                self.assertEqual(f.read(), one)

            # ... and copy it to a temporary physical file on the jobstore1.
            fh, path = tempfile.mkstemp()
            try:
                os.close(fh)
                tmpPath = path + '.read-only'
                jobstore1.read_file(fileOne, tmpPath)
                try:
                    shutil.copyfile(tmpPath, path)
                finally:
                    os.unlink(tmpPath)
                with open(path, 'rb+') as f:
                    self.assertEqual(f.read(), one)
                    # Write a different string to the local file ...
                    f.seek(0)
                    f.truncate(0)
                    f.write(two)
                # ... and create a second file from the local file.
                fileTwo = jobstore1.write_file(path, jobOnJobStore1.jobStoreID, cleanup=True)
                with jobstore2.read_file_stream(fileTwo) as f:
                    self.assertEqual(f.read(), two)
                # Now update the first file from the local file ...
                jobstore1.update_file(fileOne, path)
                with jobstore2.read_file_stream(fileOne) as f:
                    self.assertEqual(f.read(), two)
            finally:
                os.unlink(path)
            # Create a third file to test the last remaining method.
            with jobstore2.write_file_stream(jobOnJobStore1.jobStoreID, cleanup=True) as (f, fileThree):
                f.write(three)
            with jobstore1.read_file_stream(fileThree) as f:
                self.assertEqual(f.read(), three)
            # Delete a file explicitly but leave files for the implicit deletion through the parent
            jobstore2.delete_file(fileOne)

            # Check the file is gone
            #
            for store in jobstore2, jobstore1:
                self.assertFalse(store.file_exists(fileOne))
                self.assertRaises(NoSuchFileException, store.read_file, fileOne, '')
                try:
                    with store.read_file_stream(fileOne) as _:
                        pass
                    self.fail('Expecting NoSuchFileException')
                except NoSuchFileException:
                    pass

        def testStatsAndLogging(self):
            """Tests behavior of reading and writting stats and logging."""
            jobstore1 = self.jobstore_initialized
            jobstore2 = self.jobstore_resumed_noconfig

            jobOnJobStore1 = JobDescription(command='job1',
                                            requirements=self.parentJobReqs,
                                            jobName='test1', unitName='onJobStore1')

            jobstore1.assign_job_id(jobOnJobStore1)
            jobstore1.create_job(jobOnJobStore1)

            # Test stats and logging
            stats = None
            one = b'one'
            two = b'two'

            # Allows stats to be read/written to/from in read/writeStatsAndLogging.
            def callback(f2):
                stats.add(f2.read())

            # Collects stats and logging messages.
            stats = set()

            # No stats or logging added yet. Expect nothing.
            self.assertEqual(0, jobstore1.read_logs(callback))
            self.assertEqual(set(), stats)

            # Test writing and reading.
            jobstore2.write_logs(one)
            self.assertEqual(1, jobstore1.read_logs(callback))
            self.assertEqual({one}, stats)
            self.assertEqual(0, jobstore1.read_logs(callback))  # read_logs purges saved stats etc

            jobstore2.write_logs(one)
            jobstore2.write_logs(two)
            stats = set()
            self.assertEqual(2, jobstore1.read_logs(callback))
            self.assertEqual({one, two}, stats)

            largeLogEntry = os.urandom(self._largeLogEntrySize())
            stats = set()
            jobstore2.write_logs(largeLogEntry)
            self.assertEqual(1, jobstore1.read_logs(callback))
            self.assertEqual({largeLogEntry}, stats)

            # test the readAll parameter
            self.assertEqual(4, jobstore1.read_logs(callback, read_all=True))

            # Delete parent
            jobstore1.delete_job(jobOnJobStore1.jobStoreID)
            self.assertFalse(jobstore1.job_exists(jobOnJobStore1.jobStoreID))
            # TODO: Who deletes the shared files?

        def testWriteLogFiles(self):
            """Test writing log files."""
            jobNames = ['testStatsAndLogging_writeLogFiles']
            jobLogList = ['string', b'bytes', '', b'newline\n']
            config = self._createConfig()
            setattr(config, 'writeLogs', '.')
            setattr(config, 'writeLogsGzip', None)
            StatsAndLogging.writeLogFiles(jobNames, jobLogList, config)
            jobLogFile = os.path.join(config.writeLogs, jobNames[0] + '000.log')
            self.assertTrue(os.path.isfile(jobLogFile))
            with open(jobLogFile) as f:
                self.assertEqual(f.read(), 'string\nbytes\n\nnewline\n')
            os.remove(jobLogFile)

        def testBatchCreate(self):
            """Test creation of many jobs."""
            jobstore = self.jobstore_initialized
            jobRequirements = dict(memory=12, cores=34, disk=35, preemptible=True)
            jobs = []
            with jobstore.batch():
                for i in range(100):
                    overlargeJob = JobDescription(command='overlarge',
                                                  requirements=jobRequirements,
                                                  jobName='test-overlarge', unitName='onJobStore')
                    jobstore.assign_job_id(overlargeJob)
                    jobstore.create_job(overlargeJob)
                    jobs.append(overlargeJob)
            for job in jobs:
                self.assertTrue(jobstore.job_exists(job.jobStoreID))

        def testGrowingAndShrinkingJob(self):
            """Make sure jobs update correctly if they grow/shrink."""
            # Make some very large data, large enough to trigger
            # overlarge job creation if that's a thing
            # (i.e. AWSJobStore)
            arbitraryLargeData = os.urandom(500000)
            job = self.arbitraryJob()
            self.jobstore_initialized.assign_job_id(job)
            self.jobstore_initialized.create_job(job)
            # Make the job grow
            job.foo_attribute = arbitraryLargeData
            self.jobstore_initialized.update_job(job)
            check_job = self.jobstore_initialized.load_job(job.jobStoreID)
            self.assertEqual(check_job.foo_attribute, arbitraryLargeData)
            # Make the job shrink back close to its original size
            job.foo_attribute = None
            self.jobstore_initialized.update_job(job)
            check_job = self.jobstore_initialized.load_job(job.jobStoreID)
            self.assertEqual(check_job.foo_attribute, None)

        def _prepareTestFile(self, store, size=None):
            """
            Generates a URL that can be used to point at a test file in the storage mechanism
            used by the job store under test by this class. Optionally creates a file at that URL.

            :param: store: an object referencing the store, same type as _createExternalStore's
                    return value

            :param int size: The size of the test file to be created.

            :return: the URL, or a tuple (url, md5) where md5 is the file's hexadecimal MD5 digest

            :rtype: str|(str,str)
            """
            raise NotImplementedError()

        @abstractmethod
        def _hashTestFile(self, url):
            """
            Returns hexadecimal MD5 digest of the contents of the file pointed at by the URL.
            """
            raise NotImplementedError()

        @abstractmethod
        def _createExternalStore(self):
            raise NotImplementedError()

        @abstractmethod
        def _cleanUpExternalStore(self, store):
            """
            :param: store: an object referencing the store, same type as _createExternalStore's
                    return value
            """
            raise NotImplementedError()

        externalStoreCache = {}

        def _externalStore(self):
            try:
                store = self.externalStoreCache[self]
            except KeyError:
                logger.debug('Creating new external store for %s', self)
                store = self.externalStoreCache[self] = self._createExternalStore()
            else:
                logger.debug('Reusing external store for %s', self)
            return store

        @classmethod
        def cleanUpExternalStores(cls):
            for test, store in cls.externalStoreCache.items():
                logger.debug('Cleaning up external store for %s.', test)
                test._cleanUpExternalStore(store)

        mpTestPartSize = 5 << 20

        @classmethod
        def makeImportExportTests(cls):

            testClasses = [FileJobStoreTest, AWSJobStoreTest, GoogleJobStoreTest]

            activeTestClassesByName = {testCls.__name__: testCls
                                       for testCls in testClasses
                                       if not getattr(testCls, '__unittest_skip__', False)}

            def testImportExportFile(self, otherCls, size, moveExports):
                """
                :param AbstractJobStoreTest.Test self: the current test case

                :param AbstractJobStoreTest.Test otherCls: the test case class for the job store
                       to import from or export to

                :param int size: the size of the file to test importing/exporting with
                """
                # Prepare test file in other job store
                self.jobstore_initialized.partSize = cls.mpTestPartSize
                self.jobstore_initialized.moveExports = moveExports

                # Test assumes imports are not linked
                self.jobstore_initialized.linkImports = False

                # The string in otherCls() is arbitrary as long as it returns a class that has access
                # to ._externalStore() and ._prepareTestFile()
                other = otherCls('testSharedFiles')
                store = other._externalStore()

                srcUrl, srcMd5 = other._prepareTestFile(store, size)
                # Import into job store under test
                jobStoreFileID = self.jobstore_initialized.import_file(srcUrl)
                self.assertTrue(isinstance(jobStoreFileID, FileID))
                with self.jobstore_initialized.read_file_stream(jobStoreFileID) as f:
                    fileMD5 = hashlib.md5(f.read()).hexdigest()
                self.assertEqual(fileMD5, srcMd5)
                # Export back into other job store
                dstUrl = other._prepareTestFile(store)
                self.jobstore_initialized.export_file(jobStoreFileID, dstUrl)
                self.assertEqual(fileMD5, other._hashTestFile(dstUrl))

                if otherCls.__name__ == 'FileJobStoreTest':
                    if isinstance(self.jobstore_initialized, FileJobStore):
                        jobStorePath = self.jobstore_initialized._get_file_path_from_id(jobStoreFileID)
                        jobStoreHasLink = os.path.islink(jobStorePath)
                        if self.jobstore_initialized.moveExports:
                            # Ensure the export performed a move / link
                            self.assertTrue(jobStoreHasLink)
                            self.assertEqual(os.path.realpath(jobStorePath), dstUrl[7:])
                        else:
                            # Ensure the export has not moved the job store file
                            self.assertFalse(jobStoreHasLink)

                    # Remove local Files
                    os.remove(srcUrl[7:])
                    os.remove(dstUrl[7:])

            make_tests(testImportExportFile, cls, otherCls=activeTestClassesByName,
                       size=dict(zero=0,
                                 one=1,
                                 oneMiB=2 ** 20,
                                 partSizeMinusOne=cls.mpTestPartSize - 1,
                                 partSize=cls.mpTestPartSize,
                                 partSizePlusOne=cls.mpTestPartSize + 1),
                       moveExports={'deactivated': None, 'activated': True})

            def testImportSharedFile(self, otherCls):
                """
                :param AbstractJobStoreTest.Test self: the current test case

                :param AbstractJobStoreTest.Test otherCls: the test case class for the job store
                       to import from or export to
                """
                # Prepare test file in other job store
                self.jobstore_initialized.partSize = cls.mpTestPartSize
                other = otherCls('testSharedFiles')
                store = other._externalStore()

                srcUrl, srcMd5 = other._prepareTestFile(store, 42)
                # Import into job store under test
                self.assertIsNone(self.jobstore_initialized.import_file(srcUrl, shared_file_name='foo'))
                with self.jobstore_initialized.read_shared_file_stream('foo') as f:
                    fileMD5 = hashlib.md5(f.read()).hexdigest()
                self.assertEqual(fileMD5, srcMd5)
                if otherCls.__name__ == 'FileJobStoreTest':  # Remove local Files
                    os.remove(srcUrl[7:])

            make_tests(testImportSharedFile,
                       cls,
                       otherCls=activeTestClassesByName)

        def testImportHttpFile(self):
            '''Test importing a file over HTTP.'''
            http = socketserver.TCPServer(('', 0), StubHttpRequestHandler)
            try:
                httpThread = threading.Thread(target=http.serve_forever)
                httpThread.start()
                try:
                    assignedPort = http.server_address[1]
                    url = 'http://localhost:%d' % assignedPort
                    with self.jobstore_initialized.read_file_stream(
                            self.jobstore_initialized.import_file(url)) as readable:
                        f1 = readable.read()
                        f2 = StubHttpRequestHandler.fileContents
                        if isinstance(f1, bytes) and not isinstance(f2, bytes):
                            f1 = f1.decode()
                        if isinstance(f2, bytes) and not isinstance(f1, bytes):
                            f1 = f1.encode()
                        self.assertEqual(f1, f2)
                finally:
                    http.shutdown()
                    httpThread.join()
            finally:
                http.server_close()

        def testImportFtpFile(self):
            '''Test importing a file over FTP'''
            ftpfile = {'name': 'foo', 'content': 'foo bar baz qux'}
            ftp = FTPStubServer(0)
            ftp.run()
            try:
                ftp.add_file(**ftpfile)
                assignedPort = ftp.server.server_address[1]
                url = 'ftp://user1:passwd@localhost:%d/%s' % (assignedPort, ftpfile['name'])
                with self.jobstore_initialized.read_file_stream(self.jobstore_initialized.import_file(url)) as readable:
                    imported_content = readable.read()
                    # python 2/3 string/bytestring compat
                    if isinstance(imported_content, bytes):
                        imported_content = imported_content.decode('utf-8')
                    self.assertEqual(imported_content, ftpfile['content'])
            finally:
                ftp.stop()

        @slow
        def testFileDeletion(self):
            """
            Intended to cover the batch deletion of items in the AWSJobStore, but it doesn't hurt
            running it on the other job stores.
            """

            n = self._batchDeletionSize()
            for numFiles in (1, n - 1, n, n + 1, 2 * n):
                job = self.arbitraryJob()
                self.jobstore_initialized.assign_job_id(job)
                self.jobstore_initialized.create_job(job)
                fileIDs = [self.jobstore_initialized.get_empty_file_store_id(job.jobStoreID, cleanup=True) for _ in
                           range(0, numFiles)]
                self.jobstore_initialized.delete_job(job.jobStoreID)
                for fileID in fileIDs:
                    # NB: the fooStream() methods return context managers
                    self.assertRaises(NoSuchFileException, self.jobstore_initialized.read_file_stream(fileID).__enter__)

        @slow
        def testMultipartUploads(self):
            """
            This test is meant to cover multi-part uploads in the AWSJobStore but it doesn't hurt
            running it against the other job stores as well.
            """
            # http://unix.stackexchange.com/questions/11946/how-big-is-the-pipe-buffer
            bufSize = 65536
            partSize = self._partSize()
            self.assertEqual(partSize % bufSize, 0)
            job = self.arbitraryJob()
            self.jobstore_initialized.assign_job_id(job)
            self.jobstore_initialized.create_job(job)

            # Test file/stream ending on part boundary and within a part
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
                checksumThread = Thread(target=checksumThreadFn)
                checksumThread.start()
                try:
                    # Should not block. On Linux, /dev/random blocks when it's running low on entropy
                    with open('/dev/urandom', 'rb') as readable:
                        with self.jobstore_initialized.write_file_stream(job.jobStoreID, cleanup=True) as (
                        writable, fileId):
                            for i in range(int(partSize * partsPerFile / bufSize)):
                                buf = readable.read(bufSize)
                                checksumQueue.put(buf)
                                writable.write(buf)
                finally:
                    checksumQueue.put(None)
                    checksumThread.join()
                before = checksum.hexdigest()

                # Verify
                checksum = hashlib.md5()
                with self.jobstore_initialized.read_file_stream(fileId) as readable:
                    while True:
                        buf = readable.read(bufSize)
                        if not buf:
                            break
                        checksum.update(buf)
                after = checksum.hexdigest()
                self.assertEqual(before, after)

                # Multi-part upload from file
                checksum = hashlib.md5()
                fh, path = tempfile.mkstemp()
                try:
                    with os.fdopen(fh, 'wb+') as writable:
                        with open('/dev/urandom', 'rb') as readable:
                            for i in range(int(partSize * partsPerFile / bufSize)):
                                buf = readable.read(bufSize)
                                writable.write(buf)
                                checksum.update(buf)
                    fileId = self.jobstore_initialized.write_file(path, job.jobStoreID, cleanup=True)
                finally:
                    os.unlink(path)
                before = checksum.hexdigest()

                # Verify
                checksum = hashlib.md5()
                with self.jobstore_initialized.read_file_stream(fileId) as readable:
                    while True:
                        buf = readable.read(bufSize)
                        if not buf:
                            break
                        checksum.update(buf)
                after = checksum.hexdigest()
                self.assertEqual(before, after)
            self.jobstore_initialized.delete_job(job.jobStoreID)

        def testZeroLengthFiles(self):
            '''Test reading and writing of empty files.'''
            job = self.arbitraryJob()
            self.jobstore_initialized.assign_job_id(job)
            self.jobstore_initialized.create_job(job)
            nullFile = self.jobstore_initialized.write_file('/dev/null', job.jobStoreID, cleanup=True)
            with self.jobstore_initialized.read_file_stream(nullFile) as f:
                assert not f.read()
            with self.jobstore_initialized.write_file_stream(job.jobStoreID, cleanup=True) as (f, nullStream):
                pass
            with self.jobstore_initialized.read_file_stream(nullStream) as f:
                assert not f.read()
            self.jobstore_initialized.delete_job(job.jobStoreID)

        @slow
        def testLargeFile(self):
            '''Test the reading and writing of large files.'''
            # Write a large file.
            dirPath = self._createTempDir()
            filePath = os.path.join(dirPath, 'large')
            hashIn = hashlib.md5()
            with open(filePath, 'wb') as f:
                for i in range(0, 10):
                    buf = os.urandom(self._partSize())
                    f.write(buf)
                    hashIn.update(buf)

            # Load the file into a jobstore.
            job = self.arbitraryJob()
            self.jobstore_initialized.assign_job_id(job)
            self.jobstore_initialized.create_job(job)
            jobStoreFileID = self.jobstore_initialized.write_file(filePath, job.jobStoreID, cleanup=True)

            # Remove the local file.
            os.unlink(filePath)

            # Write a local copy of the file from the jobstore.
            self.jobstore_initialized.read_file(jobStoreFileID, filePath)

            # Reread the file to confirm success.
            hashOut = hashlib.md5()
            with open(filePath, 'rb') as f:
                while True:
                    buf = f.read(self._partSize())
                    if not buf:
                        break
                    hashOut.update(buf)
            self.assertEqual(hashIn.digest(), hashOut.digest())

        @retry(errors=[ConnectionError])
        def fetch_url(self, url: str) -> None:
            """
            Fetch the given URL. Throw an error if it cannot be fetched in a
            reasonable number of attempts.
            """
            urlopen(Request(url))

        def assertUrl(self, url):

            prefix, path = url.split(':', 1)
            if prefix == 'file':
                self.assertTrue(os.path.exists(path))
            else:
                try:
                    self.fetch_url(url)
                except:
                    self.fail()

        @slow
        def testCleanCache(self):
            # Make a bunch of jobs
            jobstore = self.jobstore_initialized

            # Create parent job
            rootJob = self.arbitraryJob()
            self.jobstore_initialized.assign_job_id(rootJob)
            self.jobstore_initialized.create_job(rootJob)
            # Create a bunch of child jobs
            for i in range(100):
                child = self.arbitraryJob()
                self.jobstore_initialized.assign_job_id(child)
                self.jobstore_initialized.create_job(child)
                rootJob.addChild(child.jobStoreID)
            jobstore.update_job(rootJob)
            # Make the parent the root
            jobstore.set_root_job(rootJob.jobStoreID)

            # See how long it takes to clean with no cache
            noCacheStart = time.time()
            jobstore.clean()
            noCacheEnd = time.time()

            noCacheTime = noCacheEnd - noCacheStart

            # Make sure we have all the jobs: root and children.
            self.assertEqual(len(list(jobstore.jobs())), 101)

            # See how long it takes to clean with cache
            jobCache = {job.jobStoreID: job
                        for job in jobstore.jobs()}
            cacheStart = time.time()
            jobstore.clean(jobCache)
            cacheEnd = time.time()

            cacheTime = cacheEnd - cacheStart

            logger.debug("Without cache: %f, with cache: %f.", noCacheTime, cacheTime)

            # Running with the cache should be faster.
            self.assertTrue(cacheTime <= noCacheTime)

        # NB: the 'thread' method seems to be needed here to actually
        # ensure the timeout is raised, probably because the only
        # "live" thread doesn't hold the GIL.
        @pytest.mark.timeout(45, method='thread')
        def testPartialReadFromStream(self):
            """Test whether readFileStream will deadlock on a partial read."""
            job = self.arbitraryJob()
            self.jobstore_initialized.assign_job_id(job)
            self.jobstore_initialized.create_job(job)
            with self.jobstore_initialized.write_file_stream(job.jobStoreID, cleanup=True) as (f, fileID):
                # Write enough data to make sure the writer thread
                # will get blocked on the write. Technically anything
                # greater than the pipe buffer size plus the libc
                # buffer size (64K + 4K(?))  should trigger this bug,
                # but this gives us a lot of extra room just to be sure.

                # python 3 requires self.fileContents to be a bytestring
                a = b'a'
                f.write(a * 300000)
            with self.jobstore_initialized.read_file_stream(fileID) as f:
                self.assertEqual(f.read(1), a)
            # If it times out here, there's a deadlock

        @abstractmethod
        def _corruptJobStore(self):
            """
            Deletes some part of the physical storage represented by a job store.
            """
            raise NotImplementedError()

        @slow
        def testDestructionOfCorruptedJobStore(self):
            self._corruptJobStore()
            jobstore = self._createJobStore()
            jobstore.destroy()
            # Note that self.jobstore_initialized.destroy() is done as part of shutdown

        def testDestructionIdempotence(self):
            # Jobstore is fully initialized
            self.jobstore_initialized.destroy()
            # Create a second instance for the same physical storage but do not .initialize() or
            # .resume() it.
            cleaner = self._createJobStore()
            cleaner.destroy()
            # And repeat
            self.jobstore_initialized.destroy()
            cleaner = self._createJobStore()
            cleaner.destroy()

        def testEmptyFileStoreIDIsReadable(self):
            """Simply creates an empty fileStoreID and attempts to read from it."""
            id = self.jobstore_initialized.get_empty_file_store_id()
            fh, path = tempfile.mkstemp()
            try:
                self.jobstore_initialized.read_file(id, path)
                self.assertTrue(os.path.isfile(path))
            finally:
                os.unlink(path)

        def _largeLogEntrySize(self):
            """
            Sub-classes may want to override these in order to maximize test coverage
            """
            return 1 * 1024 * 1024

        def _batchDeletionSize(self):
            return 10

        def _partSize(self):
            return 5 * 1024 * 1024


class AbstractEncryptedJobStoreTest:
    # noinspection PyAbstractClass
    class Test(AbstractJobStoreTest.Test, metaclass=ABCMeta):
        """
        A test of job stores that use encryption
        """

        def setUp(self):
            # noinspection PyAttributeOutsideInit
            self.sseKeyDir = tempfile.mkdtemp()
            super().setUp()

        def tearDown(self):
            super().tearDown()
            shutil.rmtree(self.sseKeyDir)

        def _createConfig(self):
            config = super()._createConfig()
            sseKeyFile = os.path.join(self.sseKeyDir, 'keyFile')
            with open(sseKeyFile, 'w') as f:
                f.write('01234567890123456789012345678901')
            config.sseKey = sseKeyFile
            # config.attrib['sse_key'] = sseKeyFile
            return config

        def testEncrypted(self):
            """
            Create an encrypted file. Read it in encrypted mode then try with encryption off
            to ensure that it fails.
            """
            phrase = b'This file is encrypted.'
            fileName = 'foo'
            with self.jobstore_initialized.write_shared_file_stream(fileName, encrypted=True) as f:
                f.write(phrase)
            with self.jobstore_initialized.read_shared_file_stream(fileName) as f:
                self.assertEqual(phrase, f.read())

            # disable encryption
            self.jobstore_initialized.config.sseKey = None
            try:
                with self.jobstore_initialized.read_shared_file_stream(fileName) as f:
                    self.assertEqual(phrase, f.read())
            except AssertionError as e:
                self.assertEqual("Content is encrypted but no key was provided.", e.args[0])
            else:
                self.fail("Read encryption content with encryption off.")


class FileJobStoreTest(AbstractJobStoreTest.Test):
    def _createJobStore(self):
        # Make a FileJobStore with an artificially low fan out threshold, to
        # make sure to test fan out logic
        return FileJobStore(self.namePrefix, fanOut=2)

    def _corruptJobStore(self):
        assert isinstance(self.jobstore_initialized, FileJobStore)  # type hint
        shutil.rmtree(self.jobstore_initialized.jobStoreDir)

    def _prepareTestFile(self, dirPath, size=None):
        fileName = 'testfile_%s' % uuid.uuid4()
        localFilePath = dirPath + fileName
        url = 'file://%s' % localFilePath
        if size is None:
            return url
        else:
            content = os.urandom(size)
            with open(localFilePath, 'wb') as writable:
                writable.write(content)

            return url, hashlib.md5(content).hexdigest()

    def _hashTestFile(self, url):
        localFilePath = FileJobStore._extract_path_from_url(urlparse.urlparse(url))
        with open(localFilePath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()

    def _createExternalStore(self):
        return tempfile.mkdtemp()

    def _cleanUpExternalStore(self, dirPath):
        shutil.rmtree(dirPath)

    def testPreserveFileName(self):
        """Check that the fileID ends with the given file name."""
        fh, path = tempfile.mkstemp()
        try:
            os.close(fh)
            job = self.arbitraryJob()
            self.jobstore_initialized.assign_job_id(job)
            self.jobstore_initialized.create_job(job)
            fileID = self.jobstore_initialized.write_file(path, job.jobStoreID, cleanup=True)
            self.assertTrue(fileID.endswith(os.path.basename(path)))
        finally:
            os.unlink(path)

    def test_jobstore_init_preserves_symlink_path(self):
        """Test that if we provide a fileJobStore with a symlink to a directory, it doesn't de-reference it."""
        dir_symlinked_to_original_filestore = None
        original_filestore = None
        try:
            original_filestore = self._createExternalStore()
            dir_symlinked_to_original_filestore = f'{original_filestore}-am-i-real'
            os.symlink(original_filestore, dir_symlinked_to_original_filestore)
            filejobstore_using_symlink = FileJobStore(dir_symlinked_to_original_filestore, fanOut=2)
            self.assertEqual(dir_symlinked_to_original_filestore, filejobstore_using_symlink.jobStoreDir)
        finally:
            if dir_symlinked_to_original_filestore and os.path.exists(dir_symlinked_to_original_filestore):
                os.unlink(dir_symlinked_to_original_filestore)
            if original_filestore and os.path.exists(original_filestore):
                shutil.rmtree(original_filestore)

    def test_jobstore_does_not_leak_symlinks(self):
        """Test that if we link imports into the FileJobStore, we can't get hardlinks to symlinks."""

        temp_dir = None
        download_dir = None

        try:
            # Grab a temp directory to make files in. Make sure it's on the
            # same device as everything else.
            temp_dir = os.path.abspath(self.namePrefix + '-import')
            os.mkdir(temp_dir)
            to_import = os.path.join(temp_dir, 'import-me')
            with open(to_import, 'w') as f:
                f.write('test')

            # And a temp directory next to the job store to download to
            download_dir = os.path.abspath(self.namePrefix + '-dl')
            os.mkdir(download_dir)

            # Import it as a symlink
            file_id = self.jobstore_initialized.import_file('file://' + to_import, symlink=True)

            # Take it out as a hard link or copy
            download_to = os.path.join(download_dir, 'downloaded')
            self.jobstore_initialized.read_file(file_id, download_to)

            # Make sure it isn't a symlink
            self.assertFalse(os.path.islink(download_to))

        finally:
            if temp_dir and os.path.exists(temp_dir):
                # Clean up temp directory
                shutil.rmtree(temp_dir)
            if download_dir and os.path.exists(download_dir):
                # Clean up download directory
                shutil.rmtree(download_dir)

    def test_file_link_imports(self):
        """
        Test that imported files are symlinked when when expected
        """
        store = self._externalStore()
        size = 1
        srcUrl, srcMd5 = self._prepareTestFile(store, size)
        for symlink in [True, False]:
            for link_imports in [True, False]:
                self.jobstore_initialized.linkImports = link_imports
                # Import into job store under test
                jobStoreFileID = self.jobstore_initialized.import_file(srcUrl, symlink=symlink)
                self.assertTrue(isinstance(jobStoreFileID, FileID))
                with self.jobstore_initialized.read_file_stream(jobStoreFileID) as f:
                    # gets abs path
                    filename = f.name
                    fileMD5 = hashlib.md5(f.read()).hexdigest()
                self.assertEqual(fileMD5, srcMd5)
                if link_imports and symlink:
                    self.assertTrue(os.path.islink(filename))
                else:
                    self.assertFalse(os.path.islink(filename))

                # Remove local Files
                os.remove(filename)
        os.remove(srcUrl[7:])


@needs_google_project
@needs_google_storage
@pytest.mark.xfail
class GoogleJobStoreTest(AbstractJobStoreTest.Test):
    projectID = os.getenv('TOIL_GOOGLE_PROJECTID')
    headers = {"x-goog-project-id": projectID}

    def _createJobStore(self):
        from toil.jobStores.googleJobStore import GoogleJobStore
        return GoogleJobStore(GoogleJobStoreTest.projectID + ":" + self.namePrefix)

    def _corruptJobStore(self):
        # The Google job store has only one resource, the bucket, so we can't corrupt it without
        # fully deleting it.
        pass

    def _prepareTestFile(self, bucket, size=None):
        from toil.jobStores.googleJobStore import GoogleJobStore
        fileName = 'testfile_%s' % uuid.uuid4()
        url = f'gs://{bucket.name}/{fileName}'
        if size is None:
            return url
        with open('/dev/urandom', 'rb') as readable:
            contents = str(readable.read(size))
        GoogleJobStore._write_to_url(BytesIO(bytes(contents, 'utf-8')), urlparse.urlparse(url))
        return url, hashlib.md5(contents.encode()).hexdigest()

    def _hashTestFile(self, url):
        from toil.jobStores.googleJobStore import GoogleJobStore
        contents = GoogleJobStore._get_blob_from_url(urlparse.urlparse(url)).download_as_string()
        return hashlib.md5(contents).hexdigest()

    @google_retry
    def _createExternalStore(self):
        from google.cloud import storage
        bucketName = ("import-export-test-" + str(uuid.uuid4()))
        storageClient = storage.Client()
        return storageClient.create_bucket(bucketName)

    @google_retry
    def _cleanUpExternalStore(self, bucket):
        # this is copied from googleJobStore.destroy
        try:
            bucket.delete(force=True)
            # throws ValueError if bucket has more than 256 objects. Then we must delete manually
        except ValueError:
            bucket.delete_blobs(bucket.list_blobs)
            bucket.delete()


@needs_aws_s3
class AWSJobStoreTest(AbstractJobStoreTest.Test):
    def _createJobStore(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        partSize = self._partSize()
        return AWSJobStore(self.awsRegion() + ':' + self.namePrefix, partSize=partSize)

    def _corruptJobStore(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        assert isinstance(self.jobstore_initialized, AWSJobStore)  # type hinting
        self.jobstore_initialized.destroy()

    def testSDBDomainsDeletedOnFailedJobstoreBucketCreation(self):
        """
        This test ensures that SDB domains bound to a jobstore are deleted if the jobstore bucket
        failed to be created.  We simulate a failed jobstore bucket creation by using a bucket in a
        different region with the same name.
        """
        from boto.sdb import connect_to_region
        from botocore.exceptions import ClientError

        from toil.jobStores.aws.jobStore import BucketLocationConflictException
        from toil.lib.aws.session import establish_boto3_session
        from toil.lib.aws.utils import retry_s3

        externalAWSLocation = 'us-west-1'
        for testRegion in 'us-east-1', 'us-west-2':
            # We run this test twice, once with the default s3 server us-east-1 as the test region
            # and once with another server (us-west-2).  The external server is always us-west-1.
            # This incidentally tests that the BucketLocationConflictException is thrown when using
            # both the default, and a non-default server.
            testJobStoreUUID = str(uuid.uuid4())
            # Create the bucket at the external region
            bucketName = 'domain-test-' + testJobStoreUUID + '--files'
            client = establish_boto3_session().client('s3', region_name=externalAWSLocation)
            resource = establish_boto3_session().resource('s3', region_name=externalAWSLocation)

            for attempt in retry_s3(delays=(2, 5, 10, 30, 60), timeout=600):
                with attempt:
                    # Create the bucket at the home region
                    client.create_bucket(Bucket=bucketName,
                                         CreateBucketConfiguration={'LocationConstraint': externalAWSLocation})

            owner_tag = os.environ.get('TOIL_OWNER_TAG')
            if owner_tag:
                for attempt in retry_s3(delays=(1, 1, 2, 4, 8, 16), timeout=33):
                    with attempt:
                        bucket_tagging = resource.BucketTagging(bucketName)
                        bucket_tagging.put(Tagging={'TagSet': [{'Key': 'Owner', 'Value': owner_tag}]})

            options = Job.Runner.getDefaultOptions('aws:' + testRegion + ':domain-test-' + testJobStoreUUID)
            options.logLevel = 'DEBUG'
            try:
                with Toil(options) as toil:
                    pass
            except BucketLocationConflictException:
                # Catch the expected BucketLocationConflictException and ensure that the bound
                # domains don't exist in SDB.
                sdb = connect_to_region(self.awsRegion())
                next_token = None
                allDomainNames = []
                while True:
                    domains = sdb.get_all_domains(max_domains=100, next_token=next_token)
                    allDomainNames.extend([x.name for x in domains])
                    next_token = domains.next_token
                    if next_token is None:
                        break
                self.assertFalse([d for d in allDomainNames if testJobStoreUUID in d])
            else:
                self.fail()
            finally:
                try:
                    for attempt in retry_s3():
                        with attempt:
                            client.delete_bucket(Bucket=bucketName)
                except ClientError as e:
                    # The actual HTTP code of the error is in status.
                    if e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
                        # The bucket doesn't exist; maybe a failed delete actually succeeded.
                        pass
                    else:
                        raise

    @slow
    def testInlinedFiles(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        jobstore = self.jobstore_initialized
        for encrypted in (True, False):
            n = AWSJobStore.FileInfo.maxInlinedSize()
            sizes = (1, n // 2, n - 1, n, n + 1, 2 * n)
            for size in chain(sizes, islice(reversed(sizes), 1)):
                s = os.urandom(size)
                with jobstore.write_shared_file_stream('foo') as f:
                    f.write(s)
                with jobstore.read_shared_file_stream('foo') as f:
                    self.assertEqual(s, f.read())

    def testOverlargeJob(self):
        jobstore = self.jobstore_initialized
        jobRequirements = dict(memory=12, cores=34, disk=35, preemptible=True)
        overlargeJob = JobDescription(command='overlarge',
                                      requirements=jobRequirements,
                                      jobName='test-overlarge', unitName='onJobStore')

        # Make the pickled size of the job larger than 256K
        with open("/dev/urandom", 'rb') as random:
            overlargeJob.jobName = str(random.read(512 * 1024))
        jobstore.assign_job_id(overlargeJob)
        jobstore.create_job(overlargeJob)
        self.assertTrue(jobstore.job_exists(overlargeJob.jobStoreID))
        overlargeJobDownloaded = jobstore.load_job(overlargeJob.jobStoreID)
        # Because jobs lack equality comparison, we stringify for comparison.
        jobsInJobStore = [str(job) for job in jobstore.jobs()]
        self.assertEqual(jobsInJobStore, [str(overlargeJob)])
        jobstore.delete_job(overlargeJob.jobStoreID)

    def testMultiThreadImportFile(self) -> None:
        """ Tests that importFile is thread-safe."""

        from concurrent.futures.thread import ThreadPoolExecutor

        from toil.lib.threading import cpu_count

        threads: Tuple[int, ...] = (2, cpu_count()) if cpu_count() > 2 else (2, )
        num_of_files: int = 5
        size: int = 1 << 16 + 1

        # The string in otherCls() is arbitrary as long as it returns a class that has access
        # to ._externalStore() and ._prepareTestFile()
        other: AbstractJobStoreTest.Test = AWSJobStoreTest('testSharedFiles')
        store: Any = other._externalStore()

        # prepare test files to import
        logger.debug(f'Preparing {num_of_files} test files for testMultiThreadImportFile().')
        test_files = [other._prepareTestFile(store, size) for _ in range(num_of_files)]

        for thread_count in threads:
            with self.subTest(f'Testing threaded importFile with "{thread_count}" threads.'):
                results = []

                with ThreadPoolExecutor(max_workers=thread_count) as executor:
                    for url, expected_md5 in test_files:
                        # run jobStore.importFile() asynchronously
                        future = executor.submit(self.jobstore_initialized.import_file, url)
                        results.append((future, expected_md5))

                self.assertEqual(len(results), num_of_files)

                for future, expected_md5 in results:
                    file_id = future.result()
                    self.assertIsInstance(file_id, FileID)

                    with self.jobstore_initialized.read_file_stream(file_id) as f:
                        self.assertEqual(hashlib.md5(f.read()).hexdigest(), expected_md5)

    def _prepareTestFile(self, bucket, size=None):
        from toil.lib.aws.utils import retry_s3

        file_name = 'testfile_%s' % uuid.uuid4()
        url = f's3://{bucket.name}/{file_name}'
        if size is None:
            return url
        with open('/dev/urandom', 'rb') as readable:
            for attempt in retry_s3():
                with attempt:
                    bucket.put_object(Key=file_name, Body=str(readable.read(size)))
        return url, hashlib.md5(bucket.Object(file_name).get().get('Body').read()).hexdigest()

    def _hashTestFile(self, url: str) -> str:
        from toil.jobStores.aws.jobStore import AWSJobStore
        key = get_object_for_url(urlparse.urlparse(url), existing=True)
        contents = key.get().get('Body').read()
        return hashlib.md5(contents).hexdigest()

    def _createExternalStore(self):
        """A S3.Bucket instance is returned"""
        from toil.jobStores.aws.jobStore import establish_boto3_session
        from toil.lib.aws.utils import retry_s3

        resource = establish_boto3_session().resource(
            "s3", region_name=self.awsRegion()
        )
        bucket_name = f"import-export-test-{uuid.uuid4()}"
        location = self.awsRegion()

        for attempt in retry_s3():
            with attempt:
                bucket = create_s3_bucket(resource, bucket_name, location)
                bucket.wait_until_exists()
                return bucket

    def _cleanUpExternalStore(self, bucket):
        bucket.objects.all().delete()
        bucket.delete()

    def _largeLogEntrySize(self):
        from toil.jobStores.aws.jobStore import AWSJobStore

        # So we get into the else branch of reader() in uploadStream(multiPart=False):
        return AWSJobStore.FileInfo.maxBinarySize() * 2

    def _batchDeletionSize(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        return AWSJobStore.itemsPerBatchDelete


@needs_aws_s3
class InvalidAWSJobStoreTest(ToilTest):
    def testInvalidJobStoreName(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        self.assertRaises(ValueError,
                          AWSJobStore,
                          'us-west-2:a--b')
        self.assertRaises(ValueError,
                          AWSJobStore,
                          'us-west-2:' + ('a' * 100))
        self.assertRaises(ValueError,
                          AWSJobStore,
                          'us-west-2:a_b')


@needs_aws_s3
@needs_encryption
@slow
class EncryptedAWSJobStoreTest(AWSJobStoreTest, AbstractEncryptedJobStoreTest.Test):
    pass


class StubHttpRequestHandler(http.server.SimpleHTTPRequestHandler):
    fileContents = 'A good programmer looks both ways before crossing a one-way street'

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.send_header("Content-length", len(self.fileContents))
        self.end_headers()
        self.fileContents = self.fileContents.encode('utf-8')
        self.wfile.write(self.fileContents)


AbstractJobStoreTest.Test.makeImportExportTests()
