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
from __future__ import division
from future import standard_library

standard_library.install_aliases()
from builtins import range
from builtins import str
from past.utils import old_div
from builtins import object
import socketserver
import pytest
import hashlib
import logging
import threading
import os
import sys
import shutil
import tempfile
import time
import uuid
from stubserver import FTPStubServer
from abc import abstractmethod, ABCMeta
from itertools import chain, islice
from threading import Thread
from unittest import skip
from six.moves.queue import Queue
from six.moves import SimpleHTTPServer, StringIO
from six import iteritems
import six.moves.urllib.parse as urlparse
from six.moves.urllib.request import urlopen, Request

from toil.lib.memoize import memoize
from toil.lib.exceptions import panic
# noinspection PyPackageRequirements
# (installed by `make prepare`)
from mock import patch

from toil.lib.compatibility import USING_PYTHON2
from toil.common import Config, Toil
from toil.fileStores import FileID
from toil.job import Job, JobNode
from toil.jobStores.abstractJobStore import (NoSuchJobException,
                                             NoSuchFileException)
from toil.jobStores.googleJobStore import googleRetry
from toil.jobStores.fileJobStore import FileJobStore
from toil.statsAndLogging import StatsAndLogging
from toil.test import (ToilTest,
                       needs_aws_ec2,
                       needs_encryption,
                       make_tests,
                       needs_google,
                       travis_test,
                       slow)
from future.utils import with_metaclass

logger = logging.getLogger(__name__)


def tearDownModule():
    AbstractJobStoreTest.Test.cleanUpExternalStores()


class AbstractJobStoreTest(object):
    """
    Hide abstract base class from unittest's test case loader

    http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class#answer-25695512
    """

    class Test(with_metaclass(ABCMeta, ToilTest)):
        @classmethod
        def setUpClass(cls):
            super(AbstractJobStoreTest.Test, cls).setUpClass()
            logging.basicConfig(level=logging.DEBUG)
            logging.getLogger('boto').setLevel(logging.CRITICAL)

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
            return super(AbstractJobStoreTest.Test, cls).__new__(cls)

        def _createConfig(self):
            return Config()

        @abstractmethod
        def _createJobStore(self):
            """
            :rtype: AbstractJobStore
            """
            raise NotImplementedError()

        def setUp(self):
            super(AbstractJobStoreTest.Test, self).setUp()
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
            self.arbitraryRequirements = {'memory': 1, 'disk': 2, 'cores': 1, 'preemptable': False}
            self.arbitraryJob = JobNode(command='command',
                                        jobStoreID=None,
                                        jobName='arbitrary', unitName=None,
                                        requirements=self.arbitraryRequirements)

            self.parentJobReqs = dict(memory=12, cores=34, disk=35, preemptable=True)
            self.childJobReqs1 = dict(memory=23, cores=45, disk=46, preemptable=True)
            self.childJobReqs2 = dict(memory=34, cores=56, disk=57, preemptable=False)

        def tearDown(self):
            self.jobstore_initialized.destroy()
            self.jobstore_resumed_noconfig.destroy()
            super(AbstractJobStoreTest.Test, self).tearDown()

        @travis_test
        def testInitialState(self):
            """Ensure proper handling of nonexistant files."""
            self.assertFalse(self.jobstore_initialized.exists('nonexistantFile'))
            self.assertRaises(NoSuchJobException, self.jobstore_initialized.load, 'nonexistantFile')

        @travis_test
        def testJobCreation(self):
            """
            Test creation of a job.

            Does the job exist in the jobstore it is supposed to be in?
            Are its attributes what is expected?
            """

            jobstore = self.jobstore_initialized

            # Create a job and verify its existence/properties
            aJobNode = JobNode(command='parent1',
                               requirements=self.parentJobReqs,
                               jobName='test1', unitName='onParent',
                               jobStoreID=None, predecessorNumber=0)
            job = jobstore.create(aJobNode)

            self.assertTrue(jobstore.exists(job.jobStoreID))
            self.assertEqual(job.command, 'parent1')
            self.assertEqual(job.memory, self.parentJobReqs['memory'])
            self.assertEqual(job.cores, self.parentJobReqs['cores'])
            self.assertEqual(job.disk, self.parentJobReqs['disk'])
            self.assertEqual(job.preemptable, self.parentJobReqs['preemptable'])
            self.assertEqual(job.jobName, 'test1')
            self.assertEqual(job.unitName, 'onParent')
            self.assertEqual(job.stack, [])
            self.assertEqual(job.predecessorNumber, 0)
            self.assertEqual(job.predecessorsFinished, set())
            self.assertEqual(job.logJobStoreFileID, None)

        @travis_test
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

        @travis_test
        def testJobLoadEquality(self):
            """Tests that a job loaded into one jobstore from another can be used equivalently by another."""

            # Create a job on the first jobstore.
            jobNode1 = JobNode(command='jobstore1',
                               requirements=self.parentJobReqs,
                               jobName='test1', unitName='onJS1',
                               jobStoreID=None, predecessorNumber=0)
            job1 = self.jobstore_initialized.create(jobNode1)

            # Load it onto the second jobstore
            job2 = self.jobstore_resumed_noconfig.load(job1.jobStoreID)

            self.assertEqual(job1, job2)

        @travis_test
        def testChildLoadingEquality(self):
            """Test that loading a child job operates as expected."""
            aJobNode = JobNode(command='parent1',
                               requirements=self.parentJobReqs,
                               jobName='test1', unitName='onParent',
                               jobStoreID=None, predecessorNumber=0)

            jobNodeOnChild = JobNode(command='child1',
                                     requirements=self.childJobReqs1,
                                     jobName='test2', unitName='onChild1',
                                     jobStoreID=None)
            job = self.jobstore_initialized.create(aJobNode)
            childJob = self.jobstore_initialized.create(jobNodeOnChild)
            job.stack.append(childJob)
            self.jobstore_initialized.update(job)
            self.assertEqual(self.jobstore_initialized.load(childJob.jobStoreID), childJob)

        @travis_test
        def testPersistantFilesToDelete(self):
            """
            Make sure that updating a job  carries over filesToDelete.

            The following demonstrates the job update pattern, where files to be deleted are referenced in
            "filesToDelete" array, which is persisted to disk first. If things go wrong during the update, this list of
            files to delete is used to remove the unneeded files.
            """

            # Create a job.
            jobNode = JobNode(command='job1',
                              requirements=self.parentJobReqs,
                              jobName='test1', unitName='onJS1',
                              jobStoreID=None, predecessorNumber=0)

            job = self.jobstore_initialized.create(jobNode)
            job.filesToDelete = ['1', '2']
            self.jobstore_initialized.update(job)
            self.assertEqual(self.jobstore_initialized.load(job.jobStoreID).filesToDelete, ['1', '2'])

        @travis_test
        def testUpdateBehavior(self):
            """Tests the proper behavior during updating jobs."""
            jobstore1 = self.jobstore_initialized
            jobstore2 = self.jobstore_resumed_noconfig

            aJobNode = JobNode(command='parent1',
                               requirements=self.parentJobReqs,
                               jobName='test1', unitName='onParent',
                               jobStoreID=None, predecessorNumber=0)

            jobNodeOnChild1 = JobNode(command='child1',
                                      requirements=self.childJobReqs1,
                                      jobName='test2', unitName='onChild1',
                                      jobStoreID=None)

            jobNodeOnChild2 = JobNode(command='child2',
                                      requirements=self.childJobReqs2,
                                      jobName='test3', unitName='onChild2',
                                      jobStoreID=None)

            job1 = jobstore1.create(aJobNode)
            job2 = jobstore2.load(job1.jobStoreID)

            # Create child jobs.
            childJob1 = jobstore2.create(jobNodeOnChild1)
            childJob2 = jobstore2.create(jobNodeOnChild2)

            # Add them to job2.
            job2.stack.append((childJob1, childJob2))
            jobstore2.update(job2)

            # Check equivalence between jobstore1 and jobstore2.
            # While job1 and job2 share a jobStoreID, job1 has not been "refreshed" to show the newly added child jobs.
            self.assertNotEqual(job2, job1)

            # Reload parent job on jobstore, "refreshing" the job.
            job1 = jobstore1.load(job1.jobStoreID)
            self.assertEqual(job2, job1)

            # Load children on jobstore and check equivalence
            self.assertEqual(jobstore1.load(childJob1.jobStoreID), childJob1)
            self.assertEqual(jobstore1.load(childJob2.jobStoreID), childJob2)
            self.assertEqual(job1, job2)  # The jobs should both have children now...
            self.assertIsNot(job1, job2)  # but should not be the same.

        @travis_test
        def testChangingJobStoreID(self):
            """
            Tests that changing the jobStoreID makes jobs unequivalent.

            Create two job trees, jobstore1 & jobstore2 consisting of a parent and 5 child jobs. The children of
            jobstore2 will be copied from jobstore1. Changing the jobstoreFileID on each child on jobstore1 will cause
            them to be different jobs. After updating the children of jobstore2, they should be equal.
            """

            jobstore1 = self.jobstore_initialized
            jobstore2 = self.jobstore_resumed_noconfig

            # Create a job
            aJobNode = JobNode(command='parent1',
                               requirements=self.parentJobReqs,
                               jobName='test1', unitName='onParent',
                               jobStoreID=None, predecessorNumber=0)

            # Load the job onto the two jobstores.
            parentJob1 = jobstore1.create(aJobNode)
            parentJob2 = jobstore2.load(parentJob1.jobStoreID)

            # Create an array of child jobs for each jobstore.
            for i in range(0, 5):
                jobNodeOnChild1 = JobNode(command='child' + str(i),
                                          requirements=self.childJobReqs1,
                                          jobName='test' + str(i), unitName='onChild1',
                                          jobStoreID=None)
                aChildJob = jobstore1.create(jobNodeOnChild1)
                parentJob1.stack.append(aChildJob)
                jobstore2.load(aChildJob.jobStoreID)

            # Compare children before and after update.
            for childJob in parentJob2.stack:
                self.assertEqual(childJob, jobstore1.load(childJob.jobStoreID))
                childJob.logJobStoreFileID = str(uuid.uuid4())
                childJob.remainingRetryCount = 66
                self.assertNotEqual(childJob, jobstore1.load(childJob.jobStoreID))

            # Update the children on the second jobstore.
            for childJob in parentJob2.stack:
                jobstore2.update(childJob)

            # Check that the jobs are equivalent after being reloaded.
            for childJob in parentJob2.stack:
                self.assertEqual(jobstore1.load(childJob.jobStoreID), childJob)
                self.assertEqual(jobstore2.load(childJob.jobStoreID), childJob)

        @travis_test
        def testJobDeletions(self):
            """Tests the consequences of deleting jobs."""
            # A local jobstore object for testing.
            jobstore = self.jobstore_initialized
            jobNodeOnParent = JobNode(command='job1',
                                      requirements=self.parentJobReqs,
                                      jobName='test1', unitName='onJob',
                                      jobStoreID=None, predecessorNumber=0)
            # Create jobs
            job = jobstore.create(jobNodeOnParent)

            # Create child Jobs
            jobNodeOnChild1 = JobNode(command='child1',
                                      requirements=self.childJobReqs1,
                                      jobName='test2', unitName='onChild1',
                                      jobStoreID=None)

            jobNodeOnChild2 = JobNode(command='job1',
                                      requirements=self.childJobReqs2,
                                      jobName='test3', unitName='onChild2',
                                      jobStoreID=None)

            # Add children to parent.
            child1 = jobstore.create(jobNodeOnChild1)
            child2 = jobstore.create(jobNodeOnChild2)
            job.stack.append((child1, child2))
            jobstore.update(job)

            # Reminder: We are accessing the -1st element because we just appended.
            # However, there should only be one element.
            childJobs = [jobstore.load(childNode.jobStoreID) for childNode in job.stack[-1]]

            # Test job iterator - the results of the iterator are effected by eventual
            # consistency. We cannot guarantee all jobs will appear but we can assert that all
            # jobs that show up are a subset of all existing jobs. If we had deleted jobs before
            # this we would have to worry about ghost jobs appearing and this assertion would not
            # be valid
            self.assertTrue(set(childJobs + [job]) >= set(jobstore.jobs()))

            # Test job deletions
            # First delete parent, this should have no effect on the children
            self.assertTrue(jobstore.exists(job.jobStoreID))
            jobstore.delete(job.jobStoreID)
            self.assertFalse(jobstore.exists(job.jobStoreID))

            # Check the deletion of children
            for childJob in childJobs:
                self.assertTrue(jobstore.exists(childJob.jobStoreID))
                jobstore.delete(childJob.jobStoreID)
                self.assertFalse(jobstore.exists(childJob.jobStoreID))
                self.assertRaises(NoSuchJobException, jobstore.load, childJob.jobStoreID)

            try:
                with jobstore.readSharedFileStream('missing') as _:
                    pass
                self.fail('Expecting NoSuchFileException')
            except NoSuchFileException:
                pass

        @travis_test
        def testSharedFiles(self):
            """Tests the sharing of files."""
            jobstore1 = self.jobstore_initialized
            jobstore2 = self.jobstore_resumed_noconfig

            bar = 'bar'
            if sys.version_info >= (3, 0):
                bar = b'bar'

            with jobstore1.writeSharedFileStream('foo') as f:
                f.write(bar)
            # ... read that file on worker, ...
            with jobstore2.readSharedFileStream('foo') as f:
                self.assertEqual(bar, f.read())
            # ... and read it again on jobstore1.
            with jobstore1.readSharedFileStream('foo') as f:
                self.assertEqual(bar, f.read())

            with jobstore1.writeSharedFileStream('nonEncrypted', isProtected=False) as f:
                f.write(bar)
            self.assertUrl(jobstore1.getSharedPublicUrl('nonEncrypted'))
            self.assertRaises(NoSuchFileException, jobstore1.getSharedPublicUrl, 'missing')

        @travis_test
        def testPerJobFiles(self):
            """Tests the behavior of files on jobs."""
            jobstore1 = self.jobstore_initialized
            jobstore2 = self.jobstore_resumed_noconfig

            # Create jobNodeOnJS1
            jobNodeOnJobStore1 = JobNode(command='job1',
                                         requirements=self.parentJobReqs,
                                         jobName='test1', unitName='onJobStore1',
                                         jobStoreID=None, predecessorNumber=0)

            # First recreate job
            jobOnJobStore1 = jobstore1.create(jobNodeOnJobStore1)
            fileOne = jobstore2.getEmptyFileStoreID(jobOnJobStore1.jobStoreID, cleanup=True)
            # Check file exists
            self.assertTrue(jobstore2.fileExists(fileOne))
            self.assertTrue(jobstore1.fileExists(fileOne))
            one = 'one'
            two = 'two'
            three = 'three'
            if sys.version_info >= (3, 0):
                one = b'one'
                two = b'two'
                three = b'three'
            # ... write to the file on jobstore2, ...
            with jobstore2.updateFileStream(fileOne) as f:
                f.write(one)
            # ... read the file as a stream on the jobstore1, ....
            with jobstore1.readFileStream(fileOne) as f:
                self.assertEqual(f.read(), one)

            # ... and copy it to a temporary physical file on the jobstore1.
            fh, path = tempfile.mkstemp()
            try:
                os.close(fh)
                tmpPath = path + '.read-only'
                jobstore1.readFile(fileOne, tmpPath)
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
                fileTwo = jobstore1.writeFile(path, jobOnJobStore1.jobStoreID, cleanup=True)
                with jobstore2.readFileStream(fileTwo) as f:
                    self.assertEqual(f.read(), two)
                # Now update the first file from the local file ...
                jobstore1.updateFile(fileOne, path)
                with jobstore2.readFileStream(fileOne) as f:
                    self.assertEqual(f.read(), two)
            finally:
                os.unlink(path)
            # Create a third file to test the last remaining method.
            with jobstore2.writeFileStream(jobOnJobStore1.jobStoreID, cleanup=True) as (f, fileThree):
                f.write(three)
            with jobstore1.readFileStream(fileThree) as f:
                self.assertEqual(f.read(), three)
            # Delete a file explicitly but leave files for the implicit deletion through the parent
            jobstore2.deleteFile(fileOne)

            # Check the file is gone
            #
            for store in jobstore2, jobstore1:
                self.assertFalse(store.fileExists(fileOne))
                self.assertRaises(NoSuchFileException, store.readFile, fileOne, '')
                try:
                    with store.readFileStream(fileOne) as _:
                        pass
                    self.fail('Expecting NoSuchFileException')
                except NoSuchFileException:
                    pass

        @travis_test
        def testStatsAndLogging(self):
            """Tests behavior of reading and writting stats and logging."""
            jobstore1 = self.jobstore_initialized
            jobstore2 = self.jobstore_resumed_noconfig

            jobNodeOnJobStore1 = JobNode(command='job1',
                                         requirements=self.parentJobReqs,
                                         jobName='test1', unitName='onJobStore1',
                                         jobStoreID=None, predecessorNumber=0)

            jobOnJobStore1 = jobstore1.create(jobNodeOnJobStore1)

            # Test stats and logging
            stats = None
            one = b'one' if not USING_PYTHON2 else 'one'
            two = b'two' if not USING_PYTHON2 else 'two'

            # Allows stats to be read/written to/from in read/writeStatsAndLogging.
            def callback(f2):
                stats.add(f2.read())

            # Collects stats and logging messages.
            stats = set()

            # No stats or logging added yet. Expect nothing.
            self.assertEqual(0, jobstore1.readStatsAndLogging(callback))
            self.assertEqual(set(), stats)

            # Test writing and reading.
            jobstore2.writeStatsAndLogging(one)
            self.assertEqual(1, jobstore1.readStatsAndLogging(callback))
            self.assertEqual({one}, stats)
            self.assertEqual(0, jobstore1.readStatsAndLogging(callback))  # readStatsAndLogging purges saved stats etc

            jobstore2.writeStatsAndLogging(one)
            jobstore2.writeStatsAndLogging(two)
            stats = set()
            self.assertEqual(2, jobstore1.readStatsAndLogging(callback))
            self.assertEqual({one, two}, stats)

            largeLogEntry = os.urandom(self._largeLogEntrySize())
            stats = set()
            jobstore2.writeStatsAndLogging(largeLogEntry)
            self.assertEqual(1, jobstore1.readStatsAndLogging(callback))
            self.assertEqual({largeLogEntry}, stats)

            # test the readAll parameter
            self.assertEqual(4, jobstore1.readStatsAndLogging(callback, readAll=True))

            # Delete parent
            jobstore1.delete(jobOnJobStore1.jobStoreID)
            self.assertFalse(jobstore1.exists(jobOnJobStore1.jobStoreID))
            # TODO: Who deletes the shared files?

        @travis_test
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
            with open(jobLogFile, 'r') as f:
                self.assertEqual(f.read(), 'string\nbytes\n\nnewline\n')
            os.remove(jobLogFile)

        @travis_test
        def testBatchCreate(self):
            """Test creation of many jobs."""
            jobstore = self.jobstore_initialized
            jobRequirements = dict(memory=12, cores=34, disk=35, preemptable=True)
            jobGraphs = []
            with jobstore.batch():
                for i in range(100):
                    overlargeJobNode = JobNode(command='overlarge',
                                               requirements=jobRequirements,
                                               jobName='test-overlarge', unitName='onJobStore',
                                               jobStoreID=None, predecessorNumber=0)
                    jobGraphs.append(jobstore.create(overlargeJobNode))
            for jobGraph in jobGraphs:
                self.assertTrue(jobstore.exists(jobGraph.jobStoreID))

        @travis_test
        def testGrowingAndShrinkingJob(self):
            """Make sure jobs update correctly if they grow/shrink."""
            # Make some very large data, large enough to trigger
            # overlarge job creation if that's a thing
            # (i.e. AWSJobStore)
            arbitraryLargeData = os.urandom(500000)
            job = self.jobstore_initialized.create(self.arbitraryJob)
            # Make the job grow
            job.foo_attribute = arbitraryLargeData
            self.jobstore_initialized.update(job)
            check_job = self.jobstore_initialized.load(job.jobStoreID)
            self.assertEqual(check_job.foo_attribute, arbitraryLargeData)
            # Make the job shrink back close to its original size
            job.foo_attribute = None
            self.jobstore_initialized.update(job)
            check_job = self.jobstore_initialized.load(job.jobStoreID)
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
            for test, store in iteritems(cls.externalStoreCache):
                logger.debug('Cleaning up external store for %s.', test)
                test._cleanUpExternalStore(store)

        mpTestPartSize = 5 << 20

        @classmethod
        def makeImportExportTests(cls):

            testClasses = [FileJobStoreTest, AWSJobStoreTest, GoogleJobStoreTest]

            activeTestClassesByName = {testCls.__name__: testCls
                                       for testCls in testClasses
                                       if not getattr(testCls, '__unittest_skip__', False)}

            def testImportExportFile(self, otherCls, size):
                """
                :param AbstractJobStoreTest.Test self: the current test case

                :param AbstractJobStoreTest.Test otherCls: the test case class for the job store
                       to import from or export to

                :param int size: the size of the file to test importing/exporting with
                """
                # Prepare test file in other job store
                self.jobstore_initialized.partSize = cls.mpTestPartSize
                # The string in otherCls() is arbitrary as long as it returns a class that has access
                # to ._externalStore() and ._prepareTestFile()
                other = otherCls('testSharedFiles')
                store = other._externalStore()

                srcUrl, srcMd5 = other._prepareTestFile(store, size)
                # Import into job store under test
                jobStoreFileID = self.jobstore_initialized.importFile(srcUrl)
                self.assertTrue(isinstance(jobStoreFileID, FileID))
                with self.jobstore_initialized.readFileStream(jobStoreFileID) as f:
                    fileMD5 = hashlib.md5(f.read()).hexdigest()
                self.assertEqual(fileMD5, srcMd5)
                # Export back into other job store
                dstUrl = other._prepareTestFile(store)
                self.jobstore_initialized.exportFile(jobStoreFileID, dstUrl)
                self.assertEqual(fileMD5, other._hashTestFile(dstUrl))
                if otherCls.__name__ == 'FileJobStoreTest':  # Remove local Files
                    os.remove(srcUrl[7:])
                    os.remove(dstUrl[7:])

            make_tests(testImportExportFile, cls, otherCls=activeTestClassesByName,
                       size=dict(zero=0,
                                 one=1,
                                 oneMiB=2 ** 20,
                                 partSizeMinusOne=cls.mpTestPartSize - 1,
                                 partSize=cls.mpTestPartSize,
                                 partSizePlusOne=cls.mpTestPartSize + 1))

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
                self.assertIsNone(self.jobstore_initialized.importFile(srcUrl, sharedFileName='foo'))
                with self.jobstore_initialized.readSharedFileStream('foo') as f:
                    fileMD5 = hashlib.md5(f.read()).hexdigest()
                self.assertEqual(fileMD5, srcMd5)
                if otherCls.__name__ == 'FileJobStoreTest':  # Remove local Files
                    os.remove(srcUrl[7:])

            make_tests(testImportSharedFile,
                       cls,
                       otherCls=activeTestClassesByName)

        @travis_test
        def testImportHttpFile(self):
            '''Test importing a file over HTTP.'''
            http = socketserver.TCPServer(('', 0), StubHttpRequestHandler)
            try:
                httpThread = threading.Thread(target=http.serve_forever)
                httpThread.start()
                try:
                    assignedPort = http.server_address[1]
                    url = 'http://localhost:%d' % assignedPort
                    with self.jobstore_initialized.readFileStream(
                            self.jobstore_initialized.importFile(url)) as readable:
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

        @travis_test
        def testImportFtpFile(self):
            '''Test importing a file over FTP'''
            ftpfile = {'name': 'foo', 'content': 'foo bar baz qux'}
            ftp = FTPStubServer(0)
            ftp.run()
            try:
                ftp.add_file(**ftpfile)
                assignedPort = ftp.server.server_address[1]
                url = 'ftp://user1:passwd@localhost:%d/%s' % (assignedPort, ftpfile['name'])
                with self.jobstore_initialized.readFileStream(self.jobstore_initialized.importFile(url)) as readable:
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
                job = self.jobstore_initialized.create(self.arbitraryJob)
                fileIDs = [self.jobstore_initialized.getEmptyFileStoreID(job.jobStoreID, cleanup=True) for _ in
                           range(0, numFiles)]
                self.jobstore_initialized.delete(job.jobStoreID)
                for fileID in fileIDs:
                    # NB: the fooStream() methods return context managers
                    self.assertRaises(NoSuchFileException, self.jobstore_initialized.readFileStream(fileID).__enter__)

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
            job = self.jobstore_initialized.create(self.arbitraryJob)

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
                        with self.jobstore_initialized.writeFileStream(job.jobStoreID, cleanup=True) as (
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
                with self.jobstore_initialized.readFileStream(fileId) as readable:
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
                    fileId = self.jobstore_initialized.writeFile(path, job.jobStoreID, cleanup=True)
                finally:
                    os.unlink(path)
                before = checksum.hexdigest()

                # Verify
                checksum = hashlib.md5()
                with self.jobstore_initialized.readFileStream(fileId) as readable:
                    while True:
                        buf = readable.read(bufSize)
                        if not buf:
                            break
                        checksum.update(buf)
                after = checksum.hexdigest()
                self.assertEqual(before, after)
            self.jobstore_initialized.delete(job.jobStoreID)

        @travis_test
        def testZeroLengthFiles(self):
            '''Test reading and writing of empty files.'''
            job = self.jobstore_initialized.create(self.arbitraryJob)
            nullFile = self.jobstore_initialized.writeFile('/dev/null', job.jobStoreID, cleanup=True)
            with self.jobstore_initialized.readFileStream(nullFile) as f:
                assert not f.read()
            with self.jobstore_initialized.writeFileStream(job.jobStoreID, cleanup=True) as (f, nullStream):
                pass
            with self.jobstore_initialized.readFileStream(nullStream) as f:
                assert not f.read()
            self.jobstore_initialized.delete(job.jobStoreID)

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
            job = self.jobstore_initialized.create(self.arbitraryJob)
            jobStoreFileID = self.jobstore_initialized.writeFile(filePath, job.jobStoreID, cleanup=True)

            # Remove the local file.
            os.unlink(filePath)

            # Write a local copy of the file from the jobstore.
            self.jobstore_initialized.readFile(jobStoreFileID, filePath)

            # Reread the file to confirm success.
            hashOut = hashlib.md5()
            with open(filePath, 'rb') as f:
                while True:
                    buf = f.read(self._partSize())
                    if not buf:
                        break
                    hashOut.update(buf)
            self.assertEqual(hashIn.digest(), hashOut.digest())

        def assertUrl(self, url):

            prefix, path = url.split(':', 1)
            if prefix == 'file':
                self.assertTrue(os.path.exists(path))
            else:
                try:
                    urlopen(Request(url))
                except:
                    self.fail()

        @slow
        def testCleanCache(self):
            # Make a bunch of jobs
            jobstore = self.jobstore_initialized

            # Create parent job
            rootJob = jobstore.createRootJob(self.arbitraryJob)
            # Create a bunch of child jobs
            for i in range(100):
                child = jobstore.create(self.arbitraryJob)
                rootJob.stack.append([child])
            jobstore.update(rootJob)

            # See how long it takes to clean with no cache
            noCacheStart = time.time()
            jobstore.clean()
            noCacheEnd = time.time()

            noCacheTime = noCacheEnd - noCacheStart

            # Make sure we have all the jobs: root and children.
            self.assertEqual(len(list(jobstore.jobs())), 101)

            # See how long it takes to clean with cache
            jobCache = {jobGraph.jobStoreID: jobGraph
                        for jobGraph in jobstore.jobs()}
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
        @travis_test
        @pytest.mark.timeout(45, method='thread')
        def testPartialReadFromStream(self):
            """Test whether readFileStream will deadlock on a partial read."""
            job = self.jobstore_initialized.create(self.arbitraryJob)
            with self.jobstore_initialized.writeFileStream(job.jobStoreID, cleanup=True) as (f, fileID):
                # Write enough data to make sure the writer thread
                # will get blocked on the write. Technically anything
                # greater than the pipe buffer size plus the libc
                # buffer size (64K + 4K(?))  should trigger this bug,
                # but this gives us a lot of extra room just to be sure.

                # python 3 requires self.fileContents to be a bytestring
                a = 'a'
                if sys.version_info >= (3, 0):
                    a = b'a'
                f.write(a * 300000)
            with self.jobstore_initialized.readFileStream(fileID) as f:
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

        @travis_test
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

        @travis_test
        def testEmptyFileStoreIDIsReadable(self):
            """Simply creates an empty fileStoreID and attempts to read from it."""
            id = self.jobstore_initialized.getEmptyFileStoreID()
            fh, path = tempfile.mkstemp()
            try:
                self.jobstore_initialized.readFile(id, path)
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


class AbstractEncryptedJobStoreTest(object):
    # noinspection PyAbstractClass
    class Test(with_metaclass(ABCMeta, AbstractJobStoreTest.Test)):
        """
        A test of job stores that use encryption
        """

        def setUp(self):
            # noinspection PyAttributeOutsideInit
            self.sseKeyDir = tempfile.mkdtemp()
            super(AbstractEncryptedJobStoreTest.Test, self).setUp()

        def tearDown(self):
            super(AbstractEncryptedJobStoreTest.Test, self).tearDown()
            shutil.rmtree(self.sseKeyDir)

        def _createConfig(self):
            config = super(AbstractEncryptedJobStoreTest.Test, self)._createConfig()
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
            phrase = 'This file is encrypted.'
            fileName = 'foo'
            with self.jobstore_initialized.writeSharedFileStream(fileName, isProtected=True) as f:
                f.write(phrase)
            with self.jobstore_initialized.readSharedFileStream(fileName) as f:
                self.assertEqual(phrase, f.read())

            # disable encryption
            self.jobstore_initialized.config.sseKey = None
            try:
                with self.jobstore_initialized.readSharedFileStream(fileName) as f:
                    self.assertEqual(phrase, f.read())
            except AssertionError as e:
                self.assertEqual("Content is encrypted but no key was provided.", e.message)
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
        import binascii
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
        localFilePath = FileJobStore._extractPathFromUrl(urlparse.urlparse(url))
        with open(localFilePath, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()

    def _createExternalStore(self):
        return tempfile.mkdtemp()

    def _cleanUpExternalStore(self, dirPath):
        shutil.rmtree(dirPath)

    @travis_test
    def testPreserveFileName(self):
        "Check that the fileID ends with the given file name."
        fh, path = tempfile.mkstemp()
        try:
            os.close(fh)
            job = self.jobstore_initialized.create(self.arbitraryJob)
            fileID = self.jobstore_initialized.writeFile(path, job.jobStoreID, cleanup=True)
            self.assertTrue(fileID.endswith(os.path.basename(path)))
        finally:
            os.unlink(path)


@needs_google
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
        url = 'gs://%s/%s' % (bucket.name, fileName)
        if size is None:
            return url
        read_type = 'r' if USING_PYTHON2 else 'rb'
        with open('/dev/urandom', read_type) as readable:
            if USING_PYTHON2:
                contents = readable.read(size)
            else:
                contents = str(readable.read(size))
        GoogleJobStore._writeToUrl(StringIO(contents), urlparse.urlparse(url))
        return url, hashlib.md5(contents).hexdigest()

    def _hashTestFile(self, url):
        from toil.jobStores.googleJobStore import GoogleJobStore
        contents = GoogleJobStore._getBlobFromURL(urlparse.urlparse(url)).download_as_string()
        return hashlib.md5(contents).hexdigest()

    @googleRetry
    def _createExternalStore(self):
        from google.cloud import storage
        bucketName = ("import-export-test-" + str(uuid.uuid4()))
        storageClient = storage.Client()
        return storageClient.create_bucket(bucketName)

    @googleRetry
    def _cleanUpExternalStore(self, bucket):
        # this is copied from googleJobStore.destroy
        try:
            bucket.delete(force=True)
            # throws ValueError if bucket has more than 256 objects. Then we must delete manually
        except ValueError:
            bucket.delete_blobs(bucket.list_blobs)
            bucket.delete()


@needs_aws_ec2
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
        from boto.s3.connection import Location, S3Connection
        from toil.jobStores.aws.jobStore import BucketLocationConflictException
        from toil.jobStores.aws.utils import retry_s3
        externalAWSLocation = Location.USWest
        for testRegion in 'us-east-1', 'us-west-2':
            # We run this test twice, once with the default s3 server us-east-1 as the test region
            # and once with another server (us-west-2).  The external server is always us-west-1.
            # This incidentally tests that the BucketLocationConflictException is thrown when using
            # both the default, and a non-default server.
            testJobStoreUUID = str(uuid.uuid4())
            # Create the bucket at the external region
            s3 = S3Connection()
            for attempt in retry_s3(delays=(2, 5, 10, 30, 60), timeout=600):
                with attempt:
                    bucket = s3.create_bucket('domain-test-' + testJobStoreUUID + '--files',
                                              location=externalAWSLocation)
            options = Job.Runner.getDefaultOptions('aws:' + testRegion + ':domain-test-' +
                                                   testJobStoreUUID)
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
                for attempt in retry_s3():
                    with attempt:
                        s3.delete_bucket(bucket=bucket)

    @slow
    def testInlinedFiles(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        jobstore = self.jobstore_initialized
        for encrypted in (True, False):
            n = AWSJobStore.FileInfo.maxInlinedSize()
            sizes = (1, old_div(n, 2), n - 1, n, n + 1, 2 * n)
            for size in chain(sizes, islice(reversed(sizes), 1)):
                s = os.urandom(size)
                with jobstore.writeSharedFileStream('foo') as f:
                    f.write(s)
                with jobstore.readSharedFileStream('foo') as f:
                    self.assertEqual(s, f.read())

    def testOverlargeJob(self):
        jobstore = self.jobstore_initialized
        jobRequirements = dict(memory=12, cores=34, disk=35, preemptable=True)
        overlargeJobNode = JobNode(command='overlarge',
                                   requirements=jobRequirements,
                                   jobName='test-overlarge', unitName='onJobStore',
                                   jobStoreID=None, predecessorNumber=0)

        # Make the pickled size of the job larger than 256K
        read_type = 'r' if USING_PYTHON2 else 'rb'
        with open("/dev/urandom", read_type) as random:
            if USING_PYTHON2:
                overlargeJobNode.jobName = random.read(512 * 1024)
            else:
                overlargeJobNode.jobName = str(random.read(512 * 1024))
        overlargeJob = jobstore.create(overlargeJobNode)
        self.assertTrue(jobstore.exists(overlargeJob.jobStoreID))
        overlargeJobDownloaded = jobstore.load(overlargeJob.jobStoreID)
        jobsInJobStore = [job for job in jobstore.jobs()]
        self.assertEqual(jobsInJobStore, [overlargeJob])
        jobstore.delete(overlargeJob.jobStoreID)

    def _prepareTestFile(self, bucket, size=None):
        fileName = 'testfile_%s' % uuid.uuid4()
        url = 's3://%s/%s' % (bucket.name, fileName)
        if size is None:
            return url
        read_type = 'r' if USING_PYTHON2 else 'rb'
        with open('/dev/urandom', read_type) as readable:
            if USING_PYTHON2:
                bucket.new_key(fileName).set_contents_from_string(readable.read(size))
            else:
                bucket.new_key(fileName).set_contents_from_string(str(readable.read(size)))
        return url, hashlib.md5(bucket.get_key(fileName).get_contents_as_string()).hexdigest()

    def _hashTestFile(self, url):
        from toil.jobStores.aws.jobStore import AWSJobStore
        key = AWSJobStore._getKeyForUrl(urlparse.urlparse(url), existing=True)
        try:
            contents = key.get_contents_as_string()
        finally:
            key.bucket.connection.close()
        return hashlib.md5(contents).hexdigest()

    def _createExternalStore(self):
        import boto.s3
        from toil.jobStores.aws.utils import region_to_bucket_location
        s3 = boto.s3.connect_to_region(self.awsRegion())
        try:
            return s3.create_bucket(bucket_name='import-export-test-%s' % uuid.uuid4(),
                                    location=region_to_bucket_location(self.awsRegion()))
        except:
            with panic(log=logger):
                s3.close()

    def _cleanUpExternalStore(self, bucket):
        try:
            for key in bucket.list():
                key.delete()
            bucket.delete()
        finally:
            bucket.connection.close()

    def _largeLogEntrySize(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        # So we get into the else branch of reader() in uploadStream(multiPart=False):
        return AWSJobStore.FileInfo.maxBinarySize() * 2

    def _batchDeletionSize(self):
        from toil.jobStores.aws.jobStore import AWSJobStore
        return AWSJobStore.itemsPerBatchDelete


@needs_aws_ec2
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


@needs_aws_ec2
@needs_encryption
@slow
class EncryptedAWSJobStoreTest(AWSJobStoreTest, AbstractEncryptedJobStoreTest.Test):
    pass


class StubHttpRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    fileContents = 'A good programmer looks both ways before crossing a one-way street'

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.send_header("Content-length", len(self.fileContents))
        self.end_headers()
        # python 3 requires self.fileContents to be a bytestring
        if sys.version_info >= (3, 0):
            self.fileContents = self.fileContents.encode('utf-8')
        self.wfile.write(self.fileContents)


AbstractJobStoreTest.Test.makeImportExportTests()
