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

from __future__ import absolute_import, print_function
from __future__ import division
from builtins import range
from builtins import object
from past.utils import old_div
import os
import logging
import time
import toil.test.batchSystems.batchSystemTest as batchSystemTest

from toil.job import Job
from toil.job import PromisedRequirement
from toil.test import needs_mesos, slow, travis_test
from toil.batchSystems.mesos.test import MesosTestSupport

log = logging.getLogger(__name__)


class hidden(object):
    """
    Hide abstract base class from unittest's test case loader.

    http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class#answer-25695512
    """

    class AbstractPromisedRequirementsTest(batchSystemTest.hidden.AbstractBatchSystemJobTest):
        """
        An abstract base class for testing Toil workflows with promised requirements.
        """

        @slow
        def testConcurrencyDynamic(self):
            """
            Asserts that promised core resources are allocated properly using a dynamic Toil workflow
            """
            for coresPerJob in self.allocatedCores:
                log.debug('Testing %d cores per job with CPU count %d', coresPerJob, self.cpuCount)
                tempDir = self._createTempDir('testFiles')
                counterPath = self.getCounterPath(tempDir)

                root = Job.wrapJobFn(maxConcurrency, self.cpuCount, counterPath, coresPerJob,
                                     cores=1, memory='1M', disk='1M')
                values = Job.Runner.startToil(root, self.getOptions(tempDir))
                maxValue = max(values)
                self.assertEqual(maxValue, old_div(self.cpuCount, coresPerJob))

        @slow
        def testConcurrencyStatic(self):
            """
            Asserts that promised core resources are allocated properly using a static DAG
            """
            for coresPerJob in self.allocatedCores:
                log.debug('Testing %d cores per job with CPU count %d', coresPerJob, self.cpuCount)
                tempDir = self._createTempDir('testFiles')
                counterPath = self.getCounterPath(tempDir)

                root = Job()
                one = Job.wrapFn(getOne, cores=0.1, memory='32M', disk='1M')
                thirtyTwoMb = Job.wrapFn(getThirtyTwoMb, cores=0.1, memory='32M', disk='1M')
                root.addChild(one)
                root.addChild(thirtyTwoMb)
                for _ in range(self.cpuCount):
                    root.addFollowOn(Job.wrapFn(batchSystemTest.measureConcurrency, counterPath,
                                                cores=PromisedRequirement(lambda x: x * coresPerJob, one.rv()),
                                                memory=PromisedRequirement(thirtyTwoMb.rv()),
                                                disk='1M'))
                Job.Runner.startToil(root, self.getOptions(tempDir))
                _, maxValue = batchSystemTest.getCounters(counterPath)
                self.assertEqual(maxValue, old_div(self.cpuCount, coresPerJob))

        def getOptions(self, tempDir, caching=True):
            options = super(hidden.AbstractPromisedRequirementsTest, self).getOptions(tempDir)
            # defaultCores defaults to 1 - this is coincidentally the core requirement relied upon by this
            # test, so we change defaultCores to 2 to make the test more strict
            options.defaultCores = 2
            if not caching:
                options.disableCaching = True
            return options

        def getCounterPath(self, tempDir):
            """
            Returns path to a counter file
            :param str tempDir: path to test directory
            :return: path to counter file
            """
            counterPath = os.path.join(tempDir, 'counter')
            batchSystemTest.resetCounters(counterPath)
            minValue, maxValue = batchSystemTest.getCounters(counterPath)
            assert (minValue, maxValue) == (0, 0)
            return counterPath

        @travis_test
        def testJobConcurrency(self):
            pass
        
        @travis_test
        def testPromisesWithJobStoreFileObjects(self, caching=True):
            """
            Check whether FileID objects are being pickled properly when used as return
            values of functions.  Then ensure that lambdas of promised FileID objects can be
            used to describe the requirements of a subsequent job.  This type of operation will be
            used commonly in Toil scripts.
            :return: None
            """
            file1 = 1024
            file2 = 512
            F1 = Job.wrapJobFn(_writer, file1)
            F2 = Job.wrapJobFn(_writer, file2)
            G = Job.wrapJobFn(_follower, file1 + file2,
                              disk=PromisedRequirement(lambda x, y: x.size + y.size,
                                                       F1.rv(), F2.rv()))
            F1.addChild(F2)
            F2.addChild(G)

            Job.Runner.startToil(F1, self.getOptions(self._createTempDir('testFiles'), caching=caching))
        
        @travis_test
        def testPromisesWithNonCachingFileStore(self):
            self.testPromisesWithJobStoreFileObjects(caching=False)

        @slow
        def testPromiseRequirementRaceStatic(self):
            """
            Checks for a race condition when using promised requirements and child job functions.
            """
            A = Job.wrapJobFn(logDiskUsage, 'A', sleep=5, disk=PromisedRequirement(1024))
            B = Job.wrapJobFn(logDiskUsage, 'B', disk=PromisedRequirement(lambda x: x + 1024, A.rv()))
            A.addChild(B)
            Job.Runner.startToil(A, self.getOptions(self._createTempDir('testFiles')))


def _writer(job, fileSize):
    '''
    Write a local file and return the FileID obtained from running writeGlobalFile on
    it.

    :param job job: job
    :param int fileSize: Size of the file in bytes
    :returns: the result of writeGlobalFile on a locally created file
    :rtype: job.FileID
    '''
    with open(job.fileStore.getLocalTempFileName(), 'wb') as fH:
        fH.write(os.urandom(fileSize))
    return job.fileStore.writeGlobalFile(fH.name)


def _follower(job, expectedDisk):
    """
    This job follows the _writer jobs and ensures the expected disk is used.

    :param job job: job
    :param expectedDisk: Expect disk to be used by this job
    :return: None
    """
    assert job.disk == expectedDisk


def maxConcurrency(job, cpuCount, filename, coresPerJob):
    """
    Returns the max number of concurrent tasks when using a PromisedRequirement instance
    to allocate the number of cores per job.

    :param int cpuCount: number of available cpus
    :param str filename: path to counter file
    :param int coresPerJob: number of cores assigned to each job
    :return int max concurrency value:
    """
    one = job.addChildFn(getOne, cores=0.1, memory='32M', disk='1M')
    thirtyTwoMb = job.addChildFn(getThirtyTwoMb, cores=0.1, memory='32M', disk='1M')

    values = []
    for _ in range(cpuCount):
        value = job.addFollowOnFn(batchSystemTest.measureConcurrency, filename,
                                  cores=PromisedRequirement(lambda x: x * coresPerJob, one.rv()),
                                  memory=PromisedRequirement(thirtyTwoMb.rv()),
                                  disk='1M').rv()
        values.append(value)
    return values


def getOne():
    return 1


def getThirtyTwoMb():
    return '32M'


def logDiskUsage(job, funcName, sleep=0):
    """
    Logs the job's disk usage to master and sleeps for specified amount of time.

    :return: job function's disk usage
    """
    diskUsage = job.disk
    job.fileStore.logToMaster('{}: {}'.format(funcName, diskUsage))
    time.sleep(sleep)
    return diskUsage


class SingleMachinePromisedRequirementsTest(hidden.AbstractPromisedRequirementsTest):
    """
    Tests against the SingleMachine batch system
    """

    def getBatchSystemName(self):
        return "singleMachine"

    def tearDown(self):
        pass


@needs_mesos
class MesosPromisedRequirementsTest(hidden.AbstractPromisedRequirementsTest, MesosTestSupport):
    """
    Tests against the Mesos batch system
    """

    def getOptions(self, tempDir, caching=True):
        options = super(MesosPromisedRequirementsTest, self).getOptions(tempDir, caching=caching)
        options.mesosMasterAddress = 'localhost:5050'
        return options

    def getBatchSystemName(self):
        self._startMesos(self.cpuCount)
        return "mesos"

    def tearDown(self):
        self._stopMesos()

