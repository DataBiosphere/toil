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

from __future__ import absolute_import, print_function
import os
import logging
import toil.test.batchSystems.batchSystemTest as batchSystemTest

from toil.job import Job
from toil.job import PromisedRequirement
from toil.test import needs_mesos
from toil.batchSystems.mesos.test import MesosTestSupport

log = logging.getLogger(__name__)


class hidden:
    """
    Hide abstract base class from unittest's test case loader.

    http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class#answer-25695512
    """

    class AbstractPromisedRequirementsTest(batchSystemTest.hidden.AbstractBatchSystemJobTest):
        """
        An abstract base class for testing Toil workflows with promised requirements.
        """

        def testPromisedRequirementDynamic(self):
            """
            Asserts that promised core resources are allocated properly using a dynamic Toil workflow
            """
            for coresPerJob in self.allocatedCores:
                tempDir = self._createTempDir('testFiles')
                counterPath = self.getCounterPath(tempDir)

                root = Job.wrapJobFn(maxConcurrency, self.cpuCount, counterPath, coresPerJob,
                                     cores=1, memory='1M', disk='1M')
                values = Job.Runner.startToil(root, self.getOptions(tempDir))
                maxValue = max(values)
                self.assertEqual(maxValue, self.cpuCount / coresPerJob)

        def testPromisedRequirementStatic(self):
            """
            Asserts that promised core resources are allocated properly using a static DAG
            """
            for coresPerJob in self.allocatedCores:
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
                self.assertEqual(maxValue, self.cpuCount / coresPerJob)

        def getOptions(self, tempDir):
            """
            Configures options for Toil workflow and makes job store.
            :param str tempDir: path to test directory
            :return: Toil options object
            """
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "DEBUG"
            options.batchSystem = self.batchSystemName
            options.workDir = tempDir
            options.maxCores = self.cpuCount
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

        def testJobConcurrency(self):
            """
            Override test in batchSystemTest.hidden.AbstractBatchSystemJobTest
            """
            pass


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

    def getBatchSystemName(self):
        self._startMesos(self.cpuCount)
        return "mesos"

    def tearDown(self):
        self._stopMesos()

