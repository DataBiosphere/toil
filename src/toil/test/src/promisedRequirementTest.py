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
import os, fcntl, time, tempfile, multiprocessing
import logging
from toil.job import Job
from toil.job import PromisedRequirement
from toil.test import ToilTest

log = logging.getLogger(__name__)

class MaxCoresPromisedRequirementTest(ToilTest):
    """
    This test ensures that the PromisedRequirement method properly assigns
    resource requirements to the single machine batch system.
    """

    def setUp(self):
        super(MaxCoresPromisedRequirementTest, self).setUp()
        logging.basicConfig(level=logging.INFO)

        self.counterPath = writeTempFile('0,0')
        self.cpu_count = multiprocessing.cpu_count()

    def test_dynamic(self):
        """
        Tests using a PromisedRequirement object to allocate the number of cores per function
        """
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"
        Job.Runner.startToil(Job.wrapJobFn(start, self.cpu_count, self.counterPath, 1), options)
        n, m = getCounters(self.counterPath)
        self.assertEqual((n, m), (0, self.cpu_count))

        resetCounters(self.counterPath)
        Job.Runner.startToil(Job.wrapJobFn(start, self.cpu_count, self.counterPath, 2), options)
        n, m = getCounters(self.counterPath)
        self.assertEqual((n, m), (0, self.cpu_count / 2))

    def test_static(self):
        """
        Tests using a PromisedRequirement to allocate number of cores in a static DAG.
        """
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"
        Job.Runner.startToil(Job.wrapJobFn(static_start, self.cpu_count, self.counterPath, 1), options)
        n, m = getCounters(self.counterPath)
        self.assertEqual((n, m), (0, self.cpu_count))

        resetCounters(self.counterPath)
        Job.Runner.startToil(Job.wrapJobFn(static_start, self.cpu_count, self.counterPath, 2), options)
        n, m = getCounters(self.counterPath)
        self.assertEqual((n, m), (0, self.cpu_count / 2))


def start(job, cpu_count, filename, scale):
    """
    Tests dynamic job function creation using a PromisedRequirement.
    """
    child = job.addChildFn(one)
    for _ in range(cpu_count):
        job.addFollowOnFn(increment_counter, filename,
                          cores=PromisedRequirement(lambda x: scale * x, child.rv()),
                          memory='1M')


def static_start(job, cpu_count, filename, scale):
    """
    Tests static job function creation with a PromisedRequirement
    """
    A = Job.wrapFn(one)
    job.addChild(A)
    for _ in range(cpu_count):
        A.addFollowOn(Job.wrapFn(increment_counter, filename,
                                 cores=PromisedRequirement(lambda x: scale * x, A.rv()),
                                 memory='1M'))

def one():
    return 1


def increment_counter(filepath):
    """
    Without the second argument, increment counter, sleep one second and decrement.
    Othwerise, adjust the counter by the given delta, which can be useful for services.

    This code was copied from toil.batchSystemTestMaxCoresSingleMachineBatchSystemTest
    """
    count(1, filepath)
    try:
        time.sleep(5)
    finally:
        count(-1, filepath)


def count(delta, filename="testPromisedRequirement"):
    """
    Adjust the first integer value in a file by the given amount. If the result
    exceeds the second integer value, set the second one to the first.

    This code was copied from toil.batchSystemTestMaxCoresSingleMachineBatchSystemTest
    """
    fd = os.open(filename, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            s = os.read(fd, 10)
            value, maxValue = map(int, s.split(','))
            value += delta
            if value > maxValue: maxValue = value
            os.lseek(fd, 0, 0)
            os.ftruncate(fd, 0)
            os.write(fd, ','.join(map(str, (value, maxValue))))
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


# Make a temporary file
def writeTempFile(s):
    fd, path = tempfile.mkstemp()
    try:
        assert os.write(fd, s) == len(s)
    except:
        os.unlink(path)
        raise
    else:
        return path
    finally:
        os.close(fd)


def getCounters(path):
    with open(path, 'r+') as f:
        s = f.read()
        concurrentTasks, maxConcurrentTasks = map(int, s.split(','))
    return concurrentTasks, maxConcurrentTasks


def resetCounters(path):
    with open(path, "w") as f:
        f.write("0,0")
        f.close()

