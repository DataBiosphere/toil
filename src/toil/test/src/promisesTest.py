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
from toil.job import Job
from toil.test import ToilTest


class CachedUnpicklingJobStoreTest(ToilTest):
    """
    https://github.com/BD2KGenomics/toil/issues/817
    """

    def test(self):
        """
        Runs two identical Toil workflows with different job store paths
        """
        for _ in range(2):
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = 'INFO'
            root = Job.wrapJobFn(parent)
            Job.Runner.startToil(root, options)


def parent(job):
    return job.addChildFn(child).rv()


def child():
    return 1


class ChainedIndexedPromisesTest(ToilTest):
    """
    https://github.com/BD2KGenomics/toil/issues/1021
    """

    def test(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = 'INFO'
        root = Job.wrapJobFn(a)
        self.assertEquals(Job.Runner.startToil(root, options), 42)


def a(job):
    return job.addChild(job.wrapJobFn(b)).rv(0)


def b(job):
    return job.addChild(job.wrapFn(c)).rv()


def c():
    return 42, 43


class PathIndexingPromiseTest(ToilTest):
    """
    Test support for indexing promises of arbitrarily nested data structures of lists, dicts and
    tuples, or any other object supporting the __getitem__() protocol.
    """

    def test(self):
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = 'INFO'
        root = Job.wrapJobFn(d)
        self.assertEquals(Job.Runner.startToil(root, options), ('b', 43, 3))


def d(job):
    child = job.addChild(job.wrapFn(e))
    return child.rv('a'), child.rv(42), child.rv('c', 2)


def e():
    return {'a': 'b', 42: 43, 'c': [1, 2, 3]}
