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
import sys
from subprocess import check_call
from toil.job import Job
from toil.test import ToilTest


class UserDefinedJobArgTypeTest(ToilTest):
    """
    Test for issue #423 (Toil can't unpickle classes defined in user scripts) and variants
    thereof.

    https://github.com/BD2KGenomics/toil/issues/423
    """

    def setUp(self):
        super(UserDefinedJobArgTypeTest, self).setUp()
        options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
        options.logLevel = "INFO"
        options.foo = Foo()
        self.options = options

    def testJobFunction(self):
        """Test with first job being a function"""
        Job.Runner.startToil(Job.wrapJobFn(jobFunction, 0, Foo()), self.options)

    def testJobClass(self):
        """Test with first job being an instance of a class"""
        Job.Runner.startToil(JobClass(0, Foo()), self.options)

    def testJobFunctionFromMain(self):
        """Test with first job being a function defined in __main__"""
        self._testFromMain()

    def testJobClassFromMain(self):
        """Test with first job being an instance of a class defined in __main__"""
        self._testFromMain()

    def _testFromMain(self):
        testMethodName = self.id().split('.')[-1]
        self.assertTrue(testMethodName.endswith('FromMain'))
        check_call([sys.executable, '-m', self.__module__, testMethodName[:-8]])


class JobClass(Job):
    def __init__(self, level, foo):
        Job.__init__(self, memory=100000, cores=2, disk="3G")
        self.level = level
        self.foo = foo

    def run(self, fileStore):
        self.foo.assertIsCopy()
        if self.level < 2:
            self.addChildJobFn(jobFunction, self.level + 1, Foo(), cores=1, memory="1M", disk="30G")


def jobFunction(job, level, foo):
    foo.assertIsCopy()
    if level < 2:
        job.addChild(JobClass(level + 1, Foo()))


class Foo(object):
    def __init__(self):
        super(Foo, self).__init__()
        self.original_id = id(self)

    def assertIsCopy(self):
        assert self.original_id != id(self)


def main():
    testMethodName = sys.argv[1]
    test = UserDefinedJobArgTypeTest(testMethodName)
    test.setUpClass()
    test.debug()
    test.tearDownClass()
