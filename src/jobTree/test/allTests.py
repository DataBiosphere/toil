# Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.

import unittest
from jobTree.test.mesos.mesosTest import MesosTest

from jobTree.test.sort.sortTest import SortTest
from jobTree.test.staticDeclaration.staticTest import StaticTest
from jobTree.test.src.jobTest import JobTest
from jobTree.test.utils.statsTest import StatsTest
from jobTree.test.dependencies.dependenciesTest import DependenciesTest
from jobTree.test.batchSystems.abstractBatchSystemTest import SingleMachineBatchSystemTest, MesosBatchSystemTest
from jobTree.test.jobStores.jobStoreTest import AWSJobStoreTest, FileJobStoreTest

from jobTree.lib.bioio import parseSuiteTestOptions, getBasicOptionParser

testCases = {c.__name__[:-4].lower(): c for c in ( JobTest, DependenciesTest, SortTest, StatsTest, StaticTest,
                                                   MesosTest,
                                                   AWSJobStoreTest, FileJobStoreTest,
                                                   SingleMachineBatchSystemTest, MesosBatchSystemTest )}


def allSuites(options):
    return unittest.TestSuite(unittest.makeSuite(module, 'test')
                              for name, module in testCases.iteritems()
                              if name in options.tests)


def initializeOptions(parser):
    parser.add_option('--tests',
                      help='Comma-separated list of tests. Omit to run all tests. '
                           'Valid choices: %s' % testCases.keys())


def checkOptions(options, parser):
    if options.tests is None:
        options.tests = testCases.keys()
    else:
        requested_tests = options.tests.split(',')
        for t in requested_tests:
            if t not in testCases.keys():
                parser.error('Unknown test %s. Must be one or more of %s' % (t, str(testCases.keys())))
        options.tests = requested_tests


def main():
    parser = getBasicOptionParser()
    initializeOptions(parser)
    options, args = parseSuiteTestOptions(parser)
    checkOptions(options, parser)
    suite = allSuites(options)
    runner = unittest.TextTestRunner()
    # remove test options, pass remaining options to jobTree code under test
    sys.argv[1:] = args
    i = runner.run(suite)
    return len(i.failures) + len(i.errors)


if __name__ == '__main__':
    if False:
        import cProfile
        cProfile.run('main()', "fooprof")
        import pstats
        p = pstats.Stats('fooprof')
        p.strip_dirs().sort_stats(-1).print_stats()

    import sys
    sys.exit(main())
