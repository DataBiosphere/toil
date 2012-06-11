
#Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
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
from jobTree.test.jobTree.jobTreeTest import TestCase as jobTreeTest
from jobTree.test.jobTree.jobTest import TestCase as jobTest
from jobTree.test.scriptTree.scriptTreeTest import TestCase as scriptTreeTest
from jobTree.test.sort.sortTest import TestCase as sortTest
from jobTree.test.utilities.statsTest import TestCase as statsTest
#import jobTree.test.jobTreeParasolCrashTest.TestCase as jobTreeParasolCrashTest

from sonLib.bioio import parseSuiteTestOptions

def allSuites():
    jobTestSuite = unittest.makeSuite(jobTest, 'test')
    jobTreeTestSuite = unittest.makeSuite(jobTreeTest, 'test')
    scriptTreeSuite = unittest.makeSuite(scriptTreeTest, 'test')
    sortSuite = unittest.makeSuite(sortTest, 'test')
    statsSuite = unittest.makeSuite(statsTest, 'test')
    allTests = unittest.TestSuite((jobTestSuite, jobTreeTestSuite, scriptTreeSuite, sortSuite, statsSuite))
    return allTests
        
def main(): 
    parseSuiteTestOptions()
    suite = allSuites()
    runner = unittest.TextTestRunner()
    i = runner.run(suite)
    return len(i.failures) + len(i.errors)
        
if __name__ == '__main__':
    import cProfile
    cProfile.run('main()', "fooprof")
    import pstats
    p = pstats.Stats('fooprof')
    p.strip_dirs().sort_stats(-1).print_stats()
    print p
    #import sys
    #sys.exit(main())
