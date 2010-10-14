import unittest
from workflow.jobTree.test.jobTree.jobTreeTest import TestCase as jobTreeTest
from workflow.jobTree.test.scriptTree.scriptTreeTest import TestCase as scriptTreeTest
from workflow.jobTree.test.sort.sortTest import TestCase as sortTest
from workflow.jobTree.test.utilities.statsTest import TestCase as statsTest
#import workflow.jobTree.test.jobTreeParasolCrashTest.TestCase as jobTreeParasolCrashTest

from workflow.jobTree.lib.bioio import parseSuiteTestOptions

def allSuites():
    jobTreeTestSuite = unittest.makeSuite(jobTreeTest, 'test')
    scriptTreeSuite = unittest.makeSuite(scriptTreeTest, 'test')
    sortSuite = unittest.makeSuite(sortTest, 'test')
    statsSuite = unittest.makeSuite(statsTest, 'test')
    allTests = unittest.TestSuite((jobTreeTestSuite, scriptTreeSuite, sortSuite, statsSuite))
    return allTests
        
def main(): 
    parseSuiteTestOptions()
    
    suite = allSuites()
    runner = unittest.TextTestRunner()
    runner.run(suite)
        
if __name__ == '__main__':
    main()