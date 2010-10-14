import unittest

import jobTree.allTests
from workflow.jobTree.lib.bioio import parseSuiteTestOptions

def allSuites():
    jobTreeSuite = jobTree.allTests.allSuites()
    allTests = unittest.TestSuite((jobTreeSuite))
    return allTests
        
def main():
    parseSuiteTestOptions()
    
    suite = allSuites()
    runner = unittest.TextTestRunner()
    runner.run(suite)
        
if __name__ == '__main__':
    main()