import unittest
import os
from optparse import OptionParser
from jobTree.lib.bioio import getTempFile
from jobTree.src.target import Target
from jobTree.src.stack import Stack

class TestCase(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def testStatic1(self):
        """Create a tree of targets non-dynamically and run it.
        """
        #Temporary file
        outFile = getTempFile(rootDir=os.getcwd())
        
        #Create the targets
        tA = Target.wrapFn(f, args=("A", outFile))
        tB = Target.wrapFn(f)
        tC = Target.wrapFn(f)
        tD = Target.wrapFn(f)
        tE = Target.wrapFn(f, kwargs={"string":"E"})
        
        #Connect them into a workflow, using rMap to map return values to inputs
        tA.addChild(tB, rMap={"string":0, "outputFile":1 }) 
        tB.addChild(tC, rMap={"string":0, "outputFile":1 }) 
        tC.setFollowOn(tD, rMap={"string":0, "outputFile":1 }) 
        tA.setFollowOn(tE, rMap={"outputFile":1 })
        
        #Create the runner for the workflow.
        #TODO - fix the method for specifying arguments to a jobTree, because
        #assuming parsing command line inputs is shitty
        parser = OptionParser()
        Stack.addJobTreeOptions(parser)
        options, args = parser.parse_args()
        s = Stack(tA)
        #Run the workflow, the return value being the number of failed jobs
        self.assertEquals(s.startJobTree(options), 0)
        s.cleanup(options) #This removes the jobStore
        
        #Check output
        self.assertEquals(open(outFile, 'r').readline(), "ABCDE")
        
        #Cleanup
        os.remove(outFile)
        
def f(string, outputFile):
    fH = open(outputFile, 'a')
    fH.write(string)
    fH.close()   
    return chr(ord(string)+1), outputFile     

if __name__ == '__main__':
    from jobTree.test.staticDeclaration.staticTest import *
    unittest.main()