import unittest
import os
from optparse import OptionParser
from jobTree.lib.bioio import getTempFile
from jobTree.src.target import Target
from jobTree.src.stack import Stack

"""
Tests creating a jobTree inline.
"""

class TestCase(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)

    def tearDown(self):
        unittest.TestCase.tearDown(self)

    def testStatic1(self):
        """
        Create a tree of targets non-dynamically and run it.
        """
        #Temporary file
        outFile = getTempFile(rootDir=os.getcwd())
        
        #Create the targets
        A = Target.wrapFn(f, "A", outFile)
        B = Target.wrapFn(f, A.rv(0), outFile)
        C = Target.wrapFn(f, B.rv(0), outFile)
        D = Target.wrapFn(f, C.rv(0), outFile)
        E = Target.wrapFn(f, D.rv(0), outFile)
        
        #Connect them into a workflow
        A.addChild(B)
        B.addChild(C) 
        B.setFollowOn(D) 
        A.setFollowOn(E)
        
        #Create the runner for the workflow.
        options = Stack.getDefaultOptions()
        options.logLevel = "INFO"
        s = Stack(A)
        #Run the workflow, the return value being the number of failed jobs
        self.assertEquals(s.startJobTree(options), 0)
        s.cleanup(options) #This removes the jobStore
        
        #Check output
        self.assertEquals(open(outFile, 'r').readline(), "ABCDE")
        
        #Cleanup
        os.remove(outFile)
        
def f(string, outFile):
    fH = open(outFile, 'a')
    fH.write(string)
    fH.close()   
    return chr(ord(string[0])+1)     

if __name__ == '__main__':
    unittest.main()