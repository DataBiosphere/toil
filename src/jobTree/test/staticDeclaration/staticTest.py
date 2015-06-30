import unittest
import os

from jobTree.lib.bioio import getTempFile
from jobTree.target import Target
from jobTree.test import JobTreeTest

class StaticTest(JobTreeTest):
    """
    Tests creating a jobTree inline.
    """

    def testStatic1(self):
        """
        Create a dag of targets non-dynamically and run it.
        
        A - F
        \-------
        B -> D \ 
         \       \
          ------- C -> E
          
        Follow on is marked by ->
        """
        #Temporary file
        outFile = getTempFile(rootDir=os.getcwd())
        
        #Create the targets
        A = Target.wrapFn(f, "A", outFile)
        B = Target.wrapFn(f, A.rv(0), outFile)
        C = Target.wrapFn(f, B.rv(0), outFile)
        D = Target.wrapFn(f, C.rv(0), outFile)
        E = Target.wrapFn(f, D.rv(0), outFile)
        F = Target.wrapFn(f, E.rv(0), outFile)
        
        #Connect them into a workflow
        A.addChild(B)
        A.addChild(C)
        B.addChild(C)
        B.setFollowOn(D)
        C.setFollowOn(E)
        A.setFollowOn(F)
        
        #Create the runner for the workflow.
        options = Target.Runner.getDefaultOptions()
        options.logLevel = "INFO"
        #Run the workflow, the return value being the number of failed jobs
        self.assertEquals(Target.Runner.startJobTree(A, options), 0)
        Target.Runner.cleanup(options) #This removes the jobStore
        
        #Check output
        self.assertEquals(open(outFile, 'r').readline(), "ABCDEF")
        
        #Cleanup
        os.remove(outFile)
        
def f(string, outFile):
    fH = open(outFile, 'a')
    fH.write(string)
    fH.close()   
    return chr(ord(string[0])+1)     

if __name__ == '__main__':
    unittest.main()