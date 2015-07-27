import os
from toil.lib.bioio import getTempFile
from toil.target import Target
from toil.test import ToilTest

class TargetServiceTest(ToilTest):
    """
    Tests testing the Target.Service class
    """

    def testService(self):
        """
        Tests the creation of a Target.Service.
        """
        #Temporary file
        outFile = getTempFile(rootDir=os.getcwd())
        #Wire up the services/targets
        t = Target.wrapFn(f, "1", outFile)
        t.addChildFn(f, t.addService(TestService("2", "3", outFile)), outFile)
        #Create the runner for the workflow.
        options = Target.Runner.getDefaultOptions()
        options.logLevel = "INFO"
        #Run the workflow, the return value being the number of failed jobs
        self.assertEquals(Target.Runner.startToil(t, options), 0)
        Target.Runner.cleanup(options) #This removes the jobStore
        #Check output
        self.assertEquals(open(outFile, 'r').readline(), "123")
        #Cleanup
        os.remove(outFile)
        
class TestService(Target.Service):
    def __init__(self, startString, stopString, outFile):
        Target.Service.__init__(self)
        self.startString = startString
        self.stopString = stopString
        self.outFile = outFile
        
    def start(self):
        return self.startString
    
    def stop(self):
        f(self.stopString, self.outFile)

def f(string, outFile):
    """
    Function appends string to output file
    """
    with open(outFile, 'a') as fH:
        fH.write(string)