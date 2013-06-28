#!/usr/bin/env python

"""Script is part of tests of scriptTree.

This test checks:
that the local temp dir is always empty when a target starts.
that the global temp dir is shared between a job and its follow on only.
"""
import os
import random
from optparse import OptionParser

from sonLib.bioio import getRandomAlphaNumericString

from jobTree.scriptTree.target import Target
from jobTree.scriptTree.stack import Stack
from scriptTreeTest_Wrapper import Target2 #Relative import!

class Target1(Target):
    """This target creates children and a follow on and examines
    the temp dirs it is handed.
    """
    def __init__(self, depth=0):
        Target.__init__(self, time=random.random() * 10)
        self.tempFileName = getRandomAlphaNumericString()
        self.depth = depth
    
    def run(self):
        #Check the local temp file dir is empty
        assert os.listdir(self.getLocalTempDir()) == []
        
        fileHandle = open(os.path.join(self.getGlobalTempDir(), self.tempFileName), 'w')
        fileHandle.close()
        
        #Check that global temp file dir contains only the file "one.txt"
        assert os.listdir(self.getGlobalTempDir()) == [ self.tempFileName ]
        
        #Create the children..
        if self.depth < 1:
            for childNo in xrange(random.choice(xrange(10))):
                self.addChildTarget(Target1(self.depth + 1))
        
        self.setFollowOnTarget(Target2(self.tempFileName))
        

def main():
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    options, args = parser.parse_args()
   
    #Now we are ready to run
    Stack(Target1()).startJobTree(options)
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    from jobTree.test.scriptTreeTest_Wrapper2 import *
    _test()
    main()
