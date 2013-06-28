#!/usr/bin/env python

"""Script is part of tests of scriptTree.

The test reuses elements of the jobTree tests.
"""
import os
import random
from optparse import OptionParser

import xml.etree.ElementTree as ET

def makeFileTree(rootFile, remainingDepth, tempFileTree):
    """Makes a random tree of linked files.
    """  
    tree = ET.Element("tree")
    tree.attrib["count"] = "0"
    children = ET.SubElement(tree, "children")
    if remainingDepth > 0:
        for i in xrange(random.choice(xrange(1, 10))):
            childFile = tempFileTree.getTempFile()
            ET.SubElement(children, "child", { "file":childFile })
            makeFileTree(childFile, remainingDepth-1, tempFileTree)
    fileHandle = open(rootFile, 'w')
    ET.ElementTree(tree).write(fileHandle)
    fileHandle.close()

def makeTreePointer(treeNode, tempFile):
    tree = ET.Element("tree_pointer")
    tree.attrib["file"] = treeNode
    ET.SubElement(tree, "children")
    fileHandle = open(tempFile, 'w')
    ET.ElementTree(tree).write(fileHandle)
    fileHandle.close()
    return tempFile

from sonLib.bioio import logger
from sonLib.bioio import TempFileTree
from sonLib.bioio import getRandomAlphaNumericString
from sonLib.bioio import system

from jobTree.scriptTree.target import Target
from jobTree.scriptTree.stack import Stack

class SetupFileTree(Target):
    """Target extends run to create a tree of files, upon which the jobTreeTest_Command scripts
    are run.
    
    It creates a child target, which doesn't do much apart from issue the jobTreeTest_Command,
    and a clean up target, which cleans up the file tree the thing creates.
    """
    def __init__(self):
        Target.__init__(self, 5)
        self.depth = 0
    
    def run(self):
        ##########################################
        #Setup a file tree.
        ##########################################
            
        tempFileTree = TempFileTree(os.path.join(self.getGlobalTempDir(), getRandomAlphaNumericString()))   
        
        fileTreeRootFile = tempFileTree.getTempFile()
    
        makeFileTree(fileTreeRootFile, \
                     self.depth, tempFileTree)
        
        treePointer = tempFileTree.getTempFile()
        
        makeTreePointer(fileTreeRootFile, treePointer)
        
        logger.info("We've set up the file tree")
        
        if random.random() > 0.5:
            raise RuntimeError()
        
        ##########################################
        #Issue the child and follow on jobs
        ##########################################
        
        self.addChildTarget(ChildTarget(treePointer))
        
        self.setFollowOnTarget(DestructFileTree(tempFileTree))
        
        logger.info("We've added the child target and finished SetupFileTree.run()")
        
class DestructFileTree(Target):
    """Cleans up the stuff created by the previous target.
    """
    def __init__(self, tempFileTree):
        Target.__init__(self, 10)
        self.tempFileTree = tempFileTree
        
    def run(self):
        logger.info("At the end, this is the contents of the global temp dir...")
        system("ls -l %s" % self.getGlobalTempDir())
        logger.info("And done....")
        
        if random.random() > 0.5:
            raise RuntimeError()
        
        self.tempFileTree.destroyTempFiles()
 
class ChildTarget(Target):
    """A simple target that simply issues the jobTreeTest_Command.. command.
    """
    def __init__(self, treePointer):
        Target.__init__(self, 10)
        self.treePointer = treePointer
         
    def run(self):
        self.addChildCommand("echo HELLO")
        logger.info("Added the child command and finished ChildTarget.run()")
        
        
###############
#Remaining targets come from scriptTreeTest_Wrapper2
###############
        
class Target2(Target):
    """This target checks the temp file is still
    present which was created by Target1. This target is from scriptTreeTest_Wrapper, and
    shows we can serialise targets that were not created in the same file as from which execution occurs.
    
    Also tests making CPU and Memory requirements..
    """
    def __init__(self, tempFileName):
        #Try requesting random amounts of CPU and memory requirements..
        self.requestedMemory = int(1 + random.random() *100000000)
        self.requestedCpu = int(1 + random.random() * 1)
        Target.__init__(self, time=random.random() * 10, memory=self.requestedMemory, cpu=self.requestedCpu)
        self.tempFileName = tempFileName

    def run(self):
        assert os.listdir(self.getLocalTempDir()) == [] 
        assert os.listdir(self.getGlobalTempDir()) == [ self.tempFileName ]
        self.setFollowOnTarget(Target3(self.tempFileName))
        assert self.requestedMemory <= self.getMemory()
        assert self.requestedCpu <= self.getCpu()
        if random.random() > 0.5:
            raise RuntimeError() #This is a test error
        
      
class Target3(Target):
    """This target cleans up file created in target 1. This target is from scriptTreeTest_Wrapper, and
    shows we can serialise targets that were not created in the same file as from which execution occurs.
    """
    def __init__(self, tempFileName):
        Target.__init__(self, time=random.random() * 10)
        self.tempFileName = tempFileName
        
    def run(self):
        assert os.listdir(self.getLocalTempDir()) == []
        if os.listdir(self.getGlobalTempDir()) == [ self.tempFileName ]:
            os.remove(os.path.join(self.getGlobalTempDir(), self.tempFileName))
        assert os.listdir(self.getGlobalTempDir()) == []
        
def main():
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    options, args = parser.parse_args()
    
    #Now we are ready to run
    Stack(SetupFileTree()).startJobTree(options)
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    from jobTree.test.scriptTreeTest_Wrapper import *
    _test()
    main()
