"""Tests jobTree with the parasol batch systems.

This test is important as it tests failure states of the system, i.e. what happens
when parasol and the master process are knocked over.
"""

import unittest
import os
import sys
import os.path
import random
import threading
import time
import subprocess
 
from workflow.jobTree.lib.bioio import system
from workflow.jobTree.lib.bioio import popen
from workflow.jobTree.lib.bioio import getTempFile
from workflow.jobTree.lib.bioio import TestStatus
from workflow.jobTree.lib.bioio import parseSuiteTestOptions
from workflow.jobTree.lib.bioio import logger
from workflow.jobTree.lib.bioio import TempFileTree
from workflow.jobTree.lib.bioio import workflowRootPath

from workflow.jobTree.test.jobTree.jobTreeTest import setupJobTree
from workflow.jobTree.test.jobTree.jobTreeTest import checkEndStateOfJobTree
from workflow.jobTree.test.jobTree.jobTreeTest import checkFileTreeCounts

class TestCase(unittest.TestCase):
    
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.testNo = TestStatus.getTestSetup(1, 1, 5, 5)
        self.depth = TestStatus.getTestSetup(1, 2, 2, 3)
        self.jobTreeDir = os.getcwd() + "/jobTree" #A directory for the job tree to be created in
        self.tempFileTreeDir = os.path.join(os.getcwd(), "tempFileTree")
        self.tempFileTree = TempFileTree(self.tempFileTreeDir) #A place to get temp files from
        parasolRestart()
    
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        self.tempFileTree.destroyTempFiles()
        parasolStop()
        parasolRestart()
        system("rm -rf %s %s" % (self.jobTreeDir, self.tempFileTreeDir)) #Cleanup the job tree in case it hasn't already been cleaned up.

    def testJobTree_Parasol(self):
        """Runs a test program using the job tree, whilst constantly restarting parasol
        by killing the nodes.
        """
        for test in xrange(self.testNo): #Does not run this test when doing short testing
            jobTreeCommand, fileTreeRootFile = setupJobTree(self.tempFileTree, self.jobTreeDir, 
                                                            "parasol", depth=self.depth)
            jobTreeCommand += " --rescueJobsFrequency 20"
            #Run the job
            parasolAndMasterKiller = ParasolAndMasterKiller()
            parasolAndMasterKiller.start()
            while True:
                while True:
                    process = subprocess.Popen(jobTreeCommand, shell=True)
                    sts = os.waitpid(process.pid, 0)
                    if sts[1] == 0:
                        logger.info("The job tree master ended, with an okay exit value (using parasol)")
                        break
                    else:
                        logger.info("The job tree master ended with an error exit value, restarting: %i" % sts[1])
                if checkEndStateOfJobTree(self.jobTreeDir): #Check the state of the job files
                    break
                
                jobTreeCommand = "jobTree --jobTree %s --logDebug" % self.jobTreeDir
            checkFileTreeCounts(fileTreeRootFile)
            os.system("rm -rf %s" % self.jobTreeDir)
            parasolAndMasterKiller.stopKilling()
            logger.info("Test done okay")


class ParasolAndMasterKiller(threading.Thread):
    """Thread to run around, periodically killing parasol. Requires
    init_parasol be installed.
    """
    def __init__(self, *args, **keywords):
        threading.Thread.__init__(self, *args, **keywords)
        self.kill = False
    
    def run (self):
        parasolRestart()
        while True:
            time.sleep(random.choice(xrange(240)))
            if self.kill == True:
                return
            logger.info("Going to kill a parasol/master process")
            killMasterAndParasol()
    
    def stopKilling(self):
        logger.info("Stopping killing parasol/master processes")
        self.kill = True
        
def killMasterAndParasol():
    """Method to destroy master process
    """
    tempFile = getTempFile()
    popen("ps -a", tempFile)
    fileHandle = open(tempFile, 'r')
    line = fileHandle.readline()
    #Example parasol state lines:
    #67401 ttys002    0:00.06 /Users/benedictpaten/kent/src/parasol/bin/paraNode start -hub=localhost -log=/tmp/node.2009-07-08.log -umask=002 -userPath=bin:bin/x86_64:bin/i
    #67403 ttys002    0:00.65 /Users/benedictpaten/kent/src/parasol/bin/paraHub -log=/tmp/hub.2009-07-08.log machineList subnet=127.0.0
    #68573 ttys002    0:00.00 /Users/benedictpaten/kent/src/parasol/bin/paraNode start -hub=localhost -log=/tmp/node.2009-07-08.log -umask=002 -userPath=bin:bin/x86_64:bin/i
    while line != '':
        tokens = line.split()
        if 'paraNode' in line or 'paraHub' in line:
            if random.random() > 0.5:
                i = os.system("kill %i" % int(tokens[0]))
                logger.info("Tried to kill parasol process: %i, line: %s, exit value: %i" % (int(tokens[0]), line, i))
                break
        elif 'jobTreeMaster.py' in line:
            logger.info("Have job tree master line")
            if random.random() > 0.5:
                i = os.system("kill %i" % int(tokens[0]))
                logger.info("Tried to kill master process: %i, line: %s, exit value: %i" % (int(tokens[0]), line, i))
                break
        line = fileHandle.readline()
    fileHandle.close()
    os.remove(tempFile)
    parasolRestart()

def parasolRestart():
    """Function starts the parasol hub and node.
    """
    parasolStop()
    while True:
        machineList = os.path.join(workflowRootPath(), "workflow", "jobTree", "machineList")
        #pathEnvVar = os.environ["PATH"]
        os.system("paraNode start -hub=localhost") 
        #-umask=002 -userPath=%s -sysPath=%s" % (pathEnvVar, pathEnvVar))
        os.system("paraHub %s subnet=127.0.0 &" % (machineList,))
        tempFile = getTempFile()
        dead = True
        try:
            popen("parasol status", tempFile)
            fileHandle = open(tempFile, 'r')
            line = fileHandle.readline()
            while line != '':
                if "Nodes dead" in line:
                    print line
                    if int(line.split()[-1]) == 0:
                        dead = False
                line = fileHandle.readline()
            fileHandle.close()
        except RuntimeError:
            pass
        os.remove(tempFile)
        if not dead:
            break
        else:
            logger.info("Tried to restart the parasol process, but failed, will try again")
            parasolStop()
            time.sleep(5)
    logger.info("Restarted the parasol process")

def parasolStop():
    """Function stops the parasol hub and node.
    """
    machineList = os.path.join(workflowRootPath(), "workflow", "jobTree", "machineList")
    i = os.system("paraNodeStop %s" % machineList)
    j = os.system("paraHubStop now")
    return i, j

def main():
    parseSuiteTestOptions()
    sys.argv = sys.argv[:1]
    unittest.main()

if __name__ == '__main__':
    main()