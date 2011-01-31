#!/usr/bin/env python

import sys
import os

try:
    import cPickle 
except ImportError:
    import pickle as cPickle

from workflow.jobTree.lib.bioio import getTempFile
from workflow.jobTree.lib.bioio import system

class MultiTarget():
    def __init__(self, commands):
        self.commands = commands

    def execute(self):
        task_id = int(os.environ['SGE_TASK_ID'])
        if task_id is None:
             RuntimeError("Multi-target launched without task id") 
        if task_id < 1 or task_id > len(self.commands):
             RuntimeError("Task ID not within the array range 1 <= %i <= %i", task_id, len(self.commands))
        (job, outfile) = self.commands[task_id - 1]
        if outfile is None:
                system(job)
        else:
                system("%s > %s" % (job, outfile))

    def makeRunnable(self, tempDir):
        pickleFile = tempDir.getTempFile(".pickle")
        fileHandle = open(pickleFile, 'w')
        cPickle.dump(self, fileHandle, cPickle.HIGHEST_PROTOCOL)
        fileHandle.close() 
        return "multijob %s" % (pickleFile)

if __name__ == "__main__":
        fileHandle = open(sys.argv[1], 'r')
        multitarget = cPickle.load(fileHandle)
        fileHandle.close()
        multitarget.execute() 
