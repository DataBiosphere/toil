#!/usr/bin/env python

#Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.

import os
import random
import subprocess
import time
from threading import Thread
from Queue import Queue, Empty

from jobTree.lib.bioio import logger
 
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.lib.bioio import getTempFile
from jobTree.lib.bioio import system

class Worker(Thread):
    def __init__(self, inputQueue, outputQueue):
        Thread.__init__(self)
        self.inputQueue = inputQueue
        self.outputQueue = outputQueue
        
    def run(self):
        while True:
            command, logFile, jobID = self.inputQueue.get()
            #fnull = open(os.devnull, 'w') #Pipe the output to dev/null (it is caught by the slave and will be reported if there is an error)
            tempLogFile = getTempFile()
            fileHandle = open(tempLogFile, 'w')
            process = subprocess.Popen(command, shell=True, stdout = fileHandle, stderr = fileHandle)
            sts = os.waitpid(process.pid, 0)
            fileHandle.close()
            #fnull.close()
            if os.path.exists(tempLogFile):
                system("mv %s %s" % (tempLogFile, logFile))
            self.outputQueue.put((command, sts[1], jobID))
            self.inputQueue.task_done()
        
class SingleMachineBatchSystem(AbstractBatchSystem):
    """The interface for running jobs on a single machine, runs all the jobs you
    give it as they come in, but in parallel.
    """
    def __init__(self, config, workerClass=Worker):
        AbstractBatchSystem.__init__(self, config) #Call the parent constructor
        self.jobIndex = 0
        self.jobs = {}
        self.maxThreads = int(config.attrib["max_threads"])
        assert self.maxThreads >= 1
        
        self.inputQueue = Queue()
        self.outputQueue = Queue()
        for i in xrange(int(config.attrib["max_threads"])): #Setup the threads
            worker = workerClass(self.inputQueue, self.outputQueue)
            worker.setDaemon(True)
            worker.start()

    def issueJobs(self, commands):
        """Runs the jobs right away.
        """
        issuedJobs = {}
        for command, memory, cpu, logFile in commands: #Add the commands to the queue
            assert memory != None
            assert cpu != None
            assert logFile != None
            logger.debug("Issuing the command: %s with memory: %i, cpu: %i" % (command, memory, cpu))
            self.jobs[self.jobIndex] = command
            issuedJobs[self.jobIndex] = command
            self.inputQueue.put((command, logFile, self.jobIndex))
            self.jobIndex += 1
        return issuedJobs
    
    def killJobs(self, jobIDs):
        """As jobs are already run, this method has no effect.
        """
        pass
    
    def getIssuedJobIDs(self):
        """Just returns all the jobs that have been run, but not yet returned as updated.
        """
        return self.jobs.keys()
    
    def getRunningJobIDs(self):
        """As no jobs are running in parallel, just returns an empty map.
        """
        return self.jobs.keys()
    
    def getUpdatedJobs(self):
        """Returns a map of the run jobs and the return value of their processes.
        """
        runJobs = {}
        try:
            while True:
                command, exitValue, jobID = self.outputQueue.get_nowait()
                runJobs[jobID] = exitValue
                self.jobs.pop(jobID)
                logger.debug("Ran the command: %s with exit value: %i" % (command, exitValue))
                self.outputQueue.task_done()
        except Empty:
            pass
        return runJobs
    
    def getWaitDuration(self):
        """As the main process is serial, we can make this zero.
        """
        return 0.0
    
    def getRescueJobFrequency(self):
        """This should not really occur, wihtout an error. To exercise the 
        system we allow it every minute. 
        """
        return 1800  

class BadWorker(Thread):
    """This is used to test what happens if we fail and restart jobs
    """
    def __init__(self, inputQueue, outputQueue):
        Thread.__init__(self)
        self.inputQueue = inputQueue
        self.outputQueue = outputQueue
        
    def run(self):
        while True:
            command, logFile, jobID = self.inputQueue.get()
            assert logFile != None
            fnull = open(os.devnull, 'w') #Pipe the output to dev/null (it is caught by the slave and will be reported if there is an error)
            #Run to first calculate the runtime..
            process = subprocess.Popen(command, shell=True, stdout = fnull, stderr = fnull)
            if random.choice((False, True)):
                time.sleep(random.random()*5) #Sleep up to 5 seconds before trying to kill it
                process.kill()
            process.wait()
            fnull.close()
            self.outputQueue.put((command, process.returncode, jobID))
            self.inputQueue.task_done()
