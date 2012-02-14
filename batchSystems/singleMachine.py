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
from threading import Thread, Lock
from Queue import Queue, Empty

from sonLib.bioio import logger
 
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from sonLib.bioio import getTempFile
from sonLib.bioio import system

class Worker(Thread):
    lock = Lock()
    def __init__(self, inputQueue, outputQueue):
        Thread.__init__(self)
        self.inputQueue = inputQueue
        self.outputQueue = outputQueue
        
    def run(self):
        while True:
            args = self.inputQueue.get()
            if args == None: #Case where we are reducing threads for max number of CPUs
                self.inputQueue.task_done()
                return
            command, logFile, jobID, threadsToStart = args
            startTime = time.time()
            logger.info("Starting a job with ID %s" % jobID)
            #fnull = open(os.devnull, 'w') #Pipe the output to dev/null (it is caught by the slave and will be reported if there is an error)
            tempLogFile = getTempFile()
            fileHandle = open(tempLogFile, 'w')
            Worker.lock.acquire()
            try:
                process = subprocess.Popen(command, shell=True, stdout = fileHandle, stderr = fileHandle)
            finally:
                Worker.lock.release()
            sts = os.waitpid(process.pid, 0)
            fileHandle.close()
            #fnull.close()
            if os.path.exists(tempLogFile):
                system("mv %s %s" % (tempLogFile, logFile))
            self.outputQueue.put((jobID, sts[1], threadsToStart))
            self.inputQueue.task_done()
            logger.info("Finished a job with ID %s in time %s" % (jobID, time.time() - startTime))
        
class SingleMachineBatchSystem(AbstractBatchSystem):
    """The interface for running jobs on a single machine, runs all the jobs you
    give it as they come in, but in parallel.
    """
    def __init__(self, config, workerClass=Worker):
        AbstractBatchSystem.__init__(self, config) #Call the parent constructor
        self.jobIndex = 0
        self.jobs = {}
        self.maxThreads = int(config.attrib["max_threads"])
        self.maxCpus = int(config.attrib["max_jobs"])
        logger.info("Setting up the thread pool with %i threads given the max threads %i and the max cpus %i" % (min(self.maxThreads, self.maxCpus), self.maxThreads, self.maxCpus))
        self.maxThreads = min(self.maxThreads, self.maxCpus)
        self.cpusPerThread = float(self.maxCpus) / float(self.maxThreads)
        assert self.cpusPerThread >= 1
        assert self.maxThreads >= 1
        self.inputQueue = Queue()
        self.outputQueue = Queue()
        self.workerClass = workerClass
        for i in xrange(self.maxThreads): #Setup the threads
            worker = self.workerClass(self.inputQueue, self.outputQueue)
            worker.setDaemon(True)
            worker.start()

    def issueJob(self, command, memory, cpu, logFile):
        """Runs the jobs right away.
        """
        assert memory != None
        assert cpu != None
        assert logFile != None
        if cpu > self.maxCpus:
            raise RuntimeError("Requesting more cpus than available. Requested: %s, Available: %s" % (cpu, self.maxCpus))
        assert(cpu <= self.maxCpus)
        logger.debug("Issuing the command: %s with memory: %i, cpu: %i" % (command, memory, cpu))
        self.jobs[self.jobIndex] = command
        i = self.jobIndex
        #Deal with the max cpus calculation
        k = 0
        while cpu > self.cpusPerThread:
            self.inputQueue.put(None)
            cpu -= self.cpusPerThread
            k += 1
        assert k < self.maxThreads
        self.inputQueue.put((command, logFile, self.jobIndex, k))
        self.jobIndex += 1
        return i
    
    def killJobs(self, jobIDs):
        """As jobs are already run, this method has no effect.
        """
        pass
    
    def getIssuedJobIDs(self):
        """Just returns all the jobs that have been run, but not yet returned as updated.
        """
        return self.jobs.keys()
    
    def getRunningJobIDs(self):
        """Return empty map
        """
        return dict()
    
    def getUpdatedJob(self, maxWait):
        """Returns a map of the run jobs and the return value of their processes.
        """
        i = None
        try:
            jobID, exitValue, threadsToStart = self.outputQueue.get(timeout=maxWait)
            i = (jobID, exitValue)
            self.jobs.pop(jobID)
            logger.debug("Ran jobID: %s with exit value: %i" % (jobID, exitValue))
            for j in xrange(threadsToStart):
                worker = self.workerClass(self.inputQueue, self.outputQueue)
                worker.setDaemon(True)
                worker.start()
            self.outputQueue.task_done()
        except Empty:
            pass
        return i
    
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
            args = self.inputQueue.get()
            if args == None: #Case where we are reducing threads for max number of CPUs
                self.inputQueue.task_done()
                return
            command, logFile, jobID, threadsToStart = args
            assert logFile != None
            fnull = open(os.devnull, 'w') #Pipe the output to dev/null (it is caught by the slave and will be reported if there is an error)
            #Run to first calculate the runtime..
            process = subprocess.Popen(command, shell=True, stdout = fnull, stderr = fnull)
            if random.choice((False, True)):
                time.sleep(random.random()*5) #Sleep up to 5 seconds before trying to kill it
                process.kill()
            process.wait()
            fnull.close()
            self.outputQueue.put((jobID, process.returncode, threadsToStart))
            self.inputQueue.task_done()
