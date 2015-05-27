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
import logging
import os
import random
import subprocess
import time

from jobTree import Process, Queue
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem


logger = logging.getLogger( __name__ )


class SingleMachineBatchSystem(AbstractBatchSystem):
    """The interface for running jobs on a single machine, runs all the jobs you
    give it as they come in, but in parallel.
    """
    def __init__(self, config, maxCpus, maxMemory, badWorker=False):
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory) #Call the parent constructor
        self.jobIndex = 0
        self.jobs = {}
        self.maxThreads = int(config.attrib["max_threads"])
        logger.info("Setting up the thread pool with %i threads given the max threads %i and the max cpus %i" % (min(self.maxThreads, self.maxCpus), self.maxThreads, self.maxCpus))
        self.maxThreads = min(self.maxThreads, self.maxCpus)
        self.cpusPerThread = float(self.maxCpus) / float(self.maxThreads)
        self.memoryPerThread = self.maxThreads + float(self.maxMemory) / float(self.maxThreads) #Add the maxThreads to avoid losing memory by rounding.
        assert self.cpusPerThread >= 1
        assert self.maxThreads >= 1
        assert self.maxMemory >= 1
        assert self.memoryPerThread >= 1
        self.inputQueue = Queue()
        self.outputQueue = Queue()
        self.runningJobs = {}
        self.popen = None

        # Setup threads for the worker process
        self.workerFn = self.badWorker if badWorker else self.worker
        for i in xrange(self.maxThreads):
            worker = Process(target=self.workerFn, args=(self.inputQueue, self.outputQueue))
            worker.daemon = True
            worker.start()

    def worker(self, inputQueue, outputQueue):
        while True:
            args = inputQueue.get()
            if args is None: #Case where we are reducing threads for max number of CPUs
                inputQueue.task_done()
                return
            command, jobID, threadsToStart = args
            self.popen = subprocess.Popen( command, shell=True )
            statusCode = self.popen.wait()
            if 0 != statusCode:
                raise subprocess.CalledProcessError(statusCode, command)
            outputQueue.put((jobID, 0, threadsToStart))
            inputQueue.task_done()

    def badWorker(self, inputQueue, outputQueue):
        """This is used to test what happens if we fail and restart jobs
        """
        fnull = open(os.devnull, 'w') #Pipe the output to dev/null (it is caught by the worker and will be reported if there is an error)
        while True:
            args = inputQueue.get()
            if args == None: #Case where we are reducing threads for max number of CPUs
                inputQueue.task_done()
                return
            command, jobID, threadsToStart = args
            #Run to first calculate the runtime..
            process = subprocess.Popen(command, shell=True, stdout = fnull, stderr = fnull)
            if random.choice((False, True)):
                time.sleep(random.random())
                process.kill()
                process.wait()
                outputQueue.put((jobID, 1, threadsToStart))
            else:
                process.wait()
                outputQueue.put((jobID, process.returncode, threadsToStart))
            inputQueue.task_done()

    def issueJob(self, command, memory, cpu):
        """Runs the jobs right away.
        """
        self.checkResourceRequest(memory, cpu)
        logger.debug("Issuing the command: %s with memory: %i, cpu: %i" % (command, memory, cpu))
        self.jobs[self.jobIndex] = command
        i = self.jobIndex
        #Deal with the max cpus calculation
        k = 0
        while cpu > self.cpusPerThread or memory > self.memoryPerThread:
            self.inputQueue.put(None)
            cpu -= self.cpusPerThread
            memory -= self.memoryPerThread
            k += 1
        assert k < self.maxThreads
        #self.runningJobs = {self.jobIndex : self.popen}
        self.inputQueue.put((command, self.jobIndex, k))
        self.jobIndex += 1
        return i
    
    def killJobs(self, jobIDs):
        """As jobs are already run, this method has no effect.
        """
        # for id in jobIDs:
        #     pid = self.runningJobs[id].pid
        #     os.kill(pid, 9)
        #     self.runningJobs.pop(id)
        pass
    
    def getIssuedJobIDs(self):
        """Just returns all the jobs that have been run, but not yet returned as updated.
        """
        return self.jobs.keys()
    
    def getRunningJobIDs(self):
        """Return empty map
        """
        return dict()
        #return self.runningJobs
    
    def getUpdatedJob(self, maxWait):
        """Returns a map of the run jobs and the return value of their processes.
        """
        i = self.getFromQueueSafely(self.outputQueue, maxWait)
        if i == None:
            return None
        jobID, exitValue, threadsToStart = i
        self.jobs.pop(jobID)
        logger.debug("Ran jobID: %s with exit value: %i" % (jobID, exitValue))
        for j in xrange(threadsToStart):
            worker = Process(target=self.workerFn, args=(self.inputQueue, self.outputQueue))
            worker.daemon = True
            worker.start()
        self.outputQueue.task_done()
        return (jobID, exitValue)
    
    def getRescueJobFrequency(self):
        """This should not really occur, wihtout an error. To exercise the 
        system we allow it every 90 minutes. 
        """
        return 5400