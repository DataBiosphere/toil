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
import multiprocessing
import os
import random
import subprocess
import time
import math

from threading import Thread
from threading import Semaphore, Lock, Condition
from Queue import Queue
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem


logger = logging.getLogger( __name__ )


class SingleMachineBatchSystem(AbstractBatchSystem):
    """The interface for running jobs on a single machine, runs all the jobs you
    give it as they come in, but in parallel.
    """
    cpu_count = multiprocessing.cpu_count()
    def __init__(self, config, maxCpus, maxMemory, badWorker=False):
        assert type(maxCpus)==int
        if maxCpus > self.cpu_count:
            maxCpus = self.cpu_count
            logger.warn('Limiting maxCpus to CPU count of system (%i).', self.cpu_count)
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory)

        self.scale = float(config.attrib['scale'])
        self.min_cpu = 0.1
        self.num_workers = int(maxCpus / self.min_cpu)
        self.jobIndex = 0
        self.jobs = {}
        self.inputQueue = Queue()
        self.outputQueue = Queue()
        self.runningJobs = {}
        self.workerThreads = []
        self.cpu_sem = Semaphore(self.num_workers)
        self.cpuOverflowLock = Lock()
        self.popenLock = Lock()
        self.memory_pool = self.maxMemory
        self.mem_con = Condition()
        self.cpu_overflow = 0
        # Sanity checks
        assert maxCpus >= 1
        assert self.maxMemory >= 1

        logger.info('Setting up the thread pool with {} threads given a min cpu_job {} '
                    'and maxCpus value of {}'.format(self.num_workers, self.min_cpu, maxCpus))
        self.workerFn = self.badWorker if badWorker else self.worker
        for i in xrange(self.num_workers):
            worker = Thread(target=self.workerFn, args=(self.inputQueue, self.outputQueue))
            self.workerThreads.append(worker)
            worker.start()

    def worker(self, inputQueue, outputQueue):
        while True:
            args = inputQueue.get()
            if args is None:
                logger.debug('Sentinel received, exiting worker thread.')
                return
            command, jobID, cpu, mem = args
            try:
                num_threads = int(cpu / self.min_cpu)
                logger.debug('Acquiring {} bytes of memory from memory pool of {}'.format(mem, self.memory_pool))
                self.mem_con.acquire()
                while mem > self.memory_pool:
                    logger.critical('Waiting on condition (mem)')
                    self.mem_con.wait()
                    logger.critical('Wait returns from condition (mem)')
                self.memory_pool -= mem
                self.mem_con.release()

                try:
                    logger.debug('Acquiring {} threads for {} cpus submitted'.format(num_threads, cpu))
                    c = 0
                    logger.critical('Semaphore Acquire')
                    self.cpu_sem.acquire(True)
                    try:
                        c += 1
                        logger.critical('Semaphore Acquired')
                        while c < num_threads:
                            if not self.cpu_sem.acquire(False):
                                with self.cpuOverflowLock:
                                    self.cpu_overflow += 1
                            c += 1

                        logger.info('Executing command: {}'.format(command))
                        with self.popenLock:
                            popen = subprocess.Popen(command, shell=True)
                        info = Info(time.time(), popen, kill_intended=False)
                        self.runningJobs[jobID] = info
                        try:
                            statusCode = popen.wait()
                            if 0 != statusCode:
                                if statusCode != -9 or not info.kill_intended:
                                    raise subprocess.CalledProcessError(statusCode, command)
                        finally:
                            self.runningJobs.pop(jobID)
                    finally:
                        logger.debug('Releasing threads')

                        with self.cpuOverflowLock:
                            if self.cpu_overflow > 0:
                                if self.cpu_overflow > c:
                                    self.cpu_overflow -= c
                                    c = 0
                                else:
                                    c -= self.cpu_overflow
                                    self.cpu_overflow = 0
                        for i in xrange(c):
                            self.cpu_sem.release()
                finally:
                    logger.debug('Releasing memory back to pool')
                    self.mem_con.acquire()
                    self.memory_pool += mem
                    self.mem_con.notifyAll()
                    self.mem_con.release()
            finally:
                logger.debug('Finished job. Semaphore value: {}, CPU overflow: {}'.format(
                    self.cpu_sem._Semaphore__value, self.cpu_overflow))
                outputQueue.put((jobID, 0))

    # FIXME: Remove or fix badWorker to be compliant with new thread management.
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
        """Adds the command and resources to a queue to be run.
        """
        # Round cpu to the 10ths place, applying scale (if set).
        cpu = math.ceil(cpu * self.scale / self.min_cpu) * self.min_cpu
        assert cpu <= self.maxCpus, \
            'job is requesting {} cpu, which is greater than {} available on the machine. Scale currently set ' \
            'to {} consider adjusting job or scale.'.format(cpu, multiprocessing.cpu_count(), self.scale)
        assert cpu >= self.min_cpu
        assert memory <= self.maxMemory, 'job requests {} mem, only {} total available.'.format(memory, self.maxMemory)

        self.checkResourceRequest(memory, cpu)
        logger.debug("Issuing the command: %s with memory: %i, cpu: %i" % (command, memory, cpu))
        self.jobs[self.jobIndex] = command
        self.inputQueue.put((command, self.jobIndex, cpu, memory))
        jobReturnVal = self.jobIndex
        self.jobIndex += 1
        return jobReturnVal
    
    def killJobs(self, jobIDs):
        """As jobs are already run, this method has no effect.
        """
        logger.debug('Killing jobs: {}'.format(jobIDs))
        for id in jobIDs:
            if id in self.runningJobs:
                info = self.runningJobs[id]
                info.kill_intended = True
                os.kill(info.popen.pid, 9)
                while id in self.runningJobs:
                    pass
    
    def getIssuedJobIDs(self):
        """Just returns all the jobs that have been run, but not yet returned as updated.
        """
        return self.jobs.keys()

    def getRunningJobIDs(self):
        """Return empty map
        """
        currentJobs = {}
        for jobID, info in self.runningJobs.iteritems():
            startTime = info.time
            currentJobs[jobID] = time.time() - startTime
        return currentJobs

    def shutdown(self):
        """Cleanly terminate worker threads.
        Add sentinels to inputQueue equal to maxThreads.
        Join all worker threads.
        """
        for i in xrange(self.num_workers):
            self.inputQueue.put(None)
        # Remove reference to inputQueue (raises exception if inputQueue is used after method call)
        self.inputQueue = None
        for thread in self.workerThreads:
            thread.join()

    def getUpdatedJob(self, maxWait):
        """Returns a map of the run jobs and the return value of their processes.
        """
        i = self.outputQueue.get(maxWait)
        if i == None:
            return None
        jobID, exitValue = i
        self.jobs.pop(jobID)
        logger.debug("Ran jobID: %s with exit value: %i" % (jobID, exitValue))
        self.outputQueue.task_done()
        return (jobID, exitValue)
    
    def getRescueJobFrequency(self):
        """This should not really occur, wihtout an error. To exercise the 
        system we allow it every 90 minutes. 
        """
        return 5400


class Info(object):
    def __init__(self, time, popen, kill_intended):
        self.time = time
        self.popen = popen
        self.kill_intended = kill_intended
