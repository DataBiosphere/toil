#!/usr/bin/env python

# Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
from __future__ import absolute_import
import logging
import multiprocessing
import os
import random
import subprocess
import time
import math
from threading import Thread
from threading import Semaphore, Lock, Condition
from Queue import Queue, Empty

from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem

logger = logging.getLogger(__name__)


class SingleMachineBatchSystem(AbstractBatchSystem):
    """
    The interface for running jobs on a single machine, runs all the jobs you give it as they come in, but in parallel.
    """

    numCores = multiprocessing.cpu_count()

    def __init__(self, config, maxCpus, maxMemory, maxDisk, badWorker=False):
        assert type(maxCpus) == int
        if maxCpus > self.numCores:
            logger.warn('Limiting maxCpus to CPU count of system (%i).', self.numCores)
            maxCpus = self.numCores
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory, maxDisk)
        assert self.maxCpus >= 1
        assert self.maxMemory >= 1
        # The scale allows the user to apply a factor to each task's CPU requirement, thereby squeezing more tasks
        # onto each core (scale < 1) or stretching tasks over more cores (scale > 1).
        self.scale = config.scale
        # The minimal fractional CPU. Tasks with a smaller CPU requirement will be rounded up to this value. One
        # important invariant of this class is that each worker thread represents a CPU requirement of minCpu,
        # meaning that we can never run more than numCores / minCpu jobs concurrently. With minCpu set to .1,
        # a task with cpu=1 will occupy 10 workers. One of these workers will be blocked on the Popen.wait() call for
        # the worker.py child process, the others will be blocked on the acquiring the CPU semaphore.
        self.minCpu = 0.1
        # Number of worker threads that will be started
        self.numWorkers = int(self.maxCpus / self.minCpu)
        # A counter to generate job IDs and a lock to guard it
        self.jobIndex = 0
        self.jobIndexLock = Lock()
        # A dictionary mapping IDs of submitted jobs to those jobs
        self.jobs = {}
        # A queue of jobs waiting to be executed. Consumed by the workers.
        self.inputQueue = Queue()
        # A queue of finished jobs. Produced by the workers.
        self.outputQueue = Queue()
        # A dictionary mapping IDs of currently running jobs to their Info objects
        self.runningJobs = {}
        # The list of worker threads
        self.workerThreads = []
        # A semaphore representing available CPU in units of minCpu
        self.cpuSemaphore = Semaphore(self.numWorkers)
        # A counter representing failed acquisitions of the semaphore, also in units of minCpu, and a lock to guard it
        self.cpuOverflow = 0
        self.cpuOverflowLock = Lock()
        # A lock to work around the lack of thread-safety in Python's subprocess module
        self.popenLock = Lock()
        # A counter representing available memory in bytes
        self.memoryPool = self.maxMemory
        # A condition object used to guard it (a semphore would force us to acquire each unit of memory individually)
        self.memoryCondition = Condition()
        logger.info('Setting up the thread pool with %i workers, '
                    'given a minimum CPU fraction of %f '
                    'and a maximum CPU value of %i.', self.numWorkers, self.minCpu, maxCpus)
        self.workerFn = self.badWorker if badWorker else self.worker
        for i in xrange(self.numWorkers):
            worker = Thread(target=self.workerFn, args=(self.inputQueue,))
            self.workerThreads.append(worker)
            worker.start()

    # The input queue is passed as an argument because the corresponding attribute is reset to None in shutdown()

    def worker(self, inputQueue):
        while True:
            args = inputQueue.get()
            if args is None:
                logger.debug('Received queue sentinel.')
                break
            jobCommand, jobID, jobCpu, jobMem, jobDisk = args
            try:
                numThreads = int(jobCpu / self.minCpu)
                logger.debug('Acquiring %i bytes of memory from pool of %i.', jobMem, self.memoryPool)
                self.memoryCondition.acquire()
                while jobMem > self.memoryPool:
                    logger.debug('Waiting for memory condition to change.')
                    self.memoryCondition.wait()
                    logger.debug('Memory condition changed.')
                self.memoryPool -= jobMem
                self.memoryCondition.release()

                try:
                    logger.debug('Attempting to acquire %i threads for %i cpus submitted', numThreads, jobCpu)
                    numThreadsAcquired = 0
                    # Acquire first thread blockingly
                    logger.debug('Acquiring semaphore blockingly.')
                    self.cpuSemaphore.acquire(blocking=True)
                    try:
                        numThreadsAcquired += 1
                        logger.debug('Semaphore acquired.')
                        while numThreadsAcquired < numThreads:
                            # Optimistically and non-blockingly acquire remaining threads. For every failed attempt
                            # to acquire a thread, atomically increment the overflow instead of the semaphore such
                            # any thread that later wants to release a thread, can do so into the overflow,
                            # thereby effectively surrendering that thread to this job and not into the semaphore.
                            # That way we get to start a job with as many threads as are available, and later grab
                            # more as they become available.
                            if not self.cpuSemaphore.acquire(blocking=False):
                                with self.cpuOverflowLock:
                                    self.cpuOverflow += 1
                            numThreadsAcquired += 1

                        logger.info("Executing command: '%s'.", jobCommand)
                        with self.popenLock:
                            popen = subprocess.Popen(jobCommand, shell=True)
                        info = Info(time.time(), popen, kill_intended=False)
                        self.runningJobs[jobID] = info
                        try:
                            statusCode = popen.wait()
                            if 0 != statusCode:
                                if statusCode != -9 or not info.kill_intended:
                                    logger.error("Got exit code %i (indicating failure) from command '%s'.", statusCode, jobCommand )
                        finally:
                            self.runningJobs.pop(jobID)
                    finally:
                        logger.debug('Releasing %i threads.', numThreadsAcquired)

                        with self.cpuOverflowLock:
                            if self.cpuOverflow > 0:
                                if self.cpuOverflow > numThreadsAcquired:
                                    self.cpuOverflow -= numThreadsAcquired
                                    numThreadsAcquired = 0
                                else:
                                    numThreadsAcquired -= self.cpuOverflow
                                    self.cpuOverflow = 0
                        for i in xrange(numThreadsAcquired):
                            self.cpuSemaphore.release()
                finally:
                    logger.debug('Releasing %i memory back to pool', jobMem)
                    self.memoryCondition.acquire()
                    self.memoryPool += jobMem
                    self.memoryCondition.notifyAll()
                    self.memoryCondition.release()
            finally:
                # noinspection PyProtectedMember
                value = self.cpuSemaphore._Semaphore__value
                logger.debug('Finished job. CPU semaphore value (approximate): %i, overflow: %i', value, self.cpuOverflow)
                self.outputQueue.put((jobID, 0))
        logger.debug('Exiting worker thread normally.')

    # FIXME: Remove or fix badWorker to be compliant with new thread management.

    def badWorker(self, inputQueue, outputQueue):
        """
        This is used to test what happens if we fail and restart jobs
        """
        # Pipe the output to dev/null (it is caught by the worker and will be reported if there is an error)
        fnull = open(os.devnull, 'w')
        while True:
            args = inputQueue.get()
            # Case where we are reducing threads for max number of CPUs
            if args is None:
                inputQueue.task_done()
                return
            command, jobID, threadsToStart = args
            # Run to first calculate the runtime..
            process = subprocess.Popen(command, shell=True, stdout=fnull, stderr=fnull)
            if random.choice((False, True)):
                time.sleep(random.random())
                process.kill()
                process.wait()
                outputQueue.put((jobID, 1, threadsToStart))
            else:
                process.wait()
                outputQueue.put((jobID, process.returncode, threadsToStart))
            inputQueue.task_done()

    def issueBatchJob(self, command, memory, cpu, disk):
        """
        Adds the command and resources to a queue to be run.
        """
        # Round cpu to minCpu and apply scale
        cpu = math.ceil(cpu * self.scale / self.minCpu) * self.minCpu
        assert cpu <= self.maxCpus, \
            'job is requesting {} cpu, which is greater than {} available on the machine. Scale currently set ' \
            'to {} consider adjusting job or scale.'.format(cpu, multiprocessing.cpu_count(), self.scale)
        assert cpu >= self.minCpu
        assert memory <= self.maxMemory, 'job requests {} mem, only {} total available.'.format(memory, self.maxMemory)

        self.checkResourceRequest(memory, cpu, disk)
        logger.debug("Issuing the command: %s with memory: %i, cpu: %i, disk: %i" % (command, memory, cpu, disk))
        with self.jobIndexLock:
            jobID = self.jobIndex
            self.jobIndex += 1
        self.jobs[jobID] = command
        self.inputQueue.put((command, jobID, cpu, memory, disk))
        return jobID

    def killBatchJobs(self, jobIDs):
        """
        As jobs are already run, this method has no effect.
        """
        logger.debug('Killing jobs: {}'.format(jobIDs))
        for id in jobIDs:
            if id in self.runningJobs:
                info = self.runningJobs[id]
                info.kill_intended = True
                os.kill(info.popen.pid, 9)
                while id in self.runningJobs:
                    pass

    def getIssuedBatchJobIDs(self):
        """
        Just returns all the jobs that have been run, but not yet returned as updated.
        """
        return self.jobs.keys()

    def getRunningBatchJobIDs(self):
        """
        Return empty map
        """
        currentJobs = {}
        for jobID, info in self.runningJobs.iteritems():
            startTime = info.time
            currentJobs[jobID] = time.time() - startTime
        return currentJobs

    def shutdown(self):
        """
        Cleanly terminate worker threads. Add sentinels to inputQueue equal to maxThreads. Join all worker threads.
        """
        for i in xrange(self.numWorkers):
            self.inputQueue.put(None)
        # Remove reference to inputQueue (raises exception if inputQueue is used after method call)
        self.inputQueue = None
        for thread in self.workerThreads:
            thread.join()

    def getUpdatedBatchJob(self, maxWait):
        """
        Returns a map of the run jobs and the return value of their processes.
        """
        try:
            i = self.outputQueue.get(timeout=maxWait)
        except Empty:
            return None
        jobID, exitValue = i
        self.jobs.pop(jobID)
        logger.debug("Ran jobID: %s with exit value: %i" % (jobID, exitValue))
        self.outputQueue.task_done()
        return jobID, exitValue

    @classmethod
    def getRescueBatchJobFrequency(cls):
        """
        This should not really occur, wihtout an error. To exercise the system we allow it every 90 minutes.
        """
        return 5400


class Info(object):
    def __init__(self, time, popen, kill_intended):
        self.time = time
        self.popen = popen
        self.kill_intended = kill_intended
