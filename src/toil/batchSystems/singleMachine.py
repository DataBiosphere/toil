#!/usr/bin/env python

# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        assert type(maxCores) == int
        if maxCores > self.numCores:
            logger.warn('Limiting maxCores to CPU count of system (%i).', self.numCores)
            maxCores = self.numCores
        AbstractBatchSystem.__init__(self, config, maxCores, maxMemory, maxDisk)
        assert self.maxCores >= 1
        assert self.maxMemory >= 1
        # The scale allows the user to apply a factor to each task's cores requirement, thereby squeezing more tasks
        # onto each core (scale < 1) or stretching tasks over more cores (scale > 1).
        self.scale = config.scale
        # The minimal fractional CPU. Tasks with a smaller cores requirement will be rounded up to this value. One
        # important invariant of this class is that each worker thread represents a CPU requirement of minCores,
        # meaning that we can never run more than numCores / minCores jobs concurrently. With minCores set to .1,
        # a task with cores=1 will occupy 10 workers. One of these workers will be blocked on the Popen.wait() call for
        # the worker.py child process, the others will be blocked on the acquiring the core semaphore.
        self.minCores = 0.1
        # Number of worker threads that will be started
        self.numWorkers = int(self.maxCores / self.minCores)
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
        # A semaphore representing available CPU in units of minCores
        self.coreSemaphore = Semaphore(self.numWorkers)
        # A counter representing failed acquisitions of the semaphore, also in units of minCores, and a lock to guard it
        self.coreOverflow = 0
        self.coreOverflowLock = Lock()
        # A lock to work around the lack of thread-safety in Python's subprocess module
        self.popenLock = Lock()
        # A counter representing available memory in bytes
        self.memoryPool = self.maxMemory
        # A condition object used to guard it (a semphore would force us to acquire each unit of memory individually)
        self.memoryCondition = Condition()
        logger.info('Setting up the thread pool with %i workers, '
                    'given a minimum CPU fraction of %f '
                    'and a maximum CPU value of %i.', self.numWorkers, self.minCores, maxCores)
        for i in xrange(self.numWorkers):
            worker = Thread(target=self.worker, args=(self.inputQueue,))
            self.workerThreads.append(worker)
            worker.start()

    # The input queue is passed as an argument because the corresponding attribute is reset to None in shutdown()

    def worker(self, inputQueue):
        while True:
            args = inputQueue.get()
            if args is None:
                logger.debug('Received queue sentinel.')
                break
            jobCommand, jobID,jobCores, jobMem, jobDisk = args
            try:
                numThreads = int(jobCores / self.minCores)
                logger.debug('Acquiring %i bytes of memory from pool of %i.', jobMem, self.memoryPool)
                self.memoryCondition.acquire()
                while jobMem > self.memoryPool:
                    logger.debug('Waiting for memory condition to change.')
                    self.memoryCondition.wait()
                    logger.debug('Memory condition changed.')
                self.memoryPool -= jobMem
                self.memoryCondition.release()

                try:
                    logger.debug('Attempting to acquire %i threads for %i cpus submitted', numThreads, jobCores)
                    numThreadsAcquired = 0
                    # Acquire first thread blockingly
                    logger.debug('Acquiring semaphore blockingly.')
                    self.coreSemaphore.acquire(blocking=True)
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
                            if not self.coreSemaphore.acquire(blocking=False):
                                with self.coreOverflowLock:
                                    self.coreOverflow += 1
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

                        with self.coreOverflowLock:
                            if self.coreOverflow > 0:
                                if self.coreOverflow > numThreadsAcquired:
                                    self.coreOverflow -= numThreadsAcquired
                                    numThreadsAcquired = 0
                                else:
                                    numThreadsAcquired -= self.coreOverflow
                                    self.coreOverflow = 0
                        for i in xrange(numThreadsAcquired):
                            self.coreSemaphore.release()
                finally:
                    logger.debug('Releasing %i memory back to pool', jobMem)
                    self.memoryCondition.acquire()
                    self.memoryPool += jobMem
                    self.memoryCondition.notifyAll()
                    self.memoryCondition.release()
            finally:
                # noinspection PyProtectedMember
                value = self.coreSemaphore._Semaphore__value
                logger.debug('Finished job. CPU semaphore value (approximate): %i, overflow: %i', value, self.coreOverflow)
                self.outputQueue.put((jobID, 0))
        logger.debug('Exiting worker thread normally.')

    def issueBatchJob(self, command, memory, cores, disk):
        """
        Adds the command and resources to a queue to be run.
        """
        # Round cores to minCores and apply scale
        cores = math.ceil(cores * self.scale / self.minCores) * self.minCores
        assert cores <= self.maxCores, \
            'job is requesting {} cores, which is greater than {} available on the machine. Scale currently set ' \
            'to {} consider adjusting job or scale.'.format(cores, multiprocessing.cpu_count(), self.scale)
        assert cores >= self.minCores
        assert memory <= self.maxMemory, 'job requests {} mem, only {} total available.'.format(memory, self.maxMemory)

        self.checkResourceRequest(memory, cores, disk)
        logger.debug("Issuing the command: %s with memory: %i, cores: %i, disk: %i" % (command, memory, cores, disk))
        with self.jobIndexLock:
            jobID = self.jobIndex
            self.jobIndex += 1
        self.jobs[jobID] = command
        self.inputQueue.put((command, jobID, cores, memory, disk))
        return jobID

    def killBatchJobs(self, jobIDs):
        """
        Kills jobs by ID
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
        # Remove reference to inputQueue (raises exception if inputQueue is used after method call)
        inputQueue = self.inputQueue
        self.inputQueue = None
        for i in xrange(self.numWorkers):
            inputQueue.put(None)

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
