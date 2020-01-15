# Copyright (C) 2015-2016 Regents of the University of California
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
from __future__ import division
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import range
from builtins import object
from past.utils import old_div
from contextlib import contextmanager
import logging
import os
import time
import math
from threading import Thread
from threading import Lock, Condition
from six.moves.queue import Empty, Queue

import toil
from toil import subprocess
from toil.batchSystems.abstractBatchSystem import BatchSystemSupport
from toil.lib.threading import cpu_count
from toil import worker as toil_worker
from toil.common import Toil

log = logging.getLogger(__name__)


class SingleMachineBatchSystem(BatchSystemSupport):
    """
    The interface for running jobs on a single machine, runs all the jobs you give it as they
    come in, but in parallel.
    """

    @classmethod
    def supportsAutoDeployment(cls):
        return False

    @classmethod
    def supportsWorkerCleanup(cls):
        return True

    numCores = cpu_count()

    minCores = 0.1
    """
    The minimal fractional CPU. Tasks with a smaller core requirement will be rounded up to this
    value. One important invariant of this class is that each worker thread represents a CPU
    requirement of minCores, meaning that we can never run more than numCores / minCores jobs
    concurrently.
    """
    physicalMemory = toil.physicalMemory()

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        if maxCores > self.numCores:
            log.warning('Limiting maxCores to CPU count of system (%i).', self.numCores)
            maxCores = self.numCores
        if maxMemory > self.physicalMemory:
            log.warning('Limiting maxMemory to physically available memory (%i).', self.physicalMemory)
            maxMemory = self.physicalMemory
        self.physicalDisk = toil.physicalDisk(config)
        if maxDisk > self.physicalDisk:
            log.warning('Limiting maxDisk to physically available disk (%i).', self.physicalDisk)
            maxDisk = self.physicalDisk
        super(SingleMachineBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        assert self.maxCores >= self.minCores
        assert self.maxMemory >= 1

        # The scale allows the user to apply a factor to each task's cores requirement, thereby
        # squeezing more tasks onto each core (scale < 1) or stretching tasks over more cores
        # (scale > 1).
        self.scale = config.scale
        
        if config.badWorker > 0 and config.debugWorker:
            # We can't throw SIGUSR1 at the worker because it is also going to
            # be the leader and/or test harness.
            raise RuntimeError("Cannot use badWorker and debugWorker together; "
                "worker would have to kill the leader")
                
        self.debugWorker = config.debugWorker
        
        # Number of worker threads that will be started
        self.numWorkers = int(old_div(self.maxCores, self.minCores))
        # A counter to generate job IDs and a lock to guard it
        self.jobIndex = 0
        self.jobIndexLock = Lock()
        # A dictionary mapping IDs of submitted jobs to the command line
        self.jobs = {}
        """
        :type: dict[str,toil.job.JobNode]
        """
        # A queue of jobs waiting to be executed. Consumed by the workers.
        self.inputQueue = Queue()
        # A queue of finished jobs. Produced by the workers.
        self.outputQueue = Queue()
        # A dictionary mapping IDs of currently running jobs to their Info objects
        self.runningJobs = {}
        """
        :type: dict[str,Info]
        """
        # The list of worker threads
        self.workerThreads = []
        """
        :type list[Thread]
        """
        # Variables involved with non-blocking resource acquisition
        self.acquisitionTimeout = 5
        self.acquisitionRetryDelay = 10
        self.aquisitionCondition = Condition()

        # A pool representing available CPU in units of minCores
        self.coreFractions = ResourcePool(self.numWorkers, 'cores', self.acquisitionTimeout)
        # A lock to work around the lack of thread-safety in Python's subprocess module
        self.popenLock = Lock()
        # A pool representing available memory in bytes
        self.memory = ResourcePool(self.maxMemory, 'memory', self.acquisitionTimeout)
        # A pool representing the available space in bytes
        self.disk = ResourcePool(self.maxDisk, 'disk', self.acquisitionTimeout)

        if not self.debugWorker:
            log.debug('Setting up the thread pool with %i workers, '
                     'given a minimum CPU fraction of %f '
                     'and a maximum CPU value of %i.', self.numWorkers, self.minCores, maxCores)
            for i in range(self.numWorkers):
                worker = Thread(target=self.worker, args=(self.inputQueue,))
                self.workerThreads.append(worker)
                worker.start()
        else:
            log.debug('Started in worker debug mode.')

    def _runWorker(self, jobCommand, jobID, environment):
        """
        Run the jobCommand using the worker and wait for it to finish.
        The worker is forked unless it is a '_toil_worker' job and
        debugWorker is True.
        """
        startTime = time.time()  # Time job is started
        if self.debugWorker and "_toil_worker" in jobCommand:
            # Run the worker without forking
            jobName, jobStoreLocator, jobStoreID = jobCommand.split()[1:] # Parse command
            jobStore = Toil.resumeJobStore(jobStoreLocator)
            # TODO: The following does not yet properly populate self.runningJobs so it is not possible to kill
            # running jobs in forkless mode - see the "None" value in place of popen
            info = Info(time.time(), None, killIntended=False)
            try:
                self.runningJobs[jobID] = info
                try:
                    toil_worker.workerScript(jobStore, jobStore.config, jobName, jobStoreID, 
                                             redirectOutputToLogFile=not self.debugWorker) # Call the worker
                finally:
                    self.runningJobs.pop(jobID)
            finally:
                if not info.killIntended:
                    self.outputQueue.put((jobID, 0, time.time() - startTime))
        else:
            with self.popenLock:
                popen = subprocess.Popen(jobCommand,
                                         shell=True,
                                         env=dict(os.environ, **environment))
            info = Info(time.time(), popen, killIntended=False)
            try:
                self.runningJobs[jobID] = info
                try:
                    statusCode = popen.wait()
                    if statusCode != 0 and not info.killIntended:
                        log.error("Got exit code %i (indicating failure) "
                                  "from job %s.", statusCode, self.jobs[jobID])
                finally:
                    self.runningJobs.pop(jobID)
            finally:
                if not info.killIntended:
                    self.outputQueue.put((jobID, statusCode, time.time() - startTime))
        
    # Note: The input queue is passed as an argument because the corresponding attribute is reset
    # to None in shutdown()

    def worker(self, inputQueue):
        while True:
            if self.debugWorker and inputQueue.empty():
                return
            args = inputQueue.get()
            if args is None:
                break
            jobCommand, jobID, jobCores, jobMemory, jobDisk, environment = args
            while True:
                try:
                    coreFractions = int(old_div(jobCores, self.minCores))
                    log.debug('Acquiring %i bytes of memory from a pool of %s.', jobMemory,
                              self.memory)
                    with self.memory.acquisitionOf(jobMemory):
                        log.debug('Acquiring %i fractional cores from a pool of %s to satisfy a '
                                  'request of %f cores', coreFractions, self.coreFractions,
                                  jobCores)
                        with self.coreFractions.acquisitionOf(coreFractions):
                            with self.disk.acquisitionOf(jobDisk):
                                self._runWorker(jobCommand, jobID, environment)

                except ResourcePool.AcquisitionTimeoutException as e:
                    log.debug('Could not acquire enough (%s) to run job (%s). Requested: (%s), '
                              'Avaliable: %s. Sleeping for 10s.', e.resource, jobID, e.requested,
                              e.available)
                    with self.aquisitionCondition:
                        # Make threads sleep for the given delay, or until another job finishes.
                        # Whichever is sooner.
                        self.aquisitionCondition.wait(timeout=self.acquisitionRetryDelay)
                    continue
                else:
                    log.debug('Finished job. self.coreFractions ~ %s and self.memory ~ %s',
                              self.coreFractions.value, self.memory.value)
                    with self.aquisitionCondition:
                        # Wake up sleeping threads
                        self.aquisitionCondition.notifyAll()
                    break

    def issueBatchJob(self, jobNode):
        """Adds the command and resources to a queue to be run."""
        # Round cores to minCores and apply scale
        cores = math.ceil(jobNode.cores * self.scale / self.minCores) * self.minCores
        assert cores <= self.maxCores, ('The job {} is requesting {} cores, more than the maximum of '
                                        '{} cores this batch system was configured with. Scale is '
                                        'set to {}.'.format(jobNode.jobName, cores, self.maxCores, self.scale))
        assert cores >= self.minCores
        assert jobNode.memory <= self.maxMemory, ('The job {} is requesting {} bytes of memory, more than '
                                          'the maximum of {} this batch system was configured '
                                          'with.'.format(jobNode.jobName, jobNode.memory, self.maxMemory))

        self.checkResourceRequest(jobNode.memory, cores, jobNode.disk)
        log.debug("Issuing the command: %s with memory: %i, cores: %i, disk: %i" % (
            jobNode.command, jobNode.memory, cores, jobNode.disk))
        with self.jobIndexLock:
            jobID = self.jobIndex
            self.jobIndex += 1
        self.jobs[jobID] = jobNode.command
        self.inputQueue.put((jobNode.command, jobID, cores, jobNode.memory,
                             jobNode.disk, self.environment.copy()))
        if self.debugWorker:  # then run immediately, blocking for return
            self.worker(self.inputQueue)
        return jobID

    def killBatchJobs(self, jobIDs):
        """Kills jobs by ID."""
        log.debug('Killing jobs: {}'.format(jobIDs))
        for jobID in jobIDs:
            if jobID in self.runningJobs:
                info = self.runningJobs[jobID]
                info.killIntended = True
                if info.popen != None:
                    os.kill(info.popen.pid, 9)
                else:
                    # No popen if running in forkless mode currently 
                    assert self.debugWorker
                    log.critical("Can't kill job: %s in debug mode" % jobID)
                while jobID in self.runningJobs:
                    pass

    def getIssuedBatchJobIDs(self):
        """Just returns all the jobs that have been run, but not yet returned as updated."""
        return list(self.jobs.keys())

    def getRunningBatchJobIDs(self):
        now = time.time()
        return {jobID: now - info.time for jobID, info in list(self.runningJobs.items())}

    def shutdown(self):
        """
        Cleanly terminate worker threads. Add sentinels to inputQueue equal to maxThreads. Join
        all worker threads.
        """
        # Remove reference to inputQueue (raises exception if inputQueue is used after method call)
        inputQueue = self.inputQueue
        self.inputQueue = None
        for i in range(self.numWorkers):
            inputQueue.put(None)
        for thread in self.workerThreads:
            thread.join()
        BatchSystemSupport.workerCleanup(self.workerCleanupInfo)

    def getUpdatedBatchJob(self, maxWait):
        """Returns a tuple of a no-longer-running, the return value of its process, and its runtime, or None."""
        try:
            item = self.outputQueue.get(timeout=maxWait)
        except Empty:
            return None
        jobID, exitValue, wallTime = item
        jobCommand = self.jobs.pop(jobID)
        log.debug("Ran jobID: %s with exit value: %i", jobID, exitValue)
        return jobID, exitValue, wallTime

    @classmethod
    def setOptions(cls, setOption):
        setOption("scale", default=1)


class Info(object):
    # Can't use namedtuple here since killIntended needs to be mutable
    def __init__(self, startTime, popen, killIntended):
        self.time = startTime
        self.popen = popen
        self.killIntended = killIntended


class ResourcePool(object):
    def __init__(self, initial_value, resourceType, timeout):
        super(ResourcePool, self).__init__()
        self.condition = Condition()
        self.value = initial_value
        self.resourceType = resourceType
        self.timeout = timeout

    def acquire(self, amount):
        with self.condition:
            startTime = time.time()
            while amount > self.value:
                if time.time() - startTime >= self.timeout:
                    # This means the thread timed out waiting for the resource.  We exit the nested
                    # context managers in worker to prevent blocking of a resource due to
                    # unavailability of a nested resource request.
                    raise self.AcquisitionTimeoutException(resource=self.resourceType,
                                                           requested=amount, available=self.value)
                # Allow 5 seconds to get the resource, else quit through the above if condition.
                # This wait + timeout is the last thing in the loop such that a request that takes
                # longer than 5s due to multiple wakes under the 5 second threshold are still
                # honored.
                self.condition.wait(timeout=self.timeout)
            self.value -= amount
            self.__validate()

    def release(self, amount):
        with self.condition:
            self.value += amount
            self.__validate()
            self.condition.notifyAll()

    def __validate(self):
        assert 0 <= self.value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "ResourcePool(%i)" % self.value

    @contextmanager
    def acquisitionOf(self, amount):
        self.acquire(amount)
        try:
            yield
        finally:
            self.release(amount)

    class AcquisitionTimeoutException(Exception):
        """To be raised when a resource request times out."""
        def __init__(self, resource, requested, available):
            """
            Creates an instance of this exception that indicates which resource is insufficient for
            current demands, as well as the amount requested and amount actually available.

            :param str resource: string representing the resource type

            :param int|float requested: the amount of the particular resource requested that resulted
                   in this exception

            :param int|float available: amount of the particular resource actually available
            """
            self.requested = requested
            self.available = available
            self.resource = resource
