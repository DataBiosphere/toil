# Copyright (C) 2015-2021 Regents of the University of California
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
import logging
import time
from abc import ABCMeta, abstractmethod
from datetime import datetime
from queue import Empty, Queue
from threading import Lock, Thread
from typing import Any, List

from toil.batchSystems.abstractBatchSystem import (BatchJobExitReason,
                                                   BatchSystemCleanupSupport,
                                                   UpdatedBatchJobInfo)
from toil.lib.misc import CalledProcessErrorStderr

logger = logging.getLogger(__name__)


class AbstractGridEngineBatchSystem(BatchSystemCleanupSupport):
    """
    A partial implementation of BatchSystemSupport for batch systems run on a
    standard HPC cluster. By default auto-deployment is not implemented.
    """

    class Worker(Thread, metaclass=ABCMeta):

        def __init__(self, newJobsQueue: Queue, updatedJobsQueue: Queue, killQueue: Queue, killedJobsQueue: Queue, boss: 'AbstractGridEngineBatchSystem') -> None:
            """
            Abstract worker interface class. All instances are created with five
            initial arguments (below). Note the Queue instances passed are empty.

            :param newJobsQueue: a Queue of new (unsubmitted) jobs
            :param updatedJobsQueue: a Queue of jobs that have been updated
            :param killQueue: a Queue of active jobs that need to be killed
            :param killedJobsQueue: Queue of killed jobs for this worker
            :param boss: the AbstractGridEngineBatchSystem instance that
                         controls this AbstractGridEngineWorker

            """
            Thread.__init__(self)
            self.boss = boss
            self.boss.config.statePollingWait = \
                self.boss.config.statePollingWait or self.boss.getWaitDuration()
            self.newJobsQueue = newJobsQueue
            self.updatedJobsQueue = updatedJobsQueue
            self.killQueue = killQueue
            self.killedJobsQueue = killedJobsQueue
            self.waitingJobs: List[Any] = list()
            self.runningJobs = set()
            self.runningJobsLock = Lock()
            self.batchJobIDs = dict()
            self._checkOnJobsCache = None
            self._checkOnJobsTimestamp = None

        def getBatchSystemID(self, jobID):
            """
            Get batch system-specific job ID

            Note: for the moment this is the only consistent way to cleanly get
            the batch system job ID

            :param: string jobID: toil job ID
            """
            if jobID not in self.batchJobIDs:
                raise RuntimeError("Unknown jobID, could not be converted")

            (job, task) = self.batchJobIDs[jobID]
            if task is None:
                return str(job)
            else:
                return str(job) + "." + str(task)

        def forgetJob(self, jobID):
            """
            Remove jobID passed

            :param: string jobID: toil job ID
            """
            with self.runningJobsLock:
                self.runningJobs.remove(jobID)
            del self.batchJobIDs[jobID]

        def createJobs(self, newJob: Any) -> bool:
            """
            Create a new job with the Toil job ID.

            Implementation-specific; called by AbstractGridEngineWorker.run()

            :param string newJob: Toil job ID
            """
            activity = False
            # Load new job id if present:
            if newJob is not None:
                self.waitingJobs.append(newJob)
            # Launch jobs as necessary:
            while len(self.waitingJobs) > 0 and \
                    len(self.runningJobs) < int(self.boss.config.maxLocalJobs):
                activity = True
                jobID, cpu, memory, command, jobName = self.waitingJobs.pop(0)

                # prepare job submission command
                subLine = self.prepareSubmission(cpu, memory, jobID, command, jobName)
                logger.debug("Running %r", subLine)
                batchJobID = self.boss.with_retries(self.submitJob, subLine)
                logger.debug("Submitted job %s", str(batchJobID))

                # Store dict for mapping Toil job ID to batch job ID
                # TODO: Note that this currently stores a tuple of (batch system
                # ID, Task), but the second value is None by default and doesn't
                # seem to be used
                self.batchJobIDs[jobID] = (batchJobID, None)

                # Add to queue of running jobs
                with self.runningJobsLock:
                    self.runningJobs.add(jobID)

            return activity

        def killJobs(self):
            """
            Kill any running jobs within worker
            """
            killList = list()
            while True:
                try:
                    jobId = self.killQueue.get(block=False)
                except Empty:
                    break
                else:
                    killList.append(jobId)

            if not killList:
                return False

            # Do the dirty job
            for jobID in list(killList):
                if jobID in self.runningJobs:
                    logger.debug('Killing job: %s', jobID)

                    # this call should be implementation-specific, all other
                    # code is redundant w/ other implementations
                    self.killJob(jobID)
                else:
                    if jobID in self.waitingJobs:
                        self.waitingJobs.remove(jobID)
                    self.killedJobsQueue.put(jobID)
                    killList.remove(jobID)

            # Wait to confirm the kill
            while killList:
                for jobID in list(killList):
                    batchJobID = self.getBatchSystemID(jobID)
                    if self.boss.with_retries(self.getJobExitCode, batchJobID) is not None:
                        logger.debug('Adding jobID %s to killedJobsQueue', jobID)
                        self.killedJobsQueue.put(jobID)
                        killList.remove(jobID)
                        self.forgetJob(jobID)
                if len(killList) > 0:
                    logger.warning("Some jobs weren't killed, trying again in %is.", self.boss.sleepSeconds())

            return True

        def checkOnJobs(self):
            """Check and update status of all running jobs.

            Respects statePollingWait and will return cached results if not within
            time period to talk with the scheduler.
            """
            if (self._checkOnJobsTimestamp and
                 (datetime.now() - self._checkOnJobsTimestamp).total_seconds() < self.boss.config.statePollingWait):
                return self._checkOnJobsCache

            activity = False
            for jobID in list(self.runningJobs):
                batchJobID = self.getBatchSystemID(jobID)
                status = self.boss.with_retries(self.getJobExitCode, batchJobID)
                if status is not None and isinstance(status, int):
                    activity = True
                    self.updatedJobsQueue.put(UpdatedBatchJobInfo(jobID=jobID, exitStatus=status, exitReason=None, wallTime=None))
                    self.forgetJob(jobID)
                elif status is not None and isinstance(status, BatchJobExitReason):
                    activity = True
                    self.updatedJobsQueue.put(UpdatedBatchJobInfo(jobID=jobID, exitStatus=1, exitReason=status, wallTime=None))
                    self.forgetJob(jobID)
            self._checkOnJobsCache = activity
            self._checkOnJobsTimestamp = datetime.now()
            return activity

        def _runStep(self):
            """return True if more jobs, False is all done"""
            activity = False
            newJob = None
            if not self.newJobsQueue.empty():
                activity = True
                newJob = self.newJobsQueue.get()
                if newJob is None:
                    logger.debug('Received queue sentinel.')
                    return False
            activity |= self.killJobs()
            activity |= self.createJobs(newJob)
            activity |= self.checkOnJobs()
            if not activity:
                logger.debug('No activity, sleeping for %is', self.boss.sleepSeconds())
            return True

        def run(self):
            """
            Run any new jobs
            """
            try:
                while self._runStep():
                    pass
            except Exception as ex:
                logger.error("GridEngine like batch system failure", exc_info=ex)
                raise

        @abstractmethod
        def prepareSubmission(self, cpu, memory, jobID, command, jobName):
            """
            Preparation in putting together a command-line string
            for submitting to batch system (via submitJob().)

            :param: string cpu
            :param: string memory
            :param: string jobID  : Toil job ID
            :param: string subLine: the command line string to be called
            :param: string jobName: the name of the Toil job, to provide metadata to batch systems if desired

            :rtype: string
            """
            raise NotImplementedError()

        @abstractmethod
        def submitJob(self, subLine):
            """
            Wrapper routine for submitting the actual command-line call, then
            processing the output to get the batch system job ID

            :param: string subLine: the literal command line string to be called

            :rtype: string: batch system job ID, which will be stored internally
            """
            raise NotImplementedError()

        @abstractmethod
        def getRunningJobIDs(self):
            """
            Get a list of running job IDs. Implementation-specific; called by boss
            AbstractGridEngineBatchSystem implementation via
            AbstractGridEngineBatchSystem.getRunningBatchJobIDs()

            :rtype: list
            """
            raise NotImplementedError()

        @abstractmethod
        def killJob(self, jobID):
            """
            Kill specific job with the Toil job ID. Implementation-specific; called
            by AbstractGridEngineWorker.killJobs()

            :param string jobID: Toil job ID
            """
            raise NotImplementedError()

        @abstractmethod
        def getJobExitCode(self, batchJobID):
            """
            Returns job exit code or an instance of abstractBatchSystem.BatchJobExitReason.
            if something else happened other than the job exiting.
            Implementation-specific; called by AbstractGridEngineWorker.checkOnJobs()

            :param string batchjobID: batch system job ID

            :rtype: int|toil.batchSystems.abstractBatchSystem.BatchJobExitReason: exit code int
                    or BatchJobExitReason if something else happened other than job exiting.
            """
            raise NotImplementedError()

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(AbstractGridEngineBatchSystem, self).__init__(
            config, maxCores, maxMemory, maxDisk)
        self.config = config

        self.currentJobs = set()

        self.newJobsQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.killQueue = Queue()
        self.killedJobsQueue = Queue()
        # get the associated worker class here
        self.worker = self.Worker(self.newJobsQueue, self.updatedJobsQueue,
                                  self.killQueue, self.killedJobsQueue, self)
        self.worker.start()
        self._getRunningBatchJobIDsTimestamp = None
        self._getRunningBatchJobIDsCache = {}

    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    @classmethod
    def supportsAutoDeployment(cls):
        return False

    def issueBatchJob(self, jobDesc):
        # Avoid submitting internal jobs to the batch queue, handle locally
        localID = self.handleLocalJob(jobDesc)
        if localID:
            return localID
        else:
            self.checkResourceRequest(jobDesc.memory, jobDesc.cores, jobDesc.disk)
            jobID = self.getNextJobID()
            self.currentJobs.add(jobID)
            self.newJobsQueue.put((jobID, jobDesc.cores, jobDesc.memory, jobDesc.command, jobDesc.jobName))
            logger.debug("Issued the job command: %s with job id: %s and job name %s", jobDesc.command, str(jobID),
                         jobDesc.jobName)
        return jobID

    def killBatchJobs(self, jobIDs):
        """
        Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        self.killLocalJobs(jobIDs)
        jobIDs = set(jobIDs)
        logger.debug('Jobs to be killed: %r', jobIDs)
        for jobID in jobIDs:
            self.killQueue.put(jobID)
        while jobIDs:
            killedJobId = self.killedJobsQueue.get()
            if killedJobId is None:
                break
            jobIDs.remove(killedJobId)
            if killedJobId in self.currentJobs:
                self.currentJobs.remove(killedJobId)
            if jobIDs:
                logger.debug('Some kills (%s) still pending, sleeping %is', len(jobIDs),
                             self.sleepSeconds())

    def getIssuedBatchJobIDs(self):
        """
        Gets the list of issued jobs
        """
        return list(self.getIssuedLocalJobIDs()) + list(self.currentJobs)

    def getRunningBatchJobIDs(self):
        """
        Retrieve running job IDs from local and batch scheduler.

        Respects statePollingWait and will return cached results if not within
        time period to talk with the scheduler.
        """
        if (self._getRunningBatchJobIDsTimestamp and (
                datetime.now() -
                self._getRunningBatchJobIDsTimestamp).total_seconds() <
                self.config.statePollingWait):
            batchIds = self._getRunningBatchJobIDsCache
        else:
            batchIds = self.with_retries(self.worker.getRunningJobIDs)
            self._getRunningBatchJobIDsCache = batchIds
            self._getRunningBatchJobIDsTimestamp = datetime.now()
        batchIds.update(self.getRunningLocalJobIDs())
        return batchIds

    def getUpdatedBatchJob(self, maxWait):
        local_tuple = self.getUpdatedLocalJob(0)
        if local_tuple:
            return local_tuple
        else:
            try:
                item = self.updatedJobsQueue.get(timeout=maxWait)
            except Empty:
                return None
            logger.debug('UpdatedJobsQueue Item: %s', item)
            self.currentJobs.remove(item.jobID)
            return item

    def shutdown(self):
        """
        Signals worker to shutdown (via sentinel) then cleanly joins the thread
        """
        self.shutdownLocal()
        newJobsQueue = self.newJobsQueue
        self.newJobsQueue = None

        newJobsQueue.put(None)
        self.worker.join()

    def setEnv(self, name, value=None):
        if value and ',' in value:
            raise ValueError(type(self).__name__ + " does not support commata in environment variable values")
        return super(AbstractGridEngineBatchSystem, self).setEnv(name, value)

    @classmethod
    def getWaitDuration(self):
        return 1

    def sleepSeconds(self, sleeptime=1):
        """ Helper function to drop on all state-querying functions to avoid over-querying.
        """
        time.sleep(sleeptime)
        return sleeptime

    def with_retries(self, operation, *args, **kwargs):
        """
        Call operation with args and kwargs. If one of the calls to an SGE
        command fails, sleep and try again for a set number of times.
        """
        maxTries = 3
        tries = 0
        while True:
            tries += 1
            try:
                return operation(*args, **kwargs)
            except CalledProcessErrorStderr as err:
                if tries < maxTries:
                    logger.error("Will retry errored operation %s, code %d: %s",
                                 operation.__name__, err.returncode, err.stderr)
                    time.sleep(self.config.statePollingWait)
                else:
                    logger.error("Failed operation %s, code %d: %s",
                                 operation.__name__, err.returncode, err.stderr)
                    raise err
