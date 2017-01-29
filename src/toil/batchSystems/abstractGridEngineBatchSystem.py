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

import os
import shutil
import logging
import time
from threading import Thread
from abc import ABCMeta, abstractmethod

# Python 3 compatibility imports
from six.moves.queue import Empty, Queue

from bd2k.util.objects import abstractclassmethod

from toil.batchSystems.abstractBatchSystem import BatchSystemSupport

logger = logging.getLogger(__name__)

# TODO: should this be an attribute?  Used in the worker and the batch system

class AbstractGridEngineBatchSystem(BatchSystemSupport):
    """
    A partial implementation of BatchSystemSupport for batch systems run on a
    standard HPC cluster. By default worker cleanup and hot deployment are not
    implemented.
    """

    class Worker(Thread):

        __metaclass__ = ABCMeta

        def __init__(self, newJobsQueue, updatedJobsQueue, killQueue, killedJobsQueue, boss):
            """
            Abstract worker interface class. All instances are created with five
            initial arguments (below). Note the Queue instances passed are empty.

            :param newJobsQueue: a Queue of new (unsubmitted) jobs
            :param updatedJobsQueue: a Queue of jobs that have been updated
            :param killQueue: a Queue of active jobs that need to be killed
            :param killedJobsQueue: Queue of killed jobs for this worker
            :param boss: the AbstractGridEngineBatchSystem instance that controls this AbstractGridEngineWorker

            """
            Thread.__init__(self)
            self.newJobsQueue = newJobsQueue
            self.updatedJobsQueue = updatedJobsQueue
            self.killQueue = killQueue
            self.killedJobsQueue = killedJobsQueue
            self.waitingJobs = list()
            self.runningJobs = set()
            self.boss = boss
            self.allocatedCpus = dict()
            self.batchJobIDs = dict()

        def getBatchSystemID(self, jobID):
            """
            Get batch system-specific job ID

            Note: for the moment this is the only consistent way to cleanly get
            the batch system job ID

            :param: string jobID: toil job ID
            """
            if not jobID in self.batchJobIDs:
                RuntimeError("Unknown jobID, could not be converted")

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
            self.runningJobs.remove(jobID)
            del self.allocatedCpus[jobID]
            del self.batchJobIDs[jobID]

        def createJobs(self, newJob):
            """
            Create a new job with the Toil job ID. Implementation-specific; called
            by AbstractGridEngineWorker.run()

            :param string newJob: Toil job ID
            """
            activity = False
            # Load new job id if present:
            if newJob is not None:
                self.waitingJobs.append(newJob)
            # Launch jobs as necessary:
            while (len(self.waitingJobs) > 0
                   and sum(self.allocatedCpus.values()) < int(self.boss.maxCores)):
                activity = True
                jobID, cpu, memory, command = self.waitingJobs.pop(0)

                # prepare job submission command
                subLine = self.prepareSubmission(cpu, memory, jobID, command)
                logger.debug("Running %r", subLine)

                # submit job and get batch system ID
                batchJobID = self.submitJob(subLine)
                logger.debug("Submitted job %d", batchJobID)

                # Store dict for mapping Toil job ID to batch job ID
                # TODO: Note that this currently stores a tuple of (batch system
                # ID, Task), but the second value is None by default and doesn't
                # seem to be used
                self.batchJobIDs[jobID] = (batchJobID, None)

                # Add to queue of running jobs
                self.runningJobs.add(jobID)

                # Add to allocated resources
                self.allocatedCpus[jobID] = cpu
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
                    if self.getJobExitCode(batchJobID) is not None:
                        logger.debug('Adding jobID %s to killedJobsQueue', jobID)
                        self.killedJobsQueue.put(jobID)
                        killList.remove(jobID)
                        self.forgetJob(jobID)
                if len(killList) > 0:
                    logger.warn("Some jobs weren't killed, trying again in %is.", self.boss.sleepSeconds())
                    time.sleep(self.boss.sleepSeconds())

            return True

        def checkOnJobs(self):
            """
            Check and update status of all running jobs.
            """
            activity = False
            for jobID in list(self.runningJobs):
                batchJobID = self.getBatchSystemID(jobID)
                status = self.getJobExitCode(batchJobID)
                if status is not None:
                    activity = True
                    self.updatedJobsQueue.put((jobID, status))
                    self.forgetJob(jobID)
            return activity

        def run(self):
            """
            Run any new jobs
            """

            while True:
                activity = False
                newJob = None
                if not self.newJobsQueue.empty():
                    activity = True
                    newJob = self.newJobsQueue.get()
                    if newJob is None:
                        logger.debug('Received queue sentinel.')
                        break
                activity |= self.killJobs()
                activity |= self.createJobs(newJob)
                activity |= self.checkOnJobs()
                if not activity:
                    logger.debug('No activity, sleeping for %is', self.boss.sleepSeconds())
                    time.sleep(self.boss.sleepSeconds())

        @abstractmethod
        def prepareSubmission(self, cpu, memory, jobID, command):
            """
            Preparation in putting together a command-line string
            for submitting to batch system (via submitJob().)

            :param: string cpu
            :param: string memory
            :param: string jobID  : Toil job ID
            :param: string subLine: the command line string to be called

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
            Returns job exit code. Implementation-specific; called by
            AbstractGridEngineWorker.checkOnJobs()

            :param string batchjobID: batch system job ID
            """
            raise NotImplementedError()


    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(AbstractGridEngineBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        # AbstractBatchSystem.__init__(self, config, maxCores, maxMemory, maxDisk)
        self.resultsFile = self._getResultsFileName(config.jobStore)
        # Reset the job queue and results (initially, we do this again once we've killed the jobs)
        self.resultsFileHandle = open(self.resultsFile, 'w')
        # We lose any previous state in this file, and ensure the files existence
        self.resultsFileHandle.close()
        self.currentJobs = set()

        # NOTE: this may be batch system dependent, maybe move into the worker?
        # Doing so would effectively make each subclass of AbstractGridEngineBatchSystem
        # much smaller
        self.maxCPU, self.maxMEM = self.obtainSystemConstants()

        self.nextJobID = 0
        self.newJobsQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.killQueue = Queue()
        self.killedJobsQueue = Queue()
        # get the associated worker class here
        self.worker = self.Worker(self.newJobsQueue, self.updatedJobsQueue, self.killQueue,
                              self.killedJobsQueue, self)
        self.worker.start()

    def __des__(self):
        # Closes the file handle associated with the results file.
        self.resultsFileHandle.close()

    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    @classmethod
    def supportsHotDeployment(cls):
        return False

    def issueBatchJob(self, jobNode):
        self.checkResourceRequest(jobNode.memory, jobNode.cores, jobNode.disk)
        jobID = self.nextJobID
        self.nextJobID += 1
        self.currentJobs.add(jobID)
        self.newJobsQueue.put((jobID, jobNode.cores, jobNode.memory, jobNode.command))
        logger.debug("Issued the job command: %s with job id: %s ", jobNode.command, str(jobID))
        return jobID

    def killBatchJobs(self, jobIDs):
        """
        Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
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
                sleep = self.sleepSeconds()
                logger.debug('Some kills (%s) still pending, sleeping %is', len(jobIDs),
                             sleep)
                time.sleep(sleep)

    def getIssuedBatchJobIDs(self):
        """
        Gets the list of issued jobs
        """
        return list(self.currentJobs)

    def getRunningBatchJobIDs(self):
        return self.worker.getRunningJobIDs()

    def getUpdatedBatchJob(self, maxWait):
        try:
            item = self.updatedJobsQueue.get(timeout=maxWait)
        except Empty:
            return None
        logger.debug('UpdatedJobsQueue Item: %s', item)
        jobID, retcode = item
        self.currentJobs.remove(jobID)
        return jobID, retcode, None

    def shutdown(self):
        """
        Signals worker to shutdown (via sentinel) then cleanly joins the thread
        """
        newJobsQueue = self.newJobsQueue
        self.newJobsQueue = None

        newJobsQueue.put(None)
        self.worker.join()

    def setEnv(self, name, value=None):
        if value and ',' in value:
            raise ValueError(type(self).__name__ + " does not support commata in environment variable values")
        return super(AbstractGridEngineBatchSystem,self).setEnv(name, value)

    @classmethod
    def getWaitDuration(self):
        return 5

    @classmethod
    def getRescueBatchJobFrequency(cls):
        return 30 * 60 # Half an hour

    @classmethod
    def sleepSeconds(cls):
        return 1

    @abstractclassmethod
    def obtainSystemConstants(cls):
        """
        Returns the max. memory and max. CPU for the system
        """
        raise NotImplementedError()
