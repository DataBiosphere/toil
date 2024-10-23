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
from typing import Optional, Union

from toil.batchSystems.abstractBatchSystem import (
    BatchJobExitReason,
    UpdatedBatchJobInfo,
)
from toil.batchSystems.cleanup_support import BatchSystemCleanupSupport
from toil.bus import ExternalBatchIdMessage, get_job_kind
from toil.common import Config
from toil.job import AcceleratorRequirement, JobDescription
from toil.lib.misc import CalledProcessErrorStderr
from toil.lib.retry import DEFAULT_DELAYS, old_retry

logger = logging.getLogger(__name__)


# Internally we throw around these flat tuples of random important things about a job.
# Assigned ID
# Required cores
# Required memory
# Command to run
# Unit name of the job
# Environment dict for the job
# Accelerator requirements for the job
JobTuple = tuple[
    int, float, int, str, str, dict[str, str], list[AcceleratorRequirement]
]


class ExceededRetryAttempts(Exception):
    def __init__(self):
        super().__init__("Exceeded retry attempts talking to scheduler.")


class AbstractGridEngineBatchSystem(BatchSystemCleanupSupport):
    """
    A partial implementation of BatchSystemSupport for batch systems run on a
    standard HPC cluster. By default auto-deployment is not implemented.
    """

    class GridEngineThreadException(Exception):
        pass

    class GridEngineThread(Thread, metaclass=ABCMeta):
        def __init__(
            self,
            newJobsQueue: Queue,
            updatedJobsQueue: Queue,
            killQueue: Queue,
            killedJobsQueue: Queue,
            boss: "AbstractGridEngineBatchSystem",
        ) -> None:
            """
            Abstract thread interface class. All instances are created with five
            initial arguments (below). Note the Queue instances passed are empty.

            :param newJobsQueue: a Queue of new (unsubmitted) jobs
            :param updatedJobsQueue: a Queue of jobs that have been updated
            :param killQueue: a Queue of active jobs that need to be killed
            :param killedJobsQueue: Queue of killed jobs for this thread
            :param boss: the AbstractGridEngineBatchSystem instance that
                         controls this GridEngineThread

            """
            Thread.__init__(self)
            self.boss = boss
            self.boss.config.statePollingWait = (
                self.boss.config.statePollingWait or self.boss.getWaitDuration()
            )
            self.boss.config.state_polling_timeout = (
                self.boss.config.state_polling_timeout
                or self.boss.config.statePollingWait * 10
            )
            self.newJobsQueue = newJobsQueue
            self.updatedJobsQueue = updatedJobsQueue
            self.killQueue = killQueue
            self.killedJobsQueue = killedJobsQueue
            self.waitingJobs: list[JobTuple] = list()
            self.runningJobs = set()
            # TODO: Why do we need a lock for this? We have the GIL.
            self.runningJobsLock = Lock()
            self.batchJobIDs: dict[int, str] = dict()
            self._checkOnJobsCache = None
            self._checkOnJobsTimestamp = None
            self.exception = None

        def getBatchSystemID(self, jobID: int) -> str:
            """
            Get batch system-specific job ID

            Note: for the moment this is the only consistent way to cleanly get
            the batch system job ID

            :param jobID: Toil BatchSystem numerical job ID
            """
            if jobID not in self.batchJobIDs:
                raise RuntimeError("Unknown jobID, could not be converted")

            (job, task) = self.batchJobIDs[jobID]
            if task is None:
                return str(job)
            else:
                return str(job) + "." + str(task)

        def forgetJob(self, jobID: int) -> None:
            """
            Remove jobID passed

            :param jobID: toil job ID
            """
            with self.runningJobsLock:
                self.runningJobs.remove(jobID)
            del self.batchJobIDs[jobID]

        def createJobs(self, newJob: JobTuple) -> bool:
            """
            Create a new job with the given attributes.

            Implementation-specific; called by GridEngineThread.run()
            """
            activity = False
            # Load new job id if present:
            if newJob is not None:
                self.waitingJobs.append(newJob)
            # Launch jobs as necessary:
            while len(self.waitingJobs) > 0 and len(self.runningJobs) < int(
                self.boss.config.max_jobs
            ):
                activity = True
                jobID, cpu, memory, command, jobName, environment, gpus = (
                    self.waitingJobs.pop(0)
                )
                if self.boss.config.memory_is_product and cpu > 1:
                    memory = memory // cpu
                # prepare job submission command
                subLine = self.prepareSubmission(
                    cpu, memory, jobID, command, jobName, environment, gpus
                )
                logger.debug("Running %r", subLine)
                batchJobID = self.boss.with_retries(self.submitJob, subLine)
                if self.boss._outbox is not None:
                    # JobID corresponds to the toil version of the jobID, dif from jobstore idea of the id, batchjobid is what we get from slurm
                    self.boss._outbox.publish(
                        ExternalBatchIdMessage(
                            jobID, batchJobID, self.boss.__class__.__name__
                        )
                    )

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
            Kill any running jobs within thread
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
                    logger.debug("Killing job: %s", jobID)

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
                    exit_code = self.boss.with_retries(self.getJobExitCode, batchJobID)
                    if exit_code is not None:
                        logger.debug("Adding jobID %s to killedJobsQueue", jobID)
                        self.killedJobsQueue.put(jobID)
                        killList.remove(jobID)
                        self.forgetJob(jobID)
                if len(killList) > 0:
                    logger.warning(
                        "Some jobs weren't killed, trying again in %is.",
                        self.boss.sleepSeconds(),
                    )

            return True

        def checkOnJobs(self):
            """Check and update status of all running jobs.

            Respects statePollingWait and will return cached results if not within
            time period to talk with the scheduler.
            """

            if self._checkOnJobsTimestamp:
                time_since_last_check = (
                    datetime.now() - self._checkOnJobsTimestamp
                ).total_seconds()
                if time_since_last_check < self.boss.config.statePollingWait:
                    return self._checkOnJobsCache

            activity = False
            running_job_list = list(self.runningJobs)
            batch_job_id_list = [self.getBatchSystemID(j) for j in running_job_list]
            if batch_job_id_list:
                # Get the statuses as a batch
                statuses = self.boss.with_retries(
                    self.coalesce_job_exit_codes, batch_job_id_list
                )
                # We got the statuses as a batch
                for running_job_id, status in zip(running_job_list, statuses):
                    activity = self._handle_job_status(running_job_id, status, activity)

            self._checkOnJobsCache = activity
            self._checkOnJobsTimestamp = datetime.now()
            return activity

        def _handle_job_status(
            self,
            job_id: int,
            status: Union[int, tuple[int, Optional[BatchJobExitReason]], None],
            activity: bool,
        ) -> bool:
            """
            Helper method for checkOnJobs to handle job statuses
            """
            if status is not None:
                if isinstance(status, int):
                    code = status
                    reason = None
                else:
                    code, reason = status
                self.updatedJobsQueue.put(
                    UpdatedBatchJobInfo(
                        jobID=job_id, exitStatus=code, exitReason=reason, wallTime=None
                    )
                )
                self.forgetJob(job_id)
                return True
            return activity

        def _runStep(self):
            """return True if more jobs, False is all done"""
            activity = False
            newJob = None
            if not self.newJobsQueue.empty():
                activity = True
                newJob = self.newJobsQueue.get()
                if newJob is None:
                    logger.debug("Received queue sentinel.")
                    # Send out kill signals before stopping
                    self.killJobs()
                    return False
            if self.killJobs():
                activity = True
            if self.createJobs(newJob):
                activity = True
            if self.checkOnJobs():
                activity = True
            if not activity:
                logger.debug("No activity, sleeping for %is", self.boss.sleepSeconds())
            return True

        def run(self):
            """
            Run any new jobs
            """
            try:
                while self._runStep():
                    pass
            except Exception as ex:
                self.exception = ex
                logger.error("GridEngine like batch system failure: %s", ex)
                # don't raise exception as is_alive will still be set to false,
                # signalling exception in the thread as we expect the thread to
                # always be running for the duration of the workflow

        def coalesce_job_exit_codes(
            self, batch_job_id_list: list
        ) -> list[Union[int, tuple[int, Optional[BatchJobExitReason]], None]]:
            """
            Returns exit codes and possibly exit reasons for a list of jobs, or None if they are running.

            Called by GridEngineThread.checkOnJobs().

            The default implementation falls back on self.getJobExitCode and polls each job individually

            :param string batch_job_id_list: List of batch system job ID
            """
            statuses = []
            try:
                for batch_job_id in batch_job_id_list:
                    statuses.append(
                        self.boss.with_retries(self.getJobExitCode, batch_job_id)
                    )
            except CalledProcessErrorStderr as err:
                # This avoids the nested retry issue where we could issue n^2 retries when the backing scheduler somehow disappears
                # We catch the internal retry exception and raise something else so the outer retry doesn't retry the entire function again
                raise ExceededRetryAttempts() from err
            return statuses

        @abstractmethod
        def prepareSubmission(
            self,
            cpu: int,
            memory: int,
            jobID: int,
            command: str,
            jobName: str,
            job_environment: Optional[dict[str, str]] = None,
            gpus: Optional[int] = None,
        ) -> list[str]:
            """
            Preparation in putting together a command-line string
            for submitting to batch system (via submitJob().)

            :param: int cpu
            :param: int memory
            :param: int jobID: Toil job ID
            :param: string subLine: the command line string to be called
            :param: string jobName: the name of the Toil job, to provide metadata to batch systems if desired
            :param: dict job_environment: the environment variables to be set on the worker

            :rtype: List[str]
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
            by GridEngineThread.killJobs()

            :param string jobID: Toil job ID
            """
            raise NotImplementedError()

        @abstractmethod
        def getJobExitCode(
            self, batchJobID
        ) -> Union[int, tuple[int, Optional[BatchJobExitReason]], None]:
            """
            Returns job exit code and possibly an instance of abstractBatchSystem.BatchJobExitReason.

            Returns None if the job is still running.

            If the job is not running but the exit code is not available, it
            will be EXIT_STATUS_UNAVAILABLE_VALUE. Implementation-specific;
            called by GridEngineThread.checkOnJobs().

            The exit code will only be 0 if the job affirmatively succeeded.

            :param string batchjobID: batch system job ID
            """
            raise NotImplementedError()

    def __init__(
        self, config: Config, maxCores: float, maxMemory: int, maxDisk: int
    ) -> None:
        super().__init__(config, maxCores, maxMemory, maxDisk)
        self.config = config

        self.currentJobs = set()

        self.newJobsQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.killQueue = Queue()
        self.killedJobsQueue = Queue()
        # get the associated thread class here
        self.background_thread = self.GridEngineThread(
            self.newJobsQueue,
            self.updatedJobsQueue,
            self.killQueue,
            self.killedJobsQueue,
            self,
        )
        self.background_thread.start()
        self._getRunningBatchJobIDsTimestamp = None
        self._getRunningBatchJobIDsCache = {}

    @classmethod
    def supportsAutoDeployment(cls):
        return False

    def count_needed_gpus(self, job_desc: JobDescription):
        """
        Count the number of cluster-allocateable GPUs we want to allocate for the given job.
        """
        gpus = 0
        if isinstance(job_desc.accelerators, list):
            for accelerator in job_desc.accelerators:
                if accelerator["kind"] == "gpu":
                    gpus += accelerator["count"]
        else:
            gpus = job_desc.accelerators

        return gpus

    def issueBatchJob(
        self,
        command: str,
        job_desc: JobDescription,
        job_environment: Optional[dict[str, str]] = None,
    ):
        # Avoid submitting internal jobs to the batch queue, handle locally
        local_id = self.handleLocalJob(command, job_desc)
        if local_id is not None:
            return local_id
        else:
            self.check_resource_request(job_desc)
            gpus = self.count_needed_gpus(job_desc)
            job_id = self.getNextJobID()
            self.currentJobs.add(job_id)

            self.newJobsQueue.put(
                (
                    job_id,
                    job_desc.cores,
                    job_desc.memory,
                    command,
                    get_job_kind(job_desc.get_names()),
                    job_environment,
                    gpus,
                )
            )
            logger.debug(
                "Issued the job command: %s with job id: %s and job name %s",
                command,
                str(job_id),
                get_job_kind(job_desc.get_names()),
            )
        return job_id

    def killBatchJobs(self, jobIDs):
        """
        Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        self.killLocalJobs(jobIDs)
        jobIDs = set(jobIDs)
        logger.debug("Jobs to be killed: %r", jobIDs)
        for jobID in jobIDs:
            self.killQueue.put(jobID)
        while jobIDs:
            try:
                killedJobId = self.killedJobsQueue.get(timeout=10)
            except Empty:
                if not self.background_thread.is_alive():
                    raise self.GridEngineThreadException(
                        "Grid engine thread failed unexpectedly"
                    ) from self.background_thread.exception
                continue
            if killedJobId is None:
                break
            jobIDs.remove(killedJobId)
            if killedJobId in self._getRunningBatchJobIDsCache:
                # Running batch id cache can sometimes contain a job we kill, so to ensure cache doesn't contain the job, we delete it here
                del self._getRunningBatchJobIDsCache[killedJobId]
            if killedJobId in self.currentJobs:
                self.currentJobs.remove(killedJobId)
            if jobIDs:
                logger.debug(
                    "Some kills (%s) still pending, sleeping %is",
                    len(jobIDs),
                    self.sleepSeconds(),
                )

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
        if (
            self._getRunningBatchJobIDsTimestamp
            and (datetime.now() - self._getRunningBatchJobIDsTimestamp).total_seconds()
            < self.config.statePollingWait
        ):
            batchIds = self._getRunningBatchJobIDsCache
        else:
            batchIds = self.with_retries(self.background_thread.getRunningJobIDs)
            self._getRunningBatchJobIDsCache = batchIds
            self._getRunningBatchJobIDsTimestamp = datetime.now()
        batchIds.update(self.getRunningLocalJobIDs())
        return batchIds

    def getUpdatedBatchJob(self, maxWait):
        local_tuple = self.getUpdatedLocalJob(0)

        if not self.background_thread.is_alive():
            # kill remaining jobs on the thread
            self.background_thread.killJobs()
            raise self.GridEngineThreadException(
                "Unexpected GridEngineThread failure"
            ) from self.background_thread.exception
        if local_tuple:
            return local_tuple
        else:
            try:
                item = self.updatedJobsQueue.get(timeout=maxWait)
            except Empty:
                return None
            logger.debug("UpdatedJobsQueue Item: %s", item)
            self.currentJobs.remove(item.jobID)
            return item

    def shutdown(self) -> None:
        """
        Signals thread to shutdown (via sentinel) then cleanly joins the thread
        """

        for jobID in self.getIssuedBatchJobIDs():
            # Send kill signals to any jobs that might be running
            self.killQueue.put(jobID)

        self.shutdownLocal()
        newJobsQueue = self.newJobsQueue
        self.newJobsQueue = None

        newJobsQueue.put(None)
        self.background_thread.join()

        # Now in one thread, kill all the jobs
        if len(self.background_thread.runningJobs) > 0:
            logger.warning(
                "Cleaning up %s jobs still running at shutdown",
                len(self.background_thread.runningJobs),
            )
        for job in self.background_thread.runningJobs:
            self.killQueue.put(job)
        self.background_thread.killJobs()

    def setEnv(self, name, value=None):
        if value and "," in value:
            raise ValueError(
                type(self).__name__
                + " does not support commata in environment variable values"
            )
        return super().setEnv(name, value)

    @classmethod
    def getWaitDuration(self):
        return 1

    def sleepSeconds(self, sleeptime=1):
        """Helper function to drop on all state-querying functions to avoid over-querying."""
        time.sleep(sleeptime)
        return sleeptime

    def with_retries(self, operation, *args, **kwargs):
        """
        Call operation with args and kwargs. If one of the calls to a
        command fails, sleep and try again.
        """
        for attempt in old_retry(
            # Don't retry more often than the state polling wait.
            delays=[
                max(delay, self.config.statePollingWait) for delay in DEFAULT_DELAYS
            ],
            timeout=self.config.state_polling_timeout,
            predicate=lambda e: isinstance(e, CalledProcessErrorStderr),
        ):
            with attempt:
                try:
                    return operation(*args, **kwargs)
                except CalledProcessErrorStderr as err:
                    logger.error(
                        "Errored operation %s, code %d: %s",
                        operation.__name__,
                        err.returncode,
                        err.stderr,
                    )
                    # Raise up to the retry logic, which will retry until timeout
                    raise err
