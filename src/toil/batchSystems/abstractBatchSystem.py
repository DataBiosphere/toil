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
import enum
import logging
import os
import shutil
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Optional, Tuple, Union, Dict, NamedTuple

from toil.batchSystems.registry import (BATCH_SYSTEM_FACTORY_REGISTRY,
                                        DEFAULT_BATCH_SYSTEM)
from toil.common import Toil, cacheDirName, Config
from toil.deferred import DeferredFunctionManager
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.lib.threading import LastProcessStandingArena

try:
    from toil.cwl.cwltoil import CWL_INTERNAL_JOBS
except ImportError:
    # CWL extra not installed
    CWL_INTERNAL_JOBS = ()

# Value to use as exitStatus in UpdatedBatchJobInfo.exitStatus when status is not available.
EXIT_STATUS_UNAVAILABLE_VALUE = 255
logger = logging.getLogger(__name__)


UpdatedBatchJobInfo = NamedTuple('UpdatedBatchJobInfo',
    [('jobID', str),
     # The exit status (integer value) of the job. 0 implies successful.
     # EXIT_STATUS_UNAVAILABLE_VALUE is used when the exit status is not available (e.g. job is lost).
     ('exitStatus', int),
     ('exitReason', Union[int, None]),  # The exit reason, if available. One of BatchJobExitReason enum.
     ('wallTime', Union[float, int, None])])


# Information required for worker cleanup on shutdown of the batch system.
WorkerCleanupInfo = NamedTuple('WorkerCleanupInfo',
    [('workDir', str),  # workdir path (where the cache would go)
     ('workflowID', int),  # used to identify files specific to this workflow
     ('cleanWorkDir', bool)])


class BatchJobExitReason(enum.Enum):
    FINISHED: int = 1  # Successfully finished.
    FAILED: int = 2  # Job finished, but failed.
    LOST: int = 3  # Preemptable failure (job's executing host went away).
    KILLED: int = 4  # Job killed before finishing.
    ERROR: int = 5  # Internal error.
    MEMLIMIT: int = 6  # Job hit batch system imposed memory limit


class AbstractBatchSystem(ABC):
    """
    An abstract (as far as Python currently allows) base class to represent the interface the batch
    system must provide to Toil.
    """
    @classmethod
    @abstractmethod
    def supportsAutoDeployment(cls):
        """
        Whether this batch system supports auto-deployment of the user script itself. If it does,
        the :meth:`.setUserScript` can be invoked to set the resource object representing the user
        script.

        Note to implementors: If your implementation returns True here, it should also override

        :rtype: bool
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def supportsWorkerCleanup(cls):
        """
        Indicates whether this batch system invokes
        :meth:`BatchSystemSupport.workerCleanup` after the last job for a
        particular workflow invocation finishes. Note that the term *worker*
        refers to an entire node, not just a worker process. A worker process
        may run more than one job sequentially, and more than one concurrent
        worker process may exist on a worker node, for the same workflow. The
        batch system is said to *shut down* after the last worker process
        terminates.

        :rtype: bool
        """
        raise NotImplementedError()

    def setUserScript(self, userScript):
        """
        Set the user script for this workflow. This method must be called before the first job is
        issued to this batch system, and only if :meth:`.supportsAutoDeployment` returns True,
        otherwise it will raise an exception.

        :param toil.resource.Resource userScript: the resource object representing the user script
               or module and the modules it depends on.
        """
        raise NotImplementedError()

    @abstractmethod
    def issueBatchJob(self, jobDesc, job_environment: Optional[Dict[str, str]] = None):
        """
        Issues a job with the specified command to the batch system and returns a unique jobID.

        :param jobDesc a toil.job.JobDescription
        :param job_environment: a collection of job-specific environment variables
                                to be set on the worker.

        :return: a unique jobID that can be used to reference the newly issued job
        :rtype: int
        """
        raise NotImplementedError()

    @abstractmethod
    def killBatchJobs(self, jobIDs):
        """
        Kills the given job IDs. After returning, the killed jobs will not
        appear in the results of getRunningBatchJobIDs. The killed job will not
        be returned from getUpdatedBatchJob.

        :param jobIDs: list of IDs of jobs to kill
        :type jobIDs: list[int]
        """
        raise NotImplementedError()

    # FIXME: Return value should be a set (then also fix the tests)

    @abstractmethod
    def getIssuedBatchJobIDs(self):
        """
        Gets all currently issued jobs

        :return: A list of jobs (as jobIDs) currently issued (may be running, or may be
                 waiting to be run). Despite the result being a list, the ordering should not
                 be depended upon.
        :rtype: list[str]
        """
        raise NotImplementedError()

    @abstractmethod
    def getRunningBatchJobIDs(self):
        """
        Gets a map of jobs as jobIDs that are currently running (not just waiting)
        and how long they have been running, in seconds.

        :return: dictionary with currently running jobID keys and how many seconds they have
                 been running as the value
        :rtype: dict[int,float]
        """
        raise NotImplementedError()

    @abstractmethod
    def getUpdatedBatchJob(self, maxWait):
        """
        Returns information about job that has updated its status (i.e. ceased
        running, either successfully or with an error). Each such job will be
        returned exactly once.

        Does not return info for jobs killed by killBatchJobs, although they
        may cause None to be returned earlier than maxWait.

        :param float maxWait: the number of seconds to block, waiting for a result

        :rtype: UpdatedBatchJobInfo or None
        :return: If a result is available, returns UpdatedBatchJobInfo.
                 Otherwise it returns None. wallTime is the number of seconds (a strictly
                 positive float) in wall-clock time the job ran for, or None if this
                 batch system does not support tracking wall time.
        """
        raise NotImplementedError()

    def getSchedulingStatusMessage(self):
        """
        Get a log message fragment for the user about anything that might be
        going wrong in the batch system, if available.

        If no useful message is available, return None.

        This can be used to report what resource is the limiting factor when
        scheduling jobs, for example. If the leader thinks the workflow is
        stuck, the message can be displayed to the user to help them diagnose
        why it might be stuck.

        :rtype: str or None
        :return: User-directed message about scheduling state.
        """

        # Default implementation returns None.
        # Override to provide scheduling status information.
        return None

    @abstractmethod
    def shutdown(self):
        """
        Called at the completion of a toil invocation.
        Should cleanly terminate all worker threads.
        """
        raise NotImplementedError()

    def setEnv(self, name, value=None):
        """
        Set an environment variable for the worker process before it is launched. The worker
        process will typically inherit the environment of the machine it is running on but this
        method makes it possible to override specific variables in that inherited environment
        before the worker is launched. Note that this mechanism is different to the one used by
        the worker internally to set up the environment of a job. A call to this method affects
        all jobs issued after this method returns. Note to implementors: This means that you
        would typically need to copy the variables before enqueuing a job.

        If no value is provided it will be looked up from the current environment.
        """
        raise NotImplementedError()

    @classmethod
    def setOptions(cls, setOption):
        """
        Process command line or configuration options relevant to this batch system.
        The

        :param setOption: A function with signature setOption(varName, parsingFn=None, checkFn=None, default=None)
           used to update run configuration
        """

    def getWorkerContexts(self):
        """
        Get a list of picklable context manager objects to wrap worker work in,
        in order.

        Can be used to ask the Toil worker to do things in-process (such as
        configuring environment variables, hot-deploying user scripts, or
        cleaning up a node) that would otherwise require a wrapping "executor"
        process.

        :rtype: list
        """
        return []


class BatchSystemSupport(AbstractBatchSystem):
    """
    Partial implementation of AbstractBatchSystem, support methods.
    """

    def __init__(self, config: Config, maxCores: float, maxMemory: int, maxDisk: int):
        """
        Initializes initial state of the object

        :param toil.common.Config config: object is setup by the toilSetup script and
          has configuration parameters for the jobtree. You can add code
          to that script to get parameters for your batch system.

        :param float maxCores: the maximum number of cores the batch system can
          request for any one job

        :param int maxMemory: the maximum amount of memory the batch system can
          request for any one job, in bytes

        :param int maxDisk: the maximum amount of disk space the batch system can
          request for any one job, in bytes
        """
        super(BatchSystemSupport, self).__init__()
        self.config = config
        self.maxCores = maxCores
        self.maxMemory = maxMemory
        self.maxDisk = maxDisk
        self.environment: Dict[str, str] = {}
        self.workerCleanupInfo = WorkerCleanupInfo(workDir=self.config.workDir,
                                                   workflowID=self.config.workflowID,
                                                   cleanWorkDir=self.config.cleanWorkDir)

    def checkResourceRequest(self, memory: int, cores: float, disk: int, job_name: str = '', detail: str = ''):
        """
        Check resource request is not greater than that available or allowed.

        :param int memory: amount of memory being requested, in bytes

        :param float cores: number of cores being requested

        :param int disk: amount of disk space being requested, in bytes

        :param str job_name: Name of the job being checked, for generating a useful error report.

        :param str detail: Batch-system-specific message to include in the error.

        :raise InsufficientSystemResources: raised when a resource is requested in an amount
               greater than allowed
        """
        batch_system = self.__class__.__name__ or 'this batch system'
        for resource, requested, available in [('cores', cores, self.maxCores),
                                               ('memory', memory, self.maxMemory),
                                               ('disk', disk, self.maxDisk)]:
            assert requested is not None
            if requested > available:
                unit = 'bytes of ' if resource in ('disk', 'memory') else ''
                R = f'The job {job_name} is r' if job_name else 'R'
                if resource == 'disk':
                    msg = (f'{R}equesting {requested} {unit}{resource} for temporary space, '
                           f'more than the maximum of {available} {unit}{resource} of free space on '
                           f'{self.config.workDir} that {batch_system} was configured with, or enforced '
                           f'by --max{resource.capitalize()}.  Try setting/changing the toil option '
                           f'"--workDir" or changing the base temporary directory by setting TMPDIR.')
                else:
                    msg = (f'{R}equesting {requested} {unit}{resource}, more than the maximum of '
                           f'{available} {unit}{resource} that {batch_system} was configured with, '
                           f'or enforced by --max{resource.capitalize()}.')
                if detail:
                    msg += detail

                raise InsufficientSystemResources(msg)

    def setEnv(self, name, value=None):
        """
        Set an environment variable for the worker process before it is launched. The worker
        process will typically inherit the environment of the machine it is running on but this
        method makes it possible to override specific variables in that inherited environment
        before the worker is launched. Note that this mechanism is different to the one used by
        the worker internally to set up the environment of a job. A call to this method affects
        all jobs issued after this method returns. Note to implementors: This means that you
        would typically need to copy the variables before enqueuing a job.

        If no value is provided it will be looked up from the current environment.

        :param str name: the environment variable to be set on the worker.

        :param str value: if given, the environment variable given by name will be set to this value.
               if None, the variable's current value will be used as the value on the worker

        :raise RuntimeError: if value is None and the name cannot be found in the environment
        """
        if value is None:
            try:
                value = os.environ[name]
            except KeyError:
                raise RuntimeError(f"{name} does not exist in current environment")
        self.environment[name] = value

    def formatStdOutErrPath(self, toil_job_id: int, cluster_job_id: str, std: str) -> str:
        """
        Format path for batch system standard output/error and other files
        generated by the batch system itself.

        Files will be written to the Toil work directory (which may
        be on a shared file system) with names containing both the Toil and
        batch system job IDs, for ease of debugging job failures.

        :param: int toil_job_id : The unique id that Toil gives a job.
        :param: cluster_job_id : What the cluster, for example, GridEngine, uses as its internal job id.
        :param: string std : The provenance of the stream (for example: 'err' for 'stderr' or 'out' for 'stdout')

        :rtype: string : Formatted filename; however if self.config.noStdOutErr is true,
             returns '/dev/null' or equivalent.
        """
        if self.config.noStdOutErr:
            return os.devnull

        fileName: str = f'toil_{self.config.workflowID}.{toil_job_id}.{cluster_job_id}.{std}.log'
        workDir: str = Toil.getToilWorkDir(self.config.workDir)
        return os.path.join(workDir, fileName)

    @staticmethod
    def workerCleanup(info: WorkerCleanupInfo) -> None:
        """
        Cleans up the worker node on batch system shutdown. Also see :meth:`supportsWorkerCleanup`.

        :param WorkerCleanupInfo info: A named tuple consisting of all the relevant information
               for cleaning up the worker.
        """
        assert isinstance(info, WorkerCleanupInfo)
        workflowDir = Toil.getLocalWorkflowDir(info.workflowID, info.workDir)
        DeferredFunctionManager.cleanupWorker(workflowDir)
        workflowDirContents = os.listdir(workflowDir)
        AbstractFileStore.shutdownFileStore(workflowDir, info.workflowID)
        if (info.cleanWorkDir == 'always'
            or info.cleanWorkDir in ('onSuccess', 'onError')
            and workflowDirContents in ([], [cacheDirName(info.workflowID)])):
            shutil.rmtree(workflowDir, ignore_errors=True)


class BatchSystemLocalSupport(BatchSystemSupport):
    """
    Adds a local queue for helper jobs, useful for CWL & others
    """

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(BatchSystemLocalSupport, self).__init__(config, maxCores, maxMemory, maxDisk)
        self.localBatch = BATCH_SYSTEM_FACTORY_REGISTRY[DEFAULT_BATCH_SYSTEM]()(
                config, config.maxLocalJobs, maxMemory, maxDisk)

    def handleLocalJob(self, jobDesc):  # type: (Any) -> Optional[int]
        """
        To be called by issueBatchJobs.

        Returns the jobID if the jobDesc has been submitted to the local queue,
        otherwise returns None
        """
        if (not self.config.runCwlInternalJobsOnWorkers
                and jobDesc.jobName.startswith(CWL_INTERNAL_JOBS)):
            return self.localBatch.issueBatchJob(jobDesc)
        else:
            return None

    def killLocalJobs(self, jobIDs):
        """
        To be called by killBatchJobs. Will kill all local jobs that match the
        provided jobIDs.
        """
        self.localBatch.killBatchJobs(jobIDs)

    def getIssuedLocalJobIDs(self):
        """To be called by getIssuedBatchJobIDs"""
        return self.localBatch.getIssuedBatchJobIDs()

    def getRunningLocalJobIDs(self):
        """To be called by getRunningBatchJobIDs()."""
        return self.localBatch.getRunningBatchJobIDs()

    def getUpdatedLocalJob(self, maxWait):
        # type: (int) -> Optional[Tuple[int, int, int]]
        """To be called by getUpdatedBatchJob()"""
        return self.localBatch.getUpdatedBatchJob(maxWait)

    def getNextJobID(self):  # type: () -> int
        """
        Must be used to get job IDs so that the local and batch jobs do not
        conflict.
        """
        with self.localBatch.jobIndexLock:
            jobID = self.localBatch.jobIndex
            self.localBatch.jobIndex += 1
        return jobID

    def shutdownLocal(self):  # type: () -> None
        """To be called from shutdown()"""
        self.localBatch.shutdown()


class BatchSystemCleanupSupport(BatchSystemLocalSupport):
    """
    Adds cleanup support when the last running job leaves a node, for batch
    systems that can't provide it using the backing scheduler.
    """

    @classmethod
    def supportsWorkerCleanup(cls):
        return True

    def getWorkerContexts(self):
        # Tell worker to register for and invoke cleanup

        # Create a context manager that has a copy of our cleanup info
        context = WorkerCleanupContext(self.workerCleanupInfo)

        # Send it along so the worker works inside of it
        contexts = super(BatchSystemCleanupSupport, self).getWorkerContexts()
        contexts.append(context)
        return contexts

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(BatchSystemCleanupSupport, self).__init__(config, maxCores, maxMemory, maxDisk)

class WorkerCleanupContext:
    """
    Context manager used by :class:`BatchSystemCleanupSupport` to implement
    cleanup on a node after the last worker is done working.

    Gets wrapped around the worker's work.
    """

    def __init__(self, workerCleanupInfo):
        """
        Wrap the given workerCleanupInfo in a context manager.

        :param WorkerCleanupInfo workerCleanupInfo: Info to use to clean up the worker if we are
                                                    the last to exit the context manager.
        """


        self.workerCleanupInfo = workerCleanupInfo
        self.arena = None

    def __enter__(self):
        # Set up an arena so we know who is the last worker to leave
        self.arena = LastProcessStandingArena(Toil.getToilWorkDir(self.workerCleanupInfo.workDir),
                                              self.workerCleanupInfo.workflowID + '-cleanup')
        self.arena.enter()

    def __exit__(self, type, value, traceback):
        for _ in self.arena.leave():
            # We are the last concurrent worker to finish.
            # Do batch system cleanup.
            logger.debug('Cleaning up worker')
            BatchSystemSupport.workerCleanup(self.workerCleanupInfo)
        # We have nothing to say about exceptions
        return False

class NodeInfo(object):
    """
    The coresUsed attribute  is a floating point value between 0 (all cores idle) and 1 (all cores
    busy), reflecting the CPU load of the node.

    The memoryUsed attribute is a floating point value between 0 (no memory used) and 1 (all memory
    used), reflecting the memory pressure on the node.

    The coresTotal and memoryTotal attributes are the node's resources, not just the used resources

    The requestedCores and requestedMemory attributes are all the resources that Toil Jobs have reserved on the
    node, regardless of whether the resources are actually being used by the Jobs.

    The workers attribute is an integer reflecting the number of workers currently active workers
    on the node.
    """
    def __init__(self, coresUsed, memoryUsed, coresTotal, memoryTotal,
                 requestedCores, requestedMemory, workers):
        self.coresUsed = coresUsed
        self.memoryUsed = memoryUsed

        self.coresTotal = coresTotal
        self.memoryTotal = memoryTotal

        self.requestedCores = requestedCores
        self.requestedMemory = requestedMemory

        self.workers = workers


class AbstractScalableBatchSystem(AbstractBatchSystem):
    """
    A batch system that supports a variable number of worker nodes. Used by :class:`toil.
    provisioners.clusterScaler.ClusterScaler` to scale the number of worker nodes in the cluster
    up or down depending on overall load.
    """

    @abstractmethod
    def getNodes(self, preemptable=None):
        """
        Returns a dictionary mapping node identifiers of preemptable or non-preemptable nodes to
        NodeInfo objects, one for each node.

        :param bool preemptable: If True (False) only (non-)preemptable nodes will be returned.
               If None, all nodes will be returned.

        :rtype: dict[str,NodeInfo]
        """
        raise NotImplementedError()

    @abstractmethod
    def nodeInUse(self, nodeIP):
        """
        Can be used to determine if a worker node is running any tasks. If the node is doesn't
        exist, this function should simply return False.

        :param str nodeIP: The worker nodes private IP address

        :return: True if the worker node has been issued any tasks, else False
        :rtype: bool
        """
        raise NotImplementedError()

    @abstractmethod
    @contextmanager
    def nodeFiltering(self, filter):
        """
        Used to prevent races in autoscaling where
        1) nodes have reported to the autoscaler as having no jobs
        2) scaler decides to terminate these nodes. In parallel the batch system assigns jobs to the same nodes
        3) scaler terminates nodes, resulting in job failures for all jobs on that node.

        Call this method prior to node termination to ensure that nodes being considered for termination are not
        assigned new jobs. Call the method again passing None as the filter to disable the filtering
        after node termination is done.

        :param method: This will be used as a filter on nodes considered when assigning new jobs.
            After this context manager exits the filter should be removed
        :rtype: None
        """
        raise NotImplementedError()

    @abstractmethod
    def ignoreNode(self, nodeAddress):
        """
        Stop sending jobs to this node. Used in autoscaling
        when the autoscaler is ready to terminate a node, but
        jobs are still running. This allows the node to be terminated
        after the current jobs have finished.

        :param str: IP address of node to ignore.
        :rtype: None
        """
        raise NotImplementedError()

    @abstractmethod
    def unignoreNode(self, nodeAddress):
        """
        Stop ignoring this address, presumably after
        a node with this address has been terminated. This allows for the
        possibility of a new node having the same address as a terminated one.
        """
        raise NotImplementedError()


class InsufficientSystemResources(Exception):
    pass
