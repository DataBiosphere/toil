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
from argparse import ArgumentParser, _ArgumentGroup
from contextlib import contextmanager
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    TypeVar,
    Union,
)

from toil.common import Config, Toil, cacheDirName
from toil.deferred import DeferredFunctionManager
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import JobDescription
from toil.resource import Resource

logger = logging.getLogger(__name__)

# Value to use as exitStatus in UpdatedBatchJobInfo.exitStatus when status is not available.
EXIT_STATUS_UNAVAILABLE_VALUE = 255

class BatchJobExitReason(enum.Enum):
    FINISHED: int = 1  # Successfully finished.
    FAILED: int = 2  # Job finished, but failed.
    LOST: int = 3  # Preemptable failure (job's executing host went away).
    KILLED: int = 4  # Job killed before finishing.
    ERROR: int = 5  # Internal error.
    MEMLIMIT: int = 6  # Job hit batch system imposed memory limit

class UpdatedBatchJobInfo(NamedTuple):
    jobID: int
    exitStatus: int
    """
    The exit status (integer value) of the job. 0 implies successful.

    EXIT_STATUS_UNAVAILABLE_VALUE is used when the exit status is not available (e.g. job is lost).
    """

    exitReason: Optional[BatchJobExitReason]
    wallTime: Union[float, int, None]

# Information required for worker cleanup on shutdown of the batch system.
class WorkerCleanupInfo(NamedTuple):
    workDir: str
    """workdir path (where the cache would go)"""

    workflowID: str
    """used to identify files specific to this workflow"""

    cleanWorkDir: str

class AbstractBatchSystem(ABC):
    """
    An abstract (as far as Python currently allows) base class to represent the interface the batch
    system must provide to Toil.
    """

    @classmethod
    @abstractmethod
    def supportsAutoDeployment(cls) -> bool:
        """
        Whether this batch system supports auto-deployment of the user script itself. If it does,
        the :meth:`.setUserScript` can be invoked to set the resource object representing the user
        script.

        Note to implementors: If your implementation returns True here, it should also override
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def supportsWorkerCleanup(cls) -> bool:
        """
        Indicates whether this batch system invokes
        :meth:`BatchSystemSupport.workerCleanup` after the last job for a
        particular workflow invocation finishes. Note that the term *worker*
        refers to an entire node, not just a worker process. A worker process
        may run more than one job sequentially, and more than one concurrent
        worker process may exist on a worker node, for the same workflow. The
        batch system is said to *shut down* after the last worker process
        terminates.
        """
        raise NotImplementedError()

    def setUserScript(self, userScript: Resource) -> None:
        """
        Set the user script for this workflow. This method must be called before the first job is
        issued to this batch system, and only if :meth:`.supportsAutoDeployment` returns True,
        otherwise it will raise an exception.

        :param userScript: the resource object representing the user script
               or module and the modules it depends on.
        """
        raise NotImplementedError()

    @abstractmethod
    def issueBatchJob(self, jobDesc: JobDescription, job_environment: Optional[Dict[str, str]] = None) -> int:
        """
        Issues a job with the specified command to the batch system and returns a unique jobID.

        :param jobDesc a toil.job.JobDescription
        :param job_environment: a collection of job-specific environment variables
                                to be set on the worker.

        :return: a unique jobID that can be used to reference the newly issued job
        """
        raise NotImplementedError()

    @abstractmethod
    def killBatchJobs(self, jobIDs: List[int]) -> None:
        """
        Kills the given job IDs. After returning, the killed jobs will not
        appear in the results of getRunningBatchJobIDs. The killed job will not
        be returned from getUpdatedBatchJob.

        :param jobIDs: list of IDs of jobs to kill
        """
        raise NotImplementedError()

    # FIXME: Return value should be a set (then also fix the tests)

    @abstractmethod
    def getIssuedBatchJobIDs(self) -> List[int]:
        """
        Gets all currently issued jobs

        :return: A list of jobs (as jobIDs) currently issued (may be running, or may be
                 waiting to be run). Despite the result being a list, the ordering should not
                 be depended upon.
        """
        raise NotImplementedError()

    @abstractmethod
    def getRunningBatchJobIDs(self) -> Dict[int, float]:
        """
        Gets a map of jobs as jobIDs that are currently running (not just waiting)
        and how long they have been running, in seconds.

        :return: dictionary with currently running jobID keys and how many seconds they have
                 been running as the value
        """
        raise NotImplementedError()

    @abstractmethod
    def getUpdatedBatchJob(self, maxWait: int) -> Optional[UpdatedBatchJobInfo]:
        """
        Returns information about job that has updated its status (i.e. ceased
        running, either successfully or with an error). Each such job will be
        returned exactly once.

        Does not return info for jobs killed by killBatchJobs, although they
        may cause None to be returned earlier than maxWait.

        :param maxWait: the number of seconds to block, waiting for a result

        :return: If a result is available, returns UpdatedBatchJobInfo.
                 Otherwise it returns None. wallTime is the number of seconds (a strictly
                 positive float) in wall-clock time the job ran for, or None if this
                 batch system does not support tracking wall time.
        """
        raise NotImplementedError()

    def getSchedulingStatusMessage(self) -> Optional[str]:
        """
        Get a log message fragment for the user about anything that might be
        going wrong in the batch system, if available.

        If no useful message is available, return None.

        This can be used to report what resource is the limiting factor when
        scheduling jobs, for example. If the leader thinks the workflow is
        stuck, the message can be displayed to the user to help them diagnose
        why it might be stuck.

        :return: User-directed message about scheduling state.
        """

        # Default implementation returns None.
        # Override to provide scheduling status information.
        return None

    @abstractmethod
    def shutdown(self) -> None:
        """
        Called at the completion of a toil invocation.
        Should cleanly terminate all worker threads.
        """
        raise NotImplementedError()

    def setEnv(self, name: str, value: Optional[str] = None) -> None:
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
    def add_options(cls, parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
        """
        If this batch system provides any command line options, add them to the given parser.
        """

    OptionType = TypeVar('OptionType')
    @classmethod
    def setOptions(cls, setOption: Callable[[str, Optional[Callable[[Any], OptionType]], Optional[Callable[[OptionType], None]], Optional[OptionType], Optional[List[str]]], None]) -> None:
        """
        Process command line or configuration options relevant to this batch system.

        :param setOption: A function with signature
            setOption(option_name, parsing_function=None, check_function=None, default=None, env=None)
            returning nothing, used to update run configuration as a side effect.
        """
        # TODO: change type to a Protocol to express kwarg names, or else use a
        # different interface (generator?)

    def getWorkerContexts(self) -> List[ContextManager[Any]]:
        """
        Get a list of picklable context manager objects to wrap worker work in,
        in order.

        Can be used to ask the Toil worker to do things in-process (such as
        configuring environment variables, hot-deploying user scripts, or
        cleaning up a node) that would otherwise require a wrapping "executor"
        process.
        """
        return []


class BatchSystemSupport(AbstractBatchSystem):
    """Partial implementation of AbstractBatchSystem, support methods."""

    def __init__(self, config: Config, maxCores: float, maxMemory: int, maxDisk: int) -> None:
        """
        Initialize initial state of the object.

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
        super().__init__()
        self.config = config
        self.maxCores = maxCores
        self.maxMemory = maxMemory
        self.maxDisk = maxDisk
        self.environment: Dict[str, str] = {}
        if config.workflowID is None:
            raise Exception("config.workflowID must be set")
        else:
            self.workerCleanupInfo = WorkerCleanupInfo(
                workDir=config.workDir,
                workflowID=config.workflowID,
                cleanWorkDir=config.cleanWorkDir,
            )

    def checkResourceRequest(self, memory: int, cores: float, disk: int, job_name: str = '', detail: str = '') -> None:
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

    def setEnv(self, name: str, value: Optional[str] = None) -> None:
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

class NodeInfo:
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
    def __init__(self, coresUsed: float, memoryUsed: float,
                 coresTotal: float, memoryTotal: int,
                 requestedCores: float, requestedMemory: int,
                 workers: int) -> None:
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
    def getNodes(self, preemptable: Optional[bool] = None) -> Dict[str, NodeInfo]:
        """
        Returns a dictionary mapping node identifiers of preemptable or non-preemptable nodes to
        NodeInfo objects, one for each node.

        :param preemptable: If True (False) only (non-)preemptable nodes will be returned.
               If None, all nodes will be returned.
        """
        raise NotImplementedError()

    @abstractmethod
    def nodeInUse(self, nodeIP: str) -> bool:
        """
        Can be used to determine if a worker node is running any tasks. If the node is doesn't
        exist, this function should simply return False.

        :param nodeIP: The worker nodes private IP address

        :return: True if the worker node has been issued any tasks, else False
        """
        raise NotImplementedError()

    # TODO: May be unused!
    @abstractmethod
    @contextmanager
    def nodeFiltering(self, filter: Optional[Callable[[NodeInfo], bool]]) -> Iterator[None]:
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
        """
        raise NotImplementedError()

    @abstractmethod
    def ignoreNode(self, nodeAddress: str) -> None:
        """
        Stop sending jobs to this node. Used in autoscaling
        when the autoscaler is ready to terminate a node, but
        jobs are still running. This allows the node to be terminated
        after the current jobs have finished.

        :param nodeAddress: IP address of node to ignore.
        """
        raise NotImplementedError()

    @abstractmethod
    def unignoreNode(self, nodeAddress: str) -> None:
        """
        Stop ignoring this address, presumably after
        a node with this address has been terminated. This allows for the
        possibility of a new node having the same address as a terminated one.
        """
        raise NotImplementedError()


class InsufficientSystemResources(Exception):
    pass
