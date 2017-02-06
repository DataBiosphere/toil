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
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from Queue import Queue, Empty

from bd2k.util.objects import abstractclassmethod

from toil.common import Toil, cacheDirName
from toil.fileStore import shutdownFileStore

logger = logging.getLogger(__name__)

# TODO: should this be an attribute?  Used in the worker and the batch system
sleepSeconds = 1

# A class containing the information required for worker cleanup on shutdown of the batch system.
WorkerCleanupInfo = namedtuple('WorkerCleanupInfo', (
    # A path to the value of config.workDir (where the cache would go)
    'workDir',
    # The value of config.workflowID (used to identify files specific to this workflow)
    'workflowID',
    # The value of the cleanWorkDir flag
    'cleanWorkDir'))

class AbstractBatchSystem(object):
    """
    An abstract (as far as Python currently allows) base class to represent the interface the batch
    system must provide to Toil.
    """

    __metaclass__ = ABCMeta

    # noinspection PyMethodParameters
    @abstractclassmethod
    def supportsHotDeployment(cls):
        """
        Whether this batch system supports hot deployment of the user script itself. If it does,
        the :meth:`.setUserScript` can be invoked to set the resource object representing the user
        script.

        Note to implementors: If your implementation returns True here, it should also override

        :rtype: bool
        """
        raise NotImplementedError()

    # noinspection PyMethodParameters
    @abstractclassmethod
    def supportsWorkerCleanup(cls):
        """
        Indicates whether this batch system invokes :meth:`workerCleanup` after the last job for
        a particular workflow invocation finishes. Note that the term *worker* refers to an
        entire node, not just a worker process. A worker process may run more than one job
        sequentially, and more than one concurrent worker process may exist on a worker node,
        for the same workflow. The batch system is said to *shut down* after the last worker
        process terminates.

        :rtype: bool
        """
        raise NotImplementedError()

    def setUserScript(self, userScript):
        """
        Set the user script for this workflow. This method must be called before the first job is
        issued to this batch system, and only if :meth:`supportsHotDeployment` returns True,
        otherwise it will raise an exception.

        :param toil.resource.Resource userScript: the resource object representing the user script
               or module and the modules it depends on.
        """
        raise NotImplementedError()

    @abstractmethod
    def issueBatchJob(self, jobNode):
        """
        Issues a job with the specified command to the batch system and returns a unique jobID.

        :param str command: the string to run as a command,

        :param int memory: int giving the number of bytes of memory the job needs to run

        :param float cores: the number of cores needed for the job

        :param int disk: int giving the number of bytes of disk space the job needs to run

        :param bool preemptable: True if the job can be run on a preemptable node

        :return: a unique jobID that can be used to reference the newly issued job
        :rtype: int
        """
        raise NotImplementedError()

    @abstractmethod
    def killBatchJobs(self, jobIDs):
        """
        Kills the given job IDs.

        :param list[int] jobIDs: list of IDs of jobs to kill
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
        :rtype: dict[str,float]
        """
        raise NotImplementedError()

    @abstractmethod
    def getUpdatedBatchJob(self, maxWait):
        """
        Returns a job that has updated its status.

        :param float maxWait: the number of seconds to block, waiting for a result

        :rtype: (str, int)|None
        :return: If a result is available, returns a tuple (jobID, exitValue, wallTime).
                 Otherwise it returns None. wallTime is the number of seconds (a float) in
                 wall-clock time the job ran for or None if this batch system does not support
                 tracking wall time. Returns None for jobs that were killed.
        """
        raise NotImplementedError()

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
    def getRescueBatchJobFrequency(cls):
        """
        Gets the period of time to wait (floating point, in seconds) between checking for
        missing/overlong jobs.
        """
        raise NotImplementedError()


class BatchSystemSupport(AbstractBatchSystem):
    """
    Partial implementation of AbstractBatchSystem, support methods.
    """

    def __init__(self, config, maxCores, maxMemory, maxDisk):
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
        self.environment = {}
        """
        :type: dict[str,str]
        """
        self.workerCleanupInfo = WorkerCleanupInfo(workDir=self.config.workDir,
                                                   workflowID=self.config.workflowID,
                                                   cleanWorkDir=self.config.cleanWorkDir)

    def checkResourceRequest(self, memory, cores, disk):
        """
        Check resource request is not greater than that available or allowed.

        :param int memory: amount of memory being requested, in bytes

        :param float cores: number of cores being requested

        :param int disk: amount of disk space being requested, in bytes

        :raise InsufficientSystemResources: raised when a resource is requested in an amount
               greater than allowed
        """
        assert memory is not None
        assert disk is not None
        assert cores is not None
        if cores > self.maxCores:
            raise InsufficientSystemResources('cores', cores, self.maxCores)
        if memory > self.maxMemory:
            raise InsufficientSystemResources('memory', memory, self.maxMemory)
        if disk > self.maxDisk:
            raise InsufficientSystemResources('disk', disk, self.maxDisk)


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

        NB: Only the Mesos and single-machine batch systems support passing environment
        variables. On other batch systems, this method has no effect. See
        https://github.com/BD2KGenomics/toil/issues/547.

        :param str name: the environment variable to be set on the worker.

        :param str value: if given, the environment variable given by name will be set to this value.
               if None, the variable's current value will be used as the value on the worker

        :raise RuntimeError: if value is None and the name cannot be found in the environment
        """
        if value is None:
            try:
                value = os.environ[name]
            except KeyError:
                raise RuntimeError("%s does not exist in current environment", name)
        self.environment[name] = value

    @classmethod
    def getRescueBatchJobFrequency(cls):
        """
        Gets the period of time to wait (floating point, in seconds) between checking for
        missing/overlong jobs.

        :return: time in seconds to wait in between checking for lost jobs
        :rtype: float
        """
        raise NotImplementedError()

    def _getResultsFileName(self, toilPath):
        """
        Get a path for the batch systems to store results. GridEngine, slurm,
        and LSF currently use this and only work if locator is file.
        """
        # Use  parser to extract the path and type
        locator, filePath = Toil.parseLocator(toilPath)
        assert locator == "file"
        return os.path.join(filePath, "results.txt")

    @staticmethod
    def workerCleanup(info):
        """
        Cleans up the worker node on batch system shutdown. Also see :meth:`supportsWorkerCleanup`.

        :param WorkerCleanupInfo info: A named tuple consisting of all the relevant information
               for cleaning up the worker.
        """
        assert isinstance(info, WorkerCleanupInfo)
        workflowDir = Toil.getWorkflowDir(info.workflowID, info.workDir)
        workflowDirContents = os.listdir(workflowDir)
        shutdownFileStore(workflowDir, info.workflowID)
        if (info.cleanWorkDir == 'always'
            or info.cleanWorkDir in ('onSuccess', 'onError')
            and workflowDirContents in ([], [cacheDirName(info.workflowID)])):
            shutil.rmtree(workflowDir)

class NodeInfo(object):
    """
    The cores attribute  is a floating point value between 0 (all cores idle) and 1 (all cores
    busy), reflecting the CPU load of the node.

    The memory attribute is a floating point value between 0 (no memory used) and 1 (all memory
    used), reflecting the memory pressure on the node.

    The workers attribute is an integer reflecting the number of workers currently active workers
    on the node.
    """
    def __init__(self, coresUsed, memoryUsed, coresTotal, memoryTotal,
                 requestedCores, requestedMemory, workers):
        self.coresUsed = coresUsed
        self.totalCores = coresTotal
        self.memoryUsed = memoryUsed
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

class InsufficientSystemResources(Exception):
    """
    To be raised when a job requests more of a particular resource than is either currently allowed
    or avaliable
    """
    def __init__(self, resource, requested, available):
        """
        Creates an instance of this exception that indicates which resource is insufficient for current
        demands, as well as the amount requested and amount actually available.

        :param str resource: string representing the resource type

        :param int|float requested: the amount of the particular resource requested that resulted
               in this exception

        :param int|float available: amount of the particular resource actually available
        """
        self.requested = requested
        self.available = available
        self.resource = resource

    def __str__(self):
        return 'Requesting more {} than either physically available, or enforced by --max{}. ' \
               'Requested: {}, Available: {}'.format(self.resource, self.resource.capitalize(),
                                                     self.requested, self.available)
