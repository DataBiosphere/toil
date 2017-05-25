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

import ast
import logging
import os
import pickle
import pwd
import socket
import time
import sys
from contextlib import contextmanager
from struct import unpack

import itertools

# Python 3 compatibility imports
from six.moves.queue import Empty, Queue
from six import iteritems, itervalues

import mesos.interface
import mesos.native
from bd2k.util import strict_bool
from mesos.interface import mesos_pb2

from toil import resolveEntryPoint
from toil.batchSystems.abstractBatchSystem import (AbstractScalableBatchSystem,
                                                   BatchSystemSupport,
                                                   NodeInfo)
from toil.batchSystems.mesos import ToilJob, ResourceRequirement, TaskData, JobQueue

log = logging.getLogger(__name__)


class MesosBatchSystem(BatchSystemSupport,
                       AbstractScalableBatchSystem,
                       mesos.interface.Scheduler):
    """
    A Toil batch system implementation that uses Apache Mesos to distribute toil jobs as Mesos
    tasks over a cluster of slave nodes. A Mesos framework consists of a scheduler and an
    executor. This class acts as the scheduler and is typically run on the master node that also
    runs the Mesos master process with which the scheduler communicates via a driver component.
    The executor is implemented in a separate class. It is run on each slave node and
    communicates with the Mesos slave process via another driver object. The scheduler may also
    be run on a separate node from the master, which we then call somewhat ambiguously the driver
    node.
    """

    @classmethod
    def supportsHotDeployment(cls):
        return True

    @classmethod
    def supportsWorkerCleanup(cls):
        return True

    class ExecutorInfo(object):
        def __init__(self, nodeAddress, slaveId, nodeInfo, lastSeen):
            super(MesosBatchSystem.ExecutorInfo, self).__init__()
            self.nodeAddress = nodeAddress
            self.slaveId = slaveId
            self.nodeInfo = nodeInfo
            self.lastSeen = lastSeen

    def __init__(self, config, maxCores, maxMemory, maxDisk, masterAddress):
        super(MesosBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)

        # The hot-deployed resource representing the user script. Will be passed along in every
        # Mesos task. Also see setUserScript().
        self.userScript = None
        """
        :type: toil.resource.Resource
        """

        # Dictionary of queues, which toil assigns jobs to. Each queue represents a job type,
        # defined by resource usage
        self.jobQueues = JobQueue()

        # Address of the Mesos master in the form host:port where host can be an IP or a hostname
        self.masterAddress = masterAddress

        # Written to when Mesos kills tasks, as directed by Toil
        self.killedJobIds = set()

        # The IDs of job to be killed
        self.killJobIds = set()

        # Contains jobs on which killBatchJobs were called, regardless of whether or not they
        # actually were killed or ended by themselves
        self.intendedKill = set()

        # Map of host address to job ids
        # this is somewhat redundant since Mesos returns the number of workers per
        # node. However, that information isn't guaranteed to reach the leader,
        # so we also track the state here. When the information is returned from
        # mesos, prefer that information over this attempt at state tracking.
        self.hostToJobIDs = {}

        # see self.setNodeFilter
        self.nodeFilter = []

        # Dict of launched jobIDs to TaskData objects
        self.runningJobMap = {}

        # Mesos has no easy way of getting a task's resources so we track them here
        self.taskResources = {}

        # Queue of jobs whose status has been updated, according to Mesos
        self.updatedJobsQueue = Queue()

        # The Mesos driver used by this scheduler
        self.driver = None

        # A dictionary mapping a node's IP to an ExecutorInfo object describing important
        # properties of our executor running on that node. Only an approximation of the truth.
        self.executors = {}

        # A set of Mesos slave IDs, one for each slave running on a non-preemptable node. Only an
        #  approximation of the truth. Recently launched nodes may be absent from this set for a
        # while and a node's absence from this set does not imply its preemptability. But it is
        # generally safer to assume a node is preemptable since non-preemptability is a stronger
        # requirement. If we tracked the set of preemptable nodes instead, we'd have to use
        # absence as an indicator of non-preemptability and could therefore be misled into
        # believeing that a recently launched preemptable node was non-preemptable.
        self.nonPreemptableNodes = set()

        self.executor = self._buildExecutor()

        self.unusedJobID = itertools.count()
        self.lastReconciliation = time.time()
        self.reconciliationPeriod = 120

        # These control how frequently to log a message that would indicate if no jobs are
        # currently able to run on the offers given. This can happen if the cluster is busy
        # or if the nodes in the cluster simply don't have enough resources to run the jobs
        self.lastTimeOfferLogged = 0
        self.logPeriod = 30  # seconds

        self._startDriver()

    def setUserScript(self, userScript):
        self.userScript = userScript

    def issueBatchJob(self, jobNode):
        """
        Issues the following command returning a unique jobID. Command is the string to run, memory
        is an int giving the number of bytes the job needs to run in and cores is the number of cpus
        needed for the job and error-file is the path of the file to place any std-err/std-out in.
        """
        self.checkResourceRequest(jobNode.memory, jobNode.cores, jobNode.disk)
        jobID = next(self.unusedJobID)
        job = ToilJob(jobID=jobID,
                      name=str(jobNode),
                      resources=ResourceRequirement(**jobNode._requirements),
                      command=jobNode.command,
                      userScript=self.userScript,
                      environment=self.environment.copy(),
                      workerCleanupInfo=self.workerCleanupInfo)
        jobType = job.resources
        log.debug("Queueing the job command: %s with job id: %s ...", jobNode.command, str(jobID))

        # TODO: round all elements of resources

        self.jobQueues.insertJob(job, jobType)
        self.taskResources[jobID] = job.resources
        log.debug("... queued")
        return jobID

    def killBatchJobs(self, jobIDs):
        # FIXME: probably still racy
        assert self.driver is not None
        localSet = set()
        for jobID in jobIDs:
            self.killJobIds.add(jobID)
            localSet.add(jobID)
            self.intendedKill.add(jobID)
            # FIXME: a bit too expensive for my taste
            if jobID in self.getIssuedBatchJobIDs():
                taskId = mesos_pb2.TaskID()
                taskId.value = str(jobID)
                self.driver.killTask(taskId)
            else:
                self.killJobIds.remove(jobID)
                localSet.remove(jobID)
        while localSet:
            intersection = localSet.intersection(self.killedJobIds)
            if intersection:
                localSet -= intersection
                self.killedJobIds -= intersection
            else:
                time.sleep(1)

    def getIssuedBatchJobIDs(self):
        jobIds = set(self.jobQueues.jobIDs())
        jobIds.update(self.runningJobMap.keys())
        return list(jobIds)

    def getRunningBatchJobIDs(self):
        currentTime = dict()
        for jobID, data in self.runningJobMap.items():
            currentTime[jobID] = time.time() - data.startTime
        return currentTime

    def getUpdatedBatchJob(self, maxWait):
        while True:
            try:
                item = self.updatedJobsQueue.get(timeout=maxWait)
            except Empty:
                return None
            jobId, exitValue, wallTime = item
            try:
                self.intendedKill.remove(jobId)
            except KeyError:
                log.debug('Job %s ended with status %i, took %s seconds.', jobId, exitValue,
                          '???' if wallTime is None else str(wallTime))
                return item
            else:
                log.debug('Job %s ended naturally before it could be killed.', jobId)

    def nodeInUse(self, nodeIP):
        return nodeIP in self.hostToJobIDs

    @contextmanager
    def nodeFiltering(self, filter):
        self.nodeFilter = [filter]
        yield
        self.nodeFilter = []

    def getWaitDuration(self):
        """
        Gets the period of time to wait (floating point, in seconds) between checking for
        missing/overlong jobs.
        """
        return self.reconciliationPeriod

    @classmethod
    def getRescueBatchJobFrequency(cls):
        return 30 * 60  # Half an hour

    def _buildExecutor(self):
        """
        Creates and returns an ExecutorInfo instance representing our executor implementation.
        """
        # The executor program is installed as a setuptools entry point by setup.py
        info = mesos_pb2.ExecutorInfo()
        info.name = "toil"
        info.command.value = resolveEntryPoint('_toil_mesos_executor')
        info.executor_id.value = "toil-%i" % os.getpid()
        info.source = pwd.getpwuid(os.getuid()).pw_name
        return info

    def _startDriver(self):
        """
        The Mesos driver thread which handles the scheduler's communication with the Mesos master
        """
        framework = mesos_pb2.FrameworkInfo()
        framework.user = ""  # Have Mesos fill in the current user.
        framework.name = "toil"
        framework.principal = framework.name
        self.driver = mesos.native.MesosSchedulerDriver(self,
                                                        framework,
                                                        self._resolveAddress(self.masterAddress),
                                                        True)  # enable implicit acknowledgements
        assert self.driver.start() == mesos_pb2.DRIVER_RUNNING

    @staticmethod
    def _resolveAddress(address):
        """
        Resolves the host in the given string. The input is of the form host[:port]. This method
        is idempotent, i.e. the host may already be a dotted IP address.

        >>> # noinspection PyProtectedMember
        >>> f=MesosBatchSystem._resolveAddress
        >>> f('localhost')
        '127.0.0.1'
        >>> f('127.0.0.1')
        '127.0.0.1'
        >>> f('localhost:123')
        '127.0.0.1:123'
        >>> f('127.0.0.1:123')
        '127.0.0.1:123'
        """
        address = address.split(':')
        assert len(address) in (1, 2)
        address[0] = socket.gethostbyname(address[0])
        return ':'.join(address)

    def shutdown(self):
        log.debug("Stopping Mesos driver")
        self.driver.stop()
        log.debug("Joining Mesos driver")
        driver_result = self.driver.join()
        log.debug("Joined Mesos driver")
        if driver_result != mesos_pb2.DRIVER_STOPPED:
            raise RuntimeError("Mesos driver failed with %i", driver_result)

    def registered(self, driver, frameworkId, masterInfo):
        """
        Invoked when the scheduler successfully registers with a Mesos master
        """
        log.debug("Registered with framework ID %s", frameworkId.value)

    def _declineAllOffers(self, driver, offers):
        for offer in offers:
            log.debug("Declining offer %s.", offer.id.value)
            driver.declineOffer(offer.id)

    def _parseOffer(self, offer):
        cores = 0
        memory = 0
        disk = 0
        preemptable = None
        for attribute in offer.attributes:
            if attribute.name == 'preemptable':
                assert preemptable is None, "Attribute 'preemptable' occurs more than once."
                preemptable = strict_bool(attribute.text.value)
        if preemptable is None:
            log.debug('Slave not marked as either preemptable or not. Assuming non-preemptable.')
            preemptable = False
        for resource in offer.resources:
            if resource.name == "cpus":
                cores += resource.scalar.value
            elif resource.name == "mem":
                memory += resource.scalar.value
            elif resource.name == "disk":
                disk += resource.scalar.value
        return cores, memory, disk, preemptable

    def _prepareToRun(self, jobType, offer):
        # Get the first element to insure FIFO
        job = self.jobQueues.nextJobOfType(jobType)
        task = self._newMesosTask(job, offer)
        return task

    def _updateStateToRunning(self, offer, runnableTasks):
        for task in runnableTasks:
            resourceKey = int(task.task_id.value)
            resources = self.taskResources[resourceKey]
            slaveIP = socket.gethostbyname(offer.hostname)
            try:
                self.hostToJobIDs[slaveIP].append(resourceKey)
            except KeyError:
                self.hostToJobIDs[slaveIP] = [resourceKey]

            self.runningJobMap[int(task.task_id.value)] = TaskData(startTime=time.time(),
                                                                   slaveID=offer.slave_id.value,
                                                                   slaveIP=slaveIP,
                                                                   executorID=task.executor.executor_id.value,
                                                                   cores=resources.cores,
                                                                   memory=resources.memory)
            del self.taskResources[resourceKey]
            log.debug('Launched Mesos task %s.', task.task_id.value)

    def resourceOffers(self, driver, offers):
        """
        Invoked when resources have been offered to this framework.
        """
        self._trackOfferedNodes(offers)

        jobTypes = self.jobQueues.sorted()

        # TODO: We may want to assert that numIssued >= numRunning
        if not jobTypes or len(self.getIssuedBatchJobIDs()) == len(self.getRunningBatchJobIDs()):
            log.debug('There are no queued tasks. Declining Mesos offers.')
            # Without jobs, we can get stuck with no jobs and no new offers until we decline it.
            self._declineAllOffers(driver, offers)
            return

        unableToRun = True
        # Right now, gives priority to largest jobs
        for offer in offers:
            runnableTasks = []
            # TODO: In an offer, can there ever be more than one resource with the same name?
            offerCores, offerMemory, offerDisk, offerPreemptable = self._parseOffer(offer)
            log.debug('Got offer %s for a %spreemptable slave with %.2f MiB memory, %.2f core(s) '
                      'and %.2f MiB of disk.', offer.id.value, '' if offerPreemptable else 'non-',
                      offerMemory, offerCores, offerDisk)
            remainingCores = offerCores
            remainingMemory = offerMemory
            remainingDisk = offerDisk

            for jobType in jobTypes:
                runnableTasksOfType = []
                # Because we are not removing from the list until outside of the while loop, we
                # must decrement the number of jobs left to run ourselves to avoid an infinite
                # loop.
                nextToLaunchIndex = 0
                # Toil specifies disk and memory in bytes but Mesos uses MiB
                while ( not self.jobQueues.typeEmpty(jobType)
                       # On a non-preemptable node we can run any job, on a preemptable node we
                       # can only run preemptable jobs:
                       and (not offerPreemptable or jobType.preemptable)
                       and remainingCores >= jobType.cores
                       and remainingDisk >= toMiB(jobType.disk)
                       and remainingMemory >= toMiB(jobType.memory)):
                    task = self._prepareToRun(jobType, offer)
                    # TODO: this used to be a conditional but Hannes wanted it changed to an assert
                    # TODO: ... so we can understand why it exists.
                    assert int(task.task_id.value) not in self.runningJobMap
                    runnableTasksOfType.append(task)
                    log.debug("Preparing to launch Mesos task %s using offer %s ...",
                              task.task_id.value, offer.id.value)
                    remainingCores -= jobType.cores
                    remainingMemory -= toMiB(jobType.memory)
                    remainingDisk -= toMiB(jobType.disk)
                    nextToLaunchIndex += 1
                else:
                    log.debug('Offer %(offer)s not suitable to run the tasks with requirements '
                              '%(requirements)r. Mesos offered %(memory)s memory, %(cores)s cores '
                              'and %(disk)s of disk on a %(non)spreemptable slave.',
                              dict(offer=offer.id.value,
                                   requirements=jobType,
                                   non='' if offerPreemptable else 'non-',
                                   memory=fromMiB(offerMemory),
                                   cores=offerCores,
                                   disk=fromMiB(offerDisk)))
                runnableTasks.extend(runnableTasksOfType)
            # Launch all runnable tasks together so we only call launchTasks once per offer
            if runnableTasks:
                unableToRun = False
                driver.launchTasks(offer.id, runnableTasks)
                self._updateStateToRunning(offer, runnableTasks)
            else:
                log.debug('Although there are queued jobs, none of them could be run with offer %s '
                          'extended to the framework.', offer.id)
                driver.declineOffer(offer.id)

        if unableToRun and time.time() > (self.lastTimeOfferLogged + self.logPeriod):
            self.lastTimeOfferLogged = time.time()
            log.debug('Although there are queued jobs, none of them were able to run in '
                     'any of the offers extended to the framework. There are currently '
                     '%i jobs running. Enable debug level logging to see more details about '
                     'job types and offers received.', len(self.runningJobMap))

    def _trackOfferedNodes(self, offers):
        for offer in offers:
            nodeAddress = socket.gethostbyname(offer.hostname)
            self._registerNode(nodeAddress, offer.slave_id.value)
            preemptable = False
            for attribute in offer.attributes:
                if attribute.name == 'preemptable':
                    preemptable = strict_bool(attribute.text.value)
            if preemptable:
                try:
                    self.nonPreemptableNodes.remove(offer.slave_id.value)
                except KeyError:
                    pass
            else:
                self.nonPreemptableNodes.add(offer.slave_id.value)

    def _filterOfferedNodes(self, offers):
        if not self.nodeFilter:
            return offers
        executorInfoOrNone = [self.executors.get(socket.gethostbyname(offer.hostname)) for offer in offers]
        executorInfos = filter(None, executorInfoOrNone)
        executorsToConsider = filter(self.nodeFilter[0], executorInfos)
        ipsToConsider = {ex.nodeAddress for ex in executorsToConsider}
        return [offer for offer in offers if socket.gethostbyname(offer.hostname) in ipsToConsider]

    def _newMesosTask(self, job, offer):
        """
        Build the Mesos task object for a given the Toil job and Mesos offer
        """
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(job.jobID)
        task.slave_id.value = offer.slave_id.value
        task.name = job.name
        task.data = pickle.dumps(job)
        task.executor.MergeFrom(self.executor)

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = job.resources.cores

        disk = task.resources.add()
        disk.name = "disk"
        disk.type = mesos_pb2.Value.SCALAR
        if toMiB(job.resources.disk) > 1:
            disk.scalar.value = toMiB(job.resources.disk)
        else:
            log.warning("Job %s uses less disk than Mesos requires. Rounding %s up to 1 MiB.",
                        job.jobID, job.resources.disk)
            disk.scalar.value = 1
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        if toMiB(job.resources.memory) > 1:
            mem.scalar.value = toMiB(job.resources.memory)
        else:
            log.warning("Job %s uses less memory than Mesos requires. Rounding %s up to 1 MiB.",
                        job.jobID, job.resources.memory)
            mem.scalar.value = 1
        return task

    def statusUpdate(self, driver, update):
        """
        Invoked when the status of a task has changed (e.g., a slave is lost and so the task is
        lost, a task finishes and an executor sends a status update saying so, etc). Note that
        returning from this callback _acknowledges_ receipt of this status update! If for
        whatever reason the scheduler aborts during this callback (or the process exits) another
        status update will be delivered (note, however, that this is currently not true if the
        slave sending the status update is lost/fails during that time).
        """
        jobID = int(update.task_id.value)
        stateName = mesos_pb2.TaskState.Name(update.state)
        log.debug("Job %i is in state '%s'.", jobID, stateName)

        def jobEnded(_exitStatus, wallTime=None):
            try:
                self.killJobIds.remove(jobID)
            except KeyError:
                pass
            else:
                self.killedJobIds.add(jobID)
            self.updatedJobsQueue.put((jobID, _exitStatus, wallTime))
            slaveIP = None
            try:
                slaveIP = self.runningJobMap[jobID].slaveIP
            except KeyError:
                log.warning("Job %i returned exit code %i but isn't tracked as running.",
                            jobID, _exitStatus)
            else:
                del self.runningJobMap[jobID]

            try:
                self.hostToJobIDs[slaveIP].remove(jobID)
            except KeyError:
                log.warning("Job %i returned exit code %i from unknown host.",
                            jobID, _exitStatus)

        if update.state == mesos_pb2.TASK_FINISHED:
            jobEnded(0, wallTime=unpack('d', update.data)[0])
        elif update.state == mesos_pb2.TASK_FAILED:
            try:
                exitStatus = int(update.message)
            except ValueError:
                exitStatus = 255
                log.warning("Job %i failed with message '%s'", jobID, update.message)
            else:
                log.warning('Job %i failed with exit status %i', jobID, exitStatus)
            jobEnded(exitStatus)
        elif update.state in (mesos_pb2.TASK_LOST, mesos_pb2.TASK_KILLED, mesos_pb2.TASK_ERROR):
            log.warning("Job %i is in unexpected state %s with message '%s'.",
                        jobID, stateName, update.message)
            jobEnded(255)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        """
        Invoked when an executor sends a message.
        """
        log.debug('Got framework message from executor %s running on slave %s: %s',
                  executorId.value, slaveId.value, message)
        message = ast.literal_eval(message)
        assert isinstance(message, dict)
        # Handle the mandatory fields of a message
        nodeAddress = message.pop('address')
        executor = self._registerNode(nodeAddress, slaveId.value)
        # Handle optional message fields
        for k, v in iteritems(message):
            if k == 'nodeInfo':
                assert isinstance(v, dict)
                resources = [taskData for taskData in itervalues(self.runningJobMap)
                             if taskData.executorID == executorId.value]
                requestedCores = sum(taskData.cores for taskData in resources)
                requestedMemory = sum(taskData.memory for taskData in resources)
                executor.nodeInfo = NodeInfo(requestedCores=requestedCores, requestedMemory=requestedMemory, **v)
                self.executors[nodeAddress] = executor
            else:
                raise RuntimeError("Unknown message field '%s'." % k)

    def _registerNode(self, nodeAddress, slaveId):
        executor = self.executors.get(nodeAddress)
        if executor is None or executor.slaveId != slaveId:
            executor = self.ExecutorInfo(nodeAddress=nodeAddress,
                                         slaveId=slaveId,
                                         nodeInfo=None,
                                         lastSeen=time.time())
            self.executors[nodeAddress] = executor
        else:
            executor.lastSeen = time.time()
        return executor

    def getNodes(self, preemptable=None, timeout=600):
        timeout = timeout or sys.maxint
        return {nodeAddress: executor.nodeInfo
                for nodeAddress, executor in iteritems(self.executors)
                if time.time() - executor.lastSeen < timeout
                and (preemptable is None
                     or preemptable == (executor.slaveId not in self.nonPreemptableNodes))}

    def reregistered(self, driver, masterInfo):
        """
        Invoked when the scheduler re-registers with a newly elected Mesos master.
        """
        log.debug('Registered with new master')

    def executorLost(self, driver, executorId, slaveId, status):
        """
        Invoked when an executor has exited/terminated.
        """
        log.warning("Executor '%s' lost.", executorId)


def toMiB(n):
    return n / 1024 / 1024


def fromMiB(n):
    return n * 1024 * 1024
