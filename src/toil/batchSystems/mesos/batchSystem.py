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

import ast
import logging
import os
import pickle
import pwd
import socket
import time
from Queue import Queue, Empty
from collections import defaultdict
from struct import unpack

import itertools
import mesos.interface
import mesos.native
from bd2k.util.expando import Expando
from mesos.interface import mesos_pb2

from toil import resolveEntryPoint
from toil.batchSystems.abstractBatchSystem import (AbstractScalableBatchSystem,
                                                   BatchSystemSupport,
                                                   NodeInfo)
from toil.batchSystems.mesos import ToilJob, ResourceRequirement, TaskData

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

    def __init__(self, config, maxCores, maxMemory, maxDisk, masterAddress,
                 userScript=None, toilDistribution=None):
        super(MesosBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)

        # The hot-deployed resources representing the user script and the Toil distribution
        # respectively. Will be passed along in every Mesos task. Also see toil.resource.Resource
        self.userScript = userScript
        self.toilDistribution = toilDistribution

        # Dictionary of queues, which toil assigns jobs to. Each queue represents a job type,
        # defined by resource usage
        self.jobQueues = defaultdict(list)

        # Address of the Mesos master in the form host:port where host can be an IP or a hostname
        self.masterAddress = masterAddress

        # Written to when Mesos kills tasks, as directed by Toil
        self.killedJobIds = set()

        # The IDs of job to be killed
        self.killJobIds = set()

        # Contains jobs on which killBatchJobs were called, regardless of whether or not they
        # actually were killed or ended by themselves
        self.intendedKill = set()

        # Dict of launched jobIDs to TaskData objects
        self.runningJobMap = {}

        # Queue of jobs whose status has been updated, according to Mesos
        self.updatedJobsQueue = Queue()

        # The Mesos driver used by this scheduler
        self.driver = None

        # A dictionary mapping a node's IP to an Expando object describing important properties
        # of our executor running on that node. Only an approximation of the truth.
        self.executors = {}

        # A set of Mesos slave IDs, one for each slave running on a non-preemptable node. Only an
        #  approximation of the truth. Recently launched nodes may be absent from this set for a
        # while and a node's absence from this set does not imply its preemptability. But it is
        # generally safer to assume a node is preemptable since non-preemptability is a stronger
        # requirement. If we tracked the set of preemptable nodes instead, we'd have to use
        # absence as an indicator of non-preemptability and could therefore be misled into
        # believeing that a recently launched preemptable node was non-preemptable.
        self.nonPreemptibleNodes = set()

        self.executor = self._buildExecutor()

        self.unusedJobID = itertools.count()
        self.lastReconciliation = time.time()
        self.reconciliationPeriod = 120
        self._startDriver()

    def issueBatchJob(self, command, memory, cores, disk, preemptable):
        """
        Issues the following command returning a unique jobID. Command is the string to run, memory
        is an int giving the number of bytes the job needs to run in and cores is the number of cpus
        needed for the job and error-file is the path of the file to place any std-err/std-out in.
        """
        self.checkResourceRequest(memory, cores, disk)
        jobID = next(self.unusedJobID)
        job = ToilJob(jobID=jobID,
                      resources=ResourceRequirement(memory=memory, cores=cores, disk=disk),
                      command=command,
                      userScript=self.userScript,
                      toilDistribution=self.toilDistribution,
                      environment=self.environment.copy(),
                      workerCleanupInfo=self.workerCleanupInfo)
        jobType = job.resources
        log.debug("Queueing the job command: %s with job id: %s ...", command, str(jobID))
        self.jobQueues[jobType].append(job)
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
        jobIds = set()
        for queue in self.jobQueues.values():
            for job in queue:
                jobIds.add(job.jobID)
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
                                                        True) # enable implicit acknowledgements
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
        log.info("Stopping Mesos driver")
        self.driver.stop()
        log.info("Joining Mesos driver")
        driver_result = self.driver.join()
        log.info("Joined Mesos driver")
        if driver_result != mesos_pb2.DRIVER_STOPPED:
            raise RuntimeError("Mesos driver failed with %i", driver_result)

    def registered(self, driver, frameworkId, masterInfo):
        """
        Invoked when the scheduler successfully registers with a Mesos master
        """
        log.debug("Registered with framework ID %s", frameworkId.value)

    def _sortJobsByResourceReq(self):
        jobTypes = self.jobQueues.keys()
        jobTypes.sort(key=ResourceRequirement.size)
        jobTypes.reverse()
        return jobTypes

    def _declineAllOffers(self, driver, offers):
        for offer in offers:
            log.debug("Declining offer %s.", offer.id.value)
            driver.declineOffer(offer.id)

    def _determineOfferResources(self, offer):
        cores = 0
        memory = 0
        disk = 0
        for resource in offer.resources:
            if resource.name == "cpus":
                cores += resource.scalar.value
            elif resource.name == "mem":
                memory += resource.scalar.value
            elif resource.name == "disk":
                disk += resource.scalar.value
        return cores, memory, disk

    def _prepareToRun(self, jobType, offer, index):
        # Get the first element to insure FIFO
        job = self.jobQueues[jobType][index]
        task = self._newMesosTask(job, offer)
        return task

    def _deleteByJobID(self, jobID, ):
        # FIXME: Surely there must be a more efficient way to do this
        for jobType in self.jobQueues.values():
            for job in jobType:
                if jobID == job.jobID:
                    jobType.remove(job)

    def _updateStateToRunning(self, offer, task):
        self.runningJobMap[int(task.task_id.value)] = TaskData(startTime=time.time(),
                                                               slaveID=offer.slave_id,
                                                               executorID=task.executor.executor_id)
        self._deleteByJobID(int(task.task_id.value))

    def resourceOffers(self, driver, offers):
        """
        Invoked when resources have been offered to this framework.
        """
        jobTypes = self._sortJobsByResourceReq()

        self._updatePreemptability(offers)

        # TODO: We may want to assert that numIssued >= numRunning
        if not jobTypes or len(self.getIssuedBatchJobIDs()) == len(self.getRunningBatchJobIDs()):
            log.debug('There are no queued tasks. Declining Mesos offers.')
            # Without jobs, we can get stuck with no jobs and no new offers until we decline it.
            self._declineAllOffers(driver, offers)
            return

        # Right now, gives priority to largest jobs
        for offer in offers:
            runnableTasks = []
            # TODO: In an offer, can there ever be more than one resource with the same name?
            offerCores, offerMemory, offerDisk = self._determineOfferResources(offer)
            log.debug('Received offer %s with %i MiB memory, %i core(s) and %i MiB of disk.',
                      offer.id.value, offerMemory, offerCores, offerDisk)
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
                while (len(self.jobQueues[jobType]) - nextToLaunchIndex > 0
                       and remainingCores >= jobType.cores
                       and remainingDisk >= toMiB(jobType.disk)
                       and remainingMemory >= toMiB(jobType.memory)):
                    task = self._prepareToRun(jobType, offer, nextToLaunchIndex)
                    # TODO: this used to be a conditional but Hannes wanted it changed to an assert
                    # TODO: ... so we can understand why it exists.
                    assert int(task.task_id.value) not in self.runningJobMap
                    runnableTasksOfType.append(task)
                    log.info("Preparing to launch Mesos task %s using offer %s ...",
                             task.task_id.value, offer.id.value)
                    remainingCores -= jobType.cores
                    remainingMemory -= toMiB(jobType.memory)
                    remainingDisk -= jobType.disk
                    nextToLaunchIndex += 1
                if self.jobQueues[jobType] and not runnableTasksOfType:
                    log.debug('Offer %(offer)s not suitable to run the tasks with requirements '
                              '%(requirements)r. Mesos offered %(memory)s memory, %(cores)i cores '
                              'and %(disk)s of disk.', dict(offer=offer.id.value,
                                                            requirements=jobType,
                                                            memory=fromMiB(offerMemory),
                                                            cores=offerCores,
                                                            disk=fromMiB(offerDisk)))
                runnableTasks.extend(runnableTasksOfType)
            # Launch all runnable tasks together so we only call launchTasks once per offer
            if runnableTasks:
                driver.launchTasks(offer.id, runnableTasks)
                for task in runnableTasks:
                    self._updateStateToRunning(offer, task)
                    log.info('Launched Mesos task %s.', task.task_id.value)
            else:
                log.info('Although there are queued jobs, none of them could be run with the' +
                         'offers extended to the framework. Enable debug logging to see details '
                         'about tasks queued and offers made.')
                driver.declineOffer(offer.id)

    def _updatePreemptability(self, offers):
        for offer in offers:
            preemptable = False
            for attribute in offer.attributes:
                if attribute.name == 'preemptable':
                    preemptable = bool(attribute.scalar.value)
            if preemptable:
                try:
                    self.nonPreemptibleNodes.remove(offer.slave_id.value)
                except KeyError:
                    pass
            else:
                self.nonPreemptibleNodes.add(offer.slave_id.value)

    def _newMesosTask(self, job, offer):
        """
        Build the Mesos task object for a given the Toil job and Mesos offer
        """
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(job.jobID)
        task.slave_id.value = offer.slave_id.value
        task.name = "task %d" % job.jobID
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
            try:
                del self.runningJobMap[jobID]
            except KeyError:
                log.warning("Job %i returned exit code %i but isn't tracked as running.",
                            jobID, _exitStatus)

        if update.state == mesos_pb2.TASK_FINISHED:
            jobEnded(0, wallTime=unpack('d', update.data))
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
        message = ast.literal_eval(message)
        assert isinstance(message, dict)
        # Handle the mandatory fields of a message
        nodeAddress = message.pop('address')
        executor = self.executors.get(nodeAddress)
        if executor is None or executor.slaveId != slaveId:
            executor = Expando(nodeAddress=nodeAddress, slaveId=slaveId, nodeInfo=None)
            self.executors[nodeAddress] = executor
        executor.lastSeen = time.time()
        # Handle optional message fields
        for k, v in message.iteritems():
            if k == 'nodeInfo':
                assert isinstance(v, dict)
                executor.nodeInfo = NodeInfo(**v)
            else:
                raise RuntimeError("Unknown message field '%s'." % k)

    def getNodes(self, preemptable=False):
        return {nodeAddress: executor.nodeInfo
                for nodeAddress, executor in self.executors
                if time.time() - executor.lastSeen < 600
                and preemptable == (executor.slaveId not in self.nonPreemptibleNodes)}

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
