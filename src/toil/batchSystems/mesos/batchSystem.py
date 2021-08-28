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
import ast
import getpass
import json
import logging
import os
import pickle
import pwd
import socket
import sys
import time
import traceback
from contextlib import contextmanager
from queue import Empty, Queue
from typing import Optional, Dict
from urllib.parse import quote_plus
from urllib.request import urlopen

import addict
from pymesos import MesosSchedulerDriver, Scheduler, decode_data, encode_data

from toil import resolveEntryPoint
from toil.batchSystems.abstractBatchSystem import (EXIT_STATUS_UNAVAILABLE_VALUE,
                                                   AbstractScalableBatchSystem,
                                                   BatchJobExitReason,
                                                   BatchSystemLocalSupport,
                                                   NodeInfo,
                                                   UpdatedBatchJobInfo)
from toil.batchSystems.mesos import JobQueue, MesosShape, TaskData, ToilJob
from toil.job import JobDescription
from toil.lib.memoize import strict_bool

log = logging.getLogger(__name__)


class MesosBatchSystem(BatchSystemLocalSupport,
                       AbstractScalableBatchSystem,
                       Scheduler):
    """
    A Toil batch system implementation that uses Apache Mesos to distribute toil jobs as Mesos
    tasks over a cluster of agent nodes. A Mesos framework consists of a scheduler and an
    executor. This class acts as the scheduler and is typically run on the master node that also
    runs the Mesos master process with which the scheduler communicates via a driver component.
    The executor is implemented in a separate class. It is run on each agent node and
    communicates with the Mesos agent process via another driver object. The scheduler may also
    be run on a separate node from the master, which we then call somewhat ambiguously the driver
    node.
    """

    @classmethod
    def supportsAutoDeployment(cls):
        return True

    @classmethod
    def supportsWorkerCleanup(cls):
        return True

    class ExecutorInfo(object):
        def __init__(self, nodeAddress, agentId, nodeInfo, lastSeen):
            super(MesosBatchSystem.ExecutorInfo, self).__init__()
            self.nodeAddress = nodeAddress
            self.agentId = agentId
            self.nodeInfo = nodeInfo
            self.lastSeen = lastSeen

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(MesosBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)

        # The auto-deployed resource representing the user script. Will be passed along in every
        # Mesos task. Also see setUserScript().
        self.userScript = None
        """
        :type: toil.resource.Resource
        """

        # Dictionary of queues, which toil assigns jobs to. Each queue represents a job type,
        # defined by resource usage
        self.jobQueues = JobQueue()

        # Address of the Mesos master in the form host:port where host can be an IP or a hostname
        self.mesosMasterAddress = config.mesosMasterAddress

        # Written to when Mesos kills tasks, as directed by Toil.
        # Jobs must not enter this set until they are removed from runningJobMap.
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

        # The string framework ID that we are assigned when registering with the Mesos master
        self.frameworkId = None

        # A dictionary mapping a node's IP to an ExecutorInfo object describing important
        # properties of our executor running on that node. Only an approximation of the truth.
        self.executors = {}

        # A dictionary mapping back from agent ID to the last observed IP address of its node.
        self.agentsByID = {}

        # A set of Mesos agent IDs, one for each agent running on a
        # non-preemptable node. Only an approximation of the truth. Recently
        # launched nodes may be absent from this set for a while and a node's
        # absence from this set does not imply its preemptability. But it is
        # generally safer to assume a node is preemptable since
        # non-preemptability is a stronger requirement. If we tracked the set
        # of preemptable nodes instead, we'd have to use absence as an
        # indicator of non-preemptability and could therefore be misled into
        # believing that a recently launched preemptable node was
        # non-preemptable.
        self.nonPreemptableNodes = set()

        self.executor = self._buildExecutor()

        # These control how frequently to log a message that would indicate if no jobs are
        # currently able to run on the offers given. This can happen if the cluster is busy
        # or if the nodes in the cluster simply don't have enough resources to run the jobs
        self.lastTimeOfferLogged = 0
        self.logPeriod = 30  # seconds

        self.ignoredNodes = set()

        self._startDriver()

    def setUserScript(self, userScript):
        self.userScript = userScript

    def ignoreNode(self, nodeAddress):
        self.ignoredNodes.add(nodeAddress)

    def unignoreNode(self, nodeAddress):
        self.ignoredNodes.remove(nodeAddress)

    def issueBatchJob(self, jobNode: JobDescription, job_environment: Optional[Dict[str, str]] = None):
        """
        Issues the following command returning a unique jobID. Command is the string to run, memory
        is an int giving the number of bytes the job needs to run in and cores is the number of cpus
        needed for the job and error-file is the path of the file to place any std-err/std-out in.
        """
        localID = self.handleLocalJob(jobNode)
        if localID:
            return localID

        mesos_resources = {
            "memory": jobNode.memory,
            "cores": jobNode.cores,
            "disk": jobNode.disk,
            "preemptable": jobNode.preemptable
        }
        self.checkResourceRequest(
            memory=mesos_resources["memory"],
            cores=mesos_resources["cores"],
            disk=mesos_resources["disk"]
        )

        jobID = self.getNextJobID()
        environment = self.environment.copy()
        if job_environment:
            environment.update(job_environment)

        job = ToilJob(jobID=jobID,
                      name=str(jobNode),
                      resources=MesosShape(wallTime=0, **mesos_resources),
                      command=jobNode.command,
                      userScript=self.userScript,
                      environment=environment,
                      workerCleanupInfo=self.workerCleanupInfo)
        jobType = job.resources
        log.debug("Queueing the job command: %s with job id: %s ...", jobNode.command, str(jobID))

        # TODO: round all elements of resources

        self.taskResources[jobID] = job.resources
        self.jobQueues.insertJob(job, jobType)
        log.debug("... queued")
        return jobID

    def killBatchJobs(self, jobIDs):

        # Some jobs may be local. Kill them first.
        self.killLocalJobs(jobIDs)

        # The driver thread does the actual work of killing the remote jobs.
        # We have to give it instructions, and block until the jobs are killed.
        assert self.driver is not None

        # This is the set of jobs that this invocation has asked to be killed,
        # but which haven't been killed yet.
        localSet = set()

        for jobID in jobIDs:
            # Queue the job up to be killed
            self.killJobIds.add(jobID)
            localSet.add(jobID)
            # Record that we meant to kill it, in case it finishes up by itself.
            self.intendedKill.add(jobID)

            if jobID in self.getIssuedBatchJobIDs():
                # Since the job has been issued, we have to kill it
                taskId = addict.Dict()
                taskId.value = str(jobID)
                log.debug("Kill issued job %s" % str(jobID))
                self.driver.killTask(taskId)
            else:
                # This job was never issued. Maybe it is a local job.
                # We don't have to kill it.
                log.debug("Skip non-issued job %s" % str(jobID))
                self.killJobIds.remove(jobID)
                localSet.remove(jobID)
        # Now localSet just has the non-local/issued jobs that we asked to kill
        while localSet:
            # Wait until they are all dead
            intersection = localSet.intersection(self.killedJobIds)
            if intersection:
                localSet -= intersection
                # When jobs are killed that we asked for, clear them out of
                # killedJobIds where the other thread put them
                self.killedJobIds -= intersection
            else:
                time.sleep(1)
        # Now all the jobs we asked to kill are dead. We know they are no
        # longer running, because that update happens before their IDs go into
        # killedJobIds. So we can safely return.

    def getIssuedBatchJobIDs(self):
        jobIds = set(self.jobQueues.jobIDs())
        jobIds.update(list(self.runningJobMap.keys()))
        return list(jobIds) + list(self.getIssuedLocalJobIDs())

    def getRunningBatchJobIDs(self):
        currentTime = dict()
        for jobID, data in list(self.runningJobMap.items()):
            currentTime[jobID] = time.time() - data.startTime
        currentTime.update(self.getRunningLocalJobIDs())
        return currentTime

    def getUpdatedBatchJob(self, maxWait):
        local_tuple = self.getUpdatedLocalJob(0)
        if local_tuple:
            return local_tuple
        while True:
            try:
                item = self.updatedJobsQueue.get(timeout=maxWait)
            except Empty:
                return None
            try:
                self.intendedKill.remove(item.jobID)
            except KeyError:
                log.debug('Job %s ended with status %i, took %s seconds.', item.jobID, item.exitStatus,
                          '???' if item.wallTime is None else str(item.wallTime))
                return item
            else:
                log.debug('Job %s ended naturally before it could be killed.', item.jobID)

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
        return 1

    def _buildExecutor(self):
        """
        Creates and returns an ExecutorInfo-shaped object representing our executor implementation.
        """
        # The executor program is installed as a setuptools entry point by setup.py
        info = addict.Dict()
        info.name = "toil"
        info.command.value = resolveEntryPoint('_toil_mesos_executor')
        info.executor_id.value = "toil-%i" % os.getpid()
        info.source = pwd.getpwuid(os.getuid()).pw_name
        return info

    def _startDriver(self):
        """
        The Mesos driver thread which handles the scheduler's communication with the Mesos master
        """
        framework = addict.Dict()
        framework.user = getpass.getuser()  # We must determine the user name ourselves with pymesos
        framework.name = "toil"
        framework.principal = framework.name
        # Make the driver which implements most of the scheduler logic and calls back to us for the user-defined parts.
        # Make sure it will call us with nice namespace-y addicts
        self.driver = MesosSchedulerDriver(self, framework,
                                           self._resolveAddress(self.mesosMasterAddress),
                                           use_addict=True, implicit_acknowledgements=True)
        self.driver.start()

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
        self.shutdownLocal()
        log.debug("Stopping Mesos driver")
        self.driver.stop()
        log.debug("Joining Mesos driver")
        driver_result = self.driver.join()
        log.debug("Joined Mesos driver")
        if driver_result is not None and driver_result != 'DRIVER_STOPPED':
            # TODO: The docs say join should return a code, but it keeps returning
            # None when apparently successful. So tolerate that here too.
            raise RuntimeError("Mesos driver failed with %s" % driver_result)

    def registered(self, driver, frameworkId, masterInfo):
        """
        Invoked when the scheduler successfully registers with a Mesos master
        """
        log.debug("Registered with framework ID %s", frameworkId.value)
        # Save the framework ID
        self.frameworkId = frameworkId.value

    def _declineAllOffers(self, driver, offers):
        for offer in offers:
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
            log.debug('Agent not marked as either preemptable or not. Assuming non-preemptable.')
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
        # Get the first element to ensure FIFO
        job = self.jobQueues.nextJobOfType(jobType)
        task = self._newMesosTask(job, offer)
        return task

    def _updateStateToRunning(self, offer, runnableTasks):
        for task in runnableTasks:
            resourceKey = int(task.task_id.value)
            resources = self.taskResources[resourceKey]
            agentIP = socket.gethostbyname(offer.hostname)
            try:
                self.hostToJobIDs[agentIP].append(resourceKey)
            except KeyError:
                self.hostToJobIDs[agentIP] = [resourceKey]

            self.runningJobMap[int(task.task_id.value)] = TaskData(startTime=time.time(),
                                                                   agentID=offer.agent_id.value,
                                                                   agentIP=agentIP,
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

        jobTypes = self.jobQueues.sortedTypes

        if not jobTypes:
            # Without jobs, we can get stuck with no jobs and no new offers until we decline it.
            self._declineAllOffers(driver, offers)
            return

        unableToRun = True
        # Right now, gives priority to largest jobs
        for offer in offers:
            if offer.hostname in self.ignoredNodes:
                driver.declineOffer(offer.id)
                continue
            runnableTasks = []
            # TODO: In an offer, can there ever be more than one resource with the same name?
            offerCores, offerMemory, offerDisk, offerPreemptable = self._parseOffer(offer)
            log.debug('Got offer %s for a %spreemptable agent with %.2f MiB memory, %.2f core(s) '
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
                    log.debug("Preparing to launch Mesos task %s with %.2f cores, %.2f MiB memory, and %.2f MiB disk using offer %s ...",
                              task.task_id.value, jobType.cores, toMiB(jobType.memory), toMiB(jobType.disk), offer.id.value)
                    remainingCores -= jobType.cores
                    remainingMemory -= toMiB(jobType.memory)
                    remainingDisk -= toMiB(jobType.disk)
                    nextToLaunchIndex += 1
                if not self.jobQueues.typeEmpty(jobType):
                    # report that remaining jobs cannot be run with the current resourcesq:
                    log.debug('Offer %(offer)s not suitable to run the tasks with requirements '
                              '%(requirements)r. Mesos offered %(memory)s memory, %(cores)s cores '
                              'and %(disk)s of disk on a %(non)spreemptable agent.',
                              dict(offer=offer.id.value,
                                   requirements=jobType.__dict__,
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
            # All AgentID messages are required to have a value according to the Mesos Protobuf file.
            assert 'value' in offer.agent_id
            try:
                nodeAddress = socket.gethostbyname(offer.hostname)
            except:
                log.debug("Failed to resolve hostname %s" % offer.hostname)
                raise
            self._registerNode(nodeAddress, offer.agent_id.value)
            preemptable = False
            for attribute in offer.attributes:
                if attribute.name == 'preemptable':
                    preemptable = strict_bool(attribute.text.value)
            if preemptable:
                try:
                    self.nonPreemptableNodes.remove(offer.agent_id.value)
                except KeyError:
                    pass
            else:
                self.nonPreemptableNodes.add(offer.agent_id.value)

    def _filterOfferedNodes(self, offers):
        if not self.nodeFilter:
            return offers
        executorInfoOrNone = [self.executors.get(socket.gethostbyname(offer.hostname)) for offer in offers]
        executorInfos = [_f for _f in executorInfoOrNone if _f]
        executorsToConsider = list(filter(self.nodeFilter[0], executorInfos))
        ipsToConsider = {ex.nodeAddress for ex in executorsToConsider}
        return [offer for offer in offers if socket.gethostbyname(offer.hostname) in ipsToConsider]

    def _newMesosTask(self, job, offer):
        """
        Build the Mesos task object for a given the Toil job and Mesos offer
        """
        task = addict.Dict()
        task.task_id.value = str(job.jobID)
        task.agent_id.value = offer.agent_id.value
        task.name = job.name
        task.data = encode_data(pickle.dumps(job))
        task.executor = addict.Dict(self.executor)

        task.resources = []

        task.resources.append(addict.Dict())
        cpus = task.resources[-1]
        cpus.name = 'cpus'
        cpus.type = 'SCALAR'
        cpus.scalar.value = job.resources.cores

        task.resources.append(addict.Dict())
        disk = task.resources[-1]
        disk.name = 'disk'
        disk.type = 'SCALAR'
        if toMiB(job.resources.disk) > 1:
            disk.scalar.value = toMiB(job.resources.disk)
        else:
            log.warning("Job %s uses less disk than Mesos requires. Rounding %s up to 1 MiB.",
                        job.jobID, job.resources.disk)
            disk.scalar.value = 1

        task.resources.append(addict.Dict())
        mem = task.resources[-1]
        mem.name = 'mem'
        mem.type = 'SCALAR'
        if toMiB(job.resources.memory) > 1:
            mem.scalar.value = toMiB(job.resources.memory)
        else:
            log.warning("Job %s uses less memory than Mesos requires. Rounding %s up to 1 MiB.",
                        job.jobID, job.resources.memory)
            mem.scalar.value = 1
        return task

    def statusUpdate(self, driver, update):
        """
        Invoked when the status of a task has changed (e.g., a agent is lost and so the task is
        lost, a task finishes and an executor sends a status update saying so, etc). Note that
        returning from this callback _acknowledges_ receipt of this status update! If for
        whatever reason the scheduler aborts during this callback (or the process exits) another
        status update will be delivered (note, however, that this is currently not true if the
        agent sending the status update is lost/fails during that time).
        """
        jobID = int(update.task_id.value)
        log.debug("Job %i is in state '%s' due to reason '%s'.", jobID, update.state, update.reason)

        def jobEnded(_exitStatus, wallTime=None, exitReason=None):
            """
            Notify external observers of the job ending.
            """
            self.updatedJobsQueue.put(UpdatedBatchJobInfo(jobID=jobID, exitStatus=_exitStatus, wallTime=wallTime, exitReason=exitReason))
            agentIP = None
            try:
                agentIP = self.runningJobMap[jobID].agentIP
            except KeyError:
                log.warning("Job %i returned exit code %i but isn't tracked as running.",
                            jobID, _exitStatus)
            else:
                # Mark the job as no longer running. We MUST do this BEFORE
                # saying we killed the job, or it will be possible for another
                # thread to kill a job and then see it as running.
                del self.runningJobMap[jobID]

            try:
                self.hostToJobIDs[agentIP].remove(jobID)
            except KeyError:
                log.warning("Job %i returned exit code %i from unknown host.",
                            jobID, _exitStatus)

            try:
                self.killJobIds.remove(jobID)
            except KeyError:
                pass
            else:
                # We were asked to kill this job, so say that we have done so.
                # We do this LAST, after all status updates for the job have
                # been handled, to ensure a consistent view of the scheduler
                # state from other threads.
                self.killedJobIds.add(jobID)

        if update.state == 'TASK_FINISHED':
            # We get the running time of the job via the timestamp, which is in job-local time in seconds
            labels = update.labels.labels
            wallTime = None
            for label in labels:
                if label['key'] == 'wallTime':
                    wallTime = float(label['value'])
                    break
            assert(wallTime is not None)
            jobEnded(0, wallTime=wallTime, exitReason=BatchJobExitReason.FINISHED)
        elif update.state == 'TASK_FAILED':
            try:
                exitStatus = int(update.message)
            except ValueError:
                exitStatus = EXIT_STATUS_UNAVAILABLE_VALUE
                log.warning("Job %i failed with message '%s' due to reason '%s' on executor '%s' on agent '%s'.",
                            jobID, update.message, update.reason,
                            update.executor_id, update.agent_id)
            else:
                log.warning("Job %i failed with exit status %i and message '%s' due to reason '%s' on executor '%s' on agent '%s'.",
                            jobID, exitStatus,
                            update.message, update.reason,
                            update.executor_id, update.agent_id)

            jobEnded(exitStatus, exitReason=BatchJobExitReason.FAILED)
        elif update.state == 'TASK_LOST':
            log.warning("Job %i is lost.", jobID)
            jobEnded(EXIT_STATUS_UNAVAILABLE_VALUE, exitReason=BatchJobExitReason.LOST)
        elif update.state in ('TASK_KILLED', 'TASK_ERROR'):
            log.warning("Job %i is in unexpected state %s with message '%s' due to reason '%s'.",
                        jobID, update.state, update.message, update.reason)
            jobEnded(EXIT_STATUS_UNAVAILABLE_VALUE,
                     exitReason=(BatchJobExitReason.KILLED if update.state == 'TASK_KILLED' else BatchJobExitReason.ERROR))

        if 'limitation' in update:
            log.warning("Job limit info: %s" % update.limitation)

    def frameworkMessage(self, driver, executorId, agentId, message):
        """
        Invoked when an executor sends a message.
        """

        # Take it out of base 64 encoding from Protobuf
        message = decode_data(message).decode()

        log.debug('Got framework message from executor %s running on agent %s: %s',
                  executorId.value, agentId.value, message)
        message = ast.literal_eval(message)
        assert isinstance(message, dict)
        # Handle the mandatory fields of a message
        nodeAddress = message.pop('address')
        executor = self._registerNode(nodeAddress, agentId.value)
        # Handle optional message fields
        for k, v in message.items():
            if k == 'nodeInfo':
                assert isinstance(v, dict)
                resources = [taskData for taskData in self.runningJobMap.values()
                             if taskData.executorID == executorId.value]
                requestedCores = sum(taskData.cores for taskData in resources)
                requestedMemory = sum(taskData.memory for taskData in resources)
                executor.nodeInfo = NodeInfo(requestedCores=requestedCores, requestedMemory=requestedMemory, **v)
                self.executors[nodeAddress] = executor
            else:
                raise RuntimeError("Unknown message field '%s'." % k)

    def _registerNode(self, nodeAddress, agentId, nodePort=5051):
        """
        Called when we get communication from an agent. Remembers the
        information about the agent by address, and the agent address by agent
        ID.
        """
        executor = self.executors.get(nodeAddress)
        if executor is None or executor.agentId != agentId:
            executor = self.ExecutorInfo(nodeAddress=nodeAddress,
                                         agentId=agentId,
                                         nodeInfo=None,
                                         lastSeen=time.time())
            self.executors[nodeAddress] = executor
        else:
            executor.lastSeen = time.time()

        # Record the IP under the agent id
        self.agentsByID[agentId] = nodeAddress

        return executor

    def getNodes(self, preemptable=None, timeout=600):
        timeout = timeout or sys.maxsize
        return {nodeAddress: executor.nodeInfo
                for nodeAddress, executor in self.executors.items()
                if time.time() - executor.lastSeen < timeout
                and (preemptable is None
                     or preemptable == (executor.agentId not in self.nonPreemptableNodes))}

    def reregistered(self, driver, masterInfo):
        """
        Invoked when the scheduler re-registers with a newly elected Mesos master.
        """
        log.debug('Registered with new master')

    def _handleFailedExecutor(self, agentID, executorID=None):
        """
        Should be called when we find out an executor has failed.

        Gets the log from some container (since we are never handed a container
        ID) that ran on the given executor on the given agent, if the agent is
        still up, and dumps it to our log. All IDs are strings.

        If executorID is None, dumps all executors from the agent.

        Useful for debugging failing executor code.
        """

        log.warning("Handling failure of executor '%s' on agent '%s'.",
                    executorID, agentID)

        try:
            # Look up the IP. We should always know it unless we get answers
            # back without having accepted offers.
            agentAddress = self.agentsByID[agentID]

            # For now we assume the agent is always on the same port. We could
            # maybe sniff this from the URL that comes in the offer but it's
            # not guaranteed to be there.
            agentPort = 5051

            # We need the container ID to read the log, but we are never given
            # it, and I can't find a good way to list it, because the API only
            # seems to report running containers. So we dump all the available
            # files with /files/debug and look for one that looks right.
            filesQueryURL = errorLogURL = "http://%s:%d/files/debug" % \
                (agentAddress, agentPort)

            # Download all the root mount points, which are in an object from
            # mounted name to real name
            filesDict = json.loads(urlopen(filesQueryURL).read())

            log.debug('Available files: %s', repr(filesDict.keys()))

            # Generate filenames for each container pointing to where stderr should be
            stderrFilenames = []
            # And look for the actual agent logs.
            agentLogFilenames = []
            for filename in filesDict:
                if (self.frameworkId in filename and agentID in filename and
                    (executorID is None or executorID in filename)):

                    stderrFilenames.append("%s/stderr" % filename)
                elif filename.endswith("log"):
                    agentLogFilenames.append(filename)

            if len(stderrFilenames) == 0:
                log.warning("Could not find any containers in '%s'." % filesDict)

            for stderrFilename in stderrFilenames:
                try:

                    # According to
                    # http://mesos.apache.org/documentation/latest/sandbox/ we can use
                    # the web API to fetch the error log.
                    errorLogURL = "http://%s:%d/files/download?path=%s" % \
                        (agentAddress, agentPort, quote_plus(stderrFilename))

                    log.warning("Attempting to retrieve executor error log: %s", errorLogURL)

                    for line in urlopen(errorLogURL):
                        # Warn all the lines of the executor's error log
                        log.warning("Executor: %s", line.rstrip())

                except Exception as e:
                    log.warning("Could not retrieve exceutor log due to: '%s'.", e)
                    log.warning(traceback.format_exc())

            for agentLogFilename in agentLogFilenames:
                try:
                    agentLogURL = "http://%s:%d/files/download?path=%s" % \
                        (agentAddress, agentPort, quote_plus(agentLogFilename))

                    log.warning("Attempting to retrieve agent log: %s", agentLogURL)

                    for line in urlopen(agentLogURL):
                        # Warn all the lines of the agent's log
                        log.warning("Agent: %s", line.rstrip())
                except Exception as e:
                    log.warning("Could not retrieve agent log due to: '%s'.", e)
                    log.warning(traceback.format_exc())

        except Exception as e:
            log.warning("Could not retrieve logs due to: '%s'.", e)
            log.warning(traceback.format_exc())

    def executorLost(self, driver, executorId, agentId, status):
        """
        Invoked when an executor has exited/terminated abnormally.
        """

        failedId = executorId.get('value', None)

        log.warning("Executor '%s' reported lost with status '%s'.", failedId, status)

        self._handleFailedExecutor(agentId.value, failedId)

    @classmethod
    def setOptions(cls, setOption):
        setOption("mesosMasterAddress", None, None, 'localhost:5050')


def toMiB(n):
    return n / 1024 / 1024


def fromMiB(n):
    return n * 1024 * 1024
