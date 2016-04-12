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
import os
import sys
import threading
import pickle
import logging
import subprocess
import traceback
import json
import requests
from time import sleep

import psutil
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem, WorkerCleanupInfo
from toil.resource import Resource
from fcntl import flock, LOCK_EX

log = logging.getLogger(__name__)


class MesosExecutor(mesos.interface.Executor):
    """
    Part of Mesos framework, runs on Mesos slave. A Toil job is passed to it via the task.data
    field, and launched via call(toil.command). Uses the ExecutorDriver to communicate.
    """
    def __init__(self):
        super(MesosExecutor, self).__init__()
        self.popenLock = threading.Lock()
        self.runningTasks = {}
        self.workerCleanupInfo = None
        # Required for dynamic disk offers
        self.deadSpace = None
        self.masterAddress = None
        self.mesosCredentials = None

        Resource.prepareSystem()
        # FIXME: clean up resource root dir

    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        """
        Invoked once the executor driver has been able to successfully connect with Mesos.
        """
        log.info("Registered with framework")
        statThread = threading.Thread(target=self._sendStats, args=[driver])
        statThread.setDaemon(True)
        statThread.start()

    def reregistered(self, driver, slaveInfo):
        """
        Invoked when the executor re-registers with a restarted slave.
        """
        log.info("Re-registered")

    def disconnected(self, driver):
        """
        Invoked when the executor becomes "disconnected" from the slave (e.g., the slave is being
        restarted due to an upgrade).
        """
        log.critical("Disconnected from slave")

    def killTask(self, driver, taskId):
        if taskId in self.runningTasks:
            os.kill(self.runningTasks[taskId], 9)

    def shutdown(self, driver):
        log.critical("Shutting down executor...")
        for taskId, pid in self.runningTasks.items():
            self.killTask(driver, taskId)
        Resource.cleanSystem()
        AbstractBatchSystem.workerCleanup(self.workerCleanupInfo)
        log.critical("Executor shut down")

    def error(self, driver, message):
        """
        Invoked when a fatal error has occurred with the executor and/or executor driver.
        """
        log.critical("FATAL ERROR: " + message)

    def _sendStats(self, driver):
        while True:
            coresUsage = str(psutil.cpu_percent())
            ramUsage = str(psutil.virtual_memory().percent)
            driver.sendFrameworkMessage("CPU usage: %s, memory usage: %s" % (coresUsage, ramUsage))
            log.debug("Sent stats message")
            sleep(30)

    def launchTask(self, driver, task):
        """
        Invoked by SchedulerDriver when a Mesos task should be launched by this executor
        """

        def runTask():
            log.debug("Running task %s", task.task_id.value)
            sendUpdate(mesos_pb2.TASK_RUNNING)
            taskData = pickle.loads(task.data)
            if self.workerCleanupInfo is not None:
                assert self.workerCleanupInfo == taskData.workerCleanupInfo
                assert self.masterAddress == taskData.masterAddress
                assert self.mesosCredentials == taskData.mesosCredentials
            else:
                # This is where task.data is invoked for the first time on this executor. Using this
                # position to setup workerCleanupInfo, masterAddress, mesosCredentials, and
                # deadSpace for this executor.
                self.workerCleanupInfo = taskData.workerCleanupInfo
                self.masterAddress = taskData.masterAddress
                self.mesosCredentials = taskData.mesosCredentials
                # Now that we have a master address, we can find the dead space on the node
                # Dead space is defined as the space on the disk unavailable to Mesos. By default,
                # this includes all the used space, and 50% of the free disk space, or 5GB
                # (whichever is less) when the slave was set up. When we calculate the used disk for
                # reservations, we need to factor this in.
                slaveInfo = self._getSlaveInfo(task.slave_id.value)
                diskSize = AbstractBatchSystem.getFileSystemSize(**self.workerCleanupInfo._asdict())
                diskSize = round(diskSize / 1024 / 1024.0)
                if round(slaveInfo['resources']['disk']) < 2560:  # (0.5 * 5GB)
                    initialFreeSize = round(2 * slaveInfo['resources']['disk'])
                else:
                    initialFreeSize = round(slaveInfo['resources']['disk']) + 5120
                self.deadSpace = diskSize - initialFreeSize
            # Set the environment variable TOIL_USABLE_DISK
            slaveInfo = self._getSlaveInfo(task.slave_id.value)
            os.environ['TOIL_USABLE_DISK'] = str(slaveInfo['unreserved_resources']['disk'])
            try:
                popen = runJob(taskData)
                self.runningTasks[task.task_id.value] = popen.pid
                exitStatus = None
                try:
                    exitStatus = popen.wait()
                finally:
                    # Update the reservations based on whatever the job left behind.  Use a file
                    # lock to overcome race conditions between concurrent processes on a worker.
                    with open(self.mesosCredentials, 'r+') as lockFile:
                        flock(lockFile, LOCK_EX)
                        slaveInfo = self._getSlaveInfo(task.slave_id.value)
                        usedDisk = AbstractBatchSystem.probeUsedDisk(
                                        **self.workerCleanupInfo._asdict())
                        usedDisk = round(usedDisk / 1024 / 1024.0) - self.deadSpace
                        # If there have been no previous reservations, reserve the whole amount
                        if not slaveInfo['reserved_resources']:
                            self._updateSlaveReservations(task.slave_id.value, delta=usedDisk)
                        elif usedDisk != slaveInfo['reserved_resources']['toil_leftover']['disk']:
                            # Else if the used disk has gone up or down, reserve or unreserve the
                            # delta respectively
                            delta = usedDisk - slaveInfo['reserved_resources']['toil_leftover'
                                                                                ]['disk']
                            self._updateSlaveReservations(task.slave_id.value, delta=delta)
                        else:
                            pass  # No Change
                    if exitStatus is not None:
                        if 0 == exitStatus:
                            sendUpdate(mesos_pb2.TASK_FINISHED)
                        elif -9 == exitStatus:
                            sendUpdate(mesos_pb2.TASK_KILLED)
                        else:
                            sendUpdate(mesos_pb2.TASK_FAILED, message=str(exitStatus))
                    del self.runningTasks[task.task_id.value]
            except:
                exc_info = sys.exc_info()
                log.error('Exception while running task:', exc_info=exc_info)
                exc_type, exc_value, exc_trace = exc_info
                sendUpdate(mesos_pb2.TASK_FAILED,
                           message=''.join(traceback.format_exception_only(exc_type, exc_value)))

        def runJob(job):
            """
            :type job: toil.batchSystems.mesos.ToilJob

            :rtype: subprocess.Popen
            """
            if job.userScript:
                job.userScript.register()
            log.debug("Invoking command: '%s'", job.command)
            with self.popenLock:
                return subprocess.Popen(job.command,
                                        shell=True, env=dict(os.environ, **job.environment))

        def sendUpdate(taskState, message=''):
            log.debug("Sending status update ...")
            status = mesos_pb2.TaskStatus()
            status.task_id.value = task.task_id.value
            status.message = message
            status.state = taskState
            driver.sendStatusUpdate(status)
            log.debug("Sent status update")

        thread = threading.Thread(target=runTask)
        thread.start()

    def _getSlaveInfo(self, slaveId):
        """
        Return a dict containing the information Mesos provides about slave with ID == slaveId

        :param slaveId: Id of the slave of interest
        """
        # This doesn't need authentication
        slaveInfos = requests.get('http://' + self.masterAddress + '/master/slaves').json()['slaves']
        return next(slaveInfo for slaveInfo in slaveInfos if slaveInfo['id'] == slaveId)

    def _updateSlaveReservations(self, slaveID, delta):
        """
        Updates the reservation of toil-disk on slave slaveID by delta. if delta is positive, make
        a reserve request and if it is negative, make an unreserve request.

        :param str slaveID: Id of the slave. required for reserve and unreserve requests
        :param int delta: The value to reserve/unreserve (positive/negative).
        :return: None
        """
        endpoint = 'unreserve' if delta < 0 else 'reserve'
        delta = abs(delta)
        url = 'http://' + self.masterAddress + '/master/' + endpoint
        with open(self.mesosCredentials, 'r') as cF:
            username, password = cF.readline().strip().split()
        resourceRequest = {'slaveId': slaveID,
                           'resources': json.dumps([{"name": "disk",
                                                     "type": "SCALAR",
                                                     "scalar": { "value": delta},
                                                     "role": "toil_leftover",
                                                     "reservation": {"principal": "toil"}}])}
        log.debug("Sending a %s request to the Mesos master." % endpoint)
        content = requests.post(url, auth=(username, password), data=resourceRequest)
        if content.status_code == 200:
            log.debug("Successfully '%sd' an additional (%s)mb on slave (%s)" % (endpoint, delta,
                                                                                 slaveID))
        else:
            log.warn("Failed to '%s' an additional (%s)mb on slave (%s)" % (endpoint, delta,
                                                                            slaveID))

    def frameworkMessage(self, driver, message):
        """
        Invoked when a framework message has arrived for this executor.
        """
        log.debug("Received message from framework: {}".format(message))


def main(executorClass=MesosExecutor):
    logging.basicConfig(level=logging.DEBUG)
    log.debug("Starting executor")
    executor = executorClass()
    driver = mesos.native.MesosExecutorDriver(executor)
    exit_value = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
    assert len(executor.runningTasks) == 0
    sys.exit(exit_value)
