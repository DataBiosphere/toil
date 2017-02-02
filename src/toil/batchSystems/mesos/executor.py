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
import random
import socket
import sys
import threading
import pickle
import logging
import subprocess
import traceback
from time import sleep, time

import psutil
import mesos.interface
from bd2k.util.expando import Expando
from mesos.interface import mesos_pb2
import mesos.native
from struct import pack
from toil.batchSystems.abstractBatchSystem import BatchSystemSupport
from toil.resource import Resource

log = logging.getLogger(__name__)


class MesosExecutor(mesos.interface.Executor):
    """
    Part of Toil's Mesos framework, runs on a Mesos slave. A Toil job is passed to it via the
    task.data field, and launched via call(toil.command).
    """

    def __init__(self):
        super(MesosExecutor, self).__init__()
        self.popenLock = threading.Lock()
        self.runningTasks = {}
        self.workerCleanupInfo = None
        Resource.prepareSystem()
        self.address = None
        # Setting this value at this point will ensure that the toil workflow directory will go to
        # the mesos sandbox if the user hasn't specified --workDir on the command line.
        if not os.getenv('TOIL_WORKDIR'):
            os.environ['TOIL_WORKDIR'] = os.getcwd()

    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        """
        Invoked once the executor driver has been able to successfully connect with Mesos.
        """
        log.debug("Registered with framework")
        self.address = socket.gethostbyname(slaveInfo.hostname)
        nodeInfoThread = threading.Thread(target=self._sendFrameworkMessage, args=[driver])
        nodeInfoThread.daemon = True
        nodeInfoThread.start()

    def reregistered(self, driver, slaveInfo):
        """
        Invoked when the executor re-registers with a restarted slave.
        """
        log.debug("Re-registered")

    def disconnected(self, driver):
        """
        Invoked when the executor becomes "disconnected" from the slave (e.g., the slave is being
        restarted due to an upgrade).
        """
        log.critical("Disconnected from slave")

    def killTask(self, driver, taskId):
        try:
            pid = self.runningTasks[taskId]
        except KeyError:
            pass
        else:
            os.kill(pid, 9)

    def shutdown(self, driver):
        log.critical('Shutting down executor ...')
        for taskId in self.runningTasks.keys():
            self.killTask(driver, taskId)
        Resource.cleanSystem()
        BatchSystemSupport.workerCleanup(self.workerCleanupInfo)
        log.critical('... executor shut down.')

    def error(self, driver, message):
        """
        Invoked when a fatal error has occurred with the executor and/or executor driver.
        """
        log.critical("FATAL ERROR: " + message)

    def _sendFrameworkMessage(self, driver):
        message = None
        while True:
            # The psutil documentation recommends that we ignore the value returned by the first
            # invocation of cpu_percent(). However, we do want to send a sign of life early after
            # starting (e.g. to unblock the provisioner waiting for an instance to come up) so
            # the first message we send omits the load info.
            if message is None:
                message = Expando(address=self.address)
                psutil.cpu_percent()
            else:
                message.nodeInfo = dict(coresUsed=float(psutil.cpu_percent()) * .01,
                                        memoryUsed=float(psutil.virtual_memory().percent) * .01,
                                        coresTotal=psutil.cpu_count(),
                                        memoryTotal=psutil.virtual_memory().total,
                                        workers=len(self.runningTasks))
            driver.sendFrameworkMessage(repr(message))
            # Prevent workers launched together from repeatedly hitting the leader at the same time
            sleep(random.randint(45, 75))

    def launchTask(self, driver, task):
        """
        Invoked by SchedulerDriver when a Mesos task should be launched by this executor
        """

        def runTask():
            log.debug("Running task %s", task.task_id.value)
            sendUpdate(mesos_pb2.TASK_RUNNING)
            # This is where task.data is first invoked. Using this position to setup cleanupInfo
            taskData = pickle.loads(task.data)
            if self.workerCleanupInfo is not None:
                assert self.workerCleanupInfo == taskData.workerCleanupInfo
            else:
                self.workerCleanupInfo = taskData.workerCleanupInfo
            startTime = time()
            try:
                popen = runJob(taskData)
                self.runningTasks[task.task_id.value] = popen.pid
                try:
                    exitStatus = popen.wait()
                    wallTime = time() - startTime
                    if 0 == exitStatus:
                        sendUpdate(mesos_pb2.TASK_FINISHED, wallTime)
                    elif -9 == exitStatus:
                        sendUpdate(mesos_pb2.TASK_KILLED, wallTime)
                    else:
                        sendUpdate(mesos_pb2.TASK_FAILED, wallTime, message=str(exitStatus))
                finally:
                    del self.runningTasks[task.task_id.value]
            except:
                wallTime = time() - startTime
                exc_info = sys.exc_info()
                log.error('Exception while running task:', exc_info=exc_info)
                exc_type, exc_value, exc_trace = exc_info
                sendUpdate(mesos_pb2.TASK_FAILED, wallTime,
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

        def sendUpdate(taskState, wallTime=None, message=''):
            log.debug('Sending task status update ...')
            status = mesos_pb2.TaskStatus()
            status.task_id.value = task.task_id.value
            status.message = message
            status.state = taskState
            if wallTime is not None:
                status.data = pack('d', wallTime)
            driver.sendStatusUpdate(status)
            log.debug('... done sending task status update.')

        thread = threading.Thread(target=runTask)
        thread.start()

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
