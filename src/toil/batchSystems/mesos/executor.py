#!/usr/bin/env python

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
from time import sleep

import psutil
import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from toil.resource import Resource

log = logging.getLogger(__name__)


class MesosExecutor(mesos.interface.Executor):
    """
    Part of mesos framework, runs on mesos slave. A toil job is passed to it via the task.data field, and launched
    via call(toil.command). Uses the ExecutorDriver to communicate.
    """

    def __init__(self):
        super(MesosExecutor, self).__init__()
        self.popenLock = threading.Lock()
        self.runningTasks = {}
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
        Invoked when the executor becomes "disconnected" from the slave (e.g., the slave is being restarted due to an upgrade).
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
            popen = runJob(pickle.loads(task.data))
            self.runningTasks[task.task_id.value] = popen.pid
            try:
                exitStatus = popen.wait()
                if 0 == exitStatus:
                    sendUpdate(mesos_pb2.TASK_FINISHED)
                elif -9 == exitStatus:
                    sendUpdate(mesos_pb2.TASK_KILLED)
                else:
                    sendUpdate(mesos_pb2.TASK_FAILED)
            except:
                exc_type, exc_value, exc_trace = sys.exc_info()
                sendUpdate(mesos_pb2.TASK_FAILED, message=str(traceback.format_exception_only(exc_type, exc_value)))
            finally:
                del self.runningTasks[task.task_id.value]

        def runJob(job):
            """
            :type job: toil.batchSystems.mesos.ToilJob

            :rtype: subprocess.Popen
            """
            if job.userScript:
                job.userScript.register()
            log.debug("Invoking command: '%s'", job.command)
            with self.popenLock:
                return subprocess.Popen(job.command, shell=True)

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


if __name__ == "__main__":
    main()
