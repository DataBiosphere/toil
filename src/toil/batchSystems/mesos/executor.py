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
from future import standard_library
standard_library.install_aliases()
from builtins import str
import os
import random
import socket
import signal
import sys
import threading
import logging
import psutil
import traceback
import time

import addict
from pymesos import MesosExecutorDriver, Executor, decode_data

from toil import subprocess, pickle
from toil.lib.expando import Expando
from toil.batchSystems.abstractBatchSystem import BatchSystemSupport
from toil.resource import Resource

log = logging.getLogger(__name__)


class MesosExecutor(Executor):
    """
    Part of Toil's Mesos framework, runs on a Mesos agent. A Toil job is passed to it via the
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

    def registered(self, driver, executorInfo, frameworkInfo, agentInfo):
        """
        Invoked once the executor driver has been able to successfully connect with Mesos.
        """
        log.debug("Registered with framework")
        self.address = socket.gethostbyname(agentInfo.hostname)
        nodeInfoThread = threading.Thread(target=self._sendFrameworkMessage, args=[driver])
        nodeInfoThread.daemon = True
        nodeInfoThread.start()

    def reregistered(self, driver, agentInfo):
        """
        Invoked when the executor re-registers with a restarted agent.
        """
        log.debug("Re-registered")

    def disconnected(self, driver):
        """
        Invoked when the executor becomes "disconnected" from the agent (e.g., the agent is being
        restarted due to an upgrade).
        """
        log.critical("Disconnected from agent")

    def killTask(self, driver, taskId):
        """
        Kill parent task process and all its spawned children
        """
        try:
            pid = self.runningTasks[taskId]
            pgid = os.getpgid(pid)
        except KeyError:
            pass
        else:
            os.killpg(pgid, signal.SIGKILL)

    def shutdown(self, driver):
        log.critical('Shutting down executor ...')
        for taskId in list(self.runningTasks.keys()):
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
            time.sleep(random.randint(45, 75))

    def launchTask(self, driver, task):
        """
        Invoked by SchedulerDriver when a Mesos task should be launched by this executor
        """
        def runTask():

            log.debug("Running task %s", task.task_id.value)
            startTime = time.time()
            sendUpdate(task, 'TASK_RUNNING', wallTime=0)

            # try to unpickle the task
            try:
                taskData = pickle.loads(decode_data(task.data))
            except:
                exc_info = sys.exc_info()
                log.error('Exception while unpickling task: ', exc_info=exc_info)
                exc_type, exc_value, exc_trace = exc_info
                sendUpdate(task, 'TASK_FAILED', wallTime=0, msg=''.join(traceback.format_exception_only(exc_type, exc_value)))
                return

            # This is where task.data is first invoked. Using this position to setup cleanupInfo
            if self.workerCleanupInfo is not None:
                assert self.workerCleanupInfo == taskData.workerCleanupInfo
            else:
                self.workerCleanupInfo = taskData.workerCleanupInfo

            # try to invoke a run on the unpickled task
            try:
                process = runJob(taskData)
                self.runningTasks[task.task_id.value] = process.pid
                try:
                    exitStatus = process.wait()
                    wallTime = time.time() - startTime
                    if 0 == exitStatus:
                        sendUpdate(task, 'TASK_FINISHED', wallTime)
                    elif -9 == exitStatus:
                        sendUpdate(task, 'TASK_KILLED', wallTime)
                    else:
                        sendUpdate(task, 'TASK_FAILED', wallTime, msg=str(exitStatus))
                finally:
                    del self.runningTasks[task.task_id.value]
            except:
                wallTime = time.time() - startTime
                exc_info = sys.exc_info()
                log.error('Exception while running task:', exc_info=exc_info)
                exc_type, exc_value, exc_trace = exc_info
                sendUpdate(task, 'TASK_FAILED', wallTime=wallTime, msg=''.join(traceback.format_exception_only(exc_type, exc_value)))

            wallTime = time.time() - startTime
            sendUpdate(task, 'TASK_FINISHED', wallTime)


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
                                        preexec_fn=lambda: os.setpgrp(),
                                        shell=True, env=dict(os.environ, **job.environment))

        def sendUpdate(task, taskState, wallTime=None, msg=''):
            update = addict.Dict()
            update.task_id.value = task.task_id.value
            update.state = taskState
            update.timestamp = wallTime
            update.message = msg
            driver.sendStatusUpdate(update)

        thread = threading.Thread(target=runTask)
        thread.start()

    def frameworkMessage(self, driver, message):
        """
        Invoked when a framework message has arrived for this executor.
        """
        log.debug("Received message from framework: {}".format(message))


def main():
    logging.basicConfig(level=logging.DEBUG)
    log.debug("Starting executor")
    executor = MesosExecutor()
    driver = MesosExecutorDriver(executor, use_addict=True)
    driver.start()
    driver_result = driver.join()
    
    # Tolerate a None in addition to the code the docs suggest we should receive from join()
    exit_value = 0 if (driver_result is None or driver_result == 'DRIVER_STOPPED') else 1
    assert len(executor.runningTasks) == 0
    sys.exit(exit_value)

