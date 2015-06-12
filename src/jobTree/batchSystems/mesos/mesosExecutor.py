#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import threading
import pickle
import logging
import subprocess
if False:
    import psutil

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

log = logging.getLogger( __name__ )
lock = threading.Lock()
runningTasks={}

class JobTreeMesosExecutor(mesos.interface.Executor):
    """Part of mesos framework, runs on mesos slave. A jobTree job is passed to it via the task.data field,
     and launched via call(jobTree.command). Uses the ExecutorDriver to communicate.
    """

    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        """
        Invoked once the executor driver has been able to successfully connect with Mesos.
        :param driver:
        :param executorInfo:
        :param frameworkInfo:
        :param slaveInfo:
        :return:
        """
        log.debug("Registered with framework")
        if False:
            statThread = threading.Thread(target=self._sendStats, args=driver)
            statThread.setDaemon(True)
            statThread.start()

    def reregistered(self, driver, slaveInfo):
        """
        Invoked when the executor re-registers with a restarted slave.
        :param driver:
        :param slaveInfo:
        :return:
        """
        log.debug("Re-Registered")

    def disconnected(self, driver):
        """
        Invoked when the executor becomes "disconnected" from the slave (e.g., the slave is being restarted due to an upgrade).
        :param driver:
        :return:
        """
        print "disconnected from slave"

    def killTask(self, driver, taskId):
        if taskId in runningTasks:
            os.kill(runningTasks[taskId], 9)


    def error(self, driver, message):
        """
        Invoked when a fatal error has occurred with the executor and/or executor driver.
        :param driver:
        :param message:
        :return:
        """
        log.warn(message)
        driver.sendFrameworkMessage(message)

    def _sendStats(self, driver):
        while True:
            cpuUsage = str(psutil.cpu_percent())
            ramUsage = str(psutil.virtual_memory().percent)
            driver.sendFrameworkMessage("cpu percent: %s, ram usage: %s", cpuUsage, ramUsage)

    def _callCommand(self, command, taskID):
        log.debug("Invoking command: {}".format(command))
        with lock:
            popen = subprocess.Popen(command,shell=True)
            runningTasks[taskID]=popen.pid
        return popen.wait()

    def launchTask(self, driver, task):
        """
        Invoked by SchedulerDriver when a task has been launched on this executor
        :param driver:
        :param task:
        :return:
        """
        def _run_task():
            log.debug("Running task %s" % task.task_id.value)
            self._sendUpdate(driver, task, mesos_pb2.TASK_RUNNING)

            jobTreeJob = pickle.loads( task.data )
            os.chdir( jobTreeJob.cwd )

            result = self._callCommand(jobTreeJob.command,task.task_id.value)

            if result == 0:
                self._sendUpdate(driver, task, mesos_pb2.TASK_FINISHED)
            elif result == -9:
                self._sendUpdate(driver, task, mesos_pb2.TASK_KILLED)
            else:
                self._sendUpdate(driver, task, mesos_pb2.TASK_FAILED)

        # TODO: I think there needs to be a thread.join() somewhere for each thread. Come talk to me about this.
        thread = threading.Thread(target=_run_task)
        thread.start()

    def _sendUpdate(self, driver, task, TASK_STATE):
        log.debug("Sending status update...")
        update = mesos_pb2.TaskStatus()
        update.task_id.value = task.task_id.value
        update.state = TASK_STATE
        driver.sendStatusUpdate(update)
        log.debug("Sent status update")

    def frameworkMessage(self, driver, message):
        """
        Invoked when a framework message has arrived for this executor.
        :param driver:
        :param message:
        :return:
        """
        log.debug("Received message from framework: {}".format(message))


def main( executorClass ):
    logging.basicConfig( level=logging.DEBUG )
    log.debug( "Starting executor" )
    driver = mesos.native.MesosExecutorDriver( executorClass( ) )
    sys.exit( 0 if driver.run( ) == mesos_pb2.DRIVER_STOPPED else 1 )


if __name__ == "__main__":
    main( JobTreeMesosExecutor )
