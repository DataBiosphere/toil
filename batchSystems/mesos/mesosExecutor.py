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
from time import sleep

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
from subprocess import call
import pickle


class JobTreeMesosExecutor(mesos.interface.Executor):
    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        print "Registered with framework"

    def reregistered(self, driver, slaveInfo):
        print "Re-Registered"

    def disconnected(self, driver):
        print "disconnected from slave"

    def error(self, driver, message):
        print message
        self.frameworkMessage(driver, message)

    def launchTask(self, driver, task):
        # Create a thread to run the task. Tasks should always be run in new
        # threads or processes, rather than inside launchTask itself.
        def run_task():
            print "Running task %s" % task.task_id.value
            self.sendUpdate(driver, task, mesos_pb2.TASK_RUNNING)

            jobTreeJob = pickle.loads( task.data )
            os.chdir( jobTreeJob.cwd )

            result = call(jobTreeJob.command, shell=True)

            if result != 0:
                self.sendUpdate(driver, task, mesos_pb2.TASK_FAILED)
            else:
                self.sendUpdate(driver, task, mesos_pb2.TASK_FINISHED)

        thread = threading.Thread(target=run_task)
        thread.start()

    def sendUpdate(self, driver, task, TASK_STATE):
        print "Sending status update..."
        update = mesos_pb2.TaskStatus()
        update.task_id.value = task.task_id.value
        update.state = TASK_STATE
        update.data = 'data with a \0 byte'
        driver.sendStatusUpdate(update)
        print "Sent status update"

    def frameworkMessage(self, driver, message):
        # Send it back to the scheduler.
        driver.sendFrameworkMessage(message)

if __name__ == "__main__":
    print "Starting executor"
    driver = mesos.native.MesosExecutorDriver(JobTreeMesosExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
