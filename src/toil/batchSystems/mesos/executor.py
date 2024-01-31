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
import json
import logging
import os
import os.path
import pickle
import random
import resource
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from urllib.request import urlopen

import addict
import psutil
from pymesos import Executor, MesosExecutorDriver, decode_data, encode_data

from toil.batchSystems.abstractBatchSystem import BatchSystemSupport
from toil.lib.expando import Expando
from toil.lib.threading import cpu_count
from toil.resource import Resource
from toil.statsAndLogging import configure_root_logger, set_log_level

log = logging.getLogger(__name__)


class MesosExecutor(Executor):
    """
    Part of Toil's Mesos framework, runs on a Mesos agent. A Toil job is passed to it via the
    task.data field, and launched via call(toil.command).
    """

    def __init__(self):
        super().__init__()
        self.popenLock = threading.Lock()
        self.runningTasks = {}
        self.workerCleanupInfo = None
        log.debug('Preparing system for resource download')
        Resource.prepareSystem()
        self.address = None
        self.id = None
        # Setting this value at this point will ensure that the toil workflow directory will go to
        # the mesos sandbox if the user hasn't specified --workDir on the command line.
        if not os.getenv('TOIL_WORKDIR'):
            os.environ['TOIL_WORKDIR'] = os.getcwd()

    def registered(self, driver, executorInfo, frameworkInfo, agentInfo):
        """
        Invoked once the executor driver has been able to successfully connect with Mesos.
        """

        # Get the ID we have been assigned, if we have it
        self.id = executorInfo.executor_id.get('value', None)

        log.debug("Registered executor %s with framework", self.id)
        self.address = socket.gethostbyname(agentInfo.hostname)
        nodeInfoThread = threading.Thread(target=self._sendFrameworkMessage, args=[driver], daemon=True)
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
            pid = self.runningTasks[taskId.value]
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
            # we call it once and discard the value.
            if message is None:
                message = Expando(address=self.address)
                psutil.cpu_percent()
            else:
                message.nodeInfo = dict(coresUsed=float(psutil.cpu_percent()) * .01,
                                        memoryUsed=float(psutil.virtual_memory().percent) * .01,
                                        coresTotal=cpu_count(),
                                        memoryTotal=psutil.virtual_memory().total,
                                        workers=len(self.runningTasks))
            log.debug("Send framework message: %s", message)
            driver.sendFrameworkMessage(encode_data(repr(message).encode('utf-8')))
            # Prevent workers launched together from repeatedly hitting the leader at the same time
            time.sleep(random.randint(45, 75))

    def launchTask(self, driver, task):
        """
        Invoked by SchedulerDriver when a Mesos task should be launched by this executor
        """

        log.debug("Asked to launch task %s", repr(task))

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
            # Construct the job's environment
            jobEnv = dict(os.environ, **job.environment)
            log.debug('Using environment variables: %s', jobEnv.keys())
            with self.popenLock:
                return subprocess.Popen(job.command,
                                        preexec_fn=lambda: os.setpgrp(),
                                        shell=True, env=jobEnv)

        def sendUpdate(task, taskState, wallTime, msg=''):
            update = addict.Dict()
            update.task_id.value = task.task_id.value
            if self.id is not None:
                # Sign our messages as from us, since the driver doesn't do it.
                update.executor_id.value = self.id
            update.state = taskState
            update.message = msg

            # Add wallTime as a label.
            labels = addict.Dict()
            labels.labels = [{'key': 'wallTime', 'value': str(wallTime)}]
            update.labels = labels

            driver.sendStatusUpdate(update)

        thread = threading.Thread(target=runTask, daemon=True)
        thread.start()

    def frameworkMessage(self, driver, message):
        """
        Invoked when a framework message has arrived for this executor.
        """
        log.debug(f"Received message from framework: {message}")


def main():
    configure_root_logger()
    set_log_level("INFO")

    if not os.environ.get("MESOS_AGENT_ENDPOINT"):
        # Some Mesos setups in our tests somehow lack this variable. Provide a
        # fake one to maybe convince the executor driver to work.
        os.environ["MESOS_AGENT_ENDPOINT"] = os.environ.get("MESOS_SLAVE_ENDPOINT", "127.0.0.1:5051")
        log.warning("Had to fake MESOS_AGENT_ENDPOINT as %s" % os.environ["MESOS_AGENT_ENDPOINT"])

    # must be set manually to enable toggling of the mesos log level for debugging jenkins
    # may be useful: https://github.com/DataBiosphere/toil/pull/2338#discussion_r223854931
    if False:
        try:
            urlopen("http://%s/logging/toggle?level=1&duration=15mins" % os.environ["MESOS_AGENT_ENDPOINT"]).read()
            log.debug("Toggled agent log level")
        except Exception:
            log.debug("Failed to toggle agent log level")

    # Parse the agent state
    agent_state = json.loads(urlopen("http://%s/state" % os.environ["MESOS_AGENT_ENDPOINT"]).read())
    if 'completed_frameworks' in agent_state:
        # Drop the completed frameworks which grow over time
        del agent_state['completed_frameworks']
    log.debug("Agent state: %s", str(agent_state))

    log.debug("Virtual memory info in executor: %s" % repr(psutil.virtual_memory()))

    if os.path.exists('/sys/fs/cgroup/memory'):
        # Mesos can limit memory with a cgroup, so we should report on that.
        for (dirpath, dirnames, filenames) in os.walk('/sys/fs/cgroup/memory', followlinks=True):
            for filename in filenames:
                if 'limit_in_bytes' not in filename:
                    continue
                log.debug('cgroup memory info from %s:' % os.path.join(dirpath, filename))
                try:
                    for line in open(os.path.join(dirpath, filename)):
                        log.debug(line.rstrip())
                except Exception:
                    log.debug("Failed to read file")

    # Mesos can also impose rlimit limits, including on things that really
    # ought to not be limited, like virtual address space size.
    log.debug('DATA rlimit: %s', str(resource.getrlimit(resource.RLIMIT_DATA)))
    log.debug('STACK rlimit: %s', str(resource.getrlimit(resource.RLIMIT_STACK)))
    log.debug('RSS rlimit: %s', str(resource.getrlimit(resource.RLIMIT_RSS)))
    log.debug('AS rlimit: %s', str(resource.getrlimit(resource.RLIMIT_AS)))


    executor = MesosExecutor()
    log.debug('Made executor')
    driver = MesosExecutorDriver(executor, use_addict=True)

    old_on_event = driver.on_event

    def patched_on_event(event):
        """
        Intercept and log all pymesos events.
        """
        log.debug("Event: %s", repr(event))
        old_on_event(event)

    driver.on_event = patched_on_event

    log.debug('Made driver')
    driver.start()
    log.debug('Started driver')
    driver_result = driver.join()
    log.debug('Joined driver')

    # Tolerate a None in addition to the code the docs suggest we should receive from join()
    exit_value = 0 if (driver_result is None or driver_result == 'DRIVER_STOPPED') else 1
    assert len(executor.runningTasks) == 0
    sys.exit(exit_value)
