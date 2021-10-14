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

from bisect import bisect
from collections import namedtuple
from queue import Queue
from threading import Lock

from toil.provisioners.abstractProvisioner import Shape

TaskData = namedtuple('TaskData', (
    # Time when the task was started
    'startTime',
    # Mesos' ID of the agent where task is being run
    'agentID',
    # IP of agent where task is being run
    'agentIP',
    # Mesos' ID of the executor running the task
    'executorID',
    # Memory requirement of the task
    'memory',
    # CPU requirement of the task
    'cores'))


class JobQueue:
    def __init__(self):
        # mapping of jobTypes to queues of jobs of that type
        self.queues = {}
        # list of jobTypes in decreasing resource expense
        self.sortedTypes = []
        self.jobLock = Lock()

    def insertJob(self, job, jobType):
        with self.jobLock:
            if jobType not in self.queues:
                index = bisect(self.sortedTypes, jobType)
                self.sortedTypes.insert(index, jobType)
                self.queues[jobType] = Queue()
            self.queues[jobType].put(job)

    def jobIDs(self):
        with self.jobLock:
            return [job.jobID for queue in list(self.queues.values()) for job in list(queue.queue)]

    def nextJobOfType(self, jobType):
        with self.jobLock:
            job = self.queues[jobType].get(block=False)
            if self.queues[jobType].empty():
                del self.queues[jobType]
                self.sortedTypes.remove(jobType)
            return job

    def typeEmpty(self, jobType):
        # without a lock we could get a false negative from this method
        # if it were called while nextJobOfType was executing
        with self.jobLock:
            return self.queues.get(jobType, Queue()).empty()


class MesosShape(Shape):
    def __gt__(self, other):
        """
        Inverted.  Returns True if self is less than other, else returns False.

        This is because jobTypes are sorted in decreasing order,
        and this was done to give expensive jobs priority.
        """
        return not self.greater_than(other)


ToilJob = namedtuple('ToilJob', (
    # A job ID specific to this batch system implementation
    'jobID',
    # What string to display in the mesos UI
    'name',
    # A ResourceRequirement tuple describing the resources needed by this job
    'resources',
    # The command to be run on the worker node
    'command',
    # The resource object representing the user script
    'userScript',
    # A dictionary with additional environment variables to be set on the worker process
    'environment',
    # A named tuple containing all the required info for cleaning up the worker node
    'workerCleanupInfo'))
