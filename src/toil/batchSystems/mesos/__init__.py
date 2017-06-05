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

from Queue import Queue
from collections import namedtuple
from functools import total_ordering
from bisect import bisect
from threading import Lock

TaskData = namedtuple('TaskData', (
    # Time when the task was started
    'startTime',
    # Mesos' ID of the slave where task is being run
    'slaveID',
    # IP of slave where task is being run
    'slaveIP',
    # Mesos' ID of the executor running the task
    'executorID',
    # Memory requirement of the task
    'memory',
    # CPU requirement of the task
    'cores'))


class JobQueue(object):

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

    def sorted(self):
        return list(self.sortedTypes)

    def jobIDs(self):
        with self.jobLock:
            return [job.jobID for queue in self.queues.values() for job in list(queue.queue)]

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


@total_ordering
class ResourceRequirement(object):
    def __init__(self, memory, cores, disk, preemptable):
        # Number of bytes (!) needed for a task
        self.memory = memory
        # Number of CPU cores needed for a task
        self.cores = cores
        # Number of bytes (!) needed for task on disk
        self.disk = disk
        # True, if job can be run on a preemptable node, False otherwise
        self.preemptable = preemptable

    def size(self):
        """
        The scalar size of an offer. Can be used to compare offers.
        """
        return self.cores


    def __gt__(self, other):
        """
        Returns True if self is greater than other, else returns False.
        Note that we take an unintuitive definition of "greater than" since
        we want the ResoureRequirement with the largest requirements to be considered "lesser".
        This is mainly because we want our jobTypes to be sorted in decreasing order, giving expensive jobs
        priority.

        :param other:
        :return:
        """
        if not self.preemptable and other.preemptable:
            # The dominant criteria is preemptability of jobs. Non-preemptable (NP) jobs should be
            # considered first because they can only be run on on NP nodes while P jobs can run on
            # both. Without this prioritization of NP jobs, P jobs could steal NP cores from NP jobs,
            # leaving subsequently offered P cores unused. Despite the prioritization of NP jobs,
            # NP jobs can not steal P cores from P jobs, simply because the offer-acceptance logic
            # would not accept a P offer with a NP job.
            return False
        elif self.preemptable and not other.preemptable:
            return True

        if self.cores > other.cores:
            return False
        elif self.cores < other.cores:
            return True

        if self.memory > other.memory:
            return False
        elif self.memory < other.memory:
            return True

        if self.disk > other.disk:
            return False
        elif self.disk < other.disk:
            return True

        return False

    def __eq__(self, other):
        if (self.preemptable == other.preemptable and
           self.cores == other.cores and
           self.memory == other.memory and
           self.disk == other.disk):
            return True
        return False

    def __hash__(self):
        return hash((self.preemptable, self.cores, self.memory, self.disk))


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
