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
import logging

from toil.job import JobNode

logger = logging.getLogger( __name__ )


class JobGraph(JobNode):
    """
    A class encapsulating the minimal state of a Toil job. Instances of this class are persisted
    in the job store and held in memory by the master. The actual state of job objects in user
    scripts is persisted separately since it may be much bigger than the state managed by this
    class and should therefore only be held in memory for brief periods of time.
    """
    def __init__(self, command, memory, cores, disk, unitName, jobName, preemptable,
                 jobStoreID, remainingRetryCount, predecessorNumber,
                 filesToDelete=None, predecessorsFinished=None,
                 stack=None, services=None,
                 startJobStoreID=None, terminateJobStoreID=None,
                 errorJobStoreID=None,
                 logJobStoreFileID=None,
                 checkpoint=None,
                 checkpointFilesToDelete=None,
                 chainedJobs=None):
        requirements = {'memory': memory, 'cores': cores, 'disk': disk,
                        'preemptable': preemptable}
        super(JobGraph, self).__init__(command=command,
                                       requirements=requirements,
                                       unitName=unitName, jobName=jobName,
                                       jobStoreID=jobStoreID,
                                       predecessorNumber=predecessorNumber)

        # The number of times the job should be retried if it fails This number is reduced by
        # retries until it is zero and then no further retries are made
        self.remainingRetryCount = remainingRetryCount
        
        # This variable is used in creating a graph of jobs. If a job crashes after an update to
        # the jobGraph but before the list of files to remove is deleted then this list can be
        # used to clean them up.
        self.filesToDelete = filesToDelete or []
        
        # The number of predecessor jobs of a given job. A predecessor is a job which references
        # this job in its stack.
        self.predecessorNumber = predecessorNumber
        # The IDs of predecessors that have finished. When len(predecessorsFinished) ==
        # predecessorNumber then the job can be run.
        self.predecessorsFinished = predecessorsFinished or set()
        
        # The list of successor jobs to run. Successor jobs are stored as jobNodes. Successor
        # jobs are run in reverse order from the stack.
        self.stack = stack or []
        
        # A jobStoreFileID of the log file for a job. This will be none unless the job failed and
        #  the logging has been captured to be reported on the leader.
        self.logJobStoreFileID = logJobStoreFileID 
        
        # A list of lists of service jobs to run. Each sub list is a list of service jobs
        # descriptions, each of which is stored as a 6-tuple of the form (jobStoreId, memory,
        # cores, disk, startJobStoreID, terminateJobStoreID).
        self.services = services or []
        
        # An empty file in the jobStore which when deleted is used to signal that the service
        # should cease.
        self.terminateJobStoreID = terminateJobStoreID
        
        # Similarly a empty file which when deleted is used to signal that the service is
        # established
        self.startJobStoreID = startJobStoreID
        
        # An empty file in the jobStore which when deleted is used to signal that the service
        # should terminate signaling an error.
        self.errorJobStoreID = errorJobStoreID
        
        # None, or a copy of the original command string used to reestablish the job after failure.
        self.checkpoint = checkpoint
        
        # Files that can not be deleted until the job and its successors have completed
        self.checkpointFilesToDelete = checkpointFilesToDelete

        # Names of jobs that were run as part of this job's invocation, starting with
        # this job
        self.chainedJobs = chainedJobs

    def setupJobAfterFailure(self, config):
        """
        Reduce the remainingRetryCount if greater than zero and set the memory
        to be at least as big as the default memory (in case of exhaustion of memory,
        which is common).
        """
        self.remainingRetryCount = max(0, self.remainingRetryCount - 1)
        logger.warn("Due to failure we are reducing the remaining retry count of job %s with ID %s to %s",
                    self, self.jobStoreID, self.remainingRetryCount)
        # Set the default memory to be at least as large as the default, in
        # case this was a malloc failure (we do this because of the combined
        # batch system)
        if self.memory < config.defaultMemory:
            self._memory = config.defaultMemory
            logger.warn("We have increased the default memory of the failed job %s to %s bytes",
                        self, self.memory)

    def getLogFileHandle( self, jobStore ):
        """
        Returns a context manager that yields a file handle to the log file
        """
        return jobStore.readFileStream( self.logJobStoreFileID )

    @classmethod
    def fromJobNode(cls, jobNode, jobStoreID, tryCount):
        """
        Builds a job graph from a given job node
        :param toil.job.JobNode jobNode: a job node object to build into a job graph
        :param str jobStoreID: the job store ID to assign to the resulting job graph object
        :param int tryCount: the number of times the resulting job graph object can be retried after
            failure
        :return: The newly created job graph object
        :rtype: toil.jobGraph.JobGraph
        """
        return cls(command=jobNode.command,
                   jobStoreID=jobStoreID,
                   remainingRetryCount=tryCount,
                   predecessorNumber=jobNode.predecessorNumber,
                   unitName=jobNode.unitName, jobName=jobNode.jobName,
                   **jobNode._requirements)

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.remainingRetryCount == other.remainingRetryCount
            and self.jobStoreID == other.jobStoreID
            and self.filesToDelete == other.filesToDelete
            and self.stack == other.stack
            and self.predecessorNumber == other.predecessorNumber
            and self.predecessorsFinished == other.predecessorsFinished
            and self.logJobStoreFileID == other.logJobStoreFileID)
