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
from Queue import Empty


class AbstractBatchSystem:
    """An abstract (as far as python currently allows) base class
    to represent the interface the batch system must provide to the toil.
    """

    @staticmethod
    def supportsHotDeployment():
        """
        Whether this batch system supports hot deployment of the user script and toil itself. If it does,
        the __init__ method will have to accept two optional parameters in addition to the declared ones: userScript
        and toilDistribution. Both will be instances of toil.common.HotDeployedResource that represent the user
        script and a source tarball (sdist) of toil respectively.
        """
        return False

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        """This method must be called.
        The config object is setup by the toilSetup script and
        has configuration parameters for the jobtree. You can add stuff
        to that script to get parameters for your batch system.
        """
        self.config = config
        self.maxCores = maxCores
        self.maxMemory = maxMemory
        self.maxDisk = maxDisk
        
    def checkResourceRequest(self, memory, cores, disk):
        """Check resource request is not greater than that available.
        """
        assert memory is not None
        assert disk is not None
        assert cores is not None
        if cores > self.maxCores:
            raise InsufficientSystemResources('cores', cores, self.maxCores)
        if memory > self.maxMemory:
            raise InsufficientSystemResources('memory', memory, self.maxMemory)
        if disk > self.maxDisk:
            raise InsufficientSystemResources('disk', disk, self.maxDisk)
        
    def issueBatchJob(self, command, memory, cores, disk):
        """Issues the following command returning a unique jobID. Command
        is the string to run, memory is an int giving
        the number of bytes the job needs to run in and cores is the number of cpu cores needed for
        the job and error-file is the path of the file to place any std-err/std-out in.
        """
        raise NotImplementedError('Abstract method: issueBatchJob')

    def killBatchJobs(self, jobIDs):
        """Kills the given job IDs.
        """
        raise NotImplementedError('Abstract method: killBatchJobs')

    # FIXME: Return value should be a set (then also fix the tests)

    def getIssuedBatchJobIDs(self):
        """A list of jobs (as jobIDs) currently issued (may be running, or maybe
        just waiting). Despite the result being a list, the ordering should not
        be depended upon.
        """
        raise NotImplementedError('Abstract method: getIssuedBatchJobIDs')
    
    def getRunningBatchJobIDs(self):
        """Gets a map of jobs (as jobIDs) currently running (not just waiting)
        and a how long they have been running for (in seconds).
        """
        raise NotImplementedError('Abstract method: getRunningBatchJobIDs')
    
    def getUpdatedBatchJob(self, maxWait):
        """Gets a job that has updated its status,
        according to the job manager. Max wait gives the number of seconds to pause
        waiting for a result. If a result is available returns (jobID, exitValue)
        else it returns None.
        """
        raise NotImplementedError('Abstract method: getUpdatedBatchJob')

    def shutdown(self):
        """Called at the completion of a toil invocation.
        Should cleanly terminate all worker threads.
        """
        raise NotImplementedError('Abstract Method: shutdown')

    @classmethod
    def getRescueBatchJobFrequency(cls):
        """Gets the period of time to wait (floating point, in seconds) between checking for 
        missing/overlong jobs.
        """
        raise NotImplementedError('Abstract method: getRescueBatchJobFrequency')

    # FIXME: Add a link to the issue tracker for this bug in multiprocessing

    # FIXME: Should be a no-op unless queue is a multiprocessing.Queue
    
    def getFromQueueSafely(self, queue, maxWait):
        """
        Returns an object from the given queue, avoiding a nasty bug in some versions of the
        multiprocessing queue python
        """
        if maxWait <= 0:
            try:
                return queue.get(block=False)
            except Empty:
                return None
        try:
            return queue.get(timeout=maxWait)
        except Empty:
            return None


class InsufficientSystemResources(Exception):
    def __init__(self, cores_or_mem, requested, available):
        self.requested = requested
        self.available = available
        self.cores_or_mem = cores_or_mem

    def __str__(self):
        return 'Requesting more {} than available. Requested: {}, Available: {}' \
               ''.format(self.cores_or_mem, self.requested, self.available)
