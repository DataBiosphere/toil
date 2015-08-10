#!/usr/bin/env python

"""
Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

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

    def __init__(self, config, maxCpus, maxMemory, maxDisk):
        """This method must be called.
        The config object is setup by the toilSetup script and
        has configuration parameters for the jobtree. You can add stuff
        to that script to get parameters for your batch system.
        """
        self.config = config
        self.maxCpus = maxCpus
        self.maxMemory = maxMemory
        self.maxDisk = maxDisk
        
    def checkResourceRequest(self, memory, cpu, disk):
        """Check resource request is not greater than that available.
        """
        assert memory is not None
        assert disk is not None
        assert cpu is not None
        if cpu > self.maxCpus:
            raise InsufficientSystemResources('CPUs', cpu, self.maxCpus)
        if memory > self.maxMemory:
            raise InsufficientSystemResources('memory', memory, self.maxMemory)
        if disk > self.maxDisk:
            raise InsufficientSystemResources('disk', disk, self.maxDisk)
    def issueBatchJob(self, command, memory, cpu, disk):
        """Issues the following command returning a unique jobID. Command
        is the string to run, memory is an int giving
        the number of bytes the job needs to run in and cpu is the number of cpus needed for
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
    def __init__(self, cpu_or_mem, requested, available):
        self.requested = requested
        self.available = available
        self.cpu_or_mem = cpu_or_mem

    def __str__(self):
        return 'Requesting more {} than available. Requested: {}, Available: {}' \
               ''.format(self.cpu_or_mem, self.requested, self.available)