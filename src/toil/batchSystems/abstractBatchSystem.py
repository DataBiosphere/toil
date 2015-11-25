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
import os


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
        
    def issueBatchJob(self, command, memory, cores, disk, preemptable):
        """Issues the following command returning a unique jobID. Command
        is the string to run, memory is an int giving
        the number of bytes the job needs to run in and cores is the number of cpu cores needed for
        the job and error-file is the path of the file to place any std-err/std-out in.
        
        :param booleam preemptable: If True the job can be run on a preemptable node, otherwise
        not. 
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
        else it returns None. Does not return anything for jobs that were killed.
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

    def _getResultsFileName(self, toilPath):
        """Get a path for the batch systems to store results. GridEngine
        and LSF currently use this.
        """
        return os.path.join(toilPath, "results.txt")
    
class AbstractScalableBatchSystemInterface(object):
    """
    A set of methods used by :class:`toil.provisioners.clusterScaler.ClusterScaler` to scale
    the number of worker nodes in the cluster.
    """
    def getIssuedQueueSize(self, preemptable=False):
        """
        Gets the approximate number of jobs issued but not yet running. 
        
        :param boolean preemptable: If true returns number of preemptable jobs queued, else
        returns the number of non-preemptable jobs.
        :return: Number of jobs issued but not yet running. This may be an approximation.
        """
        raise NotImplementedError('Abstract method: queueSize')
    
    def getAvgJobRuntime(self, preemptable=False):
        """
        Gets the avg. number of jobs started per second, averaging over the last N
        jobs submitted and ignoring intervals when 
        the input queue was empty.
        
        N is suggested to be 100.
        """
    
    def numberOfRecentJobsStartedPerSecond(self, preemptable=False):
        """
        Gets the avg. number of jobs started per second, averaging over the last N
        jobs submitted and ignoring intervals when 
        the input queue was empty.
        
        N is suggested to be 100.
        
        A job is started the moment its execution begins on a worker node.
        
        For example, if 50 jobs
        are issued and after 10 seconds 5 jobs have been started a call at 10 seconds 
        would return 5/10 = 0.5, after 20 seconds if no more jobs have been started it would return 
        5/10 = 0.25. If after 100 seconds all of them have been started it would return 0.5.
        If some time later another 50 jobs are added and it takes 50 seconds to start them 
        all then the method would return (50+50)/(100+50) = 0.66666* after that point.
        
        When fewer than N jobs are run the average should be over the jobs started so far.
        When no jobs have been issued the return value is 0.
        
        :param boolean preemptable: If true returns rate for preemptable jobs, else
        returns the rate for non-preemptable jobs.
        :return: Number of jobs issued per second, avg over the last N jobs. This may be an approximation.
        :rtype: float
        """
        raise NotImplementedError('Abstract method: queueSize')
    
    def getNumberOfEmptyNodes(self, preemptable=False):
        """
        A node is empty if it is not running any worker jobs.
        
        :param boolean preemptable: If true returns number of empty preemptable nodes, else
        returns the number of non-preemptable nodes.
        :return: Number of nodes in cluster that are empty. 
        :rtype: int
        """
        raise NotImplementedError('Abstract method: getNumberOfEmptyNodes')

class AbstractScalableBatchSystem(AbstractScalableBatchSystemInterface, AbstractBatchSystem):
    """
    A batch system with the added methods of the AbstractScalableBatchSystemInterface class
    """
    pass

class InsufficientSystemResources(Exception):
    def __init__(self, cores_or_mem, requested, available):
        self.requested = requested
        self.available = available
        self.cores_or_mem = cores_or_mem

    def __str__(self):
        return 'Requesting more {} than available. Requested: {}, Available: {}' \
               ''.format(self.cores_or_mem, self.requested, self.available)
