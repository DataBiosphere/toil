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
from collections import namedtuple
from toil.common import Toil
import os
import shutil

# The unit of os.stat().st_blocks according to the documentation of os.stat
unixBlockSize = 512

# A class containing the information required for worker cleanup on shutdown of the batch system.
WorkerCleanupInfo = namedtuple('WorkerCleanupInfo', (
    # A path to the value of config.workDir (where the cache would go)
    'workDir',
    # The value of config.workflowID (used to identify files specific to this workflow)
    'workflowID',
    # The value of the cleanWorkDir flag
    'cleanWorkDir'))


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
        self.environment = {}
        """
        :type dict[str,str]
        """
        self.workerCleanupInfo = WorkerCleanupInfo(workDir=self.config.workDir,
                                                   workflowID=self.config.workflowID,
                                                   cleanWorkDir=self.config.cleanWorkDir)

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
        else it returns None. Does not return anything for jobs that were killed.
        """
        raise NotImplementedError('Abstract method: getUpdatedBatchJob')

    def shutdown(self):
        """Called at the completion of a toil invocation.
        Should cleanly terminate all worker threads.
        """
        raise NotImplementedError('Abstract Method: shutdown')

    def setEnv(self, name, value=None):
        """
        Set an environment variable for the worker process before it is launched. The worker
        process will typically inherit the environment of the machine it is running on but this
        method makes it possible to override specific variables in that inherited environment
        before the worker is launched. Note that this mechanism is different to the one used by
        the worker internally to set up the environment of a job. A call to this method affects
        all jobs issued after this method returns. Note to implementors: This means that you
        would typically need to copy the variables before enqueuing a job.

        If no value is provided it will be looked up from the current environment.

        NB: Only the Mesos and single-machine batch systems support passing environment
        variables. On other batch systems, this method has no effect. See
        https://github.com/BD2KGenomics/toil/issues/547.
        """
        if value is None:
            try:
                value = os.environ[name]
            except KeyError:
                raise RuntimeError("%s does not exist in current environment", name)
        self.environment[name] = value

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

    @staticmethod
    def supportsWorkerCleanup():
        """
        Indicates whether the batch system supports the cleanup of workers on shutdown
        """
        raise NotImplementedError('Abstract method')

    @staticmethod
    def workerCleanup(workerCleanupInfo):
        """
        Cleans up the worker node on batch system shutdown. For now it does nothing.
        :param collections.namedtuple workerCleanupInfo: A named tuple consisting of all the
        relevant information for cleaning up the worker.
        """
        assert isinstance(workerCleanupInfo, WorkerCleanupInfo)
        workflowDir = Toil.getWorkflowDir(workerCleanupInfo.workflowID, workerCleanupInfo.workDir)
        dirIsEmpty = os.listdir(workflowDir) == []
        cleanWorkDir = workerCleanupInfo.cleanWorkDir
        if cleanWorkDir == 'always' or dirIsEmpty and cleanWorkDir in ('onSuccess', 'onError'):
            shutil.rmtree(workflowDir)

    @classmethod
    def probeUsedDisk(cls, workflowID, workDir=None, **kwargs):
        '''
        Used to probe the used disk space (on the block device mounting workDir) minus the space
        used by Toil. Subtracting this from the total disk size gives the batchsystem an estimate of
        how much space is available for Toil. On Mesos, this can be reserved for a non-Toil role.

        :param str workflowID: Unique ID for this workflow
        :param str workDir: value for --workDir passed by the user.
        :param dict **kwargs: Buffer to handle the other objects in the workerCleanupInfo construct
        :return: Amount of disk used by non-Toil purposes
        '''
        allWorkflowDirs = Toil.getAllWorkflowDirs(workflowID, workDir)
        toilDiskUsed = sum([cls.getWorkflowDirSize(wFD) for wFD in allWorkflowDirs])
        diskStats = os.statvfs(workflowDir)
        # Total Disk used = block size * (total blocks - free blocks)
        diskUsed = diskStats.f_frsize * (diskStats.f_blocks - diskStats.f_bavail)
        # Disk used for non-toil purposes = Total disk used - Disk used by toil (workDir)
        nonToilDiskUsed = diskUsed - toilDiskUsed
        return nonToilDiskUsed

    @classmethod
    def getWorkflowDirSize(cls, workflowDir):
        '''
        This method will walk through the workflow directory and return the cumulative filesize in
        bytes of all the files in the directory and its subdirectories.

        :param workflowDir: path to the workflow working directory
        :return: cumulative size in bytes of all files in the directory
        '''
        totalSize = 0
        # The value from running stat on each linked file is equal. To prevent the same file
        # from being counted multiple times, we save the inodes of files that have more than one
        # nlink associated with them.
        linkedFileInodes = set()
        for dirPath, dirNames, fileNames in os.walk(workflowDir):
            folderSize = cls._getDirSize(dirPath, fileNames, linkedFileInodes)
            totalSize += folderSize
        return totalSize

    @staticmethod
    def _getDirSize(dirPath, fileNames, linkedFileInodes):
        '''
        Returns the size of the directory including all linked files provided their inode was not in
        linkedFileInodes.

        :param str dirPath: Dir path
        :param list fileNames: Files in the directory
        :param set linkedFileInodes: set of inode numbers of files that have multiple links, and
                                     have already been acccounted for.
        :return: Size of the directory
        '''
        folderSize = 0
        for f in fileNames:
            fp = os.path.join(dirPath, f)
            fileStats = os.stat(fp)
            if fileStats.st_nlink > 1:
                if fileStats.st_ino not in linkedFileInodes:
                    folderSize += fileStats.st_blocks * unixBlockSize
                    linkedFileInodes.add(fileStats.st_ino)
                else:
                    continue
            else:
                folderSize += fileStats.st_size
        return folderSize

    @staticmethod
    def getFileSystemSize(workflowID, workDir=None, **kwargs):
        '''
        Returns the size of the block device containing the workflow directory, in bytes.

        :param str workflowID: Unique ID for this workflow
        :param str workDir: value for --workDir passed by the user.
        :param dict **kwargs: Buffer to handle the other objects in the workerCleanupInfo construct
        :return: size of the file system
        '''
        diskStats = os.statvfs(Toil.getWorkflowDir(workflowID, workDir))
        return diskStats.f_frsize * diskStats.f_blocks


class InsufficientSystemResources(Exception):
    def __init__(self, cores_or_mem, requested, available):
        self.requested = requested
        self.available = available
        self.cores_or_mem = cores_or_mem

    def __str__(self):
        return 'Requesting more {} than available. Requested: {}, Available: {}' \
               ''.format(self.cores_or_mem, self.requested, self.available)
