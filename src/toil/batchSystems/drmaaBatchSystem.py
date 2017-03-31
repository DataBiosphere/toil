"""
A BatchSystem build on top of the Open Grid Forum Distributed Resource Management
Application API (DRMAA). This implementation uses V1 of the API, which should be
widely available on traditional HPC systems, see https://www.drmaa.org/index.php
for details.

The python bindings used here rely on the presence of the DRMAABatchSystem C
library 'libdrmaa.so.1.0'. This may be packaged together with the batch system
and is available as 'libdrmaa-dev' on many UNIX systems. To ensure that the
Python module can locate the shared library the environment variable
DRMAA_LIBRARY_PATH should be set to point to the file. This module will try
to locate the library even if the environment variable isn't set but is not
guaranteed to succeed.
"""

from abc import abstractmethod
import logging
import os
import subprocess
import time

# Python 3 compatibility imports
from six.moves.queue import Queue

from bd2k.util.objects import abstractclassmethod
from toil.batchSystems.abstractBatchSystem import BatchSystemSupport

logger = logging.getLogger(__name__)

## The drmaa module will fail to load if the DRMAA_LIBRARY_PATH
## environment variable isn't set. In an attempt to avoid such
## failures we look for the file in some likely places.

def findFile(name, path):
    """Locate a file with the given name under path."""
    if os.name == 'posix':
        files = [line for line in subprocess.check_output(
            "{ find /opt -name libdrmaa.so.1.0 3>&2 2>&1 1>&3" +
            " | { grep -v 'Permission denied' >&3; [ $? -eq 1 ]; } } 3>&2 2>&1",
            shell=True).splitlines()]
        if files: return files[0]
        else: return None
    else:
        for root, dirs, files in os.walk(path):
            if name in files:
                return os.path.join(root, name)

## Attempt to locate and load DRMAA shared library
def drmaaPath():
    """Attempt to locate DRMAA C library"""
    if 'DRMAA_LIBRARY_PATH' in os.environ:
        return os.environ['DRMAA_LIBRARY_PATH']
    logger.info("DRMAA_LIBRARY_PATH is not set. Trying to locate shared library.")

    ## use locate if it is available
    try:
        path = subprocess.check_output("locate libdrmaa.so.1.0", shell=True).strip('\n')
        if path: return path
    except subprocess.CalledProcessError:
        pass

    ## check LD_LIBRARY_PATH and other likely locations
    search_path = os.environ['LD_LIBRARY_PATH'].split(':')
    search_path += ['/opt', '/lib', '/usr']
    for root in search_path:
        path = findFile('libdrmaa.so.1.0', root)
        if path is not None:
            return path

    logger.error("Failed to locate 'libdrmaa.so.1.0'. Ensure that it is" +
                 " installed and DRMAA_LIBRARY_PATH is set to its full path.")
    raise RuntimeError(" Could not find drmaa library." +
                       " Please specify its full path using the environment" +
                       " variable DRMAA_LIBRARY_PATH")

path = drmaaPath()
if 'DRMAA_LIBRARY_PATH' not in os.environ:
    logger.info("Found DRMAA libray. Consider setting DRMAA_LIBRARY_PATH=" + path)
os.environ['DRMAA_LIBRARY_PATH'] = path
import drmaa

class AbstractDRMAABatchSystem(BatchSystemSupport):
    @abstractmethod
    def nativeSpec(self, jobNode):
        """
        Provides system specific arguments for job submission.
        Implementations should read user defined arguments from the
        appropriate environment variable, e.g. TOIL_GRIDENGINE_ARGS
        or TOIL_SLURM_ARGS and combine them with a suitable set of
        standard parameters.

        Any occurance of %MEMORY%, %CORES% and %DISK% in the
        provided argument string should be replaced with the
        corresponding fields from jobNode.

        :rtype: string
        """
        raise NotImplementedError()

    @abstractmethod
    def timeElapsed(self, jobIDs):
        """
        Returns a dictionary mapping toil job IDs to current wall clock times for running jobs.

        :param jobIDs: A list of toil job IDs.
        """
        raise NotImplementedError()

    @abstractclassmethod
    def obtainSystemConstants(cls):
        """
        Returns the max. memory and max. CPU for the system
        """
        raise NotImplementedError()

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(AbstractDRMAABatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        self.session = drmaa.Session()
        self.session.initialize()
        self.resultsFile = self._getResultsFileName(config.jobStore)
        # Reset the results (initially, we do this again once we've killed the jobs)
        self.resultsFileHandle = open(self.resultsFile, 'w')
        # We lose any previous state in this file, and ensure the files existence
        self.resultsFileHandle.close()
        self.maxCPU, self.maxMEM = self.obtainSystemConstants()

        self.nextJobID = 0
        self.jobs = dict()
        self.completedJobs = Queue()

    def issueBatchJob(self, jobNode):
        self.checkResourceRequest(jobNode.memory, jobNode.cores, jobNode.disk)
        jobID = self.nextJobID
        self.nextJobID += 1
        jobTemplate = self.session.createJobTemplate()
        jobTemplate.joinFiles = True
        jobTemplate.workingDirectory = os.getcwd()
        jobTemplate.nativeSpecification = self.nativeSpec(jobNode)
        jobTemplate.jobName = 'toil_job_' + str(jobID)
        command = jobNode.command.split()
        jobTemplate.remoteCommand = command[0]
        jobTemplate.args = command[1:]

        logger.debug("Native specification for job %r is %r" % (jobID, jobTemplate.nativeSpecification))
        self.jobs[str(jobID)] = self.session.runJob(jobTemplate)
        logger.debug("Issued the job command: %s with job id: %s", jobNode.command, str(jobID))
        self.session.deleteJobTemplate(jobTemplate)
        return jobID

    def killBatchJobs(self, jobIDs):
        """
        Kills the given jobs, represented as Job ids, then checks they are dead by checking
        that they are no longer active.
        """
        logger.debug('Jobs to be killed: %r', jobIDs)
        jobIDs = set(jobIDs)
        for jid in jobIDs:
            self.session.control(self.jobs[str(jid)], drmaa.JobControlAction.TERMINATE)
        while jobIDs:
            for jid in jobIDs:
                if str(jid) not in self.jobs:
                    jobIDs.remove(jid)
                elif self.session.jobStatus(self.jobs[str(jid)]) == drmaa.JobState.FAILED:
                    jobIDs.remove(jid)
                    del self.jobs[str(jid)]
                elif self.session.jobStatus(self.jobs[str(jid)]) == drmaa.JobState.DONE:
                    jobIDs.remove(jid)
                    self.completedJobs.add(self.getJobInfo(str(jid)))
                    del self.jobs[str(jid)]
            if jobIDs:
                sleep = self.sleepSeconds()
                logger.debug('Some kills (%s) still pending, sleeping %is', len(jobIDs),
                             sleep)
                time.sleep(sleep)

    def getIssuedBatchJobIDs(self):
        return list(self.jobs.keys())

    def getRunningBatchJobIDs(self):
        jids = [jid for jid in self.jobs.keys()
                if self.session.jobStatus(self.jobs[jid]) == drmaa.JobState.RUNNING]
        return self.timeElapsed(jids)

    def getUpdatedBatchJob(self, maxWait):
        if not self.completedJobs.empty():
            return self.completedJobs.get_nowait()
        try:
            jid, exitCode, wallTime = self.getJobInfo(maxWait=maxWait)
        except drmaa.ExitTimeoutException:
            return None
        del self.jobs[str(jid)]
        return jid, exitCode, float(wallTime)

    def shutdown(self):
        """
        Terminates all remaining jobs and closes the DRMAA session.
        """
        self.killBatchJobs(list(self.jobs.keys()))
        self.session.exit()

    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    @classmethod
    def supportsHotDeployment(cls):
        return False

    @classmethod
    def getWaitDuration(self):
        return 5

    @classmethod
    def getRescueBatchJobFrequency(cls):
        return 30 * 60 # Half an hour

    @classmethod
    def sleepSeconds(cls):
        return 1

    def setEnv(self, name, value=None):
        if value and ',' in value:
            raise ValueError(type(self).__name__ + " does not support commata in environment variable values")
        return super(AbstractDRMAABatchSystem,self).setEnv(name, value)

    def getJobInfo(self, jobID=None, maxWait=drmaa.Session.TIMEOUT_WAIT_FOREVER):
        """
        Obtain information on the final state of a completed job.
        If no jobID is provided the batch system is queried for any completed job.

        :param int jobID: Toil job ID or None.
        :param float maxWait: The maximum number of seconds to wait for a job to complete.
        The default is to wait indefinitely.

        :raise ExitTimeoutException: if there are no completed jobs after maxWait seconds.
        """
        if jobID is None:
            jobID = self.session.JOB_IDS_SESSION_ANY
        else:
            jobID = self.jobs[str(jobID)]
        status = self.session.wait(jobID, maxWait)
        jid = int(list(self.jobs.keys())[list(self.jobs.values()).index(status.jobId)])
        return jid, status.exitStatus, status.resourceUsage['ru_wallclock']
