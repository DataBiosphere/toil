# Copyright (c) 2016 Duke Center for Genomic and Computational Biology
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
import os
from pipes import quote
import subprocess
import time
import math
from Queue import Queue, Empty
from threading import Thread

from toil.batchSystems import MemoryString
from toil.batchSystems.abstractBatchSystem import BatchSystemSupport

logger = logging.getLogger(__name__)

sleepSeconds = 1


class Worker(Thread):
    def __init__(self, newJobsQueue, updatedJobsQueue, killQueue, killedJobsQueue, boss):
        Thread.__init__(self)
        self.newJobsQueue = newJobsQueue
        self.updatedJobsQueue = updatedJobsQueue
        self.killQueue = killQueue
        self.killedJobsQueue = killedJobsQueue
        self.waitingJobs = list()
        self.runningJobs = set()
        self.boss = boss
        self.allocatedCpus = dict()
        self.slurmJobIDs = dict()

    def parse_elapsed(self, elapsed):
        # slurm returns elapsed time in days-hours:minutes:seconds format
        # Sometimes it will only return minutes:seconds, so days may be omitted
        # For ease of calculating, we'll make sure all the delimeters are ':'
        # Then reverse the list so that we're always counting up from seconds -> minutes -> hours -> days
        total_seconds = 0
        try:
            elapsed = elapsed.replace('-', ':').split(':')
            elapsed.reverse()
            seconds_per_unit = [1, 60, 3600, 86400]
            for index, multiplier in enumerate(seconds_per_unit):
                if index < len(elapsed):
                    total_seconds += multiplier * int(elapsed[index])
        except ValueError:
            pass  # slurm may return INVALID instead of a time
        return total_seconds

    def getRunningJobIDs(self):
        # Should return a dictionary of Job IDs and number of seconds
        times = {}
        currentjobs = dict((str(self.slurmJobIDs[x]), x) for x in self.runningJobs)
        # currentjobs is a dictionary that maps a slurm job id (string) to our own internal job id
        # squeue arguments:
        # -h for no header
        # --format to get jobid i, state %t and time days-hours:minutes:seconds

        lines = subprocess.check_output(['squeue', '-h', '--format', '%i %t %M']).split('\n')
        for line in lines:
            values = line.split()
            if len(values) < 3:
                continue
            slurm_jobid, state, elapsed_time = values
            if slurm_jobid in currentjobs and state == 'R':
                seconds_running = self.parse_elapsed(elapsed_time)
                times[currentjobs[slurm_jobid]] = seconds_running

        return times

    def getSlurmID(self, jobID):
        if not jobID in self.slurmJobIDs:
            RuntimeError("Unknown jobID, could not be converted")

        job = self.slurmJobIDs[jobID]
        return str(job)

    def forgetJob(self, jobID):
        self.runningJobs.remove(jobID)
        del self.allocatedCpus[jobID]
        del self.slurmJobIDs[jobID]

    def killJobs(self):
        # Load hit list:
        killList = list()
        while True:
            try:
                jobId = self.killQueue.get(block=False)
            except Empty:
                break
            else:
                killList.append(jobId)

        if not killList:
            return False

        # Do the dirty job
        for jobID in list(killList):
            if jobID in self.runningJobs:
                logger.debug('Killing job: %s', jobID)
                subprocess.check_call(['scancel', self.getSlurmID(jobID)])
            else:
                if jobID in self.waitingJobs:
                    self.waitingJobs.remove(jobID)
                self.killedJobsQueue.put(jobID)
                killList.remove(jobID)

        # Wait to confirm the kill
        while killList:
            for jobID in list(killList):
                if self.getJobExitCode(self.slurmJobIDs[jobID]) is not None:
                    logger.debug('Adding jobID %s to killedJobsQueue', jobID)
                    self.killedJobsQueue.put(jobID)
                    killList.remove(jobID)
                    self.forgetJob(jobID)
            if len(killList) > 0:
                logger.warn("Some jobs weren't killed, trying again in %is.", sleepSeconds)
                time.sleep(sleepSeconds)

        return True

    def createJobs(self, newJob):
        activity = False
        # Load new job id if present:
        if newJob is not None:
            self.waitingJobs.append(newJob)
        # Launch jobs as necessary:
        while (len(self.waitingJobs) > 0
               and sum(self.allocatedCpus.values()) < int(self.boss.maxCores)):
            activity = True
            jobID, cpu, memory, command = self.waitingJobs.pop(0)
            sbatch_line = self.prepareSbatch(cpu, memory, jobID) + ['--wrap={}'.format(command)]
            slurmJobID = self.sbatch(sbatch_line)
            self.slurmJobIDs[jobID] = slurmJobID
            self.runningJobs.add(jobID)
            self.allocatedCpus[jobID] = cpu
        return activity

    def checkOnJobs(self):
        activity = False
        logger.debug('List of running jobs: %r', self.runningJobs)
        for jobID in list(self.runningJobs):
            logger.debug("Checking status of internal job id %d", jobID)
            status = self.getJobExitCode(self.slurmJobIDs[jobID])
            if status is not None:
                activity = True
                self.updatedJobsQueue.put((jobID, status))
                self.forgetJob(jobID)
        return activity

    def run(self):
        while True:
            activity = False
            newJob = None
            if not self.newJobsQueue.empty():
                activity = True
                newJob = self.newJobsQueue.get()
                if newJob is None:
                    logger.debug('Received queue sentinel.')
                    break
            activity |= self.killJobs()
            activity |= self.createJobs(newJob)
            activity |= self.checkOnJobs()
            if not activity:
                logger.debug('No activity, sleeping for %is', sleepSeconds)
                time.sleep(sleepSeconds)

    def prepareSbatch(self, cpu, mem, jobID):
        #  Returns the sbatch command line before the script to run
        sbatch_line = ['sbatch', '-Q', '-J', 'toil_job_{}'.format(jobID)]

        if self.boss.environment:
            for k, v in self.boss.environment.iteritems():
                quoted_value = quote(os.environ[k] if v is None else v)
                sbatch_line.append('--export={}={}'.format(k, quoted_value))

        if mem is not None:
            # memory passed in is in bytes, but slurm expects megabytes
            sbatch_line.append('--mem={}'.format(int(mem) / 2 ** 20))
        if cpu is not None:
            sbatch_line.append('--cpus-per-task={}'.format(int(math.ceil(cpu))))

        # "Native extensions" for SLURM (see DRMAA or SAGA)
        nativeConfig = os.getenv('TOIL_SLURM_ARGS')
        if nativeConfig is not None:
            logger.debug("Native SLURM options appended to sbatch from TOIL_SLURM_RESOURCES env. variable: {}".format(nativeConfig))
            if "--mem" or "--cpus-per-task" in nativeConfig:
                raise ValueError("Some resource arguments are incompatible: {}".format(nativeConfig))

            sbatch_line.extend([nativeConfig])

        return sbatch_line

    def sbatch(self, sbatch_line):
        logger.debug("Running %r", sbatch_line)
        try:
            output = subprocess.check_output(sbatch_line, stderr=subprocess.STDOUT)
            # sbatch prints a line like 'Submitted batch job 2954103'
            result = int(output.strip().split()[-1])
            logger.debug("sbatch submitted job %d", result)
            return result
        except subprocess.CalledProcessError as e:
            logger.error("sbatch command failed with code %d: %s", e.returncode, e.output)
            raise e
        except OSError as e:
            logger.error("sbatch command failed")
            raise e

    def getJobExitCode(self, slurmJobID):
        logger.debug("Getting exit code for slurm job %d", slurmJobID)
        # SLURM job exit codes are obtained by running sacct.
        args = ['sacct',
                '-n', # no header
                '-j', str(slurmJobID), # job
                '--format', 'State,ExitCode', # specify output columns
                '-P', # separate columns with pipes
                '-S', '1970-01-01'] # override start time limit
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in process.stdout:
            values = line.strip().split('|')
            if len(values) < 2:
                continue
            state, exitcode = values
            logger.debug("sacct job state is %s", state)
            # If Job is in a running state, return None to indicate we don't have an update
            if state in ('PENDING', 'RUNNING', 'CONFIGURING', 'COMPLETING', 'RESIZING', 'SUSPENDED'):
                return None
            status, _ = exitcode.split(':')
            logger.debug("sacct exit code is %s, returning status %s", exitcode, status)
            return int(status)
        logger.debug("Did not find exit code for job in sacct output")
        return None


class SlurmBatchSystem(BatchSystemSupport):
    """
    The interface for SLURM
    """

    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    @classmethod
    def supportsHotDeployment(cls):
        return False

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(SlurmBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        self.slurmResultsFile = self._getResultsFileName(config.jobStore)
        # Reset the job queue and results (initially, we do this again once we've killed the jobs)
        self.slurmResultsFileHandle = open(self.slurmResultsFile, 'w')
        # We lose any previous state in this file, and ensure the files existence
        self.slurmResultsFileHandle.close()
        self.currentJobs = set()
        self.maxCPU, self.maxMEM = self.obtainSystemConstants()
        self.nextJobID = 0
        self.newJobsQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.killQueue = Queue()
        self.killedJobsQueue = Queue()
        self.worker = Worker(self.newJobsQueue, self.updatedJobsQueue, self.killQueue,
                             self.killedJobsQueue, self)
        self.worker.start()

    def __des__(self):
        # Closes the file handle associated with the results file.
        self.slurmResultsFileHandle.close()

    def issueBatchJob(self, command, memory, cores, disk, preemptable):
        self.checkResourceRequest(memory, cores, disk)
        jobID = self.nextJobID
        self.nextJobID += 1
        self.currentJobs.add(jobID)
        self.newJobsQueue.put((jobID, cores, memory, command))
        logger.debug("Issued the job command: %s with job id: %s ", command, str(jobID))
        return jobID

    def killBatchJobs(self, jobIDs):
        """
        Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        jobIDs = set(jobIDs)
        logger.debug('Jobs to be killed: %r', jobIDs)
        for jobID in jobIDs:
            self.killQueue.put(jobID)
        while jobIDs:
            killedJobId = self.killedJobsQueue.get()
            if killedJobId is None:
                break
            jobIDs.remove(killedJobId)
            if killedJobId in self.currentJobs:
                self.currentJobs.remove(killedJobId)
            if jobIDs:
                logger.debug('Some kills (%s) still pending, sleeping %is', len(jobIDs),
                             sleepSeconds)
                time.sleep(sleepSeconds)

    def getIssuedBatchJobIDs(self):
        """
        Gets the list of jobs issued to SLURM.
        """
        return list(self.currentJobs)

    def getRunningBatchJobIDs(self):
        return self.worker.getRunningJobIDs()

    def getUpdatedBatchJob(self, maxWait):
        try:
            item = self.updatedJobsQueue.get(timeout=maxWait)
        except Empty:
            return None
        logger.debug('UpdatedJobsQueue Item: %s', item)
        jobID, retcode = item
        self.currentJobs.remove(jobID)
        return jobID, retcode, None

    def shutdown(self):
        """
        Signals worker to shutdown (via sentinel) then cleanly joins the thread
        """
        newJobsQueue = self.newJobsQueue
        self.newJobsQueue = None

        newJobsQueue.put(None)
        self.worker.join()

    def getWaitDuration(self):
        return 1.0

    @classmethod
    def getRescueBatchJobFrequency(cls):
        return 30 * 60 # Half an hour

    @staticmethod
    def obtainSystemConstants():
        # sinfo -Ne --format '%m,%c'
        # sinfo arguments:
        # -N for node-oriented
        # -h for no header
        # -e for exact values (e.g. don't return 32+)
        # --format to get memory, cpu
        max_cpu = 0
        max_mem = MemoryString('0')
        lines = subprocess.check_output(['sinfo', '-Nhe', '--format', '%m %c']).split('\n')
        for line in lines:
            values = line.split()
            if len(values) < 2:
                continue
            mem, cpu = values
            max_cpu = max(max_cpu, int(cpu))
            max_mem = max(max_mem, MemoryString(mem + 'M'))
        if max_cpu == 0 or max_mem.byteVal() == 0:
            RuntimeError('sinfo did not return memory or cpu info')
        return max_cpu, max_mem
