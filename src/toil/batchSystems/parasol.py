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
import os
import re
import sys
import subprocess
import tempfile
import time
from Queue import Empty
from Queue import Queue
from threading import Thread

from bd2k.util.iterables import concat
from bd2k.util.processes import which

from toil.batchSystems.abstractBatchSystem import BatchSystemSupport
from toil.lib.bioio import getTempFile

logger = logging.getLogger(__name__)


class ParasolBatchSystem(BatchSystemSupport):
    """
    The interface for Parasol.
    """

    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    @classmethod
    def supportsHotDeployment(cls):
        return False

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(ParasolBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        if maxMemory != sys.maxint:
            logger.warn('The Parasol batch system does not support maxMemory.')
        # Keep the name of the results file for the pstat2 command..
        command = config.parasolCommand
        if os.path.sep not in command:
            try:
                command = next(which(command))
            except StopIteration:
                raise RuntimeError("Can't find %s on PATH." % command)
        logger.info('Using Parasol at %s', command)
        self.parasolCommand = command
        self.parasolResultsDir = tempfile.mkdtemp(dir=config.jobStore)

        # In Parasol, each results file corresponds to a separate batch, and all jobs in a batch
        # have the same cpu and memory requirements. The keys to this dictionary are the (cpu,
        # memory) tuples for each batch. A new batch is created whenever a job has a new unique
        # combination of cpu and memory requirements.
        self.resultsFiles = dict()
        self.maxBatches = config.parasolMaxBatches

        # Allows the worker process to send back the IDs of jobs that have finished, so the batch
        #  system can decrease its used cpus counter
        self.cpuUsageQueue = Queue()

        # Also stores finished job IDs, but is read by getUpdatedJobIDs().
        self.updatedJobsQueue = Queue()

        # Use this to stop the worker when shutting down
        self.running = True

        self.worker = Thread(target=self.updatedJobWorker, args=())
        self.worker.start()
        self.usedCpus = 0
        self.jobIDsToCpu = {}

        # Set of jobs that have been issued but aren't known to have finished or been killed yet.
        #  Jobs that end by themselves are removed in getUpdatedJob, and jobs that are killed are
        #  removed in killBatchJobs.
        self.runningJobs = set()

    def _runParasol(self, command, autoRetry=True):
        """
        Issues a parasol command using popen to capture the output. If the command fails then it
        will try pinging parasol until it gets a response. When it gets a response it will
        recursively call the issue parasol command, repeating this pattern for a maximum of N
        times. The final exit value will reflect this.
        """
        command = list(concat(self.parasolCommand, command))
        while True:
            logger.debug('Running %r', command)
            process = subprocess.Popen(command,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       bufsize=-1)
            stdout, stderr = process.communicate()
            status = process.wait()
            for line in stderr.split('\n'):
                if line: logger.warn(line)
            if status == 0:
                return 0, stdout.split('\n')
            message = 'Command %r failed with exit status %i' % (command, status)
            if autoRetry:
                logger.warn(message)
            else:
                logger.error(message)
                return status, None
            logger.warn('Waiting for a 10s, before trying again')
            time.sleep(10)

    parasolOutputPattern = re.compile("your job ([0-9]+).*")

    def issueBatchJob(self, command, memory, cores, disk, preemptable):
        """
        Issues parasol with job commands.
        """
        self.checkResourceRequest(memory, cores, disk)

        MiB = 1 << 20
        truncatedMemory = (memory / MiB) * MiB
        # Look for a batch for jobs with these resource requirements, with
        # the memory rounded down to the nearest megabyte. Rounding down
        # meams the new job can't ever decrease the memory requirements
        # of jobs already in the batch.
        if len(self.resultsFiles) >= self.maxBatches:
            raise RuntimeError( 'Number of batches reached limit of %i' % self.maxBatches)
        try:
            results = self.resultsFiles[(truncatedMemory, cores)]
        except KeyError:
            results = getTempFile(rootDir=self.parasolResultsDir)
            self.resultsFiles[(truncatedMemory, cores)] = results

        # Prefix the command with environment overrides, optionally looking them up from the
        # current environment if the value is None
        command = ' '.join(concat('env', self.__environment(), command))
        parasolCommand = ['-verbose',
                          '-ram=%i' % memory,
                          '-cpu=%i' % cores,
                          '-results=' + results,
                          'add', 'job', command]
        # Deal with the cpus
        self.usedCpus += cores
        while True:  # Process finished results with no wait
            try:
                jobID = self.cpuUsageQueue.get_nowait()
            except Empty:
                break
            if jobID in self.jobIDsToCpu.keys():
                self.usedCpus -= self.jobIDsToCpu.pop(jobID)
            assert self.usedCpus >= 0
        while self.usedCpus > self.maxCores:  # If we are still waiting
            jobID = self.cpuUsageQueue.get()
            if jobID in self.jobIDsToCpu.keys():
                self.usedCpus -= self.jobIDsToCpu.pop(jobID)
            assert self.usedCpus >= 0
        # Now keep going
        while True:
            line = self._runParasol(parasolCommand)[1][0]
            match = self.parasolOutputPattern.match(line)
            if match is None:
                # This is because parasol add job will return success, even if the job was not
                # properly issued!
                logger.info('We failed to properly add the job, we will try again after a 5s.')
                time.sleep(5)
            else:
                jobID = int(match.group(1))
                self.jobIDsToCpu[jobID] = cores
                self.runningJobs.add(jobID)
                logger.debug("Got the parasol job id: %s from line: %s" % (jobID, line))
                return jobID

    def setEnv(self, name, value=None):
        if value and ' ' in value:
            raise ValueError('Parasol does not support spaces in environment variable values.')
        return super(ParasolBatchSystem, self).setEnv(name, value)

    def __environment(self):
        return (k + '=' + (os.environ[k] if v is None else v) for k, v in self.environment.items())

    def killBatchJobs(self, jobIDs):
        """Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        while True:
            for jobID in jobIDs:
                if jobID in self.runningJobs:
                    self.runningJobs.remove(jobID)
                exitValue = self._runParasol(['remove', 'job', str(jobID)],
                                             autoRetry=False)[0]
                logger.info("Tried to remove jobID: %i, with exit value: %i" % (jobID, exitValue))
            runningJobs = self.getIssuedBatchJobIDs()
            if set(jobIDs).difference(set(runningJobs)) == set(jobIDs):
                break
            logger.warn( 'Tried to kill some jobs, but something happened and they are still '
                         'going, will try againin 5s.')
            time.sleep(5)
        # Update the CPU usage, because killed jobs aren't written to the results file.
        for jobID in jobIDs:
            if jobID in self.jobIDsToCpu.keys():
                self.usedCpus -= self.jobIDsToCpu.pop(jobID)

    queuePattern = re.compile(r'q\s+([0-9]+)')
    runningPattern = re.compile(r'r\s+([0-9]+)\s+[\S]+\s+[\S]+\s+([0-9]+)\s+[\S]+')

    def getJobIDsForResultsFile(self, resultsFile):
        """
        Get all queued and running jobs for a results file.
        """
        jobIDs = []
        for line in self._runParasol(['-results=' + resultsFile, 'pstat2'])[1]:
            runningJobMatch = self.runningPattern.match(line)
            queuedJobMatch = self.queuePattern.match(line)
            if runningJobMatch:
                jobID = runningJobMatch.group(1)
            elif queuedJobMatch:
                jobID = queuedJobMatch.group(1)
            else:
                continue
            jobIDs.append(int(jobID))
        return set(jobIDs)

    def getIssuedBatchJobIDs(self):
        """
        Gets the list of jobs issued to parasol in all results files, but not including jobs
        created by other users.
        """
        issuedJobs = set()
        for resultsFile in self.resultsFiles.itervalues():
            issuedJobs.update(self.getJobIDsForResultsFile(resultsFile))

        return list(issuedJobs)

    def getRunningBatchJobIDs(self):
        """
        Returns map of running jobIDs and the time they have been running.
        """
        # Example lines..
        # r 5410186 benedictpaten worker 1247029663 localhost
        # r 5410324 benedictpaten worker 1247030076 localhost
        runningJobs = {}
        issuedJobs = self.getIssuedBatchJobIDs()
        for line in self._runParasol(['pstat2'])[1]:
            if line != '':
                match = self.runningPattern.match(line)
                if match is not None:
                    jobID = int(match.group(1))
                    startTime = int(match.group(2))
                    if jobID in issuedJobs:  # It's one of our jobs
                        runningJobs[jobID] = time.time() - startTime
        return runningJobs

    def getUpdatedBatchJob(self, maxWait):
        while True:
            try:
                jobID, status, wallTime = self.updatedJobsQueue.get(timeout=maxWait)
            except Empty:
                return None
            try:
                self.runningJobs.remove(jobID)
            except KeyError:
                # We tried to kill this job, but it ended by itself instead, so skip it.
                pass
            else:
                return jobID, status, wallTime

    @classmethod
    def getRescueBatchJobFrequency(cls):
        """
        Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive.
        """
        return 5400  # Once every 90 minutes

    def updatedJobWorker(self):
        """
        We use the parasol results to update the status of jobs, adding them
        to the list of updated jobs.

        Results have the following structure.. (thanks Mark D!)

        int status;    /* Job status - wait() return format. 0 is good. */
        char *host;    /* Machine job ran on. */
        char *jobId;    /* Job queuing system job ID */
        char *exe;    /* Job executable file (no path) */
        int usrTicks;    /* 'User' CPU time in ticks. */
        int sysTicks;    /* 'System' CPU time in ticks. */
        unsigned submitTime;    /* Job submission time in seconds since 1/1/1970 */
        unsigned startTime;    /* Job start time in seconds since 1/1/1970 */
        unsigned endTime;    /* Job end time in seconds since 1/1/1970 */
        char *user;    /* User who ran job */
        char *errFile;    /* Location of stderr file on host */

        Plus you finally have the command name.
        """
        resultsFiles = set()
        resultsFileHandles = []
        try:
            while self.running:
                # Look for any new results files that have been created, and open them
                newResultsFiles = set(os.listdir(self.parasolResultsDir)).difference(resultsFiles)
                for newFile in newResultsFiles:
                    newFilePath = os.path.join(self.parasolResultsDir, newFile)
                    resultsFileHandles.append(open(newFilePath, 'r'))
                    resultsFiles.add(newFile)
                for fileHandle in resultsFileHandles:
                    while self.running:
                        line = fileHandle.readline()
                        if not line:
                            break
                        assert line[-1] == '\n'
                        (status, host, jobId, exe, usrTicks, sysTicks, submitTime, startTime,
                         endTime, user, errFile, command) = line[:-1].split(None, 11)
                        status = int(status)
                        jobId = int(jobId)
                        if os.WIFEXITED(status):
                            status = os.WEXITSTATUS(status)
                        else:
                            status = -status
                        self.cpuUsageQueue.put(jobId)
                        startTime = int(startTime)
                        endTime = int(endTime)
                        if endTime == startTime:
                            # Both, start and end time is an integer so to get sub-second
                            # accuracy we use the ticks reported by Parasol as an approximation.
                            # This isn't documented but what Parasol calls "ticks" is actually a
                            # hundredth of a second. Parasol does the unit conversion early on
                            # after a job finished. Search paraNode.c for ticksToHundreths. We
                            # also cheat a little by always reporting at least one hundredth of a
                            # second.
                            usrTicks = int(usrTicks)
                            sysTicks = int(sysTicks)
                            wallTime = float( max( 1, usrTicks + sysTicks) ) * 0.01
                        else:
                            wallTime = float(endTime - startTime)
                        self.updatedJobsQueue.put((jobId, status, wallTime))
                time.sleep(1)
        except:
            logger.warn("Error occurred while parsing parasol results files.")
            raise
        finally:
            for fileHandle in resultsFileHandles:
                fileHandle.close()

    def shutdown(self):
        self.killBatchJobs(self.getIssuedBatchJobIDs())  # cleanup jobs
        for results in self.resultsFiles.itervalues():
            exitValue = self._runParasol(['-results=' + results, 'clear', 'sick'],
                                         autoRetry=False)[0]
            if exitValue is not None:
                logger.warn("Could not clear sick status of the parasol batch %s" % results)
            exitValue = self._runParasol(['-results=' + results, 'flushResults'],
                                         autoRetry=False)[0]
            if exitValue is not None:
                logger.warn("Could not flush the parasol batch %s" % results)
        self.running = False
        logger.debug('Joining worker thread...')
        self.worker.join()
        logger.debug('... joined worker thread.')
        for results in self.resultsFiles.values():
            os.remove(results)
        os.rmdir(self.parasolResultsDir)
