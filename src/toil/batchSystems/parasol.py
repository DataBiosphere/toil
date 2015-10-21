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

from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem
from toil.lib.bioio import getTempFile

logger = logging.getLogger( __name__ )

def popenParasolCommand(command, runUntilSuccessful=True):
    """Issues a parasol command using popen to capture the output.
    If the command fails then it will try pinging parasol until it gets a response.
    When it gets a response it will recursively call the issue parasol command, repeating this pattern 
    for a maximum of N times. 
    The final exit value will reflect this.
    """
    while True:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=sys.stderr, bufsize=-1)
        output, nothing = process.communicate() #process.stdout.read().strip()
        exitValue = process.wait()
        if exitValue == 0:
            return 0, output.split("\n")
        message = "The following parasol command failed (exit value %s): %s" % (exitValue, command)
        if not runUntilSuccessful:
            logger.error(message)
            return exitValue, None
        else:
            logger.warn(message)
        time.sleep(10)
        logger.warn("Waited for a few seconds, will try again")



class ParasolBatchSystem(AbstractBatchSystem):
    """The interface for Parasol.
    """
    def __init__(self, config, maxCores, maxMemory, maxDisk):
        AbstractBatchSystem.__init__(self, config, maxCores, maxMemory, maxDisk) #Call the parent constructor
        if maxMemory != sys.maxint:
            logger.warn("A max memory has been specified for the parasol batch system class of %i, but currently "
                        "this batchsystem interface does not support such limiting" % maxMemory)
        #Keep the name of the results file for the pstat2 command..
        self.parasolCommand = config.parasolCommand
        self.parasolResultsDir = tempfile.mkdtemp(dir=config.jobStore)

        self.queuePattern = re.compile("q\s+([0-9]+)")
        self.runningPattern = re.compile("r\s+([0-9]+)\s+[\S]+\s+[\S]+\s+([0-9]+)\s+[\S]+")

        logger.info("Going to sleep for a few seconds to kill any existing jobs")
        time.sleep(5) #Give batch system a second to sort itself out.
        logger.info("Removed any old jobs from the queue")

        #In Parasol, each results file corresponds to a separate batch, and all
        #jobs in a batch have the same cpu and memory requirements. The keys to this
        #dictionary are the (cpu, memory) tuples for each batch. A new batch
        #is created whenever a job has a new unique combination of cpu and memory
        #requirements.
        self.resultsFiles = dict()
        self.maxBatches = config.maxParasolBatches

        #Allows the worker process to send back the IDs of
        #jobs that have finished, so the batch system can
        #decrease its used cpus counter
        self.cpuUsageQueue = Queue()

        #Also stores finished job IDs, but is read
        #by getUpdatedJobIDs().
        self.updatedJobsQueue = Queue()

        #Use this to stop the worker when shutting down
        self.running = True

        self.worker = Thread(target=self.updatedJobWorker, args=())
        self.worker.start()
        self.usedCpus = 0
        self.jobIDsToCpu = {}

        #Set of jobs that have been issued but aren't known to
        #have finished or been killed yet. Jobs that end
        #by themselves are removed in getUpdatedJob, and jobs
        #that are killed are removed in killBatchJobs.
        self.runningJobs = set()

    def issueBatchJob(self, command, memory, cores, disk):
        """Issues parasol with job commands.
        """
        self.checkResourceRequest(memory, cores, disk)

        MiB = 1 << 20
        truncatedMemory = (memory/MiB) * MiB
        #Look for a batch for jobs with these resource requirements, with
        #the memory rounded down to the nearest megabyte. Rounding down
        #meams the new job can't ever decrease the memory requirements
        #of jobs already in the batch.
        if (truncatedMemory, cores) in self.resultsFiles.keys():
            results = self.resultsFiles[(truncatedMemory, cores)]
        else:
            results = getTempFile(rootDir=self.parasolResultsDir)
            self.resultsFiles[(truncatedMemory, cores)] = results
        if len(self.resultsFiles.values()) > self.maxBatches:
            raise RuntimeError("Number of parasol batches exceeded the limit of %i" % self.maxBatches)

        pattern = re.compile("your job ([0-9]+).*")
        parasolCommand = "%s -verbose -ram=%i -cpu=%i -results=%s add job '%s'" % (self.parasolCommand, memory, cores, results, command)
        #Deal with the cpus
        self.usedCpus += cores
        while True: #Process finished results with no wait
            try:
               jobID = self.cpuUsageQueue.get_nowait()
            except Empty:
                break
            if jobID in self.jobIDsToCpu.keys():
                self.usedCpus -= self.jobIDsToCpu.pop(jobID)
            assert self.usedCpus >= 0
            self.cpuUsageQueue.task_done()
        while self.usedCpus > self.maxCores: #If we are still waiting
            jobID = self.cpuUsageQueue.get()
            if jobID in self.jobIDsToCpu.keys():
                self.usedCpus -= self.jobIDsToCpu.pop(jobID)
            assert self.usedCpus >= 0
            self.cpuUsageQueue.task_done()
        #Now keep going
        while True:
            #time.sleep(0.1) #Sleep to let parasol catch up #Apparently unnecessary
            line = popenParasolCommand(parasolCommand)[1][0]
            match = pattern.match(line)
            if match != None: #This is because parasol add job will return success, even if the job was not properly issued!
                break
            else:
                logger.info("We failed to properly add the job, we will try again after a sleep")
                time.sleep(5)
        jobID = int(match.group(1))
        self.jobIDsToCpu[jobID] = cores
        self.runningJobs.add(jobID)
        logger.debug("Got the parasol job id: %s from line: %s" % (jobID, line))
        logger.debug("Issued the job command: %s with (parasol) job id: %i " % (parasolCommand, jobID))
        return jobID

    def killBatchJobs(self, jobIDs):
        """Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        while True:
            for jobID in jobIDs:
                if jobID in self.runningJobs:
                    self.runningJobs.remove(jobID)
                exitValue = popenParasolCommand("%s remove job %i" % (self.parasolCommand, jobID), runUntilSuccessful=False)[0]
                logger.info("Tried to remove jobID: %i, with exit value: %i" % (jobID, exitValue))
            runningJobs = self.getIssuedBatchJobIDs()
            if set(jobIDs).difference(set(runningJobs)) == set(jobIDs):
                break
            time.sleep(5)
            logger.warn("Tried to kill some jobs, but something happened and they are still going, so I'll try again")
        #update the CPU usage, because killed jobs aren't
        #written to the results file.
        for jobID in jobIDs:
            if jobID in self.jobIDsToCpu.keys():
                self.usedCpus -= self.jobIDsToCpu.pop(jobID)

    def getJobIDsForResultsFile(self, resultsFile):
        """Get all queued and running jobs for a results file.
        """
        jobIDs = []
        for line in popenParasolCommand("%s -results=%s pstat2" % (self.parasolCommand, resultsFile))[1]:
            runningJobMatch = self.runningPattern.match(line)
            queuedJobMatch = self.queuePattern.match(line)
            jobID = None
            if runningJobMatch:
                jobID = runningJobMatch.group(1)
            elif queuedJobMatch:
                jobID = queuedJobMatch.group(1)
            else:
                continue
            jobIDs.append(int(jobID))
        return set(jobIDs)

    def getIssuedBatchJobIDs(self):
        """Gets the list of jobs issued to parasol in all results files, but 
        not including jobs created by other users.
        """
        issuedJobs = set()
        for resultsFile in self.resultsFiles.values():
            issuedJobs.update(self.getJobIDsForResultsFile(resultsFile))

        return list(issuedJobs)

    def getRunningBatchJobIDs(self):
        """Returns map of running jobIDs and the time they have been running.
        """
        #Example lines..
        #r 5410186 benedictpaten worker 1247029663 localhost
        #r 5410324 benedictpaten worker 1247030076 localhost
        runningJobs = {}
        issuedJobs = self.getIssuedBatchJobIDs()
        for line in popenParasolCommand("%s pstat2 " % self.parasolCommand)[1]:
            if line != '':
                match = self.runningPattern.match(line)
                if match != None:
                    jobID = int(match.group(1))
                    startTime = int(match.group(2))
                    if jobID in issuedJobs: #It's one of our jobs
                        runningJobs[jobID] = time.time() - startTime
        return runningJobs

    def getUpdatedBatchJob(self, maxWait):
        try:
            info = self.updatedJobsQueue.get(timeout=maxWait)
        except Empty:
            return None
        jobID, exitValue = info
        self.updatedJobsQueue.task_done()
        if jobID not in self.runningJobs:
            #we tried to kill this job, but it ended by itself
            #instead, so skip it.
            return self.getUpdatedJob(maxWait)
        else:
            self.runningJobs.remove(jobID)
        return (jobID, exitValue)

    @classmethod
    def getRescueBatchJobFrequency(cls):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. 
        """
        return 5400 #Once every 90 minutes
    def updatedJobWorker(self):
        """We use the parasol results to update the status of jobs, adding them
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

        plus you finally have the command name..
        """
        resultsFiles = set()
        resultsFileHandles = []
        try:
            while self.running:
                #Look for any new results files that have been created, and open them
                newResultsFiles = set(os.listdir(self.parasolResultsDir)).difference(resultsFiles)
                for newFile in newResultsFiles:
                    resultsFiles.add(newFile)
                    newFilePath = os.path.join(self.parasolResultsDir, newFile)
                    resultsFileHandles.append(open(newFilePath, 'r'))

                for fileHandle in resultsFileHandles:
                    line = fileHandle.readline()
                    if line:
                        assert line[-1] == '\n'
                        results = line.split()
                        result = int(results[0])
                        jobID = int(results[2])
                        self.cpuUsageQueue.put(jobID)
                        self.updatedJobsQueue.put((jobID, result))
                time.sleep(0.01) #Go to sleep to avoid churning
        except:
            logger.warn("Error occurred while parsing parasol results files.")
            raise
        finally:
            for fileHandle in resultsFileHandles:
                fileHandle.close()


    def shutdown(self):
        self.killBatchJobs(self.getIssuedBatchJobIDs()) #cleanup jobs
        for results in self.resultsFiles.values():
            exitValue = popenParasolCommand("%s -results=%s clear sick" % (self.parasolCommand, results), False)[0]
            if exitValue is not None:
                logger.warn("Could not clear sick status of the parasol batch %s" % results)
            exitValue = popenParasolCommand("%s -results=%s flushResults" % (self.parasolCommand, results), False)[0]
            if exitValue is not None:
                logger.warn("Could not flush the parasol batch %s" % results)
        self.running = False
        self.worker.join()
        for results in self.resultsFiles.values():
            os.remove(results)
        os.rmdir(self.parasolResultsDir)

def main():
    pass

def _test():
    import doctest
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
