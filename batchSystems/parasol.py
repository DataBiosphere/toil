#!/usr/bin/env python

#Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.

import os 
import re
import subprocess
import time
from threading import Thread
from Queue import Queue, Empty

from sonLib.bioio import logger
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem

def popenParasolCommand(command, tmpFileForStdOut, runUntilSuccessful=True):
    """Issues a parasol command using popen to capture the output.
    If the command fails then it will try pinging parasol until it gets a response.
    When it gets a response it will recursively call the issue parasol command, repeating this pattern 
    for a maximum of N times. 
    The final exit value will reflect this.
    """
    while True:
        fileHandle = open(tmpFileForStdOut, 'w')
        process = subprocess.Popen(command, shell=True, stdout=fileHandle)
        sts = os.waitpid(process.pid, 0)
        fileHandle.close()
        i = sts[1]
        if i != 0 and runUntilSuccessful:
            logger.critical("The following parasol command failed: %s" % command)
            time.sleep(10)
            logger.critical("Waited for a few seconds, will try again")
        else:
            return i
        
def getUpdatedJob(parasolResultsFileHandle, outputQueue1, outputQueue2):
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
    while True:
        line = parasolResultsFileHandle.readline()
        if line != '':
            results = line.split()
            if line[-1] == '\n':
                line = line[:-1]
            logger.debug("Parasol completed a job, this is what we got: %s" % line)
            result = int(results[0])
            jobID = int(results[2])
            outputQueue1.put(jobID)
            outputQueue2.put((jobID, result))
        else:
            time.sleep(0.01) #Go to sleep to avoid churning

class ParasolBatchSystem(AbstractBatchSystem):
    """The interface for Parasol.
    """
    
    def __init__(self, config):
        AbstractBatchSystem.__init__(self, config) #Call the parent constructor
        #Keep the name of the results file for the pstat2 command..
        self.parasolResultsFile = config.attrib["results_file"]
        #Reset the job queue and results (initially, we do this again once we've killed the jobs)
        self.parasolResultsFileHandle = open(self.parasolResultsFile, 'w')
        self.parasolResultsFileHandle.close() #We lose any previous state in this file, and ensure the files existence    
        self.queuePattern = re.compile("q\s+([0-9]+)")
        self.runningPattern = re.compile("r\s+([0-9]+)\s+[\S]+\s+[\S]+\s+([0-9]+)\s+[\S]+")
        #The scratch file
        self.scratchFile = self.config.attrib["scratch_file"]
        self.killJobs(self.getIssuedJobIDs()) #Kill any jobs on the current stack
        logger.info("Going to sleep for a few seconds to kill any existing jobs")
        time.sleep(5) #Give batch system a second to sort itself out.
        logger.info("Removed any old jobs from the queue")
        #Reset the job queue and results
        self.parasolResultsFileHandle = open(self.parasolResultsFile, 'w')
        self.parasolResultsFileHandle.close() #We lose any previous state in this file, and ensure the files existence
        self.parasolResultsFileHandle = open(self.parasolResultsFile, 'r')
        logger.info("Reset the results queue")
        #Stuff to allow max cpus to be work
        self.outputQueue1 = Queue()
        self.outputQueue2 = Queue()
        worker = Thread(target=processUpdatedJob, args=(self.outputQueue1, self.outputQueue2, self.parasolResultsFileHandle))
        worker.setDaemon(True)
        worker.start()
        self.usedCpus = 0
        self.maxCpus = int(config.attrib["max_jobs"])
        self.jobIDsToCpu = {}
         
    def issueJob(self, command, memory, cpu, logFile):
        """Issues parasol with job commands.
        """
        assert memory != None
        assert cpu != None
        assert logFile != None
        pattern = re.compile("your job ([0-9]+).*")
        parasolCommand = "parasol -verbose -ram=%i -cpu=%i -results=%s add job '%s'" % (memory, cpu, self.parasolResultsFile, command)
        self.usedCpus += cpu
        while self.usedCpus + cpu > self.maxCpus:
            self.usedCpus -= self.jobIDsToCpu.pop(self.outputQueue1.get())
            assert self.usedCpus >= 0
            self.outputQueue1.task_done()
        while True:
            #time.sleep(0.1) #Sleep to let parasol catch up #Apparently unnecessary
            popenParasolCommand(parasolCommand, self.scratchFile)
            fileHandle = open(self.scratchFile, 'r')
            line = fileHandle.readline()
            fileHandle.close()
            match = pattern.match(line)
            if match != None: #This is because parasol add job will return success, even if the job was not properly issued!
                break
            else:
                logger.info("We failed to properly add the job, we will try again after a sleep")
                time.sleep(5)
        jobID = int(match.group(1))
        self.jobIDsToCpu[jobID] = cpu
        logger.debug("Got the job id: %s from line: %s" % (jobID, line))
        logger.debug("Issued the job command: %s with job id: %i " % (parasolCommand, jobID))
        return jobID
    
    def killJobs(self, jobIDs):
        """Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        while True:
            for jobID in jobIDs:
                i = popenParasolCommand("parasol remove job %i" % jobID, tmpFileForStdOut=self.scratchFile, runUntilSuccessful=None)
                logger.info("Tried to remove jobID: %i, with exit value: %i" % (jobID, i))
            runningJobs = self.getIssuedJobIDs()
            if set(jobIDs).difference(set(runningJobs)) == set(jobIDs):
                return
            time.sleep(5)
            logger.critical("Tried to kill some jobs, but something happened and they are still going, so I'll try again")
    
    def getIssuedJobIDs(self):
        """Gets the list of jobs issued to parasol.
        """
        #Example issued job, first field is jobID, last is the results file
        #31816891 localhost  benedictpaten 2009/07/23 10:54:09 python ~/Desktop/out.txt
        popenParasolCommand("parasol -extended list jobs", self.config.attrib["scratch_file"])
        fileHandle = open(self.config.attrib["scratch_file"], 'r')
        line = fileHandle.readline()
        issuedJobs = set()
        while line != '':
            tokens = line.split()
            if tokens[-1] == self.config.attrib["results_file"]:
                jobID = int(tokens[0])
                issuedJobs.add(jobID)
            line = fileHandle.readline()
        fileHandle.close()
        return issuedJobs
    
    def getRunningJobIDs(self):
        """Returns map of runnig jobIDs and the time they have been running.
        """
        popenParasolCommand("parasol -results=%s pstat2 " % self.parasolResultsFile, self.scratchFile)
        fileHandle = open(self.scratchFile, 'r')
        line = fileHandle.readline()
        #Example lines..
        #r 5410186 benedictpaten jobTreeSlave 1247029663 localhost
        #r 5410324 benedictpaten jobTreeSlave 1247030076 localhost
        runningJobs = {}
        issuedJobs = self.getIssuedJobIDs()
        while line != '':
            match = self.runningPattern.match(line)
            if match != None:
                jobID = int(match.group(1))
                startTime = int(match.group(2))
                if jobID in issuedJobs: #It's one of our jobs
                    runningJobs[jobID] = time.time() - startTime
            line = fileHandle.readline()
        fileHandle.close()
        return runningJobs
    
    def getUpdatedJob(self, maxWait):
        i = self.outputQueue2.get(timeout=maxWait)
        if i != None:
            self.outputQueue2.task_done()
        return i
    
    def getRescueJobFrequency(self):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. We allow this every 10 minutes..
        """
        return 1800 #Half an hour
        
def main():
    pass

def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
