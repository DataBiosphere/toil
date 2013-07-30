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
import sys
import subprocess
import time

from Queue import Empty
from sonLib.bioio import logger
from multiprocessing import Process
from multiprocessing import JoinableQueue as Queue

#from threading import Thread
#from Queue import Queue, Empty

from sonLib.bioio import logger
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.src.master import getParasolResultsFileName

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
        logger.critical("The following parasol command failed (exit value %s): %s" % (exitValue, command))
        if not runUntilSuccessful:
            return exitValue, None
        time.sleep(10)
        logger.critical("Waited for a few seconds, will try again")

def getUpdatedJob(parasolResultsFile, outputQueue1, outputQueue2):
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
    parasolResultsFileHandle = open(parasolResultsFile, 'r')
    while True:
        line = parasolResultsFileHandle.readline()
        if line != '':
            results = line.split()
            result = int(results[0])
            jobID = int(results[2])
            outputQueue1.put(jobID)
            outputQueue2.put((jobID, result))
        else:
            time.sleep(0.01) #Go to sleep to avoid churning

class ParasolBatchSystem(AbstractBatchSystem):
    """The interface for Parasol.
    """
    def __init__(self, config, maxCpus, maxMemory):
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory) #Call the parent constructor
        if maxMemory != sys.maxint:
            logger.critical("A max memory has been specified for the parasol batch system class of %i, but currently this batchsystem interface does not support such limiting" % maxMemory)
        #Keep the name of the results file for the pstat2 command..
        self.parasolCommand = config.attrib["parasol_command"]
        self.parasolResultsFile = getParasolResultsFileName(config.attrib["job_tree"])
        #Reset the job queue and results (initially, we do this again once we've killed the jobs)
        self.queuePattern = re.compile("q\s+([0-9]+)")
        self.runningPattern = re.compile("r\s+([0-9]+)\s+[\S]+\s+[\S]+\s+([0-9]+)\s+[\S]+")
        self.killJobs(self.getIssuedJobIDs()) #Kill any jobs on the current stack
        logger.info("Going to sleep for a few seconds to kill any existing jobs")
        time.sleep(5) #Give batch system a second to sort itself out.
        logger.info("Removed any old jobs from the queue")
        #Reset the job queue and results
        exitValue = popenParasolCommand("%s -results=%s clear sick" % (self.parasolCommand, self.parasolResultsFile), False)[0]
        if exitValue != None:
            logger.critical("Could not clear sick status of the parasol batch %s" % self.parasolResultsFile)
        exitValue = popenParasolCommand("%s -results=%s flushResults" % (self.parasolCommand, self.parasolResultsFile), False)[0]
        if exitValue != None:
            logger.critical("Could not flush the parasol batch %s" % self.parasolResultsFile)
        open(self.parasolResultsFile, 'w').close()
        logger.info("Reset the results queue")
        #Stuff to allow max cpus to be work
        self.outputQueue1 = Queue()
        self.outputQueue2 = Queue()
        #worker = Thread(target=getUpdatedJob, args=(self.parasolResultsFileHandle, self.outputQueue1, self.outputQueue2))
        #worker.setDaemon(True)
        worker = Process(target=getUpdatedJob, args=(self.parasolResultsFile, self.outputQueue1, self.outputQueue2))
        worker.daemon = True
        worker.start()
        self.usedCpus = 0
        self.jobIDsToCpu = {}
         
    def issueJob(self, command, memory, cpu):
        """Issues parasol with job commands.
        """
        self.checkResourceRequest(memory, cpu)
        pattern = re.compile("your job ([0-9]+).*")
        parasolCommand = "%s -verbose -ram=%i -cpu=%i -results=%s add job '%s'" % (self.parasolCommand, memory, cpu, self.parasolResultsFile, command)
        #Deal with the cpus
        self.usedCpus += cpu
        while True: #Process finished results with no wait
            try:
               jobID = self.outputQueue1.get_nowait()
               self.usedCpus -= self.jobIDsToCpu.pop(jobID)
               assert self.usedCpus >= 0
               self.outputQueue1.task_done()
            except Empty:
                break
        while self.usedCpus > self.maxCpus: #If we are still waiting
            self.usedCpus -= self.jobIDsToCpu.pop(self.outputQueue1.get())
            assert self.usedCpus >= 0
            self.outputQueue1.task_done()
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
        self.jobIDsToCpu[jobID] = cpu
        logger.debug("Got the parasol job id: %s from line: %s" % (jobID, line))
        logger.debug("Issued the job command: %s with (parasol) job id: %i " % (parasolCommand, jobID))
        return jobID
    
    def killJobs(self, jobIDs):
        """Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        while True:
            for jobID in jobIDs:
                exitValue = popenParasolCommand("%s remove job %i" % (self.parasolCommand, jobID), runUntilSuccessful=False)[0]
                logger.info("Tried to remove jobID: %i, with exit value: %i" % (jobID, exitValue))
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
        issuedJobs = set()
        for line in popenParasolCommand("%s -extended list jobs" % self.parasolCommand)[1]:
            if line != '':
                tokens = line.split()
                if tokens[-1] == self.parasolResultsFile:
                    jobID = int(tokens[0])
                    issuedJobs.add(jobID)
        return list(issuedJobs)
    
    def getRunningJobIDs(self):
        """Returns map of running jobIDs and the time they have been running.
        """
        #Example lines..
        #r 5410186 benedictpaten jobTreeSlave 1247029663 localhost
        #r 5410324 benedictpaten jobTreeSlave 1247030076 localhost
        runningJobs = {}
        issuedJobs = self.getIssuedJobIDs()
        for line in popenParasolCommand("%s -results=%s pstat2 " % (self.parasolCommand, self.parasolResultsFile))[1]:
            if line != '':
                match = self.runningPattern.match(line)
                if match != None:
                    jobID = int(match.group(1))
                    startTime = int(match.group(2))
                    if jobID in issuedJobs: #It's one of our jobs
                        runningJobs[jobID] = time.time() - startTime
        return runningJobs
    
    def getUpdatedJob(self, maxWait):
        jobID = self.getFromQueueSafely(self.outputQueue2, maxWait)
        if jobID != None:
            self.outputQueue2.task_done()
        return jobID
    
    def getRescueJobFrequency(self):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. 
        """
        return 5400 #Once every 90 minutes
        
def main():
    pass

def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
