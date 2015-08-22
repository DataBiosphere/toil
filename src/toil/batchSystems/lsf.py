#!/usr/bin/env python

#Copyright (C) 2013 by Thomas Keane (tk2@sanger.ac.uk)
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
from __future__ import absolute_import
import logging
import subprocess
import time
from Queue import Queue, Empty
from threading import Thread
from datetime import date

from toil.batchSystems.abstractBatchSystem import AbstractBatchSystem
from toil.batchSystems.parasol import getParasolResultsFileName


logger = logging.getLogger( __name__ )


class MemoryString:
    def __init__(self, string):
        if string[-1] == 'K' or string[-1] == 'M' or string[-1] == 'G':
            self.unit = string[-1]
            self.val = float(string[:-1])
        else:
            self.unit = 'B'
            self.val = float(string)
        self.bytes = self.byteVal()

    def __str__(self):
        if self.unit != 'B':
            return str(self.val) + self.unit
        else:
            return str(self.val)

    def byteVal(self):
        if self.unit == 'B':
            return self.val
        elif self.unit == 'K':
            return self.val * 1000
        elif self.unit == 'M':
            return self.val * 1000000
        elif self.unit == 'G':
            return self.val * 1000000000

    def __cmp__(self, other):
        return cmp(self.bytes, other.bytes)

def prepareBsub(cpu, mem):
    mem = '' if mem is None else '-R "select[type==X86_64 && mem > ' + str(int(mem/ 1000000)) + '] rusage[mem=' + str(int(mem/ 1000000)) + ']" -M' + str(int(mem/ 1000000)) + '000'
    cpu = '' if cpu is None else '-n ' + str(int(cpu))
    bsubline = ["bsub", mem, cpu,"-cwd", ".", "-o", "/dev/null", "-e", "/dev/null"]
    return bsubline

def bsub(bsubline):
    process = subprocess.Popen(" ".join(bsubline), shell=True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
    liney = process.stdout.readline()
    logger.info("BSUB: " + liney)
    result = int(liney.strip().split()[1].strip('<>'))
    logger.debug("Got the job id: %s" % (str(result)))
    return result

def getjobexitcode(lsfJobID):
        job, task = lsfJobID
        
        #first try bjobs to find out job state
        args = ["bjobs", "-l", str(job)]
        logger.info("Checking job exit code for job via bjobs: " + str(job))
        process = subprocess.Popen(" ".join(args), shell=True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        started = 0
        for line in process.stdout:
            if line.find("Done successfully") > -1:
                logger.info("bjobs detected job completed for job: " + str(job))
                return 0
            elif line.find("Completed <exit>") > -1:
                logger.info("bjobs detected job failed for job: " + str(job))
                return 1
            elif line.find("New job is waiting for scheduling") > -1:
                logger.info("bjobs detected job pending scheduling for job: " + str(job))
                return None
            elif line.find("PENDING REASONS") > -1:
                logger.info("bjobs detected job pending for job: " + str(job))
                return None
            elif line.find("Started on ") > -1:
                started = 1
        
        if started == 1:
            logger.info("bjobs detected job started but not completed: " + str(job))
            return None

        #if not found in bjobs, then try bacct (slower than bjobs)
        logger.info("bjobs failed to detect job - trying bacct: " + str(job))
        
        args = ["bacct", "-l", str(job)]
        logger.info("Checking job exit code for job via bacct:" + str(job))
        process = subprocess.Popen(" ".join(args), shell=True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        for line in process.stdout:
            if line.find("Completed <done>") > -1:
                logger.info("Detected job completed for job: " + str(job))
                return 0
            elif line.find("Completed <exit>") > -1:
                logger.info("Detected job failed for job: " + str(job))
                return 1
        logger.info("Cant determine exit code for job or job still running: " + str(job))
        return None

class Worker(Thread):
    def __init__(self, newJobsQueue, updatedJobsQueue, boss):
        Thread.__init__(self)
        self.newJobsQueue = newJobsQueue
        self.updatedJobsQueue = updatedJobsQueue
        self.currentjobs = list()
        self.runningjobs = set()
        self.boss = boss
        
    def run(self):
        while True:
            # Load new job ids:
            while not self.newJobsQueue.empty():
                self.currentjobs.append(self.newJobsQueue.get())

            # Launch jobs as necessary:
            while len(self.currentjobs) > 0:
                jobID, bsubline = self.currentjobs.pop()
                lsfJobID = bsub(bsubline)
                self.boss.jobIDs[(lsfJobID, None)] = jobID
                self.boss.lsfJobIDs[jobID] = (lsfJobID, None)
                self.runningjobs.add((lsfJobID, None))

            # Test known job list
            for lsfJobID in list(self.runningjobs):
                exit = getjobexitcode(lsfJobID)
                if exit is not None:
                    self.updatedJobsQueue.put((lsfJobID, exit))
                    self.runningjobs.remove(lsfJobID)

            time.sleep(10)

class LSFBatchSystem(AbstractBatchSystem):
    """The interface for running jobs on lsf, runs all the jobs you
    give it as they come in, but in parallel.
    """
    def __init__(self, config, maxCores, maxMemory):
        AbstractBatchSystem.__init__(self, config, maxCores, maxMemory) #Call the parent constructor
        self.lsfResultsFile = getParasolResultsFileName(config.jobStore)
        #Reset the job queue and results (initially, we do this again once we've killed the jobs)
        self.lsfResultsFileHandle = open(self.lsfResultsFile, 'w')
        self.lsfResultsFileHandle.close() #We lose any previous state in this file, and ensure the files existence
        self.currentjobs = set()
        self.obtainSystemConstants()
        self.jobIDs = dict()
        self.lsfJobIDs = dict()
        self.nextJobID = 0

        self.newJobsQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.worker = Worker(self.newJobsQueue, self.updatedJobsQueue, self)
        self.worker.setDaemon(True)
        self.worker.start()

    def __des__(self):
        #Closes the file handle associated with the results file.
        self.lsfResultsFileHandle.close() #Close the results file, cos were done.        
    
    def issueBatchJob(self, command, memory, cores):
        jobID = self.nextJobID
        self.nextJobID += 1
        self.currentjobs.add(jobID)
        bsubline = prepareBsub(cores, memory) + [command]
        self.newJobsQueue.put((jobID, bsubline))
        logger.info("Issued the job command: %s with job id: %s " % (command, str(jobID)))
        return jobID
        
    def getLsfID(self, jobID):
        if not jobID in self.lsfJobIDs:
             RuntimeError("Unknown jobID, could not be converted")

        (job,task) = self.lsfJobIDs[jobID]
        if task is None:
             return str(job)
        else:
             return str(job) + "." + str(task)
    
    def killBatchJobs(self, jobIDs):
        """Kills the given job IDs.
        """
        for jobID in jobIDs:
            logger.info("DEL: " + str(self.getLsfID(jobID)))
            self.currentjobs.remove(jobID)
            process = subprocess.Popen(["bkill", self.getLsfID(jobID)])
            del self.jobIDs[self.lsfJobIDs[jobID]]
            del self.lsfJobIDs[jobID]

        toKill = set(jobIDs)
        while len(toKill) > 0:
            for jobID in list(toKill):
                if getjobexitcode(self.lsfJobIDs[jobID]) is not None:
                    toKill.remove(jobID)

            if len(toKill) > 0:
                logger.warn("Tried to kill some jobs, but something happened and they are still going, "
                             "so I'll try again")
                time.sleep(5)
    
    def getIssuedBatchJobIDs(self):
        """A list of jobs (as jobIDs) currently issued (may be running, or maybe 
        just waiting).
        """
        return self.currentjobs
    
    def getRunningBatchJobIDs(self):
        """Gets a map of jobs (as jobIDs) currently running (not just waiting) 
        and a how long they have been running for (in seconds).
        """
        times = {}
        currentjobs = set(self.lsfJobIDs[x] for x in self.getIssuedBatchJobIDs())
        process = subprocess.Popen(["bjobs"], stdout = subprocess.PIPE)
        
        for currline in process.stdout:
            items = curline.strip().split()
            if (len(items) > 9 and (items[0]) in currentjobs) and items[2] == 'RUN':
                jobstart = "/".join(items[7:9]) + '/' + str(date.today().year)
                jobstart = jobstart + ' ' + items[9]
                jobstart = time.mktime(time.strptime(jobstart,"%b/%d/%Y %H:%M"))
                jobstart = time.mktime(time.strptime(jobstart,"%m/%d/%Y %H:%M:%S"))
                times[self.jobIDs[(items[0])]] = time.time() - jobstart
        return times
    
    def getUpdatedBatchJob(self, maxWait):
        i = None
        try:
            sgeJobID, retcode = self.updatedJobsQueue.get(timeout=maxWait)
            self.updatedJobsQueue.task_done()
            i = (self.jobIDs[sgeJobID], retcode)
            self.currentjobs -= set([self.jobIDs[sgeJobID]])
        except Empty:
            pass

        return i
    
    def getWaitDuration(self):
        """We give parasol a second to catch its breath (in seconds)
        """
        #return 0.0
        return 15

    @classmethod
    def getRescueBatchJobFrequency(cls):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. We allow this every 10 minutes..
        """
        return 1800

    def obtainSystemConstants(self):
        p = subprocess.Popen(["lshosts"], stdout = subprocess.PIPE, stderr = subprocess.STDOUT)

        line = p.stdout.readline()
        items = line.strip().split()
        num_columns = len(items)
        cpu_index = None
        mem_index = None        
        for i in range(num_columns): 
                if items[i] == 'ncpus':
                        cpu_index = i
                elif items[i] == 'maxmem':
                        mem_index = i

        if cpu_index is None or mem_index is None:
                RuntimeError("lshosts command does not return ncpus or maxmem columns")

        p.stdout.readline()

        self.maxCPU = 0
        self.maxMEM = MemoryString("0")
        for line in p.stdout:
                items = line.strip().split()
                if len(items) < num_columns:
                        RuntimeError("lshosts output has a varying number of columns")
                if items[cpu_index] != '-' and items[cpu_index] > self.maxCPU:
                        self.maxCPU = items[cpu_index]
                if items[mem_index] != '-' and MemoryString(items[mem_index]) > self.maxMEM:
                        self.maxMEM = MemoryString(items[mem_index])

        if self.maxCPU is 0 or self.maxMEM is 0:
                RuntimeError("lshosts returns null ncpus or maxmem info")
        logger.info("Got the maxCPU: %s" % (self.maxMEM))

def main():
    pass

def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
