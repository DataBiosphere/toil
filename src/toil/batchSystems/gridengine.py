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
from __future__ import absolute_import
import logging
import os
import subprocess
import time
from Queue import Queue
from threading import Thread

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
            return str(val) + unit
        else:
            return str(val)

    def byteVal(self):
        if self.unit == 'B':
            return self.val
        elif self.unit == 'K':
            return self.val * 1024
        elif self.unit == 'M':
            return self.val * 1048576
        elif self.unit == 'G':
            return self.val * 1073741824

    def __cmp__(self, other):
        return cmp(self.bytes, other.bytes)

def prepareQsub(cpu, mem):
    qsubline = ["qsub","-b","y","-terse","-j" ,"y", "-cwd", "-o", "/dev/null", "-e", "/dev/null", "-v",
                     "LD_LIBRARY_PATH=%s" % os.environ["LD_LIBRARY_PATH"]]
    reqline = list()
    if cpu is not None:
        reqline.append("p="+str(cpu))
    if mem is not None:
        reqline.append("vf="+str(mem/ 1024)+"K")
        reqline.append("h_vmem="+str(mem/ 1024)+"K")
    if len(reqline) > 0:
        qsubline.extend(["-hard","-l", ",".join(reqline)])
    return qsubline

def qsub(qsubline):
    logger.debug("**"+" ".join(qsubline))
    process = subprocess.Popen(qsubline, stdout=subprocess.PIPE)
    result = int(process.stdout.readline().strip().split('.')[0])
    logger.debug("Got the job id: %s" % (str(result)))
    return result

def getjobexitcode(sgeJobID):
        job, task = sgeJobID
        args = ["qacct", "-j", str(job)]
        if task is not None:
             args.extend(["-t", str(task)])

        process = subprocess.Popen(args, stdout = subprocess.PIPE,stderr = subprocess.STDOUT)
        for line in process.stdout:
            if line.startswith("failed") and int(line.split()[1]) == 1:
                return 1
            elif line.startswith("exit_status"):
                return int(line.split()[1])
        return None

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
        self.sgeJobIDs = dict()

    def getRunningJobIDs(self):
        times = {}
        currentjobs = dict((self.sgeJobIDs[x], x) for x in self.runningJobs)
        process = subprocess.Popen(["qstat"], stdout = subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        for currline in stdout.split('\n'):
            items = curline.strip().split()
            if ((len(items) > 9 and (items[0],items[9]) in currentjobs) or (items[0], None) in currentjobs) and items[4] == 'r':
                jobstart = " ".join(items[5:7])
                jobstart = time.mktime(time.strptime(jobstart,"%m/%d/%Y %H:%M:%S"))
                times[currentjobs[(items[0],items[9])]] = time.time() - jobstart 

        return times

    def getSgeID(self, jobID):
        if not jobID in self.sgeJobIDs:
             RuntimeError("Unknown jobID, could not be converted")

        (job,task) = self.sgeJobIDs[jobID]
        if task is None:
             return str(job)
        else:
             return str(job) + "." + str(task)

    def forgetJob(self, jobID):
        self.runningJobs.remove(jobID)
        del self.allocatedCpus[jobID]
        del self.sgeJobIDs[jobID]

    def killJobs(self):
        # Load hit list:
        killList = list()
        while not self.killQueue.empty():
            killList.append(self.killQueue.get())

        # Do the dirty job
        for jobID in list(killList):
            if jobID in self.runningJobs:
                process = subprocess.Popen(["qdel", self.getSgeID(jobID)])
            else:
                if jobID in self.waitingJobs:
                    self.waitingJobs.remove(jobID)
                self.killedJobsQueue.put(jobID)
                killList.remove(jobID)

        # Wait to confirm the kill
        while len(killList) > 0:
            for jobID in list(killList):
                if getjobexitcode(self.sgeJobIDs[jobID]) is not None:
                    self.killedJobsQueue.put(jobID)
                    killList.remove(jobID)
                    self.forgetJob(jobID)

            if len(killList) > 0:
                logger.warn("Tried to kill some jobs, but something happened and they are still going, "
                            "so I'll try again")
                time.sleep(5)

    def createJobs(self):
        # Load new job ids:
        while not self.newJobsQueue.empty():
            self.waitingJobs.append(self.newJobsQueue.get())

        # Launch jobs as necessary:
        while len(self.waitingJobs) > 0 and sum(self.allocatedCpus.values()) < int(self.boss.maxCpus):
            jobID, cpu, memory, command = self.waitingJobs.pop(0)
            qsubline = prepareQsub(cpu, memory) + [command]
            sgeJobID = qsub(qsubline)
            self.sgeJobIDs[jobID] = (sgeJobID, None)
            self.runningJobs.add(jobID)
            self.allocatedCpus[jobID] = cpu

    def checkOnJobs(self):
        for jobID in list(self.runningJobs):
            exit = getjobexitcode(self.sgeJobIDs[jobID])
            if exit is not None:
                self.updatedJobsQueue.put((jobID, exit))
                self.forgetJob(jobID)
    
    def run(self):
        while True:
            self.killJobs()
            self.createJobs()
            self.checkOnJobs()
            time.sleep(10)

class GridengineBatchSystem(AbstractBatchSystem):
    """The interface for gridengine.
    """
    
    def __init__(self, config, maxCpus, maxMemory):
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory) #Call the parent constructor
        self.gridengineResultsFile = getParasolResultsFileName(config.jobStore)
        #Reset the job queue and results (initially, we do this again once we've killed the jobs)
        self.gridengineResultsFileHandle = open(self.gridengineResultsFile, 'w')
        self.gridengineResultsFileHandle.close() #We lose any previous state in this file, and ensure the files existence
        self.currentjobs = set()
        self.obtainSystemConstants()
        self.nextJobID = 0

        self.newJobsQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.killQueue = Queue()
        self.killedJobsQueue = Queue()
        self.worker = Worker(self.newJobsQueue, self.updatedJobsQueue, self.killQueue, self.killedJobsQueue, self)
        self.worker.setDaemon(True)
        self.worker.start()
        
    def __des__(self):
        #Closes the file handle associated with the results file.
        self.gridengineResultsFileHandle.close() #Close the results file, cos were done.

    def issueBatchJob(self, command, memory, cpu):
        self.checkResourceRequest(memory, cpu)
        jobID = self.nextJobID
        self.nextJobID += 1

        self.currentjobs.add(jobID)
        self.newJobsQueue.put((jobID, cpu, memory, command))
        logger.debug("Issued the job command: %s with job id: %s " % (command, str(jobID)))
        return jobID

    def killBatchJobs(self, jobIDs):
        """Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        for jobID in jobIDs:
            self.killQueue.put(jobID)

        killList = set(jobIDs)
        while len(killList) > 0:
            while True:
                i = self.getFromQueueSafely(self.killedJobsQueue, maxWait)
                if i is not None:
                    killList.remove(jobID)
                    self.currentjobs.remove(jobID)
                else:
                    break

        if len(killList) > 0:
            time.sleep(5)
    
    def getIssuedBatchJobIDs(self):
        """Gets the list of jobs issued to SGE.
        """
        return list(self.currentjobs)
    
    def getRunningBatchJobIDs(self):
        return self.worker.getRunningJobIDs()
    
    def getUpdatedBatchJob(self, maxWait):
        i = self.getFromQueueSafely(self.updatedJobsQueue, maxWait)
        if i == None:
            return None
        jobID, retcode = i
        self.updatedJobsQueue.task_done()
        self.currentjobs.remove(jobID)
        return i
    
    def getWaitDuration(self):
        """We give parasol a second to catch its breath (in seconds)
        """
        return 0.0

    @classmethod
    def getRescueBatchJobFrequency(cls):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. We allow this every 10 minutes..
        """
        return 1800 #Half an hour

    def obtainSystemConstants(self):
        p = subprocess.Popen(["qhost"], stdout = subprocess.PIPE,stderr = subprocess.STDOUT)

        line = p.stdout.readline()
        items = line.strip().split()
        num_columns = len(items)
        cpu_index = None
        mem_index = None        
        for i in range(num_columns):
                if items[i] == 'NCPU':
                        cpu_index = i
                elif items[i] == 'MEMTOT':
                        mem_index = i

        if cpu_index is None or mem_index is None:
                RuntimeError("qhost command does not return NCPU or MEMTOT columns")

        p.stdout.readline()

        self.maxCPU = 0
        self.maxMEM = MemoryString("0")
        for line in p.stdout:
                items = line.strip().split()
                if len(items) < num_columns:
                        RuntimeError("qhost output has a varying number of columns")
                if items[cpu_index] != '-' and items[cpu_index] > self.maxCPU:
                        self.maxCPU = items[cpu_index]
                if items[mem_index] != '-' and MemoryString(items[mem_index]) > self.maxMEM:
                        self.maxMEM = MemoryString(items[mem_index])

        if self.maxCPU is 0 or self.maxMEM is 0:
                RuntimeError("qhost returns null NCPU or MEMTOT info")
                
        
def main():
    pass

def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
