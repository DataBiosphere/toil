#!/usr/bin/env python

import os
import random
import subprocess
import time
from threading import Thread
from Queue import Queue

from workflow.jobTree.lib.bioio import logger
 
from workflow.jobTree.lib.abstractBatchSystem import AbstractBatchSystem
from workflow.jobTree.lib.bioio import getTempFile
from workflow.jobTree.lib.bioio import system

class Worker(Thread):
    def __init__(self, inputQueue, outputQueue):
        Thread.__init__(self)
        self.inputQueue = inputQueue
        self.outputQueue = outputQueue
        
    def run(self):
        while True:
            command, logFile = self.inputQueue.get()
            #fnull = open(os.devnull, 'w') #Pipe the output to dev/null (it is caught by the slave and will be reported if there is an error)
            tempLogFile = getTempFile()
            fileHandle = open(tempLogFile, 'w')
            process = subprocess.Popen(command, shell=True, stdout = fileHandle, stderr = fileHandle)
            sts = os.waitpid(process.pid, 0)
            fileHandle.close()
            #fnull.close()
            if os.path.exists(tempLogFile):
                system("mv %s %s" % (tempLogFile, logFile))
            self.outputQueue.put((command, sts[1]))
            self.inputQueue.task_done()
        
class SingleMachineBatchSystem(AbstractBatchSystem):
    """The interface for running jobs on a single machine, runs all the jobs you
    give it as they come in, but in parallel.
    """
    def __init__(self, config, worker=Worker):
        AbstractBatchSystem.__init__(self, config) #Call the parent constructor
        self.jobIndex = 0
        self.jobs = {}
        self.maxThreads = int(config.attrib["max_threads"])
        assert self.maxThreads >= 1
        self.worker = worker
        
    def issueJobs(self, commands):
        """Runs the jobs right away.
        """
        inputQueue = Queue()
        outputQueue = Queue()
        
        for i in xrange(self.maxThreads): #Setup the threads
            worker = self.worker(inputQueue, outputQueue)
            worker.setDaemon(True)
            worker.start()
        
        for command, memory, cpu, logFile in commands: #Add the commands to the queue
            assert memory != None
            assert cpu != None
            assert logFile != None
            inputQueue.put((command, logFile))
        
        inputQueue.join()
        
        #Now get stuff from the queue
        jobs2 = {}
        
        def processOutput():
            while True:
                command, exitValue = outputQueue.get()
                self.jobs[self.jobIndex] = exitValue
                jobs2[self.jobIndex] = command
                self.jobIndex += 1
                logger.debug("Ran the command: %s with exit value: %i" % (command, exitValue))
                outputQueue.task_done()
        processOutput = Thread(target=processOutput)
        processOutput.setDaemon(True)
        processOutput.start()
        
        outputQueue.join()
        
        return jobs2
    
    def killJobs(self, jobIDs):
        """As jobs are already run, this method has no effect.
        """
        pass
    
    def getIssuedJobIDs(self):
        """Just returns all the jobs that have been run, but not yet returned as updated.
        """
        return self.jobs.keys()
    
    def getRunningJobIDs(self):
        """As no jobs are running in parallel, just returns an empty map.
        """
        return {}
    
    def getUpdatedJobs(self):
        """Returns a map of the run jobs and the return value of their processes.
        """
        runJobs = self.jobs
        self.jobs = {}  
        return runJobs
    
    def getWaitDuration(self):
        """As the main process is serial, we can make this zero.
        """
        return 0.0
    
    def getRescueJobFrequency(self):
        """This should not really occur, wihtout an error. To exercise the 
        system we allow it every minute. 
        """
        return 60  

class BadWorker(Thread):
    """This is used to test what happens if we fail and restart jobs
    """
    def __init__(self, inputQueue, outputQueue):
        Thread.__init__(self)
        self.inputQueue = inputQueue
        self.outputQueue = outputQueue
        
    def run(self):
        while True:
            command, logFile = self.inputQueue.get()
            assert logFile != None
            fnull = open(os.devnull, 'w') #Pipe the output to dev/null (it is caught by the slave and will be reported if there is an error)
            #Run to first calculate the runtime..
            process = subprocess.Popen(command, shell=True, stdout = fnull, stderr = fnull)
            if random.choice((False, True)):
                time.sleep(random.random()*5) #Sleep up to 5 seconds before trying to kill it
                process.kill()
            process.wait()
            fnull.close()
            self.outputQueue.put((command, process.returncode))
            self.inputQueue.task_done()
