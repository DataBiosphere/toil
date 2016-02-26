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
import os
import sys
import copy
import random
import json

import tempfile
import traceback
import time
import socket
import logging
import cPickle
import shutil
from threading import Thread
from bd2k.util.expando import Expando, MagicExpando
import signal

logger = logging.getLogger( __name__ )


logFileByteReportLimit = 50000


def nextOpenDescriptor():
    """Gets the number of the next available file descriptor.
    """
    descriptor = os.open("/dev/null", os.O_RDONLY)
    os.close(descriptor)
    return descriptor

class AsyncJobStoreWrite:
    def __init__(self, jobStore):
        pass
    
    def writeFile(self, filePath):
        pass
    
    def writeFileStream(self):
        pass
    
    def blockUntilSync(self):
        pass
    
def main():
    logging.basicConfig()

    ##########################################
    #Import necessary modules 
    ##########################################
    
    # This is assuming that worker.py is at a path ending in "/toil/worker.py".
    sourcePath = os.path.dirname(os.path.dirname(__file__))
    if sourcePath not in sys.path:
        sys.path.append(sourcePath)
    
    #Now we can import all the necessary functions
    from toil.lib.bioio import setLogLevel
    from toil.lib.bioio import getTotalCpuTime
    from toil.lib.bioio import getTotalCpuTimeAndMemoryUsage
    from toil.lib.bioio import makePublicDir
    from toil.lib.bioio import system
    from toil.common import loadJobStore
    from toil.job import Job
    
    ########################################## 
    #Input args
    ##########################################
    
    jobStoreString = sys.argv[1]
    jobStoreID = sys.argv[2]
    
    ##########################################
    #Load the jobStore/config file
    ##########################################
    
    jobStore = loadJobStore(jobStoreString)
    config = jobStore.config
    
    ##########################################
    #Create the worker killer, if requested
    ##########################################

    if config.badWorker > 0 and random.random() < config.badWorker:
        def badWorker():
            #This will randomly kill the worker process at a random time 
            time.sleep(config.badWorkerFailInterval * random.random())
            os.kill(os.getpid(), signal.SIGKILL) #signal.SIGINT)
            #TODO: FIX OCCASIONAL DEADLOCK WITH SIGINT (tested on single machine)
        t = Thread(target=badWorker)
        t.daemon = True
        t.start()

    ##########################################
    #Load the environment for the jobWrapper
    ##########################################
    
    #First load the environment for the jobWrapper.
    with jobStore.readSharedFileStream("environment.pickle") as fileHandle:
        environment = cPickle.load(fileHandle)
    for i in environment:
        if i not in ("TMPDIR", "TMP", "HOSTNAME", "HOSTTYPE"):
            os.environ[i] = environment[i]
    # sys.path is used by __import__ to find modules
    if "PYTHONPATH" in environment:
        for e in environment["PYTHONPATH"].split(':'):
            if e != '':
                sys.path.append(e)

    setLogLevel(config.logLevel)

    tempRootDir = config.workDir
    if tempRootDir is not None and not os.path.exists(tempRootDir):
        raise RuntimeError("The temporary directory specified by workDir: %s does not exist" % tempRootDir)

    ##########################################
    #Setup the temporary directories.
    ##########################################
        
    #Dir to put all the temp files in. If tempRootDir is None, tempdir looks at environment variables to determine
    # where to put the tempDir.
    localWorkerTempDir = tempfile.mkdtemp(dir=tempRootDir)
    os.chmod(localWorkerTempDir, 0755)

    ##########################################
    #Setup the logging
    ##########################################

    #This is mildly tricky because we don't just want to
    #redirect stdout and stderr for this Python process; we want to redirect it
    #for this process and all children. Consequently, we can't just replace
    #sys.stdout and sys.stderr; we need to mess with the underlying OS-level
    #file descriptors. See <http://stackoverflow.com/a/11632982/402891>
    
    #When we start, standard input is file descriptor 0, standard output is
    #file descriptor 1, and standard error is file descriptor 2.

    #What file do we want to point FDs 1 and 2 to?
    tempWorkerLogPath = os.path.join(localWorkerTempDir, "worker_log.txt")
    
    #Save the original stdout and stderr (by opening new file descriptors to the
    #same files)
    origStdOut = os.dup(1)
    origStdErr = os.dup(2)

    #Open the file to send stdout/stderr to.
    logFh = os.open(tempWorkerLogPath, os.O_WRONLY | os.O_CREAT | os.O_APPEND)

    #Replace standard output with a descriptor for the log file
    os.dup2(logFh, 1)
    
    #Replace standard error with a descriptor for the log file
    os.dup2(logFh, 2)
    
    #Since we only opened the file once, all the descriptors duped from the
    #original will share offset information, and won't clobber each others'
    #writes. See <http://stackoverflow.com/a/5284108/402891>. This shouldn't
    #matter, since O_APPEND seeks to the end of the file before every write, but
    #maybe there's something odd going on...
    
    #Close the descriptor we used to open the file
    os.close(logFh)

    for handler in list(logger.handlers): #Remove old handlers
        logger.removeHandler(handler)
    
    #Add the new handler. The sys.stderr stream has been redirected by swapping
    #the file descriptor out from under it.
    logger.addHandler(logging.StreamHandler(sys.stderr))

    debugging = logging.getLogger().isEnabledFor(logging.DEBUG)
    ##########################################
    #Worker log file trapped from here on in
    ##########################################

    workerFailed = False
    statsDict = MagicExpando()
    statsDict.jobs = []
    messages = []
    blockFn = lambda : True
    cleanCacheFn = lambda x : True
    try:

        #Put a message at the top of the log, just to make sure it's working.
        print "---TOIL WORKER OUTPUT LOG---"
        sys.stdout.flush()
        
        #Log the number of open file descriptors so we can tell if we're leaking
        #them.
        logger.debug("Next available file descriptor: {}".format(
            nextOpenDescriptor()))
    
        ##########################################
        #Load the jobWrapper
        ##########################################
        
        jobWrapper = jobStore.load(jobStoreID)
        logger.debug("Parsed jobWrapper")
        
        ##########################################
        #Cleanup from any earlier invocation of the jobWrapper
        ##########################################
        
        if jobWrapper.command == None:
            while len(jobWrapper.stack) > 0:
                jobs = jobWrapper.stack[-1]
                #If the jobs still exist they have not been run, so break
                if jobStore.exists(jobs[0][0]):
                    break
                #However, if they are gone then we can remove them from the stack.
                #This is the only way to flush successors that have previously been run
                #, as jobs are, as far as possible, read only in the leader.
                jobWrapper.stack.pop()
                
        #This cleans the old log file which may 
        #have been left if the jobWrapper is being retried after a jobWrapper failure.
        oldLogFile = jobWrapper.logJobStoreFileID
        jobWrapper.logJobStoreFileID = None
        jobStore.update(jobWrapper) #Update first, before deleting the file
        if oldLogFile != None:
            jobStore.delete(oldLogFile)
            
        #Make a temporary file directory for the jobWrapper
        localTempDir = makePublicDir(os.path.join(localWorkerTempDir, "localTempDir"))

        ##########################################
        #Setup the stats, if requested
        ##########################################
        
        if config.stats:
            startTime = time.time()
            startClock = getTotalCpuTime()

        startTime = time.time() 
        while True:
            ##########################################
            #Run the jobWrapper, if there is one
            ##########################################
            
            if jobWrapper.command != None:
                if jobWrapper.command.startswith( "_toil " ):
                    #Load the job
                    job = Job._loadJob(jobWrapper.command, jobStore)
                    
                    #Cleanup the cache from the previous job
                    cleanCacheFn(job.effectiveRequirements(jobStore.config).cache)
                    
                    #Create a fileStore object for the job
                    fileStore = Job.FileStore(jobStore, jobWrapper, localTempDir, 
                                              blockFn)
                    #Get the next block function and list that will contain any messages
                    blockFn = fileStore._blockFn
                    messages = fileStore.loggingMessages

                    job._execute(jobWrapper=jobWrapper,
                                           stats=statsDict if config.stats else None,
                                           localTempDir=localTempDir,
                                           jobStore=jobStore,
                                           fileStore=fileStore)

                    #Set the clean cache function
                    cleanCacheFn = fileStore._cleanLocalTempDir
                    
                else: #Is another command (running outside of jobs may be deprecated)
                    #Cleanup the cache from the previous job
                    cleanCacheFn(0)
                    
                    system(jobWrapper.command)
                    #Set a dummy clean cache fn
                    cleanCacheFn = lambda x : None
            else:
                #The command may be none, in which case
                #the jobWrapper is either a shell ready to be deleted or has 
                #been scheduled after a failure to cleanup
                break
            
            if Job.FileStore._terminateEvent.isSet():
                raise RuntimeError("The termination flag is set")

            ##########################################
            #Establish if we can run another jobWrapper within the worker
            ##########################################
            
            #No more jobs to run so quit
            if len(jobWrapper.stack) == 0:
                break
            
            #Get the next set of jobs to run
            jobs = jobWrapper.stack[-1]
            assert len(jobs) > 0
            
            #If there are 2 or more jobs to run in parallel we quit
            if len(jobs) >= 2:
                logger.debug("No more jobs can run in series by this worker,"
                            " it's got %i children", len(jobs)-1)
                break
            
            #We check the requirements of the jobWrapper to see if we can run it
            #within the current worker
            successorJobStoreID, successorMemory, successorCores, successorsDisk, successorPredecessorID = jobs[0]
            if successorMemory > jobWrapper.memory:
                logger.debug("We need more memory for the next jobWrapper, so finishing")
                break
            if successorCores > jobWrapper.cores:
                logger.debug("We need more cores for the next jobWrapper, so finishing")
                break
            if successorsDisk > jobWrapper.disk:
                logger.debug("We need more disk for the next jobWrapper, so finishing")
                break
            if successorPredecessorID != None: 
                logger.debug("The jobWrapper has multiple predecessors, we must return to the leader.")
                break
          
            ##########################################
            #We have a single successor jobWrapper.
            #We load the successor jobWrapper and transplant its command and stack
            #into the current jobWrapper so that it can be run
            #as if it were a command that were part of the current jobWrapper.
            #We can then delete the successor jobWrapper in the jobStore, as it is
            #wholly incorporated into the current jobWrapper.
            ##########################################
            
            #Clone the jobWrapper and its stack
            jobWrapper = copy.deepcopy(jobWrapper)
            
            #Remove the successor jobWrapper
            jobWrapper.stack.pop()
            
            #Load the successor jobWrapper
            successorJob = jobStore.load(successorJobStoreID)
            #These should all match up
            assert successorJob.memory == successorMemory
            assert successorJob.cores == successorCores
            assert successorJob.predecessorsFinished == set()
            assert successorJob.predecessorNumber == 1
            assert successorJob.command != None
            assert successorJobStoreID == successorJob.jobStoreID
            
            #Transplant the command and stack to the current jobWrapper
            jobWrapper.command = successorJob.command
            jobWrapper.stack += successorJob.stack
            assert jobWrapper.memory >= successorJob.memory
            assert jobWrapper.cores >= successorJob.cores
            
            #Build a fileStore to update the job
            fileStore = Job.FileStore(jobStore, jobWrapper, localTempDir, blockFn)
            
            #Update blockFn
            blockFn = fileStore._blockFn
            
            #Add successorJob to those to be deleted
            fileStore.jobsToDelete.add(successorJob.jobStoreID)
            
            #This will update the job once the previous job is done
            fileStore._updateJobWhenDone()            
            
            #Clone the jobWrapper and its stack again, so that updates to it do 
            #not interfere with this update
            jobWrapper = copy.deepcopy(jobWrapper)
            
            logger.debug("Starting the next jobWrapper")
        
        ##########################################
        #Finish up the stats
        ##########################################
        if config.stats:
            totalCPUTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            statsDict.workers.time = str(time.time() - startTime)
            statsDict.workers.clock = str(totalCPUTime - startClock)
            statsDict.workers.memory = str(totalMemoryUsage)

		# logToMaster messages should be always be passed
        statsDict.workers.logsToMaster = messages

        # log the worker log path here so that if the file is truncated the path can still be found
        logger.info("Worker log can be found at %s. Set --cleanWorkDir to retain this log", localTempDir)
        logger.info("Finished running the chain of jobs on this node, we ran for a total of %f seconds", time.time() - startTime)
    
    ##########################################
    #Trapping where worker goes wrong
    ##########################################
    except: #Case that something goes wrong in worker
        traceback.print_exc()
        logger.error("Exiting the worker because of a failed jobWrapper on host %s", socket.gethostname())
        Job.FileStore._terminateEvent.set()
    
    ##########################################
    #Wait for the asynchronous chain of writes/updates to finish
    ########################################## 
       
    blockFn() 
    
    ##########################################
    #All the asynchronous worker/update threads must be finished now, 
    #so safe to test if they completed okay
    ########################################## 
    
    if Job.FileStore._terminateEvent.isSet():
        jobWrapper = jobStore.load(jobStoreID)
        jobWrapper.setupJobAfterFailure(config)
        workerFailed = True

    ##########################################
    #Cleanup
    ##########################################
    
    #Close the worker logging
    #Flush at the Python level
    sys.stdout.flush()
    sys.stderr.flush()
    #Flush at the OS level
    os.fsync(1)
    os.fsync(2)
    
    #Close redirected stdout and replace with the original standard output.
    os.dup2(origStdOut, 1)
    
    #Close redirected stderr and replace with the original standard error.
    os.dup2(origStdOut, 2)
    
    #sys.stdout and sys.stderr don't need to be modified at all. We don't need
    #to call redirectLoggerStreamHandlers since they still log to sys.stderr
    
    #Close our extra handles to the original standard output and standard error
    #streams, so we don't leak file handles.
    os.close(origStdOut)
    os.close(origStdErr)
    
    #Now our file handles are in exactly the state they were in before.
    
    #Copy back the log file to the global dir, if needed
    if workerFailed:
        jobWrapper.logJobStoreFileID = jobStore.getEmptyFileStoreID(jobWrapper.jobStoreID)
        with jobStore.updateFileStream(jobWrapper.logJobStoreFileID) as w:
            with open(tempWorkerLogPath, "r") as f:
                if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit:
                    f.seek(-logFileByteReportLimit, 2)  # seek to last tooBig bytes of file
                w.write(f.read())
        jobStore.update(jobWrapper)

    elif debugging:  # write log messages
        with open(tempWorkerLogPath, 'r') as logFile:
            if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit:
                logFile.seek(-logFileByteReportLimit, 2)  # seek to last tooBig bytes of file
            logMessages = logFile.read().splitlines()
        statsDict.logs = [Expando(jobStoreID=jobStoreID, text=logMessage) for logMessage in logMessages]

    if (debugging or config.stats or messages) and not workerFailed:  # We have stats/logging to report back
        jobStore.writeStatsAndLogging(json.dumps(statsDict))

    #Remove the temp dir
    cleanUp = config.cleanWorkDir
    if cleanUp == 'always' or (cleanUp == 'onSuccess' and not workerFailed) or (cleanUp == 'onError' and workerFailed):
        shutil.rmtree(localWorkerTempDir)
    
    #This must happen after the log file is done with, else there is no place to put the log
    if (not workerFailed) and jobWrapper.command == None and len(jobWrapper.stack) == 0:
        #We can now safely get rid of the jobWrapper
        jobStore.delete(jobWrapper.jobStoreID)
