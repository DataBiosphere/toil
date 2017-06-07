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

from __future__ import absolute_import, print_function
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
import shutil
from threading import Thread

# Python 3 compatibility imports
from six.moves import cPickle

from bd2k.util.expando import Expando, MagicExpando
from toil.common import Toil
from toil.fileStore import FileStore
from toil import logProcessContext
import signal

logger = logging.getLogger( __name__ )




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
    from toil.job import Job
    try:
        import boto
    except ImportError:
        pass
    else:
        # boto is installed, monkey patch it now
        from bd2k.util.ec2.credentials import enable_metadata_credential_caching
        enable_metadata_credential_caching()
    ##########################################
    #Input args
    ##########################################
    
    jobStoreLocator = sys.argv[1]
    jobStoreID = sys.argv[2]
    # we really want a list of job names but the ID will suffice if the job graph can't
    # be loaded. If we can discover the name, we will replace this initial entry
    listOfJobs = [jobStoreID]
    
    ##########################################
    #Load the jobStore/config file
    ##########################################
    
    jobStore = Toil.resumeJobStore(jobStoreLocator)
    config = jobStore.config
    
    ##########################################
    #Create the worker killer, if requested
    ##########################################

    logFileByteReportLimit = config.maxLogFileSize

    if config.badWorker > 0 and random.random() < config.badWorker:
        def badWorker():
            #This will randomly kill the worker process at a random time 
            time.sleep(config.badWorkerFailInterval * random.random())
            os.kill(os.getpid(), signal.SIGKILL) #signal.SIGINT)
            #TODO: FIX OCCASIONAL DEADLOCK WITH SIGINT (tested on single machine)
        t = Thread(target=badWorker)
        # Ideally this would be a daemon thread but that causes an intermittent (but benign)
        # exception similar to the one described here:
        # http://stackoverflow.com/questions/20596918/python-exception-in-thread-thread-1-most-likely-raised-during-interpreter-shutd
        # Our exception is:
        #    Exception in thread Thread-1 (most likely raised during interpreter shutdown):
        #    <type 'exceptions.AttributeError'>: 'NoneType' object has no attribute 'kill'
        # This attribute error is caused by the call os.kill() and apparently unavoidable with a
        # daemon
        t.start()

    ##########################################
    #Load the environment for the jobGraph
    ##########################################
    
    #First load the environment for the jobGraph.
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

    toilWorkflowDir = Toil.getWorkflowDir(config.workflowID, config.workDir)

    ##########################################
    #Setup the temporary directories.
    ##########################################
        
    # Dir to put all this worker's temp files in.
    localWorkerTempDir = tempfile.mkdtemp(dir=toilWorkflowDir)
    os.chmod(localWorkerTempDir, 0o755)

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

    debugging = logging.getLogger().isEnabledFor(logging.DEBUG)
    ##########################################
    #Worker log file trapped from here on in
    ##########################################

    workerFailed = False
    statsDict = MagicExpando()
    statsDict.jobs = []
    statsDict.workers.logsToMaster = []
    blockFn = lambda : True
    cleanCacheFn = lambda x : True
    try:

        #Put a message at the top of the log, just to make sure it's working.
        print("---TOIL WORKER OUTPUT LOG---")
        sys.stdout.flush()
        
        #Log the number of open file descriptors so we can tell if we're leaking
        #them.
        logger.debug("Next available file descriptor: {}".format(
            nextOpenDescriptor()))

        logProcessContext(config)

        ##########################################
        #Load the jobGraph
        ##########################################
        
        jobGraph = jobStore.load(jobStoreID)
        listOfJobs[0] = str(jobGraph)
        logger.debug("Parsed jobGraph")
        
        ##########################################
        #Cleanup from any earlier invocation of the jobGraph
        ##########################################
        
        if jobGraph.command == None:
            # Cleanup jobs already finished
            f = lambda jobs : filter(lambda x : len(x) > 0, map(lambda x :
                                    filter(lambda y : jobStore.exists(y.jobStoreID), x), jobs))
            jobGraph.stack = f(jobGraph.stack)
            jobGraph.services = f(jobGraph.services)
            logger.debug("Cleaned up any references to completed successor jobs")

        #This cleans the old log file which may 
        #have been left if the job is being retried after a job failure.
        oldLogFile = jobGraph.logJobStoreFileID
        if oldLogFile != None:
            jobGraph.logJobStoreFileID = None
            jobStore.update(jobGraph) #Update first, before deleting any files
            jobStore.deleteFile(oldLogFile)

        ##########################################
        # If a checkpoint exists, restart from the checkpoint
        ##########################################

        # The job is a checkpoint, and is being restarted after previously completing
        if jobGraph.checkpoint != None:
            logger.debug("Job is a checkpoint")
            if len(jobGraph.stack) > 0 or len(jobGraph.services) > 0 or jobGraph.command != None:
                if jobGraph.command != None:
                    assert jobGraph.command == jobGraph.checkpoint
                    logger.debug("Checkpoint job already has command set to run")
                else:
                    jobGraph.command = jobGraph.checkpoint

                # Reduce the retry count
                assert jobGraph.remainingRetryCount >= 0
                jobGraph.remainingRetryCount = max(0, jobGraph.remainingRetryCount - 1)

                jobStore.update(jobGraph) # Update immediately to ensure that checkpoint
                # is made before deleting any remaining successors

                if len(jobGraph.stack) > 0 or len(jobGraph.services) > 0:
                    # If the subtree of successors is not complete restart everything
                    logger.debug("Checkpoint job has unfinished successor jobs, deleting the jobs on the stack: %s, services: %s " %
                                 (jobGraph.stack, jobGraph.services))

                    # Delete everything on the stack, as these represent successors to clean
                    # up as we restart the queue
                    def recursiveDelete(jobGraph2):
                        # Recursive walk the stack to delete all remaining jobs
                        for jobs in jobGraph2.stack + jobGraph2.services:
                            for jobNode in jobs:
                                if jobStore.exists(jobNode.jobStoreID):
                                    recursiveDelete(jobStore.load(jobNode.jobStoreID))
                                else:
                                    logger.debug("Job %s has already been deleted", jobNode)
                        if jobGraph2 != jobGraph:
                            logger.debug("Checkpoint is deleting old successor job: %s", jobGraph2.jobStoreID)
                            jobStore.delete(jobGraph2.jobStoreID)
                    recursiveDelete(jobGraph)

                    jobGraph.stack = [ [], [] ] # Initialise the job to mimic the state of a job
                    # that has been previously serialised but which as yet has no successors

                    jobGraph.services = [] # Empty the services

                    # Update the jobStore to avoid doing this twice on failure and make this clean.
                    jobStore.update(jobGraph)

            # Otherwise, the job and successors are done, and we can cleanup stuff we couldn't clean
            # because of the job being a checkpoint
            else:
                logger.debug("The checkpoint jobs seems to have completed okay, removing any checkpoint files to delete.")
                #Delete any remnant files
                map(jobStore.deleteFile, filter(jobStore.fileExists, jobGraph.checkpointFilesToDelete))

        ##########################################
        #Setup the stats, if requested
        ##########################################
        
        if config.stats:
            startTime = time.time()
            startClock = getTotalCpuTime()

        #Make a temporary file directory for the jobGraph
        #localTempDir = makePublicDir(os.path.join(localWorkerTempDir, "localTempDir"))

        startTime = time.time()
        while True:
            ##########################################
            #Run the jobGraph, if there is one
            ##########################################
            
            if jobGraph.command is not None:
                assert jobGraph.command.startswith( "_toil " )
                logger.debug("Got a command to run: %s" % jobGraph.command)
                #Load the job
                job = Job._loadJob(jobGraph.command, jobStore)
                # If it is a checkpoint job, save the command
                if job.checkpoint:
                    jobGraph.checkpoint = jobGraph.command

                # Create a fileStore object for the job
                fileStore = FileStore.createFileStore(jobStore, jobGraph, localWorkerTempDir, blockFn,
                                                      caching=not config.disableCaching)
                with job._executor(jobGraph=jobGraph,
                                   stats=statsDict if config.stats else None,
                                   fileStore=fileStore):
                    with fileStore.open(job):
                        # Get the next block function and list that will contain any messages
                        blockFn = fileStore._blockFn

                        job._runner(jobGraph=jobGraph, jobStore=jobStore, fileStore=fileStore)

                # Accumulate messages from this job & any subsequent chained jobs
                statsDict.workers.logsToMaster += fileStore.loggingMessages

            else:
                #The command may be none, in which case
                #the jobGraph is either a shell ready to be deleted or has
                #been scheduled after a failure to cleanup
                break
            
            if FileStore._terminateEvent.isSet():
                raise RuntimeError("The termination flag is set")

            ##########################################
            #Establish if we can run another jobGraph within the worker
            ##########################################
            
            #If no more jobs to run or services not finished, quit
            if len(jobGraph.stack) == 0 or len(jobGraph.services) > 0 or jobGraph.checkpoint != None:
                logger.debug("Stopping running chain of jobs: length of stack: %s, services: %s, checkpoint: %s",
                             len(jobGraph.stack), len(jobGraph.services), jobGraph.checkpoint != None)
                break
            
            #Get the next set of jobs to run
            jobs = jobGraph.stack[-1]
            assert len(jobs) > 0
            
            #If there are 2 or more jobs to run in parallel we quit
            if len(jobs) >= 2:
                logger.debug("No more jobs can run in series by this worker,"
                            " it's got %i children", len(jobs)-1)
                break
            
            #We check the requirements of the jobGraph to see if we can run it
            #within the current worker
            successorJobNode = jobs[0]
            if successorJobNode.memory > jobGraph.memory:
                logger.debug("We need more memory for the next job, so finishing")
                break
            if successorJobNode.cores > jobGraph.cores:
                logger.debug("We need more cores for the next job, so finishing")
                break
            if successorJobNode.disk > jobGraph.disk:
                logger.debug("We need more disk for the next job, so finishing")
                break
            if successorJobNode.preemptable != jobGraph.preemptable:
                logger.debug("Preemptability is different for the next job, returning to the leader")
                break
            if successorJobNode.predecessorNumber > 1:
                logger.debug("The jobGraph has multiple predecessors, we must return to the leader.")
                break

            # Load the successor jobGraph
            successorJobGraph = jobStore.load(successorJobNode.jobStoreID)

            # add the successor to the list of jobs run
            listOfJobs.append(str(successorJobGraph))

            # Somewhat ugly, but check if job is a checkpoint job and quit if
            # so
            if successorJobGraph.command.startswith( "_toil " ):
                #Load the job
                successorJob = Job._loadJob(successorJobGraph.command, jobStore)

                # Check it is not a checkpoint
                if successorJob.checkpoint:
                    logger.debug("Next job is checkpoint, so finishing")
                    break

            ##########################################
            #We have a single successor job that is not a checkpoint job.
            #We transplant the successor jobGraph command and stack
            #into the current jobGraph object so that it can be run
            #as if it were a command that were part of the current jobGraph.
            #We can then delete the successor jobGraph in the jobStore, as it is
            #wholly incorporated into the current jobGraph.
            ##########################################
            
            #Clone the jobGraph and its stack
            jobGraph = copy.deepcopy(jobGraph)
            
            #Remove the successor jobGraph
            jobGraph.stack.pop()

            #These should all match up
            assert successorJobGraph.memory == successorJobNode.memory
            assert successorJobGraph.cores == successorJobNode.cores
            assert successorJobGraph.predecessorsFinished == set()
            assert successorJobGraph.predecessorNumber == 1
            assert successorJobGraph.command is not None
            assert successorJobGraph.jobStoreID == successorJobNode.jobStoreID

            #Transplant the command and stack to the current jobGraph
            jobGraph.command = successorJobGraph.command
            jobGraph.stack += successorJobGraph.stack
            # include some attributes for better identification of chained jobs in
            # logging output
            jobGraph.unitName = successorJobGraph.unitName
            jobGraph.jobName = successorJobGraph.jobName
            assert jobGraph.memory >= successorJobGraph.memory
            assert jobGraph.cores >= successorJobGraph.cores
            
            #Build a fileStore to update the job
            fileStore = FileStore.createFileStore(jobStore, jobGraph, localWorkerTempDir, blockFn,
                                                  caching=not config.disableCaching)

            #Update blockFn
            blockFn = fileStore._blockFn

            #Add successorJobGraph to those to be deleted
            fileStore.jobsToDelete.add(successorJobGraph.jobStoreID)
            
            #This will update the job once the previous job is done
            fileStore._updateJobWhenDone()            
            
            #Clone the jobGraph and its stack again, so that updates to it do
            #not interfere with this update
            jobGraph = copy.deepcopy(jobGraph)
            
            logger.debug("Starting the next job")
        
        ##########################################
        #Finish up the stats
        ##########################################
        if config.stats:
            totalCPUTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            statsDict.workers.time = str(time.time() - startTime)
            statsDict.workers.clock = str(totalCPUTime - startClock)
            statsDict.workers.memory = str(totalMemoryUsage)

        # log the worker log path here so that if the file is truncated the path can still be found
        logger.info("Worker log can be found at %s. Set --cleanWorkDir to retain this log", localWorkerTempDir)
        logger.info("Finished running the chain of jobs on this node, we ran for a total of %f seconds", time.time() - startTime)
    
    ##########################################
    #Trapping where worker goes wrong
    ##########################################
    except: #Case that something goes wrong in worker
        traceback.print_exc()
        logger.error("Exiting the worker because of a failed job on host %s", socket.gethostname())
        FileStore._terminateEvent.set()
    
    ##########################################
    #Wait for the asynchronous chain of writes/updates to finish
    ########################################## 
       
    blockFn() 
    
    ##########################################
    #All the asynchronous worker/update threads must be finished now, 
    #so safe to test if they completed okay
    ########################################## 
    
    if FileStore._terminateEvent.isSet():
        jobGraph = jobStore.load(jobStoreID)
        jobGraph.setupJobAfterFailure(config)
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
    os.dup2(origStdErr, 2)

    #sys.stdout and sys.stderr don't need to be modified at all. We don't need
    #to call redirectLoggerStreamHandlers since they still log to sys.stderr

    #Close our extra handles to the original standard output and standard error
    #streams, so we don't leak file handles.
    os.close(origStdOut)
    os.close(origStdErr)

    #Now our file handles are in exactly the state they were in before.

    #Copy back the log file to the global dir, if needed
    if workerFailed:
        jobGraph.logJobStoreFileID = jobStore.getEmptyFileStoreID(jobGraph.jobStoreID)
        jobGraph.chainedJobs = listOfJobs
        with jobStore.updateFileStream(jobGraph.logJobStoreFileID) as w:
            with open(tempWorkerLogPath, "r") as f:
                if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit !=0:
                    if logFileByteReportLimit > 0:
                        f.seek(-logFileByteReportLimit, 2)  # seek to last tooBig bytes of file
                    elif logFileByteReportLimit < 0:
                        f.seek(logFileByteReportLimit, 0)  # seek to first tooBig bytes of file
                w.write(f.read())
        jobStore.update(jobGraph)

    elif debugging:  # write log messages
        with open(tempWorkerLogPath, 'r') as logFile:
            if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit != 0:
                if logFileByteReportLimit > 0:
                    logFile.seek(-logFileByteReportLimit, 2)  # seek to last tooBig bytes of file
                elif logFileByteReportLimit < 0:
                    logFile.seek(logFileByteReportLimit, 0)  # seek to first tooBig bytes of file
            logMessages = logFile.read().splitlines()
        statsDict.logs.names = listOfJobs
        statsDict.logs.messages = logMessages

    if (debugging or config.stats or statsDict.workers.logsToMaster) and not workerFailed:  # We have stats/logging to report back
        jobStore.writeStatsAndLogging(json.dumps(statsDict))

    #Remove the temp dir
    cleanUp = config.cleanWorkDir
    if cleanUp == 'always' or (cleanUp == 'onSuccess' and not workerFailed) or (cleanUp == 'onError' and workerFailed):
        shutil.rmtree(localWorkerTempDir)
    
    #This must happen after the log file is done with, else there is no place to put the log
    if (not workerFailed) and jobGraph.command == None and len(jobGraph.stack) == 0 and len(jobGraph.services) == 0:
        # We can now safely get rid of the jobGraph
        jobStore.delete(jobGraph.jobStoreID)
