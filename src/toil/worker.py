# Copyright (C) 2015-2018 Regents of the University of California
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
from future import standard_library
standard_library.install_aliases()
from builtins import str
from builtins import map
from builtins import filter
import os
import sys
import copy
import random
import json
import tempfile
import traceback
import time
import signal
import socket
import logging
import shutil

from toil.lib.compatibility import USING_PYTHON2
from toil.lib.expando import MagicExpando
from toil.common import Toil, safeUnpickleFromStream
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil import logProcessContext
from toil.job import Job
from toil.lib.bioio import configureRootLogger
from toil.lib.bioio import setLogLevel
from toil.lib.bioio import getTotalCpuTime
from toil.lib.bioio import getTotalCpuTimeAndMemoryUsage
from toil.deferred import DeferredFunctionManager
try:
    from toil.cwl.cwltoil import CWL_INTERNAL_JOBS
except ImportError:
    # CWL extra not installed
    CWL_INTERNAL_JOBS = ()

logger = logging.getLogger(__name__)

def nextChainableJobGraph(jobGraph, jobStore):
    """Returns the next chainable jobGraph after this jobGraph if one
    exists, or None if the chain must terminate.
    """
    #If no more jobs to run or services not finished, quit
    if len(jobGraph.stack) == 0 or len(jobGraph.services) > 0 or jobGraph.checkpoint != None:
        logger.debug("Stopping running chain of jobs: length of stack: %s, services: %s, checkpoint: %s",
                     len(jobGraph.stack), len(jobGraph.services), jobGraph.checkpoint != None)
        return None

    #Get the next set of jobs to run
    jobs = jobGraph.stack[-1]
    assert len(jobs) > 0

    #If there are 2 or more jobs to run in parallel we quit
    if len(jobs) >= 2:
        logger.debug("No more jobs can run in series by this worker,"
                    " it's got %i children", len(jobs)-1)
        return None

    #We check the requirements of the jobGraph to see if we can run it
    #within the current worker
    successorJobNode = jobs[0]
    if successorJobNode.memory > jobGraph.memory:
        logger.debug("We need more memory for the next job, so finishing")
        return None
    if successorJobNode.cores > jobGraph.cores:
        logger.debug("We need more cores for the next job, so finishing")
        return None
    if successorJobNode.disk > jobGraph.disk:
        logger.debug("We need more disk for the next job, so finishing")
        return None
    if successorJobNode.preemptable != jobGraph.preemptable:
        logger.debug("Preemptability is different for the next job, returning to the leader")
        return None
    if successorJobNode.predecessorNumber > 1:
        logger.debug("The jobGraph has multiple predecessors, we must return to the leader.")
        return None

    # Load the successor jobGraph
    successorJobGraph = jobStore.load(successorJobNode.jobStoreID)

    # Somewhat ugly, but check if job is a checkpoint job and quit if
    # so
    if successorJobGraph.command.startswith("_toil "):
        #Load the job
        successorJob = Job._loadJob(successorJobGraph.command, jobStore)

        # Check it is not a checkpoint
        if successorJob.checkpoint:
            logger.debug("Next job is checkpoint, so finishing")
            return None

    # Made it through! This job is chainable.
    return successorJobGraph

def workerScript(jobStore, config, jobName, jobStoreID, redirectOutputToLogFile=True):
    """
    Worker process script, runs a job. 
    
    :param str jobName: The "job name" (a user friendly name) of the job to be run
    :param str jobStoreLocator: Specifies the job store to use
    :param str jobStoreID: The job store ID of the job to be run
    
    :return int: 1 if a job failed, or 0 if all jobs succeeded
    """
    
    configureRootLogger()
    setLogLevel(config.logLevel)

    ##########################################
    #Create the worker killer, if requested
    ##########################################

    logFileByteReportLimit = config.maxLogFileSize
    
    if config.badWorker > 0 and random.random() < config.badWorker:
        # We need to kill the process we are currently in, to simulate worker
        # failure. We don't want to just send SIGKILL, because we can't tell
        # that from a legitimate OOM on our CI runner. We're going to send
        # SIGUSR1 so our terminations are distinctive, and then SIGKILL if that
        # didn't stick. We definitely don't want to do this from *within* the
        # process we are trying to kill, so we fork off. TODO: We can still
        # leave the killing code running after the main Toil flow is done, but
        # since it's now in a process instead of a thread, the main Python
        # process won't wait around for its timeout to expire. I think this is
        # better than the old thread-based way where all of Toil would wait
        # around to be killed.
        
        killTarget = os.getpid()
        sleepTime = config.badWorkerFailInterval * random.random()
        if os.fork() == 0:
            # We are the child
            # Let the parent run some amount of time
            time.sleep(sleepTime)
            # Kill it gently
            os.kill(killTarget, signal.SIGUSR1)
            # Wait for that to stick
            time.sleep(0.01)
            try:
                # Kill it harder. Hope the PID hasn't already been reused.
                # If we succeeded the first time, this will OSError
                os.kill(killTarget, signal.SIGKILL)
            except OSError:
                pass
            # Exit without doing any of Toil's cleanup
            os._exit(0)
            
        # We don't need to reap the child. Either it kills us, or we finish
        # before it does. Either way, init will have to clean it up for us.

    ##########################################
    #Load the environment for the jobGraph
    ##########################################
    
    #First load the environment for the jobGraph.
    with jobStore.readSharedFileStream("environment.pickle") as fileHandle:
        environment = safeUnpickleFromStream(fileHandle)
    env_blacklist = {
        "TMPDIR",
        "TMP",
        "HOSTNAME",
        "HOSTTYPE",
        "HOME",
        "LOGNAME",
        "USER",
        "DISPLAY",
        "JAVA_HOME"
    }
    for i in environment:
        if i == "PATH":
            # Handle path specially. Sometimes e.g. leader may not include
            # /bin, but the Toil appliance needs it.
            if i in os.environ and os.environ[i] != '':
                # Use the provided PATH and then the local system's PATH
                os.environ[i] = environment[i] + ':' + os.environ[i]
            else:
                # Use the provided PATH only
                os.environ[i] = environment[i]
        elif i not in env_blacklist:
            os.environ[i] = environment[i]
    # sys.path is used by __import__ to find modules
    if "PYTHONPATH" in environment:
        for e in environment["PYTHONPATH"].split(':'):
            if e != '':
                sys.path.append(e)
                
    toilWorkflowDir = Toil.getLocalWorkflowDir(config.workflowID, config.workDir)

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
    
    # Do we even want to redirect output? Let the config make us not do it.
    redirectOutputToLogFile = redirectOutputToLogFile and not config.disableWorkerOutputCapture

    #What file do we want to point FDs 1 and 2 to?
    tempWorkerLogPath = os.path.join(localWorkerTempDir, "worker_log.txt")

    if redirectOutputToLogFile:
        # Announce that we are redirecting logging, and where it will now go.
        # This is important if we are trying to manually trace a faulty worker invocation.
        logger.info("Redirecting logging to %s", tempWorkerLogPath)
        sys.stdout.flush()
        sys.stderr.flush()
        
        # Save the original stdout and stderr (by opening new file descriptors
        # to the same files)
        origStdOut = os.dup(1)
        origStdErr = os.dup(2)

        # Open the file to send stdout/stderr to.
        logFh = os.open(tempWorkerLogPath, os.O_WRONLY | os.O_CREAT | os.O_APPEND)

        # Replace standard output with a descriptor for the log file
        os.dup2(logFh, 1)

        # Replace standard error with a descriptor for the log file
        os.dup2(logFh, 2)

        # Since we only opened the file once, all the descriptors duped from
        # the original will share offset information, and won't clobber each
        # others' writes. See <http://stackoverflow.com/a/5284108/402891>. This
        # shouldn't matter, since O_APPEND seeks to the end of the file before
        # every write, but maybe there's something odd going on...

        # Close the descriptor we used to open the file
        os.close(logFh)

    debugging = logging.getLogger().isEnabledFor(logging.DEBUG)
    ##########################################
    #Worker log file trapped from here on in
    ##########################################

    jobAttemptFailed = False
    statsDict = MagicExpando()
    statsDict.jobs = []
    statsDict.workers.logsToMaster = []
    blockFn = lambda : True
    listOfJobs = [jobName]
    job = None
    try:

        #Put a message at the top of the log, just to make sure it's working.
        logger.info("---TOIL WORKER OUTPUT LOG---")
        sys.stdout.flush()
        
        logProcessContext(config)
        
        ##########################################
        #Connect to the deferred function system
        ##########################################
        deferredFunctionManager = DeferredFunctionManager(toilWorkflowDir)

        ##########################################
        #Load the jobGraph
        ##########################################
        
        jobGraph = jobStore.load(jobStoreID)
        listOfJobs[0] = str(jobGraph)
        logger.debug("Parsed job wrapper")
        
        ##########################################
        #Cleanup from any earlier invocation of the jobGraph
        ##########################################
        
        if jobGraph.command == None:
            logger.debug("Wrapper has no user job to run.")
            # Cleanup jobs already finished
            f = lambda jobs : [z for z in [[y for y in x if jobStore.exists(y.jobStoreID)] for x in jobs] if len(z) > 0]
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
            # If the checkpoint still has extant jobs in its
            # (flattened) stack and services, its subtree didn't
            # complete properly. We handle the restart of the
            # checkpoint here, removing its previous subtree.
            if len([i for l in jobGraph.stack for i in l]) > 0 or len(jobGraph.services) > 0:
                logger.debug("Checkpoint has failed.")
                # Reduce the retry count
                assert jobGraph.remainingRetryCount >= 0
                jobGraph.remainingRetryCount = max(0, jobGraph.remainingRetryCount - 1)
                jobGraph.restartCheckpoint(jobStore)
            # Otherwise, the job and successors are done, and we can cleanup stuff we couldn't clean
            # because of the job being a checkpoint
            else:
                logger.debug("The checkpoint jobs seems to have completed okay, removing any checkpoint files to delete.")
                #Delete any remnant files
                list(map(jobStore.deleteFile, list(filter(jobStore.fileExists, jobGraph.checkpointFilesToDelete))))

        ##########################################
        #Setup the stats, if requested
        ##########################################
        
        if config.stats:
            startClock = getTotalCpuTime()

        startTime = time.time()
        while True:
            ##########################################
            #Run the jobGraph, if there is one
            ##########################################
            
            if jobGraph.command is not None:
                assert jobGraph.command.startswith("_toil ")
                logger.debug("Got a command to run: %s" % jobGraph.command)
                #Load the job
                job = Job._loadJob(jobGraph.command, jobStore)
                # If it is a checkpoint job, save the command
                if job.checkpoint:
                    jobGraph.checkpoint = jobGraph.command

                # Create a fileStore object for the job
                fileStore = AbstractFileStore.createFileStore(jobStore, jobGraph, localWorkerTempDir, blockFn,
                                                              caching=not config.disableCaching)
                with job._executor(jobGraph=jobGraph,
                                   stats=statsDict if config.stats else None,
                                   fileStore=fileStore):
                    with deferredFunctionManager.open() as defer:
                        with fileStore.open(job):
                            # Get the next block function to wait on committing this job
                            blockFn = fileStore.waitForCommit

                            job._runner(jobGraph=jobGraph, jobStore=jobStore, fileStore=fileStore, defer=defer)

                            # When the job succeeds, start committing files immediately.
                            fileStore.startCommit(jobState=False)      

                # Accumulate messages from this job & any subsequent chained jobs
                statsDict.workers.logsToMaster += fileStore.loggingMessages

            else:
                #The command may be none, in which case
                #the jobGraph is either a shell ready to be deleted or has
                #been scheduled after a failure to cleanup
                logger.debug("No user job to run, so finishing")
                break
            
            if AbstractFileStore._terminateEvent.isSet():
                raise RuntimeError("The termination flag is set")

            ##########################################
            #Establish if we can run another jobGraph within the worker
            ##########################################
            successorJobGraph = nextChainableJobGraph(jobGraph, jobStore)
            if successorJobGraph is None or config.disableChaining:
                # Can't chain any more jobs.
                # TODO: why don't we commit the last job's file store? Won't
                # its async uploads never necessarily finish?
                # If we do call startCommit here it messes with the job
                # itself and Toil thinks the job needs to run again.
                break

            ##########################################
            #We have a single successor job that is not a checkpoint job.
            #We transplant the successor jobGraph command and stack
            #into the current jobGraph object so that it can be run
            #as if it were a command that were part of the current jobGraph.
            #We can then delete the successor jobGraph in the jobStore, as it is
            #wholly incorporated into the current jobGraph.
            ##########################################

            # add the successor to the list of jobs run
            listOfJobs.append(str(successorJobGraph))

            #Clone the jobGraph and its stack
            jobGraph = copy.deepcopy(jobGraph)
            
            #Remove the successor jobGraph
            jobGraph.stack.pop()

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
            fileStore = AbstractFileStore.createFileStore(jobStore, jobGraph, localWorkerTempDir, blockFn,
                                                          caching=not config.disableCaching)

            #Update blockFn
            blockFn = fileStore.waitForCommit

            #Add successorJobGraph to those to be deleted
            fileStore.jobsToDelete.add(successorJobGraph.jobStoreID)
            
            #This will update the job once the previous job is done
            fileStore.startCommit(jobState=True)            
            
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
        if redirectOutputToLogFile:
            logger.info("Worker log can be found at %s. Set --cleanWorkDir to retain this log", localWorkerTempDir)
        
        logger.info("Finished running the chain of jobs on this node, we ran for a total of %f seconds", time.time() - startTime)
    
    ##########################################
    #Trapping where worker goes wrong
    ##########################################
    except: #Case that something goes wrong in worker
        traceback.print_exc()
        logger.error("Exiting the worker because of a failed job on host %s", socket.gethostname())
        AbstractFileStore._terminateEvent.set()
    
    ##########################################
    #Wait for the asynchronous chain of writes/updates to finish
    ########################################## 
       
    blockFn() 
    
    ##########################################
    #All the asynchronous worker/update threads must be finished now, 
    #so safe to test if they completed okay
    ########################################## 
    
    if AbstractFileStore._terminateEvent.isSet():
        jobGraph = jobStore.load(jobStoreID)
        jobGraph.setupJobAfterFailure(config)
        jobAttemptFailed = True
        if job and jobGraph.remainingRetryCount == 0:
            job._succeeded = False

    ##########################################
    #Cleanup
    ##########################################

    # Close the worker logging
    # Flush at the Python level
    sys.stdout.flush()
    sys.stderr.flush()
    if redirectOutputToLogFile:
        # Flush at the OS level
        os.fsync(1)
        os.fsync(2)

        # Close redirected stdout and replace with the original standard output.
        os.dup2(origStdOut, 1)

        # Close redirected stderr and replace with the original standard error.
        os.dup2(origStdErr, 2)

        # sys.stdout and sys.stderr don't need to be modified at all. We don't
        # need to call redirectLoggerStreamHandlers since they still log to
        # sys.stderr

        # Close our extra handles to the original standard output and standard
        # error streams, so we don't leak file handles.
        os.close(origStdOut)
        os.close(origStdErr)

    # Now our file handles are in exactly the state they were in before.

    # Copy back the log file to the global dir, if needed.
    # Note that we work with bytes instead of characters so we can seek
    # relative to the end (since Python won't decode Unicode backward, or even
    # interpret seek offsets in characters for us). TODO: We may get invalid or
    # just different Unicode by breaking up a character at the boundary!
    if jobAttemptFailed and redirectOutputToLogFile:
        jobGraph.logJobStoreFileID = jobStore.getEmptyFileStoreID(jobGraph.jobStoreID, cleanup=True)
        jobGraph.chainedJobs = listOfJobs
        with jobStore.updateFileStream(jobGraph.logJobStoreFileID) as w:
            with open(tempWorkerLogPath, 'rb') as f:
                if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit !=0:
                    if logFileByteReportLimit > 0:
                        f.seek(-logFileByteReportLimit, 2)  # seek to last tooBig bytes of file
                    elif logFileByteReportLimit < 0:
                        f.seek(logFileByteReportLimit, 0)  # seek to first tooBig bytes of file
                # Dump the possibly-invalid-Unicode bytes into the log file
                w.write(f.read()) # TODO load file using a buffer
        jobStore.update(jobGraph)

    elif ((debugging or (config.writeLogsFromAllJobs and not jobName.startswith(CWL_INTERNAL_JOBS)))
          and redirectOutputToLogFile):  # write log messages
        with open(tempWorkerLogPath, 'rb') as logFile:
            if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit != 0:
                if logFileByteReportLimit > 0:
                    logFile.seek(-logFileByteReportLimit, 2)  # seek to last tooBig bytes of file
                elif logFileByteReportLimit < 0:
                    logFile.seek(logFileByteReportLimit, 0)  # seek to first tooBig bytes of file
            # Make sure lines are Unicode so they can be JSON serialized as part of the dict.
            # We may have damaged the Unicode text by cutting it at an arbitrary byte so we drop bad characters.
            logMessages = [line.decode('utf-8', 'skip') for line in logFile.read().splitlines()]
        statsDict.logs.names = listOfJobs
        statsDict.logs.messages = logMessages

    if (debugging or config.stats or statsDict.workers.logsToMaster) and not jobAttemptFailed:  # We have stats/logging to report back
        if USING_PYTHON2:
            jobStore.writeStatsAndLogging(json.dumps(statsDict, ensure_ascii=True))
        else:
            jobStore.writeStatsAndLogging(json.dumps(statsDict, ensure_ascii=True).encode())

    #Remove the temp dir
    cleanUp = config.cleanWorkDir
    if cleanUp == 'always' or (cleanUp == 'onSuccess' and not jobAttemptFailed) or (cleanUp == 'onError' and jobAttemptFailed):
        shutil.rmtree(localWorkerTempDir)
    
    #This must happen after the log file is done with, else there is no place to put the log
    if (not jobAttemptFailed) and jobGraph.command == None and len(jobGraph.stack) == 0 and len(jobGraph.services) == 0:
        # We can now safely get rid of the jobGraph
        jobStore.delete(jobGraph.jobStoreID)
        
    if jobAttemptFailed:
        return 1
    else:
        return 0

def main(argv=None):
    if argv is None:
        argv = sys.argv

    # Do a little argument validation, in case someone tries to run us manually.
    if len(argv) < 4:
        if len(argv) < 1:
            sys.stderr.write("Error: Toil worker invoked without its own name\n")
            sys.exit(1)
        else:
            sys.stderr.write("Error: usage: %s JOB_NAME JOB_STORE_LOCATOR JOB_STORE_ID\n" % argv[0])
            sys.exit(1)

    # Parse input args
    jobName = argv[1]
    jobStoreLocator = argv[2]
    jobStoreID = argv[3]

    ##########################################
    #Load the jobStore/config file
    ##########################################

    jobStore = Toil.resumeJobStore(jobStoreLocator)
    config = jobStore.config

    # Call the worker and exit with its return value
    sys.exit(workerScript(jobStore, config, jobName, jobStoreID))
