# Copyright (C) 2015-2021 Regents of the University of California
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
import argparse
import base64
import copy
import json
import logging
import os
import pickle
import random
import shutil
import signal
import socket
import sys
import time
import traceback
from contextlib import contextmanager

from toil import logProcessContext
from toil.common import Toil, safeUnpickleFromStream
from toil.deferred import DeferredFunctionManager
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import CheckpointJobDescription, Job
from toil.lib.expando import MagicExpando
from toil.lib.io import make_public_dir
from toil.lib.resources import (get_total_cpu_time,
                                get_total_cpu_time_and_memory_usage)
from toil.statsAndLogging import configure_root_logger, set_log_level

try:
    from toil.cwl.cwltoil import CWL_INTERNAL_JOBS
except ImportError:
    # CWL extra not installed
    CWL_INTERNAL_JOBS = ()

logger = logging.getLogger(__name__)


def nextChainable(predecessor, jobStore, config):
    """
    Returns the next chainable job's JobDescription after the given predecessor
    JobDescription, if one exists, or None if the chain must terminate.

    :param toil.job.JobDescription predecessor: The job to chain from
    :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore: The JobStore to fetch JobDescriptions from.
    :param toil.common.Config config: The configuration for the current run.
    :rtype: toil.job.JobDescription or None
    """
    #If no more jobs to run or services not finished, quit
    if len(predecessor.stack) == 0 or len(predecessor.services) > 0 or (isinstance(predecessor, CheckpointJobDescription) and predecessor.checkpoint != None):
        logger.debug("Stopping running chain of jobs: length of stack: %s, services: %s, checkpoint: %s",
                     len(predecessor.stack), len(predecessor.services), (isinstance(predecessor, CheckpointJobDescription) and predecessor.checkpoint != None))
        return None

    if len(predecessor.stack) > 1 and len(predecessor.stack[-1]) > 0 and len(predecessor.stack[-2]) > 0:
        # TODO: Without a real stack list we can freely mutate, we can't chain
        # to a child, which may branch, and then go back and do the follow-ons
        # of the original job.
        # TODO: Go back to a free-form stack list and require some kind of
        # stack build phase?
        logger.debug("Stopping running chain of jobs because job has both children and follow-ons")
        return None

    #Get the next set of jobs to run
    jobs = predecessor.nextSuccessors()
    if len(jobs) == 0:
        # If there are no jobs, we might just not have any children.
        logger.debug("Stopping running chain of jobs because job has no ready children or follow-ons")
        return None

    #If there are 2 or more jobs to run in parallel we quit
    if len(jobs) >= 2:
        logger.debug("No more jobs can run in series by this worker,"
                    " it's got %i children", len(jobs)-1)
        return None

    # Grab the only job that should be there.
    successorID = next(iter(jobs))

    # Load the successor JobDescription
    successor = jobStore.load(successorID)

    #We check the requirements of the successor to see if we can run it
    #within the current worker
    if successor.memory > predecessor.memory:
        logger.debug("We need more memory for the next job, so finishing")
        return None
    if successor.cores > predecessor.cores:
        logger.debug("We need more cores for the next job, so finishing")
        return None
    if successor.disk > predecessor.disk:
        logger.debug("We need more disk for the next job, so finishing")
        return None
    if successor.preemptable != predecessor.preemptable:
        logger.debug("Preemptability is different for the next job, returning to the leader")
        return None
    if successor.predecessorNumber > 1:
        logger.debug("The next job has multiple predecessors; we must return to the leader.")
        return None

    if len(successor.services) > 0:
        logger.debug("The next job requires services that will not yet be started; we must return to the leader.")
        return None

    if isinstance(successor, CheckpointJobDescription):
        # Check if job is a checkpoint job and quit if so
        logger.debug("Next job is checkpoint, so finishing")
        return None

    # Made it through! This job is chainable.
    return successor

def workerScript(jobStore, config, jobName, jobStoreID, redirectOutputToLogFile=True):
    """
    Worker process script, runs a job.

    :param str jobName: The "job name" (a user friendly name) of the job to be run
    :param str jobStoreLocator: Specifies the job store to use
    :param str jobStoreID: The job store ID of the job to be run

    :return int: 1 if a job failed, or 0 if all jobs succeeded
    """

    configure_root_logger()
    set_log_level(config.logLevel)

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
    #Load the environment for the job
    ##########################################

    #First load the environment for the job.
    with jobStore.readSharedFileStream("environment.pickle") as fileHandle:
        environment = safeUnpickleFromStream(fileHandle)
    env_reject = {
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
        elif i not in env_reject:
            os.environ[i] = environment[i]
    # sys.path is used by __import__ to find modules
    if "PYTHONPATH" in environment:
        for e in environment["PYTHONPATH"].split(':'):
            if e != '':
                sys.path.append(e)

    ##########################################
    #Setup the temporary directories.
    ##########################################
    # Dir to put all this worker's temp files in.
    toilWorkflowDir = Toil.getLocalWorkflowDir(config.workflowID, config.workDir)
    localWorkerTempDir = make_public_dir(in_directory=toilWorkflowDir)
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
        #Load the JobDescription
        ##########################################

        jobDesc = jobStore.load(jobStoreID)
        listOfJobs[0] = str(jobDesc)
        logger.debug("Parsed job description")

        ##########################################
        #Cleanup from any earlier invocation of the job
        ##########################################

        if jobDesc.command == None:
            logger.debug("Job description has no body to run.")
            # Cleanup jobs already finished
            predicate = lambda jID: jobStore.exists(jID)
            jobDesc.filterSuccessors(predicate)
            jobDesc.filterServiceHosts(predicate)
            logger.debug("Cleaned up any references to completed successor jobs")

        # This cleans the old log file which may
        # have been left if the job is being retried after a job failure.
        oldLogFile = jobDesc.logJobStoreFileID
        if oldLogFile != None:
            jobDesc.logJobStoreFileID = None
            jobStore.update(jobDesc) #Update first, before deleting any files
            jobStore.deleteFile(oldLogFile)

        ##########################################
        # If a checkpoint exists, restart from the checkpoint
        ##########################################

        if isinstance(jobDesc, CheckpointJobDescription) and jobDesc.checkpoint is not None:
            # The job is a checkpoint, and is being restarted after previously completing
            logger.debug("Job is a checkpoint")
            # If the checkpoint still has extant successors or services, its
            # subtree didn't complete properly. We handle the restart of the
            # checkpoint here, removing its previous subtree.
            if next(jobDesc.successorsAndServiceHosts(), None) is not None:
                logger.debug("Checkpoint has failed; restoring")
                # Reduce the try count
                assert jobDesc.remainingTryCount >= 0
                jobDesc.remainingTryCount = max(0, jobDesc.remainingTryCount - 1)
                jobDesc.restartCheckpoint(jobStore)
            # Otherwise, the job and successors are done, and we can cleanup stuff we couldn't clean
            # because of the job being a checkpoint
            else:
                logger.debug("The checkpoint jobs seems to have completed okay, removing any checkpoint files to delete.")
                #Delete any remnant files
                list(map(jobStore.deleteFile, list(filter(jobStore.fileExists, jobDesc.checkpointFilesToDelete))))

        ##########################################
        #Setup the stats, if requested
        ##########################################

        if config.stats:
            startClock = get_total_cpu_time()

        startTime = time.time()
        while True:
            ##########################################
            #Run the job body, if there is one
            ##########################################

            logger.info("Working on job %s", jobDesc)

            if jobDesc.command is not None:
                assert jobDesc.command.startswith("_toil ")
                logger.debug("Got a command to run: %s" % jobDesc.command)
                # Load the job. It will use the same JobDescription we have been using.
                job = Job.loadJob(jobStore, jobDesc)
                if isinstance(jobDesc, CheckpointJobDescription):
                    # If it is a checkpoint job, save the command
                    jobDesc.checkpoint = jobDesc.command

                logger.info("Loaded body %s from description %s", job, jobDesc)

                # Create a fileStore object for the job
                fileStore = AbstractFileStore.createFileStore(jobStore, jobDesc, localWorkerTempDir, blockFn,
                                                              caching=not config.disableCaching)
                with job._executor(stats=statsDict if config.stats else None,
                                   fileStore=fileStore):
                    with deferredFunctionManager.open() as defer:
                        with fileStore.open(job):
                            # Get the next block function to wait on committing this job
                            blockFn = fileStore.waitForCommit

                            # Run the job, save new successors, and set up
                            # locally (but don't commit) successor
                            # relationships and job completion.
                            # Pass everything as name=value because Cactus
                            # likes to override _runner when it shouldn't and
                            # it needs some hope of finding the arguments it
                            # wants across multiple Toil versions. We also
                            # still pass a jobGraph argument to placate old
                            # versions of Cactus.
                            job._runner(jobGraph=None, jobStore=jobStore, fileStore=fileStore, defer=defer)

                # Accumulate messages from this job & any subsequent chained jobs
                statsDict.workers.logsToMaster += fileStore.loggingMessages

                logger.info("Completed body for %s", jobDesc)

            else:
                #The command may be none, in which case
                #the JobDescription is either a shell ready to be deleted or has
                #been scheduled after a failure to cleanup
                logger.debug("No user job to run, so finishing")
                break

            if AbstractFileStore._terminateEvent.isSet():
                raise RuntimeError("The termination flag is set")

            ##########################################
            #Establish if we can run another job within the worker
            ##########################################
            successor = nextChainable(jobDesc, jobStore, config)
            if successor is None or config.disableChaining:
                # Can't chain any more jobs. We are going to stop.

                logger.info("Not chaining from job %s", jobDesc)

                # TODO: Somehow the commit happens even if we don't start it here.

                break

            logger.info("Chaining from %s to %s", jobDesc, successor)

            ##########################################
            # We have a single successor job that is not a checkpoint job. We
            # reassign the ID of the current JobDescription to the successor.
            # We can then delete the successor JobDescription (under its old
            # ID) in the jobStore, as it is wholly incorporated into the
            # current one.
            ##########################################

            # Make sure nothing has gone wrong and we can really chain
            assert jobDesc.memory >= successor.memory
            assert jobDesc.cores >= successor.cores

            # Save the successor's original ID, so we can clean it (and its
            # body) up after we finish executing it.
            successorID = successor.jobStoreID

            # add the successor to the list of jobs run
            listOfJobs.append(str(successor))

            # Now we need to become that successor, under the original ID.
            successor.replace(jobDesc)
            jobDesc = successor

            # Problem: successor's job body is a file that will be cleaned up
            # when we delete the successor job by ID. We can't just move it. So
            # we need to roll up the deletion of the successor job by ID with
            # the deletion of the job ID we're currently working on.
            jobDesc.jobsToDelete.append(successorID)

            # Clone the now-current JobDescription (which used to be the successor).
            # TODO: Why??? Can we not?
            jobDesc = copy.deepcopy(jobDesc)

            # Build a fileStore to update the job and commit the replacement.
            # TODO: can we have a commit operation without an entire FileStore???
            fileStore = AbstractFileStore.createFileStore(jobStore, jobDesc, localWorkerTempDir, blockFn,
                                                          caching=not config.disableCaching)

            # Update blockFn to wait for that commit operation.
            blockFn = fileStore.waitForCommit

            # This will update the job once the previous job is done updating
            fileStore.startCommit(jobState=True)

            # Clone the current job description again, so that further updates
            # to it (such as new successors being added when it runs) occur
            # after the commit process we just kicked off, and aren't committed
            # early or partially.
            jobDesc = copy.deepcopy(jobDesc)

            logger.debug("Starting the next job")

        ##########################################
        #Finish up the stats
        ##########################################
        if config.stats:
            totalCPUTime, totalMemoryUsage = get_total_cpu_time_and_memory_usage()
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
        # Something has gone wrong.

        # Clobber any garbage state we have for this job from failing with
        # whatever good state is still stored in the JobStore
        jobDesc = jobStore.load(jobStoreID)
        # Remember that we failed
        jobAttemptFailed = True

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
        jobDesc.logJobStoreFileID = jobStore.getEmptyFileStoreID(jobDesc.jobStoreID, cleanup=True)
        jobDesc.chainedJobs = listOfJobs
        with jobStore.updateFileStream(jobDesc.logJobStoreFileID) as w:
            with open(tempWorkerLogPath, 'rb') as f:
                if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit !=0:
                    if logFileByteReportLimit > 0:
                        f.seek(-logFileByteReportLimit, 2)  # seek to last tooBig bytes of file
                    elif logFileByteReportLimit < 0:
                        f.seek(logFileByteReportLimit, 0)  # seek to first tooBig bytes of file
                # Dump the possibly-invalid-Unicode bytes into the log file
                w.write(f.read()) # TODO load file using a buffer
        # Commit log file reference back to JobStore
        jobStore.update(jobDesc)

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
        jobStore.writeStatsAndLogging(json.dumps(statsDict, ensure_ascii=True).encode())

    #Remove the temp dir
    cleanUp = config.cleanWorkDir
    if cleanUp == 'always' or (cleanUp == 'onSuccess' and not jobAttemptFailed) or (cleanUp == 'onError' and jobAttemptFailed):
        shutil.rmtree(localWorkerTempDir)

    #This must happen after the log file is done with, else there is no place to put the log
    if (not jobAttemptFailed) and jobDesc.command == None and next(jobDesc.successorsAndServiceHosts(), None) is None:
        # We can now safely get rid of the JobDescription, and all jobs it chained up
        for otherID in jobDesc.jobsToDelete:
            jobStore.delete(otherID)
        jobStore.delete(jobDesc.jobStoreID)

    if jobAttemptFailed:
        return 1
    else:
        return 0

def parse_args(args):
    """
    Parse command-line arguments to the worker.
    """

    # Drop the program name
    args = args[1:]

    # Make the parser
    parser = argparse.ArgumentParser()

    # Now add all the options to it

    # Base required job information
    parser.add_argument("jobName", type=str,
        help="Text name of the job being run")
    parser.add_argument("jobStoreLocator", type=str,
        help="Information required to connect to the job store")
    parser.add_argument("jobStoreID", type=str,
        help="ID of the job within the job store")

    # Additional worker abilities
    parser.add_argument("--context", default=[], action="append",
        help="""Pickled, base64-encoded context manager(s) to run job inside of.
                Allows the Toil leader to pass setup and cleanup work provided by the
                batch system, in the form of pickled Python context manager objects,
                that the worker can then run before/after the job on the batch
                system's behalf.""")

    return parser.parse_args(args)


@contextmanager
def in_contexts(contexts):
    """
    Unpickle and enter all the pickled, base64-encoded context managers in the
    given list. Then do the body, then leave them all.
    """

    if len(contexts) == 0:
        # Base case: nothing to do
        yield
    else:
        first = contexts[0]
        rest = contexts[1:]

        try:
            manager = pickle.loads(base64.b64decode(first.encode('utf-8')))
        except:
            exc_info = sys.exc_info()
            logger.error('Exception while unpickling context manager: ', exc_info=exc_info)
            raise

        with manager:
            # After entering this first context manager, do the rest.
            # It might set up stuff so we can decode later ones.
            with in_contexts(rest):
                yield


def main(argv=None):
    if argv is None:
        argv = sys.argv

    # Parse our command line
    options = parse_args(argv)

    # Parse input args
    jobName = argv[1]
    jobStoreLocator = argv[2]
    jobStoreID = argv[3]

    ##########################################
    #Load the jobStore/config file
    ##########################################

    jobStore = Toil.resumeJobStore(options.jobStoreLocator)
    config = jobStore.config

    with in_contexts(options.context):
        # Call the worker
        exit_code = workerScript(jobStore, config, options.jobName, options.jobStoreID)

    # Exit with its return value
    sys.exit(exit_code)
