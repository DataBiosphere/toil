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
import stat
import sys
import time
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Callable, Optional

from configargparse import ArgParser

from toil import logProcessContext
from toil.common import Config, Toil, safeUnpickleFromStream
from toil.cwl.utils import (
    CWL_UNSUPPORTED_REQUIREMENT_EXCEPTION,
    CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE,
)
from toil.deferred import DeferredFunctionManager
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import (
    CheckpointJobDescription,
    DebugStoppingPointReached,
    Job,
    JobDescription,
)
from toil.jobStores.abstractJobStore import AbstractJobStore
from toil.lib.expando import MagicExpando
from toil.lib.io import make_public_dir
from toil.lib.resources import ResourceMonitor
from toil.statsAndLogging import configure_root_logger, install_log_color, set_log_level

logger = logging.getLogger(__name__)


class StatsDict(MagicExpando):
    """Subclass of MagicExpando for type-checking purposes."""

    jobs: list[MagicExpando]


def nextChainable(
    predecessor: JobDescription, job_store: AbstractJobStore, config: Config
) -> Optional[JobDescription]:
    """
    Returns the next chainable job's JobDescription after the given predecessor
    JobDescription, if one exists, or None if the chain must terminate.

    :param predecessor: The job to chain from
    :param job_store: The JobStore to fetch JobDescriptions from.
    :param config: The configuration for the current run.
    """
    # If no more jobs to run or services not finished, quit
    if (
        predecessor.nextSuccessors() is None
        or len(predecessor.services) > 0
        or (
            isinstance(predecessor, CheckpointJobDescription)
            and predecessor.checkpoint is not None
        )
    ):
        logger.debug(
            "Stopping running chain of jobs: no successors: %s, services: %s, checkpoint: %s",
            predecessor.nextSuccessors() is None,
            len(predecessor.services),
            (
                isinstance(predecessor, CheckpointJobDescription)
                and predecessor.checkpoint is not None
            ),
        )
        return None

    # Get the next set of jobs to run
    jobs = list(predecessor.nextSuccessors() or set())
    if len(jobs) == 0:
        # If there are no jobs, we might just not have any children.
        logger.debug(
            "Stopping running chain of jobs because job has no ready children or follow-ons"
        )
        return None

    # If there are 2 or more jobs to run in parallel we quit
    if len(jobs) >= 2:
        logger.debug(
            "No more jobs can run in series by this worker," " it's got %i successors",
            len(jobs),
        )
        logger.debug("Two distinct successors are %s and %s", jobs[0], jobs[1])
        return None

    # Grab the only job that should be there.
    successorID = jobs[0]

    logger.debug("%s would chain to ID %s", predecessor, successorID)

    # Load the successor JobDescription
    successor = job_store.load_job(successorID)

    # We check the requirements of the successor to see if we can run it
    # within the current worker
    if successor.memory > predecessor.memory:
        logger.debug("We need more memory for the next job, so finishing")
        return None
    if successor.cores > predecessor.cores:
        logger.debug("We need more cores for the next job, so finishing")
        return None
    if successor.disk > predecessor.disk:
        logger.debug("We need more disk for the next job, so finishing")
        return None
    if successor.preemptible != predecessor.preemptible:
        logger.debug(
            "Preemptibility is different for the next job, returning to the leader"
        )
        return None
    if successor.predecessorNumber > 1:
        logger.debug(
            "The next job has multiple predecessors; we must return to the leader."
        )
        return None

    if len(successor.services) > 0:
        logger.debug(
            "The next job requires services that will not yet be started; we must return to the leader."
        )
        return None

    if isinstance(successor, CheckpointJobDescription):
        # Check if job is a checkpoint job and quit if so
        logger.debug("Next job is checkpoint, so finishing")
        return None

    if (
        not config.run_local_jobs_on_workers
        and predecessor.local
        and not successor.local
    ):
        # This job might be running on the leader, but the next job may not.
        #
        # TODO: Optimize by detecting whether we actually are on the leader,
        # somehow.
        logger.debug("Next job is not allowed to run on the leader, so finishing")
        return None

    # Made it through! This job is chainable.
    return successor


def workerScript(
    job_store: AbstractJobStore,
    config: Config,
    job_name: str,
    job_store_id: str,
    redirect_output_to_log_file: bool = True,
    local_worker_temp_dir: Optional[str] = None,
    debug_flags: Optional[set[str]] = None,
) -> int:
    """
    Worker process script, runs a job.

    :param job_store: The JobStore to fetch JobDescriptions from.
    :param config: The configuration for the current run.
    :param job_name: The "job name" (a user friendly name) of the job to be run
    :param job_store_id: The job store ID of the job to be run
    :param redirect_output_to_log_file: If False, log directly to the console
        instead of capturing job output.
    :param local_worker_temp_dir: The directory for the worker to work in. May
        be recursively removed after the job runs.
    :param debug_flags: Flags to set on each job before running it.

    :return int: 1 if a job failed, or 0 if all jobs succeeded
    """

    configure_root_logger()
    set_log_level(config.logLevel)

    if config.colored_logs:
        install_log_color()

    logger.debug("Worker started for job %s...", job_name)

    ##########################################
    # Create the worker killer, if requested
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
    # Load the environment for the job
    ##########################################

    # First load the environment for the job.
    with job_store.read_shared_file_stream("environment.pickle") as fileHandle:
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
        "JAVA_HOME",
        "XDG_SESSION_TYPE",
        "XDG_SESSION_CLASS",
        "XDG_SESSION_ID",
        "XDG_RUNTIME_DIR",
        "XDG_DATA_DIRS",
        "DBUS_SESSION_BUS_ADDRESS",
    }
    for i in environment:
        if i == "PATH":
            # Handle path specially. Sometimes e.g. leader may not include
            # /bin, but the Toil appliance needs it.
            if i in os.environ and os.environ[i] != "":
                # Use the provided PATH and then the local system's PATH
                os.environ[i] = environment[i] + ":" + os.environ[i]
            else:
                # Use the provided PATH only
                os.environ[i] = environment[i]
        elif i not in env_reject:
            os.environ[i] = environment[i]
    # sys.path is used by __import__ to find modules
    if "PYTHONPATH" in environment:
        for e in environment["PYTHONPATH"].split(":"):
            if e != "":
                sys.path.append(e)

    ##########################################
    # Setup the temporary directories.
    ##########################################
    # Dir to put all this worker's temp files in.
    if config.workflowID is None:
        raise RuntimeError("The worker workflow ID was never set.")
    toilWorkflowDir = Toil.getLocalWorkflowDir(config.workflowID, config.workDir)
    # Dir to put lock files in, ideally not on NFS.
    toil_coordination_dir = Toil.get_local_workflow_coordination_dir(
        config.workflowID, config.workDir, config.coordination_dir
    )
    if local_worker_temp_dir is None:
        # Invent a temp directory to work in
        local_worker_temp_dir = make_public_dir(toilWorkflowDir)
    os.chmod(local_worker_temp_dir, 0o755)

    ##########################################
    # Setup the logging
    ##########################################

    # This is mildly tricky because we don't just want to
    # redirect stdout and stderr for this Python process; we want to redirect it
    # for this process and all children. Consequently, we can't just replace
    # sys.stdout and sys.stderr; we need to mess with the underlying OS-level
    # file descriptors. See <http://stackoverflow.com/a/11632982/402891>

    # When we start, standard input is file descriptor 0, standard output is
    # file descriptor 1, and standard error is file descriptor 2.

    # Do we even want to redirect output? Let the config make us not do it.
    redirect_output_to_log_file = (
        redirect_output_to_log_file and not config.disableWorkerOutputCapture
    )

    # What file do we want to point FDs 1 and 2 to?
    tempWorkerLogPath = os.path.join(local_worker_temp_dir, "worker_log.txt")

    if redirect_output_to_log_file:
        # Announce that we are redirecting logging, and where it will now go.
        # This is only important if we are trying to manually trace a faulty worker invocation.
        logger.debug("Redirecting logging to %s", tempWorkerLogPath)
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
    # Worker log file trapped from here on in
    ##########################################

    jobAttemptFailed = False
    failure_exit_code = 1
    first_job_cores = None
    statsDict = StatsDict()  # type: ignore[no-untyped-call]
    statsDict.jobs = []
    statsDict.workers.logs_to_leader = []
    statsDict.workers.logging_user_streams = []

    def blockFn() -> bool:
        return True

    job = None
    try:

        # Put a message at the top of the log, just to make sure it's working.
        logger.info("---TOIL WORKER OUTPUT LOG---")
        sys.stdout.flush()

        logProcessContext(config)

        ##########################################
        # Connect to the deferred function system
        ##########################################
        deferredFunctionManager = DeferredFunctionManager(toil_coordination_dir)

        ##########################################
        # Load the JobDescription
        ##########################################

        jobDesc = job_store.load_job(job_store_id)
        logger.debug("Parsed job description")

        ##########################################
        # Cleanup from any earlier invocation of the job
        ##########################################

        if not jobDesc.has_body():
            logger.debug("Job description has no body to run.")
            # Cleanup jobs already finished
            jobDesc.clear_nonexistent_dependents(job_store)
            logger.debug("Cleaned up any references to completed successor jobs")

        # This cleans the old log file which may
        # have been left if the job is being retried after a job failure.
        oldLogFile = jobDesc.logJobStoreFileID
        if oldLogFile is not None:
            jobDesc.logJobStoreFileID = None
            job_store.update_job(jobDesc)  # Update first, before deleting any files
            job_store.delete_file(oldLogFile)

        ##########################################
        # If a checkpoint exists, restart from the checkpoint
        ##########################################

        if (
            isinstance(jobDesc, CheckpointJobDescription)
            and jobDesc.checkpoint is not None
        ):
            # The job is a checkpoint, and is being restarted after previously completing
            logger.debug("Job is a checkpoint")
            # If the checkpoint still has extant successors or services, its
            # subtree didn't complete properly. We handle the restart of the
            # checkpoint here, removing its previous subtree.
            if next(jobDesc.successorsAndServiceHosts(), None) is not None:
                logger.debug("Checkpoint has failed; restoring")
                # Reduce the try count
                if jobDesc.remainingTryCount < 0:
                    raise RuntimeError("The try count of the job cannot be negative.")
                jobDesc.remainingTryCount = max(0, jobDesc.remainingTryCount - 1)
                jobDesc.restartCheckpoint(job_store)
            # Otherwise, the job and successors are done, and we can cleanup stuff we couldn't clean
            # because of the job being a checkpoint
            else:
                logger.debug(
                    "The checkpoint jobs seems to have completed okay, removing any checkpoint files to delete."
                )
                # Delete any remnant files
                list(
                    map(
                        job_store.delete_file,
                        list(
                            filter(
                                job_store.file_exists, jobDesc.checkpointFilesToDelete
                            )
                        ),
                    )
                )

        ##########################################
        # Setup the stats, if requested
        ##########################################

        if config.stats:
            # Remember the cores from the first job, which is how many we have reserved for us.
            statsDict.workers.requested_cores = jobDesc.cores
            startClock = ResourceMonitor.get_total_cpu_time()

        startTime = time.time()
        while True:
            ##########################################
            # Run the job body, if there is one
            ##########################################

            logger.info("Working on job %s", jobDesc)

            if jobDesc.has_body():
                # Load the job. It will use the same JobDescription we have been using.
                job = Job.loadJob(job_store, jobDesc)
                if isinstance(jobDesc, CheckpointJobDescription):
                    # If it is a checkpoint job, set the checkpoint
                    jobDesc.set_checkpoint()

                logger.info("Loaded body %s from description %s", job, jobDesc)

                if debug_flags:
                    for flag in debug_flags:
                        logger.debug("Turning on debug flag %s on job", flag)
                        job.set_debug_flag(flag)

                # Create a fileStore object for the job
                fileStore = AbstractFileStore.createFileStore(
                    job_store,
                    jobDesc,
                    local_worker_temp_dir,
                    blockFn,
                    caching=config.caching,
                )
                try:
                    with job._executor(
                        stats=statsDict if config.stats else None, fileStore=fileStore
                    ):
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
                                job._runner(
                                    jobGraph=None,
                                    jobStore=job_store,
                                    fileStore=fileStore,
                                    defer=defer,
                                )

                                # When the executor for the job finishes it will
                                # kick off a commit with the link to the job body
                                # cut.
                finally:
                    # Accumulate messages from this job & any subsequent chained jobs.
                    # Keep the messages even if the job fails.
                    statsDict.workers.logs_to_leader += fileStore.logging_messages
                    statsDict.workers.logging_user_streams += (
                        fileStore.logging_user_streams
                    )

                logger.info("Completed body for %s", jobDesc)

            else:
                # The body may not be attached, in which case the
                # JobDescription is either a shell ready to be deleted or has
                # been scheduled after a failure to cleanup
                logger.debug("No user job to run, so finishing")
                break

            if AbstractFileStore._terminateEvent.is_set():
                raise RuntimeError("The termination flag is set")

            ##########################################
            # Establish if we can run another job within the worker
            ##########################################
            successor = nextChainable(jobDesc, job_store, config)
            if successor is None or config.disableChaining:
                # Can't chain any more jobs. We are going to stop.

                logger.info("Not chaining from job %s", jobDesc)

                # No need to commit because the _executor context manager did it.

                break

            logger.info("Chaining from %s to %s", jobDesc, successor)

            ##########################################
            # We have a single successor job that is not a checkpoint job. We
            # reassign the ID of the current JobDescription to the successor,
            # and take responsibility for both jobs' associated files in the
            # combined job.
            ##########################################

            # Make sure nothing has gone wrong and we can really chain
            if jobDesc.memory < successor.memory:
                raise RuntimeError(
                    "Cannot chain jobs. A job's memory cannot be less than it's successor."
                )
            if jobDesc.cores < successor.cores:
                raise RuntimeError(
                    "Cannot chain jobs. A job's cores cannot be less than it's successor."
                )

            # Save the successor's original ID, so we can clean it (and its
            # body) up after we finish executing it.
            successorID = successor.jobStoreID

            # Now we need to become that successor, under the original ID.
            successor.replace(jobDesc)
            jobDesc = successor

            # Clone the now-current JobDescription (which used to be the successor).
            # TODO: Why??? Can we not?
            jobDesc = copy.deepcopy(jobDesc)

            # Build a fileStore to update the job and commit the replacement.
            # TODO: can we have a commit operation without an entire FileStore???
            fileStore = AbstractFileStore.createFileStore(
                job_store,
                jobDesc,
                local_worker_temp_dir,
                blockFn,
                caching=config.caching,
            )

            # Update blockFn to wait for that commit operation.
            blockFn = fileStore.waitForCommit

            # This will update the job once the previous job is done updating
            fileStore.startCommit(jobState=True)

            logger.debug("Starting the next job")

        ##########################################
        # Finish up the stats
        ##########################################
        if config.stats:
            totalCPUTime, totalMemoryUsage = (
                ResourceMonitor.get_total_cpu_time_and_memory_usage()
            )
            statsDict.workers.time = str(time.time() - startTime)
            statsDict.workers.clock = str(totalCPUTime - startClock)
            statsDict.workers.memory = str(totalMemoryUsage)
            # Say the worker used the max disk we saw from any job
            max_bytes = 0
            for job_stats in statsDict.jobs:
                if "disk" in job_stats:
                    max_bytes = max(max_bytes, int(job_stats.disk))
            statsDict.workers.disk = str(max_bytes)
            # Count the jobs executed.
            # TODO: toil stats could compute this but its parser is too general to hook into simply.
            statsDict.workers.jobs_run = len(statsDict.jobs)

        # log the worker log path here so that if the file is truncated the path can still be found
        if redirect_output_to_log_file:
            logger.info(
                "Worker log can be found at %s. Set --cleanWorkDir to retain this log",
                local_worker_temp_dir,
            )

        logger.info(
            "Finished running the chain of jobs on this node, we ran for a total of %f seconds",
            time.time() - startTime,
        )

    ##########################################
    # Trapping where worker goes wrong
    ##########################################
    except DebugStoppingPointReached:
        # Job wants the worker to stop for debugging
        raise
    except (
        BaseException
    ) as e:  # Case that something goes wrong in worker, or we are asked to stop
        if not isinstance(e, SystemExit):
            logger.critical(
                "Worker crashed with traceback:\n%s", traceback.format_exc()
            )
        logger.error(
            "Exiting the worker because of a failed job on host %s",
            socket.gethostname(),
        )
        if isinstance(e, CWL_UNSUPPORTED_REQUIREMENT_EXCEPTION):
            # We need to inform the leader that this is a CWL workflow problem
            # and it needs to inform its caller.
            failure_exit_code = CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE
        elif isinstance(e, SystemExit) and isinstance(e.code, int) and e.code != 0:
            # We're meant to be exiting with a particular code.
            failure_exit_code = e.code
        else:
            try:
                from WDL.runtime.error import CommandFailed

                if isinstance(e, CommandFailed):
                    failure_exit_code = e.exit_status
            except ImportError:
                # WDL dependency not available
                pass
        AbstractFileStore._terminateEvent.set()
    finally:
        # Get rid of our deferred function manager now so we can't mistake it
        # for someone else's if we do worker cleanup.
        del deferredFunctionManager
        try:
            import cwltool.main

            cwltool.main._terminate_processes()
        except (ImportError, ModuleNotFoundError):
            pass
        except Exception as e:
            logger.debug("cwltool.main._terminate_processess exception: %s", (e))
            raise e

    ##########################################
    # Wait for the asynchronous chain of writes/updates to finish
    ##########################################

    blockFn()

    ##########################################
    # All the asynchronous worker/update threads must be finished now,
    # so safe to test if they completed okay
    ##########################################

    if AbstractFileStore._terminateEvent.is_set():
        # Something has gone wrong.

        # Clobber any garbage state we have for this job from failing with
        # whatever good state is still stored in the JobStore
        jobDesc = job_store.load_job(job_store_id)
        # Remember that we failed
        jobAttemptFailed = True

    ##########################################
    # Cleanup
    ##########################################

    # Close the worker logging
    # Flush at the Python level
    sys.stdout.flush()
    sys.stderr.flush()
    if redirect_output_to_log_file:
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
    if jobAttemptFailed and redirect_output_to_log_file:
        jobDesc.logJobStoreFileID = logJobStoreFileID = job_store.getEmptyFileStoreID(
            jobDesc.jobStoreID, cleanup=True
        )
        with job_store.update_file_stream(logJobStoreFileID) as w:
            with open(tempWorkerLogPath, "rb") as f:
                if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit != 0:
                    if logFileByteReportLimit > 0:
                        f.seek(
                            -logFileByteReportLimit, 2
                        )  # seek to last tooBig bytes of file
                    elif logFileByteReportLimit < 0:
                        f.seek(
                            logFileByteReportLimit, 0
                        )  # seek to first tooBig bytes of file
                # Dump the possibly-invalid-Unicode bytes into the log file
                w.write(f.read())  # TODO load file using a buffer
        # Commit log file reference back to JobStore
        job_store.update_job(jobDesc)

    elif (
        debugging or (config.writeLogsFromAllJobs and not jobDesc.local)
    ) and redirect_output_to_log_file:  # write log messages
        with open(tempWorkerLogPath, "rb") as logFile:
            if os.path.getsize(tempWorkerLogPath) > logFileByteReportLimit != 0:
                if logFileByteReportLimit > 0:
                    logFile.seek(
                        -logFileByteReportLimit, 2
                    )  # seek to last tooBig bytes of file
                elif logFileByteReportLimit < 0:
                    logFile.seek(
                        logFileByteReportLimit, 0
                    )  # seek to first tooBig bytes of file
            # Make sure lines are Unicode so they can be JSON serialized as part of the dict.
            # We may have damaged the Unicode text by cutting it at an arbitrary byte so we drop bad characters.
            logMessages = [
                line.decode("utf-8", "skip") for line in logFile.read().splitlines()
            ]
        statsDict.logs.names = [names.stats_name for names in jobDesc.get_chain()]
        statsDict.logs.messages = logMessages

    if (
        debugging
        or config.stats
        or statsDict.workers.logs_to_leader
        or statsDict.workers.logging_user_streams
    ):
        # We have stats/logging to report back.
        # We report even if the job attempt failed.
        # TODO: Will that upset analysis of the stats?
        job_store.write_logs(json.dumps(statsDict, ensure_ascii=True))

    # Remove the temp dir
    cleanUp = config.cleanWorkDir
    if (
        cleanUp == "always"
        or (cleanUp == "onSuccess" and not jobAttemptFailed)
        or (cleanUp == "onError" and jobAttemptFailed)
    ):

        def make_parent_writable(func: Callable[[str], Any], path: str, _: Any) -> None:
            """
            When encountering an error removing a file or directory, make sure
            the parent directory is writable.

            cwltool likes to lock down directory permissions, and doesn't clean
            up after itself.
            """
            # Just chmod it for rwx for user. This can't work anyway if it isn't ours.
            try:
                os.chmod(
                    os.path.dirname(path), stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                )
            except PermissionError as e:
                logger.error(
                    "Could not set permissions on %s to allow cleanup of %s: %s",
                    os.path.dirname(path),
                    path,
                    e,
                )

        shutil.rmtree(local_worker_temp_dir, onerror=make_parent_writable)

    # This must happen after the log file is done with, else there is no place to put the log
    if (not jobAttemptFailed) and jobDesc.is_subtree_done():
        for merged_in in jobDesc.get_chain():
            # We can now safely get rid of the JobDescription, and all jobs it chained up
            job_store.delete_job(merged_in.job_store_id)

    if jobAttemptFailed:
        return failure_exit_code
    else:
        return 0


def parse_args(args: list[str]) -> Any:
    """
    Parse command-line arguments to the worker.
    """

    # Drop the program name
    args = args[1:]

    # Make the parser
    parser = ArgParser()

    # Now add all the options to it

    # Base required job information
    parser.add_argument("jobName", type=str, help="Text name of the job being run")
    parser.add_argument(
        "jobStoreLocator",
        type=str,
        help="Information required to connect to the job store",
    )
    parser.add_argument(
        "jobStoreID", type=str, help="ID of the job within the job store"
    )

    # Additional worker abilities
    parser.add_argument(
        "--context",
        default=[],
        action="append",
        help="""Pickled, base64-encoded context manager(s) to run job inside of.
                Allows the Toil leader to pass setup and cleanup work provided by the
                batch system, in the form of pickled Python context manager objects,
                that the worker can then run before/after the job on the batch
                system's behalf.""",
    )

    return parser.parse_args(args)


@contextmanager
def in_contexts(contexts: list[str]) -> Iterator[None]:
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
            manager = pickle.loads(base64.b64decode(first.encode("utf-8")))
        except:
            exc_info = sys.exc_info()
            logger.error(
                "Exception while unpickling context manager: ", exc_info=exc_info
            )
            raise

        with manager:
            # After entering this first context manager, do the rest.
            # It might set up stuff so we can decode later ones.
            with in_contexts(rest):
                yield


def main(argv: Optional[list[str]] = None) -> None:
    if argv is None:
        argv = sys.argv
    # Parse our command line
    options = parse_args(argv)

    ##########################################
    # Load the jobStore/config file
    ##########################################

    job_store = Toil.resumeJobStore(options.jobStoreLocator)
    config = job_store.config

    with in_contexts(options.context):
        # Call the worker
        exit_code = workerScript(job_store, config, options.jobName, options.jobStoreID)

    # Exit with its return value
    sys.exit(exit_code)
