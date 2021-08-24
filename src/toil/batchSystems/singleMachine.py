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
import logging
import math
import os
import signal
import subprocess
import sys
import time
import traceback
from contextlib import contextmanager
from queue import Empty, Queue
from threading import Condition, Event, Lock, Thread
from typing import Dict, List, Optional, Sequence

import toil
import toil.job
from toil import worker as toil_worker
from toil.batchSystems.abstractBatchSystem import (EXIT_STATUS_UNAVAILABLE_VALUE,
                                                   BatchSystemSupport,
                                                   UpdatedBatchJobInfo)
from toil.common import Toil
from toil.lib.threading import cpu_count

log = logging.getLogger(__name__)


class SingleMachineBatchSystem(BatchSystemSupport):
    """
    The interface for running jobs on a single machine, runs all the jobs you
    give it as they come in, but in parallel.

    Uses a single "daddy" thread to manage a fleet of child processes.

    Communication with the daddy thread happens via two queues: one queue of
    jobs waiting to be run (the input queue), and one queue of jobs that are
    finished/stopped and need to be returned by getUpdatedBatchJob (the output
    queue).

    When the batch system is shut down, the daddy thread is stopped.

    If running in debug-worker mode, jobs are run immediately as they are sent
    to the batch system, in the sending thread, and the daddy thread is not
    run. But the queues are still used.
    """

    @classmethod
    def supportsAutoDeployment(cls):
        return False

    @classmethod
    def supportsWorkerCleanup(cls):
        return True

    numCores = cpu_count()

    minCores = 0.1
    """
    The minimal fractional CPU. Tasks with a smaller core requirement will be rounded up to this
    value.
    """
    physicalMemory = toil.physicalMemory()

    def __init__(self, config, maxCores, maxMemory, maxDisk):
        self.config = config
        # Limit to the smaller of the user-imposed limit and what we actually
        # have on this machine for each resource.
        #
        # If we don't have up to the limit of the resource (and the resource
        # isn't the inlimited sentinel), warn.
        if maxCores > self.numCores:
            if maxCores != sys.maxsize:
                # We have an actually specified limit and not the default
                log.warning('Not enough cores! User limited to %i but we only have %i.', maxCores, self.numCores)
            maxCores = self.numCores
        if maxMemory > self.physicalMemory:
            if maxMemory != sys.maxsize:
                # We have an actually specified limit and not the default
                log.warning('Not enough memory! User limited to %i bytes but we only have %i bytes.', maxMemory, self.physicalMemory)
            maxMemory = self.physicalMemory

        workdir = Toil.getLocalWorkflowDir(config.workflowID, config.workDir)  # config.workDir may be None; this sets a real directory
        self.physicalDisk = toil.physicalDisk(workdir)
        if maxDisk > self.physicalDisk:
            if maxDisk != sys.maxsize:
                # We have an actually specified limit and not the default
                log.warning('Not enough disk space! User limited to %i bytes but we only have %i bytes.', maxDisk, self.physicalDisk)
            maxDisk = self.physicalDisk

        super(SingleMachineBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        assert self.maxCores >= self.minCores
        assert self.maxMemory >= 1

        # The scale allows the user to apply a factor to each task's cores requirement, thereby
        # squeezing more tasks onto each core (scale < 1) or stretching tasks over more cores
        # (scale > 1).
        self.scale = config.scale

        if config.badWorker > 0 and config.debugWorker:
            # We can't throw SIGUSR1 at the worker because it is also going to
            # be the leader and/or test harness.
            raise RuntimeError("Cannot use badWorker and debugWorker together; "
                "worker would have to kill the leader")

        self.debugWorker = config.debugWorker

        # A counter to generate job IDs and a lock to guard it
        self.jobIndex = 0
        self.jobIndexLock = Lock()

        # A dictionary mapping IDs of submitted jobs to the command line
        self.jobs: Dict[str, toil.job.JobDescription] = {}

        # A queue of jobs waiting to be executed. Consumed by the daddy thread.
        self.inputQueue = Queue()

        # A queue of finished jobs. Produced by the daddy thread.
        self.outputQueue = Queue()

        # A dictionary mapping IDs of currently running jobs to their Info objects
        self.runningJobs: Dict[str, Info] = {}

        # These next two are only used outside debug-worker mode

        # A dict mapping PIDs to Popen objects for running jobs.
        # Jobs that don't fork are executed one at a time in the main thread.
        self.children: Dict[int, subprocess.Popen] = {}
        # A dict mapping child PIDs to the Job IDs they are supposed to be running.
        self.childToJob: Dict[int, str] = {}

        # A pool representing available CPU in units of minCores
        self.coreFractions = ResourcePool(int(self.maxCores / self.minCores), 'cores')
        # A pool representing available memory in bytes
        self.memory = ResourcePool(self.maxMemory, 'memory')
        # A pool representing the available space in bytes
        self.disk = ResourcePool(self.maxDisk, 'disk')

        # If we can't schedule something, we fill this in with a reason why
        self.schedulingStatusMessage = None

        # We use this event to signal shutdown
        self.shuttingDown = Event()

        # A thread in charge of managing all our child processes.
        # Also takes care of resource accounting.
        self.daddyThread = None
        # If it breaks it will fill this in
        self.daddyException: Optional[Exception] = None

        if self.debugWorker:
            log.debug('Started batch system %s in worker debug mode.', id(self))
        else:
            self.daddyThread = Thread(target=self.daddy, daemon=True)
            self.daddyThread.start()
            log.debug('Started batch system %s in normal mode.', id(self))

    def daddy(self):
        """
        Be the "daddy" thread.

        Our job is to look at jobs from the input queue.

        If a job fits in the available resources, we allocate resources for it
        and kick off a child process.

        We also check on our children.

        When a child finishes, we reap it, release its resources, and put its
        information in the output queue.
        """

        try:
            log.debug('Started daddy thread for batch system %s.', id(self))

            while not self.shuttingDown.is_set():
                # Main loop

                while not self.shuttingDown.is_set():
                    # Try to start as many jobs as we can try to start
                    try:
                        # Grab something from the input queue if available.
                        args = self.inputQueue.get_nowait()
                        jobCommand, jobID, jobCores, jobMemory, jobDisk, environment = args

                        coreFractions = int(jobCores / self.minCores)

                        # Try to start the child
                        result = self._startChild(jobCommand, jobID,
                            coreFractions, jobMemory, jobDisk, environment)

                        if result is None:
                            # We did not get the resources to run this job.
                            # Requeue last, so we can look at the next job.
                            # TODO: Have some kind of condition the job can wait on,
                            # but without threads (queues for jobs needing
                            # cores/memory/disk individually)?
                            self.inputQueue.put(args)
                            break

                        # Otherwise it's a PID if it succeeded, or False if it couldn't
                        # start. But we don't care either way here.

                    except Empty:
                        # Nothing to run. Stop looking in the queue.
                        break

                # Now check on our children.
                for done_pid in self._pollForDoneChildrenIn(self.children):
                    # A child has actually finished.
                    # Clean up after it.
                    self._handleChild(done_pid)

                # Then loop again: start and collect more jobs.
                # TODO: It would be good to be able to wait on a new job or a finished child, whichever comes first.
                # For now we just sleep and loop.
                time.sleep(0.01)


            # When we get here, we are shutting down.
            log.debug('Daddy thread cleaning up %d remaining children for batch system %s...', len(self.children), id(self))

            self._stop_and_wait(self.children.values())

            log.debug('Daddy thread for batch system %s finishing because no children should now exist', id(self))

            # Then exit the thread.
            return
        except Exception as e:
            log.critical('Unhandled exception in daddy thread for batch system %s: %s', id(self), traceback.format_exc())
            # Pass the exception back to the main thread so it can stop the next person who calls into us.
            self.daddyException = e
            raise

    def _checkOnDaddy(self):
        if self.daddyException is not None:
            # The daddy thread broke and we cannot do our job
            log.critical('Propagating unhandled exception in daddy thread to main thread')
            exc = self.daddyException
            self.daddyException = None
            if isinstance(exc, Exception):
                raise exc
            else:
                raise TypeError(f'Daddy thread failed with non-exception: {exc}')

    def _stop_now(self, popens: Sequence[subprocess.Popen]) -> List[int]:
        """
        Stop the given child processes and all their children. Does not reap them.

        Returns a list of PGIDs killed, where processes may exist that have not
        yet received their kill signals.
        """

        # We will potentially need to poll these PGIDs to ensure that all
        # processes in them are gone.
        pgids = []

        for popen in popens:
            # Kill all the children

            if popen.returncode is None:
                # Process is not known to be dead. Try and grab its group.
                try:
                    pgid = os.getpgid(popen.pid)
                except OSError:
                    # It just died. Assume the pgid was its PID.
                    pgid = popen.pid
            else:
                # It is dead. Try it's PID as a PGID and hope we didn't re-use it.
                pgid = popen.pid

            if pgid != os.getpgrp():
                # The child process really is in its own group, and not ours.

                # Kill the group, which hopefully hasn't been reused
                log.debug('Send shutdown kill to process group %s known to batch system %s', pgid, id(self))
                try:
                    os.killpg(pgid, signal.SIGKILL)
                    pgids.append(pgid)
                except ProcessLookupError:
                    # It is dead already
                    pass
                except PermissionError:
                    # It isn't ours actually. Ours is dead.
                    pass
            else:
                # Kill the subprocess again through popen in case it somehow
                # never managed to make the group.
                popen.kill()

        return pgids

    def _stop_and_wait(self, popens: Sequence[subprocess.Popen]) -> None:
        """
        Stop the given child processes and all their children. Blocks until the
        processes are gone.
        """

        pgids = self._stop_now(popens)

        for popen in popens:
            # Wait on all the children
            popen.wait()

            log.debug('Process %s known to batch system %s is stopped; it returned %s', popen.pid, id(self), popen.returncode)

        for pgid in pgids:
            try:
                while True:
                    # Send a kill to the group again, to see if anything in it
                    # is still alive. Our first kill might not have been
                    # delivered yet.
                    os.killpg(pgid, signal.SIGKILL)
                    # If that worked it is still alive, so wait for the kernel
                    # to stop fooling around and kill it.
                    log.warning('Sent redundant shutdown kill to surviving process group %s known to batch system %s', pgid, id(self))
                    time.sleep(0.1)
            except ProcessLookupError:
                # The group is actually gone now.
                pass
            except PermissionError:
                # The group is not only gone but reused
                pass


    def _pollForDoneChildrenIn(self, pid_to_popen):
        """
        See if any children represented in the given dict from PID to Popen
        object have finished.

        Return a collection of their PIDs.

        Guarantees that each child's exit code will be gettable via wait() on
        the child's Popen object (i.e. does not reap the child, unless via
        Popen).
        """

        # We keep our found PIDs in a set so we can work around waitid showing
        # us the same one repeatedly.
        ready = set()

        # Find the waitid function
        waitid = getattr(os, 'waitid', None)

        if callable(waitid):
            # waitid exists (not Mac)

            while True:
                # Poll for any child to have exit, but don't reap it. Leave reaping
                # to the Popen.
                # TODO: What if someone else in Toil wants to do this syscall?
                # TODO: Is this one-notification-per-done-child with WNOHANG? Or
                # can we miss some? Or do we see the same one repeatedly until it
                # is reaped?
                try:
                    siginfo = waitid(os.P_ALL, -1, os.WEXITED | os.WNOWAIT | os.WNOHANG)
                except ChildProcessError:
                    # This happens when there is nothing to wait on right now,
                    # instead of the weird C behavior of overwriting a field in
                    # a pointed-to struct.
                    siginfo = None
                if siginfo is not None and siginfo.si_pid in pid_to_popen and siginfo.si_pid not in ready:
                    # Something new finished
                    ready.add(siginfo.si_pid)
                else:
                    # Nothing we own that we haven't seen before has finished.
                    return ready
        else:
            # On Mac there's no waitid and no way to wait and not reap.
            # Fall back on polling all the Popen objects.
            # To make this vaguely efficient we have to return done children in
            # batches.
            for pid, popen in pid_to_popen.items():
                if popen.poll() is not None:
                    # Process is done
                    ready.add(pid)
                    log.debug('Child %d has stopped', pid)

            # Return all the done processes we found
            return ready

    def _runDebugJob(self, jobCommand, jobID, environment):
        """
        Run the jobCommand right now, in the current thread.
        May only be called in debug-worker mode.
        Assumes resources are available.
        """
        assert self.debugWorker
        # TODO: It is not possible to kill running jobs in forkless mode,
        # because they are run immediately in the main thread.
        info = Info(time.time(), None, None, killIntended=False)
        self.runningJobs[jobID] = info

        if jobCommand.startswith("_toil_worker "):
            # We can actually run in this thread
            jobName, jobStoreLocator, jobStoreID = jobCommand.split()[1:4] # Parse command
            jobStore = Toil.resumeJobStore(jobStoreLocator)
            toil_worker.workerScript(jobStore, jobStore.config, jobName, jobStoreID,
                                     redirectOutputToLogFile=not self.debugWorker) # Call the worker
        else:
            # Run synchronously. If starting or running the command fails, let the exception stop us.
            subprocess.check_call(jobCommand,
                                  shell=True,
                                  env=dict(os.environ, **environment))

        self.runningJobs.pop(jobID)
        if not info.killIntended:
            self.outputQueue.put(UpdatedBatchJobInfo(jobID=jobID, exitStatus=0, wallTime=time.time() - info.time, exitReason=None))

    def getSchedulingStatusMessage(self):
        # Implement the abstractBatchSystem's scheduling status message API
        return self.schedulingStatusMessage

    def _setSchedulingStatusMessage(self, message):
        """
        If we can't run a job, we record a short message about why not. If the
        leader wants to know what is up with us (for example, to diagnose a
        deadlock), it can ask us for the message.
        """

        self.schedulingStatusMessage = message

    def _startChild(self, jobCommand, jobID, coreFractions, jobMemory, jobDisk, environment):
        """
        Start a child process for the given job.

        Allocate its required resources and save it and save it in our bookkeeping structures.

        If the job is started, returns its PID.
        If the job fails to start, reports it as failed and returns False.
        If the job cannot get the resources it needs to start, returns None.
        """

        # We fill this in if we manage to actually start the child.
        popen = None

        # This is when we started working on the job.
        startTime = time.time()

        # See if we can fit the job in our resource pools right now.
        if self.coreFractions.acquireNow(coreFractions):
            # We got some cores
            if self.memory.acquireNow(jobMemory):
                # We got some memory
                if self.disk.acquireNow(jobDisk):
                    # We got the final resource, disk.
                    # Actually run the job.
                    # When it finishes we will release what it was using.
                    # So it is important to not lose track of the child process.

                    try:
                        # Launch the job.
                        # Make sure it is in its own session (and thus its own
                        # process group) so that, if the user signals the
                        # workflow, Toil will be responsible for killing the
                        # job. This also makes sure that we can signal the job
                        # and all its children together. We assume that the
                        # process group ID will equal the PID of the process we
                        # are starting.
                        popen = subprocess.Popen(jobCommand,
                                                 shell=True,
                                                 env=dict(os.environ, **environment),
                                                 start_new_session=True)
                    except Exception:
                        # If the job can't start, make sure we release resources now
                        self.coreFractions.release(coreFractions)
                        self.memory.release(jobMemory)
                        self.disk.release(jobDisk)

                        log.error('Could not start job %s: %s', jobID, traceback.format_exc())

                        # Report as failed.
                        self.outputQueue.put(UpdatedBatchJobInfo(jobID=jobID, exitStatus=EXIT_STATUS_UNAVAILABLE_VALUE, wallTime=0, exitReason=None))

                        # Free resources
                        self.coreFractions.release(coreFractions)
                        self.memory.release(jobMemory)
                        self.disk.release(jobDisk)

                        # Complain it broke.
                        return False
                    else:
                        # If the job did start, record it
                        self.children[popen.pid] = popen
                        # Make sure we can look it up by PID later
                        self.childToJob[popen.pid] = jobID
                        # Record that the job is running, and the resources it is using
                        info = Info(startTime, popen, (coreFractions, jobMemory, jobDisk), killIntended=False)
                        self.runningJobs[jobID] = info

                        log.debug('Launched job %s as child %d', jobID, popen.pid)

                        # Report success starting the job
                        # Note that if a PID were somehow 0 it would look like False
                        assert popen.pid != 0
                        return popen.pid
                else:
                    # We can't get disk, so free cores and memory
                    self.coreFractions.release(coreFractions)
                    self.memory.release(jobMemory)
                    self._setSchedulingStatusMessage('Not enough disk to run job %s' % jobID)
            else:
                # Free cores, since we can't get memory
                self.coreFractions.release(coreFractions)
                self._setSchedulingStatusMessage('Not enough memory to run job %s' % jobID)
        else:
            self._setSchedulingStatusMessage('Not enough cores to run job %s' % jobID)

        # If we get here, we didn't succeed or fail starting the job.
        # We didn't manage to get the resources.
        # Report that.
        return None

    def _handleChild(self, pid: int) -> None:
        """
        Handle a child process PID that has finished.
        The PID must be for a child job we started.
        Not thread safe to run at the same time as we are making more children.

        Remove the child from our bookkeeping structures and free its resources.
        """

        # Look up the child
        popen = self.children[pid]
        jobID = self.childToJob[pid]
        info = self.runningJobs[jobID]

        # Unpack the job resources
        (coreFractions, jobMemory, jobDisk) = info.resources

        # Clean up our records of the job.
        self.runningJobs.pop(jobID)
        self.childToJob.pop(pid)
        self.children.pop(pid)

        if popen.returncode is None or not callable(getattr(os, 'waitid', None)):
            # It isn't reaped yet, or we have to reap all children to see if thay're done.
            # Before we reap it (if possible), kill its PID as a PGID to make sure
            # it isn't leaving children behind.
            # TODO: This is a PGID re-use risk on Mac because the process is
            # reaped already and the PGID may have been reused.
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                # It is dead already
                pass
            except PermissionError:
                # It isn't ours actually. Ours is dead.
                pass

        # See how the child did, and reap it.
        statusCode = popen.wait()
        if statusCode != 0 and not info.killIntended:
            log.error("Got exit code %i (indicating failure) "
                      "from job %s.", statusCode, self.jobs[jobID])
        if not info.killIntended:
            # Report if the job failed and we didn't kill it.
            # If we killed it then it shouldn't show up in the queue.
            self.outputQueue.put(UpdatedBatchJobInfo(jobID=jobID, exitStatus=statusCode, wallTime=time.time() - info.time, exitReason=None))

        # Make absolutely sure all processes in the group have received their
        # kill signals and been cleaned up.
        # TODO: this opens a PGID reuse risk; we reaped the process and its
        # PGID may have been re-used. But it probably hasn't been and we
        # definitely want to make sure all its children died before saying the
        # job is done. Some might not be dead yet if we don't do this.
        # TODO: can we safely do this before reaping? Or would we sit forever
        # signaling a dead but unreaped process?
        try:
            while True:
                # Send a kill to the group again, to see if anything in it
                # is still alive. Our first kill might not have been
                # delivered yet.
                os.killpg(pid, signal.SIGKILL)
                # If that worked it is still alive, so wait for the kernel
                # to stop fooling around and kill it.
                log.warning('Sent redundant job completion kill to surviving process group %s known to batch system %s', pid, id(self))
                time.sleep(0.1)
        except ProcessLookupError:
            # It is dead already
            pass
        except PermissionError:
            # It isn't ours actually. Ours is dead.
            pass

        # Free up the job's resources.
        self.coreFractions.release(coreFractions)
        self.memory.release(jobMemory)
        self.disk.release(jobDisk)

        log.debug('Child %d for job %s succeeded', pid, jobID)

    def issueBatchJob(self, jobDesc, job_environment: Optional[Dict[str, str]] = None):
        """Adds the command and resources to a queue to be run."""

        self._checkOnDaddy()

        # Round cores to minCores and apply scale.
        # Make sure to give minCores even if asked for 0 cores, or negative or something.
        cores = max(math.ceil(jobDesc.cores * self.scale / self.minCores) * self.minCores, self.minCores)

        # Don't do our own assertions about job size vs. our configured size.
        # The abstract batch system can handle it.
        self.checkResourceRequest(jobDesc.memory, cores, jobDesc.disk, job_name=jobDesc.jobName,
                                  detail=f'Scale is set to {self.scale}.')
        log.debug(f"Issuing the command: {jobDesc.command} with "
                  f"memory: {jobDesc.memory}, cores: {cores}, disk: {jobDesc.disk}")
        with self.jobIndexLock:
            jobID = self.jobIndex
            self.jobIndex += 1
        self.jobs[jobID] = jobDesc.command

        environment = self.environment.copy()
        if job_environment:
            environment.update(job_environment)

        if self.debugWorker:
            # Run immediately, blocking for return.
            # Ignore resource requirements; we run one job at a time
            self._runDebugJob(jobDesc.command, jobID, environment)
        else:
            # Queue the job for later
            self.inputQueue.put((jobDesc.command, jobID, cores, jobDesc.memory,
                                jobDesc.disk, environment))

        return jobID

    def killBatchJobs(self, jobIDs: Sequence[str]) -> None:
        """Kills jobs by ID."""

        self._checkOnDaddy()

        log.debug('Killing jobs: {}'.format(jobIDs))

        # Collect the popen handles for the jobs we have to stop
        popens: List[subprocess.Popen] = []

        for jobID in jobIDs:
            if jobID in self.runningJobs:
                info = self.runningJobs[jobID]
                info.killIntended = True
                if info.popen is not None:
                    popens.append(info.popen)
                else:
                    # No popen if running in forkless mode currently
                    assert self.debugWorker
                    log.critical("Can't kill job: %s in debug mode" % jobID)

        # Stop them all in a batch. Don't reap, because we need the daddy
        # thread to reap them to mark the jobs as not running anymore.
        self._stop_now(popens)

        for jobID in jobIDs:
            while jobID in self.runningJobs:
                # Wait for the daddy thread to collect them.
                time.sleep(0.01)

    def getIssuedBatchJobIDs(self):
        """Just returns all the jobs that have been run, but not yet returned as updated."""

        self._checkOnDaddy()

        return list(self.jobs.keys())

    def getRunningBatchJobIDs(self):

        self._checkOnDaddy()

        now = time.time()
        return {jobID: now - info.time for jobID, info in list(self.runningJobs.items())}

    def shutdown(self):
        """
        Cleanly terminate and join daddy thread.
        """

        if self.daddyThread is not None:
            # Tell the daddy thread to stop.
            self.shuttingDown.set()
            # Wait for it to stop.
            self.daddyThread.join()

        BatchSystemSupport.workerCleanup(self.workerCleanupInfo)

    def getUpdatedBatchJob(self, maxWait):
        """Returns a tuple of a no-longer-running job, the return value of its process, and its runtime, or None."""

        self._checkOnDaddy()

        try:
            item = self.outputQueue.get(timeout=maxWait)
        except Empty:
            return None
        self.jobs.pop(item.jobID)
        log.debug("Ran jobID: %s with exit value: %i", item.jobID, item.exitStatus)
        return item

    @classmethod
    def setOptions(cls, setOption):
        setOption("scale", default=1)


class Info:
    """
    Record for a running job.

    Stores the start time of the job, the Popen object representing its child
    (or None), the tuple of (coreFractions, memory, disk) it is using (or
    None), and whether the job is supposed to be being killed.
    """
    # Can't use namedtuple here since killIntended needs to be mutable
    def __init__(self, startTime, popen, resources, killIntended):
        self.time = startTime
        self.popen = popen
        self.resources = resources
        self.killIntended = killIntended


class ResourcePool:
    """
    Represents an integral amount of a resource (such as memory bytes).

    Amounts can be acquired immediately or with a timeout, and released.

    Provides a context manager to do something with an amount of resource
    acquired.
    """
    def __init__(self, initial_value, resourceType, timeout=5):
        super(ResourcePool, self).__init__()
        # We use this condition to signal everyone whenever some resource is released.
        # We use its associated lock to guard value.
        self.condition = Condition()
        # This records how much resource is available right now.
        self.value = initial_value
        self.resourceType = resourceType
        self.timeout = timeout

    def acquireNow(self, amount):
        """
        Reserve the given amount of the given resource.

        Returns True if successful and False if this is not possible immediately.
        """

        with self.condition:
            if amount > self.value:
                return False
            self.value -= amount
            self.__validate()
            return True


    def acquire(self, amount):
        """
        Reserve the given amount of the given resource.

        Raises AcquisitionTimeoutException if this is not possible in under
        self.timeout time.
        """
        with self.condition:
            startTime = time.time()
            while amount > self.value:
                if time.time() - startTime >= self.timeout:
                    # This means the thread timed out waiting for the resource.
                    raise self.AcquisitionTimeoutException(resource=self.resourceType,
                                                           requested=amount, available=self.value)
                # Allow self.timeout seconds to get the resource, else quit
                # through the above if condition. This wait + timeout is the
                # last thing in the loop such that a request that takes longer
                # than self.timeout due to multiple wakes under the threshold
                # are still honored.
                self.condition.wait(timeout=self.timeout)
            self.value -= amount
            self.__validate()

    def release(self, amount):
        with self.condition:
            self.value += amount
            self.__validate()
            self.condition.notify_all()

    def __validate(self):
        assert 0 <= self.value

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "ResourcePool(%i)" % self.value

    @contextmanager
    def acquisitionOf(self, amount):
        self.acquire(amount)
        try:
            yield
        finally:
            self.release(amount)

    class AcquisitionTimeoutException(Exception):
        """To be raised when a resource request times out."""
        def __init__(self, resource, requested, available):
            """
            Creates an instance of this exception that indicates which resource is insufficient for
            current demands, as well as the amount requested and amount actually available.

            :param str resource: string representing the resource type

            :param int|float requested: the amount of the particular resource requested that resulted
                   in this exception

            :param int|float available: amount of the particular resource actually available
            """
            self.requested = requested
            self.available = available
            self.resource = resource
