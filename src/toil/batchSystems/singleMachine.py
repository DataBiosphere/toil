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
import datetime
import logging
import math
import os
import signal
import subprocess
import time
import traceback
from argparse import ArgumentParser, _ArgumentGroup
from queue import Empty, Queue
from threading import Event, Lock, Thread
from typing import Dict, List, Optional, Set, Sequence, Tuple, Union

import toil
from toil import worker as toil_worker
from toil.batchSystems.abstractBatchSystem import (EXIT_STATUS_UNAVAILABLE_VALUE,
                                                   BatchSystemSupport,
                                                   ResourcePool,
                                                   ResourceSet,
                                                   UpdatedBatchJobInfo,
                                                   InsufficientSystemResources)

from toil.bus import ExternalBatchIdMessage
from toil.batchSystems.options import OptionSetter

from toil.common import SYS_MAX_SIZE, Config, Toil, fC
from toil.job import JobDescription, AcceleratorRequirement, accelerator_satisfies, Requirer
from toil.lib.accelerators import get_individual_local_accelerators, get_restrictive_environment_for_local_accelerators
from toil.lib.threading import cpu_count

logger = logging.getLogger(__name__)


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

    def __init__(
        self, config: Config, maxCores: float, maxMemory: int, maxDisk: int, max_jobs: Optional[int] = None
    ) -> None:
        self.config = config

        if max_jobs is None:
            # Use the main limit for jobs from the config
            self.max_jobs = self.config.max_jobs
        else:
            # Use the override (probably because we are the local batch system
            # and not the main one).
            self.max_jobs = max_jobs

        # Limit to the smaller of the user-imposed limit and what we actually
        # have on this machine for each resource.
        #
        # If we don't have up to the limit of the resource (and the resource
        # isn't the inlimited sentinel), warn.
        if maxCores > self.numCores:
            if maxCores != SYS_MAX_SIZE and maxCores != float('inf'):
                # We have an actually specified limit and not the default
                logger.warning('Not enough cores! User limited to %i but we only have %i.', maxCores, self.numCores)
            maxCores = self.numCores
        if maxMemory > self.physicalMemory:
            if maxMemory != SYS_MAX_SIZE:
                # We have an actually specified limit and not the default
                logger.warning('Not enough memory! User limited to %i bytes but we only have %i bytes.', maxMemory, self.physicalMemory)
            maxMemory = self.physicalMemory

        workdir = Toil.getLocalWorkflowDir(config.workflowID, config.workDir)  # config.workDir may be None; this sets a real directory
        self.physicalDisk = toil.physicalDisk(workdir)
        if maxDisk > self.physicalDisk:
            if maxDisk != SYS_MAX_SIZE:
                # We have an actually specified limit and not the default
                logger.warning('Not enough disk space! User limited to %i bytes but we only have %i bytes.', maxDisk, self.physicalDisk)
            maxDisk = self.physicalDisk

        super().__init__(config, maxCores, maxMemory, maxDisk)
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

        # A counter to generate job IDs and a lock to guard it.
        # We make sure to start this at 1 so that job IDs are never falsy.
        self.jobIndex = 1
        self.jobIndexLock = Lock()

        # A dictionary mapping batch system IDs of submitted jobs to the command line
        self.jobs: Dict[int, JobDescription] = {}

        # A queue of jobs waiting to be executed. Consumed by the daddy thread.
        self.inputQueue = Queue()

        # A queue of finished jobs. Produced by the daddy thread.
        self.outputQueue = Queue()

        # A dictionary mapping batch system IDs of currently running jobs to their Info objects
        self.runningJobs: Dict[int, Info] = {}

        # These next two are only used outside debug-worker mode

        # A dict mapping PIDs to Popen objects for running jobs.
        # Jobs that don't fork are executed one at a time in the main thread.
        self.children: Dict[int, subprocess.Popen] = {}
        # A dict mapping child PIDs to the Job IDs they are supposed to be running.
        self.childToJob: Dict[int, str] = {}

        # For accelerators, we need a collection of what each accelerator is, and an acquirable set of them.
        self.accelerator_identities = get_individual_local_accelerators()

        # Put them all organized by resource type
        self.resource_sources = [
            # A pool representing available job slots
            ResourcePool(self.max_jobs, 'job slots'),
            # A pool representing available CPU in units of minCores
            ResourcePool(int(self.maxCores / self.minCores), 'cores'),
            # A pool representing available memory in bytes
            ResourcePool(self.maxMemory, 'memory'),
            # A pool representing the available space in bytes
            ResourcePool(self.maxDisk, 'disk'),
            # And a set for acquiring individual accelerators
            ResourceSet(set(range(len(self.accelerator_identities))), 'accelerators')
        ]

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
            logger.debug('Started batch system %s in worker debug mode.', id(self))
        else:
            self.daddyThread = Thread(target=self.daddy, daemon=True)
            self.daddyThread.start()
            logger.debug('Started batch system %s in normal mode.', id(self))

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
            logger.debug('Started daddy thread for batch system %s.', id(self))

            while not self.shuttingDown.is_set():
                # Main loop

                while not self.shuttingDown.is_set():
                    # Try to start as many jobs as we can try to start
                    try:
                        # Grab something from the input queue if available.
                        args = self.inputQueue.get_nowait()
                        jobCommand, jobID, jobCores, jobMemory, jobDisk, job_accelerators, environment = args

                        coreFractions = int(jobCores / self.minCores)

                        # Try to start the child
                        result = self._startChild(jobCommand, jobID,
                            coreFractions, jobMemory, jobDisk, job_accelerators, environment)

                        if result is None:
                            # We did not get the resources to run this job.
                            # Requeue last, so we can look at the next job.
                            # TODO: Have some kind of condition the job can wait on,
                            # but without threads (queues for jobs needing
                            # cores/memory/disk individually)?
                            self.inputQueue.put(args)
                            break
                        elif result is not False:
                            #Result is a PID

                            if self._outbox is not None:
                                # Annotate the job with the PID generated.
                                self._outbox.publish(
                                   ExternalBatchIdMessage(jobID, str(result), self.__class__.__name__))

                        # Otherwise False

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
            logger.debug('Daddy thread cleaning up %d remaining children for batch system %s...', len(self.children), id(self))

            self._stop_and_wait(self.children.values())

            logger.debug('Daddy thread for batch system %s finishing because no children should now exist', id(self))

            # Then exit the thread.
            return
        except Exception as e:
            logger.critical('Unhandled exception in daddy thread for batch system %s: %s', id(self), traceback.format_exc())
            # Pass the exception back to the main thread so it can stop the next person who calls into us.
            self.daddyException = e
            raise

    def _checkOnDaddy(self):
        if self.daddyException is not None:
            # The daddy thread broke and we cannot do our job
            logger.critical('Propagating unhandled exception in daddy thread to main thread')
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
                logger.debug('Send shutdown kill to process group %s known to batch system %s', pgid, id(self))
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

    def _stop_and_wait(self, popens: Sequence[subprocess.Popen], timeout: int = 5) -> None:
        """
        Stop the given child processes and all their children. Blocks until the
        processes are gone or timeout is passed.

        :param popens: The processes to stop and wait on.
        :param timeout: The number of seconds to wait for all process groups to
                        be gone.
        """

        pgids = self._stop_now(popens)

        for popen in popens:
            # Wait on all the children
            popen.wait()
            logger.debug('Process %s known to batch system %s is stopped; it returned %s',
                         popen.pid, id(self), popen.returncode)

        # Make sure all child processes have received their kill signal
        self._wait_for_death(pgids, timeout)

    def _wait_for_death(self, pgids: List[int], timeout: int = 5):
        """
        Wait for the process groups to be killed. Blocks until the processes
        are gone or timeout is passed.

        :param pgids: The list of process group ids.
        :param timeout: The number of seconds to wait for all process groups to
                        be gone.
        """
        # TODO: this opens a PGID reuse risk; someone else might've reaped the
        #  process and its PGID may have been re-used.

        start = datetime.datetime.now()
        while len(pgids) > 0 and (datetime.datetime.now() - start).total_seconds() < timeout:
            new_pgids: List[int] = []
            for pgid in pgids:
                try:
                    # Send a kill to the group again, to see if anything in it
                    # is still alive. Our first kill might not have been
                    # delivered yet.
                    os.killpg(pgid, signal.SIGKILL)

                    # If we reach here, something in the process group still
                    # exists.
                    new_pgids.append(pgid)
                except ProcessLookupError:
                    # The group is actually gone now.
                    pass
                except PermissionError:
                    # The group is not only gone but reused
                    pass

            pgids = new_pgids
            if len(pgids) > 0:
                time.sleep(0.1)

        if len(pgids) > 0:
            # If any processes are still alive, let user know that we may leave
            # behind dead but unreaped processes.
            logger.warning('Processes were not reaped in groups: %s.', str(pgids))
            logger.warning('Make sure your jobs are cleaning up child processes appropriately to avoid zombie '
                           'processes possibly being left behind.')

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
                    logger.debug('Child %d has stopped', pid)

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

    def check_resource_request(self, requirer: Requirer) -> None:
        try:
            super().check_resource_request(requirer)
        except InsufficientSystemResources as e:
            # Tack the scale onto the exception
            e.details.append(f'Scale is set to {self.scale}.')
            raise e

    def _check_accelerator_request(self, requirer: Requirer) -> None:
        _, problem = self._identify_sufficient_accelerators(requirer.accelerators, set(range(len(self.accelerator_identities))))
        if problem is not None:
            # We can't get the accelerators
            raise InsufficientSystemResources(requirer, 'accelerators', self.accelerator_identities, details=[
                f'The accelerator {problem} could not be provided.'
            ])


    def _release_acquired_resources(self, resources: List[Union[int, Set[int]]]) -> None:
        """
        Release all resources acquired for a job.
        Assumes resources are in the order: core fractions, memory, disk, accelerators.
        """

        # What pools and sets do we want resources from

        for resource, request in zip(self.resource_sources, resources):
            assert ((isinstance(resource, ResourcePool) and isinstance(request, int)) or
                    (isinstance(resource, ResourceSet) and isinstance(request, set)))
            resource.release(request)

    def _identify_sufficient_accelerators(self, needed_accelerators: List[AcceleratorRequirement], available_accelerator_ids: Set[int]) -> Tuple[Optional[Set[int]], Optional[AcceleratorRequirement]]:
        """
        Given the accelerator requirements of a job, and the set of available
        accelerators out of our associated collection of accelerators, find a
        set of the available accelerators that satisfies the job's
        requirements.

        Returns that set and None if the set exists, or None and an unsatisfied
        AcceleratorRequirement if it does not.

        TODO: Uses a simple greedy algorithm and not a smart matching
        algorithm, so if the job requires different kinds of accelerators, and
        some accelerators available can match multiple requirements, then it is
        possible that a solution will not be found.

        Ignores accelerator model constraints.
        """
        accelerators_needed: Set[int] = set()
        accelerators_still_available = set(available_accelerator_ids)
        for requirement in needed_accelerators:
            for i in range(requirement['count']):
                # For each individual accelerator we need
                satisfied = False
                for candidate_index in accelerators_still_available:
                    # Check all the ones we haven't grabbed yet
                    # TODO: We'll re-check early ones against this requirement if it has a count of more than one.
                    candidate = self.accelerator_identities[candidate_index]
                    if accelerator_satisfies(candidate, requirement, ignore=['model']):
                        # If this accelerator can satisfy one unit of this requirement.
                        # We ignore model constraints because as a single
                        # machine we can't really determine the models of
                        # installed accelerators in a way consistent with how
                        # workflows are going to name them, and usually people
                        # use one model anyway.

                        # Say we want it
                        accelerators_needed.add(candidate_index)
                        accelerators_still_available.remove(candidate_index)
                        # And move on to the next required unit
                        satisfied = True
                        break
                if not satisfied:
                    # We can't get the resources we need to run right now.
                    return None, requirement
        # If we get here we satisfied everything
        return accelerators_needed, None

    def _startChild(self, jobCommand, jobID, coreFractions, jobMemory, jobDisk, job_accelerators: List[AcceleratorRequirement], environment):
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

        # And what do we want from each resource in self.resource_sources?
        # We know they go job slot, cores, memory, disk, accelerators.
        resource_requests: List[Union[int, Set[int]]] = [1, coreFractions, jobMemory, jobDisk]

        # Keep a reference to the accelerators separately
        accelerators_needed = None

        if job_accelerators:
            # Try and find some accelerators to use.
            # Start with all the accelerators that are free right now
            accelerator_set : ResourceSet = self.resource_sources[-1]
            snapshot = accelerator_set.get_free_snapshot()
            # And build a plan of the ones we want
            accelerators_needed, problem = self._identify_sufficient_accelerators(job_accelerators, snapshot)
            if accelerators_needed is not None:
                # Now we have a plan to get the accelerators we need.
                resource_requests.append(accelerators_needed)
            else:
                # We couldn't make a plan; the accelerators are busy
                assert problem is not None
                logger.debug('Accelerators are busy: %s', problem)
                self._setSchedulingStatusMessage('Not enough accelerators to run job %s' % jobID)
                return None


        acquired = []
        for source, request in zip(self.resource_sources, resource_requests):
            # For each kind of resource we want, go get it
            assert ((isinstance(source, ResourcePool) and isinstance(request, int)) or
                    (isinstance(source, ResourceSet) and isinstance(request, set)))
            if source.acquireNow(request):
                acquired.append(request)
            else:
                # We can't get everything
                self._setSchedulingStatusMessage('Not enough {} to run job {}'.format(source.resource_type, jobID))
                self._release_acquired_resources(acquired)
                return None

        # Now we have all the resources!

        # Prepare the environment
        child_environment = dict(os.environ, **environment)

        # Communicate the accelerator resources, if any, to the child process
        # by modifying the environemnt
        accelerators_acquired: Set[int] = accelerators_needed if accelerators_needed is not None else set()
        child_environment.update(get_restrictive_environment_for_local_accelerators(accelerators_acquired))

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
                                     env=child_environment,
                                     start_new_session=True)
        except Exception:
            # If the job can't start, make sure we release resources now
            self._release_acquired_resources(acquired)

            logger.error('Could not start job %s: %s', jobID, traceback.format_exc())

            # Report as failed.
            self.outputQueue.put(UpdatedBatchJobInfo(jobID=jobID, exitStatus=EXIT_STATUS_UNAVAILABLE_VALUE, wallTime=0, exitReason=None))

            # Complain it broke.
            return False
        else:
            # If the job did start, record it
            self.children[popen.pid] = popen
            # Make sure we can look it up by PID later
            self.childToJob[popen.pid] = jobID
            # Record that the job is running, and the resources it is using
            info = Info(startTime, popen, acquired, killIntended=False)
            self.runningJobs[jobID] = info

            logger.debug('Launched job %s as child %d', jobID, popen.pid)

            # Report success starting the job
            # Note that if a PID were somehow 0 it would look like False
            assert popen.pid != 0
            return popen.pid

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

        # Get the job resources reserved by the job
        acquired = info.resources


        # Clean up our records of the job.
        self.runningJobs.pop(jobID)
        self.childToJob.pop(pid)
        self.children.pop(pid)

        if popen.returncode is None or not callable(getattr(os, 'waitid', None)):
            # It isn't reaped yet, or we have to reap all children to see if thay're done.
            # Before we reap it (if possible), kill its PID as a PGID to make sure
            # it isn't leaving children behind.
            # TODO: This is a PGID re-use risk on Mac because the process is
            #  reaped already and the PGID may have been reused.
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
            logger.error("Got exit code %i (indicating failure) "
                      "from job %s.", statusCode, self.jobs[jobID])
        if not info.killIntended:
            # Report if the job failed and we didn't kill it.
            # If we killed it then it shouldn't show up in the queue.
            self.outputQueue.put(UpdatedBatchJobInfo(jobID=jobID, exitStatus=statusCode, wallTime=time.time() - info.time, exitReason=None))

        # Last attempt to make sure all processes in the group have received
        # their kill signals.
        self._wait_for_death([pid])

        # Free up the job's resources.
        self._release_acquired_resources(acquired)

        logger.debug('Child %d for job %s succeeded', pid, jobID)

    def issueBatchJob(self, jobDesc: JobDescription, job_environment: Optional[Dict[str, str]] = None) -> int:
        """Adds the command and resources to a queue to be run."""

        self._checkOnDaddy()

        # Apply scale in cores
        scaled_desc = jobDesc.scale('cores', self.scale)
        # Round cores up to multiples of minCores
        scaled_desc.cores = max(math.ceil(scaled_desc.cores / self.minCores) * self.minCores, self.minCores)

        # Don't do our own assertions about job size vs. our configured size.
        # The abstract batch system can handle it.
        self.check_resource_request(scaled_desc)
        logger.debug(f"Issuing the command: {jobDesc.command} with {scaled_desc.requirements_string()}")
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
            self.inputQueue.put((jobDesc.command, jobID, scaled_desc.cores, scaled_desc.memory,
                                scaled_desc.disk, scaled_desc.accelerators, environment))

        return jobID

    def killBatchJobs(self, jobIDs: List[int]) -> None:
        """Kills jobs by ID."""

        self._checkOnDaddy()

        logger.debug(f'Killing jobs: {jobIDs}')

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
                    logger.critical("Can't kill job: %s in debug mode" % jobID)

        # Stop them all in a batch. Don't reap, because we need the daddy
        # thread to reap them to mark the jobs as not running anymore.
        self._stop_now(popens)

        for jobID in jobIDs:
            while jobID in self.runningJobs:
                # Wait for the daddy thread to collect them.
                time.sleep(0.01)

    def getIssuedBatchJobIDs(self) -> List[int]:
        """Just returns all the jobs that have been run, but not yet returned as updated."""

        self._checkOnDaddy()

        return list(self.jobs.keys())

    def getRunningBatchJobIDs(self) -> Dict[int, float]:

        self._checkOnDaddy()

        now = time.time()
        return {jobID: now - info.time for jobID, info in list(self.runningJobs.items())}

    def shutdown(self) -> None:
        """Terminate cleanly and join daddy thread."""
        if self.daddyThread is not None:
            # Tell the daddy thread to stop.
            self.shuttingDown.set()
            # Wait for it to stop.
            self.daddyThread.join()

        BatchSystemSupport.workerCleanup(self.workerCleanupInfo)

    def getUpdatedBatchJob(self, maxWait: int) -> Optional[UpdatedBatchJobInfo]:
        """Returns a tuple of a no-longer-running job, the return value of its process, and its runtime, or None."""

        self._checkOnDaddy()

        try:
            item = self.outputQueue.get(timeout=maxWait)
        except Empty:
            return None
        self.jobs.pop(item.jobID)
        logger.debug("Ran jobID: %s with exit value: %i", item.jobID, item.exitStatus)
        return item

    @classmethod
    def add_options(cls, parser: Union[ArgumentParser, _ArgumentGroup]) -> None:
        parser.add_argument("--scale", dest="scale", default=1,
                            help="A scaling factor to change the value of all submitted tasks's submitted cores.  "
                                 "Used in the single_machine batch system. Useful for running workflows on "
                                 "smaller machines than they were designed for, by setting a value less than 1. "
                                 "(default: %(default)s)")

    @classmethod
    def setOptions(cls, setOption: OptionSetter):
        setOption("scale", float, fC(0.0), default=1)


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
