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
from builtins import zip
from builtins import map
from builtins import str
import collections
import enum
import importlib
import inspect
import itertools
import logging
import sys
import os
import time
import dill
import tempfile

try:
    import cPickle as pickle
except ImportError:
    import pickle

from abc import ABCMeta, abstractmethod
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from contextlib import contextmanager
from io import BytesIO
import uuid

# Python 3 compatibility imports
from six import iteritems, string_types

from toil.lib.expando import Expando
from toil.lib.humanize import human2bytes

from toil.common import Toil, addOptions, safeUnpickleFromStream
from toil.deferred import DeferredFunction
from toil.fileStores import FileID
from toil.lib.bioio import (setLoggingFromOptions,
                            getTotalCpuTimeAndMemoryUsage,
                            getTotalCpuTime)
from toil.resource import ModuleDescriptor
from future.utils import with_metaclass

logger = logging.getLogger( __name__ )

class FakeID:
    """
    Placeholder for a job ID used by a JobDescription that has not yet been
    registered with any JobStore.
    
    Needs to be held:
        * By JobDescription objects to record normal relationships.
        * By Jobs to key their connected-component registries and to record
          predecessor relationships to facilitate EncapsulatedJob adding
          itself as a child.
        * By Services to tie back to their hosting jobs, so the service
          tree can be built up from Service objects.
    """
    def __init__(self):
        """
        Assign a unique temporary ID that won't collide with anything.
        """
        self._value = uuid.uuid4()
        
    def __str__(self):
        return self.__repr__()
        
    def __repr__(self):
        return f'FakeID({self._value})'
        
    def __hash__(self):
        return hash(self._value)
        
    def __eq__(self, other):
        return isinstance(other, FakeID) and self._value == other._value
        
    def __ne__(self, other):
        return not isinstance(other, FakeID) or self._value != other._value

class Phase(enum.IntEnum):
    """
    Successor-running phases that a described job can be in.
    
    Increases in value as the job becomes more complete, so you can use >
    to test if a phase is complete, and <= to test if it isn't.
    """
    # Job or its children need to run
    children = 1
    # All children are done, follow-ons need to run
    followOns = 2
    # All children and follow-ons are done
    done = 3

class JobDescription:
    """
    Stores all the information that the Toil Leader ever needs to know about a
    Job: requirements information, dependency information, commands to issue,
    etc.
    
    Can be obtained from an actual (i.e. executable) Job object, and can be
    used to obtain the Job object from the JobStore.
    
    Never contains other Jobs or JobDescriptions: all reference is by ID.
    
    Subclassed into variants for checkpoint jobs and service jobs that have
    their specific parameters.
    """
    
    def __init__(self, requirements=None, unitName='', displayName='', jobName=''):
        """
        Create a new JobDescription.
        
        :param dict|None requirements: Dict from string to number, string, or bool
            describing the resource requirments of the job. 'cores', 'memory',
            'disk', and 'preemptable' fields, if set, are parsed and broken out
            into properties. If unset, the relevant property will be
            unspecified, and will be pulled from the assigned Config object if
            queried (see :meth:`toil.job.JobDescription.assignConfig`). If
            unspecified and no Config object is assigned, an AttributeError
            will be raised at query time.
        :param str|None unitName: Name of this instance of this kind of job. May
            appear with jobName in logging.
        :param str|None displayName: A human-readable name to identify this
            particular job instance. Ought to be the job class's name
            if no real user-defined name is available.
        :param str|None jobName: Name of the kind of job this is. May be used in job
            store IDs and logging. Ought to be the job class's name if no real
            user-defined name is available.
        :param int predecessorNumber: Number of total predecessors
            that must finish before the described Job from being scheduled.
        """
        
        # Fill in default values
        if requirements is None:
            requirements = {}
        
        # Save requirements, parsing and validating anything that needs parsing or validating
        self._requirementOverrides = {k: self._parseResource(k, v) for (k, v) in requirements.items()}
        
        # Save names, making sure they are strings and not e.g. bytes.
        def makeString(x):
            return x if not isinstance(x, bytes) else x.decode('utf-8', errors='replace')
        self.unitName = makeString(unitName)
        self.displayName = makeString(displayName)
        self.jobName = makeString(jobName)
    
        # Set properties that are not fully filled in on creation.
        
        # ID of this job description in the JobStore.
        self.jobStoreID = FakeID()
        
        # Mostly fake, not-really-executable command string that encodes how to
        # find the Job body data that this JobDescription describes, and the
        # module(s) needed to unpickle it.
        #
        # Gets replaced with/rewritten into the real, executable command when
        # the leader passes the description off to the batch system to be
        # executed.
        self.command = None
        
        
        # Set scheduling properties that the leader read to think about scheduling.
        
        # The number of times the job should be retried if it fails This number is reduced by
        # retries until it is zero and then no further retries are made. If
        # None, taken as the default value for this workflow execution.
        self.remainingRetryCount = None
        
        # Holds FileStore FileIDs of the files that this job has deleted. Used
        # to journal deletions of files and recover from a worker crash between
        # committing a JobDescription update and actually executing the
        # requested deletions.
        self.filesToDelete = [] 
        
        # The number of direct predecessors of the job. Needs to be stored at
        # the JobDescription to support dynamically-created jobs with multiple
        # predecessors. Otherwise, we could reach a job by one path down from
        # the root and decide to schedule it without knowing that it is also
        # reachable from other paths down from the root.
        self.predecessorNumber = 0
        
        # Note that we don't hold IDs of our predecessors. Predecessors know
        # about us, and not the other way around. Otherwise we wouldn't be able
        # to save ourselves to the job store until our predecessors were saved,
        # but they'd also be waiting on us.
       
        # The IDs of all child jobs of the described job.
        # Children which are done may have been removed.
        self.childIDs = set()
        
        # The IDs of all follow-on jobs of the described job.
        # Follow-ons which are done may have been removed.
        self.followOnIDs = set()
        
        # Phase we are at in running this job and its successors.
        self._successorPhase = Phase.children
        
        # Dict from ServiceHostJob ID to list of child ServiceHostJobs that start after it.
        # All services must have an entry, if only to an empty list.
        self.serviceTree = {}
       
        # A jobStoreFileID of the log file for a job. This will be None unless the job failed and
        # the logging has been captured to be reported on the leader.
        self.logJobStoreFileID = None 
        
        # Now set properties that don't really describe the job but hook us up
        # to contextual state so our properties and methods can use it.
    
        # We can have a toil.common.Config assigned to fill in default values for
        # requirements not explicitly specified.
        self._config = None
        
    def serviceHostIDsInBatches(self):
        """
        Get an iterator over all batches of service host job IDs that can be
        started at the same time, in the order they need to start in. 
        """
      
        # First start all the jobs with no parent
        roots = set(self.serviceTree.keys())
        for parent, children in self.serviceTree:
            for child in children:
                roots.remove(child)
        batch = list(roots)
        if len(batch) > 0:
            # If there's a first batch, yield it
            yield batch
        
        while len(batch) > 0:
            nextBatch = []
            for started in batch:
                # Go find all the children that can start now that we have started.
                for child in self.serviceTree[started]:
                    nextBatch.append(child)
            
            batch = nextBatch
            if len(batch) > 0:
                # Emit the batch if nonempty
                yield batch
            
    def successorsAndServiceHosts(self):
        """
        Get an iterator over all child, follow-on, and service job IDs
        """
        return itertools.chain(self.childIDs, self.followOnIDs, self.serviceTree.keys())
        
    @property
    def services(self):
        """
        Get a collection of the IDs of service host jobs for this job, in arbitrary order.
        
        Will be empty if the job has no unfinished services.
        """
        
        return self.serviceTree.keys()
        
    def nextSuccessors(self):
        """
        Return the collection of job IDs for the successors of this job that,
        according to this job, are ready to run.
        
        If those jobs have multiple predecessor relationships, they may still
        be blocked on other jobs.
        
        Returns None when at the final phase (all successors done), and an
        empty collection when no successors remain in the current phase and the
        phase can be completed.
        
        See completePhase().
        """
        
        if self._successorPhase == Phase.followOns:
            return self.followOnIDs
        elif self._successorPhase == Phase.children:
            return self.childIDs
        elif self._successorPhase == Phase.done:
            return None
        else:
            raise RuntimeError("Unknown phase: {}".format(self._successorPhase))
            
    @property
    def stack(self):
        """
        Get an immutable collection of immutable collections of IDs of successors that need to run still.
        
        Batches of successors are in reverse order of the order they need to run in.
        
        Some successors in each batch may have already been finished. Batches may be empty.
        
        Exists so that code that used the old stack list immutably can work
        still. New development should use nextSuccessors(), and all
        mutations should use filterSuccessors() and completePhase().
        
        :return: Batches of successors that still need to run, in reverse order. 
        :rtype: tuple(tuple(str))
        """
        
        result = []
        if self._successorPhase <= Phase.followOns:
            # Follow-ons haven't all finished yet
            result.append(tuple(self.followOnIDs))
        if self._successorPhase <= Phase.children:
            # Children haven't all finished yet
            result.append(tuple(self.childIDs))
        return tuple(result)
        
    def filterSuccessors(self, predicate):
       """
       Keep only successor jobs for which the given predicate function returns True when called with the job's ID.
       
       Treats all other successors as complete and forgets them.
       """
       
       self.childIDs = {x for x in self.childIDs if predicate(x)}
       self.followOnIDs = {x for x in self.followOnIDs if predicate(x)}
       
    def filterServiceHosts(self, predicate):
       """
       Keep only services for which the given predicate function returns True when called with the service host job's ID.
       
       Treats all other services as complete and forgets them.
       """
       
       # TODO: avoid duplicate predicate calls here when services are referenced?
       self.serviceTree = {k: [x for x in v if predicate(x)] for k, v in self.serviceTree.items() if predicate(k)}
       
    def completePhase(self):
        """
        Advance to the next phase of successors to run. (Start follow-ons after children are done, or mark that all follow-ons are done.)
        
        Can only be called when nextSuccessors() is an empty collection.
        """
        
        if self._successorPhase == Phase.done:
            raise RuntimeError("Cannot advance {} past done phase".format(self))
        
        # We can't be done so this can't be None
        todo = self.nextSuccessors()
        
        if len(todo) > 0:
            raise RuntimeError("Cannot complete phase {} on {} with outstanding successors: {}".format(self._successorPhase, self, todo))
        
        # Go to the next phase in the enum, possibly readying more successors to run.
        self._successorPhase += 1
        
    def clearSuccessorsAndServiceHosts(self):
        """
        Remove all references to child, follow-on, and service jobs associated with the described job.
        """
        self.childIDs = set()
        self.followOnIDs = set()
        self.serviceTree = {}
        
    
    def replace(self, other):
        """
        Take on the ID of another JobDescription, while retaining our own state and type.
        
        When updated in the JobStore, we will save over the other JobDescription.
        
        Useful for chaining jobs: the cahined job can replace the parent job.
        
        :param toil.job.JobDescription other: Job description to replace.
        """
    
        self.jobStoreID = other.jobStoreID
        
    def addChild(self, childID):
        """
        Make the job with the given ID a child of the described job.
        """
        
        self.childIDs.add(childID)
        
    def addFollowOn(self, followOnID):
        """
        Make the job with the given ID a follow-on of the described job.
        """
        
        self.followOnIDs.add(followOnID)
        
    def addServiceHostJob(self, serviceID, parentServiceID=None):
        """
        Make the ServiceHostJob with the given ID a service of the described job.
        
        If a parent ServiceHostJob ID is given, that parent service will be started
        first, and must have already been added.
        """
        
        # Make sure we aren't clobbering something
        assert serviceID not in self.serviceTree
        self.serviceTree[serviceID] = []
        if parentServiceID is not None:
            self.serviceTree[parentServiceID].append(serviceID)
            
    def hasChild(self, childID):
        """
        Return True if the job with the given ID is a child of the described job.
        """
        
        return childID in self.childIDs
        
    def hasFollowOn(self, followOnID):
        """
        Return True if the job with the given ID is a follow-on of the described job.
        """
        
        return followOnID in self.followOnIDs
        
    def hasServiceHostJob(self, serviceID):
        """
        Return True if the ServiceHostJob with the given ID is a service of the described job.
        """
        
        return serviceID in self.serviceTree
        
    def renameReferences(self, renames):
        """
        Apply the given dict of ID renames to all references to jobs. Does not
        modify our own ID or those of finished predecessors.
        
        :param dict(FakeID, str) renames: Rename operations to apply.
        """
        
        self.childIDs = {renames[old] for old in self.childIDs}
        self.followOnIDs = {renames[old] for old in self.followOnIDs}
        self.serviceTree = {renames[parent]: [renames[child] for child in children]
                            for parent, children in self.serviceTree.items()}
        
    def addPredecessor(self):
        """
        Notify the JobDescription that a predecessor has been added to its Job.
        """
        self.predecessorNumber += 1
        
    def onRegistration(self, jobStore):
        """
        Called by the Job saving logic when this JobDescription meets the JobStore and has its ID assigned.
        
        Overridden to perform setup work (like hooking up flag files for service jobs) that requires the JobStore.
        
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore: The job store we are being placed into
        """
        pass
    
    def assignConfig(self, config):
        """
        Assign the given config object to be used to provide default values for
        requirements.
        
        :param toil.common.Config config: Config object to query
        """
        self._config = config
        
    def setupJobAfterFailure(self, exitReason=None):
        """
        Reduce the remainingRetryCount if greater than zero and set the memory
        to be at least as big as the default memory (in case of exhaustion of memory,
        which is common).
        
        Requires a configuration to have been assigned (see :meth:`toil.job.JobDescription.assignConfig`).
        
        :param toil.batchSystems.abstractBatchSystem.BatchJobExitReason exitReason: The configuration for the current workflow run.
        
        """
        
        # Avoid potential circular imports
        from toil.batchSystems.abstractBatchSystem import BatchJobExitReason
        
        assert self._config is not None
        
        if self._config.enableUnlimitedPreemptableRetries and exitReason == BatchJobExitReason.LOST:
            logger.info("*Not* reducing retry count (%s) of job %s with ID %s",
                        self.remainingRetryCount, self, self.jobStoreID)
        else:
            self.remainingRetryCount = max(0, self.remainingRetryCount - 1)
            logger.warning("Due to failure we are reducing the remaining retry count of job %s with ID %s to %s",
                           self, self.jobStoreID, self.remainingRetryCount)
        # Set the default memory to be at least as large as the default, in
        # case this was a malloc failure (we do this because of the combined
        # batch system)
        if self.memory < self._config.defaultMemory:
            self.memory = self._config.defaultMemory
            logger.warning("We have increased the default memory of the failed job %s to %s bytes",
                           self, self.memory)
            
        if self.disk < self._config.defaultDisk:
            self.disk = self._config.defaultDisk
            logger.warning("We have increased the disk of the failed job %s to the default of %s bytes",
                           self, self.disk)
                           
                           
    def getLogFileHandle(self, jobStore):
        """
        Returns a context manager that yields a file handle to the log file.
        
        Assumes logJobStoreFileID is set.
        """
        return jobStore.readFileStream(self.logJobStoreFileID)
        
    @staticmethod
    def _parseResource(name, value):
        """
        Parse a Toil job's resource requirement value and apply resource-specific type checks. If the
        value is a string, a binary or metric unit prefix in it will be evaluated and the
        corresponding integral value will be returned.

        :param str name: The name of the resource
        :param None|str|float|int value: The resource value
        :rtype: int|float|None

        >>> JobDescription._parseResource('cores', None)
        >>> JobDescription._parseResource('cores', 1), JobDescription._parseResource('disk', 1), \
        JobDescription._parseResource('memory', 1)
        (1, 1, 1)
        >>> JobDescription._parseResource('cores', '1G'), JobDescription._parseResource('disk', '1G'), \
        JobDescription._parseResource('memory', '1G')
        (1073741824, 1073741824, 1073741824)
        >>> JobDescription._parseResource('cores', 1.1)
        1.1
        >>> JobDescription._parseResource('disk', 1.1) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        TypeError: The 'disk' requirement does not accept values that are of <type 'float'>
        >>> JobDescription._parseResource('memory', object()) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        TypeError: The 'memory' requirement does not accept values that are of ...
        """
        
        if value is None:
            # Anything can be None.
            return value
        
        if name in ('memory', 'disk', 'cores'):
            # These should be numbers that accept things like "5G".
            if isinstance(value, (str, bytes)):
                value = human2bytes(value)
            if isinstance(value, int):
                return value
            elif isinstance(value, float) and name == 'cores':
                # But only cores can be fractional.
                return value
            else:
                raise TypeError(f"The '{name}' requirement does not accept values that are of type {type(value)}")
        elif name == 'preemptable':
            if isinstance(value, str):
                if value.tolower() == 'true':
                    return True
                elif value.tolower() == 'false':
                    return False
                else:
                    raise ValueError(f"The '{name}' requirement must be 'true' or 'false' but is {value}")
            elif isinstance(value, bool):
                return value
            else:
                raise TypeError(f"The '{name}' requirement does not accept values that are of type {type(value)}")
        else:
            # Anything else we just pass along without opinons
            return value
        
    def _fetchRequirement(self, requirement):
        """
        Get the value of the specified requirement ('blah') by looking it up in
        our requirement storage and querying 'defaultBlah' on the config if it
        isn't set. If the config would be queried but isn't associated, raises
        AttributeError.
        """
        if requirement in self._requirementOverrides:
            return self._requirementOverrides[requirement]
        elif self._config is not None:
            return getattr(self._config, 'default' + requirement.capitalize())
        else:
            raise AttributeError("Default value for '{}' cannot be determined".format(requirement))
        
    @property
    def disk(self):
        """
        The maximum number of bytes of disk the job will require to run.
        """
        return self._fetchRequirement('disk')
    @disk.setter
    def set_disk(self, val):
         self._requirementOverrides['disk'] = self._parseResource('disk', val)

    @property
    def memory(self):
        """
        The maximum number of bytes of memory the job will require to run.
        """
        return self._fetchRequirement('memory')
    @memory.setter
    def set_memory(self, val):
         self._requirementOverrides['memory'] = self._parseResource('memory', val)

    @property
    def cores(self):
        """
        The number of CPU cores required.
        """
        return self._fetchRequirement('cores')
    @cores.setter
    def set_cores(self, val):
         self._requirementOverrides['cores'] = self._parseResource('cores', val)

    @property
    def preemptable(self):
        """
        Whether the job can be run on a preemptable node.
        """
        return self._fetchRequirement('preemptable')
    @preemptable.setter
    def set_preemptable(self, val):
         self._requirementOverrides['preemptable'] = self._parseResource('preemptable', val)
        
    def __str__(self):
        """
        Produce a useful logging string identifying this job.
        """
        
        printedName = "'" + self.jobName + "'"
        if self.unitName:
            printedName += ' ' + self.unitName
        elif self.unitName == '':
            printedName += ' ' + '(unnamed)'
        
        if self.jobStoreID is not None:
            printedName += ' ' + self.jobStoreID
        
        return printedName
        
    # Not usable as a key (not hashable) and doesn't have any value-equality.
    # There really should only ever be one true version of a JobSescription at
    # a time, keyed by jobStoreID.

    def __repr__(self):
        return '%s( **%r )' % (self.__class__.__name__, self.__dict__)
        
        
class ServiceJobDescription(JobDescription):
    """
    A description of a job that hosts a service.
    """
    
    def __init__(self, *args, **kwargs):
        """
        Create a ServiceJobDescription to describe a ServiceHostJob.
        """
        
        # Make the base JobDescription
        super().__init__(*args, **kwargs)
        
        # Set service-specific properties
    
        # An empty file in the jobStore which when deleted is used to signal that the service
        # should cease.
        self.terminateJobStoreID = None
        
        # Similarly a empty file which when deleted is used to signal that the service is
        # established
        self.startJobStoreID = None
        
        # An empty file in the jobStore which when deleted is used to signal that the service
        # should terminate signaling an error.
        self.errorJobStoreID = None
        
    def onRegistration(self, jobStore):
        """
        When a ServiceJobDescription first meets the JobStore, it needs to set up its flag files.
        """
        super().onRegistration(jobStore)
        
        self.startJobStoreID = jobStore.getEmptyFileStoreID()
        self.terminateJobStoreID = jobStore.getEmptyFileStoreID()
        self.errorJobStoreID = jobStore.getEmptyFileStoreID()
    
class CheckpointJobDescription(JobDescription):
    """
    A description of a job that is a checkpoint.
    """
    
    def __init__(self, *args, **kwargs):
        """
        Create a CheckpointJobDescription to describe a checkpoint job.
        """
        
        # Make the base JobDescription
        super().__init__(*args, **kwargs)
        
        # Set checkpoint-specific properties
        
        # None, or a copy of the original command string used to reestablish the job after failure.
        self.checkpoint = None
        
        # Files that can not be deleted until the job and its successors have completed
        self.checkpointFilesToDelete = []

        # Human-readable names of jobs that were run as part of this job's
        # invocation, starting with this job
        self.chainedJobs = []
        
    def restartCheckpoint(self, jobStore):
        """
        Restart a checkpoint after the total failure of jobs in its subtree.

        Writes the changes to the jobStore immediately. All the
        checkpoint's successors will be deleted, but its retry count
        will *not* be decreased.

        Returns a list with the IDs of any successors deleted.
        """
        assert self.checkpoint is not None
        successorsDeleted = []
        if self.childIDs or self.followOnIDs or self.serviceTree or self.command != None:
            if self.command != None:
                assert self.command == self.checkpoint
                logger.debug("Checkpoint job already has command set to run")
            else:
                self.command = self.checkpoint

            jobStore.update(self) # Update immediately to ensure that checkpoint
            # is made before deleting any remaining successors

            if self.childIDs or self.followOnIDs or self.serviceTree:
                # If the subtree of successors is not complete restart everything
                logger.debug("Checkpoint job has unfinished successor jobs, deleting children: %s, followOns: %s, services: %s " %
                             (self.childIDs, self.followOnIDs, self.serviceTree.keys()))
                # Delete everything on the stack, as these represent successors to clean
                # up as we restart the queue
                def recursiveDelete(jobDesc):
                    # Recursive walk the stack to delete all remaining jobs
                    for otherJobID in jobDesc.successorsAndServiceHosts():
                        if jobStore.exists(otherJobID):
                            recursiveDelete(jobStore.load(otherJobID))
                        else:
                            logger.debug("Job %s has already been deleted", otherJobID)
                    if jobDesc.jobStoreID != self.jobStoreID:
                        # Delete everything under us except us.
                        logger.debug("Checkpoint is deleting old successor job: %s", jobDesc.jobStoreID)
                        jobStore.delete(jobDesc.jobStoreID)
                        successorsDeleted.append(jobDesc.jobStoreID)
                recursiveDelete(self)
                
                # Cut links to the jobs we deleted.
                self.clearSuccessorsAndServiceHosts()

                # Update again to commit the removal of successors.
                jobStore.update(self)
        return successorsDeleted

class Job:
    """
    Class represents a unit of work in toil.
    """
    def __init__(self, memory=None, cores=None, disk=None, preemptable=None,
                       unitName=None, checkpoint=False, displayName=None,
                       descriptionClass=None):
        """
        This method must be called by any overriding constructor.

        :param memory: the maximum number of bytes of memory the job will require to run.
        :param cores: the number of CPU cores required.
        :param disk: the amount of local disk space required by the job, expressed in bytes.
        :param preemptable: if the job can be run on a preemptable node.
        :param unitName: Human-readable name for this instance of the job.
        :param checkpoint: if any of this job's successor jobs completely fails,
            exhausting all their retries, remove any successor jobs and rerun this job to restart the
            subtree. Job must be a leaf vertex in the job graph when initially defined, see
            :func:`toil.job.Job.checkNewCheckpointsAreCutVertices`.
        :param displayName: Human-readable job type display name.
        :param descriptionClass: Override for the JobDescription class used to describe the job.
        
        :type memory: int or string convertible by toil.lib.humanize.human2bytes to an int
        :type cores: int or string convertible by toil.lib.humanize.human2bytes to an int
        :type disk: int or string convertible by toil.lib.humanize.human2bytes to an int
        :type preemptable: bool
        :type unitName: str
        :type checkpoint: bool
        :type displayName: str
        :type descriptionClass: class
        """
        
        # Fill in our various names
        jobName = self.__class__.__name__
        displayName = displayName if displayName is not None else jobName
        
        
        # Build a requirements dict for the description
        requirements = {'memory': memory, 'cores': cores, 'disk': disk,
                        'preemptable': preemptable}
        if descriptionClass is None:
            if checkpoint:
                # Actually describe as a checkpoint job
                descriptionClass = CheckpointJobDescription
            else:
                # Use the normal default
                descriptionClass = JobDescription
        # Create the JobDescription that owns all the scheduling information.
        # Make it with a fake ID until we can be assigned a real one by the JobStore.
        self._description = descriptionClass(requirements=requirements, unitName=unitName, displayName=displayName, jobName=jobName)
        
        # Private class variables needed to actually execute a job, in the worker.
        # Also needed for setting up job graph structures before saving to the JobStore.
        
        # This dict holds a mapping from FakeIDs to the job objects they represent.
        # Will be shared among all jobs in a disconnected piece of the job
        # graph that hasn't been registered with a JobStore yet.
        self._registry = {}
        
        # Job relationships are all stored exactly once in the JobDescription.
        # Except for predecessor relationships which are stored here, just
        # while the user is creating the job graphs, to check for duplicate
        # relationships and to let EncapsulatedJob magically add itself as a
        # child.
        self._directPredecessors = set()
        
        # Note that self.__module__ is not necessarily this module, i.e. job.py. It is the module
        # defining the class self is an instance of, which may be a subclass of Job that may be
        # defined in a different module.
        self.userModule = ModuleDescriptor.forModule(self.__module__).globalize()
        # Maps index paths into composite return values to lists of IDs of files containing
        # promised values for those return value items. An index path is a tuple of indices that
        # traverses a nested data structure of lists, dicts, tuples or any other type supporting
        # the __getitem__() protocol.. The special key `()` (the empty tuple) represents the
        # entire return value.
        self._rvs = collections.defaultdict(list)
        self._promiseJobStore = None
        self._fileStore = None
        self._defer = None
        self._tempDir = None
        
    @property
    def jobStoreID(self):
        """
        Get the ID of this Job.
        """
        # This is managed by the JobDescription.
        return self._description.jobStoreID

    @property
    def description(self):
        """
        Expose the JobDescription that describes this job.
        """
        return self._description
        
    @property
    def checkpoint(self):
        """
        Determine if the job is a checkpoint job or not.
        """
        
        return isinstance(self._description, CheckpointJobDescription)

    def assignConfig(self, config):
        """
        Assign the given config object to be used by various actions
        implemented inside the Job class.
        
        :param toil.common.Config config: Config object to query
        """
        self._description.assignConfig(config)


    def run(self, fileStore):
        """
        Override this function to perform work and dynamically create successor jobs.

        :param toil.fileStores.abstractFileStore.AbstractFileStore fileStore: Used to create local and 
               globally sharable temporary files and to send log messages to the leader
               process.

        :return: The return value of the function can be passed to other jobs by means of
                 :func:`toil.job.Job.rv`.
        """
        pass
        
    def _jobGraphsJoined(self, other):
        """
        Called whenever the job graphs of this job and the other job may have been merged into one connected component.
        
        Ought to be called on the bigger registry first.
        
        Merges FakeID registries if needed.
        """
       
        if len(self._registry) < len(other._registry):
            # Merge into the other component instead
            other._jobGraphsJoined(self)
        else:
            if self._registry != other._registry:
                # We are in fact joining connected components.
                
                # Steal everything from the other connected component's registry
                self._registry.update(other._registry)
                
                for job in other._registry.items():
                    # Point all their jobs at the new combined registry
                    job._registry = self._registry

    def addChild(self, childJob):
        """
        Adds childJob to be run as child of this job. Child jobs will be run \
        directly after this job's :func:`toil.job.Job.run` method has completed.

        :param toil.job.Job childJob:
        :return: childJob
        :rtype: toil.job.Job
        """
        
        # Join the job graphs
        self._jobGraphsJoined(childJob)
        # Remember the child relationship
        self._description.addChild(childJob.jobStoreID)
        # Record the temporary back-reference
        childJob._addPredecessor(self)
        
        return childJob

    def hasChild(self, childJob):
        """
        Check if childJob is already a child of this job.

        :param toil.job.Job childJob:
        :return: True if childJob is a child of the job, else False.
        :rtype: bool
        """
        return self._description.hasChild(childJob.jobStoreID)

    def addFollowOn(self, followOnJob):
        """
        Adds a follow-on job, follow-on jobs will be run after the child jobs and \
        their successors have been run.

        :param toil.job.Job followOnJob:
        :return: followOnJob
        :rtype: toil.job.Job
        """
        
        # Join the job graphs
        self._jobGraphsJoined(followOnJob)
        # Remember the follow-on relationship
        self._description.addFollowOn(followOnJob.jobStoreID)
        # Record the temporary back-reference
        followOnJob._addPredecessor(self)
        
        return followOnJob

    def hasFollowOn(self, followOnJob):
        """
        Check if given job is already a follow-on of this job.

        :param toil.job.Job followOnJob:
        :return: True if the followOnJob is a follow-on of this job, else False.
        :rtype: bool
        """
        return self._description.hasChild(followOnJob.jobStoreID) 
        
    def addService(self, service, parentService=None):
        """
        Add a service.

        The :func:`toil.job.Job.Service.start` method of the service will be called
        after the run method has completed but before any successors are run.
        The service's :func:`toil.job.Job.Service.stop` method will be called once
        the successors of the job have been run.

        Services allow things like databases and servers to be started and accessed
        by jobs in a workflow.

        :raises toil.job.JobException: If service has already been made the child of a job or another service.
        :param toil.job.Job.Service service: Service to add.
        :param toil.job.Job.Service parentService: Service that will be started before 'service' is
            started. Allows trees of services to be established. parentService must be a service
            of this job.
        :return: a promise that will be replaced with the return value from
            :func:`toil.job.Job.Service.start` of service in any successor of the job.
        :rtype: toil.job.Promise
        """
        
        if parentService is not None:
            if not self.hasService(parentService):
                raise JobException("Parent service is not a service of the given job")
        
        if service.hostID is not None:
            raise JobException("Service has already been added to a job")
        
        # Create a host job for the service, ad get it an ID
        hostingJob = ServiceHostJob(service)
        self._jobGraphsJoined(hostingJob)
        
        # Record the relationship to the hosting job, with its parent if any.
        self._description.addServiceHostJob(hostingJob.jobStoreID, parentService.hostID if parentService is not None else None)
        
        # Return the promise for the service's startup result
        return hostingJob.rv()
        
    def hasService(self, service):
        """
        Returns True if the given Service is a service of this job, and False otherwise.
        """
        
        return service.hostID is None or self._description.hasServiceHostJob(service.hostID)
        
    ##Convenience functions for creating jobs

    def addChildFn(self, fn, *args, **kwargs):
        """
        Adds a function as a child job.

        :param fn: Function to be run as a child job with ``*args`` and ``**kwargs`` as \
        arguments to this function. See toil.job.FunctionWrappingJob for reserved \
        keyword arguments used to specify resource requirements.
        :return: The new child job that wraps fn.
        :rtype: toil.job.FunctionWrappingJob
        """
        if PromisedRequirement.convertPromises(kwargs):
            return self.addChild(PromisedRequirementFunctionWrappingJob.create(fn, *args, **kwargs))
        else:
            return self.addChild(FunctionWrappingJob(fn, *args, **kwargs))

    def addFollowOnFn(self, fn, *args, **kwargs):
        """
        Adds a function as a follow-on job.

        :param fn: Function to be run as a follow-on job with ``*args`` and ``**kwargs`` as \
        arguments to this function. See toil.job.FunctionWrappingJob for reserved \
        keyword arguments used to specify resource requirements.
        :return: The new follow-on job that wraps fn.
        :rtype: toil.job.FunctionWrappingJob
        """
        if PromisedRequirement.convertPromises(kwargs):
            return self.addFollowOn(PromisedRequirementFunctionWrappingJob.create(fn, *args, **kwargs))
        else:
            return self.addFollowOn(FunctionWrappingJob(fn, *args, **kwargs))

    def addChildJobFn(self, fn, *args, **kwargs):
        """
        Adds a job function as a child job. See :class:`toil.job.JobFunctionWrappingJob`
        for a definition of a job function.

        :param fn: Job function to be run as a child job with ``*args`` and ``**kwargs`` as \
        arguments to this function. See toil.job.JobFunctionWrappingJob for reserved \
        keyword arguments used to specify resource requirements.
        :return: The new child job that wraps fn.
        :rtype: toil.job.JobFunctionWrappingJob
        """
        if PromisedRequirement.convertPromises(kwargs):
            return self.addChild(PromisedRequirementJobFunctionWrappingJob.create(fn, *args, **kwargs))
        else:
            return self.addChild(JobFunctionWrappingJob(fn, *args, **kwargs))

    def addFollowOnJobFn(self, fn, *args, **kwargs):
        """
        Add a follow-on job function. See :class:`toil.job.JobFunctionWrappingJob`
        for a definition of a job function.

        :param fn: Job function to be run as a follow-on job with ``*args`` and ``**kwargs`` as \
        arguments to this function. See toil.job.JobFunctionWrappingJob for reserved \
        keyword arguments used to specify resource requirements.
        :return: The new follow-on job that wraps fn.
        :rtype: toil.job.JobFunctionWrappingJob
        """
        if PromisedRequirement.convertPromises(kwargs):
            return self.addFollowOn(PromisedRequirementJobFunctionWrappingJob.create(fn, *args, **kwargs))
        else:
            return self.addFollowOn(JobFunctionWrappingJob(fn, *args, **kwargs))

    @property
    def tempDir(self):
        """
        Shortcut to calling :func:`job.fileStore.getLocalTempDir`. Temp dir is created on first call
        and will be returned for first and future calls
        :return: Path to tempDir. See `job.fileStore.getLocalTempDir`
        :rtype: str
        """
        if self._tempDir is None:
            self._tempDir = self._fileStore.getLocalTempDir()
        return self._tempDir

    def log(self, text, level=logging.INFO):
        """
        convenience wrapper for :func:`fileStore.logToMaster`
        """
        self._fileStore.logToMaster(text, level)

    @staticmethod
    def wrapFn(fn, *args, **kwargs):
        """
        Makes a Job out of a function. \
        Convenience function for constructor of :class:`toil.job.FunctionWrappingJob`.

        :param fn: Function to be run with ``*args`` and ``**kwargs`` as arguments. \
        See toil.job.JobFunctionWrappingJob for reserved keyword arguments used \
        to specify resource requirements.
        :return: The new function that wraps fn.
        :rtype: toil.job.FunctionWrappingJob
        """
        if PromisedRequirement.convertPromises(kwargs):
            return PromisedRequirementFunctionWrappingJob.create(fn, *args, **kwargs)
        else:
            return FunctionWrappingJob(fn, *args, **kwargs)

    @staticmethod
    def wrapJobFn(fn, *args, **kwargs):
        """
        Makes a Job out of a job function. \
        Convenience function for constructor of :class:`toil.job.JobFunctionWrappingJob`.

        :param fn: Job function to be run with ``*args`` and ``**kwargs`` as arguments. \
        See toil.job.JobFunctionWrappingJob for reserved keyword arguments used \
        to specify resource requirements.
        :return: The new job function that wraps fn.
        :rtype: toil.job.JobFunctionWrappingJob
        """
        if PromisedRequirement.convertPromises(kwargs):
            return PromisedRequirementJobFunctionWrappingJob.create(fn, *args, **kwargs)
        else:
            return JobFunctionWrappingJob(fn, *args, **kwargs)

    def encapsulate(self):
        """
        Encapsulates the job, see :class:`toil.job.EncapsulatedJob`.
        Convenience function for constructor of :class:`toil.job.EncapsulatedJob`.

        :return: an encapsulated version of this job.
        :rtype: toil.job.EncapsulatedJob
        """
        return EncapsulatedJob(self)

    ####################################################
    #The following function is used for passing return values between
    #job run functions
    ####################################################

    def rv(self, *path):
        """
        Creates a *promise* (:class:`toil.job.Promise`) representing a return value of the job's
        run method, or, in case of a function-wrapping job, the wrapped function's return value.

        :param (Any) path: Optional path for selecting a component of the promised return value.
               If absent or empty, the entire return value will be used. Otherwise, the first
               element of the path is used to select an individual item of the return value. For
               that to work, the return value must be a list, dictionary or of any other type
               implementing the `__getitem__()` magic method. If the selected item is yet another
               composite value, the second element of the path can be used to select an item from
               it, and so on. For example, if the return value is `[6,{'a':42}]`, `.rv(0)` would
               select `6` , `rv(1)` would select `{'a':3}` while `rv(1,'a')` would select `3`. To
               select a slice from a return value that is slicable, e.g. tuple or list, the path
               element should be a `slice` object. For example, assuming that the return value is
               `[6, 7, 8, 9]` then `.rv(slice(1, 3))` would select `[7, 8]`. Note that slicing
               really only makes sense at the end of path.

        :return: A promise representing the return value of this jobs :meth:`toil.job.Job.run`
                 method.

        :rtype: toil.job.Promise
        """
        return Promise(self, path)

    def registerPromise(self, path):
        if self._promiseJobStore is None:
            raise RuntimeError('Trying to pass a promise from a promising job that is not a ' +
                               'predecessor of the job receiving the promise')
        # TODO: can we guarantee self.jobStoreID is populated and so pass that here?
        with self._promiseJobStore.writeFileStream() as (fileHandle, jobStoreFileID):
            promise = UnfulfilledPromiseSentinel(str(self), False)
            pickle.dump(promise, fileHandle, pickle.HIGHEST_PROTOCOL)
        self._rvs[path].append(jobStoreFileID)
        return self._promiseJobStore.config.jobStore, jobStoreFileID

    def prepareForPromiseRegistration(self, jobStore):
        """
        Ensure that a promise by this job (the promissor) can register with the promissor when
        another job referring to the promise (the promissee) is being serialized. The promissee
        holds the reference to the promise (usually as part of the the job arguments) and when it
        is being pickled, so will the promises it refers to. Pickling a promise triggers it to be
        registered with the promissor.

        :return:
        """
        self._promiseJobStore = jobStore
        
    def _disablePromiseRegistration(self):
        """
        Called when the job data is about to be saved in the JobStore.
        No promises should attempt to register with the job after this has been
        called, because that registration would not be persisted.
        """
        
        self._promiseJobStore = None

    ####################################################
    #Cycle/connectivity checking
    ####################################################

    def checkJobGraphForDeadlocks(self):
        """
        Ensures that a graph of Jobs (that hasn't yet been saved to the
        JobStore) doesn't contain any pathological relationships between jobs
        that would result in deadlocks if we tried to run the jobs.
        
        See :func:`toil.job.Job.checkJobGraphConnected`,
        :func:`toil.job.Job.checkJobGraphAcyclic` and
        :func:`toil.job.Job.checkNewCheckpointsAreLeafVertices` for more info.

        :raises toil.job.JobGraphDeadlockException: if the job graph
            is cyclic, contains multiple roots or contains checkpoint jobs that are
            not leaf vertices when defined (see :func:`toil.job.Job.checkNewCheckpointsAreLeaves`).
        """
        self.checkJobGraphConnected()
        self.checkJobGraphAcylic()
        self.checkNewCheckpointsAreLeafVertices()

    def getRootJobs(self):
        """
        :return: The roots of the connected component of jobs that contains this job.
        A root is a job with no predecessors.
        
        Only works on connected components of jobs not yet added to the JobStore.

        :rtype : set of Job objects with no predecessors (i.e. which are not children, follow-ons, or services)
        """
        
        # Start assuming all jobs are roots
        roots = set(self._registry.keys())
        
        for job in self._registry:
            for other in itertools.chain(job._children, job._followOns, job._services):
                roots.remove(other)
                
        return {self._registry[jid] for jid in roots}
            
    def checkJobGraphConnected(self):
        """
        :raises toil.job.JobGraphDeadlockException: if :func:`toil.job.Job.getRootJobs` does \
        not contain exactly one root job.

        As execution always starts from one root job, having multiple root jobs will \
        cause a deadlock to occur.
        
        Only works on connected components of jobs not yet added to the JobStore.
        """
        rootJobs = self.getRootJobs()
        if len(rootJobs) != 1:
            raise JobGraphDeadlockException("Graph does not contain exactly one"
                                            " root job: %s" % rootJobs)

    def checkJobGraphAcylic(self):
        """
        :raises toil.job.JobGraphDeadlockException: if the connected component \
        of jobs containing this job contains any cycles of child/followOn dependencies \
        in the *augmented job graph* (see below). Such cycles are not allowed \
        in valid job graphs.
        
        Only works on connected components of jobs not yet added to the JobStore.

        A follow-on edge (A, B) between two jobs A and B is equivalent \
        to adding a child edge to B from (1) A, (2) from each child of A, \
        and (3) from the successors of each child of A. We call each such edge \
        an edge an "implied" edge. The augmented job graph is a job graph including \
        all the implied edges.

        For a job graph G = (V, E) the algorithm is ``O(|V|^2)``. It is ``O(|V| + |E|)`` for \
        a graph with no follow-ons. The former follow-on case could be improved!
        
        Only works on connected components of jobs not yet added to the JobStore.
        """
        #Get the root jobs
        roots = self.getRootJobs()
        if len(roots) == 0:
            raise JobGraphDeadlockException("Graph contains no root jobs due to cycles")

        #Get implied edges
        extraEdges = self._getImpliedEdges(roots)

        #Check for directed cycles in the augmented graph
        visited = set()
        for root in roots:
            root._checkJobGraphAcylicDFS([], visited, extraEdges)
            
    def _checkJobGraphAcylicDFS(self, stack, visited, extraEdges):
        """
        DFS traversal to detect cycles in augmented job graph.
        """
        if self not in visited:
            visited.add(self)
            stack.append(self)
            for successor in self._children + self._followOns + extraEdges[self]:
                successor._checkJobGraphAcylicDFS(stack, visited, extraEdges)
            assert stack.pop() == self
        if self in stack:
            stack.append(self)
            raise JobGraphDeadlockException("A cycle of job dependencies has been detected '%s'" % stack)

    @staticmethod
    def _getImpliedEdges(roots):
        """
        Gets the set of implied edges (between children and follow-ons of a common job). See Job.checkJobGraphAcylic
        """
        #Get nodes in job graph
        nodes = set()
        for root in roots:
            root._collectAllSuccessors(nodes)

        ##For each follow-on edge calculate the extra implied edges
        #Adjacency list of implied edges, i.e. map of jobs to lists of jobs
        #connected by an implied edge
        extraEdges = dict([(n, []) for n in nodes])
        for job in nodes:
            if len(job._followOns) > 0:
                #Get set of jobs connected by a directed path to job, starting
                #with a child edge
                reacheable = set()
                for child in job._children:
                    child._collectAllSuccessors(reacheable)
                #Now add extra edges
                for descendant in reacheable:
                    extraEdges[descendant] += job._followOns[:]
        return extraEdges

    def checkNewCheckpointsAreLeafVertices(self):
        """
        A checkpoint job is a job that is restarted if either it fails, or if any of \
        its successors completely fails, exhausting their retries.

        A job is a leaf it is has no successors.

        A checkpoint job must be a leaf when initially added to the job graph. When its \
        run method is invoked it can then create direct successors. This restriction is made
        to simplify implementation.
        
        Only works on connected components of jobs not yet added to the JobStore.

        :raises toil.job.JobGraphDeadlockException: if there exists a job being added to the graph for which \
        checkpoint=True and which is not a leaf.
        """
        roots = self.getRootJobs() # Roots jobs of component, these are preexisting jobs in the graph

        # All jobs in the component of the job graph containing self
        jobs = set()
        list(map(lambda x : x._collectAllSuccessors(jobs), roots))

        # Check for each job for which checkpoint is true that it is a cut vertex or leaf
        for y in [x for x in jobs if x.checkpoint]:
            if y not in roots: # The roots are the prexisting jobs
                if not Job._isLeafVertex(y):
                    raise JobGraphDeadlockException("New checkpoint job %s is not a leaf in the job graph" % y)

    ####################################################
    #Deferred function system
    ####################################################

    def defer(self, function, *args, **kwargs):
        """
        Register a deferred function, i.e. a callable that will be invoked after the current
        attempt at running this job concludes. A job attempt is said to conclude when the job
        function (or the :meth:`toil.job.Job.run` method for class-based jobs) returns, raises an
        exception or after the process running it terminates abnormally. A deferred function will
        be called on the node that attempted to run the job, even if a subsequent attempt is made
        on another node. A deferred function should be idempotent because it may be called
        multiple times on the same node or even in the same process. More than one deferred
        function may be registered per job attempt by calling this method repeatedly with
        different arguments. If the same function is registered twice with the same or different
        arguments, it will be called twice per job attempt.

        Examples for deferred functions are ones that handle cleanup of resources external to
        Toil, like Docker containers, files outside the work directory, etc.

        :param callable function: The function to be called after this job concludes.

        :param list args: The arguments to the function

        :param dict kwargs: The keyword arguments to the function
        """
        if self._defer is None:
            raise Exception('A deferred function may only be registered with a job while that job is running.')
        self._defer(DeferredFunction.create(function, *args, **kwargs))


    ####################################################
    #The following nested classes are used for
    #creating jobtrees (Job.Runner),
    #and defining a service (Job.Service)
    ####################################################

    class Runner():
        """
        Used to setup and run Toil workflow.
        """
        @staticmethod
        def getDefaultArgumentParser():
            """
            Get argument parser with added toil workflow options.

            :returns: The argument parser used by a toil workflow with added Toil options.
            :rtype: :class:`argparse.ArgumentParser`
            """
            parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
            Job.Runner.addToilOptions(parser)
            return parser

        @staticmethod
        def getDefaultOptions(jobStore):
            """
            Get default options for a toil workflow.

            :param string jobStore: A string describing the jobStore \
            for the workflow.
            :returns: The options used by a toil workflow.
            :rtype: argparse.ArgumentParser values object
            """
            parser = Job.Runner.getDefaultArgumentParser()
            return parser.parse_args(args=[jobStore])

        @staticmethod
        def addToilOptions(parser):
            """
            Adds the default toil options to an :mod:`optparse` or :mod:`argparse`
            parser object.

            :param parser: Options object to add toil options to.
            :type parser: optparse.OptionParser or argparse.ArgumentParser
            """
            addOptions(parser)

        @staticmethod
        def startToil(job, options):
            """
            Deprecated by toil.common.Toil.start. Runs the toil workflow using the given options
            (see Job.Runner.getDefaultOptions and Job.Runner.addToilOptions) starting with this
            job.
            :param toil.job.Job job: root job of the workflow
            :raises: toil.leader.FailedJobsException if at the end of function \
            their remain failed jobs.
            :return: The return value of the root job's run function.
            :rtype: Any
            """
            setLoggingFromOptions(options)
            with Toil(options) as toil:
                if not options.restart:
                    return toil.start(job)
                else:
                    return toil.restart()

    class Service(with_metaclass(ABCMeta, object)):
        """
        Abstract class used to define the interface to a service.
        
        Should be subclassed by the user to define services.
        
        Is not executed as a job; runs within a ServiceHostJob. 
        """
        def __init__(self, memory=None, cores=None, disk=None, preemptable=None, unitName=None):
            """
            Memory, core and disk requirements are specified identically to as in \
            :func:`toil.job.Job.__init__`.
            """
            
            # Save the requirements to pass on to the hosting job.
            self.requirements = {'memory': memory, 'cores': cores, 'disk': disk,
                                 'preemptable': preemptable}
            # And the unit name
            self.unitName = unitName
            
            # And the name for the hosting job
            self.jobName = self.__class__.__name__
            
            # Record that we have as of yet no ServiceHostJob
            self.hostID = None
        
        @abstractmethod
        def start(self, job):
            """
            Start the service.

            :param toil.job.Job job: The underlying host job that the service is being run in. 
                                     Can be used to register deferred functions, or to access 
                                     the fileStore for creating temporary files.

            :returns: An object describing how to access the service. The object must be pickleable
                      and will be used by jobs to access the service (see :func:`toil.job.Job.addService`).
            """
            pass

        @abstractmethod
        def stop(self, job):
            """
            Stops the service. Function can block until complete.

            :param toil.job.Job job: The underlying host job that the service is being run in. 
                                     Can be used to register deferred functions, or to access 
                                     the fileStore for creating temporary files.
            """
            pass

        def check(self):
            """
            Checks the service is still running.

            :raise exceptions.RuntimeError: If the service failed, this will cause the service job to be labeled failed.
            :returns: True if the service is still running, else False. If False then the service job will be terminated,
                and considered a success. Important point: if the service job exits due to a failure, it should raise a
                RuntimeError, not return False!
            """
            pass

    ####################################################
    #Private functions
    ####################################################

    def _addPredecessor(self, predecessorJob):
        """
        Adds a predecessor job to the set of predecessor jobs. Raises a \
        RuntimeError if the job is already a predecessor.
        """
        if predecessorJob.jobStoreID in self._directPredecessors:
            raise RuntimeError("The given job is already a predecessor of this job")
        self._directPredecessors.add(predecessorJob.jobStoreID)
        
        # Record the need for the predecessor to finish
        self._description.addPredecessor()

    @staticmethod
    def _isLeafVertex(job):
        return len(job._children) == 0 \
               and len(job._followOns) == 0 \
               and len(job._services) == 0

    @classmethod
    def _loadUserModule(cls, userModule):
        """
        Imports and returns the module object represented by the given module descriptor.

        :type userModule: ModuleDescriptor
        """
        return userModule.load()

    @classmethod
    def _unpickle(cls, userModule, fileHandle, config):
        """
        Unpickles an object graph from the given file handle while loading symbols \
        referencing the __main__ module from the given userModule instead.

        :param userModule:
        :param fileHandle: An open, binary-mode file handle.
        :returns:
        """

        def filter_main(module_name, class_name):
            try:
                if module_name == '__main__':
                    return getattr(userModule, class_name)
                else:
                    return getattr(importlib.import_module(module_name), class_name)
            except:
                if module_name == '__main__':
                    logger.debug('Failed getting %s from module %s.', class_name, userModule)
                else:
                    logger.debug('Failed getting %s from module %s.', class_name, module_name)
                raise

        class FilteredUnpickler(pickle.Unpickler):
            def find_class(self, module, name):
                return filter_main(module, name)

        unpickler = FilteredUnpickler(fileHandle)

        runnable = unpickler.load()
        assert isinstance(runnable, Job)
        runnable.assignConfig(config)
        return runnable

    def getUserScript(self):
        return self.userModule
        
    def _fulfillPromises(self, returnValues, jobStore):
        """
        Sets the values for promises using the return values from this job's run() function.
        """
        for path, promiseFileStoreIDs in iteritems(self._rvs):
            if not path:
                # Note that its possible for returnValues to be a promise, not an actual return
                # value. This is the case if the job returns a promise from another job. In
                # either case, we just pass it on.
                promisedValue = returnValues
            else:
                # If there is an path ...
                if isinstance(returnValues, Promise):
                    # ... and the value itself is a Promise, we need to created a new, narrower
                    # promise and pass it on.
                    promisedValue = Promise(returnValues.job, path)
                else:
                    # Otherwise, we just select the desired component of the return value.
                    promisedValue = returnValues
                    for index in path:
                        promisedValue = promisedValue[index]
            for promiseFileStoreID in promiseFileStoreIDs:
                # File may be gone if the job is a service being re-run and the accessing job is
                # already complete.
                if jobStore.fileExists(promiseFileStoreID):
                    with jobStore.updateFileStream(promiseFileStoreID) as fileHandle:
                        pickle.dump(promisedValue, fileHandle, pickle.HIGHEST_PROTOCOL)

    # Functions associated with Job.checkJobGraphAcyclic to establish that the job graph does not
    # contain any cycles of dependencies:

    def _collectAllSuccessors(self, visited):
        """
        Adds the job and all jobs reachable on a directed path from current node to the given set.
        """
        
        # Keep our own stack since we may have a stick in the graph long enough
        # to exhaust the real stack
        todo = [self]
        
        while len(todo) > 0:
            job = todo[-1]
            todo.pop()
            if job not in visited:
                visited.add(job)
                for successor in itertools.chain(job._children, job._followOns):
                    todo.append(successor)

    def getTopologicalOrderingOfJobs(self):
        """
        :returns: a list of jobs such that for all pairs of indices i, j for which i < j, \
        the job at index i can be run before the job at index j.
        
        Only works on jobs in this job's subgraph that hasn't yet been added to the job store.
        
        Ignores service jobs.
        
        :rtype: list
        """
        ordering = []
        visited = set()

        # We need to recurse and traverse the graph without exhausting Python's
        # stack, so we keep our own stack.
        todo = [self]

        while len(todo) > 0:
            job = todo[-1]
            todo.pop()

            #Do not add the job to the ordering until all its predecessors have been
            #added to the ordering
            outstandingPredecessor = False
            for p in job._directPredecessors:
                if p not in visited:
                    outstandingPredecessor = True
                    break
            if outstandingPredecessor:
                continue

            if job not in visited:
                visited.add(job)
                ordering.append(job)
                
                for otherID in itertools.chain(job.description.followOnIDs, job.description.childIDs):
                    # Stack up descendants so we process children and then follow-ons.
                    # So stack up follow-ons deeper
                    todo.append(self._registry[otherID])

        return ordering
        
    ####################################################
    #Storing Jobs into the JobStore
    ####################################################
    
    def _register(self, jobStore):
        """
        If this job lacks a JobStore-assigned ID, assign this job (and all
        connected jobs) IDs.
        """
        
        # TODO: If we register all jobs in the component in _saveJobGraph, do
        # we really need the separate loop over the registry here?
        
        # TODO: This doesn't really have much to do with the registry. Rename
        # the registry.
        
        if isinstance(self.jobStoreID, FakeID):
            # We need to register the connected component.
            
            allJobs = list(self._registry.values())
            
            # We use one big dict from fake ID to corresponding real ID
            fakeToReal = []
            
            for job in allJobs:
               # Save the fake ID
               fake = job.jobStoreID
               # Assign a real one
               job.description.jobStoreID = jobStore.assignID(job.description)
               # Save the mapping
               fakeToReal[fake] = job.jobStoreID
               # Make sure the JobDescription can do its JobStore-related
               # setup.
               job.description.onRegistration(jobStore)
            
            # Remake the registry in place
            self._registry.clear()
            self._registry.update({job.jobStoreID: job for job in allJobs})
            
            for job in allJobs:
                # Tell all the jobs (and thus their descriptions and services)
                # about the renames.
                job._renameReferences(fakeToReal)
                
    def _renameReferences(self, renames):
        """
        Apply the given dict of ID renames to all references to other jobs.
        
        Ignores the registry, which is shared and assumed to already be updated.
        
        :param dict(FakeID, str) renames: Rename operations to apply.
        """
        
        # Rename predecessors
        self._directPredecessors = {renames[old] for old in self._directPredecessors}
        # Do renames in the description
        self._description.renameReferences(renames)
        
    def _saveBody(self, jobStore):
        """
        Save the execution data for just this job to the JobStore, and fill in
        the JobDescription with the information needed to retrieve it.
        
        Does not save the JobDescription.
        """
        
        # Note that we can't accept any more requests for our return value
        self._disablePromiseRegistration()
  
        # Drop out the children/followOns/predecessors/services/description - which are
        # all recorded within the jobStore and do not need to be stored within
        # the job. Set to None so we know if we try to actually use them.
        # ID references shouldn't appear in the body.
        self._children = None
        self._followOns = None
        self._services = None 
        self._directPredecessors = None
        description = self._description
        self._description = None
        
        # Save the body of the job
        # TODO: we need a job store ID in the description to save the job data for cleanup, but we don't want to commit the description until the job data is on disk...
        with jobStore.writeFileStream(description.jobStoreID, cleanup=True) as (fileHandle, fileStoreID):
            pickle.dump(self, fileHandle, pickle.HIGHEST_PROTOCOL)
            
        # Restore important fields
        self._description = description
            
        # Find the user script.
        # Note that getUserScript() may have been overridden. This is intended. If we used
        # self.userModule directly, we'd be getting a reference to job.py if the job was
        # specified as a function (as opposed to a class) since that is where FunctionWrappingJob
        #  is defined. What we really want is the module that was loaded as __main__,
        # and FunctionWrappingJob overrides getUserScript() to give us just that. Only then can
        # filter_main() in _unpickle( ) do its job of resolving any user-defined type or function.
        userScript = self.getUserScript().globalize()
        
        # The command connects the body of the job to the JobDescription
        self._description.command = ' '.join(('_toil', fileStoreID) + userScript.toCommand())
        
    def _saveJobGraph(self, jobStore, saveSelf=False, returnValues=None):
        """
        Save job data and JobDescriptions to the given job store for this job
        and all descending jobs, including services.
        
        Used to save the initial job graph containing the root job of the workflow.
        
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore: The job store
            to save the jobs into.
        :param bool saveSelf: Set to True to save this job along with its children,
            follow-ons, and services, or False to just save the children, follow-ons,
            and services and to populate the return value.
        :param returnValues: The collection of values returned when executing
            the job (or starting the service the job is hosting). If saveSelf
            is not set, will be used to fulfil the job's return value promises.
        """
        
        # Prohibit cycles and multiple roots
        self.checkJobGraphForDeadlocks()
        
        # Make sure everybody in the registry is registrered with the job store
        # and has an ID.
        allJobs = list(self._registry.values())
        for job in allJobs:
            job._register(jobStore)
        
        # Make sure the whole component is ready for promise registration
        for job in allJobs:
            job.prepareForPromiseRegistration(jobStore)
        
        # Get an ordering on the jobs which we use for pickling the jobs in the
        # correct order to ensure the promises are properly established
        ordering = self.getTopologicalOrderingOfJobs()
        
        # Set up to save last job first, so promises flow the right way
        ordering.reverse()
        
        # Make sure we're the root
        assert ordering[-1] == self
        
        if not saveSelf:
            # Fulfil promises for return values (even if value is None)
            self._fulfillPromises(returnValues, jobStore)
        
        for job in ordering:
            for serviceBatch in reversed(job.description.serviceHostIDsInBatches()):
                # For each batch of service host jobs in reverse order they start
                for serviceID in serviceBatch:
                    # Find the actual job
                    serviceJob = self._registry[serviceID]
                    # Pickle the service body, which triggers all the promise stuff
                    serviceJob._saveBody(jobStore)
            if job != self or saveSelf:
                # Now pickle the job itself
                job._saveBody(jobStore)
            
        # Now that the job data is on disk, commit the JobDescriptions in reverse execution order
        for job in ordering:
            for serviceBatch in job.description.serviceHostIDsInBatches():
                for serviceID in serviceBatch:
                    jobStore.update(self._registry[serviceID].description)
            if job != self or saveSelf:
                jobStore.update(job.description)
        
    def saveAsRootJob(self, jobStore):
        """
        Save this job to the given jobStore as the root job of the workflow.
        
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore:
        :return: the JobDescription describing this job.
        """

        # Check if the workflow root is a checkpoint but not a leaf vertex.
        # All other job vertices in the graph are checked by checkNewCheckpointsAreLeafVertices
        if self.checkpoint and not Job._isLeafVertex(self):
            raise JobGraphDeadlockException(
                'New checkpoint job %s is not a leaf in the job graph' % self)

        # Save the root job and all descendants and services
        self._saveJobGraph(jobStore, saveSelf=True)
        
        # Store the name of the first job in a file in case of restart. Up to this point the
        # root job is not recoverable. FIXME: "root job" or "first job", which one is it?
        jobStore.setRootJob(self.jobStoreID)
        
        return self.description
        
    @classmethod
    def loadJob(cls, jobStore, jobDescription):
        """
        Retrieves a :class:`toil.job.Job` instance from a JobStore

        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore: The job store.
        :param toil.job.JobDescription jobDescription: the JobDescription of the job to retrieve.
        :returns: The job referenced by the JobDescription.
        :rtype: toil.job.Job
        """
        
        # Grab the command that connects the description to the job body
        command = jobDescription.command
        
        commandTokens = command.split()
        assert "_toil" == commandTokens[0]
        userModule = ModuleDescriptor.fromCommand(commandTokens[2:])
        logger.debug('Loading user module %s.', userModule)
        userModule = cls._loadUserModule(userModule)
        pickleFile = commandTokens[1]

        # Get a directory to download the job in
        directory = tempfile.mkdtemp()
        # Initialize a blank filename so the finally below can't fail due to a
        # missing variable
        filename = ''

        try:
            # Get a filename to download the job to.
            # Don't use mkstemp because we would need to delete and replace the
            # file.
            # Don't use a NamedTemporaryFile context manager because its
            # context manager exit will crash if we deleted it.
            filename = os.path.join(directory, 'job')
                
            # Download the job
            if pickleFile == "firstJob":
                jobStore.readSharedFile(pickleFile, filename)
            else:
                jobStore.readFile(pickleFile, filename)

            # Open and unpickle
            with open(filename, 'rb') as fileHandle:
                job = cls._unpickle(userModule, fileHandle, jobStore.config)
                # Fill in the current description
                job._description = jobDescription

            # TODO: We ought to just unpickle straight from a streaming read
        finally:
            # Clean up the file
            if os.path.exists(filename):
                os.unlink(filename)
            # Clean up the directory we put it in
            # TODO: we assume nobody else put anything in the directory
            if os.path.exists(directory):
                os.rmdir(directory)

    def _run(self, fileStore):
        """
        Function which worker calls to ultimately invoke
        a jobs Job.run method, and then handle created
        children/followOn jobs.
        
        May be overridden by specialized TOil-internal jobs.
        """
        return self.run(fileStore)

    @contextmanager
    def _executor(self, stats, fileStore):
        """
        This is the core wrapping method for running the job within a worker.  It sets up the stats
        and logging before yielding. After completion of the body, the function will finish up the
        stats and logging, and starts the async update process for the job.
        
        Will modify the job's description with changes that need to be committed back to the JobStore.
        """
        if stats is not None:
            startTime = time.time()
            startClock = getTotalCpuTime()
        baseDir = os.getcwd()

        yield

        # If the job is not a checkpoint job, add the promise files to delete
        # to the list of jobStoreFileIDs to delete
        # TODO: why is Promise holding a global list here???
        if not self.checkpoint:
            for jobStoreFileID in Promise.filesToDelete:
                # Make sure to wrap the job store ID in a FileID object so the file store will accept it
                # TODO: talk directly to the job sotre here instead.
                fileStore.deleteGlobalFile(FileID(jobStoreFileID, 0))
        else:
            # Else copy them to the job description to delete later
            self.description.checkpointFilesToDelete = list(Promise.filesToDelete)
        Promise.filesToDelete.clear()
        # Now indicate the asynchronous update of the job can happen
        fileStore.startCommit(jobState=True)
        # Change dir back to cwd dir, if changed by job (this is a safety issue)
        if os.getcwd() != baseDir:
            os.chdir(baseDir)
        # Finish up the stats
        if stats is not None:
            totalCpuTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            stats.jobs.append(
                Expando(
                    time=str(time.time() - startTime),
                    clock=str(totalCpuTime - startClock),
                    class_name=self._jobName(),
                    memory=str(totalMemoryUsage)
                )
            )

    def _runner(self, jobStore, fileStore, defer):
        """
        This method actually runs the job, and serialises the next jobs.

        :param class jobGraph: Instance of a jobGraph object
        :param class jobStore: Instance of the job store
        :param toil.fileStores.abstractFileStore.AbstractFileStore fileStore: Instance of a cached
               or uncached filestore
        :param defer: Function yielded by open() context
               manager of :class:`toil.DeferredFunctionManager`, which is called to
               register deferred functions.
        :return:
        """

        # Make deferred function registration available during run().
        self._defer = defer
        # Make fileStore available as an attribute during run() ...
        self._fileStore = fileStore
        # ... but also pass it to run() as an argument for backwards compatibility.
        returnValues = self._run(fileStore)
        # Clean up state changes made for run()
        self._defer = None
        self._fileStore = None


        # Serialize the new Jobs defined by the run method to the jobStore
        self._saveJobGraph(jobStore, saveSelf=False, returnValues=returnValues)
        
        # TODO: does not update this job's JobDescription to save any new child
        # or follow-on relationships. Should it?

        

    def _jobName(self):
        """
        :rtype : string, used as identifier of the job class in the stats report.
        """
        return self._description.displayName


class JobException(Exception):
    """
    General job exception.
    """
    def __init__(self, message):
        super().__init__(message)


class JobGraphDeadlockException(JobException):
    """
    An exception raised in the event that a workflow contains an unresolvable \
    dependency, such as a cycle. See :func:`toil.job.Job.checkJobGraphForDeadlocks`.
    """
    def __init__(self, string):
        super().__init__(string)


class FunctionWrappingJob(Job):
    """
    Job used to wrap a function. In its `run` method the wrapped function is called.
    """
    def __init__(self, userFunction, *args, **kwargs):
        """
        :param callable userFunction: The function to wrap. It will be called with ``*args`` and
               ``**kwargs`` as arguments.

        The keywords ``memory``, ``cores``, ``disk``, ``preemptable`` and ``checkpoint`` are
        reserved keyword arguments that if specified will be used to determine the resources
        required for the job, as :func:`toil.job.Job.__init__`. If they are keyword arguments to
        the function they will be extracted from the function definition, but may be overridden
        by the user (as you would expect).
        """
        # Use the user-specified requirements, if specified, else grab the default argument
        # from the function, if specified, else default to None
        if sys.version_info >= (3, 0):
            argSpec = inspect.getfullargspec(userFunction)
        else:
            argSpec = inspect.getargspec(userFunction)

        if argSpec.defaults is None:
            argDict = {}
        else:
            argDict = dict(list(zip(argSpec.args[-len(argSpec.defaults):], argSpec.defaults)))

        def resolve(key, default=None, dehumanize=False):
            try:
                # First, try constructor arguments, ...
                value = kwargs.pop(key)
            except KeyError:
                try:
                    # ..., then try default value for function keyword arguments, ...
                    value = argDict[key]
                except KeyError:
                    # ... and finally fall back to a default value.
                    value = default
            # Optionally, convert strings with metric or binary prefixes.
            if dehumanize and isinstance(value, string_types):
                value = human2bytes(value)
            return value

        Job.__init__(self,
                     memory=resolve('memory', dehumanize=True),
                     cores=resolve('cores', dehumanize=True),
                     disk=resolve('disk', dehumanize=True),
                     preemptable=resolve('preemptable'),
                     checkpoint=resolve('checkpoint', default=False),
                     unitName=resolve('name', default=None))

        self.userFunctionModule = ModuleDescriptor.forModule(userFunction.__module__).globalize()
        self.userFunctionName = str(userFunction.__name__)
        self.jobName = self.userFunctionName
        self._args = args
        self._kwargs = kwargs

    def _getUserFunction(self):
        logger.debug('Loading user function %s from module %s.',
                     self.userFunctionName,
                     self.userFunctionModule)
        userFunctionModule = self._loadUserModule(self.userFunctionModule)
        return getattr(userFunctionModule, self.userFunctionName)

    def run(self,fileStore):
        userFunction = self._getUserFunction( )
        return userFunction(*self._args, **self._kwargs)

    def getUserScript(self):
        return self.userFunctionModule

    def _jobName(self):
        return ".".join((self.__class__.__name__,self.userFunctionModule.name,self.userFunctionName))


class JobFunctionWrappingJob(FunctionWrappingJob):
    """
    A job function is a function whose first argument is a :class:`.Job`
    instance that is the wrapping job for the function. This can be used to
    add successor jobs for the function and perform all the functions the
    :class:`.Job` class provides.

    To enable the job function to get access to the
    :class:`toil.fileStores.abstractFileStore.AbstractFileStore` instance (see
    :func:`toil.job.Job.run`), it is made a variable of the wrapping job called
    fileStore.

    To specify a job's resource requirements the following default keyword arguments
    can be specified:

        - memory
        - disk
        - cores

    For example to wrap a function into a job we would call::

        Job.wrapJobFn(myJob, memory='100k', disk='1M', cores=0.1)

    """

    @property
    def fileStore(self):
        return self._fileStore

    def run(self, fileStore):
        userFunction = self._getUserFunction()
        rValue = userFunction(*((self,) + tuple(self._args)), **self._kwargs)
        return rValue


class PromisedRequirementFunctionWrappingJob(FunctionWrappingJob):
    """
    Handles dynamic resource allocation using :class:`toil.job.Promise` instances.
    Spawns child function using parent function parameters and fulfilled promised
    resource requirements.
    """
    def __init__(self, userFunction, *args, **kwargs):
        self._promisedKwargs = kwargs.copy()
        # Replace resource requirements in intermediate job with small values.
        kwargs.update(dict(disk='1M', memory='32M', cores=0.1))
        super().__init__(userFunction, *args, **kwargs)

    @classmethod
    def create(cls, userFunction, *args, **kwargs):
        """
        Creates an encapsulated Toil job function with unfulfilled promised resource
        requirements. After the promises are fulfilled, a child job function is created
        using updated resource values. The subgraph is encapsulated to ensure that this
        child job function is run before other children in the workflow. Otherwise, a
        different child may try to use an unresolved promise return value from the parent.
        """
        return EncapsulatedJob(cls(userFunction, *args, **kwargs))

    def run(self, fileStore):
        # Assumes promises are fulfilled when parent job is run
        self.evaluatePromisedRequirements()
        userFunction = self._getUserFunction()
        return self.addChildFn(userFunction, *self._args, **self._promisedKwargs).rv()

    def evaluatePromisedRequirements(self):
        requirements = ["disk", "memory", "cores"]
        # Fulfill resource requirement promises
        for requirement in requirements:
            try:
                if isinstance(self._promisedKwargs[requirement], PromisedRequirement):
                    self._promisedKwargs[requirement] = self._promisedKwargs[requirement].getValue()
            except KeyError:
                pass


class PromisedRequirementJobFunctionWrappingJob(PromisedRequirementFunctionWrappingJob):
    """
    Handles dynamic resource allocation for job functions.
    See :class:`toil.job.JobFunctionWrappingJob`
    """

    def run(self, fileStore):
        self.evaluatePromisedRequirements()
        userFunction = self._getUserFunction()
        return self.addChildJobFn(userFunction, *self._args, **self._promisedKwargs).rv()


class EncapsulatedJob(Job):
    """
    A convenience Job class used to make a job subgraph appear to be a single job.

    Let A be the root job of a job subgraph and B be another job we'd like to run after A
    and all its successors have completed, for this use encapsulate::

        #  Job A and subgraph, Job B
        A, B = A(), B()
        A' = A.encapsulate()
        A'.addChild(B)
        #  B will run after A and all its successors have completed, A and its subgraph of
        # successors in effect appear to be just one job.

    If the job being encapsulated has predecessors (e.g. is not the root job), then the encapsulated
    job will inherit these predecessors. If predecessors are added to the job being encapsulated
    after the encapsulated job is created then the encapsulating job will NOT inherit these
    predecessors automatically. Care should be exercised to ensure the encapsulated job has the
    proper set of predecessors.

    The return value of an encapsulatd job (as accessed by the :func:`toil.job.Job.rv` function)
    is the return value of the root job, e.g. A().encapsulate().rv() and A().rv() will resolve to
    the same value after A or A.encapsulate() has been run.
    """
    def __init__(self, job):
        """
        :param toil.job.Job job: the job to encapsulate.
        """
        # Giving the root of the subgraph the same resources as the first job in the subgraph.
        Job.__init__(self, **job._requirements)
        # Ensure that the encapsulated job has the same direct predecessors as the job
        # being encapsulated.
        if job._directPredecessors:
            for job_ in job._directPredecessors:
                job_.addChild(self)
        self.encapsulatedJob = job
        Job.addChild(self, job)
        # Use small resource requirements for dummy Job instance.
        # But not too small, or the job won't have enough resources to safely start up Toil.
        self.encapsulatedFollowOn = Job(disk='100M', memory='512M', cores=0.1)
        Job.addFollowOn(self, self.encapsulatedFollowOn)

    def addChild(self, childJob):
        return Job.addChild(self.encapsulatedFollowOn, childJob)

    def addService(self, service, parentService=None):
        return Job.addService(self.encapsulatedFollowOn, service, parentService=parentService)

    def addFollowOn(self, followOnJob):
        return Job.addFollowOn(self.encapsulatedFollowOn, followOnJob)

    def rv(self, *path):
        return self.encapsulatedJob.rv(*path)

    def prepareForPromiseRegistration(self, jobStore):
        super().prepareForPromiseRegistration(jobStore)
        self.encapsulatedJob.prepareForPromiseRegistration(jobStore)
        
    def _disablePromiseRegistration(self):
        super()._disablePromiseRegistration()
        self.encapsulatedJob._disablePromiseRegistration()

    def getUserScript(self):
        return self.encapsulatedJob.getUserScript()

class ServiceHostJob(Job):
    """
    Job that runs a service. Used internally by Toil. Users should subclass Service instead of using this.
    """
    def __init__(self, service):
        """
        This constructor should not be called by a user.

        :param service: The service to wrap in a job.
        :type service: toil.job.Job.Service
        """
        
        # Make sure the service hasn't been given a host already.
        assert service.hostID is None
        
        # Make ourselves with name info from the Service and a
        # ServiceJobDescription that has the service control flags.
        super().__init__(self, **service.requirements,
            unitName=service.unitName, descriptionClass=ServiceJobDescription)
        
        # Make sure the service knows it has a host now
        service.hostID = self.jobStoreID
        
        # service.__module__ is the module defining the class service is an instance of.
        # Will need to be loaded before unpickling the Service
        self.serviceModule = ModuleDescriptor.forModule(service.__module__).globalize()

        # The service to run, or None if it is still pickled.
        # We can't just pickle as part of ourselves because we may need to load
        # an additional module.
        self.service = service
        # The pickled service, or None if it isn't currently pickled.
        # We can't just pickle right away because we may owe promises from it.
        self.pickledService = None
        
        # Pick up our name form the service.
        self.jobName = service.jobName
        # This references the parent job wrapper. It is initialised just before
        # the job is run. It is used to access the start and terminate flags.
        self.jobGraph = None

    @property
    def fileStore(self):
        """
        Return the file store, which the Service may need.
        """
        return self._fileStore
        
    def _renameReferences(self, renames):
        # When the job store finally hads out IDs we have to fix up the
        # back-reference from our Service to us.
        super()._renameReferences(renames)
        if self.service is not None:
            self.service.hostID = renames[self.service.hostID]
        
    # Since the running service has us, make sure they don't try to tack more
    # stuff onto us.
        
    def addChild(self, child):
        raise RuntimeError("Service host jobs cannot have children, follow-ons, or services")
        
    def addFollowOn(self, followOn):
        raise RuntimeError("Service host jobs cannot have children, follow-ons, or services")
        
    def addService(self, service, parentService=None):
        raise RuntimeError("Service host jobs cannot have children, follow-ons, or services")

    def run(self, fileStore):
        # Unpickle the service
        logger.debug('Loading service module %s.', self.serviceModule)
        userModule = self._loadUserModule(self.serviceModule)
        service = self._unpickle( userModule, BytesIO( self.pickledService ), fileStore.jobStore.config )
        #Start the service
        startCredentials = service.start(self)
        try:
            #The start credentials  must be communicated to processes connecting to
            #the service, to do this while the run method is running we
            #cheat and set the return value promise within the run method
            self._fulfillPromises(startCredentials, fileStore.jobStore)
            self._rvs = {}  # Set this to avoid the return values being updated after the
            #run method has completed!

            #Now flag that the service is running jobs can connect to it
            logger.debug("Removing the start jobStoreID to indicate that establishment of the service")
            assert self.jobGraph.startJobStoreID != None
            if fileStore.jobStore.fileExists(self.jobGraph.startJobStoreID):
                fileStore.jobStore.deleteFile(self.jobGraph.startJobStoreID)
            assert not fileStore.jobStore.fileExists(self.jobGraph.startJobStoreID)

            #Now block until we are told to stop, which is indicated by the removal
            #of a file
            assert self.jobGraph.terminateJobStoreID != None
            while True:
                # Check for the terminate signal
                if not fileStore.jobStore.fileExists(self.jobGraph.terminateJobStoreID):
                    logger.debug("Detected that the terminate jobStoreID has been removed so exiting")
                    if not fileStore.jobStore.fileExists(self.jobGraph.errorJobStoreID):
                        raise RuntimeError("Detected the error jobStoreID has been removed so exiting with an error")
                    break

                # Check the service's status and exit if failed or complete
                try:
                    if not service.check():
                        logger.debug("The service has finished okay, exiting")
                        break
                except RuntimeError:
                    logger.debug("Detected termination of the service")
                    raise

                time.sleep(fileStore.jobStore.config.servicePollingInterval) #Avoid excessive polling

            # Remove link to the jobGraph
            self.jobGraph = None

            logger.debug("Service is done")
        finally:
            # The stop function is always called
            service.stop(self)

    def getUserScript(self):
        return self.serviceModule


class Promise():
    """
    References a return value from a :meth:`toil.job.Job.run` or
    :meth:`toil.job.Job.Service.start` method as a *promise* before the method itself is run.

    Let T be a job. Instances of :class:`.Promise` (termed a *promise*) are returned by T.rv(),
    which is used to reference the return value of T's run function. When the promise is passed
    to the constructor (or as an argument to a wrapped function) of a different, successor job
    the promise will be replaced by the actual referenced return value. This mechanism allows a
    return values from one job's run method to be input argument to job before the former job's
    run function has been executed.
    """
    _jobstore = None
    """
    Caches the job store instance used during unpickling to prevent it from being instantiated
    for each promise

    :type: toil.jobStores.abstractJobStore.AbstractJobStore
    """

    filesToDelete = set()
    """
    A set of IDs of files containing promised values when we know we won't need them anymore
    """
    def __init__(self, job, path):
        """
        :param Job job: the job whose return value this promise references
        :param path: see :meth:`.Job.rv`
        """
        self.job = job
        self.path = path

    def __reduce__(self):
        """
        Called during pickling when a promise (an instance of this class) is about to be be
        pickled. Returns the Promise class and construction arguments that will be evaluated
        during unpickling, namely the job store coordinates of a file that will hold the promised
        return value. By the time the promise is about to be unpickled, that file should be
        populated.
        """
        # The allocation of the file in the job store is intentionally lazy, we only allocate an
        # empty file in the job store if the promise is actually being pickled. This is done so
        # that we do not allocate files for promises that are never used.
        jobStoreLocator, jobStoreFileID = self.job.registerPromise(self.path)
        # Returning a class object here causes the pickling machinery to attempt to instantiate
        # the class. We will catch that with __new__ and return an the actual return value instead.
        return self.__class__, (jobStoreLocator, jobStoreFileID)

    @staticmethod
    def __new__(cls, *args):
        assert len(args) == 2
        if isinstance(args[0], Job):
            # Regular instantiation when promise is created, before it is being pickled
            return super().__new__(cls)
        else:
            # Attempted instantiation during unpickling, return promised value instead
            return cls._resolve(*args)

    @classmethod
    def _resolve(cls, jobStoreLocator, jobStoreFileID):
        # Initialize the cached job store if it was never initialized in the current process or
        # if it belongs to a different workflow that was run earlier in the current process.
        if cls._jobstore is None or cls._jobstore.config.jobStore != jobStoreLocator:
            cls._jobstore = Toil.resumeJobStore(jobStoreLocator)
        cls.filesToDelete.add(jobStoreFileID)
        with cls._jobstore.readFileStream(jobStoreFileID) as fileHandle:
            # If this doesn't work then the file containing the promise may not exist or be
            # corrupted
            value = safeUnpickleFromStream(fileHandle)
            return value


class PromisedRequirement():
    def __init__(self, valueOrCallable, *args):
        """
        Class for dynamically allocating job function resource requirements involving
        :class:`toil.job.Promise` instances.

        Use when resource requirements depend on the return value of a parent function.
        PromisedRequirements can be modified by passing a function that takes the
        :class:`.Promise` as input.

        For example, let f, g, and h be functions. Then a Toil workflow can be
        defined as follows::
        A = Job.wrapFn(f)
        B = A.addChildFn(g, cores=PromisedRequirement(A.rv())
        C = B.addChildFn(h, cores=PromisedRequirement(lambda x: 2*x, B.rv()))

        :param valueOrCallable: A single Promise instance or a function that
                                takes \*args as input parameters.
        :param \*args: variable length argument list
        :type \*args: int or .Promise
        """
        if hasattr(valueOrCallable, '__call__'):
            assert len(args) != 0, 'Need parameters for PromisedRequirement function.'
            func = valueOrCallable
        else:
            assert len(args) == 0, 'Define a PromisedRequirement function to handle multiple arguments.'
            func = lambda x: x
            args = [valueOrCallable]

        self._func = dill.dumps(func)
        self._args = list(args)

    def getValue(self):
        """
        Returns PromisedRequirement value
        """
        func = dill.loads(self._func)
        return func(*self._args)

    @staticmethod
    def convertPromises(kwargs):
        """
        Returns True if reserved resource keyword is a Promise or
        PromisedRequirement instance. Converts Promise instance
        to PromisedRequirement.

        :param kwargs: function keyword arguments
        :return: bool
        """
        for r in ["disk", "memory", "cores"]:
            if isinstance(kwargs.get(r), Promise):
                kwargs[r] = PromisedRequirement(kwargs[r])
                return True
            elif isinstance(kwargs.get(r), PromisedRequirement):
                return True
        return False


class UnfulfilledPromiseSentinel():
    """This should be overwritten by a proper promised value. Throws an
    exception when unpickled."""
    def __init__(self, fulfillingJobName, unpickled):
        self.fulfillingJobName = fulfillingJobName

    @staticmethod
    def __setstate__(stateDict):
        """Only called when unpickling. This won't be unpickled unless the
        promise wasn't resolved, so we throw an exception."""
        jobName = stateDict['fulfillingJobName']
        raise RuntimeError("This job was passed a promise that wasn't yet resolved when it "
                           "ran. The job {jobName} that fulfills this promise hasn't yet "
                           "finished. This means that there aren't enough constraints to "
                           "ensure the current job always runs after {jobName}. Consider adding a "
                           "follow-on indirection between this job and its parent, or adding "
                           "this job as a child/follow-on of {jobName}.".format(jobName=jobName))
