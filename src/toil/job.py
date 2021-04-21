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
import collections
import copy
import importlib
import inspect
import itertools
import logging
import os
import pickle
import shutil
import tempfile
import time
import uuid
from abc import ABCMeta, abstractmethod
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from contextlib import contextmanager
from io import BytesIO
from typing import Dict, Optional, Union, Set

import dill

from toil.common import Config, Toil, addOptions, safeUnpickleFromStream
from toil.deferred import DeferredFunction
from toil.fileStores import FileID
from toil.lib.expando import Expando
from toil.lib.conversions import human2bytes
from toil.lib.resources import (get_total_cpu_time,
                                get_total_cpu_time_and_memory_usage)
from toil.resource import ModuleDescriptor
from toil.statsAndLogging import set_logging_from_options

logger = logging.getLogger(__name__)


class JobPromiseConstraintError(RuntimeError):
    """
    Represents a problem where a job is being asked to promise its return
    value, but it has not yet been hit in the topological order of the job
    graph.
    """
    def __init__(self, promisingJob, recipientJob=None):
        """
        :param toil.job.Job promisingJob: The job being asked for its return value.
        :param toil.job.Job recipientJob: The job receiving the return value, if any.
        """

        self.promisingJob = promisingJob
        self.recipientJob = recipientJob

        if recipientJob is None:
            # Come up with a vaguer error message
            super().__init__(f"Job {promisingJob.description} cannot promise its return value to a job that is not its successor")
        else:
            # Write a full error message
            super().__init__(f"Job {promisingJob.description} cannot promise its return value to non-successor {recipientJob.description}")


class ConflictingPredecessorError(Exception):
    def __init__(self, predecessor: 'Job', successor: 'Job'):
        super().__init__(f'The given job: "{predecessor.description}" is already a predecessor of job: "{successor.description}".')


class TemporaryID:
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
        return f'TemporaryID({self._value})'

    def __hash__(self):
        return hash(self._value)

    def __eq__(self, other):
        return isinstance(other, TemporaryID) and self._value == other._value

    def __ne__(self, other):
        return not isinstance(other, TemporaryID) or self._value != other._value


class Requirer:
    """
    Base class implementing the storage and presentation of requirements for
    cores, memory, disk, and preemptability as properties.
    """

    def __init__(self, requirements):
        """
        Parse and save the given requirements.

        :param dict requirements: Dict from string to number, string, or bool
            describing a set of resource requirments. 'cores', 'memory',
            'disk', and 'preemptable' fields, if set, are parsed and broken out
            into properties. If unset, the relevant property will be
            unspecified, and will be pulled from the assigned Config object if
            queried (see :meth:`toil.job.Requirer.assignConfig`). If
            unspecified and no Config object is assigned, an AttributeError
            will be raised at query time.
        """

        super().__init__()

        # We can have a toil.common.Config assigned to fill in default values
        # for e.g. job requirements not explicitly specified.
        self._config = None

        # Save requirements, parsing and validating anything that needs parsing or validating.
        # Don't save Nones.
        self._requirementOverrides = {k: self._parseResource(k, v) for (k, v) in requirements.items() if v is not None}

    def assignConfig(self, config):
        """
        Assign the given config object to be used to provide default values.

        Must be called exactly once on a loaded JobDescription before any
        requirements are queried.

        :param toil.common.Config config: Config object to query
        """
        if self._config is not None:
            raise RuntimeError(f"Config assigned multiple times to {self}")
        self._config = config

    def __getstate__(self):
        """
        Return the dict to use as the instance's __dict__ when pickling.
        """

        # We want to exclude the config from pickling.
        state = self.__dict__.copy()
        state['_config'] = None
        return state

    def __copy__(self):
        """
        Return a semantically-shallow copy of the object, for :meth:`copy.copy`.
        """

        # See https://stackoverflow.com/a/40484215 for how to do an override
        # that uses the base implementation

        # Hide this override
        implementation = self.__copy__
        self.__copy__ = None

        # Do the copy which omits the config via __getstate__ override
        clone = copy.copy(self)

        # Put back the override on us and the copy
        self.__copy__ = implementation
        clone.__copy__ = implementation

        if self._config is not None:
            # Share a config reference
            clone.assignConfig(self._config)

        return clone

    def __deepcopy__(self, memo):
        """
        Return a semantically-deep copy of the object, for :meth:`copy.deepcopy`.
        """

        # See https://stackoverflow.com/a/40484215 for how to do an override
        # that uses the base implementation

        # Hide this override
        implementation = self.__deepcopy__
        self.__deepcopy__ = None

        # Do the deepcopy which omits the config via __getstate__ override
        clone = copy.deepcopy(self, memo)

        # Put back the override on us and the copy
        self.__deepcopy__ = implementation
        clone.__deepcopy__ = implementation

        if self._config is not None:
            # Share a config reference
            clone.assignConfig(self._config)

        return clone

    @staticmethod
    def _parseResource(name, value):
        """
        Parse a Toil resource requirement value and apply resource-specific type checks. If the
        value is a string, a binary or metric unit prefix in it will be evaluated and the
        corresponding integral value will be returned.

        :param str name: The name of the resource
        :param str|int|float|bool|None value: The resource value
        :rtype: int|float|bool|None

        >>> Requirer._parseResource('cores', None)
        >>> Requirer._parseResource('cores', 1), Requirer._parseResource('disk', 1), \
        Requirer._parseResource('memory', 1)
        (1, 1, 1)
        >>> Requirer._parseResource('cores', '1G'), Requirer._parseResource('disk', '1G'), \
        Requirer._parseResource('memory', '1G')
        (1073741824, 1073741824, 1073741824)
        >>> Requirer._parseResource('cores', 1.1)
        1.1
        >>> Requirer._parseResource('disk', 1.1) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        TypeError: The 'disk' requirement does not accept values that are of <type 'float'>
        >>> Requirer._parseResource('memory', object()) # doctest: +IGNORE_EXCEPTION_DETAIL
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
                    raise ValueError(f"The '{name}' requirement, as a string, must be 'true' or 'false' but is {value}")
            elif isinstance(value, int):
                if value == 1:
                    return True
                if value == 0:
                    return False
                else:
                    raise ValueError(f"The '{name}' requirement, asn an int, must be 1 or 0 but is {value}")
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

        :param str requirement: The name of the resource
        :rtype: int|float|bool|None
        """
        if requirement in self._requirementOverrides:
            value = self._requirementOverrides[requirement]
            if value is None:
             raise AttributeError(f"Encountered explicit None for '{requirement}' requirement of {self}")
            return value
        elif self._config is not None:
            value = getattr(self._config, 'default' + requirement.capitalize())
            if value is None:
                raise AttributeError(f"Encountered None for default '{requirement}' requirement in config: {self._config}")
            return value
        else:
            raise AttributeError(f"Default value for '{requirement}' requirement of {self} cannot be determined")

    @property
    def requirements(self):
        """
        Dict containing all non-None, non-defaulted requirements.

        :rtype: dict
        """
        return dict(self._requirementOverrides)

    @property
    def disk(self) -> int:
        """
        The maximum number of bytes of disk required.

        :rtype: int
        """
        return self._fetchRequirement('disk')
    @disk.setter
    def disk(self, val):
         self._requirementOverrides['disk'] = self._parseResource('disk', val)

    @property
    def memory(self):
        """
        The maximum number of bytes of memory required.

        :rtype: int
        """
        return self._fetchRequirement('memory')
    @memory.setter
    def memory(self, val):
         self._requirementOverrides['memory'] = self._parseResource('memory', val)

    @property
    def cores(self):
        """
        The number of CPU cores required.

        :rtype: int|float
        """
        return self._fetchRequirement('cores')
    @cores.setter
    def cores(self, val):
         self._requirementOverrides['cores'] = self._parseResource('cores', val)

    @property
    def preemptable(self):
        """
        Whether a preemptable node is permitted, or a nonpreemptable one is required.

        :rtype: bool
        """
        return self._fetchRequirement('preemptable')
    @preemptable.setter
    def preemptable(self, val):
         self._requirementOverrides['preemptable'] = self._parseResource('preemptable', val)

class JobDescription(Requirer):
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

    def __init__(self, requirements: Dict[str, Union[int, str, bool]], jobName: str, unitName: str='', displayName: str='', command: Optional[str]=None) -> None:
        """
        Create a new JobDescription.

        :param dict requirements: Dict from string to number, string, or bool
            describing the resource requirments of the job. 'cores', 'memory',
            'disk', and 'preemptable' fields, if set, are parsed and broken out
            into properties. If unset, the relevant property will be
            unspecified, and will be pulled from the assigned Config object if
            queried (see :meth:`toil.job.Requirer.assignConfig`).
        :param str jobName: Name of the kind of job this is. May be used in job
            store IDs and logging. Also used to let the cluster scaler learn a
            model for how long the job will take. Ought to be the job class's
            name if no real user-defined name is available.
        :param str unitName: Name of this instance of this kind of job. May
            appear with jobName in logging.
        :param str displayName: A human-readable name to identify this
            particular job instance. Ought to be the job class's name
            if no real user-defined name is available.
        """

        # Set requirements
        super().__init__(requirements)

        # Save names, making sure they are strings and not e.g. bytes.
        def makeString(x):
            return x if not isinstance(x, bytes) else x.decode('utf-8', errors='replace')
        self.jobName = makeString(jobName)
        self.unitName = makeString(unitName)
        self.displayName = makeString(displayName)

        # Set properties that are not fully filled in on creation.

        # ID of this job description in the JobStore.
        self.jobStoreID = TemporaryID()

        # Mostly fake, not-really-executable command string that encodes how to
        # find the Job body data that this JobDescription describes, and the
        # module(s) needed to unpickle it.
        #
        # Gets replaced with/rewritten into the real, executable command when
        # the leader passes the description off to the batch system to be
        # executed.
        self.command: Optional[str] = command

        # Set scheduling properties that the leader read to think about scheduling.

        # The number of times the job should be attempted. Includes the initial
        # try, plus the nu,ber of times to retry if the job fails. This number
        # is reduced each time the job is run, until it is zero, and then no
        # further attempts to run the job are made. If None, taken as the
        # default value for this workflow execution.
        self._remainingTryCount = None

        # Holds FileStore FileIDs of the files that this job has deleted. Used
        # to journal deletions of files and recover from a worker crash between
        # committing a JobDescription update and actually executing the
        # requested deletions.
        self.filesToDelete = []

        # Holds JobStore Job IDs of the jobs that have been chained into this
        # job, and which should be deleted when this job finally is deleted.
        self.jobsToDelete = []

        # The number of direct predecessors of the job. Needs to be stored at
        # the JobDescription to support dynamically-created jobs with multiple
        # predecessors. Otherwise, we could reach a job by one path down from
        # the root and decide to schedule it without knowing that it is also
        # reachable from other paths down from the root.
        self.predecessorNumber = 0

        # The IDs of predecessor jobs that have finished. Managed by the Leader
        # and ToilState, and just stored in the JobDescription. Never changed
        # after the job is scheduled, so we don't ahve to worry about
        # conflicting updates from workers.
        # TODO: Move into ToilState itself so leader stops mutating us so much?
        self.predecessorsFinished = set()

        # Note that we don't hold IDs of our predecessors. Predecessors know
        # about us, and not the other way around. Otherwise we wouldn't be able
        # to save ourselves to the job store until our predecessors were saved,
        # but they'd also be waiting on us.

        # The IDs of all child jobs of the described job.
        # Children which are done must be removed with filterSuccessors.
        self.childIDs = set()

        # The IDs of all follow-on jobs of the described job.
        # Follow-ons which are done must be removed with filterSuccessors.
        self.followOnIDs = set()

        # Dict from ServiceHostJob ID to list of child ServiceHostJobs that start after it.
        # All services must have an entry, if only to an empty list.
        self.serviceTree = {}

        # A jobStoreFileID of the log file for a job. This will be None unless the job failed and
        # the logging has been captured to be reported on the leader.
        self.logJobStoreFileID = None

    def serviceHostIDsInBatches(self):
        """
        Get an iterator over all batches of service host job IDs that can be
        started at the same time, in the order they need to start in.
        """

        # First start all the jobs with no parent
        roots = set(self.serviceTree.keys())
        for parent, children in self.serviceTree.items():
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

    def allSuccessors(self):
        """
        Get an iterator over all child and follow-on job IDs
        """
        return itertools.chain(self.childIDs, self.followOnIDs)

    @property
    def services(self):
        """
        Get a collection of the IDs of service host jobs for this job, in arbitrary order.

        Will be empty if the job has no unfinished services.
        """

        return list(self.serviceTree.keys())

    def nextSuccessors(self):
        """
        Return the collection of job IDs for the successors of this job that,
        according to this job, are ready to run.

        If those jobs have multiple predecessor relationships, they may still
        be blocked on other jobs.

        Returns None when at the final phase (all successors done), and an
        empty collection if there are more phases but they can't be entered yet
        (e.g. because we are waiting for the job itself to run).
        """

        if self.command is not None:
            # We ourselves need to run. So there's not nothing to do but no successors are ready.
            return []
        elif len(self.childIDs) != 0:
            # Our children need to run
            return self.childIDs
        elif len(self.followOnIDs) != 0:
            # Our follow-ons need to run
            return self.followOnIDs
        else:
            # Everything is done.
            return None

    @property
    def stack(self):
        """
        Get an immutable collection of immutable collections of IDs of successors that need to run still.

        Batches of successors are in reverse order of the order they need to run in.

        Some successors in each batch may have already been finished. Batches may be empty.

        Exists so that code that used the old stack list immutably can work
        still. New development should use nextSuccessors(), and all mutations
        should use filterSuccessors() (which automatically removes completed
        phases).

        :return: Batches of successors that still need to run, in reverse
                 order. An empty batch may exist under a non-empty batch, or at the top
                 when the job itself is not done.
        :rtype: tuple(tuple(str))
        """

        result = []
        if self.command is not None or len(self.childIDs) != 0 or len(self.followOnIDs) != 0:
            # Follow-ons haven't all finished yet
            result.append(tuple(self.followOnIDs))
        if self.command is not None or len(self.childIDs) != 0:
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

        # Get all the services we shouldn't have anymore
        toRemove = set()
        for serviceID in self.services:
            if not predicate(serviceID):
                toRemove.add(serviceID)

        # Drop everything from that set as a value and a key
        self.serviceTree = {k: [x for x in v if x not in toRemove] for k, v in self.serviceTree.items() if k not in toRemove}

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

        Useful for chaining jobs: the chained-to job can replace the parent job.

        Merges cleanup state from the job being replaced into this one.

        :param toil.job.JobDescription other: Job description to replace.
        """

        # TODO: also be able to take on the successors of the other job, under
        # ours on the stack, somehow.

        self.jobStoreID = other.jobStoreID

        # Save files and jobs to delete from the job we replaced, so we can
        # roll up a whole chain of jobs and delete them when they're all done.
        self.filesToDelete += other.filesToDelete
        self.jobsToDelete += other.jobsToDelete

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

        IDs not present in the renames dict are left as-is.

        :param dict(TemporaryID, str) renames: Rename operations to apply.
        """

        self.childIDs = {renames.get(old, old) for old in self.childIDs}
        self.followOnIDs = {renames.get(old, old) for old in self.followOnIDs}
        self.serviceTree = {renames.get(parent, parent): [renames.get(child, child) for child in children]
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

    def setupJobAfterFailure(self, exitReason=None):
        """
        Reduce the remainingTryCount if greater than zero and set the memory
        to be at least as big as the default memory (in case of exhaustion of memory,
        which is common).

        Requires a configuration to have been assigned (see :meth:`toil.job.Requirer.assignConfig`).

        :param toil.batchSystems.abstractBatchSystem.BatchJobExitReason exitReason: The configuration for the current workflow run.

        """

        # Avoid potential circular imports
        from toil.batchSystems.abstractBatchSystem import BatchJobExitReason

        # Old version of this function used to take a config. Make sure that isn't happening.
        assert not isinstance(exitReason, Config), "Passing a Config as an exit reason"
        # Make sure we have an assigned config.
        assert self._config is not None

        if self._config.enableUnlimitedPreemptableRetries and exitReason == BatchJobExitReason.LOST:
            logger.info("*Not* reducing try count (%s) of job %s with ID %s",
                        self.remainingTryCount, self, self.jobStoreID)
        else:
            self.remainingTryCount = max(0, self.remainingTryCount - 1)
            logger.warning("Due to failure we are reducing the remaining try count of job %s with ID %s to %s",
                           self, self.jobStoreID, self.remainingTryCount)
        # Set the default memory to be at least as large as the default, in
        # case this was a malloc failure (we do this because of the combined
        # batch system)
        if exitReason == BatchJobExitReason.MEMLIMIT and self._config.doubleMem:
            self.memory = self.memory * 2
            logger.warning("We have doubled the memory of the failed job %s to %s bytes due to doubleMem flag",
                           self, self.memory)
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

    @property
    def remainingTryCount(self):
        """
        The try count set on the JobDescription, or the default based on the
        retry count from the config if none is set.
        """
        if self._remainingTryCount is not None:
            return self._remainingTryCount
        elif self._config is not None:
            # Our try count should be the number of retries in the config, plus
            # 1 for the initial try
            return self._config.retryCount + 1
        else:
            raise AttributeError(f"Try count for {self} cannot be determined")
    @remainingTryCount.setter
    def remainingTryCount(self, val):
        self._remainingTryCount = val

    def clearRemainingTryCount(self):
        """
        Clear remainingTryCount and set it back to its default value.

        :returns: True if a modification to the JobDescription was made, and
                  False otherwise.
        :rtype: bool
        """
        if self._remainingTryCount is not None:
            # We had a value stored
            self._remainingTryCount = None
            return True
        else:
            # No change needed
            return False


    def __str__(self):
        """
        Produce a useful logging string identifying this job.
        """

        printedName = "'" + self.jobName + "'"
        if self.unitName:
            printedName += ' ' + self.unitName

        if self.jobStoreID is not None:
            printedName += ' ' + str(self.jobStoreID)

        return printedName

    # Not usable as a key (not hashable) and doesn't have any value-equality.
    # There really should only ever be one true version of a JobDescription at
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
        checkpoint's successors will be deleted, but its try count
        will *not* be decreased.

        Returns a list with the IDs of any successors deleted.
        """
        assert self.checkpoint is not None
        successorsDeleted = []
        if self.childIDs or self.followOnIDs or self.serviceTree or self.command is not None:
            if self.command is not None:
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
                       unitName='', checkpoint=False, displayName='',
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

        :type memory: int or string convertible by toil.lib.conversions.human2bytes to an int
        :type cores: float, int, or string convertible by toil.lib.conversions.human2bytes to an int
        :type disk: int or string convertible by toil.lib.conversions.human2bytes to an int
        :type preemptable: bool, int in {0, 1}, or string in {'false', 'true'} in any case
        :type unitName: str
        :type checkpoint: bool
        :type displayName: str
        :type descriptionClass: class
        """

        # Fill in our various names
        jobName = self.__class__.__name__
        displayName = displayName if displayName else jobName


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
        # Make it with a temporary ID until we can be assigned a real one by
        # the JobStore.
        self._description = descriptionClass(requirements, jobName, unitName=unitName, displayName=displayName)

        # Private class variables needed to actually execute a job, in the worker.
        # Also needed for setting up job graph structures before saving to the JobStore.

        # This dict holds a mapping from TemporaryIDs to the job objects they represent.
        # Will be shared among all jobs in a disconnected piece of the job
        # graph that hasn't been registered with a JobStore yet.
        # Make sure to initially register ourselves.
        self._registry = {self._description.jobStoreID: self}

        # Job relationships are all stored exactly once in the JobDescription.
        # Except for predecessor relationships which are stored here, just
        # while the user is creating the job graphs, to check for duplicate
        # relationships and to let EncapsulatedJob magically add itself as a
        # child. Note that this stores actual Job objects, to call addChild on.
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

    def __str__(self):
        """
        Produce a useful logging string to identify this Job and distinguish it
        from its JobDescription.
        """
        if self.description is None:
            return repr(self)
        else:
            return 'Job(' + str(self.description) + ')'

    @property
    def jobStoreID(self):
        """
        Get the ID of this Job.

        :rtype: str|toil.job.TemporaryID
        """
        # This is managed by the JobDescription.
        return self._description.jobStoreID

    @property
    def description(self):
        """
        Expose the JobDescription that describes this job.

        :rtype: toil.job.JobDescription
        """
        return self._description

    # Instead of being a Requirer ourselves, we pass anything about
    # requirements through to the JobDescription.

    @property
    def disk(self) -> int:
        """
        The maximum number of bytes of disk the job will require to run.

        :rtype: int
        """
        return self.description.disk
    @disk.setter
    def disk(self, val):
         self.description.disk = val

    @property
    def memory(self):
        """
        The maximum number of bytes of memory the job will require to run.

        :rtype: int
        """
        return self.description.memory
    @memory.setter
    def memory(self, val):
         self.description.memory = val

    @property
    def cores(self):
        """
        The number of CPU cores required.

       :rtype: int|float
        """
        return self.description.cores
    @cores.setter
    def cores(self, val):
         self.description.cores = val

    @property
    def preemptable(self):
        """
        Whether the job can be run on a preemptable node.

        :rtype: bool
        """
        return self.description.preemptable
    @preemptable.setter
    def preemptable(self, val):
         self.description.preemptable = val

    @property
    def checkpoint(self):
        """
        Determine if the job is a checkpoint job or not.

        :rtype: bool
        """

        return isinstance(self._description, CheckpointJobDescription)

    def assignConfig(self, config):
        """
        Assign the given config object to be used by various actions
        implemented inside the Job class.

        :param toil.common.Config config: Config object to query
        """
        self.description.assignConfig(config)


    def run(self, fileStore):
        """
        Override this function to perform work and dynamically create successor jobs.

        :param toil.fileStores.abstractFileStore.AbstractFileStore fileStore: Used to create local and
               globally sharable temporary files and to send log messages to the leader
               process.

        :return: The return value of the function can be passed to other jobs by means of
                 :func:`toil.job.Job.rv`.
        """

    def _jobGraphsJoined(self, other):
        """
        Called whenever the job graphs of this job and the other job may have been merged into one connected component.

        Ought to be called on the bigger registry first.

        Merges TemporaryID registries if needed.

        :param toil.job.Job other: A job possibly from the other connected component
        """

        # Maintain the invariant that a whole connected component has a config
        # assigned if any job in it does.
        if self.description._config is None and other.description._config is not None:
            # The other component has a config assigned but this component doesn't.
            for job in self._registry.values():
                job.assignConfig(other.description._config)
        elif other.description._config is None and self.description._config is not None:
            # We have a config assigned but the other component doesn't.
            for job in other._registry.values():
                job.assignConfig(self.description._config)

        if len(self._registry) < len(other._registry):
            # Merge into the other component instead
            other._jobGraphsJoined(self)
        else:
            if self._registry != other._registry:
                # We are in fact joining connected components.

                # Steal everything from the other connected component's registry
                self._registry.update(other._registry)

                for job in other._registry.values():
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

        assert isinstance(childJob, Job)

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

        assert isinstance(followOnJob, Job)

        # Join the job graphs
        self._jobGraphsJoined(followOnJob)
        # Remember the follow-on relationship
        self._description.addFollowOn(followOnJob.jobStoreID)
        # Record the temporary back-reference
        followOnJob._addPredecessor(self)

        return followOnJob

    def hasPredecessor(self, job: 'Job') -> bool:
        """Check if a given job is already a predecessor of this job."""
        return job in self._directPredecessors

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

        # For compatibility with old Cactus versions that tinker around with
        # our internals, we need to make the hosting job available as
        # self._services[-1]. TODO: Remove this when Cactus has updated.
        self._services = [hostingJob]

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

    def encapsulate(self, name=None):
        """
        Encapsulates the job, see :class:`toil.job.EncapsulatedJob`.
        Convenience function for constructor of :class:`toil.job.EncapsulatedJob`.

        :param str name: Human-readable name for the encapsulated job.

        :return: an encapsulated version of this job.
        :rtype: toil.job.EncapsulatedJob
        """
        return EncapsulatedJob(self, unitName=name)

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
            # We haven't had a job store set to put our return value into, so
            # we must not have been hit yet in job topological order.
            raise JobPromiseConstraintError(self)
        # TODO: can we guarantee self.jobStoreID is populated and so pass that here?
        with self._promiseJobStore.writeFileStream() as (fileHandle, jobStoreFileID):
            promise = UnfulfilledPromiseSentinel(str(self.description), False)
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

    def getRootJobs(self) -> Set['Job']:
        """
        Returns the set of root job objects that contain this job.
        A root job is a job with no predecessors (i.e. which are not children, follow-ons, or services).

        Only deals with jobs created here, rather than loaded from the job store.
        """

        # Start assuming all jobs are roots
        roots = set(self._registry.keys())

        for job in self._registry.values():
            for otherID in job.description.successorsAndServiceHosts():
                # If anything is a successor or service of anything else, it isn't a root.
                if otherID in roots:
                    # Remove it if we still think it is
                    roots.remove(otherID)

        return {self._registry[jid] for jid in roots}

    def checkJobGraphConnected(self):
        """
        :raises toil.job.JobGraphDeadlockException: if :func:`toil.job.Job.getRootJobs` does \
        not contain exactly one root job.

        As execution always starts from one root job, having multiple root jobs will \
        cause a deadlock to occur.

        Only deals with jobs created here, rather than loaded from the job store.
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

        A follow-on edge (A, B) between two jobs A and B is equivalent \
        to adding a child edge to B from (1) A, (2) from each child of A, \
        and (3) from the successors of each child of A. We call each such edge \
        an edge an "implied" edge. The augmented job graph is a job graph including \
        all the implied edges.

        For a job graph G = (V, E) the algorithm is ``O(|V|^2)``. It is ``O(|V| + |E|)`` for \
        a graph with no follow-ons. The former follow-on case could be improved!

        Only deals with jobs created here, rather than loaded from the job store.
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
            for successor in [self._registry[jID] for jID in self.description.allSuccessors() if jID in self._registry] + extraEdges[self]:
                # Grab all the successors in the current registry (i.e. added form this node) and look at them.
                successor._checkJobGraphAcylicDFS(stack, visited, extraEdges)
            assert stack.pop() == self
        if self in stack:
            stack.append(self)
            raise JobGraphDeadlockException("A cycle of job dependencies has been detected '%s'" % stack)

    @staticmethod
    def _getImpliedEdges(roots):
        """
        Gets the set of implied edges (between children and follow-ons of a common job). Used in Job.checkJobGraphAcylic.

        Only deals with jobs created here, rather than loaded from the job store.

        :returns: dict from Job object to list of Job objects that must be done before it can start.
        """
        #Get nodes (Job objects) in job graph
        nodes = set()
        for root in roots:
            root._collectAllSuccessors(nodes)

        ##For each follow-on edge calculate the extra implied edges
        #Adjacency list of implied edges, i.e. map of jobs to lists of jobs
        #connected by an implied edge
        extraEdges = dict([(n, []) for n in nodes])
        for job in nodes:
            for depth in range(1, len(job.description.stack)):
                # Add edges from all jobs in the earlier/upper subtrees to all
                # the roots of the later/lower subtrees

                upper = job.description.stack[depth]
                lower = job.description.stack[depth - 1]

                # Find everything in the upper subtree
                reacheable = set()
                for upperID in upper:
                    if upperID in job._registry:
                        # This is a locally added job, not an already-saved job
                        upperJob = job._registry[upperID]
                        upperJob._collectAllSuccessors(reacheable)

                for inUpper in reacheable:
                    # Add extra edges to the roots of all the lower subtrees
                    # But skip anything in the lower subtree not in the current _registry (i.e. not created hear)
                    extraEdges[inUpper] += [job._registry[lowerID] for lowerID in lower if lowerID in job._registry]

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
            set_logging_from_options(options)
            with Toil(options) as toil:
                if not options.restart:
                    return toil.start(job)
                else:
                    return toil.restart()

    class Service(Requirer, metaclass=ABCMeta):
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

            # Save the requirements in ourselves so they are visible on `self` to user code.
            super().__init__({'memory': memory, 'cores': cores, 'disk': disk, 'preemptable': preemptable})

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

        @abstractmethod
        def stop(self, job):
            """
            Stops the service. Function can block until complete.

            :param toil.job.Job job: The underlying host job that the service is being run in.
                                     Can be used to register deferred functions, or to access
                                     the fileStore for creating temporary files.
            """

        def check(self):
            """
            Checks the service is still running.

            :raise exceptions.RuntimeError: If the service failed, this will cause the service job to be labeled failed.
            :returns: True if the service is still running, else False. If False then the service job will be terminated,
                and considered a success. Important point: if the service job exits due to a failure, it should raise a
                RuntimeError, not return False!
            """

    def _addPredecessor(self, predecessorJob):
        """Adds a predecessor job to the set of predecessor jobs."""
        if predecessorJob in self._directPredecessors:
            raise ConflictingPredecessorError(predecessorJob, self)
        self._directPredecessors.add(predecessorJob)

        # Record the need for the predecessor to finish
        self._description.addPredecessor()

    @staticmethod
    def _isLeafVertex(job):
        return next(job.description.successorsAndServiceHosts(), None) is None

    @classmethod
    def _loadUserModule(cls, userModule):
        """
        Imports and returns the module object represented by the given module descriptor.

        :type userModule: ModuleDescriptor
        """
        return userModule.load()

    @classmethod
    def _unpickle(cls, userModule, fileHandle, requireInstanceOf=None):
        """
        Unpickles an object graph from the given file handle while loading symbols \
        referencing the __main__ module from the given userModule instead.

        :param userModule:
        :param fileHandle: An open, binary-mode file handle.
        :param requireInstanceOf: If set, require result to be an instance of this class.
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
        if requireInstanceOf is not None:
            assert isinstance(runnable, requireInstanceOf), "Did not find a {} when expected".format(requireInstanceOf)

        return runnable

    def getUserScript(self):
        return self.userModule

    def _fulfillPromises(self, returnValues, jobStore):
        """
        Sets the values for promises using the return values from this job's run() function.
        """
        for path, promiseFileStoreIDs in self._rvs.items():
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

        Only considers jobs in this job's subgraph that are newly added, not loaded from the job store.
        """

        # Keep our own stack since we may have a stick in the graph long enough
        # to exhaust the real stack
        todo = [self]

        while len(todo) > 0:
            job = todo[-1]
            todo.pop()
            if job not in visited:
                visited.add(job)
                for successorID in job.description.allSuccessors():
                    if successorID in self._registry:
                        # We added this successor locally
                        todo.append(self._registry[successorID])

    def getTopologicalOrderingOfJobs(self):
        """
        :returns: a list of jobs such that for all pairs of indices i, j for which i < j, \
        the job at index i can be run before the job at index j.

        Only considers jobs in this job's subgraph that are newly added, not loaded from the job store.

        Ignores service jobs.

        :rtype: list[Job]
        """

        # List of Job objects in order.
        ordering = []
        # Set of IDs of visited jobs.
        visited = set()

        # We need to recurse and traverse the graph without exhausting Python's
        # stack, so we keep our own stack of Job objects
        todo = [self]

        while len(todo) > 0:
            job = todo[-1]
            todo.pop()

            #Do not add the job to the ordering until all its predecessors have been
            #added to the ordering
            outstandingPredecessor = False
            for predJob in job._directPredecessors:
                if predJob.jobStoreID not in visited:
                    outstandingPredecessor = True
                    break
            if outstandingPredecessor:
                continue

            if job.jobStoreID not in visited:
                visited.add(job.jobStoreID)
                ordering.append(job)

                for otherID in itertools.chain(job.description.followOnIDs, job.description.childIDs):
                    if otherID in self._registry:
                        # Stack up descendants so we process children and then follow-ons.
                        # So stack up follow-ons deeper
                        todo.append(self._registry[otherID])

        return ordering

    ####################################################
    #Storing Jobs into the JobStore
    ####################################################

    def _register(self, jobStore):
        """
        If this job lacks a JobStore-assigned ID, assign this job an ID.
        Must be called for each job before it is saved to the JobStore for the first time.

        :returns: A list with either one old ID, new ID pair, or an empty list
        :rtype: list
        """

        # TODO: This doesn't really have much to do with the registry. Rename
        # the registry.

        if isinstance(self.jobStoreID, TemporaryID):
            # We need to get an ID.

            # Save our fake ID
            fake = self.jobStoreID

            # Replace it with a real ID
            jobStore.assignID(self.description)

            # Make sure the JobDescription can do its JobStore-related setup.
            self.description.onRegistration(jobStore)

            # Return the fake to real mapping
            return [(fake, self.description.jobStoreID)]
        else:
            # We already have an ID. No assignment or reference rewrite necessary.
            return []

    def _renameReferences(self, renames):
        """
        Apply the given dict of ID renames to all references to other jobs.

        Ignores the registry, which is shared and assumed to already be updated.

        IDs not present in the renames dict are left as-is.

        :param dict(TemporaryID, str) renames: Rename operations to apply.
        """

        # Do renames in the description
        self._description.renameReferences(renames)

    def saveBody(self, jobStore):
        """
        Save the execution data for just this job to the JobStore, and fill in
        the JobDescription with the information needed to retrieve it.

        The Job's JobDescription must have already had a real jobStoreID assigned to it.

        Does not save the JobDescription.

        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore: The job store
            to save the job body into.
        """

        # We can't save the job in the right place for cleanup unless the
        # description has a real ID.
        assert not isinstance(self.jobStoreID, TemporaryID), "Tried to save job {} without ID assigned!".format(self)

        # Note that we can't accept any more requests for our return value
        self._disablePromiseRegistration()

        # Clear out old Cactus compatibility fields that don't need to be
        # preserved and shouldn't be serialized.
        if hasattr(self, '_services'):
            delattr(self, '_services')

        # Remember fields we will overwrite
        description = self._description
        registry = self._registry
        directPredecessors = self._directPredecessors

        try:
            try:
                # Drop out the description, which the job store will manage separately
                self._description = None
                # Fix up the registry and direct predecessors for when the job is
                # loaded to be run: the registry should contain just the job itself and
                # there should be no predecessors available when the job actually runs.
                self._registry = {description.jobStoreID: self}
                self._directPredecessors = set()

                # Save the body of the job
                with jobStore.writeFileStream(description.jobStoreID, cleanup=True) as (fileHandle, fileStoreID):
                    pickle.dump(self, fileHandle, pickle.HIGHEST_PROTOCOL)
            finally:
                # Restore important fields (before handling errors)
                self._directPredecessors = directPredecessors
                self._registry = registry
                self._description = description
        except JobPromiseConstraintError as e:
            # The user is passing promises without regard to predecessor constraints.
            if e.recipientJob is None:
                # Add ourselves as the recipient job that wanted the promise.
                e = JobPromiseConstraintError(e.promisingJob, self)
            raise e

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
        Save job data and new JobDescriptions to the given job store for this
        job and all descending jobs, including services.

        Used to save the initial job graph containing the root job of the workflow.

        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore: The job store
            to save the jobs into.
        :param bool saveSelf: Set to True to save this job along with its children,
            follow-ons, and services, or False to just save the children, follow-ons,
            and services and to populate the return value.
        :param returnValues: The collection of values returned when executing
            the job (or starting the service the job is hosting). If saveSelf
            is not set, will be used to fulfill the job's return value promises.
        """

        # Prohibit cycles and multiple roots
        self.checkJobGraphForDeadlocks()

        # Make sure everybody in the registry is registered with the job store
        # and has an ID. Also rewrite ID references.
        allJobs = list(self._registry.values())
        # We use one big dict from fake ID to corresponding real ID to rewrite references.
        fakeToReal = {}
        for job in allJobs:
            # Register the job, get the old ID to new ID pair if any, and save that in the fake to real mapping
            fakeToReal.update(job._register(jobStore))
        if len(fakeToReal) > 0:
            # Some jobs changed ID. We need to rebuild the registry and apply the reference rewrites.

            # Remake the registry in place
            self._registry.clear()
            self._registry.update({job.jobStoreID: job for job in allJobs})

            for job in allJobs:
                # Tell all the jobs (and thus their descriptions and services)
                # about the renames.
                job._renameReferences(fakeToReal)

        # Make sure the whole component is ready for promise registration
        for job in allJobs:
            job.prepareForPromiseRegistration(jobStore)

        # Get an ordering on the non-service jobs which we use for pickling the
        # jobs in the correct order to ensure the promises are properly
        # established
        ordering = self.getTopologicalOrderingOfJobs()

        # Set up to save last job first, so promises flow the right way
        ordering.reverse()

        logger.info("Saving graph of %d jobs, %d new", len(allJobs), len(fakeToReal))

        # Make sure we're the root
        assert ordering[-1] == self

        # Don't verify the ordering length: it excludes service host jobs.

        if not saveSelf:
            # Fulfil promises for return values (even if value is None)
            self._fulfillPromises(returnValues, jobStore)

        for job in ordering:
            logger.info("Processing job %s", job.description)
            for serviceBatch in reversed(list(job.description.serviceHostIDsInBatches())):
                # For each batch of service host jobs in reverse order they start
                for serviceID in serviceBatch:
                    logger.info("Processing service %s", serviceID)
                    if serviceID in self._registry:
                        # It's a new service

                        # Find the actual job
                        serviceJob = self._registry[serviceID]
                        logger.info("Saving service %s", serviceJob.description)
                        # Pickle the service body, which triggers all the promise stuff
                        serviceJob.saveBody(jobStore)
            if job != self or saveSelf:
                # Now pickle the job itself
                job.saveBody(jobStore)

        # Now that the job data is on disk, commit the JobDescriptions in
        # reverse execution order, in a batch if supported.
        with jobStore.batch():
            for job in ordering:
                for serviceBatch in job.description.serviceHostIDsInBatches():
                    for serviceID in serviceBatch:
                        if serviceID in self._registry:
                            jobStore.create(self._registry[serviceID].description)
                if job != self or saveSelf:
                    jobStore.create(job.description)

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

        # Assign the config from the JobStore as if we were loaded.
        # TODO: Find a better way to make this the JobStore's responsibility
        self.description.assignConfig(jobStore.config)

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
                job = cls._unpickle(userModule, fileHandle, requireInstanceOf=Job)
                # Fill in the current description
                job._description = jobDescription

                # Set up the registry again, so children and follow-ons can be added on the worker
                job._registry = {job.jobStoreID: job}

                return job

                # TODO: We ought to just unpickle straight from a streaming read
        finally:
            # Clean up the file
            if os.path.exists(filename):
                os.unlink(filename)
            # Clean up the directory we put it in
            shutil.rmtree(directory)

    def _run(self, jobGraph=None, fileStore=None, **kwargs):
        """
        Function which worker calls to ultimately invoke
        a job's Job.run method, and then handle created
        children/followOn jobs.

        May be (but currently is not) overridden by specialized Toil-internal jobs.

        Should not be overridden by non-Toil code!

        Despite this, it has been overridden by non-Toil code, so we keep it
        around and use a hardened kwargs-based interface to try and tolerate
        bad behavior by workflows (e.g. Cactus).

        When everyone has upgraded to a sufficiently new Cactus, we can remove
        this!

        :param NoneType jobGraph: Ignored. Here for compatibility with old
               Cactus versions that pass two positional arguments.
        :param toil.fileStores.abstractFileStore.AbstractFileStore fileStore: the
               FileStore to use to access files when running the job. Required.
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
            startClock = get_total_cpu_time()
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
            totalCpuTime, totalMemoryUsage = get_total_cpu_time_and_memory_usage()
            stats.jobs.append(
                Expando(
                    time=str(time.time() - startTime),
                    clock=str(totalCpuTime - startClock),
                    class_name=self._jobName(),
                    memory=str(totalMemoryUsage)
                )
            )

    def _runner(self, jobStore=None, fileStore=None, defer=None, **kwargs):
        """
        This method actually runs the job, and serialises the next jobs.

        It marks the job as completed (by clearing its command) and creates the
        successor relationships to new successors, but it doesn't actually
        commit those updates to the current job into the JobStore.

        We take all arguments as keyword arguments, and accept and ignore
        additional keyword arguments, for compatibility with workflows (*cough*
        Cactus *cough*) which are reaching in and overriding _runner (which
        they aren't supposed to do). If everything is passed as name=value it
        won't break as soon as we add or remove a parameter.

        :param class jobStore: Instance of the job store
        :param toil.fileStores.abstractFileStore.AbstractFileStore fileStore: Instance
               of a cached or uncached filestore
        :param defer: Function yielded by open() context
               manager of :class:`toil.DeferredFunctionManager`, which is called to
               register deferred functions.
        :param kwargs: Catch-all to accept superfluous arguments passed by old
               versions of Cactus. Cactus shouldn't override this method, but it does.
        :return:
        """

        # Make deferred function registration available during run().
        self._defer = defer
        # Make fileStore available as an attribute during run() ...
        self._fileStore = fileStore
        # ... but also pass it to _run() as an argument for backwards
        # compatibility with workflows that tinker around with our internals,
        # and send a fake jobGraph in case they still think jobGraph exists.
        returnValues = self._run(jobGraph=None, fileStore=fileStore)

        # Clean up state changes made for run()
        self._defer = None
        self._fileStore = None


        # Serialize the new Jobs defined by the run method to the jobStore
        self._saveJobGraph(jobStore, saveSelf=False, returnValues=returnValues)

        # Clear out the command, because the job is done.
        self.description.command = None

        # That and the new child/follow-on relationships will need to be
        # recorded later by an update() of the JobDescription.



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
        argSpec = inspect.getfullargspec(userFunction)

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
            if dehumanize and isinstance(value, str):
                value = human2bytes(value)
            return value

        super().__init__(memory=resolve('memory', dehumanize=True),
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
        Aprime = A.encapsulate()
        Aprime.addChild(B)
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
    def __init__(self, job, unitName=None):
        """
        :param toil.job.Job job: the job to encapsulate.
        :param str unitName: human-readable name to identify this job instance.
        """

        if job is not None:
            # Initial construction, when encapsulating a job

            # Giving the root of the subgraph the same resources as the first job in the subgraph.
            super().__init__(**job.description.requirements, unitName=unitName)
            # Ensure that the encapsulated job has the same direct predecessors as the job
            # being encapsulated.
            for predJob in job._directPredecessors:
                predJob.addChild(self)
            self.encapsulatedJob = job
            Job.addChild(self, job)
            # Use small resource requirements for dummy Job instance.
            # But not too small, or the job won't have enough resources to safely start up Toil.
            self.encapsulatedFollowOn = Job(disk='100M', memory='512M', cores=0.1, unitName=None if unitName is None else unitName + '-followOn')
            Job.addFollowOn(self, self.encapsulatedFollowOn)
        else:
            # Unpickling on the worker, to be run as a no-op.
            # No need to try and hook things up, but nobody can add children or
            # follow-ons to us now either.
            super().__init__()
            self.encapsulatedJob = None
            self.encapsulatedFollowOn = None

    def addChild(self, childJob):
        assert self.encapsulatedFollowOn is not None, \
            "Children cannot be added to EncapsulatedJob while it is running"
        return Job.addChild(self.encapsulatedFollowOn, childJob)

    def addService(self, service, parentService=None):
        assert self.encapsulatedFollowOn is not None, \
            "Services cannot be added to EncapsulatedJob while it is running"
        return Job.addService(self.encapsulatedFollowOn, service, parentService=parentService)

    def addFollowOn(self, followOnJob):
        assert self.encapsulatedFollowOn is not None, \
            "Follow-ons cannot be added to EncapsulatedJob while it is running"
        return Job.addFollowOn(self.encapsulatedFollowOn, followOnJob)

    def rv(self, *path):
        assert self.encapsulatedJob is not None
        return self.encapsulatedJob.rv(*path)

    def prepareForPromiseRegistration(self, jobStore):
        # This one will be called after execution when re-serializing the
        # (unchanged) graph of jobs rooted here.
        super().prepareForPromiseRegistration(jobStore)
        if self.encapsulatedJob is not None:
            # Running where the job was created.
            self.encapsulatedJob.prepareForPromiseRegistration(jobStore)

    def _disablePromiseRegistration(self):
        assert self.encapsulatedJob is not None
        super()._disablePromiseRegistration()
        self.encapsulatedJob._disablePromiseRegistration()

    def __reduce__(self):
        """
        Called during pickling to define the pickled representation of the job.

        We don't want to pickle our internal references to the job we
        encapsulate, so we elide them here. When actually run, we're just a
        no-op job that can maybe chain.
        """

        return self.__class__, (None,)

    def getUserScript(self):
        assert self.encapsulatedJob is not None
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
        super().__init__(**service.requirements,
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

    def saveBody(self, jobStore):
        """
        Serialize the service itself before saving the host job's body.
        """
        # Save unpickled service
        service = self.service
        # Serialize service
        self.pickledService = pickle.dumps(self.service, protocol=pickle.HIGHEST_PROTOCOL)
        # Clear real service until we have the module to load it back
        self.service = None
        # Save body as normal
        super().saveBody(jobStore)
        # Restore unpickled service
        self.service = service
        self.pickledService = None

    def run(self, fileStore):
        # Unpickle the service
        logger.debug('Loading service module %s.', self.serviceModule)
        userModule = self._loadUserModule(self.serviceModule)
        service = self._unpickle(userModule, BytesIO(self.pickledService), requireInstanceOf=Job.Service)
        self.pickledService = None
        # Make sure it has the config, since it wasn't load()-ed via the JobStore
        service.assignConfig(fileStore.jobStore.config)
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
            assert self.description.startJobStoreID != None
            if fileStore.jobStore.fileExists(self.description.startJobStoreID):
                fileStore.jobStore.deleteFile(self.description.startJobStoreID)
            assert not fileStore.jobStore.fileExists(self.description.startJobStoreID)

            #Now block until we are told to stop, which is indicated by the removal
            #of a file
            assert self.description.terminateJobStoreID != None
            while True:
                # Check for the terminate signal
                if not fileStore.jobStore.fileExists(self.description.terminateJobStoreID):
                    logger.debug("Detected that the terminate jobStoreID has been removed so exiting")
                    if not fileStore.jobStore.fileExists(self.description.errorJobStoreID):
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

            logger.debug("Service is done")
        finally:
            # The stop function is always called
            service.stop(self)

    def getUserScript(self):
        return self.serviceModule


class Promise:
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


class PromisedRequirement:
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
                                takes args as input parameters.
        :param args: variable length argument list
        :type args: int or .Promise
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


class UnfulfilledPromiseSentinel:
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
