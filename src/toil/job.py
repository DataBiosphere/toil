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

import cPickle
import collections
import importlib
import inspect
import logging
import os
import sys
import time
import uuid
import dill

from abc import ABCMeta, abstractmethod
from argparse import ArgumentParser
from contextlib import contextmanager
from io import BytesIO

from bd2k.util.exceptions import require
from bd2k.util.expando import Expando
from bd2k.util.humanize import human2bytes

from toil.common import Toil, addOptions
from toil.fileStore import DeferredFunction
from toil.lib.bioio import (setLoggingFromOptions,
                            getTotalCpuTimeAndMemoryUsage,
                            getTotalCpuTime)
from toil.resource import ModuleDescriptor

logger = logging.getLogger( __name__ )


class JobLikeObject(object):
    """
    Inherit from this class to add requirement properties to a job (or job-like) object.
    If the object doesn't specify explicit requirements, these properties will fall back
    to the configured defaults. If the value cannot be determined, an AttributeError is raised.
    """
    def __init__(self, requirements, unitName, jobName=None):
        cores = requirements.get('cores')
        memory = requirements.get('memory')
        disk = requirements.get('disk')
        preemptable = requirements.get('preemptable')
        if unitName is not None:
            assert isinstance(unitName, str)
        if jobName is not None:
            assert isinstance(jobName, str)
        self.unitName = unitName
        self.jobName = jobName if jobName is not None else self.__class__.__name__
        self._cores = self._parseResource('cores', cores)
        self._memory = self._parseResource('memory', memory)
        self._disk = self._parseResource('disk', disk)
        self._preemptable = preemptable
        self._config = None

    @property
    def disk(self):
        """
        The maximum number of bytes of disk the job will require to run.
        """
        if self._disk is not None:
            return self._disk
        elif self._config is not None:
            return self._config.defaultDisk
        else:
            raise AttributeError("Default value for 'disk' cannot be determined")

    @property
    def memory(self):
        """
        The maximum number of bytes of memory the job will require to run.
        """
        if self._memory is not None:
            return self._memory
        elif self._config is not None:
            return self._config.defaultMemory
        else:
            raise AttributeError("Default value for 'memory' cannot be determined")

    @property
    def cores(self):
        """
        The number of CPU cores required.
        """
        if self._cores is not None:
            return self._cores
        elif self._config is not None:
            return self._config.defaultCores
        else:
            raise AttributeError("Default value for 'cores' cannot be determined")

    @property
    def preemptable(self):
        """
        Whether the job can be run on a preemptable node.
        """
        if self._preemptable is not None:
            return self._preemptable
        elif self._config is not None:
            return self._config.defaultPreemptable
        else:
            raise AttributeError("Default value for 'preemptable' cannot be determined")

    @property
    def _requirements(self):
        """
        Gets a dictionary of all the object's resource requirements. Unset values are defaulted to None
        """
        return {'memory': getattr(self, 'memory', None),
                'cores': getattr(self, 'cores', None),
                'disk': getattr(self, 'disk', None),
                'preemptable': getattr(self, 'preemptable', None)}

    @staticmethod
    def _parseResource(name, value):
        """
        Parse a Toil job's resource requirement value and apply resource-specific type checks. If the
        value is a string, a binary or metric unit prefix in it will be evaluated and the
        corresponding integral value will be returned.

        :param str name: The name of the resource

        :param None|str|float|int value: The resource value

        :rtype: int|float|None

        >>> Job._parseResource('cores', None)
        >>> Job._parseResource('cores', 1), Job._parseResource('disk', 1), \
        Job._parseResource('memory', 1)
        (1, 1, 1)
        >>> Job._parseResource('cores', '1G'), Job._parseResource('disk', '1G'), \
        Job._parseResource('memory', '1G')
        (1073741824, 1073741824, 1073741824)
        >>> Job._parseResource('cores', 1.1)
        1.1
        >>> Job._parseResource('disk', 1.1)
        Traceback (most recent call last):
        ...
        TypeError: The 'disk' requirement does not accept values that are of <type 'float'>
        >>> Job._parseResource('memory', object())
        Traceback (most recent call last):
        ...
        TypeError: The 'memory' requirement does not accept values that are of <type 'object'>
        """
        assert name in ('memory', 'disk', 'cores')
        if value is None:
            return value
        elif isinstance(value, str):
            value = human2bytes(value)
        if isinstance(value, int):
            return value
        elif isinstance(value, float) and name == 'cores':
            return value
        else:
            raise TypeError("The '%s' requirement does not accept values that are of %s"
                            % (name, type(value)))

    def __str__(self):
        printedName = "'" + self.jobName + "'"
        if self.unitName:
            printedName += ' ' + self.unitName
        elif self.unitName == '':
            printedName += ' ' + 'user passed empty string for name'
        return printedName


class JobNode(JobLikeObject):
    """
    This object bridges the job graph, job, and batchsystem classes
    """
    def __init__(self, requirements, jobName, unitName, jobStoreID,
                 command, predecessorNumber=1):
        super(JobNode, self).__init__(requirements=requirements, unitName=unitName, jobName=jobName)
        self.jobStoreID = jobStoreID
        self.predecessorNumber = predecessorNumber
        self.command = command

    def __str__(self):
        return super(JobNode, self).__str__() + ' ' + self.jobStoreID

    def __hash__(self):
        return hash(self.jobStoreID)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return NotImplemented

    def __repr__(self):
        return '%s( **%r )' % (self.__class__.__name__, self.__dict__)

    @classmethod
    def fromJobGraph(cls, jobGraph):
        """
        Takes a job graph object and returns a job node object
        :param toil.jobGraph.JobGraph jobGraph: A job graph object to be transformed into a job node
        :return: A job node object
        :rtype: toil.job.JobNode
        """
        return cls(jobStoreID=jobGraph.jobStoreID,
                   requirements=jobGraph._requirements,
                   command=jobGraph.command,
                   jobName=jobGraph.jobName,
                   unitName=jobGraph.unitName,
                   predecessorNumber=jobGraph.predecessorNumber)

    @classmethod
    def fromJob(cls, job, command, predecessorNumber):
        """
        Build a job node from a job object
        :param toil.job.Job job: the job object to be transformed into a job node
        :param str command: the JobNode's command
        :param int predecessorNumber: the number of predecessors that must finish
            successfully before the job can be scheduled
        :return: a JobNode object representing the job object parameter
        :rtype: toil.job.JobNode
        """
        return cls(jobStoreID=None,
                   requirements=job._requirements,
                   command=command,
                   jobName=job.jobName,
                   unitName=job.unitName,
                   predecessorNumber=predecessorNumber)


class Job(JobLikeObject):
    """
    Class represents a unit of work in toil.
    """
    def __init__(self, memory=None, cores=None, disk=None, preemptable=None, unitName=None,
                 checkpoint=False):
        """
        This method must be called by any overriding constructor.
        
        :param memory: the maximum number of bytes of memory the job will require to run.
        :param cores: the number of CPU cores required.
        :param disk: the amount of local disk space required by the job, expressed in bytes.
        :param preemptable: if the job can be run on a preemptable node.
        :param checkpoint: if any of this job's successor jobs completely fails,
            exhausting all their retries, remove any successor jobs and rerun this job to restart the
            subtree. Job must be a leaf vertex in the job graph when initially defined, see
            :func:`toil.job.Job.checkNewCheckpointsAreCutVertices`.
        :type cores: int or string convertable by bd2k.util.humanize.human2bytes to an int
        :type disk: int or string convertable by bd2k.util.humanize.human2bytes to an int
        :type preemptable: boolean
        :type cache: int or string convertable by bd2k.util.humanize.human2bytes to an int
        :type memory: int or string convertable by bd2k.util.humanize.human2bytes to an int
        """
        requirements = {'memory': memory, 'cores': cores, 'disk': disk,
                        'preemptable': preemptable}
        super(Job, self).__init__(requirements=requirements, unitName=unitName)
        self.checkpoint = checkpoint
        #Private class variables

        #See Job.addChild
        self._children = []
        #See Job.addFollowOn
        self._followOns = []
        #See Job.addService
        self._services = []
        #A follow-on, service or child of a job A, is a "direct successor" of A; if B
        #is a direct successor of A, then A is a "direct predecessor" of B.
        self._directPredecessors = set()
        # Note that self.__module__ is not necessarily this module, i.e. job.py. It is the module
        # defining the class self is an instance of, which may be a subclass of Job that may be
        # defined in a different module.
        self.userModule = ModuleDescriptor.forModule(self.__module__)
        # Maps index paths into composite return values to lists of IDs of files containing
        # promised values for those return value items. An index path is a tuple of indices that
        # traverses a nested data structure of lists, dicts, tuples or any other type supporting
        # the __getitem__() protocol.. The special key `()` (the empty tuple) represents the
        # entire return value.
        self._rvs = collections.defaultdict(list)
        self._promiseJobStore = None
        self.fileStore = None

    def run(self, fileStore):
        """
        Override this function to perform work and dynamically create successor jobs.

        :param toil.fileStore.FileStore fileStore: Used to create local and globally
               sharable temporary files and to send log messages to the leader process.

        :return: The return value of the function can be passed to other jobs by means of
                 :func:`toil.job.Job.rv`.
        """
        pass

    def addChild(self, childJob):
        """
        Adds childJob to be run as child of this job. Child jobs will be run \
        directly after this job's :func:`toil.job.Job.run` method has completed.

        :param toil.job.Job childJob:
        :return: childJob
        :rtype: toil.job.Job
        """
        self._children.append(childJob)
        childJob._addPredecessor(self)
        return childJob

    def hasChild(self, childJob):
        """
        Check if childJob is already a child of this job.

        :param toil.job.Job childJob:
        :return: True if childJob is a child of the job, else False.
        :rtype: Boolean
        """
        return childJob in self._children

    def addFollowOn(self, followOnJob):
        """
        Adds a follow-on job, follow-on jobs will be run after the child jobs and \
        their successors have been run.

        :param toil.job.Job followOnJob:
        :return: followOnJob
        :rtype: toil.job.Job
        """
        self._followOns.append(followOnJob)
        followOnJob._addPredecessor(self)
        return followOnJob

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
            # Do check to ensure that parentService is a service of this job
            def check(services):
                for jS in services:
                    if jS.service == parentService or check(jS.service._childServices):
                        return True
                return False
            if not check(self._services):
                raise JobException("Parent service is not a service of the given job")
            return parentService._addChild(service)
        else:
            if service._hasParent:
                raise JobException("The service already has a parent service")
            service._hasParent = True
            jobService = ServiceJob(service)
            self._services.append(jobService)
            return jobService.rv()

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
        :rtype: toil.job.EncapsulatedJob.
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
        jobStoreFileID = self._promiseJobStore.getEmptyFileStoreID()
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

    ####################################################
    #Cycle/connectivity checking
    ####################################################

    def checkJobGraphForDeadlocks(self):
        """
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
        :return: The roots of the connected component of jobs that contains this job. \
        A root is a job with no predecessors.

        :rtype : set of toil.job.Job instances
        """
        roots = set()
        visited = set()
        #Function to get the roots of a job
        def getRoots(job):
            if job not in visited:
                visited.add(job)
                if len(job._directPredecessors) > 0:
                    map(lambda p : getRoots(p), job._directPredecessors)
                else:
                    roots.add(job)
                #The following call ensures we explore all successor edges.
                map(lambda c : getRoots(c), job._children +
                    job._followOns)
        getRoots(self)
        return roots

    def checkJobGraphConnected(self):
        """
        :raises toil.job.JobGraphDeadlockException: if :func:`toil.job.Job.getRootJobs` does \
        not contain exactly one root job.

        As execution always starts from one root job, having multiple root jobs will \
        cause a deadlock to occur.
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

    def checkNewCheckpointsAreLeafVertices(self):
        """
        A checkpoint job is a job that is restarted if either it fails, or if any of \
        its successors completely fails, exhausting their retries.

        A job is a leaf it is has no successors.

        A checkpoint job must be a leaf when initially added to the job graph. When its \
        run method is invoked it can then create direct successors. This restriction is made
        to simplify implementation.

        :raises toil.job.JobGraphDeadlockException: if there exists a job being added to the graph for which \
        checkpoint=True and which is not a leaf.
        """
        roots = self.getRootJobs() # Roots jobs of component, these are preexisting jobs in the graph

        # All jobs in the component of the job graph containing self
        jobs = set()
        map(lambda x : x._dfs(jobs), roots)

        # Check for each job for which checkpoint is true that it is a cut vertex or leaf
        for y in filter(lambda x : x.checkpoint, jobs):
            if y not in roots: # The roots are the prexisting jobs
                if len(y._children) != 0 and len(y._followOns) != 0 and len(y._services) != 0:
                    raise JobGraphDeadlockException("New checkpoint job %s is not a leaf in the job graph" % y)

    def defer(self, function, *args, **kwargs):
        """
        Register a deferred function, i.e. a callable that will be invoked after the current
        attempt at running this job concludes. A job attempt is said to conclude when the job
        function (or the :meth:`Job.run` method for class-based jobs) returns, raises an
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
        require( self.fileStore is not None, 'A deferred function may only be registered with a '
                                             'job while that job is running.')
        self.fileStore._registerDeferredFunction(DeferredFunction.create(function, *args, **kwargs))


    ####################################################
    #The following nested classes are used for
    #creating jobtrees (Job.Runner),
    #and defining a service (Job.Service)
    ####################################################

    class Runner(object):
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
            parser = ArgumentParser()
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
            Deprecated by toil.common.Toil.run. Runs the toil workflow using the given options
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

    class Service(JobLikeObject):
        """
        Abstract class used to define the interface to a service.
        """
        __metaclass__ = ABCMeta
        def __init__(self, memory=None, cores=None, disk=None, preemptable=None, unitName=None):
            """
            Memory, core and disk requirements are specified identically to as in \
            :func:`toil.job.Job.__init__`.
            """
            requirements = {'memory': memory, 'cores': cores, 'disk': disk,
                            'preemptable': preemptable}
            super(Job.Service, self).__init__(requirements=requirements, unitName=unitName)
            self._childServices = []
            self._hasParent = False

        @abstractmethod
        def start(self, job):
            """
            Start the service.
            
            :param toil.job.Job job: The underlying job that is being run. Can be used to register
            deferred functions, or to access the fileStore for creating temporary files.

            :returns: An object describing how to access the service. The object must be pickleable \
            and will be used by jobs to access the service (see :func:`toil.job.Job.addService`).
            """
            pass

        @abstractmethod
        def stop(self, job):
            """
            Stops the service. Function can block until complete.
            
            :param toil.job.Job job: The underlying job that is being run. Can be used to register
            deferred functions, or to access the fileStore for creating temporary files.
            """
            pass

        def check(self):
            """
            Checks the service is still running.

            :raise RuntimeError: If the service failed, this will cause the service job to be labeled failed.
            :returns: True if the service is still running, else False. If False then the service job will be terminated,
                and considered a success. Important point: if the service job exits due to a failure, it should raise a
                RuntimeError, not return False!
            """
            pass

        def _addChild(self, service):
            """
            Add a child service to start up after this service has started. This should not be
            called by the user, instead use :func:`toil.job.Job.Service.addService` with the
            ``parentService`` option.

            :raises toil.job.JobException: If service has already been made the child of a job or another service.
            :param toil.job.Job.Service service: Service to add as a "child" of this service
            :return: a promise that will be replaced with the return value from \
            :func:`toil.job.Job.Service.start` of service after the service has started.
            :rtype: toil.job.Promise
            """
            if service._hasParent:
                raise JobException("The service already has a parent service")
            service._parent = True
            jobService = ServiceJob(service)
            self._childServices.append(jobService)
            return jobService.rv()

    ####################################################
    #Private functions
    ####################################################

    def _addPredecessor(self, predecessorJob):
        """
        Adds a predecessor job to the set of predecessor jobs. Raises a \
        RuntimeError if the job is already a predecessor.
        """
        if predecessorJob in self._directPredecessors:
            raise RuntimeError("The given job is already a predecessor of this job")
        self._directPredecessors.add(predecessorJob)

    @classmethod
    def _loadUserModule(cls, userModule):
        """
        Imports and returns the module object represented by the given module descriptor.

        :type userModule: ModuleDescriptor
        """
        return userModule.load()

    @classmethod
    def _loadJob(cls, command, jobStore):
        """
        Unpickles a :class:`toil.job.Job` instance by decoding command.

        The command is a reference to a jobStoreFileID containing the \
        pickle file for the job and a list of modules which must be imported so that \
        the Job can be successfully unpickled. \
        See :func:`toil.job.Job._serialiseFirstJob` and \
        :func:`toil.job.Job._makeJobGraphs` to see precisely how the Job is encoded \
        in the command.

        :param string command: encoding of the job in the job store.
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore: The job store.
        :returns: The job referenced by the command.
        :rtype: toil.job.Job
        """
        commandTokens = command.split()
        assert "_toil" == commandTokens[0]
        userModule = ModuleDescriptor.fromCommand(commandTokens[2:])
        logger.debug('Loading user module %s.', userModule)
        userModule = cls._loadUserModule(userModule)
        pickleFile = commandTokens[1]
        if pickleFile == "firstJob":
            openFileStream = jobStore.readSharedFileStream(pickleFile)
        else:
            openFileStream = jobStore.readFileStream(pickleFile)
        with openFileStream as fileHandle:
            return cls._unpickle(userModule, fileHandle, jobStore.config)


    @classmethod
    def _unpickle(cls, userModule, fileHandle, config):
        """
        Unpickles an object graph from the given file handle while loading symbols \
        referencing the __main__ module from the given userModule instead.

        :param userModule:
        :param fileHandle:
        :returns:
        """
        unpickler = cPickle.Unpickler(fileHandle)

        def filter_main(module_name, class_name):
            if module_name == '__main__':
                logger.debug('Getting %s from user module __main__ (%s).', class_name, userModule)
                return getattr(userModule, class_name)
            else:
                logger.debug('Getting %s from module %s.', class_name, module_name)
                return getattr(importlib.import_module(module_name), class_name)

        unpickler.find_global = filter_main
        runnable = unpickler.load()
        assert isinstance(runnable, JobLikeObject)
        runnable._config = config
        return runnable

    def getUserScript(self):
        return self.userModule

    def _fulfillPromises(self, returnValues, jobStore):
        """
        Sets the values for promises using the return values from this job's run() function.
        """
        for path, promiseFileStoreIDs in self._rvs.iteritems():
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
                        cPickle.dump(promisedValue, fileHandle, cPickle.HIGHEST_PROTOCOL)

    # Functions associated with Job.checkJobGraphAcyclic to establish that the job graph does not
    # contain any cycles of dependencies:

    def _dfs(self, visited):
        """
        Adds the job and all jobs reachable on a directed path from current node to the given set.
        """
        if self not in visited:
            visited.add(self)
            for successor in self._children + self._followOns:
                successor._dfs(visited)

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
        Gets the set of implied edges. See Job.checkJobGraphAcylic
        """
        #Get nodes in job graph
        nodes = set()
        for root in roots:
            root._dfs(nodes)

        ##For each follow-on edge calculate the extra implied edges
        #Adjacency list of implied edges, i.e. map of jobs to lists of jobs
        #connected by an implied edge
        extraEdges = dict(map(lambda n : (n, []), nodes))
        for job in nodes:
            if len(job._followOns) > 0:
                #Get set of jobs connected by a directed path to job, starting
                #with a child edge
                reacheable = set()
                for child in job._children:
                    child._dfs(reacheable)
                #Now add extra edges
                for descendant in reacheable:
                    extraEdges[descendant] += job._followOns[:]
        return extraEdges

    ####################################################
    #The following functions are used to serialise
    #a job graph to the jobStore
    ####################################################

    def _createEmptyJobGraphForJob(self, jobStore, command=None, predecessorNumber=0):
        """
        Create an empty job for the job.
        """
        # set _config to determine user determined default values for resource requirements
        self._config = jobStore.config
        return jobStore.create(JobNode.fromJob(self, command=command,
                                               predecessorNumber=predecessorNumber))

    def _makeJobGraphs(self, jobGraph, jobStore):
        """
        Creates a jobGraph for each job in the job graph, recursively.
        """
        jobsToJobGraphs = {self:jobGraph}
        for successors in (self._followOns, self._children):
            jobs = map(lambda successor:
                successor._makeJobGraphs2(jobStore, jobsToJobGraphs), successors)
            jobGraph.stack.append(jobs)
        return jobsToJobGraphs

    def _makeJobGraphs2(self, jobStore, jobsToJobGraphs):
        #Make the jobGraph for the job, if necessary
        if self not in jobsToJobGraphs:
            jobGraph = self._createEmptyJobGraphForJob(jobStore, predecessorNumber=len(self._directPredecessors))
            jobsToJobGraphs[self] = jobGraph
            #Add followOns/children to be run after the current job.
            for successors in (self._followOns, self._children):
                jobs = map(lambda successor:
                    successor._makeJobGraphs2(jobStore, jobsToJobGraphs), successors)
                jobGraph.stack.append(jobs)
        else:
            jobGraph = jobsToJobGraphs[self]
        #The return is a tuple stored within a job.stack
        #The tuple is jobStoreID, memory, cores, disk,
        #The predecessorID is used to establish which predecessors have been
        #completed before running the given Job - it is just a unique ID
        #per predecessor
        return JobNode.fromJobGraph(jobGraph)

    def getTopologicalOrderingOfJobs(self):
        """
        :returns: a list of jobs such that for all pairs of indices i, j for which i < j, \
        the job at index i can be run before the job at index j.
        :rtype: list
        """
        ordering = []
        visited = set()
        def getRunOrder(job):
            #Do not add the job to the ordering until all its predecessors have been
            #added to the ordering
            for p in job._directPredecessors:
                if p not in visited:
                    return
            if job not in visited:
                visited.add(job)
                ordering.append(job)
                map(getRunOrder, job._children + job._followOns)
        getRunOrder(self)
        return ordering

    def _serialiseJob(self, jobStore, jobsToJobGraphs, rootJobGraph):
        """
        Pickle a job and its jobGraph to disk.
        """
        # Pickle the job so that its run method can be run at a later time.
        # Drop out the children/followOns/predecessors/services - which are
        # all recorded within the jobStore and do not need to be stored within
        # the job
        self._children, self._followOns, self._services = [], [], []
        self._directPredecessors, self._promiseJobStore = set(), None
        # The pickled job is "run" as the command of the job, see worker
        # for the mechanism which unpickles the job and executes the Job.run
        # method.
        with jobStore.writeFileStream(rootJobGraph.jobStoreID) as (fileHandle, fileStoreID):
            cPickle.dump(self, fileHandle, cPickle.HIGHEST_PROTOCOL)
        # Note that getUserScript() may have been overridden. This is intended. If we used
        # self.userModule directly, we'd be getting a reference to job.py if the job was
        # specified as a function (as opposed to a class) since that is where FunctionWrappingJob
        #  is defined. What we really want is the module that was loaded as __main__,
        # and FunctionWrappingJob overrides getUserScript() to give us just that. Only then can
        # filter_main() in _unpickle( ) do its job of resolving any user-defined type or function.
        userScript = self.getUserScript().globalize()
        jobsToJobGraphs[self].command = ' '.join(('_toil', fileStoreID) + userScript.toCommand())
        #Update the status of the jobGraph on disk
        jobStore.update(jobsToJobGraphs[self])

    def _serialiseServices(self, jobStore, jobGraph, rootJobGraph):
        """
        Serialises the services for a job.
        """
        def processService(serviceJob, depth):
            # Extend the depth of the services if necessary
            if depth == len(jobGraph.services):
                jobGraph.services.append([])

            # Recursively call to process child services
            for childServiceJob in serviceJob.service._childServices:
                processService(childServiceJob, depth+1)

            # Make a job wrapper
            serviceJobGraph = serviceJob._createEmptyJobGraphForJob(jobStore, predecessorNumber=1)

            # Create the start and terminate flags
            serviceJobGraph.startJobStoreID = jobStore.getEmptyFileStoreID()
            serviceJobGraph.terminateJobStoreID = jobStore.getEmptyFileStoreID()
            serviceJobGraph.errorJobStoreID = jobStore.getEmptyFileStoreID()
            assert jobStore.fileExists(serviceJobGraph.startJobStoreID)
            assert jobStore.fileExists(serviceJobGraph.terminateJobStoreID)
            assert jobStore.fileExists(serviceJobGraph.errorJobStoreID)

            # Create the service job tuple
            j = ServiceJobNode(jobStoreID=serviceJobGraph.jobStoreID,
                               memory=serviceJobGraph.memory, cores=serviceJobGraph.cores,
                               disk=serviceJobGraph.disk, startJobStoreID=serviceJobGraph.startJobStoreID,
                               terminateJobStoreID=serviceJobGraph.terminateJobStoreID,
                               errorJobStoreID=serviceJobGraph.errorJobStoreID,
                               jobName=serviceJobGraph.jobName, unitName=serviceJobGraph.unitName,
                               command=serviceJobGraph.command,
                               predecessorNumber=serviceJobGraph.predecessorNumber)

            # Add the service job tuple to the list of services to run
            jobGraph.services[depth].append(j)

            # Break the links between the services to stop them being serialised together
            #childServices = serviceJob.service._childServices
            serviceJob.service._childServices = None
            assert serviceJob._services == []
            #service = serviceJob.service
            
            # Pickle the job
            serviceJob.pickledService = cPickle.dumps(serviceJob.service)
            serviceJob.service = None

            # Serialise the service job and job wrapper
            serviceJob._serialiseJob(jobStore, { serviceJob:serviceJobGraph }, rootJobGraph)

            # Restore values
            #serviceJob.service = service
            #serviceJob.service._childServices = childServices

        for serviceJob in self._services:
            processService(serviceJob, 0)

        self._services = []

    def _serialiseJobGraph(self, jobGraph, jobStore, returnValues, firstJob):
        """
        Pickle the graph of jobs in the jobStore. The graph is not fully serialised \
        until the jobGraph itself is written to disk, this is not performed by this \
        function because of the need to coordinate this operation with other updates. \
        """
        #Check if the job graph has created
        #any cycles of dependencies or has multiple roots
        self.checkJobGraphForDeadlocks()

        #Create the jobGraphs for followOns/children
        jobsToJobGraphs = self._makeJobGraphs(jobGraph, jobStore)
        #Get an ordering on the jobs which we use for pickling the jobs in the
        #correct order to ensure the promises are properly established
        ordering = self.getTopologicalOrderingOfJobs()
        assert len(ordering) == len(jobsToJobGraphs)

        # Temporarily set the jobStore locators for the promise call back functions
        for job in ordering:
            job.prepareForPromiseRegistration(jobStore)
            def setForServices(serviceJob):
                serviceJob.prepareForPromiseRegistration(jobStore)
                for childServiceJob in serviceJob.service._childServices:
                    setForServices(childServiceJob)
            for serviceJob in job._services:
                setForServices(serviceJob)

        ordering.reverse()
        assert self == ordering[-1]
        if firstJob:
            #If the first job we serialise all the jobs, including the root job
            for job in ordering:
                # Pickle the services for the job
                job._serialiseServices(jobStore, jobsToJobGraphs[job], jobGraph)
                # Now pickle the job
                job._serialiseJob(jobStore, jobsToJobGraphs, jobGraph)
        else:
            #We store the return values at this point, because if a return value
            #is a promise from another job, we need to register the promise
            #before we serialise the other jobs
            self._fulfillPromises(returnValues, jobStore)
            #Pickle the non-root jobs
            for job in ordering[:-1]:
                # Pickle the services for the job
                job._serialiseServices(jobStore, jobsToJobGraphs[job], jobGraph)
                # Pickle the job itself
                job._serialiseJob(jobStore, jobsToJobGraphs, jobGraph)
            # Pickle any services for the job
            self._serialiseServices(jobStore, jobGraph, jobGraph)

    def _serialiseFirstJob(self, jobStore):
        """
        Serialises the root job. Returns the wrapping job.

        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore:
        """
        # Create first jobGraph
        jobGraph = self._createEmptyJobGraphForJob(jobStore=jobStore, predecessorNumber=0)
        # Write the graph of jobs to disk
        self._serialiseJobGraph(jobGraph, jobStore, None, True)
        jobStore.update(jobGraph)
        # Store the name of the first job in a file in case of restart. Up to this point the
        # root job is not recoverable. FIXME: "root job" or "first job", which one is it?
        jobStore.setRootJob(jobGraph.jobStoreID)
        return jobGraph

    def _serialiseExistingJob(self, jobGraph, jobStore, returnValues):
        """
        Serialise an existing job.
        """
        self._serialiseJobGraph(jobGraph, jobStore, returnValues, False)
        #Drop the completed command, if not dropped already
        jobGraph.command = None
        #Merge any children (follow-ons) created in the initial serialisation
        #with children (follow-ons) created in the subsequent scale-up.
        assert len(jobGraph.stack) >= 4
        combinedChildren = jobGraph.stack[-1] + jobGraph.stack[-3]
        combinedFollowOns = jobGraph.stack[-2] + jobGraph.stack[-4]
        jobGraph.stack = jobGraph.stack[:-4]
        if len(combinedFollowOns) > 0:
            jobGraph.stack.append(combinedFollowOns)
        if len(combinedChildren) > 0:
            jobGraph.stack.append(combinedChildren)

    ####################################################
    #Function which worker calls to ultimately invoke
    #a jobs Job.run method, and then handle created
    #children/followOn jobs
    ####################################################

    def _run(self, jobGraph, fileStore):
        return self.run(fileStore)

    @contextmanager
    def _executor(self, jobGraph, stats, fileStore):
        """
        This is the core wrapping method for running the job within a worker.  It sets up the stats
        and logging before yielding. After completion of the body, the function will finish up the
        stats and logging, and starts the async update process for the job.
        """
        if stats is not None:
            startTime = time.time()
            startClock = getTotalCpuTime()
        baseDir = os.getcwd()

        yield

        # If the job is not a checkpoint job, add the promise files to delete
        # to the list of jobStoreFileIDs to delete
        if not self.checkpoint:
            for jobStoreFileID in Promise.filesToDelete:
                fileStore.deleteGlobalFile(jobStoreFileID)
        else:
            # Else copy them to the job wrapper to delete later
            jobGraph.checkpointFilesToDelete = list(Promise.filesToDelete)
        Promise.filesToDelete.clear()
        # Now indicate the asynchronous update of the job can happen
        fileStore._updateJobWhenDone()
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

    def _runner(self, jobGraph, jobStore, fileStore):
        """
        This method actually runs the job, and serialises the next jobs.

        :param class jobGraph: Instance of a jobGraph object
        :param class jobStore: Instance of the job store
        :param toil.fileStore.FileStore fileStore: Instance of a Cached on uncached
               filestore
        :return:
        """
        # Run the job
        returnValues = self._run(jobGraph, fileStore)
        # Serialize the new jobs defined by the run method to the jobStore
        self._serialiseExistingJob(jobGraph, jobStore, returnValues)

    def _jobName(self):
        """
        :rtype : string, used as identifier of the job class in the stats report.
        """
        return self.__class__.__name__


class JobException( Exception ):
    """
    General job exception.
    """
    def __init__( self, message ):
        super( JobException, self ).__init__( message )


class JobGraphDeadlockException( JobException ):
    """
    An exception raised in the event that a workflow contains an unresolvable \
    dependency, such as a cycle. See :func:`toil.job.Job.checkJobGraphForDeadlocks`.
    """
    def __init__( self, string ):
        super( JobGraphDeadlockException, self ).__init__( string )


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
        argSpec = inspect.getargspec(userFunction)
        if argSpec.defaults is None:
            argDict = {}
        else:
            argDict = dict(zip(argSpec.args[-len(argSpec.defaults):], argSpec.defaults))

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
            if dehumanize and isinstance(value, basestring):
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
    A job function is a function whose first argument is a :class:`job.Job` \
    instance that is the wrapping job for the function. This can be used to \
    add successor jobs for the function and perform all the functions the \
    :class:`job.Job` class provides.

    To enable the job function to get access to the :class:`toil.fileStore.FileStore` \
    instance (see :func:`toil.job.Job.Run`), it is made a variable of the wrapping job \
    called fileStore.
    """
    def run(self, fileStore):
        userFunction = self._getUserFunction()
        self.fileStore = fileStore
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
        super(PromisedRequirementFunctionWrappingJob, self).__init__(userFunction, *args, **kwargs)

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
        self.encapsulatedJob = job
        Job.addChild(self, job)
        # Use small resource requirements for dummy Job instance.
        self.encapsulatedFollowOn = Job(disk='1M', memory='32M', cores=0.1)
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
        super(EncapsulatedJob, self).prepareForPromiseRegistration(jobStore)
        self.encapsulatedJob.prepareForPromiseRegistration(jobStore)

    def getUserScript(self):
        return self.encapsulatedJob.getUserScript()


class ServiceJobNode(JobNode):
    def __init__(self, jobStoreID, memory, cores, disk, startJobStoreID, terminateJobStoreID,
                 errorJobStoreID, unitName, jobName, command, predecessorNumber):
        requirements = dict(memory=memory, cores=cores, disk=disk, preemptable=False)
        super(ServiceJobNode, self).__init__(unitName=unitName, jobName=jobName,
                                             requirements=requirements,
                                             jobStoreID=jobStoreID,
                                             command=command,
                                             predecessorNumber=predecessorNumber)
        self.startJobStoreID = startJobStoreID
        self.terminateJobStoreID = terminateJobStoreID
        self.errorJobStoreID = errorJobStoreID


class ServiceJob(Job):
    """
    Job used to wrap a :class:`toil.job.Job.Service` instance.
    """
    def __init__(self, service):
        """
        This constructor should not be called by a user.

        :param service: The service to wrap in a job.
        :type service: toil.job.Job.Service
        """
        Job.__init__(self, **service._requirements)
        # service.__module__ is the module defining the class service is an instance of.
        self.serviceModule = ModuleDescriptor.forModule(service.__module__).globalize()

        #The service to run - this will be replace before serialization with a pickled version
        self.service = service
        self.pickledService = None
        self.jobName = service.jobName
        # This references the parent job wrapper. It is initialised just before
        # the job is run. It is used to access the start and terminate flags.
        self.jobGraph = None

    def run(self, fileStore):
        
        # we need access to the filestore from underneath the service job
        self.fileStore = fileStore

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

    def _run(self, jobGraph, fileStore):
        # Set the jobGraph for the job
        self.jobGraph = jobGraph
        #Run the job
        returnValues = self.run(fileStore)
        assert jobGraph.stack == []
        assert jobGraph.services == []
        # Unset the jobGraph for the job
        self.jobGraph = None
        # Set the stack to mimic what would be expected for a non-service job (this is a hack)
        jobGraph.stack = [[], []]
        return returnValues

    def getUserScript(self):
        return self.serviceModule


class Promise(object):
    """
    References a return value from a :meth:`toil.job.Job.run` or
    :meth:`toil.job.Job.Service.start` method as a *promise* before the method itself is run.

    Let T be a job. Instances of :class:`Promise` (termed a *promise*) are returned by T.rv(),
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
        :param path: see :meth:`Job.rv`
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
            return super(Promise, cls).__new__(cls, *args)
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
            value = cPickle.load(fileHandle)
            return value


class PromisedRequirement(object):
    def __init__(self, valueOrCallable, *args):
        """
        Class for dynamically allocating job function resource requirements involving
        :class:`toil.job.Promise` instances.

        Use when resource requirements depend on the return value of a parent function.
        PromisedRequirements can be modified by passing a function that takes the
        :class:`Promise` as input.

        For example, let f, g, and h be functions. Then a Toil workflow can be
        defined as follows::
        A = Job.wrapFn(f)
        B = A.addChildFn(g, cores=PromisedRequirement(A.rv())
        C = B.addChildFn(h, cores=PromisedRequirement(lambda x: 2*x, B.rv()))

        :param valueOrCallable: A single Promise instance or a function that
                                takes \*args as input parameters.
        :param int|Promise \*args: variable length argument list
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
        requirements = ["disk", "memory", "cores"]
        foundPromisedRequirement = False
        for r in requirements:
            if isinstance(kwargs.get(r), Promise):
                kwargs[r] = PromisedRequirement(kwargs[r])
                foundPromisedRequirement = True
            elif isinstance(kwargs.get(r), PromisedRequirement):
                foundPromisedRequirement = True
        return foundPromisedRequirement
