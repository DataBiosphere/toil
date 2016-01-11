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
import importlib
from argparse import ArgumentParser
from abc import ABCMeta, abstractmethod
import tempfile
import uuid
import time
import copy_reg
import cPickle
import logging
import shutil
import stat
import inspect
from threading import Thread, Semaphore, Event
from Queue import Queue, Empty
from bd2k.util.expando import Expando
from bd2k.util.humanize import human2bytes
from io import BytesIO
from toil.resource import ModuleDescriptor
from toil.common import loadJobStore

logger = logging.getLogger( __name__ )

from toil.lib.bioio import (setLoggingFromOptions,
                               getTotalCpuTimeAndMemoryUsage, getTotalCpuTime)
from toil.common import setupToil, addOptions
from toil.leader import mainLoop

class Job(object):
    """
    Class represents a unit of work in toil. 
    """
    def __init__(self, memory=None, cores=None, disk=None, cache=None):
        """
        This method must be called by any overiding constructor.
        
        :param memory: the maximum number of bytes of memory the job will \
        require to run.  
        :param cores: the number of CPU cores required.
        :param disk: the amount of local disk space required by the job, \
        expressed in bytes.
        :param cache: the amount of disk (so that cache <= disk), expressed in bytes, \
        for storing files from previous jobs so that they can be accessed from a local copy. 
        
        :type cores: int or string convertable by bd2k.util.humanize.human2bytes to an int
        :type disk: int or string convertable by bd2k.util.humanize.human2bytes to an int
        :type cache: int or string convertable by bd2k.util.humanize.human2bytes to an int
        :type memory: int or string convertable by bd2k.util.humanize.human2bytes to an int
        """
        self.cores = cores
        parse = lambda x : x if x is None else human2bytes(str(x))
        self.memory = parse(memory)
        self.disk = parse(disk)
        self.cache = parse(cache)
        #Private class variables

        #See Job.addChild
        self._children = []
        #See Job.addFollowOn
        self._followOns = []
        #See Job.addService
        self._services = []
        #A follow-on, service or child of a job A, is a "direct successor" of A, if B
        #is a direct successor of A, then A is a "direct predecessor" of B.
        self._directPredecessors = set()
        # Note that self.__module__ is not necessarily this module, i.e. job.py. It is the module
        # defining the class self is an instance of, which may be a subclass of Job that may be
        # defined in a different module.
        self.userModule = ModuleDescriptor.forModule(self.__module__)
        #See Job.rv()
        self._rvs = {}
        self._promiseJobStore = None
        

    def run(self, fileStore):
        """
        Override this function to perform work and dynamically create successor jobs.
        
        :param toil.job.Job.FileStore fileStore: Used to create local and globally \
        sharable temporary files and to send log messages to the leader process.
        
        :return: The return value of the function can be passed to other jobs \
        by means of :func:`toil.job.Job.rv`.
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

    def addService(self, service):
        """
        Add a service. 
        
        The :func:`toil.job.Job.Service.start` method of the service will be called \
        after the run method has completed but before any successors are run. \
        The service's :func:`toil.job.Job.Service.stop` method will be called once \
        the successors of the job have been run. 
        
        Services allow things like databases and servers to be started and accessed \
        by jobs in a workflow.
        
        :param toil.job.Job.Service service: Service to add.
        :return: a promise that will be replaced with the return value from \
        :func:`toil.job.Job.Service.start` of service in any successor of the job.
        :rtype: toil.job.PromisedJobReturnValue 
        """
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
    
    def rv(self, argIndex=None):
        """
        Gets a *promise* (:class:`toil.job.PromisedJobReturnValue`) representing \
        a return value of the job's run function.
        
        :param argIndex: If None the complete return value will be returned, \
        if argIndex is an integer it is used to refer to the return value as indexable \
        (tuple/list/dictionary, or in general an object that implements __getitem__), \
        hence rv(i) would refer to the ith (indexed from 0) member of the return value.
        :type argIndex: int or None
        
        :return: A promise representing the return value of the :func:`toil.job.Job.run` function. 
        
        :rtype: toil.job.PromisedJobReturnValue, referred to as a "promise"
        """
        if argIndex not in self._rvs:
            self._rvs[argIndex] = [] #This will be a list of jobStoreFileIDs for promises which will
            #be added to when the PromisedJobReturnValue instances are serialised in a lazy fashion
        def registerPromiseCallBack():
            #Returns the jobStoreFileID and jobStore string
            if self._promiseJobStore == None:
                raise RuntimeError("Trying to pass a promise from a promising job "
                                   "that is not predecessor of the job receiving the promise")
            jobStoreFileID = self._promiseJobStore.getEmptyFileStoreID()
            self._rvs[argIndex].append(jobStoreFileID)
            return jobStoreFileID, self._promiseJobStore.config.jobStore
        return PromisedJobReturnValue(registerPromiseCallBack)

    ####################################################
    #Cycle/connectivity checking
    ####################################################

    def checkJobGraphForDeadlocks(self):
        """
        :raises toil.job.JobGraphDeadlockException: if the job graph \
        is cyclic or contains multiple roots. 
        
        See :func:`toil.job.Job.checkJobGraphConnected` and \
        :func:`toil.job.Job.checkJobGraphAcyclic` for more info.
        """
        self.checkJobGraphConnected()
        self.checkJobGraphAcylic()

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
                    job._followOns + job._services)
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

    ####################################################
    #The following nested classes are used for
    #creating jobtrees (Job.Runner),
    #managing temporary files (Job.FileStore),
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
            Runs the toil workflow using the given options \
            (see Job.Runner.getDefaultOptions and Job.Runner.addToilOptions) \
            starting with this job. 
            :param toil.job.Job job: root job of the workflow
            :raises: toil.leader.FailedJobsException if at the end of function \
            their remain failed jobs.
            :returns: return value of job's run function
            """
            setLoggingFromOptions(options)
            with setupToil(options, userScript=job.getUserScript()) as (config, batchSystem, jobStore):
                logger.info("Downloading entire JobStore")
                jobCache = {jobWrapper.jobStoreID: jobWrapper
                    for jobWrapper in jobStore.jobs()}
                logger.info("{} jobs downloaded.".format(len(jobCache)))
                if options.restart:
                    #This cleans up any half written jobs after a restart
                    jobStore.clean(job._loadRootJob(jobStore), jobCache=jobCache) 
                    rootJob = job._loadRootJob(jobStore)
                else:
                    #Make a file to store the root jobs return value in
                    jobStoreFileID = jobStore.getEmptyFileStoreID()
                    #Add the root job return value as a promise
                    if None not in job._rvs:
                        job._rvs[None] = [] 
                    job._rvs[None].append(jobStoreFileID)
                    #Write the name of the promise file in a shared file
                    with jobStore.writeSharedFileStream("rootJobReturnValue") as fH:
                        fH.write(jobStoreFileID)
                    #Setup the first wrapper.
                    rootJob = job._serialiseFirstJob(jobStore)
                    #Make sure it's cached
                    jobCache[rootJob.jobStoreID] = rootJob
                return mainLoop(config, batchSystem, jobStore, rootJob, jobCache=jobCache)

    class FileStore( object ):
        """
        Class used to manage temporary files, read and write files from the job store\
        and log messages, passed as argument to the :func:`toil.job.Job.run` method.
        """
        #Variables used for synching reads/writes
        _lockFilesLock = Semaphore()
        _lockFiles = set()
        #For files in jobStore that are on the local disk, 
        #map of jobStoreFileIDs to locations in localTempDir.
        _jobStoreFileIDToCacheLocation = {}
        _terminateEvent = Event() #Used to signify crashes in threads
        
        def __init__(self, jobStore, jobWrapper, localTempDir, inputBlockFn):
            """
            This constructor should not be called by the user, \
            FileStore instances are only provided as arguments to the run function.
            
            :param toil.jobStores.abstractJobStore.JobStore jobStore: The job store \
            for the workflow.
            :param toil.jobWrapper.JobWrapper jobWrapper: The jobWrapper for the job.
            :param string localTempDir: A temporary directory in which local temporary \
            files will be placed.
            :param method inputBlockFn: A function which blocks and which is called before \
            the fileStore completes atomically updating the jobs files in the job store.
            """
            self.jobStore = jobStore
            self.jobWrapper = jobWrapper
            self.localTempDir = os.path.abspath(localTempDir)
            self.loggingMessages = []
            self.filesToDelete = set()
            self.jobsToDelete = set()
            #Asynchronous writes stuff
            self.workerNumber = 2
            self.queue = Queue()
            self.updateSemaphore = Semaphore() 
            #Function to write files asynchronously to job store
            def asyncWrite():
                try:
                    while True:
                        try:
                            #Block for up to two seconds waiting for a file
                            args = self.queue.get(timeout=2)
                        except Empty:
                            #Check if termination event is signaled 
                            #(set in the event of an exception in the worker)
                            if self._terminateEvent.isSet():
                                raise RuntimeError("The termination flag is set, exiting")
                            continue
                        #Normal termination condition is getting None from queue
                        if args == None:
                            break
                        inputFileHandle, jobStoreFileID = args
                        #We pass in a fileHandle, rather than the file-name, in case 
                        #the file itself is deleted. The fileHandle itself should persist 
                        #while we maintain the open file handle
                        with jobStore.updateFileStream(jobStoreFileID) as outputFileHandle:
                            bufferSize=1000000 #TODO: This buffer number probably needs to be modified/tuned
                            while 1:
                                copyBuffer = inputFileHandle.read(bufferSize)
                                if not copyBuffer:
                                    break
                                outputFileHandle.write(copyBuffer)
                        inputFileHandle.close()
                        #Remove the file from the lock files
                        with self._lockFilesLock:
                            self._lockFiles.remove(jobStoreFileID)
                except:
                    self._terminateEvent.set()
                    raise
                    
            self.workers = map(lambda i : Thread(target=asyncWrite), 
                               range(self.workerNumber))
            for worker in self.workers:
                worker.start()
            self.inputBlockFn = inputBlockFn

        def getLocalTempDir(self):
            """
            Get a new local temporary directory in which to write files that persist \
            for the duration of the job.
            
            :return: The absolute path to a new local temporary directory. \
            This directory will exist for the duration of the job only, and is \
            guaranteed to be deleted once the job terminates, removing all files \
            it contains recursively. 
            :rtype: string
            """
            return os.path.abspath(tempfile.mkdtemp(prefix="t", dir=self.localTempDir))
        
        def getLocalTempFile(self):
            """
            Get a new local temporary file that will persist for the duration of the job.
            
            :return: The absolute path to a local temporary file. \
            This file will exist for the duration of the job only, and \
            is guaranteed to be deleted once the job terminates.
            :rtype: string
            """
            handle, tmpFile = tempfile.mkstemp(prefix="tmp",
                                               suffix=".tmp", dir=self.localTempDir)
            os.close(handle)
            return os.path.abspath(tmpFile)

        def writeGlobalFile(self, localFileName, cleanup=False):
            """
            Takes a file (as a path) and uploads it to the job store. 
            
            If the local file is a file returned by :func:`toil.job.Job.FileStore.getLocalTempFile` \
            or is in a directory, or, recursively, a subdirectory, returned by \
            :func:`toil.job.Job.FileStore.getLocalTempDir` then the write is asynchronous, \
            so further modifications during execution to the file pointed by \
            localFileName will result in undetermined behavior. Otherwise, the \
            method will block until the file is written to the file store. 
            
            :param string localFileName: The path to the local file to upload.
            
            :param Boolean cleanup: if True then the copy of the global file will \
            be deleted once the job and all its successors have completed running. \
            If not the global file must be deleted manually.
            
            :returns: an ID that can be used to retrieve the file. 
            """
            #Put the file into the cache if it is a path within localTempDir
            absLocalFileName = os.path.abspath(localFileName)
            cleanupID = None if not cleanup else self.jobWrapper.jobStoreID
            if absLocalFileName.startswith(self.localTempDir):
                jobStoreFileID = self.jobStore.getEmptyFileStoreID(cleanupID)
                fileHandle = open(absLocalFileName, 'r')
                if os.stat(absLocalFileName).st_uid == os.getuid():
                    #Chmod if permitted to make file read only to try to prevent accidental user modification
                    os.chmod(absLocalFileName, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                with self._lockFilesLock:
                    self._lockFiles.add(jobStoreFileID)
                # A file handle added to the queue allows the asyncWrite threads to remove their jobID from _lockFiles.
                # Therefore, a file should only be added after its fileID is added to _lockFiles
                self.queue.put((fileHandle, jobStoreFileID))
                self._jobStoreFileIDToCacheLocation[jobStoreFileID] = absLocalFileName
            else:
                #Write the file directly to the file store
                jobStoreFileID = self.jobStore.writeFile(localFileName, cleanupID)
            return jobStoreFileID
        
        def writeGlobalFileStream(self, cleanup=False):
            """
            Similar to writeGlobalFile, but allows the writing of a stream to the job store.
            
            :param Boolean cleanup: is as in :func:`toil.job.Job.FileStore.writeGlobalFile`.
            
            :returns: a context manager yielding a tuple of 1) a file handle which \
            can be written to and 2) the ID of the resulting file in the job store. \
            The yielded file handle does not need to and should not be closed explicitly.
            """
            #TODO: Make this work with the caching??
            return self.jobStore.writeFileStream(None if not cleanup else self.jobWrapper.jobStoreID)
        
        def readGlobalFile(self, fileStoreID, userPath=None, cache=True):
            """
            Get a copy of a file in the job store. 
            
            :param string userPath: a path to the name of file to which the global \
            file will be copied or hard-linked (see below).
            
            :param boolean cache: If True will use caching (see below). Caching will \
            attempt to keep copies of files between sequences of jobs run on the same \
            worker. 
            
            If cache=True and userPath is either: (1) a file path contained within \
            a directory or, recursively, a subdirectory of a temporary directory \
            returned by Job.FileStore.getLocalTempDir(), or (2) a file path returned by \ 
            Job.FileStore.getLocalTempFile() then the file will be cached and returned file \
            will be read only (have permissions 444).
            
            If userPath is specified and the file is already cached, the userPath file \
            will be a hard link to the actual location, else it will be an actual copy \
            of the file. 
            
            If the cache=False or userPath is not either of the above the file will not \
            be cached and will have default permissions. Note, if the file is already cached \
            this will result in two copies of the file on the system.
            
            :return: an absolute path to a local, temporary copy of the file keyed \
            by fileStoreID.

            :rtype : string
            """
            if fileStoreID in self.filesToDelete:
                raise RuntimeError("Trying to access a file in the jobStore you've deleted: %s" % fileStoreID)
            if userPath != None:
                userPath = os.path.abspath(userPath) #Make an absolute path
                #Turn off caching if user file is not in localTempDir
                if cache and not userPath.startswith(self.localTempDir):
                    cache = False
            #When requesting a new file from the jobStore first check if fileStoreID
            #is a key in _jobStoreFileIDToCacheLocation.
            if fileStoreID in self._jobStoreFileIDToCacheLocation:
                cachedAbsFilePath = self._jobStoreFileIDToCacheLocation[fileStoreID]   
                if cache:
                    #If the user specifies a location and it is not the current location
                    #return a hardlink to the location, else return the original location
                    if userPath == None or userPath == cachedAbsFilePath:
                        return cachedAbsFilePath
                    #Chmod to make file read only
                    if os.path.exists(userPath):
                        os.remove(userPath)
                    os.link(cachedAbsFilePath, userPath)
                    return userPath
                else:
                    #If caching is not true then make a copy of the file
                    localFilePath = userPath if userPath != None else self.getLocalTempFile()
                    shutil.copyfile(cachedAbsFilePath, localFilePath)
                    return localFilePath
            else:
                #If it is not in the cache read it from the jobStore to the 
                #desired location
                localFilePath = userPath if userPath != None else self.getLocalTempFile()
                self.jobStore.readFile(fileStoreID, localFilePath)
                #If caching is enabled and the file is in local temp dir then
                #add to cache and make read only
                if cache:
                    assert localFilePath.startswith(self.localTempDir)
                    self._jobStoreFileIDToCacheLocation[fileStoreID] = localFilePath
                    os.chmod(localFilePath, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                return localFilePath

        def readGlobalFileStream(self, fileStoreID):
            """
            Similar to readGlobalFile, but allows a stream to be read from the job \
            store.
            
            :returns: a context manager yielding a file handle which can be read from. \
            The yielded file handle does not need to and should not be closed explicitly.
            """
            if fileStoreID in self.filesToDelete:
                raise RuntimeError("Trying to access a file in the jobStore you've deleted: %s" % fileStoreID)
            
            #If fileStoreID is in the cache provide a handle from the local cache
            if fileStoreID in self._jobStoreFileIDToCacheLocation:
                #This leaks file handles (but the commented out code does not work properly)
                return open(self._jobStoreFileIDToCacheLocation[fileStoreID], 'r') 
                #with open(self._jobStoreFileIDToCacheLocation[fileStoreID], 'r') as fH:
                #        yield fH
            else:
                #TODO: Progressively add the file to the cache
                return self.jobStore.readFileStream(fileStoreID)
                #with self.jobStore.readFileStream(fileStoreID) as fH:
                #    yield fH

        def deleteGlobalFile(self, fileStoreID):
            """
            Deletes a global file with the given job store ID. 
            
            To ensure that the job can be restarted if necessary, the delete \
            will not happen until after the job's run method has completed.
            
            :param fileStoreID: the job store ID of the file to be deleted.
            """
            self.filesToDelete.add(fileStoreID)
            #If the fileStoreID is in the cache:
            if fileStoreID in self._jobStoreFileIDToCacheLocation:
                #This will result in the files removal from the cache at the end of the current job
                self._jobStoreFileIDToCacheLocation.pop(fileStoreID)

        def logToMaster(self, text, level=logging.INFO):
            """
            Send a logging message to the leader. The message will also be \
            logged by the worker at the same level.
            
            :param string: The string to log.
            :param int level: The logging level.
            """
            logger.log(level=level, msg=("LOG-TO-MASTER: " + text))
            self.loggingMessages.append(dict(text=text, level=level))

        def _updateJobWhenDone(self):
            """
            Asynchronously update the status of the job on the disk, first waiting \
            until the writing threads have finished and the input blockFn has stopped \
            blocking.
            """
            def asyncUpdate():
                try:
                    #Wait till all file writes have completed
                    for i in xrange(len(self.workers)):
                        self.queue.put(None)
            
                    for thread in self.workers:
                        thread.join()
                    
                    #Wait till input block-fn returns - in the event of an exception
                    #this will eventually terminate 
                    self.inputBlockFn()
                    
                    #Check the terminate event, if set we can not guarantee
                    #that the workers ended correctly, therefore we exit without
                    #completing the update
                    if self._terminateEvent.isSet():
                        raise RuntimeError("The termination flag is set, exiting before update")
                    
                    #Indicate any files that should be deleted once the update of 
                    #the job wrapper is completed.
                    self.jobWrapper.filesToDelete = list(self.filesToDelete)
                    
                    #Complete the job
                    self.jobStore.update(self.jobWrapper)
                    
                    #Delete any remnant jobs
                    map(self.jobStore.delete, self.jobsToDelete)
                    
                    #Delete any remnant files
                    map(self.jobStore.deleteFile, self.filesToDelete)
                    
                    #Remove the files to delete list, having successfully removed the files
                    if len(self.filesToDelete) > 0:
                        self.jobWrapper.filesToDelete = []
                        #Update, removing emptying files to delete
                        self.jobStore.update(self.jobWrapper)
                except:
                    self._terminateEvent.set()
                    raise
                finally:
                    #Indicate that _blockFn can return
                    #This code will always run
                    self.updateSemaphore.release()
            #The update semaphore is held while the jobWrapper is written to disk
            try:
                self.updateSemaphore.acquire()
                t = Thread(target=asyncUpdate)
                t.start()
            except: #This is to ensure that the semaphore is released in a crash to stop a deadlock scenario
                self.updateSemaphore.release()
                raise
            
        def _cleanLocalTempDir(self, cacheSize):
            """
            At the end of the job, remove all localTempDir files except those whose \
            value is in _jobStoreFileIDToCacheLocation.
            
            :param int cacheSize: the total number of bytes of files allowed in the cache.
            """
            #Remove files so that the total cached files are smaller than a cacheSize
            
            #List of pairs of (fileCreateTime, fileStoreID) for cached files
            with self._lockFilesLock:
                deletableCacheFiles = set(self._jobStoreFileIDToCacheLocation.keys()) - self._lockFiles
            cachedFileCreateTimes = map(lambda x : (os.stat(self._jobStoreFileIDToCacheLocation[x]).st_ctime, x),
                                        deletableCacheFiles)
            #Total number of bytes stored in cached files
            totalCachedFileSizes = sum([os.stat(self._jobStoreFileIDToCacheLocation[x]).st_size for x in
                                        self._jobStoreFileIDToCacheLocation.keys()])
            #Remove earliest created files first - this is in place of 'Remove smallest files first'.  Again, might
            #not be the best strategy.
            cachedFileCreateTimes.sort()
            cachedFileCreateTimes.reverse()
            #Now do the actual file removal
            while totalCachedFileSizes > cacheSize and len(cachedFileCreateTimes) > 0:
                fileCreateTime, fileStoreID = cachedFileCreateTimes.pop()
                fileSize = os.stat(self._jobStoreFileIDToCacheLocation[fileStoreID]).st_size
                filePath = self._jobStoreFileIDToCacheLocation[fileStoreID]
                self._jobStoreFileIDToCacheLocation.pop(fileStoreID)
                os.remove(filePath)
                totalCachedFileSizes -= fileSize
                assert totalCachedFileSizes >= 0
            
            #Iterate from the base of localTempDir and remove all 
            #files/empty directories, recursively
            cachedFiles = set(self._jobStoreFileIDToCacheLocation.values())
            
            def clean(dirOrFile, remove=True):
                canRemove = True 
                if os.path.isdir(dirOrFile):
                    for f in os.listdir(dirOrFile):
                        canRemove = canRemove and clean(os.path.join(dirOrFile, f))
                    if canRemove and remove:
                        os.rmdir(dirOrFile) #Dir should be empty if canRemove is true
                    return canRemove
                if dirOrFile in cachedFiles:
                    return False
                os.remove(dirOrFile)
                return True    
            clean(self.localTempDir, False)
        
        def _blockFn(self):
            """
            Blocks while _updateJobWhenDone is running.
            """ 
            self.updateSemaphore.acquire()
            self.updateSemaphore.release() #Release so that the block function can be recalled
            #This works, because once acquired the semaphore will not be acquired
            #by _updateJobWhenDone again.
            return
        
        def __del__(self): 
            """Cleanup function that is run when destroying the class instance \
            that ensures that all the file writing threads exit.
            """
            self.updateSemaphore.acquire()
            for i in xrange(len(self.workers)):
                self.queue.put(None)
            for thread in self.workers:
                thread.join()
            self.updateSemaphore.release()

    class Service:
        """
        Abstract class used to define the interface to a service.
        """
        __metaclass__ = ABCMeta
        def __init__(self, memory=None, cores=None, disk=None):
            """
            Memory, core and disk requirements are specified identically to as in \
            :func:`toil.job.Job.__init__`.
            """
            self.memory = memory
            self.cores = cores
            self.disk = disk

        @abstractmethod
        def start(self):
            """
            Start the service.
            
            :returns: An object describing how to access the service. Must be \
            pickleable. Will be used by jobs to access the service (see \
            :func:`toil.job.Job.addService`).
            """
            pass

        @abstractmethod
        def stop(self):
            """
            Stops the service. 
            
            Function can block until complete. 
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
        if predecessorJob in self._directPredecessors:
            raise RuntimeError("The given job is already a predecessor of this job")
        self._directPredecessors.add(predecessorJob)

    @staticmethod
    def _loadRootJob(jobStore):
        """
        Loads the root job in the job store.
        
        :raises toil.job.JobException: If root job is not in the job store. 
        :return: The root job.
        :rtype: toil.job.Job
        """
        with jobStore.readSharedFileStream("rootJobStoreID") as f: #Load the root job
            rootJobID = f.read()
        if not jobStore.exists(rootJobID):
            raise JobException("No root job (%s) left in toil workflow (workflow "
                               "has finished successfully or not been started?)" % rootJobID)
        return jobStore.load(rootJobID)

    @classmethod
    def _loadUserModule(cls, userModule):
        """
        Imports and returns the module object represented by the given module descriptor.

        :type userModule: ModuleDescriptor
        """
        if not userModule.belongsToToil:
            userModule = userModule.localize()
        if userModule.dirPath not in sys.path:
            sys.path.append(userModule.dirPath)
        try:
            return importlib.import_module(userModule.name)
        except ImportError:
            logger.error('Failed to import user module %r from sys.path=%r', userModule, sys.path)
            raise

    @classmethod
    def _loadJob(cls, command, jobStore):
        """
        Unpickles a :class:`toil.job.Job` instance by decoding command. 
        
        The command is a reference to a jobStoreFileID containing the \
        pickle file for the job and a list of modules which must be imported so that \
        the Job can be successfully unpickled. \
        See :func:`toil.job.Job._serialiseFirstJob` and \
        :func:`toil.job.Job._makeJobWrappers` to see precisely how the Job is encoded \
        in the command. 
        
        :param string command: encoding of the job in the job store.
        :param toil.jobStores.abstractJobStore.AbstractJobStore jobStore: The job store.
        :returns: The job referenced by the command.
        :rtype: toil.job.Job
        """
        commandTokens = command.split()
        assert "_toil" == commandTokens[0]
        userModule = ModuleDescriptor(*(commandTokens[2:]))
        userModule = cls._loadUserModule(userModule)
        pickleFile = commandTokens[1]
        if pickleFile == "firstJob":
            openFileStream = jobStore.readSharedFileStream(pickleFile)
        else:
            openFileStream = jobStore.readFileStream(pickleFile)
        with openFileStream as fileHandle:
            return cls._unpickle(userModule, fileHandle)

    @classmethod
    def _unpickle(cls, userModule, fileHandle):
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
                return getattr(userModule, class_name)
            else:
                return getattr(importlib.import_module(module_name), class_name)

        unpickler.find_global = filter_main
        return unpickler.load()
    
    def getUserScript(self):
        return self.userModule

    ####################################################
    #Functions to pass Job.run return values to the
    #input arguments of other Job instances
    ####################################################

    def _setReturnValuesForPromises(self, returnValues, jobStore):
        """
        Sets the values for promises using the return values from the job's run function.
        """
        for i in self._rvs.keys():
            if i == None:
                argToStore = returnValues
            else:
                argToStore = returnValues[i]
            for promiseFileStoreID in self._rvs[i]:
                with jobStore.updateFileStream(promiseFileStoreID) as fileHandle:
                    cPickle.dump(argToStore, fileHandle, cPickle.HIGHEST_PROTOCOL)

    ####################################################
    #Functions associated with Job.checkJobGraphAcyclic to establish
    #that the job graph does not contain any cycles of dependencies.
    ####################################################

    def _dfs(self, visited):
        """Adds the job and all jobs reachable on a directed path from current \
        node to the set 'visited'.
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

    def _modifyJobGraphForServices(self, jobStore, jobStoreID):
        """
        Modifies the job graph just before it is serialised to disk \
        to correctly schedule any services defined for the job.
        """
        if len(self._services) > 0:
            #Set the start/stop jobStore fileIDs for each service
            for service in self._services:
                service.startFileStoreID = jobStore.getEmptyFileStoreID(jobStoreID)
                assert jobStore.fileExists(service.startFileStoreID)
                service.stopFileStoreID = jobStore.getEmptyFileStoreID(jobStoreID)
                assert jobStore.fileExists(service.stopFileStoreID)

            def removePredecessor(job):
                assert self in job._directPredecessors
                job._directPredecessors.remove(self)

            #t1 and t2 are used to run the children and followOns of the job
            #after the services of the job are started
            startFileStoreIDs = map(lambda i : i.startFileStoreID, self._services)
            t1, t2 = Job.wrapJobFn(blockUntilDeleted, startFileStoreIDs), Job()
            #t1 runs the children of the job
            for child in self._children:
                removePredecessor(child)
                t1.addChild(child)
            self._children = []
            #t2 runs the followOns of the job
            for followOn in self._followOns:
                removePredecessor(followOn)
                t2.addChild(followOn)
            self._followOns = []
            #Now make the services children of the job
            for service in self._services:
                self.addChild(service)
                assert service._directPredecessors == set((self,))
            #Wire up the self, t1 and t2
            self.addChild(t1)
            t1.addFollowOn(t2)
            #The final task once t1 and t2 have finished is to stop the services
            #this is achieved by deleting the stopFileStoreIDs.
            t2.addFollowOnJobFn(deleteFileStoreIDs, map(lambda i : i.stopFileStoreID, self._services))
            self._services = [] #Defensive

    def _createEmptyJobForJob(self, jobStore, command=None, predecessorNumber=0):
        """
        Create an empty job for the job.
        """
        requirements = self.effectiveRequirements(jobStore.config)
        del requirements.cache
        return jobStore.create(command=command, predecessorNumber=predecessorNumber, **requirements)

    def effectiveRequirements(self, config):
        """
        Determine and validate the effective requirements for this job, substituting a missing
        explict requirement with a default from the configuration.

        :rtype: Expando
        :return: a dictionary/object hybrid with one entry/attribute for each requirement
        """
        requirements = Expando(
            memory=float(config.defaultMemory) if self.memory is None else self.memory,
            cores=float(config.defaultCores) if self.cores is None else self.cores,
            disk=float(config.defaultDisk) if self.disk is None else self.disk)
        if self.cache is None:
            requirements.cache = min(requirements.disk, float(config.defaultCache))
        else:
            requirements.cache = self.cache
        if requirements.cache > requirements.disk:
            raise RuntimeError("Trying to allocate a cache ({cache}) larger than the disk "
                               "requirement for the job ({disk})".format(**requirements))
        return requirements

    def _makeJobWrappers(self, jobWrapper, jobStore):
        """
        Creates a job for each job in the job graph, recursively.
        """
        jobsToJobWrappers = { self:jobWrapper }
        for successors in (self._followOns, self._children):
            jobs = map(lambda successor:
                successor._makeJobWrappers2(jobStore, jobsToJobWrappers), successors)
            jobWrapper.stack.append(jobs)
        return jobsToJobWrappers

    def _makeJobWrappers2(self, jobStore, jobsToJobWrappers):
        #Make the jobWrapper for the job, if necessary
        if self not in jobsToJobWrappers:
            jobWrapper = self._createEmptyJobForJob(jobStore, predecessorNumber=len(self._directPredecessors))
            jobsToJobWrappers[self] = jobWrapper
            #Add followOns/children to be run after the current job.
            for successors in (self._followOns, self._children):
                jobs = map(lambda successor:
                    successor._makeJobWrappers2(jobStore, jobsToJobWrappers), successors)
                jobWrapper.stack.append(jobs)
        else:
            jobWrapper = jobsToJobWrappers[self]
        #The return is a tuple stored within a job.stack 
        #The tuple is jobStoreID, memory, cores, disk, predecessorID
        #The predecessorID is used to establish which predecessors have been
        #completed before running the given Job - it is just a unique ID
        #per predecessor
        return (jobWrapper.jobStoreID, jobWrapper.memory, jobWrapper.cores, jobWrapper.disk,
                None if jobWrapper.predecessorNumber <= 1 else str(uuid.uuid4()))
        
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
    
    def _serialiseJob(self, jobStore, jobsToJobWrappers, rootJobWrapper):
        """
        Pickle a job and its jobWrapper to disk.
        """
        #Pickle the job so that its run method can be run at a later time.
        #Drop out the children/followOns/predecessors/services - which are
        #all recorded within the jobStore and do not need to be stored within
        #the job
        self._children = []
        self._followOns = []
        self._services = []
        self._directPredecessors = set()
        #The pickled job is "run" as the command of the job, see worker
        #for the mechanism which unpickles the job and executes the Job.run
        #method.
        with jobStore.writeFileStream(rootJobWrapper.jobStoreID) as (fileHandle, fileStoreID):
            cPickle.dump(self, fileHandle, cPickle.HIGHEST_PROTOCOL)
        # Note that getUserScript() may have beeen overridden. This is intended. If we used
        # self.userModule directly, we'd be getting a reference to job.py if the job was
        # specified as a function (as opposed to a class) since that is where FunctionWrappingJob
        #  is defined. What we really want is the module that was loaded as __main__,
        # and FunctionWrappingJob overrides getUserScript() to give us just that. Only then can
        # filter_main() in _unpickle( ) do its job of resolveing any user-defined type or function.
        userScript = self.getUserScript().globalize()
        jobsToJobWrappers[self].command = ' '.join( ('_toil', fileStoreID) + userScript)
        #Update the status of the jobWrapper on disk
        jobStore.update(jobsToJobWrappers[self])
    
    def _serialiseJobGraph(self, jobWrapper, jobStore, returnValues, firstJob):  
        """
        Pickle the graph of jobs in the jobStore. The graph is not fully serialised \
        until the jobWrapper itself is written to disk, this is not performed by this \
        function because of the need to coordinate this operation with other updates. \
        """
        #Modify job graph to run any services correctly
        self._modifyJobGraphForServices(jobStore, jobWrapper.jobStoreID)
        #Check if the job graph has created
        #any cycles of dependencies or has multiple roots
        self.checkJobGraphForDeadlocks()
        #Create the jobWrappers for followOns/children
        jobsToJobWrappers = self._makeJobWrappers(jobWrapper, jobStore)
        #Get an ordering on the jobs which we use for pickling the jobs in the 
        #correct order to ensure the promises are properly established
        ordering = self.getTopologicalOrderingOfJobs()
        assert len(ordering) == len(jobsToJobWrappers)
        #Temporarily set the jobStore strings for the promise call back functions
        for job in ordering:
            job._promiseJobStore = jobStore 
        ordering.reverse()
        assert self == ordering[-1]
        if firstJob:
            #If the first job we serialise all the jobs, including the root job
            for job in ordering:
                job._promiseJobStore = None
                job._serialiseJob(jobStore, jobsToJobWrappers, jobWrapper)
        else:
            #We store the return values at this point, because if a return value
            #is a promise from another job, we need to register the promise
            #before we serialise the other jobs
            self._setReturnValuesForPromises(returnValues, jobStore)
            #Pickle the non-root jobs
            for job in ordering[:-1]:
                job._promiseJobStore = None
                job._serialiseJob(jobStore, jobsToJobWrappers, jobWrapper)
            #Drop the completed command
            assert jobWrapper.command != None
            jobWrapper.command = None
            #Merge any children (follow-ons) created in the initial serialisation
            #with children (follow-ons) created in the subsequent scale-up.
            assert len(jobWrapper.stack) >= 4
            combinedChildren = jobWrapper.stack[-1] + jobWrapper.stack[-3]
            combinedFollowOns = jobWrapper.stack[-2] + jobWrapper.stack[-4]
            jobWrapper.stack = jobWrapper.stack[:-4]
            if len(combinedFollowOns) > 0:
                jobWrapper.stack.append(combinedFollowOns)
            if len(combinedChildren) > 0:
                jobWrapper.stack.append(combinedChildren)
            
    def _serialiseFirstJob(self, jobStore):
        """
        Serialises the root job. Returns the wrapping job.
        """
        #Create first jobWrapper
        jobWrapper = self._createEmptyJobForJob(jobStore, None,
                                                predecessorNumber=0)
        #Write the graph of jobs to disk
        self._serialiseJobGraph(jobWrapper, jobStore, None, True)
        jobStore.update(jobWrapper) 
        #Store the name of the first job in a file in case of restart
        #Up to this point the root-job is not recoverable
        with jobStore.writeSharedFileStream("rootJobStoreID") as f:
            f.write(jobWrapper.jobStoreID)
        #Return the first job wrapper
        return jobWrapper

    ####################################################
    #Function which worker calls to ultimately invoke
    #a jobs Job.run method, and then handle created
    #children/followOn jobs
    ####################################################

    def _execute(self, jobWrapper, stats, localTempDir, jobStore, fileStore):
        """
        This is the core method for running the job within a worker.
        """
        if stats != None:
            startTime = time.time()
            startClock = getTotalCpuTime()
        baseDir = os.getcwd()
        #Run the job
        returnValues = self.run(fileStore)
        #Serialize the new jobs defined by the run method to the jobStore
        self._serialiseJobGraph(jobWrapper, jobStore, returnValues, False)
        #Add the promise files to delete to the list of jobStoreFileIDs to delete
        for jobStoreFileID in promiseFilesToDelete:
            fileStore.deleteGlobalFile(jobStoreFileID)
        promiseFilesToDelete.clear() 
        #Now indicate the asynchronous update of the job can happen
            
        fileStore._updateJobWhenDone()
        #Change dir back to cwd dir, if changed by job (this is a safety issue)
        if os.getcwd() != baseDir:
            os.chdir(baseDir)
        #Finish up the stats
        if stats != None:
            totalCpuTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            stats.jobs.append(
                Expando(
                    time=str(time.time() - startTime),
                    clock=str(totalCpuTime - startClock),
                    class_name=self._jobName(),
                    memory=str(totalMemoryUsage)
                )
            )

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
    Job used to wrap a function. In its run method the wrapped function is called.
    """
    def __init__(self, userFunction, *args, **kwargs):
        """
        :param userFunction: The function to wrap. The userFunction will be called \
        with the ``*args`` and ``**kwargs`` as arguments.
        
        The keywords "memory", "cores", "disk", "cache" are reserved keyword arguments \
        that if specified will be used to determine the resources for the job, \
        as :func:`toil.job.Job.__init__`. If they are keyword arguments to the function 
        they will be extracted from the function definition, but may be overridden by 
        the user (as you would expect).
        """
        # Use the user specified resource argument, if specified, else 
        # grab the default argument from the function, if specified, else default to None
        argSpec = inspect.getargspec(userFunction)
        argDict = dict(zip(argSpec.args[-len(argSpec.defaults):],argSpec.defaults)) \
                        if argSpec.defaults != None else {}
        argFn = lambda x : kwargs.pop(x) if x in kwargs else \
                            (human2bytes(str(argDict[x])) if x in argDict.keys() else None)
        Job.__init__(self, memory=argFn("memory"), cores=argFn("cores"), 
                     disk=argFn("disk"), cache=argFn("cache"))
        #If dill is installed pickle the user function directly
        #TODO: Add dill support
        #else use indirect method
        self.userFunctionModule = ModuleDescriptor.forModule(userFunction.__module__).globalize()
        self.userFunctionName = str(userFunction.__name__)
        self._args=args
        self._kwargs=kwargs

    def _getUserFunction(self):
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
    
    To enable the job function to get access to the :class:`toil.job.Job.FileStore` \
    instance (see :func:`toil.job.Job.Run`), it is made a variable of the wrapping job \
    called fileStore.
    """
    def run(self, fileStore):
        userFunction = self._getUserFunction()
        self.fileStore = fileStore
        rValue = userFunction(*((self,) + tuple(self._args)), **self._kwargs)
        return rValue

class EncapsulatedJob(Job):
    """
    A convenience Job class used to make a job subgraph appear to be a single job. 
    
    Let A be the root job of a job subgraph and B be another job we'd like to run after A
    and all its successors have completed, for this use encapsulate::

        A, B = A(), B() #Job A and subgraph, Job B
        A' = A.encapsulate()
        A'.addChild(B) #B will run after A and all its successors have 
        # completed, A and its subgraph of successors in effect appear 
        # to be just one job.
    
    The return value of an encapsulatd job (as accessed by the :func:`toil.job.Job.rv` function) \
    is the return value of the root job, e.g. A().encapsulate().rv() and A().rv() \
    will resolve to the same value after A or A.encapsulate() has been run.
    """
    def __init__(self, job):
        """
        :param toil.job.Job job: the job to encapsulate.
        """
        Job.__init__(self)
        self.encapsulatedJob = job
        Job.addChild(self, job)
        self.encapsulatedFollowOn = Job()
        Job.addFollowOn(self, self.encapsulatedFollowOn)

    def addChild(self, childJob):
        return Job.addChild(self.encapsulatedFollowOn, childJob)

    def addService(self, service):
        return Job.addService(self.encapsulatedFollowOn, service)

    def addFollowOn(self, followOnJob):
        return Job.addFollowOn(self.encapsulatedFollowOn, followOnJob)

    def rv(self, argIndex=None):
        return self.encapsulatedJob.rv(argIndex)

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
        Job.__init__(self, memory=service.memory, cores=service.cores, disk=service.disk)
        # service.__module__ is the module defining the class service is an instance of.
        self.serviceModule = ModuleDescriptor.forModule(service.__module__).globalize()
        #The service to run, pickled
        self.pickledService = cPickle.dumps(service)
        #An empty file in the jobStore which when deleted is used to signal
        #that the service should cease, is initialised in
        #Job._modifyJobGraphForServices
        self.stopFileStoreID = None
        #Similarly a empty file which when deleted is used to signal that the
        #service is established
        self.startFileStoreID = None

    def run(self, fileStore):
        #Unpickle the service
        userModule = self._loadUserModule(self.serviceModule)
        service = self._unpickle( userModule, BytesIO( self.pickledService ) )
        #Start the service
        startCredentials = service.start()
        #The start credentials  must be communicated to processes connecting to
        #the service, to do this while the run method is running we
        #cheat and set the return value promise within the run method
        self._setReturnValuesForPromises(startCredentials, fileStore.jobStore)
        self._rvs = {}  # Set this to avoid the return values being updated after the
        #run method has completed!
        #Now flag that the service is running jobs can connect to it
        assert self.startFileStoreID != None
        assert fileStore.jobStore.fileExists(self.startFileStoreID)
        fileStore.jobStore.deleteFile(self.startFileStoreID)
        assert not fileStore.jobStore.fileExists(self.startFileStoreID)
        #Now block until we are told to stop, which is indicated by the removal
        #of a file
        assert self.stopFileStoreID != None
        while fileStore.jobStore.fileExists(self.stopFileStoreID):
            time.sleep(1) #Avoid excessive polling
        #Now kill the service
        service.stop()

    def getUserScript(self):
        return self.serviceModule

class PromisedJobReturnValue(object):
    """
    References a return value from a :func:`toil.job.Job.run` or \
    :func:`toil.job.Job.Service.start` method as a *promise* before the method \
    itself is run. 
    
    Let T be a job. Instances of PromisedJobReturnValue (termed a *promise*) are returned by \
    T.rv(), which is used to reference the return value of T's run function. \
    When the promise is passed to the constructor (or as an argument to a wrapped function) \
    of a different, successor job \
    the promise will be replaced by the actual referenced return value. \
    This mechanism allows a return values from one job's run method to be input \
    argument to job before the former job's run function has been executed.
    """
    def __init__(self, promiseCallBackFunction):
        self.promiseCallBackFunction = promiseCallBackFunction 
        
def promisedJobReturnValuePickleFunction(promise):
    """
    This function and promisedJobReturnValueUnpickleFunction are used as custom \
    pickle/unpickle functions to ensure that when the PromisedJobReturnValue instance \
    p is unpickled it is replaced with the object pickled in p.jobStoreFileID
    """
    #The creation of the jobStoreFileID is intentionally lazy, we only
    #create a fileID if the promise is being pickled. This is done so
    #that we do not create fileIDs that are discarded/never used.
    jobStoreFileID, jobStoreString = promise.promiseCallBackFunction()
    return promisedJobReturnValueUnpickleFunction, (jobStoreString, jobStoreFileID)

#These promise files must be deleted when we know we don't need the promise again.
promiseFilesToDelete = set()
promisedJobReturnValueUnpickleFunction_jobStore = None #This is a jobStore instance
#used to unpickle promises

def promisedJobReturnValueUnpickleFunction(jobStoreString, jobStoreFileID):
    """
    The PromisedJobReturnValue custom unpickle function.
    """
    global promisedJobReturnValueUnpickleFunction_jobStore
    if promisedJobReturnValueUnpickleFunction_jobStore == None:
        promisedJobReturnValueUnpickleFunction_jobStore = loadJobStore(jobStoreString)
    promiseFilesToDelete.add(jobStoreFileID)
    with promisedJobReturnValueUnpickleFunction_jobStore.readFileStream(jobStoreFileID) as fileHandle:
        value = cPickle.load(fileHandle) #If this doesn't work then the file containing the promise may not exist or be corrupted.
        return value

#This sets up the custom magic for pickling/unpickling a PromisedJobReturnValue
copy_reg.pickle(PromisedJobReturnValue,
                promisedJobReturnValuePickleFunction,
                promisedJobReturnValueUnpickleFunction)

def deleteFileStoreIDs(job, jobStoreFileIDsToDelete):
    """
    Job function that deletes a bunch of files using their jobStoreFileIDs.
    """
    map(lambda i : job.fileStore.jobStore.deleteFile(i), jobStoreFileIDsToDelete)

def blockUntilDeleted(job, jobStoreFileIDs):
    """
    Function will not terminate until all the fileStoreIDs in jobStoreFileIDs
    cease to exist.
    """
    while True:
        jobStoreFileIDs = [ i for i in jobStoreFileIDs
                           if job.fileStore.jobStore.fileExists(i) ]
        if len(jobStoreFileIDs) == 0:
            break
        time.sleep(1)
