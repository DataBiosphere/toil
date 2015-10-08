#!/usr/bin/env python

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
import xml.etree.cElementTree as ET
from abc import ABCMeta, abstractmethod
import tempfile
import uuid
import time
import copy_reg
import cPickle
import logging
import shutil
import stat
from threading import Thread, Semaphore
from Queue import Queue, Empty
from bd2k.util.humanize import human2bytes
from io import BytesIO

from toil.resource import ModuleDescriptor
from toil.common import loadJobStore

logger = logging.getLogger( __name__ )

from toil.lib.bioio import (setLoggingFromOptions,
                               getTotalCpuTimeAndMemoryUsage, getTotalCpuTime)
from toil.common import setupToil, addOptions
from toil.leader import mainLoop

class JobException( Exception ):
    def __init__( self, message ):
        super( JobException, self ).__init__( message )

class Job(object):
    """
    Represents a unit of work in toil. Jobs are composed into graphs
    which make up a workflow. 
    
    This public functions of this class and its  nested classes are the API 
    to toil.
    """
    def __init__(self, memory=None, cores=None, disk=None):
        """
        This method must be called by any overiding constructor.
        
        Memory is the maximum number of bytes of memory the job will
        require to run. Cores is the number of CPU cores required.
        """
        self.cores = cores
        self.memory = human2bytes(str(memory)) if memory is not None else memory
        self.disk = human2bytes(str(disk)) if disk is not None else disk
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
        Do user stuff here, including creating any follow on jobs.
        
        The fileStore argument is an instance of Job.FileStore, and can
        be used to create temporary files which can be shared between jobs.
        
        The return values of the function can be passed to other jobs
        by means of the rv() function.
        """
        pass

    def addChild(self, childJob):
        """
        Adds the child job to be run as child of this job. Returns childJob.
        Child jobs are run after the Job.run method has completed.
        
        See Job.checkJobGraphAcylic for formal definition of allowed forms of
        job graph.
        """
        self._children.append(childJob)
        childJob._addPredecessor(self)
        return childJob

    def hasChild(self, childJob):
        """
        Check if the job is already a child of this job.
        """
        return childJob in self._children

    def addService(self, service):
        """
        Add a service of type Job.Service. The Job.Service.start() method
        will be called after the run method has completed but before any successors 
        are run. It's Job.Service.stop() method will be called once the
        successors of the job have been run. 
        
        Services allow things like databases and servers to be started and accessed
        by jobs in a workflow.
        
        :rtype : An instance of PromisedJobReturnValue which will be replaced
        with the return value from the service.start() in any successor of the job.
        """
        jobService = ServiceJob(service)
        self._services.append(jobService)
        return jobService.rv()

    def addFollowOn(self, followOnJob):
        """
        Adds a follow-on job, follow-on jobs will be run
        after the child jobs and their descendants have been run.
        Returns followOnJob.
        
        See Job.checkJobGraphAcylic for formal definition of allowed forms of
        job graph.
        """
        self._followOns.append(followOnJob)
        followOnJob._addPredecessor(self)
        return followOnJob

    ##Convenience functions for creating jobs

    def addChildFn(self, fn, *args, **kwargs):
        """
        Adds a child fn. See FunctionWrappingJob.
        Returns the new child Job.
        """
        return self.addChild(FunctionWrappingJob(fn, *args, **kwargs))

    def addChildJobFn(self, fn, *args, **kwargs):
        """
        Adds a child job fn. See JobFunctionWrappingJob.
        Returns the new child Job.
        """
        return self.addChild(JobFunctionWrappingJob(fn, *args, **kwargs))

    def addFollowOnFn(self, fn, *args, **kwargs):
        """
        Adds a follow-on fn. See FunctionWrappingJob.
        Returns the new follow-on Job.
        """
        return self.addFollowOn(FunctionWrappingJob(fn, *args, **kwargs))

    def addFollowOnJobFn(self, fn, *args, **kwargs):
        """
        Add a follow-on job fn. See JobFunctionWrappingJob.
        Returns the new follow-on Job.
        """
        return self.addFollowOn(JobFunctionWrappingJob(fn, *args, **kwargs))

    @staticmethod
    def wrapJobFn(fn, *args, **kwargs):
        """
        Makes a Job out of a job function.
        
        Convenience function for constructor of JobFunctionWrappingJob
        """
        return JobFunctionWrappingJob(fn, *args, **kwargs)

    @staticmethod
    def wrapFn(fn, *args, **kwargs):
        """
        Makes a Job out of a function.
        
        Convenience function for constructor of FunctionWrappingJob
        """
        return FunctionWrappingJob(fn, *args, **kwargs)

    def encapsulate(self):
        """
        See EncapsulatedJob.
        
        :rtype : A new EncapsulatedJob for this job.
        """
        return EncapsulatedJob(self)

    ####################################################
    #The following function is used for passing return values between
    #job run functions
    ####################################################
    
    def rv(self, argIndex=None):
        """
        Gets a PromisedJobReturnValue, representing the argIndex return
        value of the run function (see run method for description).
        This PromisedJobReturnValue, if a class attribute of a Job instance,
        call it T, will be replaced by the actual return value when the 
        T is loaded. The function rv therefore allows the output 
        from one Job to be wired as input to another Job before either
        is actually run.  
        
        :param argIndex: If None the complete return value will be returned, if argIndex
        is an integer it is used to refer to the return value as indexable 
        (tuple/list/dictionary, or in general object that implements __getitem__), 
        hence rv(i) would refer to the ith (indexed from 0) member of return value.
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
        Raises a JobGraphDeadlockException exception if the job graph
        is cyclic or contains multiple roots.
        """
        self.checkJobGraphConnected()
        self.checkJobGraphAcylic()

    def getRootJobs(self):
        """
        A root is a job with no predecessors.
        :rtype : set, the roots of the connected component of jobs that
        contains this job.
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
        Raises a JobGraphDeadlockException exception if getRootJobs() does not
        contain exactly one root job.
        As execution always starts from one root job, having multiple root jobs will
        cause a deadlock to occur.
        """
        rootJobs = self.getRootJobs()
        if len(rootJobs) != 1:
            raise JobGraphDeadlockException("Graph does not contain exactly one root job: %s" % rootJobs)

    def checkJobGraphAcylic(self):
        """
        Raises a JobGraphDeadlockException exception if the connected component
        of jobs containing this job contains any cycles of child/followOn dependencies
        in the augmented job graph (see below). Such cycles are not allowed
        in valid job graphs. This function is run during execution.
        
        A job B that is on a directed path of child/followOn edges from a
        job A in the job graph is a descendant of A,
        similarly A is an ancestor of B.
        
        A follow-on edge (A, B) between two jobs A and B is equivalent
        to adding a child edge to B from (1) A, (2) from each child of A, 
        and (3) from the descendants of each child of A. We
        call such an edge an "implied" edge. The augmented job graph is a
        job graph including all the implied edges.

        For a job (V, E) the algorithm is O(|V|^2). It is O(|V| + |E|) for
        a graph with no follow-ons. The former follow on case could be improved!
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
        Used to setup and run a graph of jobs.
        """
        @staticmethod
        def getDefaultOptions(jobStore):
            """
            Returns an optparse.Values object of the 
            options used by a toil.
            """
            parser = ArgumentParser()
            Job.Runner.addToilOptions(parser)
            options = parser.parse_args(args=[jobStore])
            return options

        @staticmethod
        def addToilOptions(parser):
            """
            Adds the default toil options to an optparse or argparse
            parser object.
            """
            addOptions(parser)

        @staticmethod
        def startToil(job, options):
            """
            Runs the toil workflow using the given options 
            (see Job.Runner.getDefaultOptions and Job.Runner.addToilOptions) 
            starting with this job. 
            
            :raises: toil.leader.FailedJobsException if at the end of function their remain
    failed jobs
            """
            setLoggingFromOptions(options)
            with setupToil(options, userScript=job.getUserScript()) as (config, batchSystem, jobStore):
                if options.restart:
                    jobStore.clean(job._loadRootJob(jobStore)) #This cleans up any half written jobs after a restart
                    rootJob = job._loadRootJob(jobStore)
                else:
                    #Setup the first wrapper.
                    rootJob = job._serialiseFirstJob(jobStore)
                return mainLoop(config, batchSystem, jobStore, rootJob)

    class FileStore( object ):
        """
        Class used to manage temporary files and log messages, 
        passed as argument to the Job.run method.
        """
        def __init__(self, jobStore, jobWrapper, localTempDir, 
                     inputBlockFn, jobStoreFileIDToCacheLocation, terminateEvent):
            """
            This constructor should not be called by the user, 
            FileStore instances are only provided as arguments 
            to the run function.
            """
            self.jobStore = jobStore
            self.jobWrapper = jobWrapper
            self.localTempDir = localTempDir
            self.loggingMessages = []
            self.filesToDelete = set()
            self.jobsToDelete = set()
            #Asynchronous writes stuff
            self.workerNumber = 2
            self.queue = Queue()
            self.updateSemaphore = Semaphore() 
            self.terminateEvent = terminateEvent
            #Function to write files to job store
            def asyncWrite():
                try:
                    while True:
                        try:
                            #Block for up to two seconds waiting for a file
                            args = self.queue.get(timeout=2)
                        except Empty:
                            #Check if termination event is signaled 
                            #(set in the event of an exception in the worker)
                            if terminateEvent.isSet():
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
                except:
                    terminateEvent.set()
                    raise
                    
            self.workers = map(lambda i : Thread(target=asyncWrite), 
                               range(self.workerNumber))
            for worker in self.workers:
                worker.start()
            self.inputBlockFn = inputBlockFn
            #Caching 
            #
            #For files in jobStore that are on the local disk, 
            #map of jobStoreFileIDs to locations in localTempDir.
            self.jobStoreFileIDToCacheLocation = jobStoreFileIDToCacheLocation

        def getLocalTempDir(self):
            """
            Get the absolute path to a new local temporary directory. 
            This directory will exist for the 
            duration of the job only, and is guaranteed to be deleted once
            the job terminates, removing all files it contains recursively. 
            """
            return os.path.abspath(tempfile.mkdtemp(prefix="t", dir=self.localTempDir))
        
        def getLocalTempFile(self):
            """
            Get an absolute path to a local temporary file. 
            This file will exist for the duration of the job only, and
            is guaranteed to be deleted once the job terminates.
            """
            handle, tmpFile = tempfile.mkstemp(prefix="tmp", 
                                               suffix=".tmp", dir=self.localTempDir)
            os.close(handle)
            return os.path.abspath(tmpFile)

        def writeGlobalFile(self, localFileName, cleanup=False):
            """
            Takes a file (as a path) and uploads it to to the global file store, 
            returns an ID that can be used to retrieve the file. 
            
            If cleanup is True then the global file will be deleted once the job
            and all its successors have completed running. If not the global file 
            must be deleted manually.
            
            The write is asynchronous, so further modifications during execution 
            to the file pointed by localFileName will result in undetermined behavior. 
            """
            jobStoreFileID = self.jobStore.getEmptyFileStoreID(None 
                            if not cleanup else self.jobWrapper.jobStoreID)
            self.queue.put((open(localFileName, 'r'), jobStoreFileID))
            #Now put the file into the cache if it is a path within localTempDir
            absLocalFileName = os.path.abspath(localFileName)
            if absLocalFileName.startswith(self.localTempDir):
                #Chmod to make file read only
                os.chmod(absLocalFileName, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                self.jobStoreFileIDToCacheLocation[jobStoreFileID] = absLocalFileName
            return jobStoreFileID
        
        def writeGlobalFileStream(self, cleanup=False):
            """
            Similar to writeGlobalFile, but returns a context manager yielding a 
            tuple of 1) a file handle which can be written to and 2) the ID of 
            the resulting file in the job store. The yielded file handle does
            not need to and should not be closed explicitly.
            
            cleanup is as in writeGlobalFile.
            """
            #TODO: Make this work with the caching??
            return self.jobStore.writeFileStream(None if not cleanup else self.jobWrapper.jobStoreID)
        
        def readGlobalFile(self, fileStoreID, userPath=None):
            """
            Returns an absolute path to a local, temporary copy of the file 
            keyed by fileStoreID. 
            
            *The returned file will be read only (have permissions 444).* 
            
            :param userPath: a path to the name of file to which the global file will be 
            copied or hard-linked (see below). userPath must either be: (1) a 
            file path contained within a directory or, recursively, a subdirectory 
            of a temporary directory returned by Job.FileStore.getLocalTempDir(), 
            or (2) a file path returned by Job.FileStore.getLocalTempFile(). 
            If userPath is specified and this is not true a RuntimeError exception 
            will be raised. If userPath is specified and the file is already cached, 
            the userPath file will be a hard link to the actual location, else it 
            will be an actual copy of the file.  
            """
            if fileStoreID in self.filesToDelete:
                raise RuntimeError("Trying to access a file in the jobStore you've deleted: %s" % fileStoreID)
            if userPath != None:
                userPath = os.path.abspath(userPath) #Make an absolute path
                #Check it is a valid location
                if not userPath.startswith(self.localTempDir):
                    raise RuntimeError("The user path is not contained within the"
                                       " temporary file hierarchy created by the job."
                                       " User path: %s, temporary file root path: %s" % 
                                       (userPath, self.localTempDir))
            #When requesting a new file from the jobStore first check if fileStoreID
            #is a key in jobStoreFileIDToCacheLocation.
            if fileStoreID in self.jobStoreFileIDToCacheLocation:
                cachedAbsFilePath = self.jobStoreFileIDToCacheLocation[fileStoreID]   
                #If the user specifies a location and it is not the current location
                # return a hardlink to the location, else return the original location
                if userPath == None or userPath == cachedAbsFilePath:
                    return cachedAbsFilePath
                if os.path.exists(userPath):
                    os.remove(userPath)
                os.link(cachedAbsFilePath, userPath)
                return userPath
            else:
                #If it is not in the cache read it from the jobStore to the 
                #desired location
                localFilePath = userPath if userPath != None else self.getLocalTempFile()
                self.jobStore.readFile(fileStoreID, localFilePath)
                self.jobStoreFileIDToCacheLocation[fileStoreID] = localFilePath
                #Chmod to make file read only
                os.chmod(localFilePath, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
                return localFilePath

        def readGlobalFileStream(self, fileStoreID):
            """
            Similar to readGlobalFile, but returns a context manager yielding a 
            file handle which can be read from. The yielded file handle does not 
            need to and should not be closed explicitly.
            """
            if fileStoreID in self.filesToDelete:
                raise RuntimeError("Trying to access a file in the jobStore you've deleted: %s" % fileStoreID)
            
            #If fileStoreID is in the cache provide a handle from the local cache
            if fileStoreID in self.jobStoreFileIDToCacheLocation:
                #This leaks file handles (but the commented out code does not work properly)
                return open(self.jobStoreFileIDToCacheLocation[fileStoreID], 'r') 
                #with open(self.jobStoreFileIDToCacheLocation[fileStoreID], 'r') as fH:
                #        yield fH
            else:
                #TODO: Progressively add the file to the cache
                return self.jobStore.readFileStream(fileStoreID)
                #with self.jobStore.readFileStream(fileStoreID) as fH:
                #    yield fH

        def deleteGlobalFile(self, fileStoreID):
            """
            Deletes a global file with the given fileStoreID. 
            To ensure that the job can be restarted if necessary, 
            the delete will not happen until after the job's run method has completed.
            """
            self.filesToDelete.add(fileStoreID)
            #If the fileStoreID is in the cache:
            if fileStoreID in self.jobStoreFileIDToCacheLocation:
                #This will result in the files removal from the cache at the end of the current job
                self.jobStoreFileIDToCacheLocation.pop(fileStoreID)

        def logToMaster(self, string, level=logging.INFO):
            """
            Send a logging message to the leader. 
            The message will also be logged by the worker at the same level.
            """
            logger.log(level=level, msg=("LOG-TO-MASTER: " + string))
            self.loggingMessages.append((str(string), level))
            
        #Private methods 
        
        def _updateJobWhenDone(self):
            """
            Asynchronously update the status of the job on the disk, first waiting
            until the writing threads have finished and the inputBlockFn has stopped
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
                    if self.terminateEvent.isSet():
                        raise RuntimeError("The termination flag is set, exiting before update")
                    
                    #Indicate any files that should be deleted once the update of 
                    #the job wrapper is completed.
                    self.jobWrapper.filesToDelete = len(self.filesToDelete)
                    
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
                    self.terminateEvent.set()
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
            At the end of the job, remove all localTempDir files except those whose value is in
            jobStoreFileIDToCacheLocation.
            
            The param cacheSize is the total number of bytes of files allowed in the cache.
            """
            #Remove files so that the total cached files are smaller than a cacheSize
            
            #List of pairs of (fileSize, fileStoreID) for cached files
            cachedFileSizes = map(lambda x : (os.stat(self.jobStoreFileIDToCacheLocation[x]).st_size, x), 
                                  self.jobStoreFileIDToCacheLocation.keys())
            #Total number of bytes stored in cached files
            totalCachedFileSizes = sum(map(lambda x : x[0], cachedFileSizes))
            #Remove smallest files first - this is not obviously best, could do it a different
            #way
            cachedFileSizes.sort()
            cachedFileSizes.reverse()
            #Now do the actual file removal
            while totalCachedFileSizes > cacheSize:
                fileSize, fileStoreID =  cachedFileSizes.pop()
                filePath = self.jobStoreFileIDToCacheLocation[fileStoreID]
                self.jobStoreFileIDToCacheLocation.pop(fileStoreID)
                os.remove(filePath)
                totalCachedFileSizes -= fileSize
                assert totalCachedFileSizes >= 0
            
            #Iterate from the base of localTempDir and remove all 
            #files/empty directories, recursively
            cachedFiles = set(self.jobStoreFileIDToCacheLocation.values())
            
            def clean(dirOrFile):
                canRemove = True 
                if os.path.isdir(dirOrFile):
                    for f in os.listdir(dirOrFile):
                        canRemove = canRemove and clean(os.path.join(dirOrFile, f))
                    if canRemove:
                        os.rmdir(dirOrFile) #Dir should be empty if canRemove is true
                    return canRemove
                if dirOrFile in cachedFiles:
                    return False
                os.remove(dirOrFile)
                return True    
            clean(self.localTempDir)
        
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
            """Cleanup function that is run when destroying the class instance 
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
        def __init__(self, memory=None, cores=None):
            """
            Memory and core requirements are specified identically to the Job
            constructor.
            """
            self.memory = memory
            self.cores = cores

        @abstractmethod
        def start(self):
            """
            Start the service.
            
            :rtype : An object describing how to access the service. Must be 
            pickleable. Will be used by a job to access the service.
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
        Adds a predecessor job to the set of predecessor jobs. Raises a
        RuntimeError is the job is already a predecessor.
        """
        if predecessorJob in self._directPredecessors:
            raise RuntimeError("The given job is already a predecessor of this job")
        self._directPredecessors.add(predecessorJob)

    @staticmethod
    def _loadRootJob(jobStore):
        """
        Loads the root job.
        :throws JobException: If root job is not in the job store. 
        """
        with jobStore.readSharedFileStream("rootJobStoreID") as f: #Load the root job
            rootJobID = f.read()
        if not jobStore.exists(rootJobID):
            raise JobException("No root job (%s) left in toil workflow (workflow has finished successfully?)" % rootJobID)
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
        return importlib.import_module(userModule.name)

    @classmethod
    def _loadJob(cls, command, jobStore):
        """
        Unpickles a job.Job instance by decoding the command. See job.Job._serialiseFirstJob and
        job.Job._makeJobWrappers to see how the Job is encoded in the command. Essentially the
        command is a reference to a jobStoreFileID containing the pickle file for the job and a
        list of modules which must be imported so that the Job can be successfully unpickled.
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
        Unpickles an object graph from the given file handle while loading symbols referencing
        the __main__ module from the given userModule instead.

        :param userModule:
        :param fileHandle:
        :return:
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
        Sets the values for promises using the return values from the job's
        run function.
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
        """Adds the job and all jobs reachable on a directed path from current
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
        Modifies the job graph just before it is serialised to disk 
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

    def _createEmptyJobForJob(self, jobStore, command=None,
                                 predecessorNumber=0):
        """
        Create an empty job for the job.
        """
        return jobStore.create(command=command,
                               memory=(self.memory if self.memory is not None
                                       else jobStore.config.defaultMemory),
                               cores=(self.cores if self.cores is not None
                                    else float(jobStore.config.defaultCores)),
                               disk=(self.disk if self.disk is not None
                                    else float(jobStore.config.defaultDisk)),
                               predecessorNumber=predecessorNumber)
        
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
        Get a list of jobs such that for all pairs of indices i, j for which i < j, 
        the job at index i can be run before the job at index j.
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
        jobsToJobWrappers[self].command = ' '.join( ('_toil', fileStoreID) + self.userModule.globalize())      
        #Update the status of the jobWrapper on disk
        jobStore.update(jobsToJobWrappers[self])
    
    def _serialiseJobGraph(self, jobWrapper, jobStore, returnValues, firstJob):  
        """
        Pickle the graph of jobs in the jobStore. The graph is not fully serialised
        until the jobWrapper itself is written to disk, this is not performed by this
        function because of the need to coordinate this operation with other updates.
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
        """This is the core method for running the job within a worker.
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
            stats = ET.SubElement(stats, "job")
            stats.attrib["time"] = str(time.time() - startTime)
            totalCpuTime, totalMemoryUsage = getTotalCpuTimeAndMemoryUsage()
            stats.attrib["clock"] = str(totalCpuTime - startClock)
            stats.attrib["class"] = self._jobName()
            stats.attrib["memory"] = str(totalMemoryUsage)

    ####################################################
    #Method used to resolve the module in which an inherited job instances
    #class is defined
    ####################################################

    @staticmethod
    def _resolveMainModule( moduleName ):
        """
        Returns a tuple of two elements, the first element being the path 
        to the directory containing the given
        module and the second element being the name of the module. 
        If the given module name is "__main__",
        then that is translated to the actual file name of the top-level 
        script without .py or .pyc extensions. The
        caller can then add the first element of the returned tuple to 
        sys.path and load the module from there. See also worker.loadJob().
        """
        # looks up corresponding module in sys.modules, gets base name, drops .py or .pyc
        moduleDirPath, moduleName = os.path.split(os.path.abspath(sys.modules[moduleName].__file__))
        if moduleName.endswith('.py'):
            moduleName = moduleName[:-3]
        elif moduleName.endswith('.pyc'):
            moduleName = moduleName[:-4]
        else:
            raise RuntimeError(
                "Can only handle main modules loaded from .py or .pyc files, but not '%s'" %
                moduleName)
        return moduleDirPath, moduleName

    def _jobName(self):
        """
        :rtype : string, used as identifier of the job class in the stats report. 
        """
        return self.__class__.__name__

class JobGraphDeadlockException( Exception ):
    def __init__( self, string ):
        super( JobGraphDeadlockException, self ).__init__( string )

class FunctionWrappingJob(Job):
    """
    Job used to wrap a function.
    
    If dill is installed 
    Function can be nested function class function, currently.
    *args and **kwargs are used as the arguments to the function.
    """
    def __init__(self, userFunction, *args, **kwargs):
        # FIXME: I'd rather not duplicate the defaults here, unless absolutely necessary
        cores = kwargs.pop("cores") if "cores" in kwargs else None
        disk = kwargs.pop("disk") if "disk" in kwargs else None
        memory = kwargs.pop("memory") if "memory" in kwargs else None
        Job.__init__(self, memory=memory, cores=cores, disk=disk)
        #If dill is installed pickle the user function directly

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
    Job used to wrap a function.
    A job function is a function which takes as its first argument a reference
    to the wrapping job.
    
    To enable the job function to get access to the Job.FileStore
    instance (see Job.Run), it is made a variable of the wrapping job, so in the wrapped
    job function the attribute "fileStore" of the first argument (the job) is
    an instance of the Job.FileStore class.
    """
    def __init__(self, userFunction, *args, **kwargs):
        super(JobFunctionWrappingJob, self).__init__(userFunction, *args, **kwargs)

    def run(self, fileStore):
        userFunction = self._getUserFunction()
        self.fileStore = fileStore
        rValue = userFunction(*((self,) + tuple(self._args)), **self._kwargs)
        return rValue

class ServiceJob(Job):
    """
    Job used to wrap a Job.Service instance. This constructor should not be called by a user.
    """
    def __init__(self, service):
        """
        :type service: Job.Service
        """
        Job.__init__(self, memory=service.memory, cores=service.cores)
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

class EncapsulatedJob(Job):
    """
    An convenience Job class used to make a job subgraph appear to
    be a single job. 
    
    Let A be a root job potentially with children and follow-ons.
    Without an encapsulated job the simplest way to specify a job B which
    runs after A and all its successors is to create a parent of A' and then make B 
    a follow-on of A'. In turn if we wish to run C after B and its successors then we 
    repeat the process to create B', a parent of B, creating a graph in which A' is run,
    then A as a child of A', then the successors of A, then B' as a follow on of A', 
    then B as a child of B', then the successors of B, then finally C as follow on of B', 
    e.g.
    
    A, B, C = A(), B(), C() #Functions to create job graphs
    A' = Job()
    B' = Job()
    A'.addChild(A)
    A'.addFollowOn(B')
    B'.addChild(B)
    B'.addFollowOn(C)
    
    An encapsulated job of E(A) of A saves making A' and B', instead we can write:
    
    A, B, C = A().encapsulate(), B(), C() #Functions to create job graphs
    A.addChild(B)
    A.addFollowOn(C)
    
    Note the call to encapsulate creates the EncapsulatedJob.
    
    The return value of an encapusulatd job (as accessed by the Job.rv function)
    is the return value of the root job, e.g. A().encapsulate().rv() and A().rv()
    will resolve to the same value after A or A.encapsulate() has been run.
    """
    def __init__(self, job):
        """
        job is the job to encapsulate.
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

class PromisedJobReturnValue(object):
    """
    References a return value from a Job's run function. Let T be a job.
    Instances of PromisedJobReturnValue are created by
    T.rv(), which is used to reference the return value of T's run function.
    When passed to the constructor of a different, successor Job the PromisedJobReturnValue
    will be replaced by the actual referenced return value.
    This mechanism allows a return values from one Job's run method to be input
    argument to Job before the former Job's run function has been executed.
    """
    def __init__(self, promiseCallBackFunction):
        self.promiseCallBackFunction = promiseCallBackFunction 
        
def promisedJobReturnValuePickleFunction(promise):
    """
    This function and promisedJobReturnValueUnpickleFunction are used as custom pickle/unpickle 
    functions to ensure that when the PromisedJobReturnValue instance p is unpickled it is replaced with 
    the object pickled in p.jobStoreFileID
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
    Job function that deletes a bunch of files using their jobStoreFileIDs
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
