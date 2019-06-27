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
from builtins import map
from builtins import str
from builtins import range
from builtins import object
from abc import abstractmethod, ABCMeta
from collections import namedtuple, defaultdict
from contextlib import contextmanager
from fcntl import flock, LOCK_EX, LOCK_UN
from functools import partial
from hashlib import sha1
from threading import Thread, Semaphore, Event
from future.utils import with_metaclass
from six.moves.queue import Empty, Queue
import base64
import dill
import errno
import logging
import os
import shutil
import sqlite3
import stat
import tempfile
import time
import uuid

from toil.common import cacheDirName, getDirSizeRecursively, getFileSystemSize
from toil.lib.bioio import makePublicDir
from toil.lib.humanize import bytes2human
from toil.lib.misc import mkdir_p
from toil.lib.objects import abstractclassmethod
from toil.resource import ModuleDescriptor
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.fileStores import FileID

logger = logging.getLogger(__name__)

class CacheError(Exception):
    """
    Error Raised if the user attempts to add a non-local file to cache
    """

    def __init__(self, message):
        super(CacheError, self).__init__(message)


class CacheUnbalancedError(CacheError):
    """
    Raised if file store can't free enough space for caching
    """
    message = 'Unable unable to free enough space for caching.  This error frequently arises due ' \
              'to jobs using more disk than they have requested.  Turn on debug logging to see ' \
              'more information leading up to this error through cache usage logs.'

    def __init__(self):
        super(CacheUnbalancedError, self).__init__(self.message)


class IllegalDeletionCacheError(CacheError):
    """
    Error Raised if the Toil detects the user deletes a cached file
    """

    def __init__(self, deletedFile):
        message = 'Cache tracked file (%s) deleted explicitly by user. Use deleteLocalFile to ' \
                  'delete such files.' % deletedFile
        super(IllegalDeletionCacheError, self).__init__(message)


class InvalidSourceCacheError(CacheError):
    """
    Error Raised if the user attempts to add a non-local file to cache
    """

    def __init__(self, message):
        super(InvalidSourceCacheError, self).__init__(message)

class CachingFileStore(AbstractFileStore):
    """
    A cache-enabled file store.

    Provides files that are read out as symlinks or hard links into a cache
    directory for the node, if permitted by the workflow.

    Also attempts to write files back to the backing JobStore asynchronously,
    after quickly taking them into the cache. Writes are only required to
    finish when the job's actual state after running is committed back to the
    job store.
    
    Internaly, manages caching using a database. Each node has its own
    database, shared between all the workers on the node. The database contains
    several tables:

    files contains one entry for each file in the cache. Each entry knows the
    path to its data on disk. It also knows its global file ID, whether it is
    in the process of being read, written, or deleted (and if so by what
    worker). It also knows its size.

    references contains one entry for each outstanding reference to a cached
    file (hard link, symlink, or full copy). It remembers what job ID has the
    reference, and the path the reference is at.
    References can be mutable (link) or immutable (full copy).
    Only files with no  immutable references are eligible for eviction.
    Mutabel references only exist to let deleteLocalFile find all the
    previously read copies, and can exist even when the file is not cached.

    jobs contains one entry for each job currently running. It keeps track of
    the job's ID, the worker that is supposed to be running the job, the job's
    disk requirement, and the job's local temp dir path that will need to be
    cleaned up. When workers check for jobs whose workers have died, they null
    out the old worker, and grab ownership of and clean up jobs and their
    references until the null-worker jobs are gone.

    properties contains key, value pairs for tracking total space available,
    and whether caching is free for this run.
    
    """

    def __init__(self, jobStore, jobGraph, localTempDir, waitForPreviousCommit):
        super(CachingFileStore, self).__init__(jobStore, jobGraph, localTempDir, waitForPreviousCommit)
        
        # Variables related to caching
        # Decide where the cache directory will be. We put it next to the
        # local temp dirs for all of the jobs run on this machine.
        # At this point in worker startup, when we are setting up caching,
        # localTempDir is the worker directory, not the job directory.
        self.localCacheDir = os.path.join(os.path.dirname(localTempDir),
                                          cacheDirName(self.jobStore.config.workflowID))
        
        # Since each worker has it's own unique CachingFileStore instance, and only one Job can run
        # at a time on a worker, we can track some stuff about the running job in ourselves.
        self.jobName = str(self.jobGraph)
        self.jobID = sha1(self.jobName.encode('utf-8')).hexdigest()
        logger.debug('Starting job (%s) with ID (%s).', self.jobName, self.jobID)

        # When the job actually starts, we will fill this in with the job's disk requirement.
        self.jobDiskBytes = None

        # We need to track what attempt of the workflow we are, to prevent crosstalk between attempts' caches.
        self.workflowAttemptNumber = self.jobStore.config.workflowAttemptNumber

        # Make sure the cache directory exists
        mkdir_p(self.localCacheDir)

        # Determine if caching is free

        # Connect to the cache database in there, or create it if not present
        dbPath = os.path.join(self.localCacheDir, 'cache-{}.db'.format(self.workflowAttemptNumber))
        self.db = sqlite3.connect(dbPath).cursor()

        # Make sure to register this as the current database, clobbering any previous attempts.
        # We need this for shutdown to be able to find the database from the most recent execution and clean up all its files.
        os.link(dbPath, os.path.join(self.localCacheDir, 'cache.db'))

        # Set up the tables
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id TEXT NOT NULL PRIMARY KEY, 
                path TEXT UNIQUE NOT NULL,
                size INT NOT NULL,
                reader INT,
                writer INT,
                deleter INT
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS references (
                path TEXT NOT NULL PRIMARY KEY,
                file_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                mutable BOOLEAN NOT NULL
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT NOT NULL PRIMARY KEY,
                temp TEXT NOT NULL,
                disk INT NOT NULL,
                worker INT
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                name TEXT NOT NULL PRIMARY KEY,
                value INT NOT NULL
            )
        """)

        
        # Initialize the space accounting properties
        freeSpace, _ = getFileSystemSize(tempCacheDir)
        self.db.execute('INSERT OR IGNORE INTO properties VALUES (?, ?)', ('maxSpace', freeSpace))

        # Space used by caching and by jobs is accounted with queries

    # Caching-specific API

    def getCacheLimit(self):
        """
        Return the total number of bytes to which the cache is limited.

        If no limit is in place, return None.
        """

        for row in self.db.execute('SELECT value FROM properties WHERE name = ?', ('maxSpace',)):
            return row[0]
        return None

    def getCacheUsed(self):
        """
        Return the total number of bytes used in the cache.

        If no value is available, return None.
        """

        for row in self.db.execute('SELECT SUM(size) FROM files'):
            return row[0]
        return None

    def getCacheExtraJobSpace(self):
        """
        Return the total number of bytes of disk space requested by jobs
        running against this cache but not yet used.

        We can get into a situation where the jobs on the node take up all its
        space, but then they want to write to or read from the cache. So when
        that happens, we need to debit space from them somehow...

        If no value is available, return None.
        """

        # Total up the sizes of all the reads of files and subtract it from the total disk reservation of all jobs
        for row in self.db.execute("""
            SELECT (
                (SELECT SUM(disk) FROM jobs) - 
                (SELECT SUM(files.size) FROM references INNER JOIN files ON references.file_id = files.id WHERE references.mutable = FALSE)
            ) as result
        """):
            return row[0]
        return None

    def getCacheAvailable(self):
        """
        Return the total number of free bytes available for caching, or, if
        negative, the total number of bytes of cached files that need to be
        evicted to free up enough space for all the currently scheduled jobs.
        """

        # Get the max space on our disk.
        # Subtract out the number of bytes of cached content.
        # Also subtract out the number of bytes of job disk requirements that
        # aren't being spent by those jobs on immutable references to cached
        # content.

        for row in self.db.execute("""
            SELECT (
                (SELECT value FROM properties WHERE name = 'maxSpace') -
                (SELECT SUM(size) FROM files) -
                ((SELECT SUM(disk) FROM jobs) - 
                (SELECT SUM(files.size) FROM references INNER JOIN files ON references.file_id = files.id WHERE references.mutable = FALSE))
            ) as result
        """):
            return row[0]
        return None

    def getCacheJobRequirement(self):
        """
        Return the total number of bytes of disk space requested by the current
        job.

        The cache tracks this in order to enable the cache's disk space to grow
        and shrink as jobs start and stop using it.

        If no value is available, return None.
        """

        # TODO: remove "cache" from name?
    
        return self.jobDiskBytes


    def adjustCacheLimit(self, newTotalBytes):
        """
        Adjust the total cache size limit to the given number of bytes.
        """

        self.db.execute('UPDATE properties SET value = ? WHERE name = ?', (newTotalBytes, 'maxSpace')):

    def fileIsCached(self, fileID):
        """
        Return true if the given file is currently cached, and false otherwise.
        """

        for row in self.db.execute('SELECT COUNT(*) FROM files WHERE id = ?', (fileID,)):
            return True
        return False

    def getFileReaderCount(self, fileID):
        """
        Return the number of current outstanding reads of the given file from
        the cache.
        """

        for row in self.db.execute('SELECT COUNT(*) FROM references WHERE file_id = ?', (fileID,)):
            return row[0]
        return 0
        

    def cachingIsFree(self):
        """
        Return true if files can be cached for free, without taking up space.
        Return false otherwise.

        This will be true when working with certain job stores in certain
        configurations, most notably the FileJobStore.
        """

        for row in self.db.execute('SELECT value FROM properties WHERE name = ?', ('freeCaching',)):
            return row[0] == 1

        # Otherwise we need to set it
        from toil.jobStores.fileJobStore import FileJobStore
        if isinstance(self.jobStore, FileJobStore):
            # Caching may be free since we are using a file job store.

            # Create an empty file.
            emptyID = self.jobStore.getEmptyFileStoreID()

            # Read it out to a generated name.
            destDir = tempfile.mkdtemp(dir=self.localCacheDir)
            cachedFile = os.path.join(destDir, 'sniffLinkCount') 
            self.jobStore.readFile(emptyID, cachedFile, symlink=False)

            # Check the link count
            if os.stat(cachedFile).st_nlink == 2:
                # Caching must be free
                free = 1
            else:
                # If we only have one link, caching costs disk.
                free = 0

            # Clean up
            os.unlink(cachedFile)
            os.rmdir(destDir)
            self.jobStore.deleteFile(emptyID)
        else:
            # Caching is only ever free with the file job store
            free = 0

        # Save to the database if we're the first to work this out
        self.db.execute('INSERT OR IGNORE INTO properties VALUES (?, ?)', ('freeCaching', free))

        # Return true if we said caching was free
        return free == 1


    # Internal caching logic

    def _allocateSpaceForJob(self, newJobReqs):
        """ 
        A new job is starting that needs newJobReqs space.

        We need to record that we have a job running now that needs this much space.

        We also need to evict enough stuff from the cache so that we have room
        for this job to fill up that much space even if it doesn't cache
        anything.

        localTempDir must have already been pointed to the job's temp dir.

        :param float newJobReqs: the total number of bytes that this job requires.
        """

        # Put an entry in the database for this job being run on this worker
        pid = os.getpid()
        self.db.execute('INSERT INTO jobs VALUES (?, ?, ?, ?)', (self.jobID, self.localTempDir, newJobReqs, pid))

        # Now we need to make sure that we can fit all currently cached files,
        # and the parts of the total job requirements not currently spent on
        # cached files, in under the total disk space limit.

        if self.getCacheAvailable() >= 0:
            # We're fine on disk space
            return

        # Otherwise we need to clear stuff.

        while self.getCacheAvailable() < 0:

            # Find entries with set deleters for which the deleters have died.
            deletingWorkers = []
            for row in self.db.execute('SELECT UNIQUE(deleter) FROM files WHERE deleter IS NOT NULL'):
                deletingWorkers.append(row[0])
            deadWorkers = []
            for worker in deletingWorkers:
                if not self._pidExists(worker):
                    deadWorkers.append(worker)

            for worker in deadWorkers:
                # Try and adopt all of them
                self.db.execute('UPDATE files SET deleter = ? WHERE deleter = ?', (pid, worker))
            
            # Track if we actually deleted anything
            deletedAnything = False
            
            for row in self.db.execute('SELECT (id, path) FROM files WHERE deleter = ?', (pid,))
                # Grab everything we are supposed to delete and delete it
                fileId = row[0]
                filePath = row[1]
                try:
                    os.unlink(filePath)
                except OSError:
                    # Probably already deleted
                    continue

                # We have something we will drop from the database
                deletedAnything = True

            # Now clear out everything we should have just deleted.
            # Nobody is going to be dumping more files on our PID.
            self.db.execute('DELETE FROM files WHERE deleter = ?', (pid,))

            if deletedAnything:
                # We got rid of something. Maybe we don't need to find new stuff to get rid of.
                continue

            # Otherwise, cleaning up after dead workers wasn't enough.
            
            # Find something that has no references and is not already being deleted.
            self.db.execute("""
                SELECT files.id FROM files WHERE files.deleter IS NULL AND files.reader IS NULL AND files.writer IS NULL AND NOT EXISTS (
                    SELECT NULL FROM references WHERE references.file_id = files.id AND references.mutable = FALSE
                ) LIMIT 1
            """)
            row = self.db.fetch()
            if row is None:
                # Nothing can be evicted by us.
                # Someone else might be in the process of evicting something that will free up space for us too.
                
                # For now we will just spin and hope that is the case.
                # TODO: detect too many spins/spins when nobody else has anything either and raise a CacheUnbalanceError

                continue

            # Otherwise we found an eviction candidate.
            fileId = row[0]
            
            # See if we can grab it.
            self.db.execute('UPDATE files SET deleter = ? WHERE id = ?', (pid, fileId))
            self.db.execute('SELECT path FROM files WHERE id = ? AND deleter = ?', (fileId, pid))
            row = self.db.fetchone()
            if row is None:
                # We didn't get it. Look for something else to evict
                continue

            # We are now the deleter for this file. Nobody is allowed to use it until we delete it.
            # They must wait and poll to see if the cache entry goes away or the deleter dies before reading it.

            # Find and delete the file itself
            filePath = row[0]
            # Unlink shouldn't fail the first time.
            os.unlink(filePath)

            # Drop it from the database
            self.db.execute('DELETE FROM files WHERE id = ?', (fileId,))

        # TODO: handle cases of dead readers and writers. These can cause
        # trouble because the file is present in the cache and has no
        # references but can't be evicted.
        # TODO: Roll all handling of dead readers/writers/workers into one big reap function. Then we poke it every time we're wanting to do something...


    # Normal AbstractFileStore API

    @contextmanager
    def open(self, job):
        """
        This context manager decorated method allows cache-specific operations to be conducted
        before and after the execution of a job in worker.py
        """
        # Create a working directory for the job
        startingDir = os.getcwd()
        # Move self.localTempDir from the worker directory set up in __init__ to a per-job directory.
        self.localTempDir = makePublicDir(os.path.join(self.localTempDir, str(uuid.uuid4())))
        # Check the status of all jobs on this node. If there are jobs that started and died before
        # cleaning up their presence from the database, clean them up ourselves.
        self._removeDeadJobs(self.db)
        # Get the requirements for the job and clean the cache if necessary. _allocateSpaceForJob will
        # ensure that the requirements for this job are stored in the state file.
        self.jobDiskBytes = job.disk
        # Register the current job as taking this much space, and evict files
        # from the cache to make room.
        self._allocateSpaceForJob(self.jobDiskBytes)
        try:
            os.chdir(self.localTempDir)
            yield
        finally:
            # See how much disk space is used at the end of the job.
            # Not a real peak disk usage, but close enough to be useful for warning the user.
            # TODO: Push this logic into the abstract file store
            diskUsed = getDirSizeRecursively(self.localTempDir)
            logString = ("Job {jobName} used {percent:.2f}% ({humanDisk}B [{disk}B] used, "
                         "{humanRequestedDisk}B [{requestedDisk}B] requested) at the end of "
                         "its run.".format(jobName=self.jobName,
                                           percent=(float(diskUsed) / self.jobDiskBytes * 100 if
                                                    self.jobDiskBytes > 0 else 0.0),
                                           humanDisk=bytes2human(diskUsed),
                                           disk=diskUsed,
                                           humanRequestedDisk=bytes2human(self.jobDiskBytes),
                                           requestedDisk=self.jobDiskBytes))
            self.logToMaster(logString, level=logging.DEBUG)
            if diskUsed > self.jobDiskBytes:
                self.logToMaster("Job used more disk than requested. Please reconsider modifying "
                                 "the user script to avoid the chance  of failure due to "
                                 "incorrectly requested resources. " + logString,
                                 level=logging.WARNING)

            # Go back up to the per-worker local temp directory.
            os.chdir(startingDir)
            self.cleanupInProgress = True
            
            # TODO: record that self.jobDiskBytes bytes of disk space are now no longer used by this job.

    def writeGlobalFile(self, localFileName, cleanup=False):
        absLocalFileName = self._resolveAbsoluteLocalPath(localFileName)
        creatorID = self.jobGraph.jobStoreID
        fileStoreID = self.jobStore.writeFile(absLocalFileName, creatorID, cleanup)
        self.localFileMap[fileStoreID].append(absLocalFileName)
        return FileID.forPath(fileStoreID, absLocalFileName)

    def readGlobalFile(self, fileStoreID, userPath=None, cache=True, mutable=False, symlink=False):
        if userPath is not None:
            localFilePath = self._resolveAbsoluteLocalPath(userPath)
            if os.path.exists(localFilePath):
                raise RuntimeError(' File %s ' % localFilePath + ' exists. Cannot Overwrite.')
        else:
            localFilePath = self.getLocalTempFileName()

        self.jobStore.readFile(fileStoreID, localFilePath, symlink=symlink)
        self.localFileMap[fileStoreID].append(localFilePath)
        return localFilePath

    def readGlobalFileStream(self, fileStoreID):
        return self.jobStore.readFileStream(fileStoreID)

    def deleteLocalFile(self, fileStoreID):
        try:
            localFilePaths = self.localFileMap.pop(fileStoreID)
        except KeyError:
            raise OSError(errno.ENOENT, "Attempting to delete a non-local file")
        else:
            for localFilePath in localFilePaths:
                os.remove(localFilePath) 

    def deleteGlobalFile(self, fileStoreID):
        jobStateIsPopulated = False
        with self._CacheState.open(self) as cacheInfo:
            if self.jobID in cacheInfo.jobState:
                jobState = self._JobState(cacheInfo.jobState[self.jobID])
                jobStateIsPopulated = True
        if jobStateIsPopulated and fileStoreID in list(jobState.jobSpecificFiles.keys()):
            # Use deleteLocalFile in the backend to delete the local copy of the file.
            self.deleteLocalFile(fileStoreID)
            # At this point, the local file has been deleted, and possibly the cached copy. If
            # the cached copy exists, it is either because another job is using the file, or
            # because retaining the file in cache doesn't unbalance the caching equation. The
            # first case is unacceptable for deleteGlobalFile and the second requires explicit
            # deletion of the cached copy.
        # Check if the fileStoreID is in the cache. If it is, ensure only the current job is
        # using it.
        cachedFile = self.encodedFileID(fileStoreID)
        if os.path.exists(cachedFile):
            self.removeSingleCachedFile(fileStoreID)
        # Add the file to the list of files to be deleted once the run method completes.
        self.filesToDelete.add(fileStoreID)
        self.logToMaster('Added file with ID \'%s\' to the list of files to be' % fileStoreID +
                         ' globally deleted.', level=logging.DEBUG)

    def exportFile(self, jobStoreFileID, dstUrl):
        self.jobStore.exportFile(jobStoreFileID, dstUrl)

    def waitForCommit(self):
        # there is no asynchronicity in this file store so no need to block at all
        return True

    def commitCurrentJob(self):
        try:
            # Indicate any files that should be deleted once the update of
            # the job wrapper is completed.
            self.jobGraph.filesToDelete = list(self.filesToDelete)
            # Complete the job
            self.jobStore.update(self.jobGraph)
            # Delete any remnant jobs
            list(map(self.jobStore.delete, self.jobsToDelete))
            # Delete any remnant files
            list(map(self.jobStore.deleteFile, self.filesToDelete))
            # Remove the files to delete list, having successfully removed the files
            if len(self.filesToDelete) > 0:
                self.jobGraph.filesToDelete = []
                # Update, removing emptying files to delete
                self.jobStore.update(self.jobGraph)
        except:
            self._terminateEvent.set()
            raise

    @classmethod
    def shutdown(cls, dir_):
        """
        :param dir_: The cache directory, containing cache state database.
               Job local temp directories will be removed due to their appearance
               in the database. 
        """
        
        # We don't have access to a class instance, nor do we have access to
        # the workflow attempt number that we would need in order to find the
        # right database.
        dbPath = os.path.join(dir_, 'cache.db')

        if os.path.exists(dbPath):
            try:
                # The database exists, see if we can open it
                db = sqlite3.connect(dbPath).cursor()
            except:
                # Probably someone deleted it.
                pass
            else:
                # We got a database connection

                

                db.close()
        
        shutil.rmtree(dir_)

    def __del__(self):
        """
        Cleanup function that is run when destroying the class instance that ensures that all the
        file writing threads exit.
        """
        pass

    @classmethod
    def _removeDeadJobs(cls, db):
        """
        Look at the state of all jobs registered in the database, and handle them
        (clean up the disk)

        :param sqlite3.Cursor db: Cursor into the cache database.
        :return:
        """

        # Get all the worker PIDs
        workers = []
        for row in db.execute('SELECT DISTINCT worker FROM jobs WHERE worker IS NOT NULL'):
            workers.append(row[0])

        # Work out which of them are not currently running.
        # TODO: account for PID reuse somehow.
        deadWorkers = []
        for worker in workers:
            if not cls._pidExists(worker):
                deadWorkers.append(worker)

        # Now we know which workers are dead.
        # Clear them off of the jobs they had.
        for deadWorker in deadWorkers:
            db.execute('UPDATE jobs SET worker = NULL WHERE worker = ?', (pid, deadWorker))

        
        # Work out our PID for taking ownership of jobs
        pid = os.getpid()
        
        while True:
            # Find an unowned job
            db.execute('SELECT (id, temp) FROM jobs WHERE worker IS NULL LIMIT 1')
            row = db.fetchone()
            if row is None:
                # We cleaned up all the jobs
                break

            # Try to own this job
            db.execute('UPDATE jobs SET worker = ? WHERE id = ? AND worker IS NULL', (pid, row[0]))

            # See if we won the race
            db.execute('SELECT (id, temp) FROM jobs WHERE id = ? AND worker = ?', (row[0], pid))
            row = db.fetchone()
            if row is None:
                # We didn't win the race. Try another one.
                continue

            # Now we own this job, so do it
            jobId = row[0]
            jobTemp = row[1]
            

            for row in db.execute('SELECT path FROM references WHERE job_id = ?', (jobId,)):
                try:
                    # Delete all the reference files.
                    os.unlink(row[0])
                except OSError:
                    # May not exist
                    pass
            # And their database entries
            db.execute('DELETE FROM references WHERE job_id = ?', (jobId,))

            try: 
                # Delete the job's temp directory.
                shutil.rmtree(jobTmp)
            except FileNotFoundError:
                pass

            # Strike the job from the database
            db.execute('DELETE FROM jobs WHERE id = ?', (jobId,))

        # Now we have cleaned up all the jobs that belonged to dead workers that were dead when we entered this function.


