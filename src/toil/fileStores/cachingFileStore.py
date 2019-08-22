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
from future.utils import with_metaclass
from six.moves.queue import Empty, Queue
import base64
import dill
import errno
import hashlib
import logging
import os
import shutil
import sqlite3
import stat
import sys
import tempfile
import threading
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

if sys.version_info[0] < 3:
    # Define a usable FileNotFoundError as will be raised by os.remove on a
    # nonexistent file.
    FileNotFoundError = OSError

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
    Error raised if the caching code discovers a file that represents a
    reference to a cached file to have gone missing.

    This can be a big problem if a hard link is moved, because then the cache
    will be unable to evict the file it links to.
    
    Remember that files read with readGlobalFile may not be deleted by the user
    and need to be deleted with deleteLocalFile.
    """

    def __init__(self, deletedFile):
        message = 'Cache tracked file (%s) has been deleted or moved by user ' \
                  ' without updating cache database. Use deleteLocalFile to ' \
                  'delete such files.' % deletedFile
        super(IllegalDeletionCacheError, self).__init__(message)


class InvalidSourceCacheError(CacheError):
    """
    Error raised if the user attempts to add a non-local file to cache
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
    path to its data on disk. It also knows its global file ID, its state, and
    its owning worker PID. If the owning worker dies, another worker will pick
    it up. It also knows its size.

    File states are:
        
    - "cached": happily stored in the cache. Reads can happen immediately.
      Owner is null. May be adopted and moved to state "deleting" by anyone, if
      it has no outstanding immutable references.
    
    - "downloading": in the process of being saved to the cache by a non-null
      owner. Reads must wait for the state to become "cached". If the worker
      dies, goes to state "deleting", because we don't know if it was fully
      downloaded or if anyone still needs it. No references can be created to a
      "downloading" file except by the worker responsible for downloading it.

    - "uploadable": stored in the cache and ready to be written to the job
      store by a non-null owner. Transitions to "uploading" when a (thread of)
      the owning worker process picks it up and begins uploading it, to free
      cache space or to commit a completed job. If the worker dies, goes to
      state "cached", because it may have outstanding immutable references from
      the dead-but-not-cleaned-up job that was going to write it.
      
    - "uploading": stored in the cache and being written to the job store by a
      non-null owner. Transitions to "cached" when successfully uploaded. If
      the worker dies, goes to state "cached", because it may have outstanding
      immutable references from the dead-but-not-cleaned-up job that was
      writing it.

    - "deleting": in the process of being removed from the cache by a non-null
      owner. Will eventually be removed from the database.

    refs contains one entry for each outstanding reference to a cached file
    (hard link, symlink, or full copy). The table name is refs instead of
    references because references is an SQL reserved word. It remembers what
    job ID has the reference, and the path the reference is at. References have
    three states:

    - "immutable": represents a hardlink or symlink to a file in the cache.
      Dedicates the file's size in bytes of the job's disk requirement to the
      cache, to be used to cache this file or to keep around other files
      without references. May be upgraded to "copying" if the link can't
      actually be created.

    - "copying": records that a file in the cache is in the process of being
      copied to a path. Will be upgraded to a mutable reference eventually.

    - "mutable": records that a file from the cache was copied to a certain
      path. Exist only to support deleteLocalFile's API. Only files with only
      mutable references (or no references) are eligible for eviction.
    
    jobs contains one entry for each job currently running. It keeps track of
    the job's ID, the worker that is supposed to be running the job, the job's
    disk requirement, and the job's local temp dir path that will need to be
    cleaned up. When workers check for jobs whose workers have died, they null
    out the old worker, and grab ownership of and clean up jobs and their
    references until the null-worker jobs are gone.

    properties contains key, value pairs for tracking total space available,
    and whether caching is free for this run.
    
    """

    def __init__(self, jobStore, jobGraph, localTempDir, waitForPreviousCommit, forceNonFreeCaching=True):
        super(CachingFileStore, self).__init__(jobStore, jobGraph, localTempDir, waitForPreviousCommit)

        # For testing, we have the ability to force caching to be non-free, by never linking from the file store
        self.forceNonFreeCaching = forceNonFreeCaching
        
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
        self.jobID = self.jobGraph.jobStoreID
        logger.debug('Starting job (%s) with ID (%s).', self.jobName, self.jobID)

        # When the job actually starts, we will fill this in with the job's disk requirement.
        self.jobDiskBytes = None

        # We need to track what attempt of the workflow we are, to prevent crosstalk between attempts' caches.
        self.workflowAttemptNumber = self.jobStore.config.workflowAttemptNumber

        # Make sure the cache directory exists
        mkdir_p(self.localCacheDir)

        # Connect to the cache database in there, or create it if not present
        self.dbPath = os.path.join(self.localCacheDir, 'cache-{}.db'.format(self.workflowAttemptNumber))
        # We need to hold onto both a connection (to commit) and a cursor (to actually use the database)
        self.con = sqlite3.connect(self.dbPath)
        self.cur = self.con.cursor()
        
        # Note that sqlite3 automatically starts a transaction when we go to
        # modify the database.
        # To finish this transaction and let other people read our writes (or
        # write themselves), we need to COMMIT after every coherent set of
        # writes.

        # Make sure to register this as the current database, clobbering any previous attempts.
        # We need this for shutdown to be able to find the database from the most recent execution and clean up all its files.
        linkDir = tempfile.mkdtemp(dir=self.localCacheDir)
        linkName = os.path.join(linkDir, 'cache.db')
        os.link(self.dbPath, linkName)
        os.rename(linkName, os.path.join(self.localCacheDir, 'cache.db'))
        if os.path.exists(linkName):
            # TODO: How can this file exist if it got renamed away?
            os.unlink(linkName)
        os.rmdir(linkDir)
        assert(os.path.exists(os.path.join(self.localCacheDir, 'cache.db')))
        assert(os.stat(os.path.join(self.localCacheDir, 'cache.db')).st_ino == os.stat(self.dbPath).st_ino)

        # Set up the tables
        self._ensureTables(self.con)
        
        # Initialize the space accounting properties
        freeSpace, _ = getFileSystemSize(self.localCacheDir)
        self.cur.execute('INSERT OR IGNORE INTO properties VALUES (?, ?)', ('maxSpace', freeSpace))
        self.con.commit()

        # Space used by caching and by jobs is accounted with queries

        # We maintain an asynchronous upload thread, which gets kicked off when
        # we commit the job's completion. It will be None until then. When it
        # is running, it has exclusive control over our database connection,
        # because the job we exist for will have already completed. However, it
        # has to coordinate its activities with other CachingFileStore objects
        # in the same process (and thus sharing the same PID) and ensure that
        # only one of them is working on uploading any given file at any given
        # time.
        self.uploadThread = None

    @classmethod
    def _ensureTables(cls, con):
        """
        Ensure that the database tables we expect exist.
        """

        # Get a cursor
        cur = con.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id TEXT NOT NULL PRIMARY KEY, 
                path TEXT UNIQUE NOT NULL,
                size INT NOT NULL,
                state TEXT NOT NULL,
                owner INT 
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS refs (
                path TEXT NOT NULL,
                file_id TEXT NOT NULL,
                job_id TEXT NOT NULL,
                state TEXT NOT NULL,
                PRIMARY KEY (path, file_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT NOT NULL PRIMARY KEY,
                tempdir TEXT NOT NULL,
                disk INT NOT NULL,
                worker INT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS properties (
                name TEXT NOT NULL PRIMARY KEY,
                value INT NOT NULL
            )
        """)
        con.commit()
    
    # Caching-specific API

    def getCacheLimit(self):
        """
        Return the total number of bytes to which the cache is limited.

        If no limit is in place, return None.
        """

        for row in self.cur.execute('SELECT value FROM properties WHERE name = ?', ('maxSpace',)):
            return row[0]
        return None

    def getCacheUsed(self):
        """
        Return the total number of bytes used in the cache.

        If no value is available, return None.
        """

        # Space never counts as used if caching is free
        if self.cachingIsFree():
            return 0

        for row in self.cur.execute('SELECT TOTAL(size) FROM files'):
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
        for row in self.cur.execute("""
            SELECT (
                (SELECT TOTAL(disk) FROM jobs) - 
                (SELECT TOTAL(files.size) FROM refs INNER JOIN files ON refs.file_id = files.id WHERE refs.state == 'immutable')
            ) as result
        """):
            return row[0]
        return None

    def getCacheAvailable(self):
        """
        Return the total number of free bytes available for caching, or, if
        negative, the total number of bytes of cached files that need to be
        evicted to free up enough space for all the currently scheduled jobs.

        Returns None if not retrievable.
        """

        # Get the max space on our disk.
        # Subtract out the number of bytes of cached content.
        # Also subtract out the number of bytes of job disk requirements that
        # aren't being spent by those jobs on immutable references to cached
        # content.

        # Do a little report first
        for row in self.cur.execute("SELECT value FROM properties WHERE name = 'maxSpace'"):
            logger.info('Max space: %d', row[0])
        for row in self.cur.execute("SELECT TOTAL(size) FROM files"):
            logger.info('Total file size: %d', row[0])
        for row in self.cur.execute("SELECT TOTAL(disk) FROM jobs"):
            logger.info('Total job disk requirement size: %d', row[0])
        for row in self.cur.execute("SELECT TOTAL(files.size) FROM refs INNER JOIN files ON refs.file_id = files.id WHERE refs.state = 'immutable'"):
            logger.info('Total immutable reference size: %d', row[0])

        if self.cachingIsFree():
            # If caching is free, we just say that all the space is always available.
            for row in self.cur.execute("SELECT value FROM properties WHERE name = 'maxSpace'"):
                return row[0]
            return None
            

        for row in self.cur.execute("""
            SELECT (
                (SELECT value FROM properties WHERE name = 'maxSpace') -
                (SELECT TOTAL(size) FROM files) -
                ((SELECT TOTAL(disk) FROM jobs) - 
                (SELECT TOTAL(files.size) FROM refs INNER JOIN files ON refs.file_id = files.id WHERE refs.state = 'immutable'))
            ) as result
        """):
            return row[0]
        return None

    def getSpaceUsableForJobs(self):
        """
        Return the total number of bytes that are not taken up by job requirements, ignoring files and file usage.
        We can't ever run more jobs than we actually have room for, even with caching.

        Returns None if not retrievable.
        """

        for row in self.cur.execute("""
            SELECT (
                (SELECT value FROM properties WHERE name = 'maxSpace') -
                (SELECT TOTAL(disk) FROM jobs)
            ) as result
        """):
            return row[0]
        return None

    def getCacheUnusedJobRequirement(self):
        """
        Return the total number of bytes of disk space requested by the current
        job and not used by files the job is using in the cache.

        Mutable references don't count, but immutable/uploading ones do.

        If no value is available, return None.
        """

        logger.info('Get unused space for job %s', self.jobID)

        for row in self.cur.execute('SELECT * FROM files'):
            logger.info('File record: %s', str(row))

        for row in self.cur.execute('SELECT * FROM refs'):
            logger.info('Ref record: %s', str(row))


        for row in self.cur.execute('SELECT TOTAL(files.size) FROM refs INNER JOIN files ON refs.file_id = files.id WHERE refs.job_id = ? AND refs.state != ?',
            (self.jobID, 'mutable')):
            # Sum up all the sizes of our referenced files, then subtract that from how much we came in with    
            return self.jobDiskBytes - row[0]
        return None
    
    def adjustCacheLimit(self, newTotalBytes):
        """
        Adjust the total cache size limit to the given number of bytes.
        """

        self.cur.execute('UPDATE properties SET value = ? WHERE name = ?', (newTotalBytes, 'maxSpace'))
        self.con.commit()

    def fileIsCached(self, fileID):
        """
        Return true if the given file is currently cached, and false otherwise.
        
        Note that this can't really be relied upon because a file may go cached
        -> deleting after you look at it. If you need to do something with the
        file you need to do it in a transaction.
        """

        for row in self.cur.execute('SELECT COUNT(*) FROM files WHERE id = ? AND (state = ? OR state = ? OR state = ?)',
            (fileID, 'cached', 'uploadable', 'uploading')):
            
            return row[0] > 0
        return False

    def getFileReaderCount(self, fileID):
        """
        Return the number of current outstanding reads of the given file.

        Counts mutable references too.
        """

        for row in self.cur.execute('SELECT COUNT(*) FROM refs WHERE file_id = ?', (fileID,)):
            return row[0]
        return 0
        

    def cachingIsFree(self):
        """
        Return true if files can be cached for free, without taking up space.
        Return false otherwise.

        This will be true when working with certain job stores in certain
        configurations, most notably the FileJobStore.
        """

        for row in self.cur.execute('SELECT value FROM properties WHERE name = ?', ('freeCaching',)):
            return row[0] == 1

        # Otherwise we need to set it
        from toil.jobStores.fileJobStore import FileJobStore
        if isinstance(self.jobStore, FileJobStore) and not self.forceNonFreeCaching:
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
        self.cur.execute('INSERT OR IGNORE INTO properties VALUES (?, ?)', ('freeCaching', free))
        self.con.commit()

        # Return true if we said caching was free
        return free == 1


    # Internal caching logic

    def _getNewCachingPath(self, fileStoreID):
        """
        Get a path at which the given file ID can be cached.

        Will be unique for every call. 

        The file will not be created if it does not exist.
        """

        # Hash the file ID
        hasher = hashlib.sha1()
        hasher.update(fileStoreID.encode('utf-8'))

        # Get a unique temp file name, including the file ID's hash to make
        # sure we can never collide even though we are going to remove the
        # file.
        # TODO: use a de-slashed version of the ID instead?
        handle, path = tempfile.mkstemp(dir=self.localCacheDir, suffix=hasher.hexdigest())
        os.close(handle)
        os.unlink(path)

        return path
    
    def _stealWorkFromTheDead(self):
        """
        Take ownership of any files we can see whose owners have died.
        
        We don't actually process them here. We take action based on the states of files we own later.
        """
        
        pid = os.getpid()
        
        # Get a list of all file owner processes on this node.
        # Exclude NULL because it comes out as 0 and we can't look for PID 0.
        owners = []
        for row in self.cur.execute('SELECT DISTINCT owner FROM files WHERE owner IS NOT NULL'):
            owners.append(row[0])
            
        # Work out which of them have died.
        # TODO: use GUIDs or something to account for PID re-use?
        deadOwners = []
        for owner in owners:
            if not self._pidExists(owner):
                deadOwners.append(owner)

        for owner in deadOwners:
            # Try and adopt all the files that any dead owner had
            
            # If they were deleting, we delete
            self.cur.execute('UPDATE files SET owner = ?, state = ? WHERE owner = ? AND state = ?',
                (pid, 'deleting', owner, 'deleting'))
            # If they were downloading, we delete. Any outstanding references
            # can't be in use since they are from the dead downloader. 
            self.cur.execute('UPDATE files SET owner = ?, state = ? WHERE owner = ? AND state = ?',
                (pid, 'deleting', owner, 'downloading'))
            # If they were uploading or uploadable, we mark as cached even
            # though it never made it to the job store (and leave it unowned).
            #
            # Once the dead job that it was being uploaded from is cleaned up,
            # and there are no longer any immutable references, it will be
            # evicted as normal. Since the dead job can't have been marked
            # successfully completed (since the file is still not uploaded),
            # nobody is allowed to actually try and use the file.
            #
            # TODO: if we ever let other PIDs be responsible for writing our
            # files asynchronously, this will need to change.
            self.cur.execute('UPDATE files SET owner = NULL, state = ? WHERE owner = ? AND (state = ? OR state = ?)',
                ('cached', owner, 'uploadable', 'uploading'))
            self.con.commit()

            logger.info('Tried to adopt file operations from dead worker %d', owner)
            
    def _executePendingDeletions(self):
        """
        Delete all the files that are registered in the database as in the
        process of being deleted by us.
        
        Returns the number of files that were deleted.
        """
        
        pid = os.getpid()
        
        # Remember the file IDs we are deleting
        deletedFiles = []
        for row in self.cur.execute('SELECT id, path FROM files WHERE owner = ? AND state = ?', (pid, 'deleting')):
            # Grab everything we are supposed to delete and delete it
            fileID = row[0]
            filePath = row[1]
            try:
                os.unlink(filePath)
            except OSError:
                # Probably already deleted
                continue

            deletedFiles.append(fileID)

        for fileID in deletedFiles:
            # Drop all the files. They should have stayed in deleting state. We move them from there to not present at all.
            self.cur.execute('DELETE FROM files WHERE id = ? AND state = ?', (fileID, 'deleting'))
            # Also drop their references, if they had any from dead downloaders.
            self.cur.execute('DELETE FROM refs WHERE file_id = ?', (fileID,))
        self.con.commit()

        return len(deletedFiles)

    def _executePendingUploads(self):
        """
        Uploads all files in uploadable state that we own.

        Returns the number of files that were uploaded.
        """

        # Work out who we are
        pid = os.getpid()
        
        # Record how many files we upload
        uploadedCount = 0
        while True:
            # Try and find a file we might want to upload
            fileID = None
            filePath = None
            for row in self.cur.execute('SELECT id, path FROM files WHERE state = ? AND owner = ? LIMIT 1', ('uploadable', pid)):
                fileID = row[0]
                filePath = row[1]

            if fileID is None:
                # Nothing else exists to upload
                break

            # We need to set it to uploading in a way that we can detect that *we* won the update race instead of anyone else.
            self.cur.execute('UPDATE files SET state = ? WHERE id = ? AND state = ?', ('uploading', fileID, 'uploadable'))
            self.con.commit()
            if self.cur.rowcount != 1:
                # We didn't manage to update it. Someone else (a running job if
                # we are a committing thread, or visa versa) must have grabbed
                # it.
                logger.info('Lost race to upload %s', fileID)
                # Try again to see if there is something else to grab.
                continue

            # Upload the file
            logger.info('Actually executing upload for file %s', fileID)
            self.jobStore.updateFile(fileID, filePath)

            # Count it for the total uploaded files value we need to return
            uploadedCount += 1

            # Remember that we uploaded it in the database
            self.cur.execute('UPDATE files SET state = ?, owner = NULL WHERE id = ?', ('cached', fileID))
            self.con.commit()

        return uploadedCount

            
    
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

        # Put an entry in the database for this job being run on this worker.
        # This will take up space for us and potentially make the cache over-full.
        # But we won't actually let the job run and use any of this space until
        # the cache has been successfully cleared out.
        pid = os.getpid()
        self.cur.execute('INSERT INTO jobs VALUES (?, ?, ?, ?)', (self.jobID, self.localTempDir, newJobReqs, pid))
        self.con.commit()

        # Now we need to make sure that we can fit all currently cached files,
        # and the parts of the total job requirements not currently spent on
        # cached files, in under the total disk space limit.

        available = self.getCacheAvailable()

        logger.info('Available space with job: %d bytes', available)

        if available >= 0:
            # We're fine on disk space
            return

        # Otherwise we need to clear stuff.
        self._freeUpSpace()

    @classmethod
    def _removeJob(cls, con, cur, jobID):
        """
        Get rid of the job with the given ID.
        The job must be owned by us.
        
        Deletes the job's database entry, all its references, and its whole
        temporary directory.

        :param sqlite3.Connection con: Connection to the cache database.
        :param sqlite3.Cursor cur: Cursor in the cache database.
        :param str jobID: Hash-based ID of the job being removed. Not a Toil JobStore ID.
        """

        # Get the job's temp dir
        for row in cur.execute('SELECT tempdir FROM jobs WHERE id = ?', (jobID,)):
            jobTemp = row[0]

        for row in cur.execute('SELECT path FROM refs WHERE job_id = ?', (jobID,)):
            try:
                # Delete all the reference files.
                os.unlink(row[0])
            except OSError:
                # May not exist
                pass
        # And their database entries
        cur.execute('DELETE FROM refs WHERE job_id = ?', (jobID,))
        con.commit()

        try: 
            # Delete the job's temp directory to the extent that we can.
            shutil.rmtree(jobTemp)
        except OSError:
            pass

        # Strike the job from the database
        cur.execute('DELETE FROM jobs WHERE id = ?', (jobID,))
        con.commit()

    def _deallocateSpaceForJob(self):
        """ 
        Our current job that was using oldJobReqs space has finished.

        We need to record that the job is no longer running, so its space not
        taken up by files in the cache will be free.

        """

        self._removeJob(self.con, self.cur, self.jobID) 

    def _tryToFreeUpSpace(self):
        """
        If disk space is overcommitted, try one round of collecting files to upload/download/delete/evict.
        Return whether we manage to get any space freed or not.
        """

        # First we want to make sure that dead jobs aren't holding
        # references to files and keeping them from looking unused.
        self._removeDeadJobs(self.con)
        
        # Adopt work from any dead workers
        self._stealWorkFromTheDead()
        
        if self._executePendingDeletions() > 0:
            # We actually had something to delete, which we deleted.
            # Maybe there is space now
            logger.info('Successfully executed pending deletions to free space')
            return True

        if self._executePendingUploads() > 0:
            # We had something to upload. Maybe it can be evicted now.
            logger.info('Successfully executed pending uploads to free space')
            return True

        # Otherwise, not enough files could be found in deleting state to solve our problem.
        # We need to put something into the deleting state.
        # TODO: give other people time to finish their in-progress
        # evictions before starting more, or we might evict everything as
        # soon as we hit the cache limit.
        
        # Find something that has no non-mutable references and is not already being deleted.
        self.cur.execute("""
            SELECT files.id FROM files WHERE files.state = 'cached' AND NOT EXISTS (
                SELECT NULL FROM refs WHERE refs.file_id = files.id AND refs.state != 'mutable'
            ) LIMIT 1
        """)
        row = self.cur.fetchone()
        if row is None:
            # Nothing can be evicted by us.
            # Someone else might be in the process of evicting something that will free up space for us too.
            # Or someone mught be uploading something and we have to wait for them to finish before it can be deleted.
            logger.info('Could not find anything to evict! Cannot free up space!')
            return False

        # Otherwise we found an eviction candidate.
        fileID = row[0]
        
        # Work out who we are
        pid = os.getpid()

        # Try and grab it for deletion, subject to the condition that nothing has started reading it
        self.cur.execute("""
            UPDATE files SET owner = ?, state = ? WHERE id = ? AND state = ? 
            AND owner IS NULL AND NOT EXISTS (
                SELECT NULL FROM refs WHERE refs.file_id = files.id AND refs.state != 'mutable'
            )
            """,
            (pid, 'deleting', fileID, 'cached'))
        self.con.commit()

        logger.info('Evicting file %s', fileID)
            
        # Whether we actually got it or not, try deleting everything we have to delete
        if self._executePendingDeletions() > 0:
            # We deleted something
            logger.info('Successfully executed pending deletions to free space')
            return True

    def _freeUpSpace(self):
        """
        If disk space is overcomitted, block and evict eligible things from the
        cache until it is no longer overcommitted.
        """

        availableSpace = self.getCacheAvailable()

        # Track how long we are willing to wait for cache space to free up without making progress evicting things before we give up.
        # This is the longes that we will wait for uploads and other deleters.
        patience = 10

        while availableSpace < 0:
            # While there isn't enough space for the thing we want
            logger.info('Cache is full (%d bytes free). Trying to free up space!', availableSpace)
            # Free up space. See if we made any progress
            progress = self._tryToFreeUpSpace()
            availableSpace = self.getCacheAvailable()

            if progress:
                # Reset our patience
                patience = 10
            else:
                # See if we've been oversubscribed.
                jobSpace = self.getSpaceUsableForJobs()
                if jobSpace < 0:
                    logger.critical('Jobs on this machine have oversubscribed our total available space (%d bytes)!', jobSpace)
                    raise CacheUnbalancedError
                else:
                    patience -= 1
                    if patience <= 0:
                        logger.critical('Waited implausibly long for active uploads and deletes.')
                        raise CacheUnbalancedError
                    else:
                        # Wait a bit and come back
                        time.sleep(2)

        logger.info('Cache has %d bytes free.', availableSpace)
            
            
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
        self._removeDeadJobs(self.con)
        # Get the requirements for the job.
        self.jobDiskBytes = job.disk

        logger.info('Actually running job (%s) with ID (%s) which wants %d of our %d bytes.',
            self.jobName, self.jobID, self.jobDiskBytes, self.getCacheLimit())

        # Register the current job as taking this much space, and evict files
        # from the cache to make room before letting the job run.
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

            # Record that our job is no longer using its space, and clean up
            # its temp dir and database entry. 
            self._deallocateSpaceForJob()

    def writeGlobalFile(self, localFileName, cleanup=False):
    
        # Work out the file itself
        absLocalFileName = self._resolveAbsoluteLocalPath(localFileName)
        
        # And get its size
        fileSize = os.stat(absLocalFileName).st_size
    
        # Work out who is making the file
        creatorID = self.jobGraph.jobStoreID
    
        # Create an empty file to get an ID.
        # TODO: this empty file could leak if we die now...
        fileID = self.jobStore.getEmptyFileStoreID(creatorID, cleanup)
    
        # Work out who we are
        pid = os.getpid()
        
        # Work out where the file ought to go in the cache
        cachePath = self._getNewCachingPath(fileID)
    
        # Create a file in uploadable state and a reference, in the same transaction.
        # Say the reference is an immutable reference
        self.cur.execute('INSERT INTO files VALUES (?, ?, ?, ?, ?)', (fileID, cachePath, fileSize, 'uploadable', pid))
        self.cur.execute('INSERT INTO refs VALUES (?, ?, ?, ?)', (absLocalFileName, fileID, creatorID, 'immutable'))
        self.con.commit()

        if absLocalFileName.startswith(self.localTempDir):
            # We should link into the cache, because the upload is coming from our local temp dir
            try:
                # Try and hardlink the file into the cache.
                # This can only fail if the system doesn't have hardlinks, or the
                # file we're trying to link to has too many hardlinks to it
                # already, or something.
                os.link(absLocalFileName, cachePath)

                linkedToCache = True

                logger.info('Linked file %s into cache at %s; deferring write to job store', localFileName, cachePath)
                
                # Don't do the upload now. Let it be deferred until later (when the job is committing).
            except OSError:
                # We couldn't make the link for some reason
                linkedToCache = False
        else:
            # The tests insist that if you are uploading a file from outside
            # the local temp dir, it should not be linked into the cache.
            linkedToCache = False


        if not linkedToCache:
            # If we can't do the link into the cache and upload from there, we
            # have to just upload right away.  We can't guarantee sufficient
            # space to make a full copy in the cache, if we aren't allowed to
            # take this copy away from the writer.

            # Change the reference to 'mutable', which it will be 
            self.cur.execute('UPDATE refs SET state = ? WHERE path = ?', ('mutable', absLocalFileName))
            # And drop the file altogether
            self.cur.execute('DELETE FROM files WHERE id = ?', (fileID,))
            self.con.commit()

            # Save the file to the job store right now
            logger.info('Actually executing upload for file %s', fileID)
            self.jobStore.updateFile(fileID, absLocalFileName)
        
        # Ship out the completed FileID object with its real size.
        return FileID.forPath(fileID, absLocalFileName)

    def readGlobalFile(self, fileStoreID, userPath=None, cache=True, mutable=False, symlink=False):
        if not isinstance(fileStoreID, FileID):
            # Don't let the user forge File IDs.
            raise TypeError('Received file ID not of type FileID: {}'.format(fileStoreID))

        if fileStoreID in self.filesToDelete:
            # File has already been deleted
            raise FileNotFoundError('Attempted to read deleted file: {}'.format(fileStoreID))
            
        if userPath is not None:
            # Validate the destination we got
            localFilePath = self._resolveAbsoluteLocalPath(userPath)
            if os.path.exists(localFilePath):
                raise RuntimeError(' File %s ' % localFilePath + ' exists. Cannot Overwrite.')
        else:
            # Make our own destination
            localFilePath = self.getLocalTempFileName()

        # Work out who we are
        pid = os.getpid()

        # And what job we are operating on behalf of
        readerID = self.jobGraph.jobStoreID

        if not cache:
            # We would like to read directly from the backing job store, since
            # we don't want to cache the result. However, we may be trying to
            # read a file that is 'uploadable' or 'uploading' and hasn't hit
            # the backing job store yet.

            # Try and make a 'copying' reference to such a file
            self.cur.execute('INSERT INTO refs SELECT ?, id, ?, ? FROM files WHERE id = ? AND (state = ? OR state = ?)',
                (localFilePath, readerID, 'copying', fileStoreID, 'uploadable', 'uploading'))
            self.con.commit()

            # See if we got it
            have_reference = False
            for row in self.cur.execute('SELECT COUNT(*) FROM refs WHERE path = ? and file_id = ?', (localFilePath, fileStoreID)):
                have_reference = row[0] > 0

            if have_reference:
                # If we succeed, copy the file. We know the job has space for it
                # because if we didn't do this we'd be getting a fresh copy from
                # the job store.

                # Find where the file is cached
                for row in self.cur.execute('SELECT path FROM files WHERE id = ?', (fileStoreID,)):
                    cachedPath = row[0]

                with open(localFilePath, 'wb') as outStream:
                    with open(cachedPath, 'rb') as inStream:
                        # Copy it
                        shutil.copyfileobj(inStream, outStream)

                # Change the reference to mutable
                self.cur.execute('UPDATE refs SET state = ? WHERE path = ? and file_id = ?', ('mutable', localFilePath, fileStoreID))
                self.con.commit()
            
            else:

                # If we fail, the file isn't cached here in 'uploadable' or
                # 'uploading' state, so that means it must actually be in the
                # backing job store, so we can get it from the backing job store.

                # Create a 'mutable' reference (even if we end up with a link)
                # so we can see this file in deleteLocalFile.
                self.cur.execute('INSERT INTO refs VALUES (?, ?, ?, ?)',
                    (localFilePath, fileStoreID, readerID, 'mutable'))
                self.con.commit()

                # Just read directly
                if mutable or self.forceNonFreeCaching:
                    # Always copy
                    with open(localFilePath, 'wb') as outStream:
                        with self.jobStore.readFileStream(fileStoreID) as inStream:
                            shutil.copyfileobj(inStream, outStream)
                else:
                    # Link or maybe copy
                    self.jobStore.readFile(fileStoreID, localFilePath, symlink=symlink)

            # Now we got the file, somehow.
            return localFilePath

        # Now we know to use the cache

        # Work out where to cache the file if it isn't cached already
        cachedPath = self._getNewCachingPath(fileStoreID)

        # This tracks if we are responsible for downloading this file
        own_download = False
        # This tracks if we have a reference to the file yet
        have_reference = False

        # Start a loop until we can do one of these
        while True:
            # Try and create a downloading entry if no entry exists
            logger.info('Trying to make file record for id %s', fileStoreID)
            self.cur.execute('INSERT OR IGNORE INTO files VALUES (?, ?, ?, ?, ?)',
                (fileStoreID, cachedPath, fileStoreID.size, 'downloading', pid))
            if not mutable:
                # Make sure to create a reference at the same time if it succeeds, to bill it against our job's space.
                # Don't create the mutable reference yet because we might not necessarily be able to clear that space.
                logger.info('Trying to make file reference to %s', fileStoreID)
                self.cur.execute('INSERT INTO refs SELECT ?, id, ?, ? FROM files WHERE id = ? AND state = ? AND owner = ?',
                    (localFilePath, readerID, 'immutable', fileStoreID, 'downloading', pid))
            self.con.commit()

            # See if we won the race
            for row in self.cur.execute('SELECT COUNT(*) FROM files WHERE id = ? AND state = ? AND owner = ?', (fileStoreID, 'downloading', pid)):
                own_download = row[0] > 0

            if own_download:
                # We are responsible for downloading the file
                logger.info('We are now responsible for downloading file %s', fileStoreID)
                if not mutable:
                    # And we have a reference already
                    have_reference = True
                break
            else:
                logger.info('Someone else is already responsible for file %s', fileStoreID)
                for row in self.cur.execute('SELECT * FROM files WHERE id = ?', (fileStoreID,)):
                    logger.info('File record: %s', str(row))
       
            # A record already existed for this file.
            # Try and create an immutable or copying reference to an entry that
            # is in 'cached' or 'uploadable' or 'uploading' state.
            # It might be uploading because *we* are supposed to be uploading it.
            logger.info('Trying to make reference to file %s', fileStoreID)
            self.cur.execute('INSERT INTO refs SELECT ?, id, ?, ? FROM files WHERE id = ? AND (state = ? OR state = ? OR state = ?)',
                (localFilePath, readerID, 'copying' if mutable else 'immutable', fileStoreID, 'cached', 'uploadable', 'uploading'))
            self.con.commit()

            # See if we got it
            for row in self.cur.execute('SELECT COUNT(*) FROM refs WHERE path = ? and file_id = ?', (localFilePath, fileStoreID)):
                have_reference = row[0] > 0

            if have_reference:
                # The file is cached and we can copy or link it
                logger.info('Obtained reference to file %s', fileStoreID)

                # Get the path it is actually at in the cache, instead of where we wanted to put it
                for row in self.cur.execute('SELECT path FROM files WHERE id = ?', (fileStoreID,)):
                    cachedPath = row[0]

                break
            else:
                logger.info('Could not obtain reference to file %s', fileStoreID)

            # If we didn't get one of those either, adopt and do work from dead workers and loop again.
            # We may have to wait for someone else's download to finish. If they die, we will notice.
            self._removeDeadJobs(self.con)
            self._stealWorkFromTheDead()
            self._executePendingDeletions()

        if own_download:
            # If we ended up with a downloading entry
            
            # Make sure we have space for this download.
            self._freeUpSpace()

            # Do the download into the cache.
            if self.forceNonFreeCaching:
                # Always copy
                with open(cachedPath, 'wb') as outStream:
                    with self.jobStore.readFileStream(fileStoreID) as inStream:
                        shutil.copyfileobj(inStream, outStream)
            else:
                # Link or maybe copy
                self.jobStore.readFile(fileStoreID, cachedPath, symlink=False)

            # Change state from downloading to cached
            self.cur.execute('UPDATE files SET state = ?, owner = NULL WHERE id = ?',
                ('cached', fileStoreID))
            self.con.commit()
            # We no longer own the download
            own_download = False

        if not mutable:
            # If we are wanting an immutable reference.
            # The reference should already exist.
            assert(have_reference)
            
            try:
                try:
                    # Try and make the hard link.
                    os.link(cachedPath, localFilePath)
                    # Now we're done
                    return localFilePath
                except OSError:
                    if symlink:
                        # Or symlink 
                        os.symlink(cachedPath, localFilePath)
                        # Now we're done
                        return localFilePath
                    else:
                        raise
            except OSError:
                # If we can't, change the reference to copying.
                self.cur.execute('UPDATE refs SET state = ? WHERE path = ?', ('copying', localFilePath))
                self.con.commit()

                # Decide to be mutable
                mutable = True

        # By now, we only have mutable reads to deal with
        assert(mutable)

        if not have_reference:
            # We might not yet have the reference for the mutable case. Create
            # it as a copying reference and consume more space.
            self.cur.execute('INSERT INTO refs VALUES (?, ?, ?, ?)',
                (localFilePath, fileStoreID, readerID, 'copying'))
            self.con.commit()
            have_reference = True

        while self.getCacheAvailable() < 0:
            # If the reference is (now) copying, see if we have used too much space.
            # If so, try to free up some space by deleting or uploading, but
            # don't loop forever if we can't get enough.
            self._tryToFreeUpSpace()

            if self.getCacheAvailable() >= 0:
                # We made room
                break

            # See if we have no other references and we can give away the file.
            # Change it to downloading owned by us if we can grab it.
            self.cur.execute("""
                UPDATE files SET files.owner = ?, files.state = ? WHERE files.id = ? AND files.state = ? 
                AND files.owner IS NULL AND NOT EXISTS (
                    SELECT NULL FROM refs WHERE refs.file_id = files.id AND refs.state != 'mutable'
                )
                """,
                (pid, 'downloading', fileStoreID, 'cached'))
            self.con.commit()

            # See if we got it
            for row in self.cur.execute('SELECT COUNT(*) FROM files WHERE id = ? AND state = ? AND owner = ?',
                (fileStoreID, 'downloading', pid)):
                own_download = row[0] > 0

            if own_download:
                # Now we have exclusive control of the cached copy of the file, so we can give it away.
                break

            # If we don't have space, and we couldn't make space, and we
            # couldn't get exclusive control of the file to give it away, we
            # need to wait for one of those people with references to the file
            # to finish and give it up.
            # TODO: work out if that will never happen somehow.

        # OK, now we either have space to make a copy, or control of the original to give it away.

        if own_download:
            # We are giving it away
            shutil.move(cachedPath, localFilePath)

            # Record that.
            self.cur.execute('UPDATE refs SET state = ? WHERE path = ?', ('mutable', localFilePath))
            self.cur.execute('DELETE FROM files WHERE id = ?', (fileStoreID,))
            self.con.commit()

            # Now we're done
            return localFilePath
        else:
            # We are copying it
            shutil.copyfile(cachedPath, localFilePath)
            self.cur.execute('UPDATE refs SET state = ? WHERE path = ?', ('mutable', localFilePath))
            self.con.commit()

            # Now we're done
            return localFilePath
    

    def readGlobalFileStream(self, fileStoreID):
        if not isinstance(fileStoreID, FileID):
            # Don't let the user forge File IDs.
            raise TypeError('Received file ID not of type FileID: {}'.format(fileStoreID))

        if fileStoreID in self.filesToDelete:
            # File has already been deleted
            raise FileNotFoundError('Attempted to read deleted file: {}'.format(fileStoreID))

        # TODO: can we fulfil this from the cache if the file is in the cache?
        # I think we can because if a job is keeping the file data on disk due to having it open, it must be paying for it itself.
        return self.jobStore.readFileStream(fileStoreID)

    def deleteLocalFile(self, fileStoreID):
        if not isinstance(fileStoreID, FileID):
            # Don't let the user forge File IDs.
            raise TypeError('Received file ID not of type FileID: {}'.format(fileStoreID))

        # What job are we operating as?
        jobID = self.jobID

        # What paths did we delete
        deleted = []
        # What's the first path, if any, that was missing? If we encounter a
        # missing ref file, we will raise an error about it and stop deleting
        # things.
        missingFile = None
        for row in self.cur.execute('SELECT path FROM refs WHERE file_id = ? AND job_id = ?', (fileStoreID, jobID)):
            # Delete all the files that are references to this cached file (even mutable copies)
            path = row[0]

            if path.startswith(self.localTempDir):
                # It is actually in the local temp dir where we are supposed to be deleting things
                try:
                    os.remove(path)
                except FileNotFoundError as err:
                    if err.errno != errno.ENOENT:
                        # Something else went wrong
                        raise
                        
                    # Otherwise, file is missing, but that's fine.
                    missingFile = path
                    break
                deleted.append(path)

        for path in deleted:
            # Drop the references
            self.cur.execute('DELETE FROM refs WHERE file_id = ? AND job_id = ? AND path = ?', (fileStoreID, jobID, path))
            self.con.commit()

        # Now space has been revoked from the cache because that job needs its space back.
        # That might result in stuff having to be evicted.
        self._freeUpSpace()

        if missingFile is not None:
            # Now throw an error about the file we couldn't find to delete, if
            # any. TODO: Only users who know to call deleteLocalFile will ever
            # see this.  We also should check at the end of the job to make
            # sure all the refs are intact.
            raise IllegalDeletionCacheError(missingFile)

    def deleteGlobalFile(self, fileStoreID):
        if not isinstance(fileStoreID, FileID):
            # Don't let the user forge File IDs.
            raise TypeError('Received file ID not of type FileID: {}'.format(fileStoreID))

        # Delete local copies for this job
        self.deleteLocalFile(fileStoreID)

        # Work out who we are
        pid = os.getpid()

        # Make sure nobody else has references to it
        for row in self.cur.execute('SELECT job_id FROM refs WHERE file_id = ? AND state != ?', (fileStoreID, 'mutable')):
            raise RuntimeError('Deleted file ID %s which is still in use by job %s' % (fileStoreID, row[0]))
        # TODO: should we just let other jobs and the cache keep the file until
        # it gets evicted, and only delete at the back end?

        # Pop the file into deleting state owned by us if it exists
        self.cur.execute('UPDATE files SET state = ?, owner = ? WHERE id = ?', ('deleting', pid, fileStoreID))
        self.con.commit()
            
        # Finish the delete if the file is present
        self._executePendingDeletions()

        # Add the file to the list of files to be deleted from the job store
        # once the run method completes.
        self.filesToDelete.add(fileStoreID)
        self.logToMaster('Added file with ID \'%s\' to the list of files to be' % fileStoreID +
                         ' globally deleted.', level=logging.DEBUG)

    def exportFile(self, jobStoreFileID, dstUrl):
        # First we need to make sure the file is actually in the job store if
        # we have it cached and need to upload it.

        # We don't have to worry about the case where a different process is
        # uploading it because we aren't supposed to have the ID from them
        # until they are done.

        # For safety and simplicity, we just execute all pending uploads now.
        self._executePendingUploads()

        # Then we let the job store export. TODO: let the export come from the
        # cache? How would we write the URL?
        self.jobStore.exportFile(jobStoreFileID, dstUrl)

    def waitForCommit(self):
        # We need to block on the upload thread.
        # We may be called even if commitCurrentJob is not called. In that
        # case, a new instance of this class should have been created by the
        # worker and ought to pick up all our work by PID via the database, and
        # this instance doesn't actually have to commit.

        if self.uploadThread is not None:
            self.uploadThread.join()
        
        return True

    def commitCurrentJob(self):
        # Start the commit thread
        self.uploadThread = threading.Thread(target=self.commitCurrentJobThread)
        self.uploadThread.start()
        
    def commitCurrentJobThread(self):
        """
        Run in a thread to actually commit the current job.
        """

        # Make sure the previous job is committed, if any
        if self.waitForPreviousCommit is not None:
            self.waitForPreviousCommit()

        try:
            # Reconnect to the database from this thread. The main thread
            # should no longer do anything other then waitForCommit, so this
            # will be safe. We need to do this because SQLite objects are tied
            # to a thread.
            self.con = sqlite3.connect(self.dbPath)
            self.cur = self.con.cursor()

            # Finish all uploads
            self._executePendingUploads()
            # Finish all deletions
            self._executePendingDeletions()

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
        # right database. So we rely on this hard link to the most recent
        # database.
        dbPath = os.path.join(dir_, 'cache.db')

        if os.path.exists(dbPath):
            try:
                # The database exists, see if we can open it
                con = sqlite3.connect(dbPath)
            except:
                # Probably someone deleted it.
                pass
            else:
                # We got a database connection

                # Create the tables if they don't exist so deletion of dead
                # jobs won't fail.
                cls._ensureTables(con)

                # Remove dead jobs
                cls._removeDeadJobs(con)

                con.close()
        
        # Delete the state DB and everything cached.
        shutil.rmtree(dir_)

    def __del__(self):
        """
        Cleanup function that is run when destroying the class instance that ensures that all the
        file writing threads exit.
        """
        pass

    @classmethod
    def _removeDeadJobs(cls, con):
        """
        Look at the state of all jobs registered in the database, and handle them
        (clean up the disk)

        :param sqlite3.Connection con: Connection to the cache database.
        """
    
        # Get a cursor
        cur = con.cursor()
        
        # Work out our PID for taking ownership of jobs
        pid = os.getpid()

        # Get all the dead worker PIDs
        workers = []
        for row in cur.execute('SELECT DISTINCT worker FROM jobs WHERE worker IS NOT NULL'):
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
            cur.execute('UPDATE jobs SET worker = NULL WHERE worker = ?', (deadWorker,))
            con.commit()
        if len(deadWorkers) > 0:
            logger.info('Reaped %d dead workers', len(deadWorkers))

        while True:
            # Find an unowned job.
            # Don't take all of them; other people could come along and want to help us with the other jobs.
            cur.execute('SELECT id FROM jobs WHERE worker IS NULL LIMIT 1')
            row = cur.fetchone()
            if row is None:
                # We cleaned up all the jobs
                break

            jobID = row[0]

            # Try to own this job
            cur.execute('UPDATE jobs SET worker = ? WHERE id = ? AND worker IS NULL', (pid, jobID))
            con.commit()

            # See if we won the race
            cur.execute('SELECT id, tempdir FROM jobs WHERE id = ? AND worker = ?', (jobID, pid))
            row = cur.fetchone()
            if row is None:
                # We didn't win the race. Try another one.
                continue

            # If we did win, delete the job and its files and temp dir
            cls._removeJob(con, cur, jobID)

            logger.info('Cleaned up orphanded job %s', jobID)

        # Now we have cleaned up all the jobs that belonged to dead workers that were dead when we entered this function.


