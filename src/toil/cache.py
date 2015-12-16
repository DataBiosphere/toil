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
import tempfile
import logging
import subprocess
import hashlib
import shutil
import time
from contextlib import contextmanager
from struct import pack, unpack
from fcntl import flock, LOCK_EX

logger = logging.getLogger( __name__ )



class Cache(object):
    '''
    This class is involved with all teh caching related activities used in toil
    '''
    def __init__(self, localCacheDir, defaultCache):
        '''
        :param: str localCacheDir: Location where cached files will reside
        :param: float defaultCache: value of config.defaultCache - The default amount of disk to use for caching
        '''
        self.localCacheDir = localCacheDir
        self.defaultCache = defaultCache
        self.cacheLockFile = os.path.join(localCacheDir, '.cacheLock')
        self.nlinkThreshold = 1
        self.linkedCacheFiles = {}
        self._setupCache()

    @contextmanager
    def cacheLock(self):
        '''
        This is a context manager to acquire a lock on the Lock file that will be used to prevent synchronous cache
        operations between workers.
        :yield: File descriptor for cache Lock file in r+ mode
        '''
        cacheLockFile = open(self.cacheLockFile, 'r+')
        try:
            flock(cacheLockFile, LOCK_EX)
            logger.debug("Obtained Cache Lock")
            # TODO Add logic for sanity checks on caching here
            yield cacheLockFile
        except IOError:
            logger.critical('Unable to acquire lock on ' + cacheLockFile.name)
            raise
        finally:
            cacheLockFile.close()
            logger.debug("Released Cache Lock")

    def _setupCache(self):
        '''
        Setup the cache based on the provided values for localCacheDir and defaultcache.
        :return: None
        '''
        try:
            os.mkdir(self.localCacheDir, 0755)
        except OSError as err:
            # If the error is not Errno 17 (file already exists), reraise the exception
            if err.errno != 17:
                raise
            sleepTime = 1
            while not os.path.exists(self.cacheLockFile):
                time.sleep(1)
                sleepTime+=1
                if sleepTime == 10:
                    raise RuntimeError('Something went terribly wrong with caching.')
        else:
            self._createCacheLockFile()

    def _createCacheLockFile(self):
        '''
        Create the availableCacheSize file to contain the free space available for caching if necessary
        :return: None
        '''
        # Create a temp file, modfiy it, then rename to the desired name.  This has to be a race condition because a new
        # worker can get assigned to a place where the Lock file exists and since os.rename silently replaces the file
        # if it is present, we will get incorrect caching values in the file if we go ahead without the if exists
        # statement.
        freeSpace = int(subprocess.check_output(['df', self.localCacheDir]).split('\n')[1].split()[3]) * 1024
        # If defaultCache is a fraction, then it's meant to be a percentage of the total
        # TODO
        if 0.0 < self.defaultCache <= 1.0:
             cacheSpace = freeSpace * self.defaultCache
        else:
            cacheSpace = self.defaultCache
        # If the user has told TOIL to use more space than exists, use 80% of all the free space
        if  cacheSpace > freeSpace:
            warn('Provided cache allotment > free space on disk.')
            cacheSpace = 0.8 * freeSpace
        with open(self.cacheLockFile, 'w') as fileHandle:
            fileHandle.write(pack('ddd', freeSpace, 0.0, 0.0))

    def hashCachedJSID(self, JobStoreFileID):
        '''
        Uses sha1 to hash the jobStoreFileID into a unique identifier to store the file as within cache
        :param jobStoreFileID: string representing a file name
        :return: str path to hashed file in localCacheDir
        '''
        outCachedFile = os.path.join(self.localCacheDir, hashlib.sha1(JobStoreFileID).hexdigest())
        return outCachedFile

    def addToCache(self, src, jobStoreFileID):
        '''
        Used to link a given file to the cache directory.
        :param src:
        :param jobStoreFileID:
        :return:
        '''
        with self.cacheLock() as cacheFile:
            cachedFile = self.hashCachedJSID(jobStoreFileID)
            # If they're on the same filesystem, hard link them
            if os.stat(self.localCacheDir).st_dev == os.stat(src).st_dev:
                os.link(src, cachedFile)
            else:
                shutil.copy(src, cachedFile)
            # Chmod to make file read only to try to prevent accidental user modification
            os.chmod(cachedFile, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    def cleanCache(self, newJobReqs):
        """
        Cleanup all files in the cache directory to ensure that at most cacheSize bytes are used for caching.
        :param float cacheSize: the total number of bytes of files allowed in the cache.
        """
        with self.cacheLock() as cacheFile:
            # Read the values from the cache lock file
            totalFreeSpace, totaltalCachedSpace, sigmaJobDisk = unpack('ddd', cacheFile.readLine())
            # Add the new job's disk requirements to the sigmaJobDisk variable
            sigmaJobDisk += newJobReqs

            # The inequality of cachedSpace + sigmaJobDisk <= totalFreeSpace is met, do nothing.  Essentially, if the
            # sum of all cached jobs + disk requirements of all running jobs is less than the available space on the
            # system, then cache eviction is not required.
            if totalCachedSpace + sigmaJobDisk <= totalFreeSpace:
                # Update the file to the latest values first!
                _cacheWrite(pack('ddd', totalFreeSpace, totalCachedSpace, sigmaJobDisk), cacheFile)
                return None

            #  List of deletable cached files.  A deletable cache file is one
            #  that is not in use by any other worker (identified by the number of symlinks to the file)
            allCacheFiles = [os.path.join(self.localCacheDir, x) for x in os.listdir(self.localCacheDir) if
                             not x.startswith('.')]
            deletableCacheFiles = set([(x, y.st_ctime, y.st_size) for x, y in [(z, os.stat(z)) for z in allCacheFiles]
                                       if y.st_nlink == self.nlinkThreshold])

            # Sort such that we will remove earliest created files first
            deletableCacheFiles = sorted(deletableCacheFiles, key=lambda x: x[1])

            #Now do the actual file removal
            while totalCachedSpace + sigmaJobDisk > totalFreeSpace and len(deletableCacheFiles) > 0:
                cachedFile, fileCreateTime, cachedFileSize = deletableCacheFiles.pop()
                os.remove(cachedFile)
                totalCachedSpace -= cachedFileSize
                assert totalCachedSpace >= 0
            assert totalCachedSpace + sigmaJobDisk <= totalFreeSpace, 'Unable to free up enough space for caching.'
            _cacheWrite(pack('ddd', totalFreeSpace, totalCachedSpace, sigmaJobDisk), cacheFile)

    def _cacheWrite(self, writeString, cacheFile):
        '''
        Writes a wtring to the cache lock file after flushing it.
        :param str writeString: String to write to the cache lock file
        :param file cacheFile: Open file handle to a writeable file
        :return: None
        '''
        cacheFile.seek(0)
        cacheFile.truncate()
        cacheFile.write(writeString)