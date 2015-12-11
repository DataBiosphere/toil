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
from contextlib import contextmanager
from fcntl import flock, LOCK_EX

logger = logging.getLogger( __name__ )



class Cache(object):
    '''
    This class is involved with all teh caching related activities used in toil
    '''
    def __init__(self, localCacheDir, defaultCache):
        '''
        :param: localCacheDir: Location where cached files will reside
        :param: defaultCache: value of config.defaultCache - The default amount of disk to use for caching
        '''
        self.localCacheDir = localCacheDir
        self.defaultCache = defaultCache
        self.cacheLockFile = os.path.join(localCacheDir, '.availableCachingDisk')

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
            yield cacheLockFile
        except IOError:
            logger.critical('Unable to acquire lock on ' + cacheLockFile.name)
            raise
        finally:
            cacheLockFile.close()
            logger.debug("Released Cache Lock")

    def createCacheDir(self):
        '''
        Attempt to create the cache directory at localCacheDir
        :return: None
        '''
        try:
            os.mkdir(self.localCacheDir, 0755)
        except OSError as err:
            # If the error is not Errno 17 (file already exists), reraise the exception
            if err.errno != 17:
                raise

    def createCacheLockFile(self):
        '''
        Create the availableCacheSize file to contain the free space available for caching if necessary
        :return: None
        '''
        # Create a temp file, modfiy it, then rename to the desired name.  This has to be a race condition because a new
        # worker can get assigned to a place where the Lock file exists and since os.rename silently replaces the file
        # if it is present, we will get incorrect caching values in the file if we go ahead without the if exists
        # statement.
        handle, tmpFile = tempfile.mkstemp(dir=self.localCacheDir)
        os.close(handle)
        # If defaultCache is a fraction, then it's meant to be a percentage of the total
        if 0.0 < self.defaultCache <= 1.0:
            freeSpace = int(subprocess.check_output(['df', self.localCacheDir]).split('\n')[1].split()[3]) * 1024 * \
                            self.defaultCache
        else:
            freeSpace = self.defaultCache
        with open(tmpFile, 'w') as fileHandle:
            fileHandle.write(str(freeSpace))
        os.rename(tmpFile, self.localCacheDir)

    def hashCachedJSID(self, JobStoreFileID):
        '''
        Uses sha1 to hash the jobStoreFileID into a unique identifier to store the file as within cache
        :param jobStoreFileID: string representing a file name
        :return: path to hashed file in localCacheDir
        '''
        outCachedFile = os.path.join(self.localCacheDir, hashlib.sha1(JobStoreFileID).hexdigest())
        return outCachedFile

    def linkToCache(self, src, jobStoreFileID):
        '''
        Used to link a given file to the cache directory.
        :param src:
        :param jobStoreFileID:
        :return:
        '''
        cachedFile = self.hashCachedJSID(jobStoreFileID)
        os.link(src, cachedFile)
        #Chmod to make file read only to try to prevent accidental user modification
        os.chmod(cachedFile, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
