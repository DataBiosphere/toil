from __future__ import absolute_import

from contextlib import contextmanager
import errno
import logging
import os
import shutil

from bd2k.util.exceptions import require

from toil.lib.bioio import absSymPath
from toil.jobStores.abstractJobStore import (
                                             NoSuchFileException,
                                             JobStoreExistsException,
                                             )

logger = logging.getLogger( __name__ )

class FileStore(object):
    def __init__(self, path):
        """
        :param str path: Path to directory holding the job store
        """
        self.jobStoreDir = absSymPath(path)
        logger.debug("Path to job store directory is '%s'.", self.jobStoreDir)

    def initialize(self, config):
        try:
            os.mkdir(self.jobStoreDir)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise JobStoreExistsException(self.jobStoreDir)
            else:
                raise
        logger.debug('initialized')

    def resume(self):
        if not os.path.exists(self.jobStoreDir):
            raise NoSuchJobStoreException(self.jobStoreDir)
        require( os.path.isdir, "'%s' is not a directory", self.jobStoreDir)
        logger.debug("Resuming...")

    def destroy(self):
        if os.path.exists(self.jobStoreDir):
            shutil.rmtree(self.jobStoreDir)

    ##########################################
    # Function that deal with files
    ##########################################

    def writeFile(self, localFilePath, fileID):
        absPath = self._getAbsPath(fileID)
        shutil.copyfile(localFilePath, absPath)

    def readFile(self, fileID, localFilePath):
        jobStoreFilePath = self._getAbsPath(fileID)
        localDirPath = os.path.dirname(localFilePath)
        # If local file would end up on same file system as the one hosting this job store ...
        if os.stat(jobStoreFilePath).st_dev == os.stat(localDirPath).st_dev:
            # ... we can hard-link the file, ...
            try:
                os.link(jobStoreFilePath, localFilePath)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    # Overwrite existing file, emulating shutil.copyfile().
                    os.unlink(localFilePath)
                    # It would be very unlikely to fail again for same reason but possible
                    # nonetheless in which case we should just give up.
                    os.link(jobStoreFilePath, localFilePath)
                else:
                    raise
        else:
            # ... otherwise we have to copy it.
            shutil.copyfile(jobStoreFilePath, localFilePath)

    def deleteFile(self, fileID):
        if not self.fileExists(fileID):
            return

        os.remove(self._getAbsPath(fileID))

    ##########################################
    # Function that deal with file streams
    ##########################################

    @contextmanager
    def readFileStream(self, fileID):
        with open(self._getAbsPath(fileID), 'r') as f:
            yield f

    @contextmanager
    def writeFileStream(self, fileID):
        absPath = self._getAbsPath(fileID)
        self.__check_and_mkdir(os.path.dirname(absPath))
        with open(absPath, 'w') as f:
            yield f

    ##########################################
    # Functions that deal with file URLs
    ##########################################

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() == 'file'

    @classmethod
    def _readFromUrl(cls, url, writable):
        with open(cls._extractPathFromUrl(url), 'r') as f:
            writable.write(f.read())

    @classmethod
    def _writeToUrl(cls, readable, url):
        with open(cls._extractPathFromUrl(url), 'w') as f:
            f.write(readable.read())

    def getPublicUrl(self, fileID):
        absPath = self._getAbsPath(fileID)
        if os.path.exists(absPath):
            return 'file://' + self._getAbsPath(fileID)
        else:
            raise NoSuchFileException(absPath)

    ##########################################
    # Private methods
    ##########################################

    def _getAbsPath(self, relativePath):
        """
        :rtype : string, string is the absolute path to a file path relative
        to the self.jobStoreDir.
        """
        return os.path.join(self.jobStoreDir, relativePath)

    @staticmethod
    def _extractPathFromUrl(url):
        """
        :return: local file path of file pointed at by the given URL
        """
        if url.netloc != '' and url.netloc != 'localhost':
            raise RuntimeError("The URL '%s' is invalid" % url.geturl())
        return url.netloc + url.path

    @staticmethod
    def __check_and_mkdir(tempDir):
        if os.path.exists(tempDir): return

        try:
            os.mkdir(tempDir)
        except os.error:
            if not os.path.exists(tempDir): # In the case that a collision occurs and
                # it is created while we wait then we ignore
                raise
