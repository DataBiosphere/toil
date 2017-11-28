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

from __future__ import absolute_import

from future import standard_library
standard_library.install_aliases()
from builtins import str
import base64
from contextlib import contextmanager
import hashlib
import uuid
import logging
import time
from google.cloud import storage, exceptions

try:
    import cPickle as pickle
except ImportError:
    import pickle

# Python 3 compatibility imports
from six.moves import StringIO

from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             NoSuchFileException, NoSuchJobStoreException,
                                             JobStoreExistsException,
                                             ConcurrentFileModificationException)
from toil.jobStores.utils import WritablePipe, ReadablePipe
from toil.jobGraph import JobGraph
log = logging.getLogger(__name__)

GOOGLE_STORAGE = 'gs'


# TODO
# - Update gcs_oauth2_boto_plugin.
# - Check consistency.
# - update GCE instructions
#   - needed to run 'gsutil config' to get 'gs_oauth2_refresh_token' in the boto file
#   - needed to copy client_id and client_secret to the oauth section
# - Azure uses bz2 compression with pickling. Is this useful here?
# - boto3? http://boto.cloudhackers.com/en/latest/ref/gs.html
# - better way to assign job ids? - currently 'job'+uuid

class GoogleJobStore(AbstractJobStore):

    # BOTO WILL UPDATE HEADERS WITHOUT COPYING THEM FIRST. To enforce immutability & prevent
    # this, we use getters that return copies of our original dictionaries. reported:
    # https://github.com/boto/boto/issues/3517
    @property
    def encryptedHeaders(self):
        return self._encryptedHeaders.copy()

    @encryptedHeaders.setter
    def encryptedHeaders(self, value):
        self._encryptedHeaders = value

    @property
    def headerValues(self):
        return self._headerValues.copy()

    @headerValues.setter
    def headerValues(self, value):
        self._headerValues = value

    def __init__(self, locator):
        super(GoogleJobStore, self).__init__()

        try:
            projectID, namePrefix = locator.split(":", 1)
        except ValueError:
            # we don't have a specified projectID
            namePrefix = locator
            projectID = None

        self.locator = locator
        self.projectID = projectID
        self.bucketName = namePrefix+"--toil"
        log.debug("Instantiating google jobStore with name: %s", self.bucketName)

        import gcs_oauth2_boto_plugin # needed to import authentication handler
        self.bucket = None

        self.statsBaseID = 'f16eef0c-b597-4b8b-9b0c-4d605b4f506c'
        self.statsReadPrefix = '_'
        self.readStatsBaseID = self.statsReadPrefix+self.statsBaseID

    def initialize(self, config=None):
        storageClient = storage.Client()
        try:
            self.bucket = storageClient.create_bucket(self.bucketName)
        except exceptions.Conflict:
            raise JobStoreExistsException(self.locator)
        super(GoogleJobStore, self).initialize(config)

    def resume(self):
        storage_client = storage.Client()
        try:
            self.bucket = storage_client.get_bucket(self.bucketName)
        except exceptions.NotFound:
            raise NoSuchJobStoreException(self.locator)
        super(GoogleJobStore, self).resume()

    @property
    def sseKeyPath(self):
        return self.config.sseKey

    def destroy(self):
        # just return if not connect to physical storage. Needed for idempotency
        if self.bucket is None:
            return

        try:
            self.bucket.delete(force=True)
            # throws ValueError if bucket has more than 256 objects. Then we must delete manually
        except ValueError:
            self.bucket.delete_blobs(self.bucket.list_blobs)
            self.bucket.delete()
            # if ^ throws a google.cloud.exceptions.Conflict, then we should have a deletion retry mechanism.

        # google freaks out if we call delete multiple times on the bucket obj, so after success
        # just set to None.
        self.bucket = None


    def create(self, jobNode):
        jobStoreID = self._newJobID()
        log.debug("Creating job %s for '%s'",
                  jobStoreID, '<no command>' if jobNode.command is None else jobNode.command)
        job = JobGraph.fromJobNode(jobNode, jobStoreID=jobStoreID, tryCount=self._defaultTryCount())
        if hasattr(self, "_batchedJobGraphs") and self._batchedJobGraphs is not None:
            self._batchedJobGraphs.append(job)
        else:
            self._writeString(jobStoreID, pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL)) #UPDATE: bz2.compress(
        return job

    def _newJobID(self):
        return "job"+str(uuid.uuid4())

    def exists(self, jobStoreID):
        return self.bucket.blob(bytes(jobStoreID)).exists()

    def getPublicUrl(self, fileName):
        blob = self.bucket.get_blob(bytes(fileName))
        if blob is None:
            raise NoSuchFileException(fileName)
        return blob.generate_signed_url(self.publicUrlExpiration)

    def getSharedPublicUrl(self, sharedFileName):
        return self.getPublicUrl(sharedFileName)

    def load(self, jobStoreID):
        try:
            jobString = self._readContents(jobStoreID)
        except NoSuchFileException:
            raise NoSuchJobException(jobStoreID)
        return pickle.loads(jobString) #UPDATE bz2.decompress(

    def update(self, job):
        self._writeString(job.jobStoreID, pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL), update=True)

    def delete(self, jobStoreID):
        # jobs will always be encrypted when avaliable
        self._delete(jobStoreID, encrypt=True)

        # best effort delete associated files
        for blob in self.bucket.list_blobs(prefix=bytes(jobStoreID)):
            self._delete(blob.name)

    def jobs(self):
        for blob in self.bucket.list_blobs(prefix=b'job'):
            jobStoreID = blob.name
            if len(jobStoreID) == 39:  # 'job' + uuid length
                yield self.load(jobStoreID)

    def writeFile(self, localFilePath, jobStoreID=None):
        fileID = self._newID(isFile=True, jobStoreID=jobStoreID)
        with open(localFilePath) as f:
            self._writeFile(fileID, f)
        return fileID

    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        fileID = self._newID(isFile=True, jobStoreID=jobStoreID)
        with self._uploadStream(fileID, update=True) as writable:
            yield writable, fileID

    def getEmptyFileStoreID(self, jobStoreID=None):
        fileID = self._newID(isFile=True, jobStoreID=jobStoreID)
        self._writeFile(fileID, StringIO(""))
        return fileID

    def readFile(self, jobStoreFileID, localFilePath):
        # used on non-shared files which will be encrypted if available
        # TODO deal with encryption stuff
        # checking for JobStoreID existence
        if not self.fileExists(jobStoreFileID):
            raise NoSuchFileException(jobStoreFileID)
        with open(localFilePath, 'w') as writeable:
            self.bucket.get_blob(bytes(jobStoreFileID)).download_to_file(writeable)

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        with self.readSharedFileStream(jobStoreFileID, isProtected=True) as readable:
            yield readable

    def deleteFile(self, jobStoreFileID):
        self._delete(jobStoreFileID)

    def fileExists(self, jobStoreFileID):
        return self.bucket.blob(bytes(jobStoreFileID)).exists()

    def updateFile(self, jobStoreFileID, localFilePath):
        with open(localFilePath) as f:
            self._writeFile(jobStoreFileID, f, update=True)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        with self._uploadStream(jobStoreFileID, update=True) as writable:
            yield writable

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=True):
        with self._uploadStream(sharedFileName, encrypt=isProtected, update=True) as writable:
            yield writable

    @contextmanager
    def readSharedFileStream(self, sharedFileName, isProtected=True):
        with self._downloadStream(sharedFileName, encrypt=isProtected) as readable:
            yield readable

    @classmethod
    def _getBlobFromURL(cls, url, exists=False):
        """
        Gets the blob specified by the url.

        caution: makes no api request. blob may not ACTUALLY exist

        :param urlparse.ParseResult url: the URL

        :param bool exists: if True, then syncs local blob object with cloud
        and raises exceptions if it doesn't exist remotely

        :return: the blob requested
        :rtype: :class:`~google.cloud.storage.blob.Blob`
        """
        # TODO: this needs to work with encryption
        bucketName = url.netloc
        fileName = url.path

        storageClient = storage.Client()
        bucket = storageClient.get_bucket(bucketName)
        blob = bucket.blob(bytes(fileName))

        if exists:
            if not blob.exists():
                raise NoSuchFileException
            # sync with cloud so info like size is available
            blob.reload()
        return blob

    @classmethod
    def getSize(cls, url):
        return cls._getBlobFromURL(url, exists=True).size

    @classmethod
    def _readFromUrl(cls, url, writable):
        blob = cls._getBlobFromURL(url, exists=True)
        blob.download_to_file(writable)

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() == 'gs'

    @classmethod
    def _writeToUrl(cls, readable, url):
        blob = cls._getBlobFromURL(url)
        blob.upload_from_file(readable)

    def writeStatsAndLogging(self, statsAndLoggingString):
        statsID = self.statsBaseID + str(uuid.uuid4())
        log.debug("Writing stats file: %s", statsID)
        with self._uploadStream(statsID, encrypt=False, update=False) as f:
            f.write(statsAndLoggingString)

    def readStatsAndLogging(self, callback, readAll=False):
        prefix = self.readStatsBaseID if readAll else self.statsBaseID
        filesRead = 0
        lastTry = False

        while True:
            filesReadThisLoop = 0
            # prefix seems broken
            for blob in self.bucket.list_blobs(prefix=bytes(prefix)):
                try:
                    with self.readSharedFileStream(blob.name) as readable:
                        log.debug("Reading stats file: %s", blob.name)
                        callback(readable)
                        filesReadThisLoop += 1
                    if not readAll:
                        # rename this file by copying it and deleting the old version to avoid
                        # rereading it
                        newID = self.readStatsBaseID + blob.name[len(self.statsBaseID):]
                        # NOTE: just copies then deletes old.
                        # TODO: abstract renaming for ultimate efficiency
                        self.bucket.rename_blob(blob, bytes(newID))
                except NoSuchFileException:
                    log.debug("Stats file not found: %s", blob.name)
            if readAll:
                # The readAll parameter is only by the toil stats util after the completion of the
                # pipeline. Assume that this means the bucket is in a consistent state when readAll
                # is passed.
                return filesReadThisLoop
            if filesReadThisLoop == 0:
                # Listing is unfortunately eventually consistent so we can't be 100% sure there
                # really aren't any stats files left to read
                if lastTry:
                    # this was our second try, we are reasonably sure there aren't any stats
                    # left to gather
                        break
                # Try one more time in a couple seconds
                time.sleep(5)
                lastTry = True
                continue
            else:
                lastTry = False
                filesRead += filesReadThisLoop

        return filesRead

    @staticmethod
    def _newID(isFile=False, jobStoreID=None):
        if isFile and jobStoreID:  # file associated with job
            return jobStoreID+str(uuid.uuid4())
        elif isFile:  # nonassociated file
            return str(uuid.uuid4())
        else:  # job id
            return "job"+str(uuid.uuid4())

    def _resolveEncryptionHeaders(self, config):
        sseKeyPath = config.sseKey
        if sseKeyPath is None:
            return {}
        else:
            with open(sseKeyPath) as f:
                sseKey = f.read()
            assert len(sseKey) == 32
            encodedSseKey = base64.b64encode(sseKey)
            encodedSseKeyMd5 = base64.b64encode(hashlib.sha256(sseKey).digest())
            return {'x-goog-encryption-algorithm': 'AES256',
                    'x-goog-encryption-key': encodedSseKey,
                    'x-goog-encryption-key-sha256': encodedSseKeyMd5,
                    "Cache-Control": "no-store"}

    def _delete(self, jobStoreFileID, encrypt=True):
        # TODO: deal with encryption
        if self.fileExists(jobStoreFileID):
            self.bucket.get_blob(bytes(jobStoreFileID)).delete()
        # remember, this is supposed to be idempotent, so we don't do anything
        # if the file doesn't exist

    # TODO: abstract and require implementation?
    def _readContents(self, jobStoreID):
        """
        To be used on files representing jobs only. Which will be encrypted if possible.
        :param jobStoreID: the ID of the job
        :type jobStoreID: str
        :return: contents of the job file
        :rtype: string
        """
        job = self.bucket.get_blob(bytes(jobStoreID))
        if job is None:
            raise NoSuchJobException(jobStoreID)
        return job.download_as_string()

    # TODO: abstract and require implementation?
    def _writeFile(self, jobStoreID, fileObj, update=False, encrypt=True):
        # TODO: add encryption stuff here
        blob = self.bucket.blob(bytes(jobStoreID))
        if not update:
            # TODO: should probably raise a special exception and be added to all jobStores
            assert not blob.exists()
        blob.upload_from_file(fileObj)

    # TODO: abstract?
    def _writeString(self, jobStoreID, stringToUpload, **kwarg):
        self._writeFile(jobStoreID, StringIO(stringToUpload), **kwarg)

    @contextmanager
    def _uploadStream(self, fileName, update=False, encrypt=True):
        """
        Yields a context manager that can be used to write to the bucket
        with a stream. See :class:`~toil.jobStores.utils.WritablePipe` for an example.

        Will throw assertion error if the file shouldn't be updated
        and yet exists.

        :param fileName: name of file to be inserted into bucket
        :type fileName: str
        :param update: whether or not the file is to be updated
        :type update: bool
        :param encrypt: whether or not the file is encrypted
        :type encrypt: bool
        :return: an instance of WritablePipe.
        :rtype: :class:`~toil.jobStores.utils.writablePipe`
        """
        blob = self.bucket.blob(bytes(fileName))

        class UploadPipe(WritablePipe):
            def readFrom(self, readable):
                if not update:
                    assert not blob.exists()
                blob.upload_from_file(readable)

        with UploadPipe() as writable:
            yield writable

    @contextmanager
    def _downloadStream(self, fileName, encrypt=True):
        """
        Yields a context manager that can be used to read from the bucket
        with a stream. See :class:`~toil.jobStores.utils.WritablePipe` for an example.

        :param fileName: name of file in bucket to be read
        :type fileName: str
        :param encrypt: whether or not the file is encrypted
        :type encrypt: bool
        :return: an instance of ReadablePipe.
        :rtype: :class:`~toil.jobStores.utils.ReadablePipe`
        """
        blob = self.bucket.get_blob(bytes(fileName))
        if blob is None:
            raise NoSuchFileException(fileName)

        class DownloadPipe(ReadablePipe):
            def writeTo(self, writable):
                # TODO encryption stuff here
                try:
                    blob.download_to_file(writable)
                finally:
                    writable.close()

        with DownloadPipe() as readable:
            yield readable
