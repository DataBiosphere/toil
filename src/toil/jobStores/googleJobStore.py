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
import logging
import os
import pickle
import stat
import time
import uuid
from contextlib import contextmanager
from functools import wraps
from io import BytesIO

from google.api_core.exceptions import (GoogleAPICallError,
                                        InternalServerError,
                                        ServiceUnavailable)
from google.cloud import exceptions, storage

from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             JobStoreExistsException,
                                             NoSuchFileException,
                                             NoSuchJobException,
                                             NoSuchJobStoreException)
from toil.jobStores.utils import ReadablePipe, WritablePipe
from toil.lib.compatibility import compat_bytes
from toil.lib.io import AtomicFileCreate
from toil.lib.misc import truncExpBackoff
from toil.lib.retry import old_retry

log = logging.getLogger(__name__)

GOOGLE_STORAGE = 'gs'

MAX_BATCH_SIZE = 1000

# TODO
#   - needed to run 'gsutil config' to get 'gs_oauth2_refresh_token' in the boto file
#   - needed to copy client_id and client_secret to the oauth section
# - Azure uses bz2 compression with pickling. Is this useful here?
# - better way to assign job ids? - currently 'job'+uuid


def googleRetryPredicate(e):
    """
    necessary because under heavy load google may throw
        TooManyRequests: 429
        The project exceeded the rate limit for creating and deleting buckets.

    or numerous other server errors which need to be retried.
    """
    if isinstance(e, GoogleAPICallError) and e.code == 429:
        return True
    if isinstance(e, InternalServerError) or isinstance(e, ServiceUnavailable):
        return True
    return False


def googleRetry(f):
    """
    This decorator retries the wrapped function if google throws any angry service
    errors.

    It should wrap any function that makes use of the Google Client API
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        for attempt in old_retry(delays=truncExpBackoff(),
                                 timeout=300,
                                 predicate=googleRetryPredicate):
            with attempt:
                return f(*args, **kwargs)
    return wrapper


class GoogleJobStore(AbstractJobStore):

    nodeServiceAccountJson = '/root/service_account.json'
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

        # this is a :class:`~google.cloud.storage.bucket.Bucket`
        self.bucket = None

        self.statsBaseID = 'f16eef0c-b597-4b8b-9b0c-4d605b4f506c'
        self.statsReadPrefix = '_'
        self.readStatsBaseID = self.statsReadPrefix+self.statsBaseID

        self.sseKey = None

        # Determine if we have an override environment variable for our credentials.
        # We don't pull out the filename; we just see if a name is there.
        self.credentialsFromEnvironment = bool(os.getenv('GOOGLE_APPLICATION_CREDENTIALS', False))

        if self.credentialsFromEnvironment and not os.path.exists(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')):
            # If the file is missing, complain.
            # This variable holds a file name and not any sensitive data itself.
            log.warning("File '%s' from GOOGLE_APPLICATION_CREDENTIALS is unavailable! "
                        "We may not be able to authenticate!",
                        os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))

        if not self.credentialsFromEnvironment and os.path.exists(self.nodeServiceAccountJson):
            # load credentials from a particular file on GCE nodes if an override path is not set
            self.storageClient = storage.Client.from_service_account_json(self.nodeServiceAccountJson)
        else:
            # Either a filename is specified, or our fallback file isn't there.
            # See if Google can work out how to authenticate.
            self.storageClient = storage.Client()


    @googleRetry
    def initialize(self, config=None):
        try:
            self.bucket = self.storageClient.create_bucket(self.bucketName)
        except exceptions.Conflict:
            raise JobStoreExistsException(self.locator)
        super(GoogleJobStore, self).initialize(config)

        # set up sever side encryption after we set up config in super
        if self.config.sseKey is not None:
            with open(self.config.sseKey, 'rb') as f:
                self.sseKey = compat_bytes(f.read())
                assert len(self.sseKey) == 32

    @googleRetry
    def resume(self):
        try:
            self.bucket = self.storageClient.get_bucket(self.bucketName)
        except exceptions.NotFound:
            raise NoSuchJobStoreException(self.locator)
        super(GoogleJobStore, self).resume()

    @googleRetry
    def destroy(self):
        try:
            self.bucket = self.storageClient.get_bucket(self.bucketName)

        except exceptions.NotFound:
            # just return if not connect to physical storage. Needed for idempotency
            return
        
        try:
            self.bucket.delete(force=True)
            # throws ValueError if bucket has more than 256 objects. Then we must delete manually
        except ValueError:
            # use google batching to delete. Improved efficiency compared to deleting sequentially 
            blobs_to_delete = self.buckets.list_blobs()
            count = 0
            while count < len(blobs_to_delete):
                with self.storageClient.batch():
                    for blob in blobs_to_delete[count:count + MAX_BATCH_SIZE]:
                        blob.delete()
                    count = count + MAX_BATCH_SIZE
            self.bucket.delete()

    def _newJobID(self):
        return "job"+str(uuid.uuid4())

    def assignID(self, jobDescription):
        jobStoreID = self._newJobID()
        log.debug("Assigning ID to job %s for '%s'",
                  jobStoreID, '<no command>' if jobDescription.command is None else jobDescription.command)
        jobDescription.jobStoreID = jobStoreID

    @contextmanager
    def batch(self):
        # not implemented, google could storage does not support batching for uploading or downloading (2021)
        yield
        

    def create(self, jobDescription):
        jobDescription.pre_update_hook()
        self._writeBytes(jobDescription.jobStoreID, pickle.dumps(jobDescription, protocol=pickle.HIGHEST_PROTOCOL))
        return jobDescription

    @googleRetry
    def exists(self, jobStoreID):
        return self.bucket.blob(compat_bytes(jobStoreID), encryption_key=self.sseKey).exists()

    @googleRetry
    def getPublicUrl(self, fileName):
        blob = self.bucket.get_blob(compat_bytes(fileName), encryption_key=self.sseKey)
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
        job = pickle.loads(jobString)
        # It is our responsibility to make sure that the JobDescription is
        # connected to the current config on this machine, for filling in
        # defaults. The leader and worker should never see config-less
        # JobDescriptions.
        job.assignConfig(self.config)
        return job

    def update(self, job):
        job.pre_update_hook()
        self._writeBytes(job.jobStoreID, pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL), update=True)

    @googleRetry
    def delete(self, jobStoreID):
        self._delete(jobStoreID)

        # best effort delete associated files
        for blob in self.bucket.list_blobs(prefix=compat_bytes(jobStoreID)):
            self._delete(blob.name)

    def getEnv(self):
        """
        Return a dict of environment variables to send out to the workers
        so they can load the job store.
        """

        env = {}

        if self.credentialsFromEnvironment:
            # Send along the environment variable that points to the credentials file.
            # It must be available in the same place on all nodes.
            env['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

        return env

    @googleRetry
    def jobs(self):
        for blob in self.bucket.list_blobs(prefix=b'job'):
            jobStoreID = blob.name
            if len(jobStoreID) == 39:  # 'job' + uuid length
                yield self.load(jobStoreID)

    def writeFile(self, localFilePath, jobStoreID=None, cleanup=False):
        fileID = self._newID(isFile=True, jobStoreID=jobStoreID if cleanup else None)
        with open(localFilePath, 'rb') as f:
            self._writeFile(fileID, f)
        return fileID

    @contextmanager
    def writeFileStream(self, jobStoreID=None, cleanup=False, basename=None, encoding=None, errors=None):
        fileID = self._newID(isFile=True, jobStoreID=jobStoreID if cleanup else None)
        with self._uploadStream(fileID, update=False, encoding=encoding, errors=errors) as writable:
            yield writable, fileID

    def getEmptyFileStoreID(self, jobStoreID=None, cleanup=False, basename=None):
        fileID = self._newID(isFile=True, jobStoreID=jobStoreID if cleanup else None)
        self._writeFile(fileID, BytesIO(b""))
        return fileID

    @googleRetry
    def readFile(self, jobStoreFileID, localFilePath, symlink=False):
        # used on non-shared files which will be encrypted if available
        # checking for JobStoreID existence
        if not self.fileExists(jobStoreFileID):
            raise NoSuchFileException(jobStoreFileID)
        with AtomicFileCreate(localFilePath) as tmpPath:
            with open(tmpPath, 'wb') as writeable:
                blob = self.bucket.get_blob(compat_bytes(jobStoreFileID), encryption_key=self.sseKey)
                blob.download_to_file(writeable)
        if getattr(jobStoreFileID, 'executable', False):
            os.chmod(localFilePath, os.stat(localFilePath).st_mode | stat.S_IXUSR)

    @contextmanager
    def readFileStream(self, jobStoreFileID, encoding=None, errors=None):
        with self.readSharedFileStream(jobStoreFileID, isProtected=True, encoding=encoding,
                            errors=errors) as readable:
            yield readable

    def deleteFile(self, jobStoreFileID):
        self._delete(jobStoreFileID)

    @googleRetry
    def fileExists(self, jobStoreFileID):
        return self.bucket.blob(compat_bytes(jobStoreFileID), encryption_key=self.sseKey).exists()

    @googleRetry
    def getFileSize(self, jobStoreFileID):
        if not self.fileExists(jobStoreFileID):
            return 0
        return self.bucket.get_blob(compat_bytes(jobStoreFileID), encryption_key=self.sseKey).size

    def updateFile(self, jobStoreFileID, localFilePath):
        with open(localFilePath, 'rb') as f:
            self._writeFile(jobStoreFileID, f, update=True)

    @contextmanager
    def updateFileStream(self, jobStoreFileID, encoding=None, errors=None):
        with self._uploadStream(jobStoreFileID, update=True, encoding=encoding, errors=errors) as writable:
            yield writable

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=True, encoding=None, errors=None):
        with self._uploadStream(sharedFileName, encrypt=isProtected, update=True, encoding=encoding,
                                    errors=errors) as writable:
            yield writable

    @contextmanager
    def readSharedFileStream(self, sharedFileName, isProtected=True, encoding=None, errors=None):
        with self._downloadStream(sharedFileName, encrypt=isProtected, encoding=encoding, errors=errors) as readable:
            yield readable

    @classmethod
    @googleRetry
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
        bucketName = url.netloc
        fileName = url.path

        # remove leading '/', which can cause problems if fileName is a path
        if fileName.startswith('/'):
            fileName = fileName[1:]

        storageClient = storage.Client()
        bucket = storageClient.get_bucket(bucketName)
        blob = bucket.blob(compat_bytes(fileName))

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
        return blob.size, False

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() == 'gs'

    @classmethod
    def _writeToUrl(cls, readable: bytes, url: str, executable: bool = False) -> None:
        blob = cls._getBlobFromURL(url)
        blob.upload_from_file(readable)

    def writeStatsAndLogging(self, statsAndLoggingString: bytes) -> None:
        statsID = self.statsBaseID + str(uuid.uuid4())
        log.debug("Writing stats file: %s", statsID)
        with self._uploadStream(statsID, encrypt=False, update=False) as f:
            f.write(statsAndLoggingString)

    @googleRetry
    def readStatsAndLogging(self, callback, readAll=False):
        prefix = self.readStatsBaseID if readAll else self.statsBaseID
        filesRead = 0
        lastTry = False

        while True:
            filesReadThisLoop = 0
            # prefix seems broken
            for blob in self.bucket.list_blobs(prefix=compat_bytes(prefix)):
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
                        self.bucket.rename_blob(blob, compat_bytes(newID))
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

    @googleRetry
    def _delete(self, jobStoreFileID):
        if self.fileExists(jobStoreFileID):
            self.bucket.get_blob(compat_bytes(jobStoreFileID)).delete()
        # remember, this is supposed to be idempotent, so we don't do anything
        # if the file doesn't exist

    @googleRetry
    def _readContents(self, jobStoreID):
        """
        To be used on files representing jobs only. Which will be encrypted if possible.
        :param jobStoreID: the ID of the job
        :type jobStoreID: str
        :return: contents of the job file
        :rtype: string
        """
        job = self.bucket.get_blob(compat_bytes(jobStoreID), encryption_key=self.sseKey)
        if job is None:
            raise NoSuchJobException(jobStoreID)
        return job.download_as_string()

    @googleRetry
    def _writeFile(self, jobStoreID: str, fileObj: bytes, update=False, encrypt=True) -> None:
        blob = self.bucket.blob(compat_bytes(jobStoreID), encryption_key=self.sseKey if encrypt else None)
        if not update:
            # TODO: should probably raise a special exception and be added to all jobStores
            assert not blob.exists()
        else:
            if not blob.exists():
                raise NoSuchFileException(jobStoreID)
        blob.upload_from_file(fileObj)

    def _writeBytes(self, jobStoreID: str, stringToUpload: bytes, **kwarg) -> None:
        self._writeFile(jobStoreID, BytesIO(stringToUpload), **kwarg)

    @contextmanager
    @googleRetry
    def _uploadStream(self, fileName, update=False, encrypt=True, encoding=None, errors=None):
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

        :param str encoding: the name of the encoding used to encode the file. Encodings are the same
                as for encode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.

        :return: an instance of WritablePipe.
        :rtype: :class:`~toil.jobStores.utils.writablePipe`
        """
        blob = self.bucket.blob(compat_bytes(fileName), encryption_key=self.sseKey if encrypt else None)
        class UploadPipe(WritablePipe):
            def readFrom(self, readable):
                if not update:
                    assert not blob.exists()
                blob.upload_from_file(readable)

        with UploadPipe(encoding=encoding, errors=errors) as writable:
            yield writable

    @contextmanager
    @googleRetry
    def _downloadStream(self, fileName, encrypt=True, encoding=None, errors=None):
        """
        Yields a context manager that can be used to read from the bucket
        with a stream. See :class:`~toil.jobStores.utils.WritablePipe` for an example.

        :param fileName: name of file in bucket to be read
        :type fileName: str

        :param encrypt: whether or not the file is encrypted
        :type encrypt: bool

        :param str encoding: the name of the encoding used to encode the file. Encodings are the same
                as for encode(). Defaults to None which represents binary mode.

        :param str errors: an optional string that specifies how encoding errors are to be handled. Errors
                are the same as for open(). Defaults to 'strict' when an encoding is specified.

        :return: an instance of ReadablePipe.
        :rtype: :class:`~toil.jobStores.utils.ReadablePipe`
        """

        blob = self.bucket.get_blob(compat_bytes(fileName), encryption_key=self.sseKey if encrypt else None)
        if blob is None:
            raise NoSuchFileException(fileName)

        class DownloadPipe(ReadablePipe):
            def writeTo(self, writable):
                try:
                    blob.download_to_file(writable)
                finally:
                    writable.close()

        with DownloadPipe(encoding=encoding, errors=errors) as readable:
            yield readable
