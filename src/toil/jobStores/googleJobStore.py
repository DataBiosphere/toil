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
from typing import List, Optional
from urllib.parse import ParseResult

from google.api_core.exceptions import (GoogleAPICallError,
                                        InternalServerError,
                                        ServiceUnavailable)
from google.cloud import exceptions, storage
from google.auth.exceptions import DefaultCredentialsError

from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             JobStoreExistsException,
                                             NoSuchFileException,
                                             NoSuchJobException,
                                             NoSuchJobStoreException)

from toil.fileStores import FileID

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


def google_retry_predicate(e):
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


def google_retry(f):
    """
    This decorator retries the wrapped function if google throws any angry service
    errors.

    It should wrap any function that makes use of the Google Client API
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        for attempt in old_retry(delays=truncExpBackoff(),
                                 timeout=300,
                                 predicate=google_retry_predicate):
            with attempt:
                return f(*args, **kwargs)
    return wrapper


class GoogleJobStore(AbstractJobStore):

    nodeServiceAccountJson = '/root/service_account.json'
    def __init__(self, locator: str) -> None:
        super().__init__(locator)

        try:
            projectID, namePrefix = locator.split(":", 1)
        except ValueError:
            # we don't have a specified projectID
            namePrefix = locator
            projectID = None

        self.projectID = projectID
        self.bucketName = namePrefix+"--toil"
        log.debug("Instantiating google jobStore with name: %s", self.bucketName)

        # this is a :class:`~google.cloud.storage.bucket.Bucket`
        self.bucket = None

        self.statsBaseID = 'f16eef0c-b597-4b8b-9b0c-4d605b4f506c'
        self.statsReadPrefix = '_'
        self.readStatsBaseID = self.statsReadPrefix+self.statsBaseID

        self.sseKey = None
        self.storageClient = self.create_client()


    @classmethod
    def create_client(cls) -> storage.Client:
        """
        Produce a client for Google Sotrage with the highest level of access we can get.

        Fall back to anonymous access if no project is available, unlike the
        Google Storage module's behavior.

        Warn if GOOGLE_APPLICATION_CREDENTIALS is set but not actually present.
        """

        # Determine if we have an override environment variable for our credentials.
        # We get the path to check existence, but Google Storage works out what
        # to use later by looking at the environment again.
        credentials_path: Optional[str] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)
        if credentials_path is not None and not os.path.exists(credentials_path):
            # If the file is missing, complain.
            # This variable holds a file name and not any sensitive data itself.
            log.warning("File '%s' from GOOGLE_APPLICATION_CREDENTIALS is unavailable! "
                        "We may not be able to authenticate!",
                        credentials_path)

        if credentials_path is None and os.path.exists(cls.nodeServiceAccountJson):
            try:
                # load credentials from a particular file on GCE nodes if an override path is not set
                return storage.Client.from_service_account_json(cls.nodeServiceAccountJson)
            except OSError:
                # Probably we don't have permission to use the file.
                log.warning("File '%s' exists but didn't work to authenticate!",
                            cls.nodeServiceAccountJson)
                pass

        # Either a filename is specified, or our fallback file isn't there.
        try:
            # See if Google can work out how to authenticate.
            return storage.Client()
        except (DefaultCredentialsError, EnvironmentError):
            # Depending on which Google codepath or module version (???)
            # realizes we have no credentials, we can get an EnvironemntError,
            # or the new DefaultCredentialsError we are supposedly specced to
            # get.

            # Google can't find credentials, fall back to being anonymous.
            # This is likely to happen all the time so don't warn.
            return storage.Client.create_anonymous_client()


    @google_retry
    def initialize(self, config=None):
        try:
            self.bucket = self.storageClient.create_bucket(self.bucketName)
        except exceptions.Conflict:
            raise JobStoreExistsException(self.locator)
        super().initialize(config)

        # set up sever side encryption after we set up config in super
        if self.config.sseKey is not None:
            with open(self.config.sseKey, 'rb') as f:
                self.sseKey = compat_bytes(f.read())
                assert len(self.sseKey) == 32

    @google_retry
    def resume(self):
        try:
            self.bucket = self.storageClient.get_bucket(self.bucketName)
        except exceptions.NotFound:
            raise NoSuchJobStoreException(self.locator)
        super().resume()

    @google_retry
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
            blobs_to_delete = self.bucket.list_blobs()
            count = 0
            while count < len(blobs_to_delete):
                with self.storageClient.batch():
                    for blob in blobs_to_delete[count:count + MAX_BATCH_SIZE]:
                        blob.delete()
                    count = count + MAX_BATCH_SIZE
            self.bucket.delete()

    def _new_job_id(self):
        return f'job-{uuid.uuid4()}'

    def assign_job_id(self, job_description):
        jobStoreID = self._new_job_id()
        log.debug("Assigning ID to job %s for '%s'",
                  jobStoreID, '<no command>' if job_description.command is None else job_description.command)
        job_description.jobStoreID = jobStoreID

    @contextmanager
    def batch(self):
        # not implemented, google could storage does not support batching for uploading or downloading (2021)
        yield

    def create_job(self, job_description):
        job_description.pre_update_hook()
        self._write_bytes(job_description.jobStoreID, pickle.dumps(job_description, protocol=pickle.HIGHEST_PROTOCOL))
        return job_description

    @google_retry
    def job_exists(self, job_id):
        return self.bucket.blob(compat_bytes(job_id), encryption_key=self.sseKey).exists()

    @google_retry
    def get_public_url(self, fileName):
        blob = self.bucket.get_blob(compat_bytes(fileName), encryption_key=self.sseKey)
        if blob is None:
            raise NoSuchFileException(fileName)
        return blob.generate_signed_url(self.publicUrlExpiration)

    def get_shared_public_url(self, sharedFileName):
        return self.get_public_url(sharedFileName)

    def load_job(self, job_id):
        try:
            jobString = self._read_contents(job_id)
        except NoSuchFileException:
            raise NoSuchJobException(job_id)
        job = pickle.loads(jobString)
        # It is our responsibility to make sure that the JobDescription is
        # connected to the current config on this machine, for filling in
        # defaults. The leader and worker should never see config-less
        # JobDescriptions.
        job.assignConfig(self.config)
        return job

    def update_job(self, job):
        job.pre_update_hook()
        self._write_bytes(job.jobStoreID, pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL), update=True)

    @google_retry
    def delete_job(self, job_id):
        self._delete(job_id)

        # best effort delete associated files
        for blob in self.bucket.list_blobs(prefix=compat_bytes(job_id)):
            self._delete(blob.name)

    def get_env(self):
        """
        Return a dict of environment variables to send out to the workers
        so they can load the job store.
        """

        env = {}

        credentials_path: Optional[str] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)
        if credentials_path is not None:
            # Send along the environment variable that points to the credentials file.
            # It must be available in the same place on all nodes.
            env['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

        return env

    @google_retry
    def jobs(self):
        for blob in self.bucket.list_blobs(prefix=b'job-'):
            jobStoreID = blob.name
            # TODO: do this better
            if len(jobStoreID) == 40 and jobStoreID.startswith('job-'):  # 'job-' + uuid length
                yield self.load_job(jobStoreID)

    def write_file(self, local_path, job_id=None, cleanup=False):
        fileID = self._new_id(isFile=True, jobStoreID=job_id if cleanup else None)
        with open(local_path, 'rb') as f:
            self._write_file(fileID, f)
        return fileID

    @contextmanager
    def write_file_stream(self, job_id=None, cleanup=False, basename=None, encoding=None, errors=None):
        fileID = self._new_id(isFile=True, jobStoreID=job_id if cleanup else None)
        with self._upload_stream(fileID, update=False, encoding=encoding, errors=errors) as writable:
            yield writable, fileID

    def get_empty_file_store_id(self, jobStoreID=None, cleanup=False, basename=None):
        fileID = self._new_id(isFile=True, jobStoreID=jobStoreID if cleanup else None)
        self._write_file(fileID, BytesIO(b""))
        return fileID

    @google_retry
    def read_file(self, file_id, local_path, symlink=False):
        # used on non-shared files which will be encrypted if available
        # checking for JobStoreID existence
        if not self.file_exists(file_id):
            raise NoSuchFileException(file_id)
        with AtomicFileCreate(local_path) as tmpPath:
            with open(tmpPath, 'wb') as writeable:
                blob = self.bucket.get_blob(compat_bytes(file_id), encryption_key=self.sseKey)
                blob.download_to_file(writeable)
        if getattr(file_id, 'executable', False):
            os.chmod(local_path, os.stat(local_path).st_mode | stat.S_IXUSR)

    @contextmanager
    def read_file_stream(self, file_id, encoding=None, errors=None):
        with self.read_shared_file_stream(file_id, isProtected=True, encoding=encoding,
                                          errors=errors) as readable:
            yield readable

    def delete_file(self, file_id):
        self._delete(file_id)

    @google_retry
    def file_exists(self, file_id):
        return self.bucket.blob(compat_bytes(file_id), encryption_key=self.sseKey).exists()

    @google_retry
    def get_file_size(self, file_id):
        if not self.file_exists(file_id):
            return 0
        return self.bucket.get_blob(compat_bytes(file_id), encryption_key=self.sseKey).size

    def update_file(self, file_id, local_path):
        with open(local_path, 'rb') as f:
            self._write_file(file_id, f, update=True)

    @contextmanager
    def update_file_stream(self, file_id, encoding=None, errors=None):
        with self._upload_stream(file_id, update=True, encoding=encoding, errors=errors) as writable:
            yield writable

    @contextmanager
    def write_shared_file_stream(self, shared_file_name, encrypted=True, encoding=None, errors=None):
        with self._upload_stream(shared_file_name, encrypt=encrypted, update=True, encoding=encoding,
                                 errors=errors) as writable:
            yield writable

    @contextmanager
    def read_shared_file_stream(self, shared_file_name, isProtected=True, encoding=None, errors=None):
        with self._download_stream(shared_file_name, encrypt=isProtected, encoding=encoding, errors=errors) as readable:
            yield readable

    @classmethod
    @google_retry
    def _get_blob_from_url(cls, url, exists=False):
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

        storageClient = cls.create_client()
        bucket = storageClient.bucket(bucket_name=bucketName)
        blob = bucket.blob(compat_bytes(fileName))

        if exists:
            if not blob.exists():
                raise NoSuchFileException
            # sync with cloud so info like size is available
            blob.reload()
        return blob

    @classmethod
    def get_size(cls, url):
        return cls._get_blob_from_url(url, exists=True).size

    @classmethod
    def _read_from_url(cls, url, writable):
        blob = cls._get_blob_from_url(url, exists=True)
        blob.download_to_file(writable)
        return blob.size, False

    @classmethod
    def _supports_url(cls, url, export=False):
        return url.scheme.lower() == 'gs'

    @classmethod
    def _write_to_url(cls, readable: bytes, url: str, executable: bool = False) -> None:
        blob = cls._get_blob_from_url(url)
        blob.upload_from_file(readable)

    @classmethod
    def _list_url(cls, url: ParseResult) -> List[str]:
        raise NotImplementedError("Listing files in Google buckets is not yet implemented!")

    @classmethod
    def _get_is_directory(cls, url: ParseResult) -> bool:
        raise NotImplementedError("Checking directory status in Google buckets is not yet implemented!")

    @google_retry
    def write_logs(self, msg: bytes) -> None:
        statsID = self.statsBaseID + str(uuid.uuid4())
        log.debug("Writing stats file: %s", statsID)
        with self._upload_stream(statsID, encrypt=False, update=False) as f:
            if isinstance(msg, str):
                f.write(msg.encode("utf-8"))
            else:
                f.write(msg)

    @google_retry
    def read_logs(self, callback, read_all=False):
        prefix = self.readStatsBaseID if read_all else self.statsBaseID
        filesRead = 0
        lastTry = False

        while True:
            filesReadThisLoop = 0
            # prefix seems broken
            for blob in self.bucket.list_blobs(prefix=compat_bytes(prefix)):
                try:
                    with self.read_shared_file_stream(blob.name) as readable:
                        log.debug("Reading stats file: %s", blob.name)
                        callback(readable)
                        filesReadThisLoop += 1
                    if not read_all:
                        # rename this file by copying it and deleting the old version to avoid
                        # rereading it
                        newID = self.readStatsBaseID + blob.name[len(self.statsBaseID):]
                        # NOTE: just copies then deletes old.
                        self.bucket.rename_blob(blob, compat_bytes(newID))
                except NoSuchFileException:
                    log.debug("Stats file not found: %s", blob.name)
            if read_all:
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
    def _new_id(isFile=False, jobStoreID=None):
        if isFile and jobStoreID:  # file associated with job
            return jobStoreID+str(uuid.uuid4())
        elif isFile:  # nonassociated file
            return str(uuid.uuid4())
        else:  # job id
            return f'job-{uuid.uuid4()}'

    @google_retry
    def _delete(self, jobStoreFileID):
        if self.file_exists(jobStoreFileID):
            self.bucket.get_blob(compat_bytes(jobStoreFileID)).delete()
        # remember, this is supposed to be idempotent, so we don't do anything
        # if the file doesn't exist

    @google_retry
    def _read_contents(self, jobStoreID):
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

    @google_retry
    def _write_file(self, jobStoreID: str, fileObj: bytes, update=False, encrypt=True) -> None:
        blob = self.bucket.blob(compat_bytes(jobStoreID), encryption_key=self.sseKey if encrypt else None)
        if not update:
            # TODO: should probably raise a special exception and be added to all jobStores
            assert not blob.exists()
        else:
            if not blob.exists():
                raise NoSuchFileException(jobStoreID)
        blob.upload_from_file(fileObj)

    def _write_bytes(self, jobStoreID: str, stringToUpload: bytes, **kwarg) -> None:
        self._write_file(jobStoreID, BytesIO(stringToUpload), **kwarg)

    @contextmanager
    @google_retry
    def _upload_stream(self, fileName, update=False, encrypt=True, encoding=None, errors=None):
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
                if readable.seekable():
                    blob.upload_from_file(readable)
                else:
                    blob.upload_from_string(readable.read())

        with UploadPipe(encoding=encoding, errors=errors) as writable:
            yield writable

    @contextmanager
    @google_retry
    def _download_stream(self, fileName, encrypt=True, encoding=None, errors=None):
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
