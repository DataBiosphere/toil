import base64
from contextlib import contextmanager
import hashlib
import os
import uuid
from bd2k.util.threading import ExceptionalThread
import boto
import logging
import time

# Python 3 compatibility imports
from six.moves import cPickle, StringIO

from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             NoSuchFileException,
                                             ConcurrentFileModificationException)
from toil.jobStores.utils import WritablePipe, ReadablePipe
from toil.jobGraph import JobGraph

log = logging.getLogger(__name__)

GOOGLE_STORAGE = 'gs'


class GoogleJobStore(AbstractJobStore):

    @classmethod
    def initialize(cls, locator, config=None):
        try:
            projectID, namePrefix = locator.split(":", 1)
        except ValueError:
            # we don't have a specified projectID
            namePrefix = locator
            projectID = None
        return cls(namePrefix, projectID, config)

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

    def __init__(self, namePrefix, projectID=None, config=None):
        #  create 2 buckets
        self.projectID = projectID

        self.bucketName = namePrefix+"--toil"
        log.debug("Instantiating google jobStore with name: %s", self.bucketName)
        self.gsBucketURL = "gs://"+self.bucketName

        self._headerValues = {"x-goog-project-id": projectID} if projectID else {}
        self._encryptedHeaders = self.headerValues

        self.uri = boto.storage_uri(self.gsBucketURL, GOOGLE_STORAGE)
        self.files = None

        exists = True
        try:
            self.files = self.uri.get_bucket(headers=self.headerValues, validate=True)
        except boto.exception.GSResponseError:
            exists = False

        create = config is not None
        self._checkJobStoreCreation(create, exists, projectID+':'+namePrefix)

        if not exists:
            self.files = self._retryCreateBucket(self.uri, self.headerValues)

        super(GoogleJobStore, self).__init__(config=config)
        self.sseKeyPath = self.config.sseKey
        # functionally equivalent to dictionary1.update(dictionary2) but works with our immutable dicts
        self.encryptedHeaders = dict(self.encryptedHeaders, **self._resolveEncryptionHeaders())

        self.statsBaseID = 'f16eef0c-b597-4b8b-9b0c-4d605b4f506c'
        self.statsReadPrefix = '_'
        self.readStatsBaseID = self.statsReadPrefix+self.statsBaseID

    def destroy(self):
        # no upper time limit on this call keep trying delete calls until we succeed - we can
        # fail because of eventual consistency in 2 ways: 1) skipping unlisted objects in bucket
        # that are meant to be deleted 2) listing of ghost objects when trying to delete bucket
        while True:
            try:
                self.uri.delete_bucket()
            except boto.exception.GSResponseError as e:
                if e.status == 404:
                    return  # the bucket doesn't exist so we are done
                else:
                    # bucket could still have objects, or contain ghost objects
                    time.sleep(0.5)
            else:
                # we have succesfully deleted bucket
                return

            # object could have been deleted already
            for obj in self.files.list():
                try:
                    obj.delete()
                except boto.exception.GSResponseError:
                    pass

    def create(self, jobNode):
        jobStoreID = self._newID()
        job = JobGraph(jobStoreID=jobStoreID, unitName=jobNode.name, jobName=jobNode.job,
                       command=jobNode.command, remainingRetryCount=self._defaultTryCount(),
                       logJobStoreFileID=None, predecessorNumber=jobNode.predecessorNumber,
                       **jobNode._requirements)
        self._writeString(jobStoreID, cPickle.dumps(job, protocol=cPickle.HIGHEST_PROTOCOL))
        return job

    def exists(self, jobStoreID):
        # used on job files, which will be encrypted if avaliable
        headers = self.encryptedHeaders
        try:
            self._getKey(jobStoreID, headers)
        except NoSuchFileException:
            return False
        return True

    def getPublicUrl(self, fileName):
        try:
            key = self._getKey(fileName)
        except:
            raise NoSuchFileException(fileName)
        return key.generate_url(expires_in=self.publicUrlExpiration.total_seconds())

    def getSharedPublicUrl(self, sharedFileName):
        return self.getPublicUrl(sharedFileName)

    def load(self, jobStoreID):
        try:
            jobString = self._readContents(jobStoreID)
        except NoSuchFileException:
            raise NoSuchJobException(jobStoreID)
        return cPickle.loads(jobString)

    def update(self, job):
        self._writeString(job.jobStoreID, cPickle.dumps(job, protocol=cPickle.HIGHEST_PROTOCOL), update=True)

    def delete(self, jobStoreID):
        # jobs will always be encrypted when avaliable
        self._delete(jobStoreID, encrypt=True)

    def jobs(self):
        for key in self.files.list(prefix='job'):
            jobStoreID = key.name
            if len(jobStoreID) == 39:
                yield self.load(jobStoreID)

    def writeFile(self, localFilePath, jobStoreID=None):
        fileID = self._newID(isFile=True, jobStoreID=jobStoreID)
        with open(localFilePath) as f:
            self._writeFile(fileID, f)
        return fileID

    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        fileID = self._newID(isFile=True, jobStoreID=jobStoreID)
        key = self._newKey(fileID)
        with self._uploadStream(key, update=False) as writable:
            yield writable, key.name

    def getEmptyFileStoreID(self, jobStoreID=None):
        fileID = self._newID(isFile=True, jobStoreID=jobStoreID)
        self._writeFile(fileID, StringIO(""))
        return fileID

    def readFile(self, jobStoreFileID, localFilePath):
        # used on non-shared files which will be encrypted if avaliable
        headers = self.encryptedHeaders
        # checking for JobStoreID existance
        if not self.exists(jobStoreFileID):
            raise NoSuchFileException(jobStoreFileID)
        with open(localFilePath, 'w') as writeable:
            self._getKey(jobStoreFileID, headers).get_contents_to_file(writeable, headers=headers)

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        with self.readSharedFileStream(jobStoreFileID, isProtected=True) as readable:
            yield readable

    def deleteFile(self, jobStoreFileID):
        headers = self.encryptedHeaders
        try:
            self._getKey(jobStoreFileID, headers).delete(headers)
        except boto.exception.GSDataError as e:
            # we tried to delete unencrypted file with encryption headers. unfortunately,
            # we can't determine whether the file passed in is encrypted or not beforehand.
            if e.status == 400:
                headers = self.headerValues
                self._getKey(jobStoreFileID, headers).delete(headers)
            else:
                raise e

    def fileExists(self, jobStoreFileID):
        try:
            self._getKey(jobStoreFileID)
            return True
        except (NoSuchFileException, boto.exception.GSResponseError) as e:
            if isinstance(e, NoSuchFileException):
                return False
            elif e.status == 400:
                # will happen w/ self.fileExists(encryptedFile). If file didn't exist code == 404
                return True
            else:
                return False

    def updateFile(self, jobStoreFileID, localFilePath):
        with open(localFilePath) as f:
            self._writeFile(jobStoreFileID, f, update=True)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        headers = self.encryptedHeaders
        key = self._getKey(jobStoreFileID, headers)
        with self._uploadStream(key, update=True) as readable:
            yield readable

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=True):
        key = self._newKey(sharedFileName)
        with self._uploadStream(key, encrypt=isProtected, update=True) as readable:
            yield readable

    @contextmanager
    def readSharedFileStream(self, sharedFileName, isProtected=True):
        headers = self.encryptedHeaders if isProtected else self.headerValues
        key = self._getKey(sharedFileName, headers=headers)
        with self._downloadStream(key, encrypt=isProtected) as readable:
            yield readable

    @staticmethod
    def _getResources(url):
        projectID = url.host
        bucketAndKey = url.path
        return projectID, 'gs://'+bucketAndKey

    @classmethod
    def getSize(cls, url):
        projectID, uri = GoogleJobStore._getResources(url)
        uri = boto.storage_uri(uri, GOOGLE_STORAGE)
        return uri.get_key().size

    @classmethod
    def _readFromUrl(cls, url, writable):
        # gs://projectid/bucket/key
        projectID, uri = GoogleJobStore._getResources(url)
        uri = boto.storage_uri(uri, GOOGLE_STORAGE)
        headers = {"x-goog-project-id": projectID}
        uri.get_contents_to_file(writable, headers=headers)

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() == 'gs'

    @classmethod
    def _writeToUrl(cls, readable, url):
        projectID, uri = GoogleJobStore._getResources(url)
        uri = boto.storage_uri(uri, GOOGLE_STORAGE)
        headers = {"x-goog-project-id": projectID}
        uri.set_contents_from_file(readable, headers=headers)

    def writeStatsAndLogging(self, statsAndLoggingString):
        statsID = self.statsBaseID + str(uuid.uuid4())
        key = self._newKey(statsID)
        log.debug("Writing stats file: %s", key.name)
        with self._uploadStream(key, encrypt=False, update=False) as f:
            f.write(statsAndLoggingString)

    def readStatsAndLogging(self, callback, readAll=False):
        prefix = self.readStatsBaseID if readAll else self.statsBaseID
        filesRead = 0
        lastTry = False

        while True:
            filesReadThisLoop = 0
            for key in list(self.files.list(prefix=prefix)):
                try:
                    with self.readSharedFileStream(key.name) as readable:
                        log.debug("Reading stats file: %s", key.name)
                        callback(readable)
                        filesReadThisLoop += 1
                    if not readAll:
                        # rename this file by copying it and deleting the old version to avoid
                        # rereading it
                        newID = self.readStatsBaseID + key.name[len(self.statsBaseID):]
                        self.files.copy_key(newID, self.files.name, key.name)
                        key.delete()
                except NoSuchFileException:
                    log.debug("Stats file not found: %s", key.name)
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
    def _retryCreateBucket(uri, headers):
        # FIMXE: This should use retry from utils
        while True:
            try:
                # FIMXE: this leaks a connection on exceptions
                return uri.create_bucket(headers=headers)
            except boto.exception.GSResponseError as e:
                if e.status == 429:
                    time.sleep(10)
                else:
                    raise

    @staticmethod
    def _newID(isFile=False, jobStoreID=None):
        if isFile and jobStoreID:  # file associated with job
            return jobStoreID+str(uuid.uuid4())
        elif isFile:  # nonassociated file
            return str(uuid.uuid4())
        else:  # job id
            return "job"+str(uuid.uuid4())

    def _resolveEncryptionHeaders(self):
        sseKeyPath = self.sseKeyPath
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

    def _delete(self, jobStoreID, encrypt=True):
        headers = self.encryptedHeaders if encrypt else self.headerValues
        try:
            key = self._getKey(jobStoreID, headers)
        except NoSuchFileException:
            pass
        else:
            try:
                key.delete()
            except boto.exception.GSResponseError as e:
                if e.status == 404:
                    pass
        # best effort delete associated files
        for fileID in self.files.list(prefix=jobStoreID):
            try:
                self.deleteFile(fileID)
            except NoSuchFileException:
                pass

    def _getKey(self, jobStoreID=None, headers=None):
        # gets remote key, in contrast to self._newKey
        key = None
        try:
            key = self.files.get_key(jobStoreID, headers=headers)
        except boto.exception.GSDataError:
            if headers == self.encryptedHeaders:
                # https://github.com/boto/boto/issues/3518
                # see self._writeFile for more
                pass
            else:
                raise
        if key is None:
            raise NoSuchFileException(jobStoreID)
        else:
            return key

    def _newKey(self, jobStoreID):
        # Does not create a key remotely. Careful -- does not ensure key name is unique
        return self.files.new_key(jobStoreID)

    def _readContents(self, jobStoreID):
        # used on job files only, which will be encrypted if avaliable
        headers = self.encryptedHeaders
        return self._getKey(jobStoreID, headers).get_contents_as_string(headers=headers)

    def _writeFile(self, jobStoreID, fileObj, update=False, encrypt=True):
        headers = self.encryptedHeaders if encrypt else self.headerValues
        if update:
            key = self._getKey(jobStoreID=jobStoreID, headers=headers)
        else:
            key = self._newKey(jobStoreID=jobStoreID)
        headers = self.encryptedHeaders if encrypt else self.headerValues
        try:
            key.set_contents_from_file(fileObj, headers=headers)
        except boto.exception.GSDataError:
            if encrypt:
                # Per https://cloud.google.com/storage/docs/encryption#customer-supplied_encryption_keys
                # the etag and md5 will not match with customer supplied
                # keys. However boto didn't get the memo apparently, and will raise this error if
                # they dont match. Reported: https://github.com/boto/boto/issues/3518
                pass
            else:
                raise

    def _writeString(self, jobStoreID, stringToUpload, **kwarg):
        self._writeFile(jobStoreID, StringIO(stringToUpload), **kwarg)

    @contextmanager
    def _uploadStream(self, key, update=False, encrypt=True):
        store = self

        class UploadPipe(WritablePipe):
            def readFrom(self, readable):
                headers = store.encryptedHeaders if encrypt else store.headerValues
                if update:
                    try:
                        key.set_contents_from_stream(readable, headers=headers)
                    except boto.exception.GSDataError:
                        if encrypt:
                            # https://github.com/boto/boto/issues/3518
                            # see self._writeFile for more
                            pass
                        else:
                            raise
                else:
                    try:
                        # The if_generation argument insures that the existing key matches the
                        # given generation, i.e. version, before modifying anything. Passing a
                        # generation of 0 insures that the key does not exist remotely.
                        key.set_contents_from_stream(readable, headers=headers, if_generation=0)
                    except (boto.exception.GSResponseError, boto.exception.GSDataError) as e:
                        if isinstance(e, boto.exception.GSResponseError):
                            if e.status == 412:
                                raise ConcurrentFileModificationException(key.name)
                            else:
                                raise e
                        elif encrypt:
                            # https://github.com/boto/boto/issues/3518
                            # see self._writeFile for more
                            pass
                        else:
                            raise

        with UploadPipe() as writable:
            yield writable

    @contextmanager
    def _downloadStream(self, key, encrypt=True):
        store = self

        class DownloadPipe(ReadablePipe):
            def writeTo(self, writable):
                headers = store.encryptedHeaders if encrypt else store.headerValues
                try:
                    key.get_file(writable, headers=headers)
                finally:
                    writable.close()

        with DownloadPipe() as readable:
            yield readable
