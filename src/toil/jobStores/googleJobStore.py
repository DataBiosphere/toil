import base64
from contextlib import contextmanager
import hashlib
import os
import uuid
from StringIO import StringIO

from collections import namedtuple
from bd2k.util.threading import ExceptionalThread
import boto
import logging
import cPickle
import time
from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             NoSuchFileException,
                                             ConcurrentFileModificationException)
from toil.jobStores.utils import retry
from toil.jobWrapper import JobWrapper

log = logging.getLogger(__name__)

GOOGLE_STORAGE = 'gs'


class GoogleJobStore(AbstractJobStore):
    class Locator(namedtuple('Locator', ('projectID', 'namePrefix', 'headers', 'uri')),
                  AbstractJobStore.Locator):
        """
        Represents the location of a job store in Google Storage similar to how URLs are used to
        locate resources on the Internet.  Has the following attributes:
            * projectID: The project ID of the job store as a string if one is given. If a project
                         ID is not given this is None.
            * namePrefix: The name prefix of the job store as a string.
            * headerValues: A dictionary of header values.
            * uri: A boto storage uri object for the job store.

        The syntax for a Google job store locator string is as follows:
            google:<project ID>:<name prefix>

        Note that a project ID may be omitted if one does not exist, like so:
            google:<name prefix>

        For name prefix syntax see exception messages in parse method below.
        """
        jobStoreName = 'google'

        # See https://cloud.google.com/storage/docs/naming#requirements
        #
        maxBucketNameLen = 63
        bucketSuffix = "--toil"

        @property
        def jobStoreCls(self):
            return GoogleJobStore

        @classmethod
        def parse(cls, locator):
            try:
                projectID, prefix = locator.split(':')
            except ValueError as e:
                if e.message == 'need more than 1 value to unpack':
                    projectID = None
                    prefix = locator
                else:
                    raise ValueError("The job store locator '%s' is invalid." % locator)

            if 'google' in prefix:
                raise ValueError("Invalid name prefix '%s'. Name prefixes cannot contain "
                                 "'google'" % prefix)
            elif len(prefix) > cls.maxBucketNameLen - len(cls.bucketSuffix) - len('gs://'):
                raise ValueError("Invalid name prefix '%s'. Name prefixes may not be longer "
                                 "than 52 characters." % prefix)
            else:
                headers = {"x-goog-project-id": projectID} if projectID else {}
                uri = boto.storage_uri("gs://%s%s" % (prefix, cls.bucketSuffix), GOOGLE_STORAGE)
                return cls(projectID, prefix, headers, uri)

        def __str__(self):
            return ':'.join((self.jobStoreName, self.projectID or '', self.namePrefix))

    readStatsBaseID = '_f16eef0c-b597-4b8b-9b0c-4d605b4f506c'

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
        return self.locator.headers.copy()

    @headerValues.setter
    def headerValues(self, value):
        self.locator.headers = value

    # Do not invoke the constructor, use the loadOrCreateJobStore factory method.

    def __init__(self, locator, config=None):
        self.locator = locator
        log.debug("Creating job store at '%s'", self.locator)
        self._encryptedHeaders = self.locator.headers
        self.files = None
        exists = True
        try:
            self.files = self.locator.uri.get_bucket(headers=self.headerValues, validate=True)
        except boto.exception.GSResponseError as e:
            if e.error_code == 'NoSuchBucket':
                exists = False
            else:
                raise
        self._checkJobStoreCreation(config is not None, exists, self.locator)
        if not exists:
            self.files = self._retryCreateBucket(self.locator.uri, self.headerValues)
        super(GoogleJobStore, self).__init__(config=config)

        self.sseKeyPath = self.config.sseKey
        # functionally equivalent to dictionary1.update(dictionary2) but works with our immutable
        # dicts
        self.encryptedHeaders = dict(self.encryptedHeaders, **self._resolveEncryptionHeaders())

    def deleteJobStore(self):
        self.doDeleteJobStore(self.locator)

    @classmethod
    def doDeleteJobStore(cls, locator):
        def noSuchBucket(e):
            return isinstance(e, boto.exception.GSResponseError) and e.error_code == 'NoSuchBucket'

        existed = False
        try:
            for attempt in retry(timeout=5, predicate=noSuchBucket):
                with attempt:
                    bucket = locator.uri.get_bucket(validate=True)
        except boto.exception.GSResponseError as e:
            if e.error_code == 'NoSuchBucket':
                pass
            else:
                raise
        else:
            existed = True
            log.info("Attempting to delete job store files bucket at '%s'...", locator.uri)
            # no upper time limit on this call keep trying delete calls until we succeed - we can
            # fail because of eventual consistency in 2 ways: 1) skipping unlisted objects in bucket
            # that are meant to be deleted 2) listing of ghost objects when trying to delete bucket
            while True:
                try:
                    locator.uri.delete_bucket()
                except boto.exception.GSResponseError as e:
                    if e.status == 404:
                        break
                    else:
                        log.debug("Either the bucket at '%s' is not empty or it contains ghost "
                                  "objects...", locator.uri)
                        time.sleep(0.5)
                else:
                    log.debug("Successfully deleted files bucket at '%s'.", locator.uri)
                    break

                # object could have been deleted already
                log.debug("Attempting to empty files bucket at '%s' if any objects exist...",
                          locator.uri)
                for obj in bucket.list():
                    try:
                        obj.delete()
                    except boto.exception.GSResponseError:
                        pass
        if existed:
            log.info("Successfully deleted job store at '%s'.", locator)
        elif not existed:
            log.info("No job store found at '%s'.", locator)

    def create(self, command, memory, cores, disk, preemptable, predecessorNumber=0):
        jobStoreID = self._newID()
        job = JobWrapper(jobStoreID=jobStoreID,
                         command=command, memory=memory, cores=cores, disk=disk,
                         remainingRetryCount=self._defaultTryCount(), logJobStoreFileID=None,
                         preemptable=preemptable,
                         predecessorNumber=predecessorNumber)
        self._writeString(jobStoreID, cPickle.dumps(job))
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
        self._writeString(job.jobStoreID, cPickle.dumps(job), update=True)

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
        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def writer():
                    headers = self.encryptedHeaders if encrypt else self.headerValues
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
                            # The if_condition kwarg insures that the existing key matches given
                            # generation (version) before modifying anything. Setting
                            # if_generation=0 insures key does not exist remotely
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
                thread = ExceptionalThread(target=writer)
                thread.start()
                yield writable
            thread.join()

    @contextmanager
    def _downloadStream(self, key, encrypt=True):
        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def writer():
                    headers = self.encryptedHeaders if encrypt else self.headerValues
                    try:
                        key.get_file(writable, headers=headers)
                    finally:
                        writable.close()

                thread = ExceptionalThread(target=writer)
                thread.start()
                yield readable
                thread.join()
