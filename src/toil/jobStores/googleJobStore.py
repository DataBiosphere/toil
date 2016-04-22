import base64
from contextlib import contextmanager
import hashlib
import os
import random
import uuid
from StringIO import StringIO
from bd2k.util.threading import ExceptionalThread
import boto
import logging
import cPickle
import time
from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             NoSuchFileException,
                                             ConcurrentFileModificationException)
from toil.jobWrapper import JobWrapper

log = logging.getLogger(__name__)

GOOGLE_STORAGE = 'gs'


class GoogleJobStore(AbstractJobStore):

    @classmethod
    def createJobStore(cls, jobStoreString, config=None):
        try:
            namePrefix, projectID = jobStoreString.split(":", 1)
            # jobstorestring = gs:project_id:bucket
        except ValueError:
            # we don't have a specified projectID
            namePrefix = jobStoreString
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

        exists = True
        try:
            self.files = self.uri.get_bucket(headers=self.headerValues, validate=True)
        except boto.exception.GSResponseError:
            exists = False

        create = config is not None
        self._checkJobStoreCreation(create, exists, projectID+':'+namePrefix)

        if not exists:
            self.files = self.uri.create_bucket(headers=self.headerValues)

        super(GoogleJobStore, self).__init__(config=config)
        self.sseKeyPath = self.config.sseKey
        # functionally equivalent to dictionary1.update(dictionary2) but preserves immutability
        self.encryptedHeaders = dict(self.encryptedHeaders, **self._resolveEncryptionHeaders())

        self.statsBaseID = 'f16eef0c-b597-4b8b-9b0c-4d605b4f506c'
        self.statsReadPrefix = '_'
        self.readStatsBaseID = self.statsReadPrefix+self.statsBaseID
        self.start = 0  # tracks the starting index of available statsFileIDs
        self.statsIDRange = 30

    def deleteJobStore(self):
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
        # eventually consistent listing necessitates a more complex stats and logging mechanism
        # All stats and logging fileIDs are in the form prefix+integer (stats0, stats1, etc)
        # To get a free fileID we walk over the namespace until we reach a free ID at index x
        # then write at a random index from x-(x+self.statsRange)
        # We get the following invariants with this methodology:
        # the first n indices are all written to. n may be 0, but will only increase
        # the len(self.statsRange) indices following the first n may or may not be written to
        # at indexes > n+ len(self.statsRange) no indices may be written to

        # when a stats file is read it is renamed with a specified index. The beenRead kwarg
        # indicates we should generate an ID with the read prefix
        while (self.fileExists(self._getPrimaryStatsID(self.start))
               or self.fileExists(self._getSecondaryStatsID(self.start))):
            self.start += 1
        # start now points to the (n+1)th file
        freeIndex = self.start + random.randrange(0, self.statsIDRange)
        freeID = self._getPrimaryStatsID(freeIndex)
        key = self._newKey(freeID)
        try:
            with self._uploadStream(key, encrypt=False, update=False) as f:
                f.write(statsAndLoggingString)
            log.debug("writing stats file: %s", key.name)
        except ConcurrentFileModificationException:
            # try again until we get a freeID
            self.writeStatsAndLogging(statsAndLoggingString)

    def readStatsAndLogging(self, callback, readAll=False):
        read = 0
        if readAll:
            self.start = 0
        while True:
            currRead = self._readStats(callback, readAll)
            read += currRead
            if readAll or 0 == currRead:
                break
        return read

    def _readStats(self, callback, readall):

        def readPrimaryIDs(index, fn, copy=True):
            # reads unread files and optionally renames them afterwards
            # Raises NoSuchFileException
            primaryID = self._getPrimaryStatsID(index)
            with self.readSharedFileStream(primaryID, isProtected=False) as f:
                contents = f.read()
                fn(StringIO(contents))
                log.debug("reading stats file: %s", primaryID)
            if copy:
                secondaryID = self._getSecondaryStatsID(index)
                with self.writeSharedFileStream(secondaryID, isProtected=False) as writable:
                    writable.write(contents)

        def readAllIDs(index, fn):
            # Raises NoSuchFileException
            try:
                readPrimaryIDs(index, fn, copy=False)
            except NoSuchFileException:
                secondaryID = self._getSecondaryStatsID(index)
                with self.readSharedFileStream(secondaryID, isProtected=False) as f:
                    contents = f.read()
                    fn(StringIO(contents))
                    log.debug("reading stats file: %s", secondaryID)

        readFn = readAllIDs if readall else readPrimaryIDs
        beenRead = set()  # set of indices from start - start+self.statsRange that have been read

        if not readall:
            self.skipAlreadyReadFiles()

        read = self.readSequentialExistingFiles(beenRead, callback, readFn, readall)

        beenRead = {x for x in beenRead if x > self.start}  # filter indexes we have passed.

        # start now points to an index not yet written to. Files up to start+range may still exist
        read += self.readIDsInRange(beenRead, callback, readFn, readall)
        return read

    def skipAlreadyReadFiles(self):
        secondaryID = self._getSecondaryStatsID(self.start)
        while self.fileExists(secondaryID):
            self.start += 1

    def readSequentialExistingFiles(self, beenRead, callback, readFn, readall):
        read = 0
        while True:
            if self.start not in beenRead:
                secondaryID = self._getSecondaryStatsID(self.start)
                if not readall and self.fileExists(secondaryID):
                    # if secondary file exists we don't want to reread the ID unless readall==True
                    self.start += 1
                    continue
                try:
                    readFn(str(self.start), callback)
                    read += 1
                    self.start += 1
                except NoSuchFileException:
                        break
            else:
                self.start += 1
        return read

    def readIDsInRange(self, beenRead, callback, readFn, readAll):
        read = 0
        for possible in [x for x in range(0, self.statsIDRange) if x not in beenRead]:
            indexToTry = self.start + possible
            checkID = self._getPrimaryStatsID(indexToTry, beenRead=True)
            if not readAll and self.fileExists(checkID):
                continue
            try:
                readFn(str(indexToTry), callback)
                read += 1
                beenRead.add(indexToTry)
            except NoSuchFileException:
                # these are just possible indexes, OK if we don't find a file there
                pass
        return read

    def _getPrimaryStatsID(self, index, beenRead=False):
        newID = self.readStatsBaseID if beenRead else self.statsBaseID
        newID += str(index)
        return newID

    def _getSecondaryStatsID(self, index):
        return self._getPrimaryStatsID(index, beenRead=True)

    def _getStatsIDPair(self, index):
        return self._getPrimaryStatsID(index), self._getSecondaryStatsID(index)

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
