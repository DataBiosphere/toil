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
import boto
from boto.exception import GSResponseError, GSDataError
import logging
import time

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
from toil.jobStores.aws.utils import SDBHelper
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

        #  create 2 buckets
        self.locator = locator
        self.projectID = projectID
        self.bucketName = namePrefix+"--toil"
        log.debug("Instantiating google jobStore with name: %s", self.bucketName)
        self.gsBucketURL = bytes("gs://"+self.bucketName)
        self._headerValues = {"x-goog-project-id": bytes(projectID)} if projectID else {}
        self._encryptedHeaders = self.headerValues

        import gcs_oauth2_boto_plugin # needed to import authentication handler
        self.uri = boto.storage_uri(self.gsBucketURL, GOOGLE_STORAGE)
        self.files = None

        self.statsBaseID = 'f16eef0c-b597-4b8b-9b0c-4d605b4f506c'
        self.statsReadPrefix = '_'
        self.readStatsBaseID = self.statsReadPrefix+self.statsBaseID

    def initialize(self, config=None):
        exists = True
        try:
            self.uri.get_bucket(headers=self.headerValues, validate=True)
        except GSResponseError:
            exists = False

        if exists:
            raise JobStoreExistsException(self.locator)

        self.files = self._retryCreateBucket(self.uri, self.headerValues)

        # functionally equivalent to dictionary1.update(dictionary2) but works with our immutable dicts
        self.encryptedHeaders = dict(self.encryptedHeaders, **self._resolveEncryptionHeaders(config))

        super(GoogleJobStore, self).initialize(config)

    def resume(self):
        try:
            self.files = self.uri.get_bucket(headers=self.headerValues, validate=True)
        except GSResponseError:
            raise NoSuchJobStoreException(self.locator)
        super(GoogleJobStore, self).resume()

    @property
    def sseKeyPath(self):
        return self.config.sseKey

    def destroy(self):
        # no upper time limit on this call keep trying delete calls until we succeed - we can
        # fail because of eventual consistency in 2 ways: 1) skipping unlisted objects in bucket
        # that are meant to be deleted 2) listing of ghost objects when trying to delete bucket

        # just return if not connect to physical storage
        if self.files is None:
            return
        while True:
            try:
                self.uri.delete_bucket()
            except GSResponseError as e:
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
                except GSResponseError:
                    pass

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

        #https://github.com/GoogleCloudPlatform/gcs-oauth2-boto-plugin/issues/15
        # The google connection is missing sign_string().
        # It is only signing 'GET', though, so skipping the encoding might be fine.
        def sign_string(self, string_to_sign):
            #import base64
            #from oauth2client.crypt import Signer
            #signer = Signer.from_string(self.oauth2_client._private_key)
            #return base64.b64encode(signer.sign(string_to_sign))
            return string_to_sign
        import types
        key.bucket.connection._auth_handler.sign_string = types.MethodType(sign_string, key.bucket.connection._auth_handler)
        key.set_canned_acl('public-read')
        return key.generate_url(query_auth=False,
                               expires_in=self.publicUrlExpiration.total_seconds())

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

    def jobs(self):
        for key in self.files.list():
            jobStoreID = key.name
            if len(jobStoreID) == 39: # 'job' + uuid length
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

    def readFile(self, jobStoreFileID, localFilePath, symlink=False):
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
        except GSDataError as e:
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
        except (NoSuchFileException, GSResponseError) as e:
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
        return 'gs://' + url.netloc + url.path

    @classmethod
    def getSize(cls, url):
        uri = GoogleJobStore._getResources(url)
        uri = boto.storage_uri(uri, GOOGLE_STORAGE)
        return uri.get_key().size

    @classmethod
    def _readFromUrl(cls, url, writable):
        # gs://projectid/bucket/key
        uri = GoogleJobStore._getResources(url)
        uri = boto.storage_uri(uri, GOOGLE_STORAGE)
        uri.get_contents_to_file(writable)

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() == 'gs'

    @classmethod
    def _writeToUrl(cls, readable, url):
        uri = GoogleJobStore._getResources(url)
        uri = boto.storage_uri(uri, GOOGLE_STORAGE)
        uri.set_contents_from_stream(readable)

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
            # prefix seems broken
            for key in list(self.files.list()): #prefix=prefix)):
                if not key.name.startswith(prefix):
                    continue
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
            except GSResponseError as e:
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

    def _delete(self, jobStoreID, encrypt=True):
        headers = self.encryptedHeaders if encrypt else self.headerValues
        try:
            key = self._getKey(jobStoreID, headers)
        except NoSuchFileException:
            pass
        else:
            try:
                key.delete()
            except GSResponseError as e:
                if e.status == 404:
                    pass
        # best effort delete associated files
        for fileID in self.files.list():
            if fileID.name.startswith(jobStoreID):
                try:
                    self.deleteFile(fileID)
                except NoSuchFileException:
                    pass

    def _getKey(self, jobStoreID=None, headers=None):

        # UPDATE _getKey() has problems - listing buckets works though
        for obj in self.uri.get_bucket():
            #print '%s://%s/%s' % (self.uri.scheme, self.uri.bucket_name, obj.name)
            if jobStoreID == obj.name:
                return obj

        # gets remote key, in contrast to self._newKey
        key = None
        try:
            key = self.files.get_key(jobStoreID, headers=headers)
        except GSDataError:
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
        headers = self.encryptedHeaders if encrypt else self.headerValues # UPDATE: why is this duplicated?
        try:
            key.set_contents_from_file(fileObj, headers=headers)
        except GSDataError:
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
                    except GSDataError:
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
                    except (GSResponseError, GSDataError) as e:
                        if isinstance(e, GSResponseError):
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
