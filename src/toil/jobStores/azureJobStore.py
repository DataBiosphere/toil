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

import os
import uuid
import logging
from contextlib import contextmanager
from threading import Thread
import inspect
import bz2
import cPickle
import base64
import socket
import httplib
from datetime import datetime, timedelta

from ConfigParser import RawConfigParser, NoOptionError

from azure import WindowsAzureMissingResourceError

from azure.storage import (TableService, BlobService, SharedAccessPolicy, AccessPolicy,
                           BlobSharedAccessPermissions)

from toil.jobWrapper import JobWrapper
from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             ConcurrentFileModificationException,
                                             NoSuchFileException)
from toil.lib.encryption import encrypt, decrypt, encryptionOverhead

log = logging.getLogger(__name__)

credential_file_path = '~/.toilAzureCredentials'


def _fetchAzureAccountKey(accountName):
    """
    Find the account key for a given Azure storage account.

    The account key is taken from the AZURE_ACCOUNT_KEY environment
    variable if it exists, or from looking in the file
    "~/.toilAzureCredentials". That file has format:

    [AzureStorageCredentials]
    accountName1=ACCOUNTKEY1==
    accountName2=ACCOUNTKEY2==
    """
    if 'AZURE_ACCOUNT_KEY' in os.environ:
        return os.environ['AZURE_ACCOUNT_KEY']
    configParser = RawConfigParser()
    configParser.read(os.path.expanduser(credential_file_path))
    try:
        return configParser.get('AzureStorageCredentials', accountName)
    except NoOptionError:
        raise RuntimeError("No account key found for %s, please provide it in " +
                           credential_file_path % accountName)


class AzureJobStore(AbstractJobStore):
    """
    A job store that uses Azure's blob store for file storage and
    Table Service to store job info with strong consistency."""

    def __init__(self, accountName, namePrefix, config=None, jobChunkSize=65535):
        self.jobChunkSize = jobChunkSize
        self.keyPath = None

        account_key = _fetchAzureAccountKey(accountName)

        # Table names have strict requirements in Azure
        self.namePrefix = self._sanitizeTableName(namePrefix)
        log.debug("Creating job store with name prefix '%s'" % self.namePrefix)

        # These are the main API entrypoints.
        self.tableService = TableService(account_key=account_key, account_name=accountName)
        self.blobService = BlobService(account_key=account_key, account_name=accountName)

        # Register our job-store in the global table for this storage account
        self.registryTable = self._getOrCreateTable('toilRegistry')
        exists = self.registryTable.get_entity(row_key=self.namePrefix)
        self._checkJobStoreCreation(config is not None, exists, accountName + ":" + self.namePrefix)
        self.registryTable.insert_or_replace_entity(row_key=self.namePrefix,
                                                    entity={'exists': True})

        # Serialized jobs table
        self.jobItems = self._getOrCreateTable(self.qualify('jobs'))
        # Job<->file mapping table
        self.jobFileIDs = self._getOrCreateTable(self.qualify('jobFileIDs'))

        # Container for all shared and unshared files
        self.files = self._getOrCreateBlobContainer(self.qualify('files'))

        # Stats and logging strings
        self.statsFiles = self._getOrCreateBlobContainer(self.qualify('statsfiles'))
        # File IDs that contain stats and logging strings
        self.statsFileIDs = self._getOrCreateTable(self.qualify('statsFileIDs'))

        super(AzureJobStore, self).__init__(config=config)

        if self.config.cseKey is not None:
            self.keyPath = self.config.cseKey

    # tables must be alphanumeric
    nameSeparator = 'xx'

    def qualify(self, name):
        return self.namePrefix + self.nameSeparator + name

    def jobs(self):
        for jobEntity in self.jobItems.query_entities():
            yield AzureJob.fromEntity(jobEntity)

    def create(self, command, memory, cores, disk, updateID=None,
               predecessorNumber=0):
        jobStoreID = self._newJobID()
        job = AzureJob(jobStoreID=jobStoreID,
                       command=command, memory=memory, cores=cores, disk=disk,
                       remainingRetryCount=self._defaultTryCount(), logJobStoreFileID=None,
                       updateID=updateID, predecessorNumber=predecessorNumber)
        entity = job.toItem(chunkSize=self.jobChunkSize)
        entity['RowKey'] = jobStoreID
        self.jobItems.insert_entity(entity=entity)
        return job

    def exists(self, jobStoreID):
        if self.jobItems.get_entity(row_key=jobStoreID) is None:
            return False
        return True

    def load(self, jobStoreID):
        jobEntity = self.jobItems.get_entity(row_key=jobStoreID)
        if jobEntity is None:
            raise NoSuchJobException(jobStoreID)
        return AzureJob.fromEntity(jobEntity)

    def update(self, job):
        self.jobItems.update_entity(row_key=job.jobStoreID,
                                    entity=job.toItem(chunkSize=self.jobChunkSize))

    def delete(self, jobStoreID):
        try:
            self.jobItems.delete_entity(row_key=jobStoreID)
        except WindowsAzureMissingResourceError:
            # Job deletion is idempotent, and this job has been deleted already
            return
        filterString = "PartitionKey eq '%s'" % jobStoreID
        for fileEntity in self.jobFileIDs.query_entities(filter=filterString):
            jobStoreFileID = fileEntity.RowKey
            self.deleteFile(jobStoreFileID)

    def deleteJobStore(self):
        self.registryTable.delete_entity(row_key=self.namePrefix)
        self.jobItems.delete_table()
        self.jobFileIDs.delete_table()
        self.files.delete_container()
        self.statsFiles.delete_container()
        self.statsFileIDs.delete_table()

    def writeFile(self, localFilePath, jobStoreID=None):
        jobStoreFileID = self._newFileID()
        self.updateFile(jobStoreFileID, localFilePath)
        self._associateFileWithJob(jobStoreFileID, jobStoreID)
        return jobStoreFileID

    def updateFile(self, jobStoreFileID, localFilePath):
        with open(localFilePath) as read_fd:
            with self._uploadStream(jobStoreFileID, self.files,
                                    encrypted=self.keyPath is not None) as write_fd:
                while True:
                    buf = read_fd.read(self._maxAzureBlockBytes)
                    write_fd.write(buf)
                    if len(buf) == 0:
                        break

    def readFile(self, jobStoreFileID, localFilePath):
        try:
            with self._downloadStream(jobStoreFileID, self.files,
                                      encrypted=self.keyPath is not None) as read_fd:
                with open(localFilePath, 'w') as write_fd:
                    write_fd.write(read_fd.read(self._maxAzureBlockBytes))
        except WindowsAzureMissingResourceError:
            raise NoSuchFileException(jobStoreFileID)

    def deleteFile(self, jobStoreFileID):
        try:
            self.files.delete_blob(blob_name=jobStoreFileID)
            self._dissociateFileFromJob(jobStoreFileID)
        except WindowsAzureMissingResourceError:
            pass

    def fileExists(self, jobStoreFileID):
        # As Azure doesn't have a blob_exists method (at least in the
        # python API) we just try to download the metadata, and hope
        # the metadata is small so the call will be fast.
        try:
            self.files.get_blob_metadata(blob_name=jobStoreFileID)
            return True
        except WindowsAzureMissingResourceError:
            return False

    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        # TODO: this (and all stream methods) should probably use the
        # Append Blob type, but that is not currently supported by the
        # Azure Python API.
        jobStoreFileID = self._newFileID()
        with self._uploadStream(jobStoreFileID, self.files,
                                encrypted=self.keyPath is not None) as fd:
            yield fd, jobStoreFileID
        self._associateFileWithJob(jobStoreFileID, jobStoreID)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        with self._uploadStream(jobStoreFileID, self.files, checkForModification=True,
                                encrypted=self.keyPath is not None) as fd:
            yield fd

    def getEmptyFileStoreID(self, jobStoreID=None):
        jobStoreFileID = self._newFileID()
        self.files.put_blob(blob_name=jobStoreFileID, blob='',
                            x_ms_blob_type='BlockBlob')
        self._associateFileWithJob(jobStoreFileID, jobStoreID)
        return jobStoreFileID

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        if not self.fileExists(jobStoreFileID):
            raise NoSuchFileException(jobStoreFileID)
        with self._downloadStream(jobStoreFileID, self.files,
                                  encrypted=self.keyPath is not None) as fd:
            yield fd

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=True):
        sharedFileID = self._newFileID(sharedFileName)
        with self._uploadStream(sharedFileID, self.files,
                                encrypted=isProtected and self.keyPath is not None) as fd:
            yield fd

    @contextmanager
    def readSharedFileStream(self, sharedFileName, isProtected=True):
        sharedFileID = self._newFileID(sharedFileName)
        if not self.fileExists(sharedFileID):
            raise NoSuchFileException(sharedFileID)
        with self._downloadStream(sharedFileID, self.files,
                                  encrypted=isProtected and self.keyPath is not None) as fd:
            yield fd

    def writeStatsAndLogging(self, statsAndLoggingString):
        # TODO: would be a great use case for the append blobs, once
        # they are implemented in the python api.
        jobStoreFileID = self._newFileID()
        self.statsFiles.put_block_blob_from_text(blob_name=jobStoreFileID,
                                                 text=statsAndLoggingString)
        self.statsFileIDs.insert_entity(entity={'RowKey': jobStoreFileID})

    def readStatsAndLogging(self, statsAndLoggingCallbackFn):
        numStatsFiles = 0
        for entity in self.statsFileIDs.query_entities():
            jobStoreFileID = entity.RowKey
            with self._downloadStream(jobStoreFileID, self.statsFiles, encrypted=False) as fd:
                statsAndLoggingCallbackFn(fd)
            self.statsFiles.delete_blob(blob_name=jobStoreFileID)
            self.statsFileIDs.delete_entity(row_key=jobStoreFileID)
            numStatsFiles += 1
        return numStatsFiles

    _azureTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    def getPublicUrl(self, jobStoreFileID):
        # By default, we provide a link to the file which expires in one hour.
        startTimeStr = (datetime.utcnow() - timedelta(minutes=5)).strftime(self._azureTimeFormat)
        endTimeStr = (datetime.utcnow() + timedelta(hours=1)).strftime(self._azureTimeFormat)
        sap = SharedAccessPolicy(AccessPolicy(startTimeStr, endTimeStr,
                                              BlobSharedAccessPermissions.READ))
        sas_token = self.files.generate_shared_access_signature(blob_name=jobStoreFileID,
                                                                shared_access_policy=sap)
        return self.files.make_blob_url(blob_name=jobStoreFileID) + '?' + sas_token

    def getSharedPublicUrl(self, fileName):
        jobStoreFileID = self._newFileID(fileName)
        return self.getPublicUrl(jobStoreFileID)

    def _newJobID(self):
        # raw UUIDs don't work for Azure property names because the '-' character is disallowed.
        return str(uuid.uuid4()).replace('-', '_')

    # A dummy job ID under which all shared files are stored.
    sharedFileJobID = uuid.UUID('891f7db6-e4d9-4221-a58e-ab6cc4395f94')

    def _newFileID(self, sharedFileName=None):
        if sharedFileName is None:
            ret = str(uuid.uuid4())
        else:
            ret = str(uuid.uuid5(self.sharedFileJobID, str(sharedFileName)))
        return ret.replace('-', '_')

    def _associateFileWithJob(self, jobStoreFileID, jobStoreID=None):
        if jobStoreID is not None:
            self.jobFileIDs.insert_entity(entity={'PartitionKey': jobStoreID,
                                                  'RowKey': jobStoreFileID})

    def _dissociateFileFromJob(self, jobStoreFileID):
        entities = self.jobFileIDs.query_entities(filter="RowKey eq '%s'" % jobStoreFileID)
        if entities:
            assert len(entities) == 1
            jobStoreID = entities[0].PartitionKey
            self.jobFileIDs.delete_entity(partition_key=jobStoreID, row_key=jobStoreFileID)

    def _getOrCreateTable(self, tableName):
        # This will not fail if the table already exists.
        for attempt in retry_on_error():
            with attempt:
                self.tableService.create_table(tableName)
        return AzureTable(self.tableService, tableName)

    def _getOrCreateBlobContainer(self, containerName):
        for attempt in retry_on_error():
            with attempt:
                self.blobService.create_container(containerName)
        return AzureBlobContainer(self.blobService, containerName)

    def _sanitizeTableName(self, tableName):
        """
        Azure table names must start with a letter and be alphanumeric.

        This will never cause a collision if uuids are used, but
        otherwise may not be safe.
        """
        return 'a' + filter(lambda x: x.isalnum(), tableName)

    # Maximum bytes that can be in any block of an Azure block blob
    _maxAzureBlockBytes = 4194000

    @contextmanager
    def _uploadStream(self, jobStoreFileID, container, checkForModification=False, encrypted=False):
        # This is a straightforward transliteration into Azure of the
        # _uploadStream method of AWSJobStore.
        if checkForModification:
            try:
                expectedVersion = container.get_blob_properties(blob_name=jobStoreFileID)['etag']
            except WindowsAzureMissingResourceError:
                expectedVersion = None

        if encrypted and self.keyPath is None:
            encrypted = False
            log.warning("Encryption requested but no key available, not encrypting")

        maxBlockSize = self._maxAzureBlockBytes
        if encrypted:
            # There is a small overhead for encrypted data.
            maxBlockSize -= encryptionOverhead
        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def reader():
                    try:
                        blockIDs = []
                        try:
                            while True:
                                buf = readable.read(maxBlockSize)
                                if len(buf) == 0:
                                    # We're safe to break here even if we never read anything, since
                                    # putting an empty block list creates an empty blob.
                                    break
                                if encrypted:
                                    buf = encrypt(buf, self.keyPath)
                                blockID = self._newFileID()
                                container.put_block(blob_name=jobStoreFileID, block=buf,
                                                    blockid=blockID)
                                blockIDs.append(blockID)
                        except:
                            # This is guaranteed to delete any uncommitted
                            # blocks.
                            container.delete_blob(blob_name=jobStoreFileID)
                            raise

                        if checkForModification and expectedVersion is not None:
                            # Acquire a (60-second) write lock,
                            leaseID = container.lease_blob(blob_name=jobStoreFileID,
                                                           x_ms_lease_action='acquire'
                                                           )['x-ms-lease-id']
                            # check for modification,
                            blobProperties = container.get_blob_properties(blob_name=jobStoreFileID)
                            if blobProperties['etag'] != expectedVersion:
                                container.lease_blob(blob_name=jobStoreFileID,
                                                     x_ms_lease_action='release',
                                                     x_ms_lease_id=leaseID)
                                raise ConcurrentFileModificationException(jobStoreFileID)
                            # commit the file,
                            container.put_block_list(blob_name=jobStoreFileID, block_list=blockIDs,
                                                     x_ms_lease_id=leaseID)
                            # then release the lock.
                            container.lease_blob(blob_name=jobStoreFileID,
                                                 x_ms_lease_action='release', x_ms_lease_id=leaseID)
                        else:
                            # No need to check for modification, just blindly write over whatever
                            # was there.
                            container.put_block_list(blob_name=jobStoreFileID, block_list=blockIDs)
                    except:
                        log.exception("Multipart reader thread encountered an exception")

                thread = Thread(target=reader)
                thread.start()
                yield writable
            # The writable is now closed. This will send EOF to the readable and cause that
            # thread to finish.
            thread.join()

    @contextmanager
    def _downloadStream(self, jobStoreFileID, container, encrypted=False):
        # Transliteration of _downloadStream method in AWSJobStore.
        if encrypted and self.keyPath is None:
            encrypted = False
            log.warning("Encryption requested but no key available, not decrypting")

        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def writer():
                    try:
                        chunkStartPos = 0
                        blobProps = container.get_blob_properties(blob_name=jobStoreFileID)
                        fileSize = int(blobProps['Content-Length'])
                        while chunkStartPos < fileSize:
                            chunkEndPos = chunkStartPos + self._maxAzureBlockBytes - 1
                            buf = container.get_blob(blob_name=jobStoreFileID,
                                                     x_ms_range="bytes=%d-%d" % (chunkStartPos,
                                                                                 chunkEndPos))
                            if encrypted:
                                buf = decrypt(buf, self.keyPath)
                            writable.write(buf)
                            chunkStartPos = chunkEndPos + 1
                    except:
                        log.exception("Exception encountered in writer thread")
                    finally:
                        # Ensure readers aren't left blocking if this thread crashes.
                        # This close() will send EOF to the reading end and ultimately cause the
                        # yield to return. It also makes the implict .close() done by the enclosing
                        # "with" context redundant but that should be ok since .close() on file
                        # objects are idempotent.
                        writable.close()

                thread = Thread(target=writer)
                thread.start()
                yield readable
                thread.join()


class AzureTable(object):
    """
    A shim over the Azure TableService API, specfic for a single table.

    This class automatically forwards method calls to the TableService
    API, including the proper table name and default partition key if
    needed. To avoid confusion, all method calls must use *only*
    keyword arguments.

    In addition, this wrapper:
      - allows a default partition key to be used when one is not specified
      - returns None when attempting to get a non-existent entity.
    """

    def __init__(self, tableService, tableName):
        self.tableService = tableService
        self.tableName = tableName

    defaultPartition = 'default'

    def __getattr__(self, name):
        def f(*args, **kwargs):
            assert len(args) == 0
            function = getattr(self.tableService, name)
            funcArgs, _, _, _ = inspect.getargspec(function)
            kwargs['table_name'] = self.tableName
            if 'partition_key' not in kwargs and 'partition_key' in funcArgs:
                kwargs['partition_key'] = self.defaultPartition
            if 'entity' in kwargs:
                if 'PartitionKey' not in kwargs['entity']:
                    kwargs['entity']['PartitionKey'] = self.defaultPartition

            for attempt in retry_on_error():
                with attempt:
                    return function(**kwargs)

        return f

    def get_entity(self, **kwargs):
        try:
            return self.__getattr__('get_entity')(**kwargs)
        except WindowsAzureMissingResourceError:
            return None


class AzureBlobContainer(object):
    """
    A shim over the BlobService API, so that the container name is automatically filled in.

    To avoid confusion over the position of any remaining positional
    arguments, all method calls must use *only* keyword arguments.
    """

    def __init__(self, blobService, containerName):
        self.blobService = blobService
        self.containerName = containerName

    def __getattr__(self, name):
        def f(*args, **kwargs):
            assert len(args) == 0
            function = getattr(self.blobService, name)
            kwargs['container_name'] = self.containerName

            for attempt in retry_on_error():
                with attempt:
                    return function(**kwargs)

        return f


class AzureJob(JobWrapper):
    """
    Serialize and unserialize a job for storage on Azure.

    Copied almost entirely from AWSJob, except to take into account the
    fact that Azure properties must start with a letter or underscore.
    """

    defaultAttrs = ['PartitionKey', 'RowKey', 'etag', 'Timestamp']

    @classmethod
    def fromEntity(cls, jobEntity):
        """
        :type jobEntity: Entity
        :rtype: AzureJob
        """
        jobEntity = jobEntity.__dict__
        for attr in cls.defaultAttrs:
            del jobEntity[attr]
        return cls.fromItem(jobEntity)

    @classmethod
    def fromItem(cls, item):
        """
        :type item: dict
        :rtype: AzureJob
        """
        chunkedJob = item.items()
        chunkedJob.sort()
        if len(chunkedJob) == 1:
            # First element of list = tuple, second element of tuple = serialized job
            wholeJobString = chunkedJob[0][1]
        else:
            wholeJobString = ''.join(item[1] for item in chunkedJob)
        return cPickle.loads(bz2.decompress(base64.b64decode(wholeJobString)))

    # Max size of a string value in Azure is 64K
    def toItem(self, chunkSize=65535):
        """
        :rtype: dict
        """
        item = {}
        serializedAndEncodedJob = base64.b64encode(bz2.compress(cPickle.dumps(self)))
        jobChunks = [serializedAndEncodedJob[i:i + chunkSize]
                     for i in range(0, len(serializedAndEncodedJob), chunkSize)]
        for attributeOrder, chunk in enumerate(jobChunks):
            item['_' + str(attributeOrder).zfill(3)] = chunk
        return item


def retry_on_error(num_tries=5, retriable_exceptions=(socket.error, socket.gaierror,
                                                      httplib.HTTPException)):
    """
    Retries on a set of allowable exceptions, mimicking boto's behavior by default.

    :param num_tries: number of times to try before giving up.

    :return: a generator yielding contextmanagers

    Retry the correct number of times and then give up and reraise
    >>> i = 0
    >>> for attempt in retry_on_error(retriable_exceptions=(RuntimeError,)):
    ...     with attempt:
    ...         i += 1
    ...         raise RuntimeError("foo")
    Traceback (most recent call last):
    ...
    RuntimeError: foo
    >>> i
    5

    Give up and reraise on any unexpected exceptions
    >>> i = 0
    >>> for attempt in retry_on_error(num_tries=5, retriable_exceptions=()):
    ...     with attempt:
    ...         i += 1
    ...         raise RuntimeError("foo")
    Traceback (most recent call last):
    ...
    RuntimeError: foo
    >>> i
    1

    Do things only once if they succeed!
    >>> i = 0
    >>> for attempt in retry_on_error():
    ...     with attempt:
    ...         i += 1
    >>> i
    1
    """
    go = [None]

    @contextmanager
    def attempt(last=False):
        try:
            yield
        except retriable_exceptions as e:
            if last:
                raise
            else:
                log.info("Got a retriable exception %s, trying again" % e.__class__.__name__)
        else:
            go.pop()

    while go:
        if num_tries == 1:
            yield attempt(last=True)
        else:
            yield attempt()
        # It's safe to do this, even with Python's weird default
        # arguments behavior, since we are assigning to num_tries
        # rather than mutating it.
        num_tries -= 1
