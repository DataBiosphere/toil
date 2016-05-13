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
import inspect
import bz2
import cPickle
import socket
import httplib
from datetime import datetime, timedelta

from ConfigParser import RawConfigParser, NoOptionError

from azure.common import AzureMissingResourceHttpError, AzureException
from azure.storage import SharedAccessPolicy, AccessPolicy
from azure.storage.table import TableService, EntityProperty
from azure.storage.blob import BlobService, BlobSharedAccessPermissions

import requests
from bd2k.util import strict_bool

from bd2k.util.threading import ExceptionalThread

from toil.jobWrapper import JobWrapper
from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             ConcurrentFileModificationException,
                                             NoSuchFileException)
import toil.lib.encryption as encryption

logger = logging.getLogger(__name__)

credential_file_path = '~/.toilAzureCredentials'


def _fetchAzureAccountKey(accountName):
    """
    Find the account key for a given Azure storage account.

    The account key is taken from the AZURE_ACCOUNT_KEY_<account> environment variable if it
    exists, then from plain AZURE_ACCOUNT_KEY, and then from looking in the file
    ~/.toilAzureCredentials. That file has format:

    [AzureStorageCredentials]
    accountName1=ACCOUNTKEY1==
    accountName2=ACCOUNTKEY2==
    """
    try:
        return os.environ['AZURE_ACCOUNT_KEY_' + accountName]
    except KeyError:
        try:
            return os.environ['AZURE_ACCOUNT_KEY']
        except KeyError:
            configParser = RawConfigParser()
            configParser.read(os.path.expanduser(credential_file_path))
            try:
                return configParser.get('AzureStorageCredentials', accountName)
            except NoOptionError:
                raise RuntimeError("No account key found for '%s', please provide it in '%s'" %
                                   (accountName, credential_file_path))


maxAzureTablePropertySize = 64 * 1024


class AzureJobStore(AbstractJobStore):
    """
    A job store that uses Azure's blob store for file storage and
    Table Service to store job info with strong consistency."""

    def __init__(self, accountName, namePrefix, config=None, jobChunkSize=maxAzureTablePropertySize):
        self.jobChunkSize = jobChunkSize
        self.keyPath = None

        self.account_key = _fetchAzureAccountKey(accountName)
        self.accountName = accountName
        # Table names have strict requirements in Azure
        self.namePrefix = self._sanitizeTableName(namePrefix)
        logger.debug("Creating job store with name prefix '%s'" % self.namePrefix)

        # These are the main API entrypoints.
        self.tableService = TableService(account_key=self.account_key, account_name=accountName)
        self.blobService = BlobService(account_key=self.account_key, account_name=accountName)

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

    # Table names must be alphanumeric
    nameSeparator = 'xx'

    # Length of a jobID - used to test if a stats file has been read already or not
    jobIDLength = len(str(uuid.uuid4()))

    def qualify(self, name):
        return self.namePrefix + self.nameSeparator + name

    def jobs(self):
        
        # How many jobs have we done?
        total_processed = 0
            
        for jobEntity in self.jobItems.query_entities_auto():
            # Process the items in the page
            yield AzureJob.fromEntity(jobEntity)
            total_processed += 1
            
            if total_processed % 1000 == 0:
                # Produce some feedback for the user, because this can take
                # a long time on, for example, Azure
                logger.info("Processed %d total jobs" % total_processed)
            
        logger.info("Processed %d total jobs" % total_processed)

    def create(self, command, memory, cores, disk, preemptable, predecessorNumber=0):
        jobStoreID = self._newJobID()
        job = AzureJob(jobStoreID=jobStoreID, command=command,
                       memory=memory, cores=cores, disk=disk, preemptable=preemptable,
                       remainingRetryCount=self._defaultTryCount(), logJobStoreFileID=None,
                       predecessorNumber=predecessorNumber)
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
        except AzureMissingResourceHttpError:
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

    def getEnv(self):
        return dict(AZURE_ACCOUNT_KEY=self.account_key)

    @classmethod
    def _readFromUrl(cls, url, writable):
        blobService, containerName, blobName = cls._extractBlobInfoFromUrl(url)
        blobService.get_blob_to_file(containerName, blobName, writable)

    @classmethod
    def _writeToUrl(cls, readable, url):
        blobService, containerName, blobName = cls._extractBlobInfoFromUrl(url)
        blobService.put_block_blob_from_file(containerName, blobName, readable)
        blobService.get_blob(containerName, blobName)

    @staticmethod
    def _extractBlobInfoFromUrl(url):
        """
        :return: (blobService, containerName, blobName)
        """
        def invalidUrl():
            raise RuntimeError("The URL '%s' is invalid" % url.geturl())

        netloc = url.netloc.split('@')
        if len(netloc) != 2:
            invalidUrl()

        accountEnd = netloc[1].find('.blob.core.windows.net')
        if accountEnd == -1:
            invalidUrl()

        containerName, accountName = netloc[0], netloc[1][0:accountEnd]
        blobName = url.path[1:]  # urlparse always includes a leading '/'
        blobService = BlobService(account_key=_fetchAzureAccountKey(accountName),
                                  account_name=accountName)
        return blobService, containerName, blobName

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() == 'wasb' or url.scheme.lower() == 'wasbs'

    def writeFile(self, localFilePath, jobStoreID=None):
        jobStoreFileID = self._newFileID()
        self.updateFile(jobStoreFileID, localFilePath)
        self._associateFileWithJob(jobStoreFileID, jobStoreID)
        return jobStoreFileID

    def updateFile(self, jobStoreFileID, localFilePath):
        with open(localFilePath) as read_fd:
            with self._uploadStream(jobStoreFileID, self.files) as write_fd:
                while True:
                    buf = read_fd.read(self._maxAzureBlockBytes)
                    write_fd.write(buf)
                    if len(buf) == 0:
                        break

    def readFile(self, jobStoreFileID, localFilePath):
        try:
            with self._downloadStream(jobStoreFileID, self.files) as read_fd:
                with open(localFilePath, 'w') as write_fd:
                    while True:
                        buf = read_fd.read(self._maxAzureBlockBytes)
                        write_fd.write(buf)
                        if not buf: break
        except AzureMissingResourceHttpError:
            raise NoSuchFileException(jobStoreFileID)

    def deleteFile(self, jobStoreFileID):
        try:
            self.files.delete_blob(blob_name=jobStoreFileID)
            self._dissociateFileFromJob(jobStoreFileID)
        except AzureMissingResourceHttpError:
            pass

    def fileExists(self, jobStoreFileID):
        # As Azure doesn't have a blob_exists method (at least in the
        # python API) we just try to download the metadata, and hope
        # the metadata is small so the call will be fast.
        try:
            self.files.get_blob_metadata(blob_name=jobStoreFileID)
            return True
        except AzureMissingResourceHttpError:
            return False

    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        # TODO: this (and all stream methods) should probably use the
        # Append Blob type, but that is not currently supported by the
        # Azure Python API.
        jobStoreFileID = self._newFileID()
        with self._uploadStream(jobStoreFileID, self.files) as fd:
            yield fd, jobStoreFileID
        self._associateFileWithJob(jobStoreFileID, jobStoreID)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        with self._uploadStream(jobStoreFileID, self.files, checkForModification=True) as fd:
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
        with self._downloadStream(jobStoreFileID, self.files) as fd:
            yield fd

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        sharedFileID = self._newFileID(sharedFileName)
        with self._uploadStream(sharedFileID, self.files, encrypted=isProtected) as fd:
            yield fd

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        sharedFileID = self._newFileID(sharedFileName)
        if not self.fileExists(sharedFileID):
            raise NoSuchFileException(sharedFileID)
        with self._downloadStream(sharedFileID, self.files) as fd:
            yield fd

    def writeStatsAndLogging(self, statsAndLoggingString):
        # TODO: would be a great use case for the append blobs, once implemented in the Azure SDK
        jobStoreFileID = self._newFileID()
        encrypted = self.keyPath is not None
        if encrypted:
            statsAndLoggingString = encryption.encrypt(statsAndLoggingString, self.keyPath)
        self.statsFiles.put_block_blob_from_text(blob_name=jobStoreFileID,
                                                 text=statsAndLoggingString,
                                                 x_ms_meta_name_values=dict(
                                                     encrypted=str(encrypted)))
        self.statsFileIDs.insert_entity(entity={'RowKey': jobStoreFileID})

    def readStatsAndLogging(self, callback, readAll=False):
        suffix = '_old'
        numStatsFiles = 0
        for entity in self.statsFileIDs.query_entities():
            jobStoreFileID = entity.RowKey
            hasBeenRead = len(jobStoreFileID) > self.jobIDLength
            if not hasBeenRead:
                with self._downloadStream(jobStoreFileID, self.statsFiles) as fd:
                    callback(fd)
                # Mark this entity as read by appending the suffix
                self.statsFileIDs.insert_entity(entity={'RowKey': jobStoreFileID + suffix})
                self.statsFileIDs.delete_entity(row_key=jobStoreFileID)
                numStatsFiles += 1
            elif readAll:
                # Strip the suffix to get the original ID
                jobStoreFileID = jobStoreFileID[:-len(suffix)]
                with self._downloadStream(jobStoreFileID, self.statsFiles) as fd:
                    callback(fd)
                numStatsFiles += 1
        return numStatsFiles

    _azureTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    def getPublicUrl(self, jobStoreFileID):
        try:
            self.files.get_blob_properties(blob_name=jobStoreFileID)
        except AzureMissingResourceHttpError:
            raise NoSuchFileException(jobStoreFileID)
        # Compensate of a little bit of clock skew
        startTimeStr = (datetime.utcnow() - timedelta(minutes=5)).strftime(self._azureTimeFormat)
        endTime = datetime.utcnow() + self.publicUrlExpiration
        endTimeStr = endTime.strftime(self._azureTimeFormat)
        sap = SharedAccessPolicy(AccessPolicy(startTimeStr, endTimeStr,
                                              BlobSharedAccessPermissions.READ))
        sas_token = self.files.generate_shared_access_signature(blob_name=jobStoreFileID,
                                                                shared_access_policy=sap)
        return self.files.make_blob_url(blob_name=jobStoreFileID) + '?' + sas_token

    def getSharedPublicUrl(self, sharedFileName):
        jobStoreFileID = self._newFileID(sharedFileName)
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
    # https://github.com/Azure/azure-storage-python/blob/4c7666e05a9556c10154508335738ee44d7cb104/azure/storage/blob/blobservice.py#L106
    _maxAzureBlockBytes = 4 * 1024 * 1024

    @contextmanager
    def _uploadStream(self, jobStoreFileID, container, checkForModification=False, encrypted=None):
        """
        :param encrypted: True to enforce encryption (will raise exception unless key is set),
        False to prevent encryption or None to encrypt if key is set.
        """
        if checkForModification:
            try:
                expectedVersion = container.get_blob_properties(blob_name=jobStoreFileID)['etag']
            except AzureMissingResourceHttpError:
                expectedVersion = None

        if encrypted is None:
            encrypted = self.keyPath is not None
        elif encrypted:
            if self.keyPath is None:
                raise RuntimeError('Encryption requested but no key was provided')

        maxBlockSize = self._maxAzureBlockBytes
        if encrypted:
            # There is a small overhead for encrypted data.
            maxBlockSize -= encryption.overhead
        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def reader():
                    blockIDs = []
                    try:
                        while True:
                            buf = readable.read(maxBlockSize)
                            if len(buf) == 0:
                                # We're safe to break here even if we never read anything, since
                                # putting an empty block list creates an empty blob.
                                break
                            if encrypted:
                                buf = encryption.encrypt(buf, self.keyPath)
                            blockID = self._newFileID()
                            container.put_block(blob_name=jobStoreFileID,
                                                block=buf,
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
                                                       x_ms_lease_action='acquire')['x-ms-lease-id']
                        # check for modification,
                        blobProperties = container.get_blob_properties(blob_name=jobStoreFileID)
                        if blobProperties['etag'] != expectedVersion:
                            container.lease_blob(blob_name=jobStoreFileID,
                                                 x_ms_lease_action='release',
                                                 x_ms_lease_id=leaseID)
                            raise ConcurrentFileModificationException(jobStoreFileID)
                        # commit the file,
                        container.put_block_list(blob_name=jobStoreFileID,
                                                 block_list=blockIDs,
                                                 x_ms_lease_id=leaseID,
                                                 x_ms_meta_name_values=dict(
                                                     encrypted=str(encrypted)))
                        # then release the lock.
                        container.lease_blob(blob_name=jobStoreFileID,
                                             x_ms_lease_action='release',
                                             x_ms_lease_id=leaseID)
                    else:
                        # No need to check for modification, just blindly write over whatever
                        # was there.
                        container.put_block_list(blob_name=jobStoreFileID,
                                                 block_list=blockIDs,
                                                 x_ms_meta_name_values=dict(
                                                     encrypted=str(encrypted)))

                thread = ExceptionalThread(target=reader)
                thread.start()
                yield writable
            # The writable is now closed. This will send EOF to the readable and cause that
            # thread to finish.
            thread.join()

    @contextmanager
    def _downloadStream(self, jobStoreFileID, container):
        # The reason this is not in the writer is so we catch non-existant blobs early

        blobProps = container.get_blob_properties(blob_name=jobStoreFileID)

        encrypted = strict_bool(blobProps['x-ms-meta-encrypted'])
        if encrypted and self.keyPath is None:
            raise AssertionError('Content is encrypted but no key was provided.')

        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def writer():
                    try:
                        chunkStartPos = 0
                        fileSize = int(blobProps['Content-Length'])
                        while chunkStartPos < fileSize:
                            chunkEndPos = chunkStartPos + self._maxAzureBlockBytes - 1
                            buf = container.get_blob(blob_name=jobStoreFileID,
                                                     x_ms_range="bytes=%d-%d" % (chunkStartPos,
                                                                                 chunkEndPos))
                            if encrypted:
                                buf = encryption.decrypt(buf, self.keyPath)
                            writable.write(buf)
                            chunkStartPos = chunkEndPos + 1
                    finally:
                        # Ensure readers aren't left blocking if this thread crashes.
                        # This close() will send EOF to the reading end and ultimately cause the
                        # yield to return. It also makes the implict .close() done by the enclosing
                        # "with" context redundant but that should be ok since .close() on file
                        # objects are idempotent.
                        writable.close()

                thread = ExceptionalThread(target=writer)
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
        except AzureMissingResourceHttpError:
            return None
    
    def query_entities_auto(self, **kwargs):
        """
        An automatically-paged version of query_entities. The iterator just
        yields all entities matching the query, occasionally going back to Azure
        for the next page.
        """
        
        # We need to page through the results, since we only get some of them at
        # a time. Just like in the BlobService. See the only documentation
        # available: the API bindings source code, at:
        # https://github.com/Azure/azure-storage-python/blob/09e9f186740407672777d6cb6646c33a2273e1a8/azure/storage/table/tableservice.py#L385
        
        # These two together constitute the primary key for an item. 
        next_partition_key = None
        next_row_key = None
    
        while True:
            # Get a page (up to 1000 items)
            kwargs['next_partition_key'] = next_partition_key
            kwargs['next_row_key'] = next_row_key
            page = self.query_entities(**kwargs)
            
            for result in page:
                # Yield each item one at a time
                yield result
                
            if hasattr(page, 'x_ms_continuation'):
                # Next time ask for the next page. If you use .get() you need
                # the lower-case versions, but this is some kind of fancy case-
                # insensitive dictionary.
                next_partition_key = page.x_ms_continuation['NextPartitionKey']
                next_row_key = page.x_ms_continuation['NextRowKey']
            else:
                # No continuation to check
                next_partition_key = None
                next_row_key = None
            
            if not next_partition_key and not next_row_key:
                # If we run out of pages, stop
                break


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
            wholeJobString = chunkedJob[0][1].value
        else:
            wholeJobString = ''.join(item[1].value for item in chunkedJob)
        return cPickle.loads(bz2.decompress(wholeJobString))

    def toItem(self, chunkSize=maxAzureTablePropertySize):
        """
        :param chunkSize: the size of a chunk for splitting up the serialized job into chunks
        that each fit into a property value of the an Azure table entity
        :rtype: dict
        """
        assert chunkSize <= maxAzureTablePropertySize
        item = {}
        serializedAndEncodedJob = bz2.compress(cPickle.dumps(self))
        jobChunks = [serializedAndEncodedJob[i:i + chunkSize]
                     for i in range(0, len(serializedAndEncodedJob), chunkSize)]
        for attributeOrder, chunk in enumerate(jobChunks):
            item['_' + str(attributeOrder).zfill(3)] = EntityProperty('Edm.Binary', chunk)
        return item


def retryOnAzureTimeout(exception):
    timeoutMsg = "could not be completed within the specified time"
    busyMsg = "Service Unavailable"
    return (isinstance(exception, AzureException) and
            (timeoutMsg in str(exception) or busyMsg in str(exception)))


def retry_on_error(num_tries=5, retriable_exceptions=(socket.error, socket.gaierror,
                                                      httplib.HTTPException, requests.ConnectionError),
                   retriable_check=retryOnAzureTimeout):
    """
    Retries on a set of allowable exceptions, retrying temporary Azure errors by default.

    :param num_tries: number of times to try before giving up.
    :param retriable_exceptions: a tuple of exceptions that should always be retried.
    :param retriable_check: a function that takes an exception not in retriable_exceptions \
    and returns True if it should be retried, and False otherwise.

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

    Retriable check function works as expected
    >>> i = 0
    >>> for attempt in retry_on_error(num_tries=5, retriable_exceptions=(),
    ...                               retriable_check=lambda x: str(x) == 'foo'):
    ...     with attempt:
    ...         i += 1
    ...         if i == 3:
    ...             raise RuntimeError("bar")
    ...         else:
    ...             raise RuntimeError("foo")
    Traceback (most recent call last):
    ...
    RuntimeError: bar
    >>> i
    3

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
            # Any instance of these exceptions is automatically retriable.
            if last:
                raise
            else:
                logger.info("Got a retriable exception %s, trying again" % e.__class__.__name__)
        except Exception as e:
            # For other exceptions, the retriable_check function determines whether to retry
            if retriable_check(e):
                logger.info("Exception %s passed predicate, trying again" % e.__class__.__name__)
            else:
                raise
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
