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

import bz2
import cPickle
import httplib
import inspect
import logging
import os
import re
import socket
import uuid
from ConfigParser import RawConfigParser, NoOptionError
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime, timedelta

from azure.common import AzureMissingResourceHttpError, AzureException
from azure.storage import SharedAccessPolicy, AccessPolicy
from azure.storage.blob import BlobService, BlobSharedAccessPermissions
from azure.storage.table import TableService, EntityProperty

# noinspection PyPackageRequirements
# (pulled in transitively)
import requests
from bd2k.util import strict_bool, memoize
from bd2k.util.exceptions import panic
from bd2k.util.retry import retry

from toil.jobStores.utils import WritablePipe, ReadablePipe
from toil.jobWrapper import JobWrapper
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             NoSuchJobException,
                                             ConcurrentFileModificationException,
                                             NoSuchFileException,
                                             InvalidImportExportUrlException,
                                             JobStoreExistsException,
                                             NoSuchJobStoreException)
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
    A job store that uses Azure's blob store for file storage and Table Service to store job info
    with strong consistency.
    """

    # Dots in container names should be avoided because container names are used in HTTPS bucket
    # URLs where the may interfere with the certificate common name. We use a double underscore
    # as a separator instead.
    #
    containerNameRe = re.compile(r'^[a-z0-9](-?[a-z0-9]+)+[a-z0-9]$')

    # See https://msdn.microsoft.com/en-us/library/azure/dd135715.aspx
    #
    minContainerNameLen = 3
    maxContainerNameLen = 63
    maxNameLen = 10
    nameSeparator = 'xx'  # Table names must be alphanumeric
    # Length of a jobID - used to test if a stats file has been read already or not
    jobIDLength = len(str(uuid.uuid4()))

    def __init__(self, locator, jobChunkSize=maxAzureTablePropertySize):
        super(AzureJobStore, self).__init__()
        accountName, namePrefix = locator.split(':', 1)
        if '--' in namePrefix:
            raise ValueError("Invalid name prefix '%s'. Name prefixes may not contain %s."
                             % (namePrefix, self.nameSeparator))
        if not self.containerNameRe.match(namePrefix):
            raise ValueError("Invalid name prefix '%s'. Name prefixes must contain only digits, "
                             "hyphens or lower-case letters and must not start or end in a "
                             "hyphen." % namePrefix)
        # Reserve 13 for separator and suffix
        if len(namePrefix) > self.maxContainerNameLen - self.maxNameLen - len(self.nameSeparator):
            raise ValueError(("Invalid name prefix '%s'. Name prefixes may not be longer than 50 "
                              "characters." % namePrefix))
        if '--' in namePrefix:
            raise ValueError("Invalid name prefix '%s'. Name prefixes may not contain "
                             "%s." % (namePrefix, self.nameSeparator))
        self.locator = locator
        self.jobChunkSize = jobChunkSize
        self.accountKey = _fetchAzureAccountKey(accountName)
        self.accountName = accountName
        # Table names have strict requirements in Azure
        self.namePrefix = self._sanitizeTableName(namePrefix)
        # These are the main API entry points.
        self.tableService = TableService(account_key=self.accountKey, account_name=accountName)
        self.blobService = BlobService(account_key=self.accountKey, account_name=accountName)
        # Serialized jobs table
        self.jobItems = None
        # Job<->file mapping table
        self.jobFileIDs = None
        # Container for all shared and unshared files
        self.files = None
        # Stats and logging strings
        self.statsFiles = None
        # File IDs that contain stats and logging strings
        self.statsFileIDs = None

    @property
    def keyPath(self):
        return self.config.cseKey

    def initialize(self, config):
        if self._jobStoreExists():
            raise JobStoreExistsException(self.locator)
        logger.debug("Creating job store at '%s'" % self.locator)
        self._bind(create=True)
        super(AzureJobStore, self).initialize(config)

    def resume(self):
        if not self._jobStoreExists():
            raise NoSuchJobStoreException(self.locator)
        logger.debug("Using existing job store at '%s'" % self.locator)
        self._bind(create=False)
        super(AzureJobStore, self).resume()

    def destroy(self):
        for name in 'jobItems', 'jobFileIDs', 'files', 'statsFiles', 'statsFileIDs':
            resource = getattr(self, name)
            if resource is not None:
                if isinstance(resource, AzureTable):
                    resource.delete_table()
                elif isinstance(resource, AzureBlobContainer):
                    resource.delete_container()
                else:
                    assert False
                setattr(self, name, None)

    def _jobStoreExists(self):
        """
        Checks if job store exists by querying the existence of the statsFileIDs table. Note that
        this is the last component that is deleted in :meth:`.destroy`.
        """
        for attempt in retry_azure():
            with attempt:
                try:
                    table = self.tableService.query_tables(table_name=self._qualify('statsFileIDs'))
                except AzureMissingResourceHttpError as e:
                    if e.status_code == 404:
                        return False
                    else:
                        raise
                else:
                    return table is not None

    def _bind(self, create=False):
        table = self._bindTable
        container = self._bindContainer
        for name, binder in (('jobItems', table),
                             ('jobFileIDs', table),
                             ('files', container),
                             ('statsFiles', container),
                             ('statsFileIDs', table)):
            if getattr(self, name) is None:
                setattr(self, name, binder(self._qualify(name), create=create))

    def _qualify(self, name):
        return self.namePrefix + self.nameSeparator + name.lower()

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

    def getEnv(self):
        return dict(AZURE_ACCOUNT_KEY=self.accountKey)

    class BlobInfo(namedtuple('BlobInfo', ('account', 'container', 'name'))):
        @property
        @memoize
        def service(self):
            return BlobService(account_name=self.account,
                               account_key=_fetchAzureAccountKey(self.account))

    @classmethod
    def _readFromUrl(cls, url, writable):
        blob = cls._parseWasbUrl(url)
        blob.service.get_blob_to_file(container_name=blob.container,
                                      blob_name=blob.name,
                                      stream=writable)

    @classmethod
    def _writeToUrl(cls, readable, url):
        blob = cls._parseWasbUrl(url)
        blob.service.put_block_blob_from_file(container_name=blob.container,
                                              blob_name=blob.name,
                                              stream=readable)

    @classmethod
    def _parseWasbUrl(cls, url):
        """
        :param urlparse.ParseResult url: x
        :rtype: AzureJobStore.BlobInfo
        """
        assert url.scheme in ('wasb', 'wasbs')
        try:
            container, account = url.netloc.split('@')
        except ValueError:
            raise InvalidImportExportUrlException(url)
        suffix = '.blob.core.windows.net'
        if account.endswith(suffix):
            account = account[:-len(suffix)]
        else:
            raise InvalidImportExportUrlException(url)
        assert url.path[0] == '/'
        return cls.BlobInfo(account=account, container=container, name=url.path[1:])

    @classmethod
    def _supportsUrl(cls, url, export=False):
        return url.scheme.lower() in ('wasb', 'wasbs')

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
                        if not buf:
                            break
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
        assert self._validateSharedFileName(sharedFileName)
        sharedFileID = self._newFileID(sharedFileName)
        with self._uploadStream(sharedFileID, self.files, encrypted=isProtected) as fd:
            yield fd

    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        assert self._validateSharedFileName(sharedFileName)
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

    def _bindTable(self, tableName, create=False):
        for attempt in retry_azure():
            with attempt:
                try:
                    tables = self.tableService.query_tables(table_name=tableName)
                except AzureMissingResourceHttpError as e:
                    if e.status_code != 404:
                        raise
                else:
                    if tables:
                        assert tables[0].name == tableName
                        return AzureTable(self.tableService, tableName)
                if create:
                    self.tableService.create_table(tableName)
                    return AzureTable(self.tableService, tableName)
                else:
                    return None

    def _bindContainer(self, containerName, create=False):
        for attempt in retry_azure():
            with attempt:
                try:
                    self.blobService.get_container_properties(containerName)
                except AzureMissingResourceHttpError as e:
                    if e.status_code == 404:
                        if create:
                            self.blobService.create_container(containerName)
                        else:
                            return None
                    else:
                        raise
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

        store = self

        class UploadPipe(WritablePipe):

            def readFrom(self, readable):
                blockIDs = []
                try:
                    while True:
                        buf = readable.read(maxBlockSize)
                        if len(buf) == 0:
                            # We're safe to break here even if we never read anything, since
                            # putting an empty block list creates an empty blob.
                            break
                        if encrypted:
                            buf = encryption.encrypt(buf, store.keyPath)
                        blockID = store._newFileID()
                        container.put_block(blob_name=jobStoreFileID,
                                            block=buf,
                                            blockid=blockID)
                        blockIDs.append(blockID)
                except:
                    with panic(log=logger):
                        # This is guaranteed to delete any uncommitted blocks.
                        container.delete_blob(blob_name=jobStoreFileID)

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
                                             x_ms_meta_name_values=dict(encrypted=str(encrypted)))

        with UploadPipe() as writable:
            yield writable

    @contextmanager
    def _downloadStream(self, jobStoreFileID, container):
        # The reason this is not in the writer is so we catch non-existant blobs early

        blobProps = container.get_blob_properties(blob_name=jobStoreFileID)

        encrypted = strict_bool(blobProps['x-ms-meta-encrypted'])
        if encrypted and self.keyPath is None:
            raise AssertionError('Content is encrypted but no key was provided.')

        outer_self = self

        class DownloadPipe(ReadablePipe):
            def writeTo(self, writable):
                chunkStart = 0
                fileSize = int(blobProps['Content-Length'])
                while chunkStart < fileSize:
                    chunkEnd = chunkStart + outer_self._maxAzureBlockBytes - 1
                    buf = container.get_blob(blob_name=jobStoreFileID,
                                             x_ms_range="bytes=%d-%d" % (chunkStart, chunkEnd))
                    if encrypted:
                        buf = encryption.decrypt(buf, outer_self.keyPath)
                    writable.write(buf)
                    chunkStart = chunkEnd + 1

        with DownloadPipe() as readable:
            yield readable


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

            for attempt in retry_azure():
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

    To avoid confusion over the position of any remaining positional arguments, all method calls
    must use *only* keyword arguments.
    """

    def __init__(self, blobService, containerName):
        self.blobService = blobService
        self.containerName = containerName

    def __getattr__(self, name):
        def f(*args, **kwargs):
            assert len(args) == 0
            function = getattr(self.blobService, name)
            kwargs['container_name'] = self.containerName

            for attempt in retry_azure():
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


def defaultRetryPredicate(exception):
    """
    >>> defaultRetryPredicate(socket.error())
    True
    >>> defaultRetryPredicate(socket.gaierror())
    True
    >>> defaultRetryPredicate(httplib.HTTPException())
    True
    >>> defaultRetryPredicate(requests.ConnectionError())
    True
    >>> defaultRetryPredicate(AzureException('x could not be completed within the specified time'))
    True
    >>> defaultRetryPredicate(AzureException('x service unavailable'))
    True
    >>> defaultRetryPredicate(AzureException('x server is busy'))
    True
    >>> defaultRetryPredicate(AzureException('x'))
    False
    >>> defaultRetryPredicate(RuntimeError())
    False
    """
    return (isinstance(exception, (socket.error,
                                   socket.gaierror,
                                   httplib.HTTPException,
                                   requests.ConnectionError))
            or isinstance(exception, AzureException) and
            any(message in str(exception).lower() for message in (
                "could not be completed within the specified time",
                "service unavailable",
                "server is busy")))


def retry_azure(delays=(0, 1, 1, 4, 16, 64), timeout=300, predicate=defaultRetryPredicate):
    return retry(delays=delays, timeout=timeout, predicate=predicate)
