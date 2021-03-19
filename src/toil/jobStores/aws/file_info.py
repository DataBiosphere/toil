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
import os
import hashlib
import itertools
import logging
import pickle
import re
import reprlib
import stat
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager
from io import BytesIO
from typing import Optional, Tuple

import boto.sdb
from boto.exception import SDBResponseError
from botocore.exceptions import ClientError

import toil.lib.encryption as encryption
from toil.fileStores import FileID
from toil.jobStores.abstractJobStore import (AbstractJobStore,
                                             ConcurrentFileModificationException,
                                             JobStoreExistsException,
                                             NoSuchFileException,
                                             NoSuchJobException,
                                             NoSuchJobStoreException)
from toil.jobStores.aws.utils import (SDBHelper,
                                      bucket_location_to_region,
                                      uploadFromPath,
                                      uploadFile,
                                      copyKeyMultipart,
                                      fileSizeAndTime,
                                      region_to_bucket_location)
from toil.lib.pipes import (ReadablePipe,
                            ReadableTransformingPipe,
                            WritablePipe)
from toil.lib.checksum import compute_checksum_for_file
from toil.lib.compatibility import compat_bytes
from toil.lib.ec2 import establish_boto3_session
from toil.lib.aws.s3 import MultiPartPipe, SinglePartPipe
from toil.lib.ec2nodes import EC2Regions
from toil.lib.exceptions import panic
from toil.lib.memoize import strict_bool
from toil.lib.io import AtomicFileCreate
from toil.lib.objects import InnerClass
from toil.lib.retry import retry

boto3_session = establish_boto3_session()
s3_boto3_resource = boto3_session.resource('s3')
s3_boto3_client = boto3_session.client('s3')
logger = logging.getLogger(__name__)


class ChecksumError(Exception):
    """Raised when a download from AWS does not contain the correct data."""


class AWSFile(SDBHelper):
    def __init__(self,
                 fileID,
                 ownerID,
                 encrypted,
                 version=None,
                 content=None,
                 numContentChunks=0,
                 checksum=None,
                 sseKeyPath=None):
        """
        :type fileID: str
        :param fileID: the file's ID

        :type ownerID: str
        :param ownerID: ID of the entity owning this file, typically a job ID aka jobStoreID

        :type encrypted: bool
        :param encrypted: whether the file is stored in encrypted form

        :type version: str|None
        :param version: a non-empty string containing the most recent version of the S3
        object storing this file's content, None if the file is new, or empty string if the
        file is inlined.

        :type content: str|None
        :param content: this file's inlined content

        :type numContentChunks: int
        :param numContentChunks: the number of SDB domain attributes occupied by this files

        :type checksum: str|None
        :param checksum: the checksum of the file, if available. Formatted
        as <algorithm>$<lowercase hex hash>.

        inlined content. Note that an inlined empty string still occupies one chunk.
        """
        super(AWSFile, self).__init__()
        self.fileID = fileID
        self.ownerID = ownerID
        self.encrypted = encrypted
        self._version = version
        self._previousVersion = version
        assert content is None or isinstance(content, bytes)
        self._content = content
        self.checksum = checksum
        self._numContentChunks = numContentChunks
        self.sseKeyPath = sseKeyPath

    @property
    def previousVersion(self):
        return self._previousVersion

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        # Version should only change once
        assert self._previousVersion == self._version
        self._version = version
        if version:
            self.content = None

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, content):
        assert content is None or isinstance(content, bytes)
        self._content = content
        if content is not None:
            self.version = ''

    def create(self, ownerID):
        return AWSFile(str(uuid.uuid4()), ownerID, encrypted=self.sseKeyPath is not None)

    @classmethod
    def exists(cls, jobStoreFileID):
        # add retry
        # check if jobStoreFileID key in dynamodb
        return True

    @classmethod
    def load(cls, jobStoreFileID, check: bool = True):
        """If check if True, this will raise if the file does not exist."""
        # add retry
        # return dictionary value from jobStoreFileID key in dynamodb
        return# item

    def fromItem(self, item):
        """Convert a DB item to an instance of this class."""
        return AWSFile(fileID=item.name,
                       ownerID=item.get('ownerID', None),
                       encrypted=item.get('encrypted', None),
                       version=item.get('version', None),
                       checksum=item.get('checksum', None))

    def toItem(self):
        """
        Convert this instance to an attribute dictionary suitable for SDB put_attributes().

        :rtype: (dict,int)

        :return: the attributes dict and an integer specifying the the number of chunk
                 attributes in the dictionary that are used for storing inlined content.
        """
        return dict(fileID=self.fileID,
                    ownerID=self.ownerID,
                    encrypted=self.encrypted,
                    version=self.version or None,
                    checksum=self.checksum or None)

    @staticmethod
    def maxInlinedSize():
        return 256

    def save(self):
        # delete old version of object
        # put metadata into db/bucket
        pass

    def upload(self, localFilePath, calculateChecksum=True):
        # actually upload content into s3
        headerArgs = self._s3EncryptionArgs()
        # Create a new Resource in case it needs to be on its own thread
        resource = boto3_session.resource('s3', region_name=self.outer.region)

        self.checksum = compute_checksum_for_file(localFilePath) if calculateChecksum else None
        self.version = uploadFromPath(localFilePath,
                                      resource=resource,
                                      bucketName=self.outer.filesBucket.name,
                                      fileID=compat_bytes(self.fileID),
                                      headerArgs=headerArgs,
                                      partSize=self.outer.partSize)

    def _update_checksum(self, checksum_in_progress, data):
        """
        Update a checksum in progress from _start_checksum with new data.
        """
        checksum_in_progress[1].update(data)

    def _finish_checksum(self, checksum_in_progress):
        """
        Complete a checksum in progress from _start_checksum and return the
        checksum result string.
        """

        result_hash = checksum_in_progress[1].hexdigest()

        logger.debug(f'Completed checksum with hash {result_hash} vs. expected {checksum_in_progress[2]}')
        if checksum_in_progress[2] is not None:
            # We expected a particular hash
            if result_hash != checksum_in_progress[2]:
                raise ChecksumError('Checksum mismatch. Expected: %s Actual: %s' %
                                    (checksum_in_progress[2], result_hash))

        return '$'.join([checksum_in_progress[0], result_hash])

    @contextmanager
    def uploadStream(self, multipart=True, allowInlining=True):
        """
        Context manager that gives out a binary-mode upload stream to upload data.
        """

        # Note that we have to handle already having a content or a version
        # if we are overwriting something.

        # But make sure we don't have both.
        assert not (bool(self.version) and self.content is not None)

        info = self
        store = self.outer

        pipe = MultiPartPipe() if multipart else SinglePartPipe()
        with pipe as writable:
            yield writable

        if not pipe.reader_done:
            logger.debug('Version: {} Content: {}'.format(self.version, self.content))
            raise RuntimeError('Escaped context manager without written data being read!')

        # We check our work to make sure we have exactly one of embedded
        # content or a real object version.

        if self.content is None:
            if not bool(self.version):
                logger.debug('Version: {} Content: {}'.format(self.version, self.content))
                raise RuntimeError('No content added and no version created')
        else:
            if bool(self.version):
                logger.debug('Version: {} Content: {}'.format(self.version, self.content))
                raise RuntimeError('Content added and version created')

    def copyFrom(self, srcObj):
        """
        Copies contents of source key into this file.

        :param S3.Object srcObj: The key (object) that will be copied from
        """
        assert srcObj.content_length is not None
        if srcObj.content_length <= self.maxInlinedSize():
            self.content = srcObj.get().get('Body').read()
        else:
            # Create a new Resource in case it needs to be on its own thread
            resource = boto3_session.resource('s3', region_name=self.outer.region)
            self.version = copyKeyMultipart(resource,
                                            srcBucketName=compat_bytes(srcObj.bucket_name),
                                            srcKeyName=compat_bytes(srcObj.key),
                                            srcKeyVersion=compat_bytes(srcObj.version_id),
                                            dstBucketName=compat_bytes(self.outer.filesBucket.name),
                                            dstKeyName=compat_bytes(self._fileID),
                                            sseAlgorithm='AES256',
                                            sseKey=self._getSSEKey())

    def copyTo(self, dstObj):
        """
        Copies contents of this file to the given key.

        :param S3.Object dstObj: The key (object) to copy this file's content to
        """
        if self.content is not None:
            dstObj.put(Body=self.content)
        elif self.version:
            # Create a new Resource in case it needs to be on its own thread
            resource = boto3_session.resource('s3', region_name=self.outer.region)

            copyKeyMultipart(resource,
                             srcBucketName=compat_bytes(self.outer.filesBucket.name),
                             srcKeyName=compat_bytes(self.fileID),
                             srcKeyVersion=compat_bytes(self.version),
                             dstBucketName=compat_bytes(dstObj.bucket_name),
                             dstKeyName=compat_bytes(dstObj.key),
                             copySourceSseAlgorithm='AES256',
                             copySourceSseKey=self._getSSEKey())
        else:
            assert False

    def download(self, localFilePath, verifyChecksum=True):
        if self.content is not None:
            with AtomicFileCreate(localFilePath) as tmpPath:
                with open(tmpPath, 'wb') as f:
                    f.write(self.content)
        elif self.version:
            headerArgs = self._s3EncryptionArgs()
            obj = self.outer.filesBucket.Object(compat_bytes(self.fileID))

            with AtomicFileCreate(localFilePath) as tmpPath:
                obj.download_file(Filename=tmpPath, ExtraArgs={'VersionId': self.version, **headerArgs})

            if verifyChecksum and self.checksum:
                algorithm, expected_checksum = self.checksum.split('$')
                computed = compute_checksum_for_file(localFilePath, algorithm=algorithm)
                if self.checksum != computed:
                    raise ChecksumError(f'Checksum mismatch for file {localFilePath}. '
                                        f'Expected: {self.checksum} Actual: {computed}')
                # The error will get caught and result in a retry of the download until we run out of retries.
                # TODO: handle obviously truncated downloads by resuming instead.
        else:
            assert False

    @contextmanager
    def downloadStream(self, verifyChecksum=True):
        info = self

        class DownloadPipe(ReadablePipe):
            def writeTo(self, writable):
                if info.content is not None:
                    writable.write(info.content)
                elif info.version:
                    headerArgs = info._s3EncryptionArgs()
                    obj = info.outer.filesBucket.Object(compat_bytes(info.fileID))
                    obj.download_fileobj(writable, ExtraArgs={'VersionId': info.version, **headerArgs})
                else:
                    assert False

        class HashingPipe(ReadableTransformingPipe):
            """
            Class which checksums all the data read through it. If it
            reaches EOF and the checksum isn't correct, raises
            ChecksumError.

            Assumes info actually has a checksum.
            """

            def transform(self, readable, writable):
                algorithm, _ = info.checksum.split('$')
                hasher = getattr(hashlib, algorithm)()
                contents = readable.read(1024 * 1024)
                while contents != b'':
                    hasher.update(contents)
                    try:
                        writable.write(contents)
                    except BrokenPipeError:
                        # Read was stopped early by user code.
                        # Can't check the checksum.
                        return
                    contents = readable.read(1024 * 1024)
                # We reached EOF in the input.
                # Finish checksumming and verify.
                result_hash = hasher.hexdigest()
                if f'{algorithm}${result_hash}' != info.checksum:
                    raise ChecksumError('')
                # Now stop so EOF happens in the output.

        with DownloadPipe() as readable:
            if verifyChecksum and self.checksum:
                # Interpose a pipe to check the hash
                with HashingPipe(readable) as verified:
                    yield verified
            else:
                # No true checksum available, so don't hash
                yield readable

    def delete(self):
        store = self.outer
        if self.previousVersion is not None:
            store.filesDomain.delete_attributes(
                compat_bytes(self.fileID),
                expected_values=['version', self.previousVersion])
            if self.previousVersion:
                store.s3_client.delete_object(Bucket=store.filesBucket.name,
                                              Key=compat_bytes(self.fileID),
                                              VersionId=self.previousVersion)

    def getSize(self):
        """
        Return the size of the referenced item in bytes.
        """
        if self.content is not None:
            return len(self.content)
        elif self.version:
            obj = self.outer.filesBucket.Object(compat_bytes(self.fileID))
            return obj.content_length
        else:
            return 0

    def _getSSEKey(self):
        sseKeyPath = self.outer.sseKeyPath
        if sseKeyPath is None:
            return None
        else:
            with open(sseKeyPath, 'rb') as f:
                sseKey = f.read()
                return sseKey

    def _s3EncryptionArgs(self):
        # the keys of the returned dictionary are unpacked to the corresponding boto3 optional
        # parameters and will be used to set the http headers
        if self.encrypted:
            sseKey = self._getSSEKey()
            assert sseKey is not None, 'Content is encrypted but no key was provided.'
            assert len(sseKey) == 32
            # boto3 encodes the key and calculates the MD5 for us
            return {'SSECustomerAlgorithm': 'AES256', 'SSECustomerKey': sseKey}
        return {}

    def __repr__(self):
        aRepr = reprlib.Repr()
        aRepr.maxstring = 38  # so UUIDs don't get truncated (36 for UUID plus 2 for quotes)
        r = custom_repr = aRepr.repr
        d = (('fileID', r(self.fileID)),
             ('ownerID', r(self.ownerID)),
             ('encrypted', r(self.encrypted)),
             ('version', r(self.version)),
             ('previousVersion', r(self.previousVersion)),
             ('content', r(self.content)),
             ('checksum', r(self.checksum)),
             ('_numContentChunks', r(self._numContentChunks)))
        return "{}({})".format(type(self).__name__, ', '.join('%s=%s' % (k, v) for k, v in d))
