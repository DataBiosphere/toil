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
import base64
import bz2
import errno
import logging
import os
import socket
from ssl import SSLError
from typing import Optional

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from toil.lib.compatibility import compat_bytes
from toil.lib.retry import retry, ErrorCondition

logger = logging.getLogger(__name__)


class SDBHelper(object):
    """
    A mixin with methods for storing limited amounts of binary data in an SDB item

    >>> import os
    >>> H=SDBHelper
    >>> H.presenceIndicator() # doctest: +ALLOW_UNICODE
    u'numChunks'
    >>> H.binaryToAttributes(None)['numChunks']
    0
    >>> H.attributesToBinary({u'numChunks': 0})
    (None, 0)
    >>> H.binaryToAttributes(b'') # doctest: +ALLOW_UNICODE +ALLOW_BYTES
    {u'000': b'VQ==', u'numChunks': 1}
    >>> H.attributesToBinary({u'numChunks': 1, u'000': b'VQ=='}) # doctest: +ALLOW_BYTES
    (b'', 1)

    Good pseudo-random data is very likely smaller than its bzip2ed form. Subtract 1 for the type
    character, i.e  'C' or 'U', with which the string is prefixed. We should get one full chunk:

    >>> s = os.urandom(H.maxRawValueSize-1)
    >>> d = H.binaryToAttributes(s)
    >>> len(d), len(d['000'])
    (2, 1024)
    >>> H.attributesToBinary(d) == (s, 1)
    True

    One byte more and we should overflow four bytes into the second chunk, two bytes for
    base64-encoding the additional character and two bytes for base64-padding to the next quartet.

    >>> s += s[0:1]
    >>> d = H.binaryToAttributes(s)
    >>> len(d), len(d['000']), len(d['001'])
    (3, 1024, 4)
    >>> H.attributesToBinary(d) == (s, 2)
    True

    """
    # The SDB documentation is not clear as to whether the attribute value size limit of 1024
    # applies to the base64-encoded value or the raw value. It suggests that responses are
    # automatically encoded from which I conclude that the limit should apply to the raw,
    # unencoded value. However, there seems to be a discrepancy between how Boto computes the
    # request signature if a value contains a binary data, and how SDB does it. This causes
    # requests to fail signature verification, resulting in a 403. We therefore have to
    # base64-encode values ourselves even if that means we loose a quarter of capacity.

    maxAttributesPerItem = 256
    maxValueSize = 1024
    maxRawValueSize = maxValueSize * 3 // 4
    # Just make sure we don't have a problem with padding or integer truncation:
    assert len(base64.b64encode(b' ' * maxRawValueSize)) == 1024
    assert len(base64.b64encode(b' ' * (1 + maxRawValueSize))) > 1024

    @classmethod
    def _reservedAttributes(cls):
        """
        Override in subclass to reserve a certain number of attributes that can't be used for
        chunks.
        """
        return 1

    @classmethod
    def _maxChunks(cls):
        return cls.maxAttributesPerItem - cls._reservedAttributes()

    @classmethod
    def maxBinarySize(cls, extraReservedChunks=0):
        return (cls._maxChunks() - extraReservedChunks) * cls.maxRawValueSize - 1  # for the 'C' or 'U' prefix

    @classmethod
    def _maxEncodedSize(cls):
        return cls._maxChunks() * cls.maxValueSize

    @classmethod
    def _chunkName(cls, i):
        return str(i).zfill(3)

    @classmethod
    def _isValidChunkName(cls, s):
        return len(s) == 3 and s.isdigit()

    @classmethod
    def presenceIndicator(cls):
        """
        The key that is guaranteed to be present in the return value of binaryToAttributes().
        Assuming that binaryToAttributes() is used with SDB's PutAttributes, the return value of
        this method could be used to detect the presence/absence of an item in SDB.
        """
        return u'numChunks'

    @classmethod
    def attributesToBinary(cls, attributes):
        """
        :rtype: (str|None,int)
        :return: the binary data and the number of chunks it was composed from
        """
        chunks = [(int(k), v) for k, v in attributes.items() if cls._isValidChunkName(k)]
        chunks.sort()
        numChunks = int(attributes[u'numChunks'])
        if numChunks:
            serializedJob = b''.join(v.encode() for k, v in chunks)
            compressed = base64.b64decode(serializedJob)
            if compressed[0] == b'C'[0]:
                binary = bz2.decompress(compressed[1:])
            elif compressed[0] == b'U'[0]:
                binary = compressed[1:]
            else:
                raise RuntimeError('Unexpected prefix {}'.format(compressed[0]))
        else:
            binary = None
        return binary, numChunks


def fileSizeAndTime(localFilePath):
    file_stat = os.stat(localFilePath)
    return file_stat.st_size, file_stat.st_mtime


@retry(errors=[ErrorCondition(
    error=ClientError,
    error_codes=[404, 500, 502, 503, 504]
)])
def uploadFromPath(localFilePath: str,
                   resource,
                   bucketName: str,
                   fileID: str,
                   headerArgs: Optional[dict] = None,
                   partSize: int = 50 << 20):
    """
    Uploads a file to s3, using multipart uploading if applicable

    :param str localFilePath: Path of the file to upload to s3
    :param S3.Resource resource: boto3 resource
    :param str bucketName: name of the bucket to upload to
    :param str fileID: the name of the file to upload to
    :param dict headerArgs: http headers to use when uploading - generally used for encryption purposes
    :param int partSize: max size of each part in the multipart upload, in bytes

    :return: version of the newly uploaded file
    """
    if headerArgs is None:
        headerArgs = {}

    client = resource.meta.client
    file_size, file_time = fileSizeAndTime(localFilePath)

    version = uploadFile(localFilePath, resource, bucketName, fileID, headerArgs, partSize)
    info = client.head_object(Bucket=bucketName, Key=compat_bytes(fileID), VersionId=version, **headerArgs)
    size = info.get('ContentLength')

    assert size == file_size

    # Make reasonably sure that the file wasn't touched during the upload
    assert fileSizeAndTime(localFilePath) == (file_size, file_time)
    return version


@retry(errors=[ErrorCondition(
    error=ClientError,
    error_codes=[404, 500, 502, 503, 504]
)])
def uploadFile(readable,
               resource,
               bucketName: str,
               fileID: str,
               headerArgs: Optional[dict] = None,
               partSize: int = 50 << 20):
    """
    Upload a readable object to s3, using multipart uploading if applicable.
    :param readable: a readable stream or a file path to upload to s3
    :param S3.Resource resource: boto3 resource
    :param str bucketName: name of the bucket to upload to
    :param str fileID: the name of the file to upload to
    :param dict headerArgs: http headers to use when uploading - generally used for encryption purposes
    :param int partSize: max size of each part in the multipart upload, in bytes
    :return: version of the newly uploaded file
    """
    if headerArgs is None:
        headerArgs = {}

    client = resource.meta.client
    config = TransferConfig(
        multipart_threshold=partSize,
        multipart_chunksize=partSize,
        use_threads=True
    )
    if isinstance(readable, str):
        client.upload_file(Filename=readable,
                           Bucket=bucketName,
                           Key=compat_bytes(fileID),
                           ExtraArgs=headerArgs,
                           Config=config)
    else:
        client.upload_fileobj(Fileobj=readable,
                              Bucket=bucketName,
                              Key=compat_bytes(fileID),
                              ExtraArgs=headerArgs,
                              Config=config)

        # Wait until the object exists before calling head_object
        object_summary = resource.ObjectSummary(bucketName, compat_bytes(fileID))
        object_summary.wait_until_exists(**headerArgs)

    info = client.head_object(Bucket=bucketName, Key=compat_bytes(fileID), **headerArgs)
    return info.get('VersionId', None)


@retry(errors=[ErrorCondition(
    error=ClientError,
    error_codes=[404, 500, 502, 503, 504]
)])
def copyKeyMultipart(resource,
                     srcBucketName: str,
                     srcKeyName: str,
                     srcKeyVersion: str,
                     dstBucketName: str,
                     dstKeyName: str,
                     sseAlgorithm: Optional[str] = None,
                     sseKey: Optional[str] = None,
                     copySourceSseAlgorithm: Optional[str] = None,
                     copySourceSseKey: Optional[str] = None):
    """
    Copies a key from a source key to a destination key in multiple parts. Note that if the
    destination key exists it will be overwritten implicitly, and if it does not exist a new
    key will be created. If the destination bucket does not exist an error will be raised.

    :param S3.Resource resource: boto3 resource
    :param str srcBucketName: The name of the bucket to be copied from.
    :param str srcKeyName: The name of the key to be copied from.
    :param str srcKeyVersion: The version of the key to be copied from.
    :param str dstBucketName: The name of the destination bucket for the copy.
    :param str dstKeyName: The name of the destination key that will be created or overwritten.
    :param str sseAlgorithm: Server-side encryption algorithm for the destination.
    :param str sseKey: Server-side encryption key for the destination.
    :param str copySourceSseAlgorithm: Server-side encryption algorithm for the source.
    :param str copySourceSseKey: Server-side encryption key for the source.

    :rtype: str
    :return: The version of the copied file (or None if versioning is not enabled for dstBucket).
    """
    dstBucket = resource.Bucket(compat_bytes(dstBucketName))
    dstObject = dstBucket.Object(compat_bytes(dstKeyName))
    copySource = {'Bucket': compat_bytes(srcBucketName), 'Key': compat_bytes(srcKeyName)}
    if srcKeyVersion is not None:
        copySource['VersionId'] = compat_bytes(srcKeyVersion)

    # The boto3 functions don't allow passing parameters as None to
    # indicate they weren't provided. So we have to do a bit of work
    # to ensure we only provide the parameters when they are actually
    # required.
    destEncryptionArgs = {}
    if sseKey is not None:
        destEncryptionArgs.update({'SSECustomerAlgorithm': sseAlgorithm,
                                   'SSECustomerKey': sseKey})
    copyEncryptionArgs = {}
    if copySourceSseKey is not None:
        copyEncryptionArgs.update({'CopySourceSSECustomerAlgorithm': copySourceSseAlgorithm,
                                   'CopySourceSSECustomerKey': copySourceSseKey})
    copyEncryptionArgs.update(destEncryptionArgs)

    dstObject.copy(copySource, ExtraArgs=copyEncryptionArgs)

    # Wait until the object exists before calling head_object
    object_summary = resource.ObjectSummary(dstObject.bucket_name, dstObject.key)
    object_summary.wait_until_exists(**destEncryptionArgs)

    # Unfortunately, boto3's managed copy doesn't return the version
    # that it actually copied to. So we have to check immediately
    # after, leaving open the possibility that it may have been
    # modified again in the few seconds since the copy finished. There
    # isn't much we can do about it.
    info = resource.meta.client.head_object(Bucket=dstObject.bucket_name,
                                            Key=dstObject.key,
                                            **destEncryptionArgs)
    return info.get('VersionId', None)


def connection_reset(e):
    # For some reason we get 'error: [Errno 104] Connection reset by peer' where the
    # English description suggests that errno is 54 (ECONNRESET) while the actual
    # errno is listed as 104. To be safe, we check for both:
    return isinstance(e, socket.error) and e.errno in (errno.ECONNRESET, 104)


def retryable_ssl_error(e):
    # https://github.com/BD2KGenomics/toil/issues/978
    return isinstance(e, SSLError) and e.reason == 'DECRYPTION_FAILED_OR_BAD_RECORD_MAC'


def region_to_bucket_location(region):
    return '' if region == 'us-east-1' else region


def bucket_location_to_region(location):
    return 'us-east-1' if location == '' else location
