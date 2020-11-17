# Copyright (C) 2015-2020 Regents of the University of California
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
from past.utils import old_div
import base64
import bz2
import os
import socket
import logging
import types
import itertools
import errno

from ssl import SSLError
from six import iteritems

from toil.lib.exceptions import panic
from toil.lib.compatibility import compat_oldstr, compat_bytes, USING_PYTHON2
from toil.lib.retry import old_retry
from boto.exception import (SDBResponseError,
                            BotoServerError,
                            S3ResponseError,
                            S3CreateError,
                            S3CopyError)
import boto3

log = logging.getLogger(__name__)


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
    # in python2 1 / 2 == 0, in python 3 1 / 2 == 0.5
    # old_div implents the python2 behavior in both 2 & 3
    maxRawValueSize = old_div(maxValueSize * 3, 4)
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
    def binaryToAttributes(cls, binary):
        """
        Turn a bytestring, or None, into SimpleDB attributes.
        """
        if binary is None: return {u'numChunks': 0}
        assert isinstance(binary, bytes)
        assert len(binary) <= cls.maxBinarySize()
        # The use of compression is just an optimization. We can't include it in the maxValueSize
        # computation because the compression ratio depends on the input.
        compressed = bz2.compress(binary)
        if len(compressed) > len(binary):
            compressed = b'U' + binary
        else:
            compressed = b'C' + compressed
        encoded = base64.b64encode(compressed)
        assert len(encoded) <= cls._maxEncodedSize()
        n = cls.maxValueSize
        chunks = (encoded[i:i + n] for i in range(0, len(encoded), n))
        attributes = {cls._chunkName(i): chunk for i, chunk in enumerate(chunks)}
        attributes.update({u'numChunks': len(attributes)})
        return attributes

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
        chunks = [(int(k), v) for k, v in iteritems(attributes) if cls._isValidChunkName(k)]
        chunks.sort()
        numChunks = int(attributes[u'numChunks'])
        if numChunks:
            if USING_PYTHON2:
                serializedJob = b''.join(v for k, v in chunks)
            else:
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


from boto.sdb.connection import SDBConnection


def fileSizeAndTime(localFilePath):
    file_stat = os.stat(localFilePath)
    return file_stat.st_size, file_stat.st_mtime


def uploadFromPath(localFilePath, partSize, bucket, fileID, headers):
    """
    Uploads a file to s3, using multipart uploading if applicable

    :param str localFilePath: Path of the file to upload to s3
    :param int partSize: max size of each part in the multipart upload, in bytes
    :param boto.s3.Bucket bucket: the s3 bucket to upload to
    :param str fileID: the name of the file to upload to
    :param headers: http headers to use when uploading - generally used for encryption purposes
    :return: version of the newly uploaded file
    """
    file_size, file_time = fileSizeAndTime(localFilePath)
    if file_size <= partSize:
        key = bucket.new_key(key_name=compat_bytes(fileID))
        key.name = fileID
        for attempt in retry_s3():
            with attempt:
                key.set_contents_from_filename(localFilePath, headers=headers)
        version = key.version_id
    else:
        with open(localFilePath, 'rb') as f:
            version = chunkedFileUpload(f, bucket, fileID, file_size, headers, partSize)
    for attempt in retry_s3():
        with attempt:
            key = bucket.get_key(compat_bytes(fileID),
                                 headers=headers,
                                 version_id=version)
    assert key.size == file_size
    # Make reasonably sure that the file wasn't touched during the upload
    assert fileSizeAndTime(localFilePath) == (file_size, file_time)
    return version


def chunkedFileUpload(readable, bucket, fileID, file_size, headers=None, partSize=50 << 20):
    for attempt in retry_s3():
        with attempt:
            upload = bucket.initiate_multipart_upload(
                key_name=compat_bytes(fileID),
                headers=headers)
    try:
        start = 0
        part_num = itertools.count()
        while start < file_size:
            end = min(start + partSize, file_size)
            assert readable.tell() == start
            for attempt in retry_s3():
                with attempt:
                    upload.upload_part_from_file(fp=readable,
                                                 part_num=next(part_num) + 1,
                                                 size=end - start,
                                                 headers=headers)
            start = end
        assert readable.tell() == file_size == start
    except:
        with panic(log=log):
            for attempt in retry_s3():
                with attempt:
                    upload.cancel_upload()
    else:
        for attempt in retry_s3():
            with attempt:
                version = upload.complete_upload().version_id
    return version


def copyKeyMultipart(srcBucketName, srcKeyName, srcKeyVersion, dstBucketName, dstKeyName, sseAlgorithm=None, sseKey=None,
                     copySourceSseAlgorithm=None, copySourceSseKey=None):
    """
    Copies a key from a source key to a destination key in multiple parts. Note that if the
    destination key exists it will be overwritten implicitly, and if it does not exist a new
    key will be created. If the destination bucket does not exist an error will be raised.

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
    s3 = boto3.resource('s3')
    dstBucket = s3.Bucket(compat_oldstr(dstBucketName))
    dstObject = dstBucket.Object(compat_oldstr(dstKeyName))
    copySource = {'Bucket': compat_oldstr(srcBucketName), 'Key': compat_oldstr(srcKeyName)}
    if srcKeyVersion is not None:
        copySource['VersionId'] = compat_oldstr(srcKeyVersion)

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
    object_summary = s3.ObjectSummary(dstObject.bucket_name, dstObject.key)
    object_summary.wait_until_exists(**destEncryptionArgs)

    # Unfortunately, boto3's managed copy doesn't return the version
    # that it actually copied to. So we have to check immediately
    # after, leaving open the possibility that it may have been
    # modified again in the few seconds since the copy finished. There
    # isn't much we can do about it.
    info = boto3.client('s3').head_object(Bucket=dstObject.bucket_name, Key=dstObject.key,
                                          **destEncryptionArgs)
    return info.get('VersionId', None)

def _put_attributes_using_post(self, domain_or_name, item_name, attributes,
                               replace=True, expected_value=None):
    """
    Monkey-patched version of SDBConnection.put_attributes that uses POST instead of GET

    The GET version is subject to the URL length limit which kicks in before the 256 x 1024 limit
    for attribute values. Using POST prevents that.

    https://github.com/BD2KGenomics/toil/issues/502
    """
    domain, domain_name = self.get_domain_and_name(domain_or_name)
    params = {'DomainName': domain_name,
              'ItemName': item_name}
    self._build_name_value_list(params, attributes, replace)
    if expected_value:
        self._build_expected_value(params, expected_value)
    # The addition of the verb keyword argument is the only difference to put_attributes (Hannes)
    return self.get_status('PutAttributes', params, verb='POST')


def monkeyPatchSdbConnection(sdb):
    """
    :type sdb: SDBConnection
    """
    sdb.put_attributes = types.MethodType(_put_attributes_using_post, sdb)


default_delays = (0, 1, 1, 4, 16, 64)
default_timeout = 300


def connection_reset(e):
    # For some reason we get 'error: [Errno 104] Connection reset by peer' where the
    # English description suggests that errno is 54 (ECONNRESET) while the actual
    # errno is listed as 104. To be safe, we check for both:
    return isinstance(e, socket.error) and e.errno in (errno.ECONNRESET, 104)


def sdb_unavailable(e):
    return isinstance(e, BotoServerError) and e.status in (500, 503)


def no_such_sdb_domain(e):
    return (isinstance(e, SDBResponseError)
            and e.error_code
            and e.error_code.endswith('NoSuchDomain'))


def retryable_ssl_error(e):
    # https://github.com/BD2KGenomics/toil/issues/978
    return isinstance(e, SSLError) and e.reason == 'DECRYPTION_FAILED_OR_BAD_RECORD_MAC'


def retryable_sdb_errors(e):
    return (sdb_unavailable(e)
            or no_such_sdb_domain(e)
            or connection_reset(e)
            or retryable_ssl_error(e))


def retry_sdb(delays=default_delays, timeout=default_timeout, predicate=retryable_sdb_errors):
    return old_retry(delays=delays, timeout=timeout, predicate=predicate)


def retryable_s3_errors(e):
    return ((isinstance(e, (S3CreateError, S3ResponseError))
             and e.status == 409
             and 'try again' in e.message)
            or connection_reset(e)
            or (isinstance(e, BotoServerError) and e.status == 500)
            # Throttling response sometimes received on bucket creation
            or (isinstance(e, BotoServerError) and e.status == 503 and e.code == 'SlowDown')
            or (isinstance(e, S3CopyError) and 'try again' in e.message))


def retry_s3(delays=default_delays, timeout=default_timeout, predicate=retryable_s3_errors):
    return old_retry(delays=delays, timeout=timeout, predicate=predicate)


def region_to_bucket_location(region):
    return '' if region == 'us-east-1' else region


def bucket_location_to_region(location):
    return 'us-east-1' if location == '' else location
