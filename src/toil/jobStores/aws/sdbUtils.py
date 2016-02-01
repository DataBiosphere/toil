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

from __future__ import absolute_import
import base64
import bz2
from contextlib import contextmanager
import logging
import types
from boto.exception import SDBResponseError, BotoServerError
import time

log = logging.getLogger(__name__)


class SDBHelper(object):
    """
    A mixin with methods for storing limited amounts of binary data in an SDB item

    >>> import os
    >>> H=SDBHelper
    >>> H.binaryToAttributes(None)
    {}
    >>> H.attributesToBinary({})
    (None, 0)
    >>> H.binaryToAttributes('')
    {'000': 'VQ=='}
    >>> H.attributesToBinary({'000': 'VQ=='})
    ('', 1)

    Good pseudo-random data is very likely smaller than its bzip2ed form. Subtract 1 for the type
    character, i.e  'C' or 'U', with which the string is prefixed. We should get one full chunk:

    >>> s = os.urandom(H.maxRawValueSize-1)
    >>> d = H.binaryToAttributes(s)
    >>> len(d), len(d['000'])
    (1, 1024)
    >>> H.attributesToBinary(d) == (s, 1)
    True

    One byte more and we should overflow four bytes into the second chunk, two bytes for
    base64-encoding the additional character and two bytes for base64-padding to the next quartet.

    >>> s += s[0]
    >>> d = H.binaryToAttributes(s)
    >>> len(d), len(d['000']), len(d['001'])
    (2, 1024, 4)
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
    maxRawValueSize = maxValueSize * 3 / 4
    # Just make sure we don't have a problem with padding or integer truncation:
    assert len(base64.b64encode(' ' * maxRawValueSize)) == 1024
    assert len(base64.b64encode(' ' * (1 + maxRawValueSize))) > 1024

    @classmethod
    def _reservedAttributes(cls):
        """
        Override in subclass to reserve a certain number of attributes that can't be used for
        chunks.
        """
        return 0

    @classmethod
    def _maxChunks(cls):
        return cls.maxAttributesPerItem - cls._reservedAttributes()

    @classmethod
    def maxBinarySize(cls):
        return cls._maxChunks() * cls.maxRawValueSize - 1  # for the 'C' or 'U' prefix

    @classmethod
    def _maxEncodedSize(cls):
        return cls._maxChunks() * cls.maxValueSize


    @classmethod
    def binaryToAttributes(cls, binary):
        if binary is None: return {}
        assert len(binary) <= cls.maxBinarySize()
        # The use of compression is just an optimization. We can't include it in the maxValueSize
        # computation because the compression ratio depends on the input.
        compressed = bz2.compress(binary)
        if len(compressed) > len(binary):
            compressed = 'U' + binary
        else:
            compressed = 'C' + compressed
        encoded = base64.b64encode(compressed)
        assert len(encoded) <= cls._maxEncodedSize()
        n = cls.maxValueSize
        chunks = (encoded[i:i + n] for i in range(0, len(encoded), n))
        return {cls._chunkName(i): chunk for i, chunk in enumerate(chunks)}

    @classmethod
    def _chunkName(cls, i):
        return str(i).zfill(3)

    @classmethod
    def _isValidChunkName(cls, s):
        return len(s) == 3 and s.isdigit()

    @classmethod
    def attributesToBinary(cls, attributes):
        """
        :rtype: (str|None,int)
        :return: the binary data and the number of chunks it was composed from
        """
        chunks = [(int(k), v) for k, v in attributes.iteritems() if cls._isValidChunkName(k)]
        chunks.sort()
        numChunks = len(chunks)
        if numChunks:
            assert len(set(k for k, v in chunks)) == chunks[-1][0] + 1 == numChunks
            serializedJob = ''.join(v for k, v in chunks)
            compressed = base64.b64decode(serializedJob)
            if compressed[0] == 'C':
                binary = bz2.decompress(compressed[1:])
            elif compressed[0] == 'U':
                binary = compressed[1:]
            else:
                assert False
        else:
            binary = None
        return binary, numChunks

from boto.sdb.connection import SDBConnection

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


# FIXME: This was lifted from cgcloud-lib where we use it for EC2 retries. The only difference
# FIXME: ... between that code and this is the name of the exception.

a_short_time = 5

a_long_time = 60 * 60


def no_such_domain(e):
    return (isinstance(e, SDBResponseError)
            and e.error_code
            and e.error_code.endswith('NoSuchDomain'))


def sdb_unavailable(e):
    return e.__class__ == BotoServerError and e.status.startswith("503")


def true(_):
    return True


def false(_):
    return False


def retry_sdb(retry_after=a_short_time,
              retry_for=10 * a_short_time,
              retry_while=no_such_domain):
    """
    Retry an SDB operation while the failure matches a given predicate and until a given timeout
    expires, waiting a given amount of time in between attempts. This function is a generator
    that yields contextmanagers. See doctests below for example usage.

    :param retry_after: the delay in seconds between attempts

    :param retry_for: the timeout in seconds.

    :param retry_while: a callable with one argument, an instance of SDBResponseError, returning \
    True if another attempt should be made or False otherwise

    :return: a generator yielding contextmanagers

    Retry for a limited amount of time:
    >>> i = 0
    >>> for attempt in retry_sdb( retry_after=0, retry_for=.1, retry_while=true ):
    ...     with attempt:
    ...         i += 1
    ...         raise SDBResponseError( 'foo', 'bar' )
    Traceback (most recent call last):
    ...
    SDBResponseError: SDBResponseError: foo bar
    <BLANKLINE>
    >>> i > 1
    True

    Do exactly one attempt:
    >>> i = 0
    >>> for attempt in retry_sdb( retry_for=0 ):
    ...     with attempt:
    ...         i += 1
    ...         raise SDBResponseError( 'foo', 'bar' )
    Traceback (most recent call last):
    ...
    SDBResponseError: SDBResponseError: foo bar
    <BLANKLINE>
    >>> i
    1

    Don't retry on success
    >>> i = 0
    >>> for attempt in retry_sdb( retry_after=0, retry_for=.1, retry_while=true ):
    ...     with attempt:
    ...         i += 1
    >>> i
    1

    Don't retry on unless condition returns
    >>> i = 0
    >>> for attempt in retry_sdb( retry_after=0, retry_for=.1, retry_while=false ):
    ...     with attempt:
    ...         i += 1
    ...         raise SDBResponseError( 'foo', 'bar' )
    Traceback (most recent call last):
    ...
    SDBResponseError: SDBResponseError: foo bar
    <BLANKLINE>
    >>> i
    1
    """
    if retry_for > 0:
        go = [None]

        @contextmanager
        def repeated_attempt():
            try:
                yield
            except BotoServerError as e:
                if time.time() + retry_after < expiration:
                    if retry_while(e):
                        log.info('... got %s, trying again in %is ...', e.error_code, retry_after)
                        time.sleep(retry_after)
                    else:
                        log.info('Exception failed predicate, giving up.')
                        raise
                else:
                    log.info('Retry timeout expired, giving up.')
                    raise
            else:
                go.pop()

        expiration = time.time() + retry_for
        while go:
            yield repeated_attempt()
    else:
        @contextmanager
        def single_attempt():
            yield

        yield single_attempt()
