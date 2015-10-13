from __future__ import absolute_import
import base64
import bz2
from contextlib import contextmanager
import logging
from boto.exception import SDBResponseError, BotoServerError
import time

log = logging.getLogger(__name__)


class SDBHelper(object):
    """
    A mixin with methods for storing limited amounts of binary data in an SDB item

    >>> H=SDBHelper
    >>> H.binaryToAttributes(None)
    {}
    >>> H.attributesToBinary({})
    (None, 0)
    >>> H.binaryToAttributes('')
    {'000': 'VQ=='}
    >>> H.attributesToBinary({'000': 'VQ=='})
    ('', 1)
    >>> a = H.binaryToAttributes('l'*100)
    >>> a
    {'000': 'Q0JaaDkxQVkmU1kS6uw5AAACAQBABCAAIQCCCxdyRThQkBLq7Dk='}
    >>> H.attributesToBinary(a) == ('l'*100,1)
    True
    >>> import os
    >>> sorted( H.binaryToAttributes(os.urandom(2048)).keys() )
    ['000', '001', '002']

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
        return 0

    @classmethod
    def maxBinarySize(cls, encoded=False):
        numAttributes = cls.maxAttributesPerItem - cls._reservedAttributes()
        maxValueSize = cls.maxValueSize if encoded else cls.maxRawValueSize
        return numAttributes * maxValueSize

    @classmethod
    def binaryToAttributes(cls, binary):
        if binary is None: return {}
        assert len(binary) <= cls.maxBinarySize(encoded=False)
        # The use of compression is just an optimization. We can't include it in the maxValueSize
        # computation because the compression ratio depends on the input.
        compressed = bz2.compress(binary)
        if len(compressed) > len(binary):
            compressed = 'U' + binary
        else:
            compressed = 'C' + compressed
        encoded = base64.b64encode(compressed)
        assert len(encoded) < cls.maxBinarySize(encoded=True)
        n = cls.maxValueSize
        chunks = (encoded[i:i + n] for i in range(0, len(encoded), n))
        return {str(index).zfill(3): chunk for index, chunk in enumerate(chunks)}

    @classmethod
    def attributesToBinary(cls, attributes):
        """
        :rtype: (str|None,int)
        :return: the binary data and the number of chunks it was composed from
        """
        chunks = [(int(k), v) for k, v in attributes.iteritems() if len(k) == 3 and k.isdigit()]
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


# FIXME: This was lifted from cgcloud-lib where we use it for EC2 retries. The only difference
# FIXME: ... between that code and this is the name of the exception.

a_short_time = 5

a_long_time = 60 * 60


def no_such_domain(e):
    return isinstance(e, SDBResponseError) and e.error_code.endswith('NoSuchDomain')


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

    :param retry_while: a callable with one argument, an instance of SDBResponseError, returning
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
