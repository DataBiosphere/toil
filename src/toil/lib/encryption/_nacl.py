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

import nacl
from nacl.secret import SecretBox

# 16-byte MAC plus a nonce is added to every message.
overhead = 16 + SecretBox.NONCE_SIZE


def encrypt(message: bytes, keyPath: str) -> bytes:
    """
    Encrypts a message given a path to a local file containing a key.

    :param message: The message to be encrypted.
    :param keyPath: A path to a file containing a 256-bit key (and nothing else).
    :type message: bytes
    :type keyPath: str
    :rtype: bytes

    A constant overhead is added to every encrypted message (for the nonce and MAC).
    >>> import tempfile
    >>> k = tempfile.mktemp()
    >>> with open(k, 'wb') as f:
    ...     _ = f.write(nacl.utils.random(SecretBox.KEY_SIZE))
    >>> message = 'test'.encode('utf-8')
    >>> len(encrypt(message, k)) == overhead + len(message)
    True
    >>> import os
    >>> os.remove(k)
    """
    with open(keyPath, 'rb') as f:
        key = f.read()
    if len(key) != SecretBox.KEY_SIZE:
        raise ValueError("Key is %d bytes, but must be exactly %d bytes" % (len(key),
                                                                            SecretBox.KEY_SIZE))
    sb = SecretBox(key)
    # We generate the nonce using secure random bits. For long enough
    # nonce size, the chance of a random nonce collision becomes
    # *much* smaller than the chance of a subtle coding error causing
    # a nonce reuse. Currently the nonce size is 192 bits--the chance
    # of a collision is astronomically low. (This approach is
    # recommended in the libsodium documentation.)
    nonce = nacl.utils.random(SecretBox.NONCE_SIZE)
    assert len(nonce) == SecretBox.NONCE_SIZE
    return bytes(sb.encrypt(message, nonce))


def decrypt(ciphertext: bytes, keyPath: str) -> bytes:
    """
    Decrypts a given message that was encrypted with the encrypt() method.

    :param ciphertext: The encrypted message (as a string).
    :param keyPath: A path to a file containing a 256-bit key (and nothing else).
    :type ciphertext: bytes
    :type keyPath: str
    :rtype: bytes

    Raises an error if ciphertext was modified
    >>> import tempfile
    >>> k = tempfile.mktemp()
    >>> with open(k, 'wb') as f:
    ...     _ = f.write(nacl.utils.random(SecretBox.KEY_SIZE))
    >>> ciphertext = encrypt("testMessage".encode('utf-8'), k)
    >>> ciphertext = b'5' + ciphertext[1:]
    >>> decrypt(ciphertext, k) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    CryptoError: Decryption failed. Ciphertext failed verification

    Otherwise works correctly
    >>> decrypt(encrypt("testMessage".encode('utf-8'), k), k).decode('utf-8') in (u'testMessage', b'testMessage', 'testMessage') # doctest: +ALLOW_UNICODE
    True

    >>> import os
    >>> os.remove(k)
    """
    with open(keyPath, 'rb') as f:
        key = f.read()
    if len(key) != SecretBox.KEY_SIZE:
        raise ValueError("Key is %d bytes, but must be exactly %d bytes" % (len(key),
                                                                            SecretBox.KEY_SIZE))
    sb = SecretBox(key)
    # The nonce is kept with the message.
    return sb.decrypt(ciphertext)
