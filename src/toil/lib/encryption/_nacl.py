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
from typing import Optional

# 16-byte MAC plus a nonce is added to every message.
overhead = 16 + SecretBox.NONCE_SIZE


def encrypt(message: bytes, key_path: Optional[str] = None, key: Optional[str] = None) -> bytes:
    """
    Encrypts a message given a path to a local file containing a key.

    :param message: The message to be encrypted.
    :param key_path: A path to a file containing a 256-bit key (and nothing else).
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
    box = get_secret_box(key_path, key)
    nonce = nacl.utils.random(SecretBox.NONCE_SIZE)
    return bytes(box.encrypt(message, nonce))


def decrypt(ciphertext: bytes, key_path: Optional[str] = None, key: Optional[str] = None) -> bytes:
    """
    Decrypts a given message that was encrypted with the encrypt() method.

    :param ciphertext: The encrypted message (as a string).
    :param key_path: A path to a file containing a 256-bit key (and nothing else).
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
    box = get_secret_box(key_path, key)
    return box.decrypt(ciphertext)


def get_secret_box(key_path: Optional[str] = None, key: Optional[str] = None) -> SecretBox:
    if (not key_path and not key) or (key_path and key):
        raise ValueError('Please only specify either key_path OR key.')
    if isinstance(key, str):
        key = key.encode('utf-8', errors='ignore')
    if key_path:
        with open(key_path, 'rb') as f:
            key = f.read()
    if len(key) != SecretBox.KEY_SIZE:
        raise ValueError("Key is %d bytes, but must be exactly %d bytes" %
                         (len(key), SecretBox.KEY_SIZE))
    return SecretBox(key)
