# -*- coding: utf-8 -*-
#
#  PublicKey/PKCS8.py : PKCS#8 functions
#
# ===================================================================
# The contents of this file are dedicated to the public domain.  To
# the extent that dedication to the public domain is not available,
# everyone is granted a worldwide, perpetual, royalty-free,
# non-exclusive license to exercise all rights associated with the
# contents of this file for any purpose whatsoever.
# No rights are reserved.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# ===================================================================
"""
Module for handling private keys wrapped according to `PKCS#8`_.

PKCS8 is a standard for storing and transferring private key information.
The wrapped key can either be clear or encrypted.

All encryption algorithms are based on passphrase-based key derivation.
The following mechanisms are fully supported:

* *PBKDF2WithHMAC-SHA1AndAES128-CBC*
* *PBKDF2WithHMAC-SHA1AndAES192-CBC*
* *PBKDF2WithHMAC-SHA1AndAES256-CBC*
* *PBKDF2WithHMAC-SHA1AndDES-EDE3-CBC*

The following mechanisms are only supported for importing keys.
They are much weaker than the ones listed above, and they are provided
for backward compatibility only:

* *pbeWithMD5AndRC2-CBC*
* *pbeWithMD5AndDES-CBC*
* *pbeWithSHA1AndRC2-CBC*
* *pbeWithSHA1AndDES-CBC*

.. _`PKCS#8`: http://www.ietf.org/rfc/rfc5208.txt

"""

import sys

if sys.version_info[0] == 2 and sys.version_info[1] == 1:
    from toil.lib.crypto.Util.py21compat import *
from toil.lib.crypto.Util.py3compat import *

from toil.lib.crypto.Util.asn1 import *

# from Crypto.IO._PBES import PBES1, PBES2

__all__ = ['wrap', 'unwrap']


def decode_der(obj_class, binstr):
    """Instantiate a DER object class, decode a DER binary string in it, and
    return the object."""
    der = obj_class()
    der.decode(binstr)
    return der


def wrap(private_key, key_oid, passphrase=None, protection=None,
         prot_params=None, key_params=None, randfunc=None):
    """Wrap a private key into a PKCS#8 blob (clear or encrypted).

    :Parameters:

      private_key : byte string
        The private key encoded in binary form. The actual encoding is
        algorithm specific. In most cases, it is DER.

      key_oid : string
        The object identifier (OID) of the private key to wrap.
        It is a dotted string, like "``1.2.840.113549.1.1.1``" (for RSA keys).

      passphrase : (binary) string
        The secret passphrase from which the wrapping key is derived.
        Set it only if encryption is required.

      protection : string
        The identifier of the algorithm to use for securely wrapping the key.
        The default value is '``PBKDF2WithHMAC-SHA1AndDES-EDE3-CBC``'.

      prot_params : dictionary
        Parameters for the protection algorithm.

        +------------------+-----------------------------------------------+
        | Key              | Description                                   |
        +==================+===============================================+
        | iteration_count  | The KDF algorithm is repeated several times to|
        |                  | slow down brute force attacks on passwords.   |
        |                  | The default value is 1 000.                   |
        +------------------+-----------------------------------------------+
        | salt_size        | Salt is used to thwart dictionary and rainbow |
        |                  | attacks on passwords. The default value is 8  |
        |                  | bytes.                                        |
        +------------------+-----------------------------------------------+

      key_params : DER object
        The algorithm parameters associated to the private key.
        It is required for algorithms like DSA, but not for others like RSA.

      randfunc : callable
        Random number generation function; it should accept a single integer
        N and return a string of random data, N bytes long.
        If not specified, a new RNG will be instantiated
        from ``Crypto.Random``.

    :Return:
      The PKCS#8-wrapped private key (possibly encrypted),
      as a binary string.
    """

    if key_params is None:
        key_params = DerNull()

    #
    #   PrivateKeyInfo ::= SEQUENCE {
    #       version                 Version,
    #       privateKeyAlgorithm     PrivateKeyAlgorithmIdentifier,
    #       privateKey              PrivateKey,
    #       attributes              [0]  IMPLICIT Attributes OPTIONAL
    #   }
    #
    pk_info = newDerSequence(
        0,
        newDerSequence(
            DerObjectId(key_oid),
            key_params
        ),
        newDerOctetString(private_key)
    )
    pk_info_der = pk_info.encode()

    if not passphrase:
        return pk_info_der

    assert False  # code deleted
