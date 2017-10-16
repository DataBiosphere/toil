# -*- coding: ascii -*-
#
#  Util/PEM.py : Privacy Enhanced Mail utilities
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
"""Set of functions for encapsulating data according to the PEM format.

PEM (Privacy Enhanced Mail) was an IETF standard for securing emails via a
Public Key Infrastructure. It is specified in RFC 1421-1424.

Even though it has been abandoned, the simple message encapsulation it defined
is still widely used today for encoding *binary* cryptographic objects like
keys and certificates into text.
"""

__all__ = ['encode', 'decode']

import sys
if sys.version_info[0] == 2 and sys.version_info[1] == 1:
    from toil.lib.crypto.Util.py21compat import *
from toil.lib.crypto.Util.py3compat import *

import re
from binascii import hexlify, unhexlify, a2b_base64, b2a_base64

from toil.lib.crypto.Hash import MD5


def decode(pem_data, passphrase=None):
    """Decode a PEM block into binary.

    :Parameters:
      pem_data : string
        The PEM block.
      passphrase : byte string
        If given and the PEM block is encrypted,
        the key will be derived from the passphrase.
    :Returns:
      A tuple with the binary data, the marker string, and a boolean to
      indicate if decryption was performed.
    :Raises ValueError:
      If decoding fails, if the PEM file is encrypted and no passphrase has
      been provided or if the passphrase is incorrect.
    """

    # Verify Pre-Encapsulation Boundary
    r = re.compile("\s*-----BEGIN (.*)-----\n")
    m = r.match(pem_data)
    if not m:
        raise ValueError("Not a valid PEM pre boundary")
    marker = m.group(1)

    # Verify Post-Encapsulation Boundary
    r = re.compile("-----END (.*)-----\s*$")
    m = r.search(pem_data)
    if not m or m.group(1) != marker:
        raise ValueError("Not a valid PEM post boundary")

    # Removes spaces and slit on lines
    lines = pem_data.replace(" ", '').split()

    # Decrypts, if necessary
    if lines[1].startswith('Proc-Type:4,ENCRYPTED'):
        assert False  # code deleted
    else:
        objdec = None

    # Decode body
    data = a2b_base64(b(''.join(lines[1:-1])))
    enc_flag = False
    if objdec:
        assert False  # code deleted

    return (data, marker, enc_flag)
