#
#   number.py : Number-theoretic functions
#
#  Part of the Python Cryptography Toolkit
#
#  Written by Andrew M. Kuchling, Barry A. Warsaw, and others
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
#

__revision__ = "$Id$"

from warnings import warn as _warn
import math
import sys

from toil.lib.crypto.pct_warnings import GetRandomNumber_DeprecationWarning, PowmInsecureWarning
from toil.lib.crypto.Util.py3compat import *

bignum = long
try:
    from toil.lib.crypto.PublicKey import _fastmath
except ImportError:
    # For production, we are going to let import issues due to gmp/mpir shared
    # libraries not loading slide silently and use slowmath. If you'd rather
    # see an exception raised if _fastmath exists but cannot be imported,
    # uncomment the below
    #
    # from distutils.sysconfig import get_config_var
    # import inspect, os
    # _fm_path = os.path.normpath(os.path.dirname(os.path.abspath(
        # inspect.getfile(inspect.currentframe())))
        # +"/../../PublicKey/_fastmath"+get_config_var("SO"))
    # if os.path.exists(_fm_path):
        # raise ImportError("While the _fastmath module exists, importing "+
            # "it failed. This may point to the gmp or mpir shared library "+
            # "not being in the path. _fastmath was found at "+_fm_path)
    _fastmath = None

# You need libgmp v5 or later to get mpz_powm_sec.  Warn if it's not available.
if _fastmath is not None and not _fastmath.HAVE_DECL_MPZ_POWM_SEC:
    _warn(
        "Not using mpz_powm_sec.  You should rebuild using libgmp >= 5 to avoid timing attack vulnerability.",
        PowmInsecureWarning)

# New functions
# from _number_new import *

# Commented out and replaced with faster versions below
# def long2str(n):
# s=''
# while n>0:
##         s=chr(n & 255)+s
# n=n>>8
# return s

## import types
# def str2long(s):
# if type(s)!=types.StringType: return s   # Integers will be left alone
# return reduce(lambda x,y : x*256+ord(y), s, 0L)


def inverse(u, v):
    """inverse(u:long, v:long):long
    Return the inverse of u mod v.
    """
    u3, v3 = long(u), long(v)
    u1, v1 = 1, 0
    while v3 > 0:
        q = divmod(u3, v3)[0]
        u1, v1 = v1, u1 - v1 * q
        u3, v3 = v3, u3 - v3 * q
    while u1 < 0:
        u1 = u1 + v
    return u1

# Improved conversion functions contributed by Barry Warsaw, after
# careful benchmarking


import struct


def long_to_bytes(n, blocksize=0):
    """long_to_bytes(n:long, blocksize:int) : string
    Convert a long integer to a byte string.

    If optional blocksize is given and greater than zero, pad the front of the
    byte string with binary zeros so that the length is a multiple of
    blocksize.
    """
    # after much testing, this algorithm was deemed to be the fastest
    s = b('')
    n = long(n)
    pack = struct.pack
    while n > 0:
        s = pack('>I', n & 0xffffffff) + s
        n = n >> 32
    # strip off leading zeros
    for i in range(len(s)):
        if s[i] != b('\000')[0]:
            break
    else:
        # only happens when n == 0
        s = b('\000')
        i = 0
    s = s[i:]
    # add back some pad bytes.  this could be done more efficiently w.r.t. the
    # de-padding being done above, but sigh...
    if blocksize > 0 and len(s) % blocksize:
        s = (blocksize - len(s) % blocksize) * b('\000') + s
    return s


def bytes_to_long(s):
    """bytes_to_long(string) : long
    Convert a byte string to a long integer.

    This is (essentially) the inverse of long_to_bytes().
    """
    acc = 0
    unpack = struct.unpack
    length = len(s)
    if length % 4:
        extra = (4 - length % 4)
        s = b('\000') * extra + s
        length = length + extra
    for i in range(0, length, 4):
        acc = (acc << 32) + unpack('>I', s[i:i + 4])[0]
    return acc


# For backwards compatibility...
import warnings


def long2str(n, blocksize=0):
    warnings.warn("long2str() has been replaced by long_to_bytes()")
    return long_to_bytes(n, blocksize)


def str2long(s):
    warnings.warn("str2long() has been replaced by bytes_to_long()")
    return bytes_to_long(s)


def _import_Random():
    # This is called in a function instead of at the module level in order to
    # avoid problems with recursive imports
    global Random, StrongRandom
    from toil.lib.crypto import Random
    from toil.lib.crypto.Random.random import StrongRandom
