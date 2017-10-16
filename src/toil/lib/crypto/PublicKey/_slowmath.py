# -*- coding: utf-8 -*-
#
#  PubKey/RSA/_slowmath.py : Pure Python implementation of the RSA portions of _fastmath
#
# Written in 2008 by Dwayne C. Litzenberger <dlitz@dlitz.net>
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

"""Pure Python implementation of the RSA-related portions of Crypto.PublicKey._fastmath."""

__revision__ = "$Id$"

__all__ = ['rsa_construct']

import sys

if sys.version_info[0] == 2 and sys.version_info[1] == 1:
    from toil.lib.crypto.Util.py21compat import *
from toil.lib.crypto.Util.number import inverse


class error(Exception):
    pass


class _RSAKey(object):
    def has_private(self):
        return hasattr(self, 'd')


def rsa_construct(n, e, d=None, p=None, q=None, u=None):
    """Construct an RSAKey object"""
    assert isinstance(n, long)
    assert isinstance(e, long)
    assert isinstance(d, (long, type(None)))
    assert isinstance(p, (long, type(None)))
    assert isinstance(q, (long, type(None)))
    assert isinstance(u, (long, type(None)))
    obj = _RSAKey()
    obj.n = n
    obj.e = e
    if d is None:
        return obj
    obj.d = d
    if p is not None and q is not None:
        obj.p = p
        obj.q = q
    else:
        assert False  # code deleted
    if u is not None:
        obj.u = u
    else:
        obj.u = inverse(obj.p, obj.q)
    return obj

# vim:set ts=4 sw=4 sts=4 expandtab:
