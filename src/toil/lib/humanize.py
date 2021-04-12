# http://code.activestate.com/recipes/578019-bytes-to-human-human-to-bytes-converter/

from typing import Optional, SupportsInt
from toil.lib.conversions import bytes2human as b2h, human2bytes as h2b

"""
Bytes-to-human / human-to-bytes converter.
Based on: http://goo.gl/kTQMs
Working with Python 2.x and 3.x.

Author: Giampaolo Rodola' <g.rodola [AT] gmail [DOT] com>
License: MIT
"""


def bytes2human(n: SupportsInt, fmt: Optional[str] = None, symbols: Optional[str] = None) -> str:
    """
    Deprecated by toil.lib.conversions.bytes2human.

    Convert n bytes into a human readable string based on format.
    symbols can be either "customary", "customary_ext", "iec" or "iec_ext",
    see: http://goo.gl/kTQMs
    """
    if not fmt:
        fmt = '%(value).1f %(symbol)s'
    return b2h(n, fmt, iec=symbols == 'iec')


def human2bytes(s):
    """
    Deprecated by toil.lib.conversions.human2bytes.

    Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.

    When unable to recognize the format ValueError is raised.
    """
    return h2b(s)
