# Used by cactus; now a wrapper and not used in Toil.
# TODO: Remove from cactus and then remove from Toil.
#   See https://github.com/DataBiosphere/toil/pull/3529#discussion_r611735988

# http://code.activestate.com/recipes/578019-bytes-to-human-human-to-bytes-converter/
import logging
from typing import SupportsInt

from toil.lib.conversions import bytes2human as b2h
from toil.lib.conversions import human2bytes as h2b

"""
Bytes-to-human / human-to-bytes converter.
Based on: http://goo.gl/kTQMs
Working with Python 2.x and 3.x.

Author: Giampaolo Rodola' <g.rodola [AT] gmail [DOT] com>
License: MIT
"""

logger = logging.getLogger(__name__)


def bytes2human(n: SupportsInt) -> str:
    """
    Convert n bytes into a human readable string.
    """
    logger.warning('Deprecated toil method.  Please use "toil.lib.conversions.bytes2human()" instead."')
    return b2h(n)


def human2bytes(s: str) -> int:
    """
    Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.

    When unable to recognize the format ValueError is raised.
    """
    logger.warning('Deprecated toil method.  Please use "toil.lib.conversions.human2bytes()" instead."')
    return h2b(s)
