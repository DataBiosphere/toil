"""Conversion utilities for mapping memory, disk, core declarations from strings to numbers and vice versa."""
from functools import total_ordering
from typing import Optional, SupportsInt

VALID_UNITS = ['b', 'k', 'm', 'g', 't', 'p', 'e', 'kb', 'mb', 'gb', 'tb', 'pb', 'eb',
               'ki', 'mi', 'gi', 'ti', 'pi', 'ei', 'kib', 'mib', 'gib', 'tib', 'pib', 'eib']


def bytes_in_unit(unit: str = 'B') -> int:
    num_bytes = 1
    if unit.lower() in ['ki', 'kib']:
        num_bytes = 1 << 10
    if unit.lower() in ['mi', 'mib']:
        num_bytes = 1 << 20
    if unit.lower() in ['gi', 'gib']:
        num_bytes = 1 << 30
    if unit.lower() in ['ti', 'tib']:
        num_bytes = 1 << 40
    if unit.lower() in ['pi', 'pib']:
        num_bytes = 1 << 50
    if unit.lower() in ['ei', 'eib']:
        num_bytes = 1 << 60

    if unit.lower() in ['k', 'kb']:
        num_bytes = 1000
    if unit.lower() in ['m', 'mb']:
        num_bytes = 1000 ** 2
    if unit.lower() in ['g', 'gb']:
        num_bytes = 1000 ** 3
    if unit.lower() in ['t', 'tb']:
        num_bytes = 1000 ** 4
    if unit.lower() in ['p', 'pb']:
        num_bytes = 1000 ** 5
    if unit.lower() in ['e', 'eb']:
        num_bytes = 1000 ** 6
    return num_bytes


def convert_units(num: float,
                  src_unit: str,
                  dst_unit: Optional[str] = 'B') -> float:
    """Returns a float representing the converted input in dst_units."""
    assert src_unit.lower() in VALID_UNITS, f"{src_unit} not a valid unit, valid units are {VALID_UNITS}."
    assert dst_unit.lower() in VALID_UNITS, f"{dst_unit} not a valid unit, valid units are {VALID_UNITS}."
    return (num * bytes_in_unit(src_unit)) / bytes_in_unit(dst_unit)


def human2bytes(string: str) -> int:
    """Returns the amount of bytes, in binary format, given a memory string."""
    for index, char in enumerate(string):
        # find the first character of the unit
        if char not in '0123456789.-_ ':
            val = float(string[:index])
            unit = string[index:].strip()
            break
    else:
        val = float(string)
        unit = 'B'
    assert unit.lower() in VALID_UNITS, f"{unit} not a valid unit, valid units are {VALID_UNITS}."

    if unit != 'B':
        # return the binary version of unit
        return int(convert_units(val, src_unit=unit[0] + 'i', dst_unit='B'))
    return int(val)


def bytes2human(n: SupportsInt,
                fmt: Optional[str] = '%(value).1f %(symbol)s',
                iec: Optional[bool] = False) -> str:
    """
    Bytes-to-human converter.

    Based on: http://goo.gl/kTQMs
    Author: Giampaolo Rodola' <g.rodola [AT] gmail [DOT] com>
    License: MIT

    Adapted from: http://code.activestate.com/recipes/578019-bytes-to-human-human-to-bytes-converter/
    """
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")
    if iec:
        symbols = ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei')
    else:
        symbols = ('B', 'K', 'M', 'G', 'T', 'P', 'E')
    prefix = {unit: bytes_in_unit(unit) for unit in symbols[1:]}

    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) // prefix[symbol]
            return fmt % locals()
    return fmt % dict(symbol=symbols[0], value=n)


@total_ordering
class MemoryString:
    """
    Represents an amount of bytes, as a string, using suffixes for the unit.

    Comparable based on the actual number of bytes instead of string value.
    """
    def __init__(self, string: str):
        self.string = string
        self.bytes = human2bytes(string)

    def __str__(self) -> str:
        return self.string

    def __eq__(self, other):
        return self.bytes == other.bytes

    def __lt__(self, other):
        return self.bytes < other.bytes
