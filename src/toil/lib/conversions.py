"""Conversion utilities for mapping memory, disk, core declarations from strings to numbers and vice versa."""
# TODO: Consolidate all conversion utilities here to use the same functions:
#  src/toil/lib/humanize.py (bytes2human; human2bytes)
#
from functools import total_ordering
from typing import Optional


VALID_UNITS = ['b', 'k', 'm', 'g', 't', 'kb', 'mb', 'gb', 'tb',
               'ki', 'mi', 'gi', 'ti', 'kib', 'mib', 'gib', 'tib']


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

    if unit.lower() in ['k', 'kb']:
        num_bytes = 1000
    if unit.lower() in ['m', 'mb']:
        num_bytes = 1000 ** 2
    if unit.lower() in ['g', 'gb']:
        num_bytes = 1000 ** 3
    if unit.lower() in ['t', 'tb']:
        num_bytes = 1000 ** 4
    return num_bytes


def convert_units(num: float,
                  src_unit: str,
                  dst_unit: Optional[str] = 'B') -> float:
    """Returns a float representing the converted input in dst_units."""
    assert src_unit.lower() in VALID_UNITS, f"{src_unit} not a valid unit, valid units are {VALID_UNITS}."
    assert dst_unit.lower() in VALID_UNITS, f"{dst_unit} not a valid unit, valid units are {VALID_UNITS}."
    return (num * bytes_in_unit(src_unit)) / bytes_in_unit(dst_unit)


def human2bytes(string: str) -> int:
    """Returns the amount of bytes, given a memory string."""
    for index, char in enumerate(string):
        # find the first character of the unit
        if char not in '0123456789. ':
            val = float(string[:index])
            unit = string[index:].strip()
            break
    else:
        val = float(string)
        unit = 'B'
    assert unit.lower() in VALID_UNITS, f"{unit} not a valid unit, valid units are {VALID_UNITS}."

    if unit != 'B':
        # assume kB, MB, GB, and TB represent the binary versions KiB, MiB, GiB, and TiB
        return int(convert_units(val, src_unit=unit[0] + 'i', dst_unit='B'))
    return int(val)


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
