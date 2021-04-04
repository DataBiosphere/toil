"""Conversion utilities for mapping memory, disk, core declarations from strings to numbers and vice versa."""
# TODO: Consolidate all conversion utilities here to use the same functions:
#  src/toil/lib/humanize.py (bytes2human; human2bytes)
#
from functools import total_ordering
from typing import Tuple, Optional


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
    units = ['b', 'k', 'm', 'g', 't', 'kb', 'mb', 'gb', 'tb',
             'ki', 'mi', 'gi', 'ti', 'kib', 'mib', 'gib', 'tib']
    assert src_unit.lower() in units, f"{src_unit} not a valid unit, valid units are {units}."
    assert dst_unit.lower() in units, f"{dst_unit} not a valid unit, valid units are {units}."
    return (num * bytes_in_unit(src_unit)) / bytes_in_unit(dst_unit)


def parse_unit(string: str) -> Tuple[float, str]:
    """Given a memory string, separates the floating-point number from its unit."""
    for index, char in enumerate(string):
        # find the first character of the unit
        if char not in '0123456789. ':
            val = float(string[:index])
            unit = string[index:].strip()
            break
    else:
        val = float(string)
        unit = 'B'
    return val, unit


@total_ordering
class MemoryString:
    """
    Represents an amount of bytes, as a string, using suffixes for the unit.

    Comparable based on the actual number of bytes instead of string value.
    """
    def __init__(self, string: str):
        val, unit = parse_unit(string)
        assert unit.lower() in ['b', 'k', 'm', 'g', 't', 'kb', 'mb', 'gb', 'tb']

        self.val = val
        self.unit = unit[0]
        self.bytes = self.byte_val()

    def __str__(self) -> str:
        if self.unit != 'B':
            return str(self.val) + self.unit
        else:
            return str(self.val)

    def byte_val(self) -> float:
        """ Returns the amount of bytes as a float."""
        if self.unit != 'B':
            # assume kB, MB, GB, and TB represent the binary versions KiB, MiB, GiB, and TiB
            return self.val * bytes_in_unit(self.unit + 'i')
        return self.val

    def __eq__(self, other):
        return self.bytes == other.bytes

    def __lt__(self, other):
        return self.bytes < other.bytes
