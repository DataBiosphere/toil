"""Conversion utilities for mapping memory, disk, core declarations from strings to numbers and vice versa."""
# TODO: Consolidate all conversion utilities here to use the same functions:
#  src/toil/lib/humanize.py (bytes2human; human2bytes)
#
from functools import total_ordering


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
                  dst_unit: str) -> float:
    """Returns a float representing the converted input in dst_units."""
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    assert src_unit in units, f"{src_unit} not a valid unit, valid units are {units}."
    assert dst_unit in units, f"{dst_unit} not a valid unit, valid units are {units}."
    return (num * bytes_in_unit(src_unit)) / bytes_in_unit(dst_unit)


@total_ordering
class MemoryString:
    """
    Represents an amount of bytes, as a string, using suffixes for the unit.

    Comparable based on the actual number of bytes instead of string value.
    """
    def __init__(self, string: str):
        if string[-1] == 'K' or string[-1] == 'M' or string[-1] == 'G' or string[-1] == 'T':  # 10K
            self.unit = string[-1]
            self.val = float(string[:-1])
        elif len(string) >= 3 and (string[-2] == 'k' or string[-2] == 'M' or string[-2] == 'G' or string[-2] == 'T'):
            self.unit = string[-2]
            self.val = float(string[:-2])
        else:
            self.unit = 'B'
            self.val = float(string)
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
            return self.val * bytes_in_unit(self.unit + 'ib')
        return self.val

    def __eq__(self, other):
        return self.bytes == other.bytes

    def __lt__(self, other):
        return self.bytes < other.bytes
