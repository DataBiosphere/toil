"""Conversion utilities for mapping memory, disk, core declarations from strings to numbers and vice versa."""
# TODO: Consolidate all conversion utilities here to use the same functions:
#  src/toil/lib/humanize.py (bytes2human; human2bytes)
#  src/toil/batchSystems/__init__.py (MemoryString)
#


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
