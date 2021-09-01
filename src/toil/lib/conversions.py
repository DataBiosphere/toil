"""
Conversion utilities for mapping memory, disk, core declarations from strings to numbers and vice versa.
Also contains general conversion functions
"""

import math
import urllib.parse

from typing import Optional, SupportsInt, Tuple, List

KIB = 1024
MIB = 1024 ** 2
GIB = 1024 ** 3
TIB = 1024 ** 4
PIB = 1024 ** 5
EIB = 1024 ** 6

KB = 1000
MB = 1000 ** 2
GB = 1000 ** 3
TB = 1000 ** 4
PB = 1000 ** 5
EB = 1000 ** 6

# See https://en.wikipedia.org/wiki/Binary_prefix
BINARY_PREFIXES = ['ki', 'mi', 'gi', 'ti', 'pi', 'ei', 'kib', 'mib', 'gib', 'tib', 'pib', 'eib']
DECIMAL_PREFIXES = ['b', 'k', 'm', 'g', 't', 'p', 'e', 'kb', 'mb', 'gb', 'tb', 'pb', 'eb']
VALID_PREFIXES = BINARY_PREFIXES + DECIMAL_PREFIXES


def bytes_in_unit(unit: str = 'B') -> int:
    num_bytes = 1
    if unit.lower() in ['ki', 'kib']:
        num_bytes = KIB
    if unit.lower() in ['mi', 'mib']:
        num_bytes = MIB
    if unit.lower() in ['gi', 'gib']:
        num_bytes = GIB
    if unit.lower() in ['ti', 'tib']:
        num_bytes = TIB
    if unit.lower() in ['pi', 'pib']:
        num_bytes = PIB
    if unit.lower() in ['ei', 'eib']:
        num_bytes = EIB

    if unit.lower() in ['k', 'kb']:
        num_bytes = KB
    if unit.lower() in ['m', 'mb']:
        num_bytes = MB
    if unit.lower() in ['g', 'gb']:
        num_bytes = GB
    if unit.lower() in ['t', 'tb']:
        num_bytes = TB
    if unit.lower() in ['p', 'pb']:
        num_bytes = PB
    if unit.lower() in ['e', 'eb']:
        num_bytes = EB
    return num_bytes


def convert_units(num: float,
                  src_unit: str,
                  dst_unit: str = 'B') -> float:
    """Returns a float representing the converted input in dst_units."""
    assert src_unit.lower() in VALID_PREFIXES, f"{src_unit} not a valid unit, valid units are {VALID_PREFIXES}."
    assert dst_unit.lower() in VALID_PREFIXES, f"{dst_unit} not a valid unit, valid units are {VALID_PREFIXES}."
    return (num * bytes_in_unit(src_unit)) / bytes_in_unit(dst_unit)


def parse_memory_string(string: str) -> Tuple[float, str]:
    """
    Given a string representation of some memory (i.e. '1024 Mib'), return the
    number and unit.
    """
    for i, character in enumerate(string):
        # find the first character of the unit
        if character not in '0123456789.-_ ':
            units = string[i:].strip()
            assert units.lower() in VALID_PREFIXES, f"{units} not a valid unit, valid units are {VALID_PREFIXES}."
            return float(string[:i]), units
    return float(string), 'b'


def human2bytes(string: str) -> int:
    """
    Given a string representation of some memory (i.e. '1024 Mib'), return the
    integer number of bytes.
    """
    value, unit = parse_memory_string(string)
    return int(convert_units(value, src_unit=unit, dst_unit='b'))


def bytes2human(n: SupportsInt) -> str:
    """Return a binary value as a human readable string with units."""
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")
    elif n < 1:
        return '0 b'

    power_level = math.floor(math.log(n, 1024))
    units = ('b', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei')

    unit = units[power_level if power_level < len(units) else -1]
    value = convert_units(n, "b", unit)
    return f'{value:.1f} {unit}'


def modify_url(url: str, remove: List[str]) -> str:
    """
    Given a valid URL string, split out the params, remove any offending
    params in 'remove', and return the cleaned URL.
    """
    scheme, netloc, path, query, fragment = urllib.parse.urlsplit(url)
    params = urllib.parse.parse_qs(query)
    for param_key in remove:
        if param_key in params:
            del params[param_key]
    query = urllib.parse.urlencode(params, doseq=True)
    return urllib.parse.urlunsplit((scheme, netloc, path, query, fragment))


def hms_duration_to_seconds(hms: str) -> float: 
    """
    Parses a given time string in hours:minutes:seconds, 
    returns an equivalent total seconds value
    """
    vals_to_convert = hms.split(':')
    seconds = 0.0
    
    for val in vals_to_convert:
        if(float(val) < 0): 
            raise ValueError("Invalid Time, negative value")

    if(len(vals_to_convert) != 3):
        raise ValueError("Invalid amount of fields, function takes input in 'hh:mm:ss'")

    seconds += float(vals_to_convert[0]) * 60 * 60 
    seconds += float(vals_to_convert[1]) * 60 
    seconds += float(vals_to_convert[2]) 

    return seconds
