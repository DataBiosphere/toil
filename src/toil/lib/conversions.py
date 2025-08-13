"""
Conversion utilities for mapping memory, disk, core declarations from strings to numbers and vice versa.
Also contains general conversion functions
"""
import math
import urllib.parse

from typing import Optional, SupportsInt, Union, List

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


def bytes_in_unit(unit: str = "B") -> int:
    num_bytes = 1
    if unit.lower() in ["ki", "kib"]:
        num_bytes = 1 << 10
    if unit.lower() in ["mi", "mib"]:
        num_bytes = 1 << 20
    if unit.lower() in ["gi", "gib"]:
        num_bytes = 1 << 30
    if unit.lower() in ["ti", "tib"]:
        num_bytes = 1 << 40
    if unit.lower() in ["pi", "pib"]:
        num_bytes = 1 << 50
    if unit.lower() in ["ei", "eib"]:
        num_bytes = 1 << 60

    if unit.lower() in ["k", "kb"]:
        num_bytes = 1000
    if unit.lower() in ["m", "mb"]:
        num_bytes = 1000**2
    if unit.lower() in ["g", "gb"]:
        num_bytes = 1000**3
    if unit.lower() in ["t", "tb"]:
        num_bytes = 1000**4
    if unit.lower() in ["p", "pb"]:
        num_bytes = 1000**5
    if unit.lower() in ["e", "eb"]:
        num_bytes = 1000**6
    return num_bytes


def convert_units(num: float, src_unit: str, dst_unit: str = "B") -> float:
    """Returns a float representing the converted input in dst_units."""
    if not src_unit.lower() in VALID_PREFIXES:
        raise RuntimeError(
            f"{src_unit} not a valid unit, valid units are {VALID_PREFIXES}."
        )
    if not dst_unit.lower() in VALID_PREFIXES:
        raise RuntimeError(
            f"{dst_unit} not a valid unit, valid units are {VALID_PREFIXES}."
        )
    return (num * bytes_in_unit(src_unit)) / bytes_in_unit(dst_unit)


def parse_memory_string(string: str) -> tuple[float, str]:
    """
    Given a string representation of some memory (i.e. '1024 Mib'), return the
    number and unit.
    """
    for i, character in enumerate(string):
        # find the first character of the unit
        if character not in "0123456789.-_ ":
            units = string[i:].strip()
            if not units.lower() in VALID_PREFIXES:
                raise RuntimeError(
                    f"{units} not a valid unit, valid units are {VALID_PREFIXES}."
                )
            return float(string[:i]), units
    return float(string), "b"


def human2bytes(string: str) -> int:
    """
    Given a string representation of some memory (i.e. '1024 Mib'), return the
    integer number of bytes.
    """
    value, unit = parse_memory_string(string)

    return int(convert_units(value, src_unit=unit, dst_unit="b"))


def bytes2human(n: SupportsInt) -> str:
    """Return a binary value as a human readable string with units."""
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")
    elif n < 1:
        return "0 b"

    power_level = math.floor(math.log(n, 1024))
    units = ("b", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei")

    unit = units[power_level if power_level < len(units) else -1]
    value = convert_units(n, "b", unit)
    return f"{value:.1f} {unit}"


def b_to_mib(n: Union[int, float]) -> float:
    """
    Convert a number from bytes to mibibytes.
    """
    return convert_units(n, "b", "mib")


def mib_to_b(n: Union[int, float]) -> float:
    """
    Convert a number from mibibytes to bytes.
    """
    return convert_units(n, "mib", "b")


# General Conversions


def hms_duration_to_seconds(hms: str) -> float:
    """
    Parses a given time string in hours:minutes:seconds,
    returns an equivalent total seconds value
    """
    vals_to_convert = hms.split(":")
    seconds = 0.0

    for val in vals_to_convert:
        if float(val) < 0:
            raise ValueError("Invalid Time, negative value")

    if len(vals_to_convert) != 3:
        raise ValueError("Invalid amount of fields, function takes input in 'hh:mm:ss'")

    seconds += float(vals_to_convert[0]) * 60 * 60
    seconds += float(vals_to_convert[1]) * 60
    seconds += float(vals_to_convert[2])

    return seconds


def strtobool(val: str) -> bool:
    """
    Make a human-readable string into a bool.

    Convert a string along the lines of "y", "1", "ON", "TrUe", or
    "Yes" to True, and the corresponding false-ish values to False.
    """
    # We only track prefixes, so "y" covers "y", "yes",
    # and "yeah no" and makes them all True.
    TABLE = {True: ["1", "on", "y", "t"], False: ["0", "off", "n", "f"]}
    lowered = val.lower()
    for result, prefixes in TABLE.items():
        for prefix in prefixes:
            if lowered.startswith(prefix):
                return result
    raise ValueError(f'Cannot convert "{val}" to a bool')


def opt_strtobool(b: Optional[str]) -> Optional[bool]:
    """Convert an optional string representation of bool to None or bool"""
    return b if b is None else strtobool(b)


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
