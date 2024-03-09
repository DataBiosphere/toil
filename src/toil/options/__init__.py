from typing import Optional

from toil.lib.conversions import strtobool


def convert_bool(b: str) -> bool:
    """Convert a string representation of bool to bool"""
    return bool(strtobool(b))


def opt_strtobool(b: Optional[str]) -> Optional[bool]:
    """Convert an optional string representation of bool to None or bool"""
    return b if b is None else convert_bool(b)