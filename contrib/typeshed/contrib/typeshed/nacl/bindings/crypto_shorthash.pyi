from nacl._sodium import ffi as ffi, lib as lib
from nacl.exceptions import ensure as ensure
from typing import Any

BYTES: Any
KEYBYTES: Any
XBYTES: Any
XKEYBYTES: Any

def crypto_shorthash_siphash24(data: Any, key: Any): ...
def crypto_shorthash_siphashx24(data: Any, key: Any): ...
