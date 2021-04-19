from nacl._sodium import ffi as ffi, lib as lib
from nacl.exceptions import ensure as ensure

def sodium_init() -> None: ...
