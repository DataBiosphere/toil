from nacl._sodium import ffi as ffi, lib as lib
from nacl.exceptions import ensure as ensure
from typing import Any

crypto_scalarmult_BYTES: Any
crypto_scalarmult_SCALARBYTES: Any

def crypto_scalarmult_base(n: Any): ...
def crypto_scalarmult(n: Any, p: Any): ...
