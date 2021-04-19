from nacl._sodium import ffi as ffi, lib as lib
from nacl.exceptions import ensure as ensure
from typing import Any

crypto_hash_BYTES: Any
crypto_hash_sha256_BYTES: Any
crypto_hash_sha512_BYTES: Any

def crypto_hash(message: Any): ...
def crypto_hash_sha256(message: Any): ...
def crypto_hash_sha512(message: Any): ...
