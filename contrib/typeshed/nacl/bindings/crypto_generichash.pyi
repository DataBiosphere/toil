from nacl._sodium import ffi as ffi, lib as lib
from nacl.exceptions import ensure as ensure
from typing import Any

crypto_generichash_BYTES: Any
crypto_generichash_BYTES_MIN: Any
crypto_generichash_BYTES_MAX: Any
crypto_generichash_KEYBYTES: Any
crypto_generichash_KEYBYTES_MIN: Any
crypto_generichash_KEYBYTES_MAX: Any
crypto_generichash_SALTBYTES: Any
crypto_generichash_PERSONALBYTES: Any
crypto_generichash_STATEBYTES: Any

def generichash_blake2b_salt_personal(data: Any, digest_size: Any = ..., key: bytes = ..., salt: bytes = ..., person: bytes = ...): ...
def generichash_blake2b_init(key: bytes = ..., salt: bytes = ..., person: bytes = ..., digest_size: Any = ...): ...
def generichash_blake2b_update(statebuf: Any, data: Any) -> None: ...
def generichash_blake2b_final(statebuf: Any, digest_size: Any): ...
def generichash_blake2b_state_copy(statebuf: Any): ...
