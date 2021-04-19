from typing import Any

BLAKE2B_BYTES: Any
BLAKE2B_BYTES_MIN: Any
BLAKE2B_BYTES_MAX: Any
BLAKE2B_KEYBYTES: Any
BLAKE2B_KEYBYTES_MIN: Any
BLAKE2B_KEYBYTES_MAX: Any
BLAKE2B_SALTBYTES: Any
BLAKE2B_PERSONALBYTES: Any
SIPHASH_BYTES: Any
SIPHASH_KEYBYTES: Any
SIPHASHX_BYTES: Any
SIPHASHX_KEYBYTES: Any

def sha256(message: Any, encoder: Any = ...): ...
def sha512(message: Any, encoder: Any = ...): ...
def blake2b(data: Any, digest_size: Any = ..., key: bytes = ..., salt: bytes = ..., person: bytes = ..., encoder: Any = ...): ...
generichash = blake2b

def siphash24(message: Any, key: bytes = ..., encoder: Any = ...): ...
shorthash = siphash24

def siphashx24(message: Any, key: bytes = ..., encoder: Any = ...): ...
