from nacl._sodium import ffi as ffi, lib as lib
from nacl.exceptions import ensure as ensure
from typing import Any

crypto_secretbox_KEYBYTES: Any
crypto_secretbox_NONCEBYTES: Any
crypto_secretbox_ZEROBYTES: Any
crypto_secretbox_BOXZEROBYTES: Any
crypto_secretbox_MACBYTES: Any
crypto_secretbox_MESSAGEBYTES_MAX: Any

def crypto_secretbox(message: Any, nonce: Any, key: Any): ...
def crypto_secretbox_open(ciphertext: Any, nonce: Any, key: Any): ...
