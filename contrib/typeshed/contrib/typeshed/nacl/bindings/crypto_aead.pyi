from nacl._sodium import ffi as ffi, lib as lib
from nacl.exceptions import ensure as ensure
from typing import Any

crypto_aead_chacha20poly1305_ietf_KEYBYTES: Any
crypto_aead_chacha20poly1305_ietf_NSECBYTES: Any
crypto_aead_chacha20poly1305_ietf_NPUBBYTES: Any
crypto_aead_chacha20poly1305_ietf_ABYTES: Any
crypto_aead_chacha20poly1305_ietf_MESSAGEBYTES_MAX: Any
crypto_aead_chacha20poly1305_KEYBYTES: Any
crypto_aead_chacha20poly1305_NSECBYTES: Any
crypto_aead_chacha20poly1305_NPUBBYTES: Any
crypto_aead_chacha20poly1305_ABYTES: Any
crypto_aead_chacha20poly1305_MESSAGEBYTES_MAX: Any
crypto_aead_xchacha20poly1305_ietf_KEYBYTES: Any
crypto_aead_xchacha20poly1305_ietf_NSECBYTES: Any
crypto_aead_xchacha20poly1305_ietf_NPUBBYTES: Any
crypto_aead_xchacha20poly1305_ietf_ABYTES: Any
crypto_aead_xchacha20poly1305_ietf_MESSAGEBYTES_MAX: Any

def crypto_aead_chacha20poly1305_ietf_encrypt(message: Any, aad: Any, nonce: Any, key: Any): ...
def crypto_aead_chacha20poly1305_ietf_decrypt(ciphertext: Any, aad: Any, nonce: Any, key: Any): ...
def crypto_aead_chacha20poly1305_encrypt(message: Any, aad: Any, nonce: Any, key: Any): ...
def crypto_aead_chacha20poly1305_decrypt(ciphertext: Any, aad: Any, nonce: Any, key: Any): ...
def crypto_aead_xchacha20poly1305_ietf_encrypt(message: Any, aad: Any, nonce: Any, key: Any): ...
def crypto_aead_xchacha20poly1305_ietf_decrypt(ciphertext: Any, aad: Any, nonce: Any, key: Any): ...
