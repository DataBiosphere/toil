from . import argon2i as argon2i, argon2id as argon2id, scrypt as scrypt
from nacl.exceptions import InvalidkeyError as InvalidkeyError
from typing import Any

STRPREFIX: Any
PWHASH_SIZE: Any
PASSWD_MIN: Any
PASSWD_MAX: Any
MEMLIMIT_MAX: Any
MEMLIMIT_MIN: Any
OPSLIMIT_MAX: Any
OPSLIMIT_MIN: Any
OPSLIMIT_INTERACTIVE: Any
MEMLIMIT_INTERACTIVE: Any
OPSLIMIT_MODERATE: Any
MEMLIMIT_MODERATE: Any
OPSLIMIT_SENSITIVE: Any
MEMLIMIT_SENSITIVE: Any
str: Any
SCRYPT_SALTBYTES: Any
SCRYPT_PWHASH_SIZE: Any
SCRYPT_OPSLIMIT_INTERACTIVE: Any
SCRYPT_MEMLIMIT_INTERACTIVE: Any
SCRYPT_OPSLIMIT_SENSITIVE: Any
SCRYPT_MEMLIMIT_SENSITIVE: Any
kdf_scryptsalsa208sha256: Any
scryptsalsa208sha256_str: Any
verify_scryptsalsa208sha256: Any

def verify(password_hash: Any, password: Any): ...
