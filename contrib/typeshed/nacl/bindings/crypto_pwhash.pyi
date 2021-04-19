from nacl._sodium import ffi as ffi, lib as lib
from nacl.exceptions import ensure as ensure
from typing import Any

crypto_pwhash_scryptsalsa208sha256_STRPREFIX: Any
crypto_pwhash_scryptsalsa208sha256_SALTBYTES: Any
crypto_pwhash_scryptsalsa208sha256_STRBYTES: Any
crypto_pwhash_scryptsalsa208sha256_PASSWD_MIN: Any
crypto_pwhash_scryptsalsa208sha256_PASSWD_MAX: Any
crypto_pwhash_scryptsalsa208sha256_BYTES_MIN: Any
crypto_pwhash_scryptsalsa208sha256_BYTES_MAX: Any
crypto_pwhash_scryptsalsa208sha256_MEMLIMIT_MIN: Any
crypto_pwhash_scryptsalsa208sha256_MEMLIMIT_MAX: Any
crypto_pwhash_scryptsalsa208sha256_OPSLIMIT_MIN: Any
crypto_pwhash_scryptsalsa208sha256_OPSLIMIT_MAX: Any
crypto_pwhash_scryptsalsa208sha256_OPSLIMIT_INTERACTIVE: Any
crypto_pwhash_scryptsalsa208sha256_MEMLIMIT_INTERACTIVE: Any
crypto_pwhash_scryptsalsa208sha256_OPSLIMIT_SENSITIVE: Any
crypto_pwhash_scryptsalsa208sha256_MEMLIMIT_SENSITIVE: Any
crypto_pwhash_ALG_ARGON2I13: Any
crypto_pwhash_ALG_ARGON2ID13: Any
crypto_pwhash_ALG_DEFAULT: Any
crypto_pwhash_SALTBYTES: Any
crypto_pwhash_STRBYTES: Any
crypto_pwhash_PASSWD_MIN: Any
crypto_pwhash_PASSWD_MAX: Any
crypto_pwhash_BYTES_MIN: Any
crypto_pwhash_BYTES_MAX: Any
crypto_pwhash_argon2i_STRPREFIX: Any
crypto_pwhash_argon2i_MEMLIMIT_MIN: Any
crypto_pwhash_argon2i_MEMLIMIT_MAX: Any
crypto_pwhash_argon2i_OPSLIMIT_MIN: Any
crypto_pwhash_argon2i_OPSLIMIT_MAX: Any
crypto_pwhash_argon2i_OPSLIMIT_INTERACTIVE: Any
crypto_pwhash_argon2i_MEMLIMIT_INTERACTIVE: Any
crypto_pwhash_argon2i_OPSLIMIT_MODERATE: Any
crypto_pwhash_argon2i_MEMLIMIT_MODERATE: Any
crypto_pwhash_argon2i_OPSLIMIT_SENSITIVE: Any
crypto_pwhash_argon2i_MEMLIMIT_SENSITIVE: Any
crypto_pwhash_argon2id_STRPREFIX: Any
crypto_pwhash_argon2id_MEMLIMIT_MIN: Any
crypto_pwhash_argon2id_MEMLIMIT_MAX: Any
crypto_pwhash_argon2id_OPSLIMIT_MIN: Any
crypto_pwhash_argon2id_OPSLIMIT_MAX: Any
crypto_pwhash_argon2id_OPSLIMIT_INTERACTIVE: Any
crypto_pwhash_argon2id_MEMLIMIT_INTERACTIVE: Any
crypto_pwhash_argon2id_OPSLIMIT_MODERATE: Any
crypto_pwhash_argon2id_MEMLIMIT_MODERATE: Any
crypto_pwhash_argon2id_OPSLIMIT_SENSITIVE: Any
crypto_pwhash_argon2id_MEMLIMIT_SENSITIVE: Any
SCRYPT_OPSLIMIT_INTERACTIVE = crypto_pwhash_scryptsalsa208sha256_OPSLIMIT_INTERACTIVE
SCRYPT_MEMLIMIT_INTERACTIVE = crypto_pwhash_scryptsalsa208sha256_MEMLIMIT_INTERACTIVE
SCRYPT_OPSLIMIT_SENSITIVE = crypto_pwhash_scryptsalsa208sha256_OPSLIMIT_SENSITIVE
SCRYPT_MEMLIMIT_SENSITIVE = crypto_pwhash_scryptsalsa208sha256_MEMLIMIT_SENSITIVE
SCRYPT_SALTBYTES = crypto_pwhash_scryptsalsa208sha256_SALTBYTES
SCRYPT_STRBYTES = crypto_pwhash_scryptsalsa208sha256_STRBYTES
SCRYPT_PR_MAX: Any
LOG2_UINT64_MAX: int
UINT64_MAX: Any
SCRYPT_MAX_MEM: Any

def nacl_bindings_pick_scrypt_params(opslimit: Any, memlimit: Any): ...
def crypto_pwhash_scryptsalsa208sha256_ll(passwd: Any, salt: Any, n: Any, r: Any, p: Any, dklen: int = ..., maxmem: Any = ...): ...
def crypto_pwhash_scryptsalsa208sha256_str(passwd: Any, opslimit: Any = ..., memlimit: Any = ...): ...
def crypto_pwhash_scryptsalsa208sha256_str_verify(passwd_hash: Any, passwd: Any): ...
def crypto_pwhash_alg(outlen: Any, passwd: Any, salt: Any, opslimit: Any, memlimit: Any, alg: Any): ...
def crypto_pwhash_str_alg(passwd: Any, opslimit: Any, memlimit: Any, alg: Any): ...
def crypto_pwhash_str_verify(passwd_hash: Any, passwd: Any): ...
crypto_pwhash_argon2i_str_verify = crypto_pwhash_str_verify
