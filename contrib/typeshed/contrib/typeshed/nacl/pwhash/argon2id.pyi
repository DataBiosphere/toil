from typing import Any

ALG: Any
STRPREFIX: Any
SALTBYTES: Any
PASSWD_MIN: Any
PASSWD_MAX: Any
PWHASH_SIZE: Any
BYTES_MIN: Any
BYTES_MAX: Any
verify: Any
MEMLIMIT_MIN: Any
MEMLIMIT_MAX: Any
OPSLIMIT_MIN: Any
OPSLIMIT_MAX: Any
OPSLIMIT_INTERACTIVE: Any
MEMLIMIT_INTERACTIVE: Any
OPSLIMIT_SENSITIVE: Any
MEMLIMIT_SENSITIVE: Any
OPSLIMIT_MODERATE: Any
MEMLIMIT_MODERATE: Any

def kdf(size: Any, password: Any, salt: Any, opslimit: Any = ..., memlimit: Any = ..., encoder: Any = ...): ...
def str(password: Any, opslimit: Any = ..., memlimit: Any = ...): ...
