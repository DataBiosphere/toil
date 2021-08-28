from typing import Union

def compat_bytes(s: Union[bytes, str]) -> str:
    return s.decode('utf-8') if isinstance(s, bytes) else s
