def compat_bytes(s):
    return s.decode('utf-8') if isinstance(s, bytes) else s
