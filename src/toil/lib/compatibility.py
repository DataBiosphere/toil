def compat_oldstr(s):
    return s.decode('utf-8') if isinstance(s, bytes) else s


def compat_bytes(s):
    return s.decode('utf-8') if isinstance(s, bytes) else s


def compat_plain(s):
    return s.decode('utf-8') if isinstance(s, bytes) else s
