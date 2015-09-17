# https://pytest.org/latest/example/pythoncollection.html

collect_ignore = []

try:
    import nacl
except ImportError:
    collect_ignore.append("_nacl.py")
