# https://pytest.org/latest/example/pythoncollection.html

collect_ignore = []

try:
    import nacl
    print(nacl.__file__)  # to keep this import from being removed
except ImportError:
    collect_ignore.append("_nacl.py")
