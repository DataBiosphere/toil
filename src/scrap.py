from urllib.parse import urlparse

f = "FilE://test"
a = urlparse(f).scheme

print(a)
