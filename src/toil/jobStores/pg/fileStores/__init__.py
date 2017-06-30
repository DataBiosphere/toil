def fromUrl(url):
    scheme, path = url.split(':', 1)
    from local import FileStore
    return FileStore(path)
