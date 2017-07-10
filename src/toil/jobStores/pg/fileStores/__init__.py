class NoSupportedFileStoreException(Exception):
    """Indicates that the specified job store does not exist."""
    def __init__(self, locator):
        super(NoSupportedFileStoreException, self).__init__(
            "The file store '%s' is not supported" % locator)

def getStore(url):
    scheme, _ = url.split('://', 1)
    if scheme == 'file':
        from local import FileStore
        return FileStore
    elif scheme == 's3':
        from s3 import FileStore
        return FileStore
    else:
        raise NoSupportedFileStoreException(url)


def fromUrl(url):
    _, path = url.split('://', 1)
    return getStore(url)(path)
