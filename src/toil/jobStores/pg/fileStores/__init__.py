class NoSupportedFileStoreException(Exception):
    """Indicates that the specified job store does not exist."""
    def __init__(self, locator):
        super(NoSupportedFileStoreException, self).__init__(
            "The file store '%s' is not supported" % locator)


def fromUrl(url):
    scheme, path = url.split('://', 1)
    if scheme == 'file':
        from local import FileStore
        return FileStore(path)
    elif scheme == 's3':
        from s3 import FileStore
        return FileStore(path)
    else:
        raise NoSupportedFileStoreException(url)
