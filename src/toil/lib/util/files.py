import errno
import os


def mkdir_p( path ):
    """
    The equivalent of mkdir -p
    """
    try:
        os.makedirs( path )
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir( path ):
            pass
        else:
            raise


def rm_f( path ):
    """
    Remove the file at the given path with os.remove(), ignoring errors caused by the file's absence.
    """
    try:
        os.remove( path )
    except OSError as e:
        if e.errno == errno.ENOENT:
            pass
        else:
            raise


def copyfileobj( src, dst, limit=None, bufsize=1024 * 1024 ):
    """
    Copy the contents of one file object to another file object. If limit is given, stop after at
    most limit bytes were copied. The copying will begin at the current file pointer of each file
    object.

    :param src: the file object to copy from

    :param dst: the file object to copy to

    :param limit: the maximum number of bytes to copy or None if all remaining bytes in src
           should be copied

    :param bufsize: the size of the intermediate copy buffer. No more than that many bytes will
           ever be read from src or written to dst at any one time.

    :return: None if limit is None, otherwise the difference between limit and the number of
    bytes actually copied. This will be > 0 if and only if the source file hit EOF before limit
    number of bytes could be read.

    >>> import tempfile
    >>> with open('/dev/urandom') as f1:
    ...     with tempfile.TemporaryFile() as f2:
    ...         copyfileobj(f1,f2,limit=100)
    ...         f2.seek(60)
    ...         with tempfile.TemporaryFile() as f3:
    ...             copyfileobj(f2,f3), f2.tell(), f3.tell()
    (None, 100, 40)
    """
    while limit is None or limit > 0:
        buf = src.read( bufsize if limit is None or bufsize < limit else limit )
        if buf:
            if limit is not None:
                limit -= len( buf )
                assert limit >= 0
            dst.write( buf )
        else:
            return limit


if False:
    # These are not needed for Python 2.7 as Python's builtin file object's read() and write()
    # method are greedy. For Python 3.x these may be useful.

    def gread( readable, n ):
        """
        Greedy read. Read until readable is exhausted, and error occurs or the given number of bytes
        have been read. If it returns fewer than the requested number bytes if and only if the end of
        file has been reached.

        :type readable: io.FileIO
        """
        bufs = [ ]
        i = 0
        while i < n:
            buf = readable.read( n - i )
            m = len( buf )
            if m == 0:
                break
            bufs.append( buf )
            i += m
        return ''.join( bufs )


    def gwrite( writable, buf ):
        """
        Greedy write. Write until the entire buffer has been written to or an error occurs.

        :type writable: io.FileIO[str|bytearray]

        :type buf: str|bytearray
        """
        n = len( buf )
        i = 0
        while i < n:
            i += writable.write( buf[ i: ] )
