import random
from six.moves import xrange
from math import sqrt
import errno
import os
import shutil
import sys
import time
import socket
from contextlib import contextmanager

if sys.version_info[0] < 3:
    # Define a usable FileNotFoundError as will be raised by os.remove on a
    # nonexistent file.
    FileNotFoundError = OSError


def mkdir_p(path):
    """The equivalent of mkdir -p"""
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def robust_rmtree(path):
    """
    Robustly tries to delete paths.

    Continues silently if the path to be removed is already gone, or if it
    goes away while this function is executing.

    May raise an error if a path changes between file and directory while the
    function is executing, or if a permission error is encountered.

    path may be str, bytes, or unicode.
    """

    if not isinstance(path, bytes):
        # Internally we must work in bytes, in case we find an undecodeable
        # filename.
        path = path.encode('utf-8')

    if not os.path.exists(path):
        # Nothing to do!
        return

    if not os.path.islink(path) and os.path.isdir(path):
        # It is or has been a directory

        try:
            children = os.listdir(path)
        except FileNotFoundError:
            # Directory went away
            return

        # We assume the directory going away while we have it open won't upset
        # the listdir iterator.
        for child in children:
            # Get the path for each child item in the directory
            child_path = os.path.join(path, child)

            # Remove it if still present
            robust_rmtree(child_path)

        try:
            # Actually remove the directory once the children are gone
            os.rmdir(path)
        except FileNotFoundError:
            # Directory went away
            return

    else:
        # It is not or was not a directory.
        try:
            # Unlink it as a normal file
            os.unlink(path)
        except FileNotFoundError:
            # File went away
            return

def mean(xs):
    """
    Return the mean value of a sequence of values.

    >>> mean([2,4,4,4,5,5,7,9])
    5.0
    >>> mean([9,10,11,7,13])
    10.0
    >>> mean([1,1,10,19,19])
    10.0
    >>> mean([10,10,10,10,10])
    10.0
    >>> mean([1,"b"])
    Traceback (most recent call last):
      ...
    ValueError: Input can't have non-numeric elements
    >>> mean([])
    Traceback (most recent call last):
      ...
    ValueError: Input can't be empty
    """
    try:
        return sum(xs) / float(len(xs))
    except TypeError:
        raise ValueError("Input can't have non-numeric elements")
    except ZeroDivisionError:
        raise ValueError("Input can't be empty")


def std_dev(xs):
    """
    Returns the standard deviation of the given iterable of numbers.

    From http://rosettacode.org/wiki/Standard_deviation#Python

    An empty list, or a list with non-numeric elements will raise a TypeError.

    >>> std_dev([2,4,4,4,5,5,7,9])
    2.0

    >>> std_dev([9,10,11,7,13])
    2.0

    >>> std_dev([1,1,10,19,19])
    8.049844718999243

    >>> std_dev({1,1,10,19,19}) == std_dev({19,10,1})
    True

    >>> std_dev([10,10,10,10,10])
    0.0

    >>> std_dev([1,"b"])
    Traceback (most recent call last):
    ...
    ValueError: Input can't have non-numeric elements

    >>> std_dev([])
    Traceback (most recent call last):
    ...
    ValueError: Input can't be empty
    """
    m = mean(xs)  # this checks our pre-conditions, too
    return sqrt(sum((x - m) ** 2 for x in xs) / float(len(xs)))


def partition_seq(seq, size):
    """
    Splits a sequence into an iterable of subsequences. All subsequences are of the given size,
    except the last one, which may be smaller. If the input list is modified while the returned
    list is processed, the behavior of the program is undefined.

    :param seq: the list to split
    :param size: the desired size of the sublists, must be > 0
    :type size: int
    :return: an iterable of sublists

    >>> list(partition_seq("",1))
    []
    >>> list(partition_seq("abcde",2))
    ['ab', 'cd', 'e']
    >>> list(partition_seq("abcd",2))
    ['ab', 'cd']
    >>> list(partition_seq("abcde",1))
    ['a', 'b', 'c', 'd', 'e']
    >>> list(partition_seq("abcde",0))
    Traceback (most recent call last):
    ...
    ValueError: Size must be greater than 0
    >>> l=[1,2,3,4]
    >>> i = iter( partition_seq(l,2) )
    >>> l.pop(0)
    1
    >>> next(i)
    [2, 3]
    """
    if size < 1:
        raise ValueError('Size must be greater than 0')
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))


def truncExpBackoff():
    # as recommended here https://forums.aws.amazon.com/thread.jspa?messageID=406788#406788
    # and here https://cloud.google.com/storage/docs/xml-api/reference-status
    yield 0
    t = 1
    while t < 1024:
        # google suggests this dither
        yield t + random.random()
        t *= 2
    while True:
        yield t


def atomic_tmp_file(final_path):
    """Return a tmp file name to use with atomic_install.  This will be in the
    same directory as final_path. The temporary file will have the same extension
    as finalPath.  It the final path is in /dev (/dev/null, /dev/stdout), it is
    returned unchanged and atomic_tmp_install will do nothing."""
    final_dir = os.path.dirname(os.path.normpath(final_path))  # can be empty
    if final_dir == '/dev':
        return final_path
    final_basename = os.path.basename(final_path)
    final_ext = os.path.splitext(final_path)[1]
    base_name = "{}.{}.{}.tmp{}".format(final_basename, socket.gethostname(), os.getpid(), final_ext)
    return os.path.join(final_dir, base_name)


def atomic_install(tmp_path, final_path):
    "atomic install of tmp_path as final_path"
    if os.path.dirname(os.path.normpath(final_path)) != '/dev':
        os.rename(tmp_path, final_path)

@contextmanager
def AtomicFileCreate(final_path, keep=False):
    """Context manager to create a temporary file.  Entering returns path to
    the temporary file in the same directory as finalPath.  If the code in
    context succeeds, the file renamed to its actually name.  If an error
    occurs, the file is not installed and is removed unless keep is specified.
    """
    tmp_path = atomic_tmp_file(final_path)
    try:
        yield tmp_path
        atomic_install(tmp_path, final_path)
    except Exception as ex:
        if not keep:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
        raise

def atomic_copy(src_path, dest_path):
    """Copy a file using posix atomic creations semantics."""
    with AtomicFileCreate(dest_path) as dest_path_tmp:
        shutil.copyfile(src_path, dest_path_tmp)

def atomic_copyobj(src_fh, dest_path, length=16384):
    """Copy an open file using posix atomic creations semantics."""
    with AtomicFileCreate(dest_path) as dest_path_tmp:
        with open(dest_path_tmp, 'wb') as dest_path_fh:
            shutil.copyfileobj(src_fh, dest_path_fh, length=length)
