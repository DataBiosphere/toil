import hashlib
import logging
import os
import shutil
import stat
import sys
import tempfile
import uuid
from collections.abc import Iterator, Iterable
from contextlib import contextmanager
from io import BytesIO
from typing import IO, Any, Callable, Optional, Protocol, Union

from toil.lib.memoize import memoize
from toil.lib.misc import StrPath

logger = logging.getLogger(__name__)


@memoize
def get_toil_home() -> str:
    """
    Get the Toil home directory for storing configuration and global state.

    Raises an error if it does not exist and cannot be created. Safe to run
    simultaneously in multiple processes.
    """
   
    # TODO: should this use an XDG config directory or ~/.config to not clutter the
    # base home directory?
    toil_home_dir = os.path.join(os.path.expanduser("~"), ".toil")

    dir_path = try_path(toil_home_dir)
    if dir_path is None:
        raise RuntimeError(
            f"Cannot create or access Toil configuration directory {toil_home_dir}"
        )
    return dir_path

TOIL_URI_SCHEME = "toilfile:"


STANDARD_SCHEMES = ["http:", "https:", "s3:", "gs:", "ftp:"]
REMOTE_SCHEMES = STANDARD_SCHEMES + [TOIL_URI_SCHEME]
ALL_SCHEMES = REMOTE_SCHEMES + ["file:"]

def is_standard_url(filename: str) -> bool:
    """
    Return True if the given URL is a non-Toil, non-file: URL.
    """
    return is_url_with_scheme(filename, STANDARD_SCHEMES)

def is_remote_url(filename: str) -> bool:
    """
    Decide if a filename is a known, non-file kind of URL
    """
    return is_url_with_scheme(filename, REMOTE_SCHEMES)

def is_any_url(filename: str) -> bool:
    """
    Decide if a string is a URI like http:// or file://.

    Otherwise it might be a bare path.
    """
    return is_url_with_scheme(filename, ALL_SCHEMES)

def is_url_with_scheme(filename: str, schemes: list[str]) -> bool:
    """
    Return True if filename is a URL with any of the given schemes and False otherwise.
    """
    # TODO: "http:myfile.dat" is a valid filename and *not* a valid URL
    for scheme in schemes:
        if filename.startswith(scheme):
            return True
    return False

def is_toil_url(filename: str) -> bool:
    """
    Return True if a URL is a toilfile: URL.
    """
    return is_url_with_scheme(filename, [TOIL_URI_SCHEME])

def is_file_url(filename: str) -> bool:
    """
    Return True if a URL is a file: URL.

    Will return False for bare paths.
    """
    return is_url_with_scheme(filename, ["file:"])

def mkdtemp(
    suffix: Optional[str] = None,
    prefix: Optional[str] = None,
    dir: Optional[StrPath] = None,
) -> str:
    """
    Make a temporary directory like tempfile.mkdtemp, but with relaxed permissions.

    The permissions on the directory will be 711 instead of 700, allowing the
    group and all other users to traverse the directory. This is necessary if
    the directory is on NFS and the Docker daemon would like to mount it or a
    file inside it into a container, because on NFS even the Docker daemon
    appears bound by the file permissions.

    See <https://github.com/DataBiosphere/toil/issues/4644>, and
    <https://stackoverflow.com/a/67928880> which talks about a similar problem
    but in the context of user namespaces.
    """
    # Make the directory
    result = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    # Grant all the permissions: full control for user, and execute for group and other
    os.chmod(
        result, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    )
    # Return the path created
    return result


def robust_rmtree(path: Union[str, bytes]) -> None:
    """
    Robustly tries to delete paths.

    Continues silently if the path to be removed is already gone, or if it
    goes away while this function is executing.

    May raise an error if a path changes between file and directory while the
    function is executing, or if a permission error is encountered.
    """
    # TODO: only allow str or bytes as an input
    if not isinstance(path, bytes):
        # Internally we must work in bytes, in case we find an undecodeable
        # filename.
        path = path.encode("utf-8")

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
        except OSError as exc:
            if exc.errno == 16:
                # 'Device or resource busy'
                return
            raise

        # We assume the directory going away while we have it open won't upset
        # the listdir iterator.
        for child in children:
            # Get the path for each child item in the directory
            child_path = os.path.join(path, child)

            # Remove it if still present
            robust_rmtree(child_path)

        try:
            # Actually remove the directory once the children are gone
            shutil.rmtree(path)
        except FileNotFoundError:
            # Directory went away
            return
        except OSError as exc:
            if exc.errno == 16:
                # 'Device or resource busy'
                return
            raise

    else:
        # It is not or was not a directory.
        try:
            # Unlink it as a normal file
            os.unlink(path)
        except FileNotFoundError:
            # File went away
            return
        except OSError as exc:
            if exc.errno == 16:
                # 'Device or resource busy'
                return
            raise


def atomic_tmp_file(final_path: StrPath) -> str:
    """Return a tmp file name to use with atomic_install.  This will be in the
    same directory as final_path. The temporary file will have the same extension
    as finalPath.  It the final path is in /dev (/dev/null, /dev/stdout), it is
    returned unchanged and atomic_tmp_install will do nothing."""
    final_dir = os.path.dirname(os.path.normpath(final_path))  # can be empty
    if final_dir == "/dev":
        return str(final_path)
    final_basename = os.path.basename(final_path)
    final_ext = os.path.splitext(final_path)[1]
    base_name = f"{final_basename}.{str(uuid.uuid4())}.tmp{final_ext}"
    return os.path.join(final_dir, base_name)


def atomic_install(tmp_path: StrPath, final_path: StrPath) -> None:
    """atomic install of tmp_path as final_path"""
    if os.path.dirname(os.path.normpath(final_path)) != "/dev":
        os.rename(tmp_path, final_path)


@contextmanager
def AtomicFileCreate(final_path: StrPath, keep: bool = False) -> Iterator[str]:
    """Context manager to create a temporary file.  Entering returns path to
    the temporary file in the same directory as finalPath.  If the code in
    context succeeds, the file renamed to its actual name.  If an error
    occurs, the file is not installed and is removed unless keep is specified.
    """
    tmp_path = atomic_tmp_file(final_path)
    try:
        yield tmp_path
        atomic_install(tmp_path, final_path)
    except Exception:
        if not keep:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
        raise


def atomic_copy(
    src_path: str, dest_path: str, executable: Optional[bool] = None
) -> None:
    """Copy a file using posix atomic creations semantics."""
    if executable is None:
        executable = os.stat(src_path).st_mode & stat.S_IXUSR != 0
    with AtomicFileCreate(dest_path) as dest_path_tmp:
        shutil.copyfile(src_path, dest_path_tmp)
        if executable:
            os.chmod(dest_path_tmp, os.stat(dest_path_tmp).st_mode | stat.S_IXUSR)


def atomic_copyobj(
    src_fh: BytesIO, dest_path: str, length: int = 16384, executable: bool = False
) -> None:
    """Copy an open file using posix atomic creations semantics."""
    with AtomicFileCreate(dest_path) as dest_path_tmp:
        with open(dest_path_tmp, "wb") as dest_path_fh:
            shutil.copyfileobj(src_fh, dest_path_fh, length=length)
        if executable:
            os.chmod(dest_path_tmp, os.stat(dest_path_tmp).st_mode | stat.S_IXUSR)


def make_public_dir(in_directory: str, suggested_name: Optional[str] = None) -> str:
    """
    Make a publicly-accessible directory in the given directory.

    :param suggested_name: Use this directory name first if possible.

    Try to make a random directory name with length 4 that doesn't exist, with the given prefix.
    Otherwise, try length 5, length 6, etc, up to a max of 32 (len of uuid4 with dashes replaced).
    This function's purpose is mostly to avoid having long file names when generating directories.
    If somehow this fails, which should be incredibly unlikely, default to a normal uuid4, which was
    our old default.
    """
    if suggested_name is not None:
        generated_dir_path1 = os.path.join(in_directory, suggested_name)
        try:
            os.mkdir(generated_dir_path1)
            os.chmod(generated_dir_path1, 0o777)
            return generated_dir_path1
        except FileExistsError:
            pass
    for i in range(
        4, 32 + 1
    ):  # make random uuids and truncate to lengths starting at 4 and working up to max 32
        for _ in range(10):  # make 10 attempts for each length
            truncated_uuid = str(uuid.uuid4()).replace("-", "")[:i]
            generated_dir_path2 = os.path.join(in_directory, truncated_uuid)
            try:
                os.mkdir(generated_dir_path2)
                os.chmod(generated_dir_path2, 0o777)
                return generated_dir_path2
            except FileExistsError:
                pass
    this_should_never_happen: str = os.path.join(in_directory, str(uuid.uuid4()))
    os.mkdir(this_should_never_happen)
    os.chmod(this_should_never_happen, 0o777)
    return this_should_never_happen


def try_path(path: str, min_size: int = 100 * 1024 * 1024) -> Optional[str]:
    """
    Try to use the given path. Return it if it exists or can be made,
    and we can make things within it, or None otherwise.

    :param min_size: Reject paths on filesystems smaller than this many bytes.
    """

    try:
        os.makedirs(path, exist_ok=True)
    except OSError:
        # Maybe we lack permissions
        return None

    if not os.path.exists(path):
        # We didn't manage to make it
        return None

    if not os.access(path, os.W_OK):
        # It doesn't look writable
        return None

    try:
        stats = os.statvfs(path)
    except OSError:
        # Maybe we lack permissions
        return None

    # Is the filesystem big enough?
    # We need to look at the FS size and not the free space so we don't change
    # over to a different filesystem when this one fills up.
    fs_size = stats.f_frsize * stats.f_blocks
    if fs_size < min_size:
        # Too small
        return None

    return path


class WriteWatchingStream:
    """
    A stream wrapping class that calls any functions passed to onWrite() with the number of bytes written for every write.

    Not seekable.
    """

    def __init__(self, backingStream: IO[Any]) -> None:
        """
        Wrap the given backing stream.
        """

        self.backingStream = backingStream
        # We have no write listeners yet
        self.writeListeners: list[Callable[[int], None]] = []

    def onWrite(self, listener: Callable[[int], None]) -> None:
        """
        Call the given listener with the number of bytes written on every write.
        """

        self.writeListeners.append(listener)

    # Implement the file API from https://docs.python.org/2.4/lib/bltin-file-objects.html

    def write(self, data: bytes) -> None:
        """
        Write the given data to the file.
        """

        # Do the write
        self.backingStream.write(data)

        for listener in self.writeListeners:
            # Send out notifications
            listener(len(data))

    def writelines(self, datas: Iterable[bytes]) -> None:
        """
        Write each string from the given iterable, without newlines.
        """

        for data in datas:
            self.write(data)

    def flush(self) -> None:
        """
        Flush the backing stream.
        """

        self.backingStream.flush()

    def close(self) -> None:
        """
        Close the backing stream.
        """

        self.backingStream.close()

class ReadableFileObj(Protocol):
    """
    Protocol that is more specific than what file_digest takes as an argument.
    Also guarantees a read() method.
    Would extend the protocol from Typeshed for hashlib but those are only
    declared for 3.11+.
    """
    def readinto(self, buf: bytearray, /) -> int: ...
    def readable(self) -> bool: ...
    def read(self, number: int) -> bytes: ...

# hashlib._Hash seems to not appear at runtime
def file_digest(f: ReadableFileObj, alg_name: str) -> "hashlib._Hash":
    """
    Polyfilled hashlib.file_digest that works on Python <3.11.
    """
    if sys.version_info >= (3, 11):
        return hashlib.file_digest(f, alg_name)
    BUFFER_SIZE = 1024 * 1024
    hasher = hashlib.new(alg_name)
    buffer = f.read(BUFFER_SIZE)
    while buffer:
        hasher.update(buffer)
        buffer = f.read(BUFFER_SIZE)
    return hasher


