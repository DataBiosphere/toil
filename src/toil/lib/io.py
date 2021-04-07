import logging
import os
import stat
import shutil
import tempfile
import uuid

from contextlib import contextmanager
from io import BytesIO
from typing import Iterator, Union, Optional

logger = logging.getLogger(__name__)


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
            shutil.rmtree(path)
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


def atomic_tmp_file(final_path: str) -> str:
    """Return a tmp file name to use with atomic_install.  This will be in the
    same directory as final_path. The temporary file will have the same extension
    as finalPath.  It the final path is in /dev (/dev/null, /dev/stdout), it is
    returned unchanged and atomic_tmp_install will do nothing."""
    final_dir = os.path.dirname(os.path.normpath(final_path))  # can be empty
    if final_dir == '/dev':
        return final_path
    final_basename = os.path.basename(final_path)
    final_ext = os.path.splitext(final_path)[1]
    base_name = "{}.{}.tmp{}".format(final_basename, uuid.uuid4(), final_ext)
    return os.path.join(final_dir, base_name)


def atomic_install(tmp_path, final_path) -> None:
    """atomic install of tmp_path as final_path"""
    if os.path.dirname(os.path.normpath(final_path)) != '/dev':
        os.rename(tmp_path, final_path)

@contextmanager
def AtomicFileCreate(final_path: str, keep: bool = False) -> Iterator[str]:
    """Context manager to create a temporary file.  Entering returns path to
    the temporary file in the same directory as finalPath.  If the code in
    context succeeds, the file renamed to its actually name.  If an error
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


def atomic_copy(src_path: str, dest_path: str, executable: Optional[bool] = None) -> None:
    """Copy a file using posix atomic creations semantics."""
    if executable is None:
        executable = os.stat(src_path).st_mode & stat.S_IXUSR != 0
    with AtomicFileCreate(dest_path) as dest_path_tmp:
        shutil.copyfile(src_path, dest_path_tmp)
        if executable:
            os.chmod(dest_path_tmp, os.stat(dest_path_tmp).st_mode | stat.S_IXUSR)


def atomic_copyobj(src_fh: BytesIO, dest_path: str, length: int = 16384, executable: bool = False) -> None:
    """Copy an open file using posix atomic creations semantics."""
    with AtomicFileCreate(dest_path) as dest_path_tmp:
        with open(dest_path_tmp, 'wb') as dest_path_fh:
            shutil.copyfileobj(src_fh, dest_path_fh, length=length)
        if executable:
            os.chmod(dest_path_tmp, os.stat(dest_path_tmp).st_mode | stat.S_IXUSR)


def make_public_dir(in_directory: Optional[str] = None) -> str:
    """
    Try to make a random directory name with length 4 that doesn't exist, with the given prefix.
    Otherwise, try length 5, length 6, etc, up to a max of 32 (len of uuid4 with dashes replaced).
    This function's purpose is mostly to avoid having long file names when generating directories.
    If somehow this fails, which should be incredibly unlikely, default to a normal uuid4, which was
    our old default.
    """
    for i in range(4, 32 + 1):  # make random uuids and truncate to lengths starting at 4 and working up to max 32
        for _ in range(10):  # make 10 attempts for each length
            truncated_uuid: str = str(uuid.uuid4()).replace('-', '')[:i]
            generated_dir_path: str = os.path.join(in_directory, truncated_uuid)
            try:
                os.mkdir(generated_dir_path)
                os.chmod(generated_dir_path, 0o777)
                return generated_dir_path
            except FileExistsError:
                pass
    this_should_never_happen: str = os.path.join(in_directory, str(uuid.uuid4()))
    os.mkdir(this_should_never_happen)
    os.chmod(this_should_never_happen, 0o777)
    return this_should_never_happen


class WriteWatchingStream:
    """
    A stream wrapping class that calls any functions passed to onWrite() with the number of bytes written for every write.

    Not seekable.
    """

    def __init__(self, backingStream):
        """
        Wrap the given backing stream.
        """

        self.backingStream = backingStream
        # We have no write listeners yet
        self.writeListeners = []

    def onWrite(self, listener):
        """
        Call the given listener with the number of bytes written on every write.
        """

        self.writeListeners.append(listener)

    # Implement the file API from https://docs.python.org/2.4/lib/bltin-file-objects.html

    def write(self, data):
        """
        Write the given data to the file.
        """

        # Do the write
        self.backingStream.write(data)

        for listener in self.writeListeners:
            # Send out notifications
            listener(len(data))

    def writelines(self, datas):
        """
        Write each string from the given iterable, without newlines.
        """

        for data in datas:
            self.write(data)

    def flush(self):
        """
        Flush the backing stream.
        """

        self.backingStream.flush()

    def close(self):
        """
        Close the backing stream.
        """

        self.backingStream.close()
