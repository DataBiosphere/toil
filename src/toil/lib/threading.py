# Copyright (C) 2015-2021 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# 5.14.2018: copied into Toil from https://github.com/BD2KGenomics/bd2k-python-lib
# Note: renamed from "threading.py" to "threading.py" to avoid conflicting imports
# from the built-in "threading" from psutil in python3.9
import atexit
import errno
import fcntl
import logging
import math
import os
import platform
import subprocess
import sys
import tempfile
import time
import threading
import traceback
from contextlib import contextmanager
from typing import Dict, Iterator, Optional, Union, cast

import psutil

from toil.lib.exceptions import raise_
from toil.lib.io import robust_rmtree
from toil.lib.memoize import memoize

logger = logging.getLogger(__name__)

def ensure_filesystem_lockable(path: str, timeout: float = 30, hint: Optional[str] = None) -> None:
    """
    Make sure that the filesystem used at the given path is one where locks are safe to use.

    File locks are not safe to use on Ceph. See
    <https://github.com/DataBiosphere/toil/issues/4972>.

    Raises an exception if the filesystem is detected as one where using locks
    is known to trigger bugs in the filesystem implementation. Also raises an
    exception if the given path does not exist, or if attempting to determine
    the filesystem type takes more than the timeout in seconds.
    
    If the filesystem type cannot be determined, does nothing.

    :param hint: Extra text to include in an error, if raised, telling the user
        how to change the offending path.
    """

    if not os.path.exists(path):
        # Raise a normal-looking FileNotFoundError. See <https://stackoverflow.com/a/36077407>
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

    if platform.system() == "Linux":
        # We know how to find the filesystem here.
        
        try:
            # Start a child process to stat the path. See <https://unix.stackexchange.com/a/402236>.
            # We really should call statfs but no bindings for it are in PyPI.
            completed = subprocess.run(["stat", "-f", "-c", "%T", path], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        except subprocess.TimeoutExpired as e:
            # The subprocess itself is Too Slow
            raise RuntimeError(f"Polling filesystem type at {path} took more than {timeout} seconds; is your filesystem working?") from e
        except subprocess.CalledProcessError as e:
            # Stat didn't work. Maybe we don't have the right version of stat installed?
            logger.warning("Could not determine filesystem type at %s because of: %s", path, e.stderr.decode("utf-8", errors="replace").strip())
            # If we don't know the filesystem type, keep going anyway.
            return

        filesystem_type = completed.stdout.decode("utf-8", errors="replace").strip()

        if filesystem_type == "ceph":
            # Ceph is known to deadlock the MDS and break the parent directory when locking.
            message = [f"Refusing to use {path} because file locks are known to break {filesystem_type} filesystems."]
            if hint:
                # Hint the user how to fix this.
                message.append(hint)
            raise RuntimeError(' '.join(message))
        else:
            # Other filesystem types are fine (even though NFS is sometimes
            # flaky with regard to locks actually locking anything).
            logger.debug("Detected that %s has lockable filesystem type: %s", path, filesystem_type)

    # Other platforms (Mac) probably aren't mounting Ceph and also don't
    # usually use the same stat binary implementation.

def safe_lock(fd: int, block: bool = True, shared: bool = False) -> None:
    """
    Get an fcntl lock, while retrying on IO errors.

    Raises OSError with EACCES or EAGAIN when a nonblocking lock is not
    immediately available.
    """

    # Set up retry logic. TODO: Use @retry instead.
    error_backoff = 1
    MAX_ERROR_TRIES = 10
    error_tries = 0

    while True:
        try:
            # Wait until we can exclusively lock it.
            lock_mode = (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | (fcntl.LOCK_NB if not block else 0)
            fcntl.flock(fd, lock_mode)
            return
        except OSError as e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                # Nonblocking lock not available.
                raise
            elif e.errno == errno.EIO:
                # Sometimes Ceph produces IO errors when talking to lock files.
                # Back off and try again.
                # TODO: Should we eventually give up if the disk really is
                # broken? If so we should use the retry system.
                if error_tries < MAX_ERROR_TRIES:
                    logger.error("IO error talking to lock file. Retrying after %s seconds.", error_backoff)
                    time.sleep(error_backoff)
                    error_backoff = min(60, error_backoff * 2)
                    error_tries += 1
                    continue
                else:
                    logger.critical("Too many IO errors talking to lock file. If using Ceph, check for MDS deadlocks. See <https://tracker.ceph.com/issues/62123>.")
                    raise
            else:
                raise

def safe_unlock_and_close(fd: int) -> None:
    """
    Release an fcntl lock and close the file descriptor, while handling fcntl IO errors.
    """
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError as e:
        if e.errno != errno.EIO:
            raise
        # Sometimes Ceph produces EIO. We don't need to retry then because
        # we're going to close the FD and after that the file can't remain
        # locked by us.
    os.close(fd)

class ExceptionalThread(threading.Thread):
    """
    A thread whose join() method re-raises exceptions raised during run(). While join() is
    idempotent, the exception is only during the first invocation of join() that successfully
    joined the thread. If join() times out, no exception will be re reraised even though an
    exception might already have occurred in run().

    When subclassing this thread, override tryRun() instead of run().

    >>> def f():
    ...     assert 0
    >>> t = ExceptionalThread(target=f)
    >>> t.start()
    >>> t.join()
    Traceback (most recent call last):
    ...
    AssertionError

    >>> class MyThread(ExceptionalThread):
    ...     def tryRun( self ):
    ...         assert 0
    >>> t = MyThread()
    >>> t.start()
    >>> t.join()
    Traceback (most recent call last):
    ...
    AssertionError

    """
    exc_info = None

    def run(self) -> None:
        try:
            self.tryRun()
        except:
            self.exc_info = sys.exc_info()
            raise

    def tryRun(self) -> None:
        super().run()

    def join(self, *args: Optional[float], **kwargs: Optional[float]) -> None:
        super().join(*args, **kwargs)
        if not self.is_alive() and self.exc_info is not None:
            exc_type, exc_value, traceback = self.exc_info
            self.exc_info = None
            raise_(exc_type, exc_value, traceback)


def cpu_count() -> int:
    """
    Get the rounded-up integer number of whole CPUs available.

    Counts hyperthreads as CPUs.

    Uses the system's actual CPU count, or the current v1 cgroup's quota per
    period, if the quota is set.

    Ignores the cgroup's cpu shares value, because it's extremely difficult to
    interpret. See https://github.com/kubernetes/kubernetes/issues/81021.

    Caches result for efficiency.

    :return: Integer count of available CPUs, minimum 1.
    :rtype: int
    """

    cached = getattr(cpu_count, 'result', None)
    if cached is not None:
        # We already got a CPU count.
        return cast(int, cached)

    # Get the fallback answer of all the CPUs on the machine
    psutil_cpu_count = cast(Optional[int], psutil.cpu_count(logical=True))
    if psutil_cpu_count is None:
        logger.debug('Could not retrieve the logical CPU count.')

    total_machine_size: Union[float, int] = psutil_cpu_count if psutil_cpu_count is not None else float('inf')
    logger.debug('Total machine size: %s core(s)', total_machine_size)

    # cgroups may limit the size
    cgroup_size: Union[float, int] = float('inf')

    try:
        # See if we can fetch these and use them
        quota: Optional[int] = None
        period: Optional[int] = None

        # CGroups v1 keeps quota and period separate
        CGROUP1_QUOTA_FILE = '/sys/fs/cgroup/cpu/cpu.cfs_quota_us'
        CGROUP1_PERIOD_FILE = '/sys/fs/cgroup/cpu/cpu.cfs_period_us'
        # CGroups v2 keeps both in one file, space-separated, quota first
        CGROUP2_COMBINED_FILE = '/sys/fs/cgroup/cpu.max'

        if os.path.exists(CGROUP1_QUOTA_FILE) and os.path.exists(CGROUP1_PERIOD_FILE):
            logger.debug('CPU quota and period available from cgroups v1')
            with open(CGROUP1_QUOTA_FILE) as stream:
                # Read the quota
                quota = int(stream.read())

            with open(CGROUP1_PERIOD_FILE) as stream:
                # Read the period in which we are allowed to burn the quota
                period = int(stream.read())
        elif os.path.exists(CGROUP2_COMBINED_FILE):
            logger.debug('CPU quota and period available from cgroups v2')
            with open(CGROUP2_COMBINED_FILE) as stream:
                # Read the quota and the period together
                quota, period = (int(part) for part in stream.read().split(' '))
        else:
            logger.debug('CPU quota/period not available from cgroups v1 or cgroups v2')

        if quota is not None and period is not None:
            # We got a quota and a period.
            logger.debug('CPU quota: %d period: %d', quota, period)

            if quota == -1:
                # But the quota can be -1 for unset.
                # Assume we can use the whole machine.
                cgroup_size = float('inf')
            else:
                # The thread count is how many multiples of a wall clock period we
                # can burn in that period.
                cgroup_size = int(math.ceil(float(quota)/float(period)))

            logger.debug('Control group size in cores: %s', cgroup_size)
    except:
        # We can't actually read these cgroup fields. Maybe we are a mac or something.
        logger.debug('Could not inspect cgroup: %s', traceback.format_exc())

    # CPU affinity may limit the size
    affinity_size: Union[float, int] = float('inf')
    if hasattr(os, 'sched_getaffinity'):
        try:
            logger.debug('CPU affinity available')
            affinity_size = len(os.sched_getaffinity(0))
            logger.debug('CPU affinity is restricted to %d cores', affinity_size)
        except:
             # We can't actually read this even though it exists.
            logger.debug('Could not inspect scheduling affinity: %s', traceback.format_exc())
    else:
        logger.debug('CPU affinity not available')

    limit: Union[float, int] = float('inf')
    # Apply all the limits to take the smallest
    limit = min(limit, total_machine_size)
    limit = min(limit, cgroup_size)
    limit = min(limit, affinity_size)
    if limit < 1 or limit == float('inf'):
        # Fall back to 1 if we can't get a size
        limit = 1
    result = int(limit)
    logger.debug('cpu_count: %s', result)
    # Make sure to remember it for the next call
    setattr(cpu_count, 'result', result)
    return result


# PIDs are a bad identifier, because they are not shared between containers
# and also may be reused.
# So instead we have another system for file store implementations to
# coordinate among themselves, based on file locks.
# TODO: deduplicate with DeferredFunctionManager?
# TODO: Wrap in a class as static methods?

# Note that we don't offer a way to enumerate these names. You can only get
# your name and poll others' names (or your own). So we don't have
# distinguishing prefixes or WIP suffixes to allow for enumeration.

# We keep one name per unique base directory (probably a Toil coordination
# directory).

# We have a global lock to control looking things up
current_process_name_lock = threading.Lock()
# And a global dict from work directory to name in that work directory.
# We also have a file descriptor per work directory but it is just leaked.
current_process_name_for: Dict[str, str] = {}

def collect_process_name_garbage() -> None:
    """
    Delete all the process names that point to files that don't exist anymore
    (because the work directory was temporary and got cleaned up). This is
    known to happen during the tests, which get their own temp directories.

    Caller must hold current_process_name_lock.
    """

    global current_process_name_for

    # Collect the base_dirs of the missing names to delete them after iterating.
    missing = []

    for base_dir, name in current_process_name_for.items():
        if not os.path.exists(os.path.join(base_dir, name)):
            # The name file is gone, probably because the work dir is gone.
            missing.append(base_dir)

    for base_dir in missing:
        del current_process_name_for[base_dir]

def destroy_all_process_names() -> None:
    """
    Delete all our process name files because our process is going away.

    We let all our FDs get closed by the process death.

    We assume there is nobody else using the system during exit to race with.
    """

    global current_process_name_for

    for base_dir, name in current_process_name_for.items():
        robust_rmtree(os.path.join(base_dir, name))

# Run the cleanup at exit
atexit.register(destroy_all_process_names)

def get_process_name(base_dir: str) -> str:
    """
    Return the name of the current process. Like a PID but visible between
    containers on what to Toil appears to be a node.

    :param str base_dir: Base directory to work in. Defines the shared namespace.
    :return: Process's assigned name
    :rtype: str
    """

    global current_process_name_lock
    global current_process_name_for

    with current_process_name_lock:

        # Make sure all the names still exist.
        # TODO: a bit O(n^2) in the number of base_dirs in flight at any one time.
        collect_process_name_garbage()

        if base_dir in current_process_name_for:
            # If we already gave ourselves a name, return that.
            return current_process_name_for[base_dir]

        # We need to get a name file.
        nameFD, nameFileName = tempfile.mkstemp(dir=base_dir)

        # Lock the file. The lock will automatically go away if our process does.
        try:
            safe_lock(nameFD, block=False)
        except OSError as e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                # Someone else locked it even though they should not have.
                raise RuntimeError(f"Could not lock process name file {nameFileName}") from e
            else:
                # Something else is wrong
                raise

        # Save the basename
        current_process_name_for[base_dir] = os.path.basename(nameFileName)

        # Return the basename
        return current_process_name_for[base_dir]

        # TODO: we leave the file open forever. We might need that in order for
        # it to stay locked while we are alive.


def process_name_exists(base_dir: str, name: str) -> bool:
    """
    Return true if the process named by the given name (from process_name) exists, and false otherwise.

    Can see across container boundaries using the given node workflow directory.

    :param str base_dir: Base directory to work in. Defines the shared namespace.
    :param str name: Process's name to poll
    :return: True if the named process is still alive, and False otherwise.
    :rtype: bool
    """

    global current_process_name_lock
    global current_process_name_for

    with current_process_name_lock:
        if current_process_name_for.get(base_dir, None) == name:
            # We are asking about ourselves. We are alive.
            return True

    # Work out what the corresponding file name is
    nameFileName = os.path.join(base_dir, name)
    if not os.path.exists(nameFileName):
        # If the file is gone, the process can't exist.
        return False


    nameFD = None
    try:
        try:
            # Otherwise see if we can lock it shared, for which we need an FD, but
            # only for reading.
            nameFD = os.open(nameFileName, os.O_RDONLY)
        except FileNotFoundError as e:
            # File has vanished
            return False
        try:
            safe_lock(nameFD, block=False, shared=True)
        except OSError as e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                # Could not lock. Process is alive.
                return True
            else:
                # Something else went wrong
                raise
        else:
            # Could lock. Process is dead.
            # Remove the file. We race to be the first to do so.
            try:
                os.remove(nameFileName)
            except:
                pass
            safe_unlock_and_close(nameFD)
            nameFD = None
            # Report process death
            return False
    finally:
        if nameFD is not None:
            try:
                os.close(nameFD)
            except:
                pass

# Similar to the process naming system above, we define a global mutex system
# for critical sections, based just around file locks.
@contextmanager
def global_mutex(base_dir: str, mutex: str) -> Iterator[None]:
    """
    Context manager that locks a mutex. The mutex is identified by the given
    name, and scoped to the given directory. Works across all containers that
    have access to the given diectory. Mutexes held by dead processes are
    automatically released.

    Only works between processes, NOT between threads.

    :param str base_dir: Base directory to work in. Defines the shared namespace.
    :param str mutex: Mutex to lock. Must be a permissible path component.
    """

    if not os.path.isdir(base_dir):
        raise RuntimeError(f"Directory {base_dir} for mutex does not exist")

    # TODO: We don't know what CLI option controls where to put this mutex, so
    # we aren't very helpful if the location is bad.
    ensure_filesystem_lockable(
        base_dir,
        hint=f"Specify a different place to put the {mutex} mutex."
    )

    # Define a filename
    lock_filename = os.path.join(base_dir, 'toil-mutex-' + mutex)

    logger.debug('PID %d acquiring mutex %s', os.getpid(), lock_filename)

    # We can't just create/open and lock a file, because when we clean up
    # there's a race where someone can open the file before we unlink it and
    # get a lock on the deleted file.

    error_backoff = 1

    while True:
        # Try to create the file, ignoring if it exists or not.
        fd = os.open(lock_filename, os.O_CREAT | os.O_WRONLY)

        try:
            # Wait until we can exclusively lock it, handling error retry.
            safe_lock(fd)
        except:
            # Something went wrong
            os.close(fd)
            raise

        # Holding the lock, make sure we are looking at the same file on disk still.
        try:
            # So get the stats from the open file
            fd_stats = os.fstat(fd)
        except OSError as e:
            if e.errno == errno.ESTALE:
                # The file handle has gone stale, because somebody removed the
                # file.
                # Try again.
                safe_unlock_and_close(fd)
                continue
            else:
                # Something else broke
                os.close(fd)
                raise

        try:
            # And get the stats for the name in the directory
            path_stats: Optional[os.stat_result] = os.stat(lock_filename)
        except FileNotFoundError:
            path_stats = None

        if path_stats is None or fd_stats.st_dev != path_stats.st_dev or fd_stats.st_ino != path_stats.st_ino:
            # The file we have a lock on is not the file linked to the name (if
            # any). This usually happens, because before someone releases a
            # lock, they delete the file. Go back and contend again. TODO: This
            # allows a lot of queue jumping on our mutex.
            safe_unlock_and_close(fd)
            continue
        else:
            # We have a lock on the file that the name points to. Since we
            # hold the lock, nobody will be deleting it or can be in the
            # process of deleting it. Stop contending; we have the mutex.
            break

    try:
        # When we have it, do the thing we are protecting.
        logger.debug('PID %d now holds mutex %s', os.getpid(), lock_filename)
        yield
    finally:
        # Delete it while we still own it, so we can't delete it from out from
        # under someone else who thinks they are holding it.
        logger.debug('PID %d releasing mutex %s', os.getpid(), lock_filename)

        # We have had observations in the wild of the lock file not exisiting
        # when we go to unlink it, causing a crash on mutex release. See
        # <https://github.com/DataBiosphere/toil/issues/4654>.
        #
        # We want to tolerate this; maybe unlink() interacts with fcntl() locks
        # on NFS in a way that is actually fine, somehow? But we also want to
        # complain loudly if something is tampering with our locks or not
        # really enforcing locks on the filesystem, so we will notice if it is
        # the cause of further problems.
        try:
            path_stats = os.stat(lock_filename)
        except FileNotFoundError:
            path_stats = None

        # Check to make sure it still looks locked before we unlink.
        if path_stats is None:
            logger.error('PID %d had mutex %s disappear while locked! Mutex system is not working!', os.getpid(), lock_filename)
        elif fd_stats.st_dev != path_stats.st_dev or fd_stats.st_ino != path_stats.st_ino:
            logger.error('PID %d had mutex %s get replaced while locked! Mutex system is not working!', os.getpid(), lock_filename)

        if path_stats is not None:
            try:
                # Unlink the file
                os.unlink(lock_filename)
            except FileNotFoundError:
                logger.error('PID %d had mutex %s disappear between stat and unlink while unlocking! Mutex system is not working!', os.getpid(), lock_filename)

        # Note that we are unlinking it and then unlocking it; a lot of people
        # might have opened it before we unlinked it and will wake up when they
        # get the worthless lock on the now-unlinked file. We have to do some
        # stat gymnastics above to work around this.
        safe_unlock_and_close(fd)


class LastProcessStandingArena:
    """
    Class that lets a bunch of processes detect and elect a last process
    standing.

    Process enter and leave (sometimes due to sudden existence failure). We
    guarantee that the last process to leave, if it leaves properly, will get a
    chance to do some cleanup. If new processes try to enter during the
    cleanup, they will be delayed until after the cleanup has happened and the
    previous "last" process has finished leaving.

    The user is responsible for making sure you always leave if you enter!
    Consider using a try/finally; this class is not a context manager.
    """

    def __init__(self, base_dir: str, name: str) -> None:
        """
        Connect to the arena specified by the given base_dir and name.

        Any process that can access base_dir, in any container, can connect to
        the arena. Many arenas can be active with different names.

        Doesn't enter or leave the arena.

        :param str base_dir: Base directory to work in. Defines the shared namespace.
        :param str name: Name of the arena. Must be a permissible path component.
        """

        # Save the base_dir which namespaces everything
        self.base_dir = base_dir

        # We need a mutex name to allow only one process to be entering or
        # leaving at a time.
        self.mutex = name + '-arena-lock'

        # We need a way to track who is actually in, and who was in but died.
        # So everybody gets a locked file (again).
        # TODO: deduplicate with the similar logic for process names, and also
        # deferred functions.
        self.lockfileDir = os.path.join(base_dir, name + '-arena-members')

        # When we enter the arena, we fill this in with the FD of the locked
        # file that represents our presence.
        self.lockfileFD = None
        # And we fill this in with the file name
        self.lockfileName = None

    def enter(self) -> None:
        """
        This process is entering the arena. If cleanup is in progress, blocks
        until it is finished.

        You may not enter the arena again before leaving it.
        """

        logger.debug('Joining arena %s', self.lockfileDir)

        # Make sure we're not in it already.
        if self.lockfileName is not None or self.lockfileFD is not None:
            raise RuntimeError("A process is already in the arena")

        with global_mutex(self.base_dir, self.mutex):
            # Now nobody else should also be trying to join or leave.

            try:
                # Make sure the lockfile directory exists.
                os.mkdir(self.lockfileDir)
            except FileExistsError:
                pass
            except Exception as e:
                raise RuntimeError("Could not make lock file directory " + self.lockfileDir) from e

            # Make ourselves a file in it and lock it to prove we are alive.
            try:
                self.lockfileFD, self.lockfileName = tempfile.mkstemp(dir=self.lockfileDir) # type: ignore
            except Exception as e:
                raise RuntimeError("Could not make lock file in " + self.lockfileDir) from e
            # Nobody can see it yet, so lock it right away
            safe_lock(self.lockfileFD) # type: ignore

            # Now we're properly in, so release the global mutex

        logger.debug('Now in arena %s', self.lockfileDir)

    def leave(self) -> Iterator[bool]:
        """
        This process is leaving the arena. If this process happens to be the
        last process standing, yields something, with other processes blocked
        from joining the arena until the loop body completes and the process
        has finished leaving. Otherwise, does not yield anything.

        Should be used in a loop:

            for _ in arena.leave():
                # If we get here, we were the last process. Do the cleanup
                pass
        """

        # Make sure we're in it to start.
        if self.lockfileName is None or self.lockfileFD is None:
            raise RuntimeError("This process is not in the arena.")

        logger.debug('Leaving arena %s', self.lockfileDir)

        with global_mutex(self.base_dir, self.mutex):
            # Now nobody else should also be trying to join or leave.

            # Take ourselves out.
            try:
                os.unlink(self.lockfileName)
            except:
                pass
            self.lockfileName = None
            safe_unlock_and_close(self.lockfileFD)
            self.lockfileFD = None

            for item in os.listdir(self.lockfileDir):
                # There is someone claiming to be here. Are they alive?
                full_path = os.path.join(self.lockfileDir, item)

                try:
                    fd = os.open(full_path, os.O_RDONLY)
                except OSError as e:
                    # suddenly file doesnt exist on network file system?
                    continue

                try:
                    safe_lock(fd, block=False, shared=True)
                except OSError as e:
                    if e.errno in (errno.EACCES, errno.EAGAIN):
                        # Could not lock. It's alive!
                        break
                    else:
                        # Something else is wrong
                        os.close(fd)
                        raise
                else:
                    # Could lock. Process is dead.
                    try:
                        os.remove(full_path)
                    except:
                        pass
                    safe_unlock_and_close(fd)
                    # Continue with the loop normally.
            else:
                # Nothing alive was found. Nobody will come in while we hold
                # the global mutex, so we are the Last Process Standing.
                logger.debug('We are the Last Process Standing in arena %s', self.lockfileDir)
                yield True

                try:
                    # Delete the arena directory so as to leave nothing behind.
                    os.rmdir(self.lockfileDir)
                except:
                    logger.warning('Could not clean up arena %s completely: %s',
                                   self.lockfileDir, traceback.format_exc())

            # Now we're done, whether we were the last one or not, and can
            # release the mutex.

        logger.debug('Now out of arena %s', self.lockfileDir)
