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
import fcntl
import logging
import math
import os
import sys
import tempfile
import threading
import traceback
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional, Union, cast

import psutil  # type: ignore

from toil.lib.exceptions import raise_
from toil.lib.io import robust_rmtree

logger = logging.getLogger(__name__)


class ExceptionalThread(threading.Thread):
    """
    A thread whose join() method re-raises exceptions raised during run(). While join() is
    idempotent, the exception is only during the first invocation of join() that successfully
    joined the thread. If join() times out, no exception will be re reraised even though an
    exception might already have occured in run().

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
    total_machine_size = cast(int, psutil.cpu_count(logical=True))

    logger.debug('Total machine size: %d cores', total_machine_size)

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
                return total_machine_size

            # The thread count is how many multiples of a wall clock period we
            # can burn in that period.
            cgroup_size = int(math.ceil(float(quota)/float(period)))

            logger.debug('Control group size in cores: %d', cgroup_size)
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

    # Return the smaller of the actual thread count and the cgroup's limit, minimum 1.
    result = cast(int, max(1, min(min(affinity_size, cgroup_size), total_machine_size)))
    logger.debug('cpu_count: %s', str(result))
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
            fcntl.lockf(nameFD, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            # Someone else might have locked it even though they should not have.
            raise RuntimeError(f"Could not lock process name file {nameFileName}: {str(e)}")

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
            fcntl.lockf(nameFD, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except FileNotFoundError as e:
            # File has vanished
            return False
        except OSError as e:
            # Could not lock. Process is alive.
            return True
        else:
            # Could lock. Process is dead.
            # Remove the file. We race to be the first to do so.
            try:
                os.remove(nameFileName)
            except:
                pass
            # Unlock
            fcntl.lockf(nameFD, fcntl.LOCK_UN)
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

    # Define a filename
    lock_filename = os.path.join(base_dir, 'toil-mutex-' + mutex)

    logger.debug('PID %d acquiring mutex %s', os.getpid(), lock_filename)

    # We can't just create/open and lock a file, because when we clean up
    # there's a race where someone can open the file before we unlink it and
    # get a lock on the deleted file.

    while True:
        fd = -1

        try:
            # Try to create the file, ignoring if it exists or not.
            fd = os.open(lock_filename, os.O_CREAT | os.O_WRONLY)

            # Wait until we can exclusively lock it.
            fcntl.lockf(fd, fcntl.LOCK_EX)

            # Holding the lock, make sure we are looking at the same file on disk still.
            fd_stats = os.fstat(fd)

            path_stats: Optional[os.stat_result] = os.stat(lock_filename)
        except FileNotFoundError:
            path_stats = None

        if path_stats is None or fd_stats.st_dev != path_stats.st_dev or fd_stats.st_ino != path_stats.st_ino:
            # The file we have a lock on is not the file linked to the name (if
            # any). This usually happens, because before someone releases a
            # lock, they delete the file. Go back and contend again. TODO: This
            # allows a lot of queue jumping on our mutex.
            if fd != -1:
                fcntl.lockf(fd, fcntl.LOCK_UN)
                os.close(fd)
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
        os.unlink(lock_filename)
        if fd != -1:
            fcntl.lockf(fd, fcntl.LOCK_UN)
            # Note that we are unlinking it and then unlocking it; a lot of people
            # might have opened it before we unlinked it and will wake up when they
            # get the worthless lock on the now-unlinked file. We have to do some
            # stat gymnastics above to work around this.
            os.close(fd)


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
        assert self.lockfileName is None
        assert self.lockfileFD is None

        with global_mutex(self.base_dir, self.mutex):
            # Now nobody else should also be trying to join or leave.

            try:
                # Make sure the lockfile directory exists.
                os.mkdir(self.lockfileDir)
            except FileExistsError:
                pass

            # Make ourselves a file in it and lock it to prove we are alive.
            self.lockfileFD, self.lockfileName = tempfile.mkstemp(dir=self.lockfileDir) # type: ignore
            # Nobody can see it yet, so lock it right away
            fcntl.lockf(self.lockfileFD, fcntl.LOCK_EX) # type: ignore

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
        assert self.lockfileName is not None
        assert self.lockfileFD is not None

        logger.debug('Leaving arena %s', self.lockfileDir)

        with global_mutex(self.base_dir, self.mutex):
            # Now nobody else should also be trying to join or leave.

            # Take ourselves out.
            try:
                os.unlink(self.lockfileName)
            except:
                pass
            self.lockfileName = None
            fcntl.lockf(self.lockfileFD, fcntl.LOCK_UN)
            os.close(self.lockfileFD)
            self.lockfileFD = None

            for item in os.listdir(self.lockfileDir):
                # There is someone claiming to be here. Are they alive?
                full_path = os.path.join(self.lockfileDir, item)

                fd = os.open(full_path, os.O_RDONLY)
                try:
                    fcntl.lockf(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except OSError as e:
                    # Could not lock. It's alive!
                    break
                else:
                    # Could lock. Process is dead.
                    try:
                        os.remove(full_path)
                    except:
                        pass
                    fcntl.lockf(fd, fcntl.LOCK_UN)
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
