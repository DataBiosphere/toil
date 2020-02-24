# Copyright (C) 2015-2018 Regents of the University of California
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

from __future__ import absolute_import
from future.utils import raise_
from builtins import range
import atexit
import fcntl
import logging
import math
import os
import sys
import tempfile
import threading
import traceback
if sys.version_info >= (3, 0):
    from threading import BoundedSemaphore
else:
    from threading import _BoundedSemaphore as BoundedSemaphore

import psutil

from toil.lib.misc import mkdir_p, robust_rmtree

log = logging.getLogger(__name__)

class BoundedEmptySemaphore( BoundedSemaphore ):
    """
    A bounded semaphore that is initially empty.
    """

    def __init__( self, value=1, verbose=None ):
        super( BoundedEmptySemaphore, self ).__init__( value, verbose )
        for i in range( value ):
            assert self.acquire( blocking=False )


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

    def run( self ):
        try:
            self.tryRun( )
        except:
            self.exc_info = sys.exc_info( )
            raise

    def tryRun( self ):
        super( ExceptionalThread, self ).run( )

    def join( self, *args, **kwargs ):
        super( ExceptionalThread, self ).join( *args, **kwargs )
        if not self.is_alive( ) and self.exc_info is not None:
            type, value, traceback = self.exc_info
            self.exc_info = None
            raise_(type, value, traceback)


# noinspection PyPep8Naming
class defaultlocal(threading.local):
    """
    Thread local storage with default values for each field in each thread

    >>>
    >>> l = defaultlocal( foo=42 )
    >>> def f(): print(l.foo)
    >>> t = threading.Thread(target=f)
    >>> t.start() ; t.join()
    42
    """

    def __init__( self, **kwargs ):
        super( defaultlocal, self ).__init__( )
        self.__dict__.update( kwargs )


def cpu_count():
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
        return cached

    # Get the fallback answer of all the CPUs on the machine
    total_machine_size = psutil.cpu_count(logical=True)

    log.debug('Total machine size: %d cores', total_machine_size) 

    try:
        with open('/sys/fs/cgroup/cpu/cpu.cfs_quota_us', 'r') as stream:
            # Read the quota
            quota = int(stream.read())

        log.debug('CPU quota: %d', quota)

        if quota == -1:
            # Assume we can use the whole machine
            return total_machine_size

        with open('/sys/fs/cgroup/cpu/cpu.cfs_period_us', 'r') as stream:
            # Read the period in which we are allowed to burn the quota
            period = int(stream.read())

        log.debug('CPU quota period: %d', period)

        # The thread count is how many multiples of a wall clcok period we can burn in that period.
        cgroup_size = int(math.ceil(float(quota)/float(period)))

        log.debug('Cgroup size in cores: %d', cgroup_size)

    except:
        # We can't actually read these cgroup fields. Maybe we are a mac or something.
        log.debug('Could not inspect cgroup: %s', traceback.format_exc())
        cgroup_size = float('inf')

    # Return the smaller of the actual thread count and the cgroup's limit, minimum 1.
    result = max(1, min(cgroup_size, total_machine_size))
    log.debug('cpu_count: %s', str(result))
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

# We keep one name per unique Toil workDir (i.e. /tmp or whatever existing
# directory Toil tries to put its workflow directory under.)

# We have a global lock to control looking things up
current_process_name_lock = threading.Lock()
# And a global dict from work directory to name in that work directory.
# We also have a file descriptor per work directory but it is just leaked.
current_process_name_for = {}

def collect_process_name_garbage():
    """
    Delete all the process names that point to files that don't exist anymore
    (because the work directory was temporary and got cleaned up). This is
    known to happen during the tests, which get their own temp directories.

    Caller must hold current_process_name_lock.
    """
    
    global current_process_name_for

    # Collect the workDirs of the missing names to delete them after iterating.
    missing = []

    for workDir, name in current_process_name_for.items():
        if not os.path.exists(os.path.join(workDir, name)):
            # The name file is gone, probably because the work dir is gone.
            missing.append(workDir)

    for workDir in missing:
        del current_process_name_for[workDir]

def destroy_all_process_names():
    """
    Delete all our process name files because our process is going away.

    We let all our FDs get closed by the process death.

    We assume there is nobody else using the system during exit to race with.
    """

    global current_process_name_for

    for workDir, name in current_process_name_for.items():
        robust_rmtree(os.path.join(workDir, name))

# Run the cleanup at exit
atexit.register(destroy_all_process_names)

def get_process_name(workDir):
    """
    Return the name of the current process. Like a PID but visible between
    containers on what to Toil appears to be a node.

    :param str workDir: The Toil work directory. Defines the shared namespace.
    :return: Process's assigned name
    :rtype: str
    """

    global current_process_name_lock
    global current_process_name_for

    with current_process_name_lock:

        # Make sure all the names still exist.
        # TODO: a bit O(n^2) in the number of workDirs in flight at any one time.
        collect_process_name_garbage()

        if workDir in current_process_name_for:
            # If we already gave ourselves a name, return that.
            return current_process_name_for[workDir]

        # We need to get a name file.
        nameFD, nameFileName = tempfile.mkstemp(dir=workDir)

        # Lock the file. The lock will automatically go away if our process does.
        try:
            fcntl.lockf(nameFD, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as e:
            # Someone else might have locked it even though they should not have.
            raise RuntimeError("Could not lock process name file %s: %s" % (nameFileName, str(e)))

        # Save the basename
        current_process_name_for[workDir] = os.path.basename(nameFileName)

        # Return the basename
        return current_process_name_for[workDir] 

        # TODO: we leave the file open forever. We might need that in order for
        # it to stay locked while we are alive.

def process_name_exists(workDir, name):
    """
    Return true if the process named by the given name (from process_name) exists, and false otherwise.

    Can see across container boundaries using the given node workflow directory.

    :param str workDir: The Toil work directory. Defines the shared namespace.
    :param str name: Process's name to poll
    :return: True if the named process is still alive, and False otherwise.
    :rtype: bool
    """

    global current_process_name_lock
    global current_process_name_for
    
    with current_process_name_lock:
        if current_process_name_for.get(workDir, None) == name:
            # We are asking about ourselves. We are alive.
            return True

    # Work out what the corresponding file name is
    nameFileName = os.path.join(workDir, name)
    if not os.path.exists(nameFileName):
        # If the file is gone, the process can't exist.
        return False

    
    nameFD = None
    try:
        # Otherwise see if we can lock it shared, for which we need an FD, but
        # only for reading.
        nameFD = os.open(nameFileName, os.O_RDONLY)
        try:
            fcntl.lockf(nameFD, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except IOError as e:
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
    





        
        

