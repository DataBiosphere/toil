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
from contextlib import contextmanager
from threading import BoundedSemaphore

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
            # Empty out the semaphore
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
    
# Similar to the process naming system above, we define a global mutex system
# for critical sections, based just around file locks.
@contextmanager
def global_mutex(workDir, mutex):
    """
    Context manager that locks a mutex. The mutex is identified by the given
    name, and scoped to the given directory. Works across all containers that
    have access to the given diectory. Mutexes held by dead processes are
    automatically released.
    
    Only works between processes, NOT between threads.
    
    :param str workDir: The Toil work directory. Defines the shared namespace.
    :param str mutex: Mutex to lock. Must be a permissible path component.
    """
    
    # Define a filename
    lock_filename = os.path.join(workDir, 'toil-mutex-' + mutex)
    
    log.debug('PID %d acquiring mutex %s', os.getpid(), lock_filename)
    
    # We can't just create/open and lock a file, because when we clean up
    # there's a race where someone can open the file before we unlink it and
    # get a lock on the deleted file.
    
    while True:
        # Try to create the file, ignoring if it exists or not.
        fd = os.open(lock_filename, os.O_CREAT | os.O_WRONLY)
        
        # Wait until we can exclusively lock it.
        fcntl.lockf(fd, fcntl.LOCK_EX)
        
        # Holding the lock, make sure we are looking at the same file on disk still.
        fd_stats = os.fstat(fd)
        try:
            path_stats = os.stat(lock_filename)
        except FileNotFoundError:
            path_stats = None
        
        if path_stats is None or fd_stats.st_dev != path_stats.st_dev or fd_stats.st_ino != path_stats.st_ino:
            # The file we have a lock on is not the file linked to the name (if
            # any). This usually happens, because before someone releases a
            # lock, they delete the file. Go back and contend again. TODO: This
            # allows a lot of queue jumping on our mutex.
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
        log.debug('PID %d now holds mutex %s', os.getpid(), lock_filename)
        yield
    finally:
        # Delete it while we still own it, so we can't delete it from out from
        # under someone else who thinks they are holding it.
        log.debug('PID %d releasing mutex %s', os.getpid(), lock_filename)
        os.unlink(lock_filename)
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

    def __init__(self, workDir, name):
        """
        Connect to the arena specified by the given workDir and name.
        
        Any process that can access workDir, in any container, can connect to
        the arena. Many arenas can be active with different names.
        
        Doesn't enter or leave the arena.
        
        :param str workDir: The Toil work directory. Defines the shared namespace.
        :param str name: Name of the arena. Must be a permissible path component.
        """
        
        # Save the workDir which namespaces everything
        self.workDir = workDir
        
        # We need a mutex name to allow only one process to be entering or
        # leaving at a time.
        self.mutex = name + '-arena-lock'
        
        # We need a way to track who is actually in, and who was in but died.
        # So everybody gets a locked file (again).
        # TODO: deduplicate with the similar logic for process names, and also
        # deferred functions.
        self.lockfileDir = os.path.join(workDir, name + '-arena-members')
        
        # When we enter the arena, we fill this in with the FD of the locked
        # file that represents our presence.
        self.lockfileFD = None
        # And we fill this in with the file name
        self.lockfileName = None
        
    def enter(self):
        """
        This process is entering the arena. If cleanup is in progress, blocks
        until it is finished.
        
        You may not enter the arena again before leaving it.
        """
       
        log.debug('Joining arena %s', self.lockfileDir)
       
        # Make sure we're not in it already.
        assert self.lockfileName is None
        assert self.lockfileFD is None
        
        with global_mutex(self.workDir, self.mutex):
            # Now nobody else should also be trying to join or leave.
            
            try:
                # Make sure the lockfile directory exists.
                os.mkdir(self.lockfileDir)
            except FileExistsError:
                pass
            
            # Make ourselves a file in it and lock it to prove we are alive.
            self.lockfileFD, self.lockfileName = tempfile.mkstemp(dir=self.lockfileDir)
            # Nobody can see it yet, so lock it right away
            fcntl.lockf(self.lockfileFD, fcntl.LOCK_EX)
            
            # Now we're properly in, so release the global mutex
            
        log.debug('Now in arena %s', self.lockfileDir)
        
    def leave(self):
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
        
        log.debug('Leaving arena %s', self.lockfileDir)
        
        with global_mutex(self.workDir, self.mutex):
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
                except IOError as e:
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
                log.debug('We are the Last Process Standing in arena %s', self.lockfileDir)
                yield True
                
                try:
                    # Delete the arena directory so as to leave nothing behind.
                    os.rmdir(self.lockfileDir)
                except:
                    log.warning('Could not clean up arena %s completely: %s',
                                self.lockfileDir, traceback.format_exc())
                    pass
            
            # Now we're done, whether we were the last one or not, and can
            # release the mutex.
            
        log.debug('Now out of arena %s', self.lockfileDir)


        
        

