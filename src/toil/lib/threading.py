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
import logging
import math
import sys
import threading
import traceback
if sys.version_info >= (3, 0):
    from threading import BoundedSemaphore
else:
    from threading import _BoundedSemaphore as BoundedSemaphore

import psutil

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
    
