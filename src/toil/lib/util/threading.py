from __future__ import absolute_import
from builtins import range
import sys
import threading


class ExceptionalThread( threading.Thread ):
    """
    A thread whose join() method re-raises exceptions raised during run(). While join() is
    idempotent, the exception is only during the first invocation of join() that succesfully
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
            tmp_exc_info = self.exc_info
            self.exc_info = None
            raise tmp_exc_info


# noinspection PyPep8Naming
class defaultlocal( threading.local ):
    """
    Thread local storage with default values for each field in each thread

    >>> l = defaultlocal( foo=42 )
    >>> def f(): print l.foo
    >>> t = threading.Thread(target=f)
    >>> t.start() ; t.join()
    42
    """

    def __init__( self, **kwargs ):
        super( defaultlocal, self ).__init__( )
        self.__dict__.update( kwargs )
