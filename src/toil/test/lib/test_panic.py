import inspect
import logging
import unittest
import sys

from bd2k.util.exceptions import panic

log = logging.getLogger( __name__ )
logging.basicConfig( )


class TestPanic( unittest.TestCase ):
    def test_panic_by_hand( self ):
        try:
            self.try_and_panic_by_hand( )
        except:
            self.__assert_raised_exception_is_primary( )

    def test_panic( self ):
        try:
            self.try_and_panic( )
        except:
            self.__assert_raised_exception_is_primary( )

    def test_panic_with_secondary( self ):
        try:
            self.try_and_panic_with_secondary( )
        except:
            self.__assert_raised_exception_is_primary( )

    def test_nested_panic( self ):
        try:
            self.try_and_nested_panic_with_secondary( )
        except:
            self.__assert_raised_exception_is_primary( )

    def try_and_panic_by_hand( self ):
        try:
            self.line_of_primary_exc = inspect.currentframe( ).f_lineno + 1
            raise ValueError( "primary" )
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info( )
            try:
                raise RuntimeError( "secondary" )
            except Exception:
                pass
            raise exc_type, exc_value, exc_traceback

    def try_and_panic( self ):
        try:
            self.line_of_primary_exc = inspect.currentframe( ).f_lineno + 1
            raise ValueError( "primary" )
        except:
            with panic( log ):
                pass

    def try_and_panic_with_secondary( self ):
        try:
            self.line_of_primary_exc = inspect.currentframe( ).f_lineno + 1
            raise ValueError( "primary" )
        except:
            with panic( log ):
                raise RuntimeError( "secondary" )

    def try_and_nested_panic_with_secondary( self ):
        try:
            self.line_of_primary_exc = inspect.currentframe( ).f_lineno + 1
            raise ValueError( "primary" )
        except:
            with panic( log ):
                with panic( log ):
                    raise RuntimeError( "secondary" )

    def __assert_raised_exception_is_primary( self ):
        exc_type, exc_value, exc_traceback = sys.exc_info( )
        self.assertEquals( exc_type, ValueError )
        self.assertEquals( exc_value.message, "primary" )
        while exc_traceback.tb_next is not None:
            exc_traceback = exc_traceback.tb_next
        self.assertEquals( exc_traceback.tb_lineno, self.line_of_primary_exc )
