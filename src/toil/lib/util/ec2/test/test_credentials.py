from builtins import range
import logging

import errno

import os
import unittest

from toil.lib.util.ec2.credentials import (enable_metadata_credential_caching,
                                       disable_metadata_credential_caching, cache_path)


def get_access_key( ):
    from boto.provider import Provider
    provider = Provider( 'aws' )
    return None if provider._credential_expiry_time is None else provider.get_access_key( ) 


class CredentialsTest( unittest.TestCase ):
    def __init__( self, *args, **kwargs ):
        super( CredentialsTest, self ).__init__( *args, **kwargs )
        self.cache_path = os.path.expanduser( cache_path )

    @classmethod
    def setUpClass( cls ):
        super( CredentialsTest, cls ).setUpClass( )
        logging.basicConfig( level=logging.DEBUG )

    def setUp( self ):
        super( CredentialsTest, self ).setUp( )
        self.cleanUp( )

    def cleanUp( self ):
        try:
            os.unlink( self.cache_path )
        except OSError as e:
            if e.errno == errno.ENOENT:
                pass
            else:
                raise

    def tearDown( self ):
        super( CredentialsTest, self ).tearDown( )
        self.cleanUp( )

    def test_metadata_credential_caching( self ):
        """
        Brute forces many concurrent requests for getting temporary credentials. If you comment
        out the calls to enable_metadata_credential_caching, you should see some failures due to
        requests timing out. The test will also take much longer in that case.
        """
        num_tests = 1000
        num_processes = 32
        # Get key without caching
        access_key = get_access_key( )
        self.assertFalse( os.path.exists( self.cache_path ) )
        enable_metadata_credential_caching( )
        # Again for idempotence
        enable_metadata_credential_caching( )
        try:
            futures = [ ]
            from multiprocessing import Pool
            pool = Pool( num_processes )
            try:
                for i in range( num_tests ):
                    futures.append( pool.apply_async( get_access_key ) )
            except:
                pool.close( )
                pool.terminate( )
                raise
            else:
                pool.close( )
                pool.join( )
        finally:
            disable_metadata_credential_caching( )
            # Again for idempotence
            disable_metadata_credential_caching( )
        self.assertEquals( access_key is not None, os.path.exists( self.cache_path ) )
        self.assertEquals( len( futures ), num_tests )
        access_keys = [ f.get( ) for f in futures ]
        self.assertEquals( len( access_keys ), num_tests )
        access_keys = set( access_keys )
        self.assertEquals( len( access_keys ), 1 )
        self.assertEquals( access_keys.pop( ), access_key )
