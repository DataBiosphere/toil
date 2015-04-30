import os
import unittest


class JobTreeTest( unittest.TestCase ):
    @classmethod
    def setUpClass( cls ):
        super( JobTreeTest, cls ).setUpClass( )
        binPath = os.path.join( os.path.dirname( os.path.dirname( __file__ ) ), 'bin' )
        if not os.path.isdir( binPath ):
            raise AssertionError( "The script directory '%s' doesn not exist. Be sure to run 'make "
                                  "scripts' from the root directory." % binPath )
        path = os.environ[ 'PATH' ].split( os.path.pathsep )
        if not binPath in path:
            path.append( binPath )
            os.environ[ 'PATH' ] = os.path.pathsep.join( path )