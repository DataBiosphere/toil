import logging
import uuid
from xml.etree.cElementTree import Element

from jobStores.awsJobStore import AWSJobStore
from test import JobTreeTest

logger = logging.getLogger( __name__ )


class AWSJobStoreTest( JobTreeTest ):
    @classmethod
    def setUpClass( cls ):
        super( AWSJobStoreTest, cls ).setUpClass( )
        logging.basicConfig( level=logging.DEBUG )
        logging.getLogger('boto').setLevel(logging.INFO)

    testRegion = "us-west-2"

    def _dummyConfig( self ):
        config = Element( "config" )
        config.attrib[ "try_count" ] = "1"
        return config

    def setUp( self ):
        super( AWSJobStoreTest, self ).setUp( )
        self.namePrefix = str( uuid.uuid4( ) )
        config = self._dummyConfig( )
        self.master = AWSJobStore.create( "%s:%s" % (self.testRegion, self.namePrefix), config )

    def tearDown( self ):
        self.master.destroy( )
        super( AWSJobStoreTest, self ).tearDown( )

    def test( self ):
        self.assertFalse( self.master.exists( "foo" ) )
        job = self.master.createFirstJob( "0", 1, 2 )
        self.assertTrue( self.master.exists( job.jobStoreID ) )
        with self.master.writeSharedFileStream( "foo" ) as f:
            f.write( "bar" )
        worker = AWSJobStore( region=self.testRegion, namePrefix=self.namePrefix )
        with worker.readSharedFileStream( "foo" ) as f:
            self.assertEquals( "bar", f.read( ) )


