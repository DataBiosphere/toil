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
        logging.getLogger( 'boto' ).setLevel( logging.INFO )

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
        master = self.master
        self.assertFalse( master.loadJobTreeState( ).started )
        # Test negative case for exists()
        self.assertFalse( master.exists( "foo" ) )

        # Create parent job and verify its existence
        job = master.createFirstJob( "command1", 12, 34 )
        self.assertTrue( master.loadJobTreeState( ).started )
        self.assertTrue( master.exists( job.jobStoreID ) )
        self.assertEquals( job.followOnCommands, [ ('command1', 12, 34, 0) ] )

        # Create a second instance of the job store class, like the one created by a worker ...
        worker = AWSJobStore( region=self.testRegion, namePrefix=self.namePrefix )
        self.assertTrue( worker.loadJobTreeState( ).started )
        # ... and load the parent job there.
        jobOnWorker = worker.load( job.jobStoreID )
        self.assertEquals( job, jobOnWorker )

        # Add two children
        worker.addChildren( jobOnWorker, [ ("command2", 23, 45), ("command3", 34, 56) ] )
        self.assertNotEquals( jobOnWorker, job )
        self.assertEquals( len( jobOnWorker.children ), 2 )
        # Reload parent job on master
        job = master.load( job.jobStoreID )
        self.assertEquals( jobOnWorker, job )

        state = master.loadJobTreeState( )
        self.assertTrue( state.started )
        self.assertEquals( state.childCounts, { job.jobStoreID: 2 } )
        self.assertEquals( len( state.childJobStoreIdToParentJob ), 2 )
        for child in job.children:
            self.assertEquals( state.childJobStoreIdToParentJob[child[0]], job )

        # Test shared files
        with master.writeSharedFileStream( "foo" ) as f:
            f.write( "bar" )
        with worker.readSharedFileStream( "foo" ) as f:
            self.assertEquals( "bar", f.read( ) )
        with master.readSharedFileStream( "foo" ) as f:
            self.assertEquals( "bar", f.read( ) )
