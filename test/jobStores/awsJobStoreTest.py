import logging
import os
import tempfile
import uuid
from xml.etree.cElementTree import Element

from jobStores.abstractJobStore import NoSuchJobException, NoSuchFileException

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
        jobOnMaster = master.createFirstJob( "command1", 12, 34 )
        self.assertTrue( master.loadJobTreeState( ).started )
        self.assertTrue( master.exists( jobOnMaster.jobStoreID ) )
        self.assertEquals( jobOnMaster.followOnCommands, [ ('command1', 12, 34, 0) ] )

        # Create a second instance of the job store class, simulating a worker ...
        worker = AWSJobStore( region=self.testRegion, namePrefix=self.namePrefix )
        self.assertTrue( worker.loadJobTreeState( ).started )
        # ... and load the parent job there.
        jobOnWorker = worker.load( jobOnMaster.jobStoreID )
        self.assertEquals( jobOnMaster, jobOnWorker )

        # Add two children
        childSpecs = { ("command2", 23, 45), ("command3", 34, 56) }
        worker.addChildren( jobOnWorker, childSpecs )
        self.assertNotEquals( jobOnWorker, jobOnMaster )
        self.assertEquals( len( jobOnWorker.children ), 2 )
        # Reload parent job on master
        jobOnMaster = master.load( jobOnMaster.jobStoreID )
        self.assertEquals( jobOnWorker, jobOnMaster )
        # Load children
        childJobs = { worker.load( childCommand[ 0 ] ) for childCommand in jobOnMaster.children }

        # Now load the job tree state, i.e. all jobs, indexed to jobTree's liking
        state = master.loadJobTreeState( )
        self.assertTrue( state.started )
        self.assertEquals( state.shellJobs, set( ) )
        self.assertEquals( state.updatedJobs, childJobs )
        # The parent should have two children
        self.assertEquals( state.childCounts, { jobOnMaster.jobStoreID: 2 } )
        self.assertEquals( len( state.childJobStoreIdToParentJob ), 2 )
        # Ensure consistency between children as referred to by the parent and by the jobTree state
        for child in jobOnMaster.children:
            childJobStoreId = child[ 0 ]
            self.assertEquals( state.childJobStoreIdToParentJob[ childJobStoreId ], jobOnMaster )
            childJob = worker.load( childJobStoreId )
            self.assertTrue( childJob in childJobs )
            self.assertEquals( childJob.jobStoreID, childJobStoreId )
            self.assertEquals( childJob.children, [ ] )
            # This would throw if the child command wasn't present
            childSpecs.remove( childJob.followOnCommands[ 0 ][ 0:3 ] )
        # Make sure every child command is accounted for
        self.assertEquals( childSpecs, set( ) )

        for childJob in childJobs:
            childJob.messages.append( 'foo' )
            childJob.followOnCommands.append( ("command4", 45, 67, 0) )
            childJob.logJobStoreFileID = str( uuid.uuid4( ) )
            childJob.remainingRetryCount = 66
            self.assertNotEquals( childJob, master.load( childJob.jobStoreID ) )
        for childJob in childJobs:
            worker.store( childJob )
        for childJob in childJobs:
            self.assertEquals( master.load( childJob.jobStoreID ), childJob )

        # Test job deletions
        for childJob in childJobs:
            master.delete( childJob )
        for childJob in childJobs:
            self.assertFalse( worker.exists( childJob.jobStoreID ) )
            self.assertRaises( NoSuchJobException, worker.load, childJob.jobStoreID )
        # delete should be idempotent
        for childJob in childJobs:
            master.delete( childJob )

        # Test shared files: Write shared file on master, ...
        with master.writeSharedFileStream( "foo" ) as f:
            f.write( "bar" )
        # ... read that file on worker, ...
        with worker.readSharedFileStream( "foo" ) as f:
            self.assertEquals( "bar", f.read( ) )
        # ... and read it again on master.
        with master.readSharedFileStream( "foo" ) as f:
            self.assertEquals( "bar", f.read( ) )

        # Test per-job files: Create empty file on master, ...
        fileOne = worker.getEmptyFileStoreID( jobOnMaster.jobStoreID )
        # ... write to the file on worker, ...
        with worker.updateFileStream( fileOne ) as f:
            f.write( "one" )
        # ... read the file as a stream on the master, ....
        with master.readFileStream( fileOne ) as f:
            self.assertEquals( f.read( ), "one" )
        # ... and copy it to a temporary physical file on the master.
        fh, path = tempfile.mkstemp( )
        try:
            os.close( fh )
            master.readFile( fileOne, path )
            with open( path, 'r+' ) as f:
                self.assertEquals( f.read( ), "one" )
                # Write a different string to the local file ...
                f.seek( 0 )
                f.truncate( 0 )
                f.write( "two" )
            # ... and create a second file from the local file.
            fileTwo = master.writeFile( jobOnMaster.jobStoreID, path )
            with worker.readFileStream( fileTwo ) as f:
                self.assertEquals( f.read( ), "two" )
            # Now update the first file from the local file ...
            master.updateFile( fileOne, path )
            with worker.readFileStream( fileOne ) as f:
                self.assertEquals( f.read( ), "two" )
        finally:
            os.unlink( path )
        # Create a third file to test the last remaining method.
        with worker.writeFileStream( jobOnMaster.jobStoreID ) as ( f, fileThree ):
            f.write( "three" )
        with master.readFileStream( fileThree ) as f:
            self.assertEquals( f.read( ), "three" )
        # Delete a file explicitly but leave files for the implicit deletion through the parent
        worker.deleteFile( fileOne )

        # Delete parent and its associated file
        self.master.delete( jobOnMaster )
        self.assertFalse( self.master.exists( jobOnMaster.jobStoreID ) )
        # Files should be gone as well. NB: the fooStream() methods are context managers,
        # hence the funny invocation.
        self.assertRaises( NoSuchFileException, worker.readFileStream( fileTwo ).__enter__ )
        self.assertRaises( NoSuchFileException, worker.readFileStream( fileThree ).__enter__ )

        # TODO: Who deletes the shared files?

        # TODO: Test stats methods

        # TODO: Test big uploads and downloads

        # TODO: Make this a generic test and run against FileJobStore, too