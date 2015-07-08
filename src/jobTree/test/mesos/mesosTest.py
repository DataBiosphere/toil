import tempfile
import shutil
import os
import subprocess
import threading
import unittest
from time import sleep

from jobTree.lib.bioio import getLogLevelString
from jobTree.test.mesos.stress import main as stressMain
from jobTree.test import JobTreeTest
from jobTree.batchSystems.mesos.test import MesosTestSupport

lock = threading.Lock()
numCores = 2

class MesosTest( JobTreeTest, MesosTestSupport ):

    @classmethod
    def setUpClass( cls ):
        super( MesosTest, cls ).setUpClass( )
        shutil.rmtree('/tmp/mesos', ignore_errors=True)

    def setUp( self ):
        # shutil.rmtree("/tmp/mesos/")
        self._startMesos(numCores)
        self.startDir = os.getcwd( )
        self.tempDir = tempfile.mkdtemp( )
        print "Using %s for files and directories created by this test run" % self.tempDir
        os.chdir( self.tempDir )

    def tearDown( self ):
        self._stopMesos()
        os.chdir( self.startDir )
        shutil.rmtree( self.tempDir )
    @unittest.skip
    def test_hello_world( self ):
        dir = os.path.dirname( os.path.abspath( __file__ ) )
        subprocess.check_call( "python {dir}/helloWorld.py "
                               "--batchSystem=mesos "
                               "--logLevel={logLevel}".format( dir=dir,
                                                               logLevel=getLogLevelString( ) ),
                               shell=True )
        self.assertTrue( os.path.isfile( "./bar_bam.txt" ) )

    def __do_test_stress( self, useBadExecutor, numTargets ):
        """
        Set task number to number of files you wish to create. Actual number of targets is targets+2
        Right now task is set to fail 1/2 tries. To change this, go to badExecutor launchTask method
        """
        stressMain( numTargets, useBadExecutor=useBadExecutor )
        for i in range( 0, numTargets ):
            self.assertTrue( os.path.isfile( "./hello_world_child_{}.txt".format( i ) ),
                             "actual files: {}".format( os.listdir( "." ) ) )
            self.assertTrue( os.path.isfile( "./hello_world_followOn_{}.txt".format( i ) ),
                             "actual files: {}".format( os.listdir( "." ) ) )
            self.assertTrue("hello_world_parentFollowOn_.txt")

    def test_stress_good( self ):
        self.__do_test_stress( False, 2 )

    def test_stress_bad( self ):
        # the second argument is the number of targets. BadExecutor fails odd tasks, so certain numbers of tasks
        # may never finish because of the "Despite" bug/feature
        self.__do_test_stress( True, 2 )

