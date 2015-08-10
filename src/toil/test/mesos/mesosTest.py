import tempfile
import shutil
import os
import subprocess
import threading
import unittest
from time import sleep

from toil.lib.bioio import getLogLevelString
from toil.test.mesos.stress import main as stressMain
from toil.test import ToilTest
from toil.batchSystems.mesos.test import MesosTestSupport

lock = threading.Lock()
numCores = 2

class MesosTest( ToilTest, MesosTestSupport ):

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

    def test_hello_world( self ):
        dir = os.path.dirname( os.path.abspath( __file__ ) )
        subprocess.check_call( "python {dir}/helloWorld.py "
                               "--batchSystem=mesos "
                               "--logLevel={logLevel}".format( dir=dir,
                                                               logLevel=getLogLevelString( ) ),
                               shell=True )

    def __do_test_stress( self, useBadExecutor, numJobs ):
        """
        Set task number to number of files you wish to create. Actual number of jobs is jobs+2
        Right now task is set to fail 1/2 tries. To change this, go to badExecutor launchTask method
        """
        stressMain( numJobs, useBadExecutor=useBadExecutor )

    def test_stress_good( self ):
        self.__do_test_stress( False, 2 )

    def test_stress_bad( self ):
        # the second argument is the number of jobs. BadExecutor fails odd tasks, so certain numbers of tasks
        # may never finish because of the "Despite" bug/feature
        self.__do_test_stress( True, 2 )

