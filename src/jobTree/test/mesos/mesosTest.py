import tempfile
import shutil
import unittest
import os
import subprocess
import threading
from time import sleep

from jobTree.test.mesos.stress import main as stressMain
from jobTree.test import JobTreeTest


lock = threading.Lock()
class MesosTest( JobTreeTest ):

    class MesosMasterThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.popen = None

        def run(self):
            with lock:
                self.popen = subprocess.Popen(['mesos-master', '--registry=in_memory', '--ip=127.0.0.1'])
            self.popen.wait()


    class MesosSlaveThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.popen = None

        def run(self):
            with lock:
                self.popen = subprocess.Popen(['mesos-slave', '--ip=127.0.0.1', '--master=127.0.0.1:5050'])
            self.popen.wait()

    master = MesosMasterThread( )
    slave = MesosSlaveThread( )

    @classmethod
    def setUpClass( cls ):
        super( MesosTest, cls ).setUpClass( )
        # FIXME: avoid daemon threads use join
        cls.master.setDaemon( True )
        cls.slave.setDaemon( True )
        cls.master.start( )
        cls.slave.start( )

    @classmethod
    def tearDownClass( cls ):
        super( MesosTest, cls ).tearDownClass( )
        cls.master.popen.kill( )
        cls.slave.popen.kill( )
        # FIXME: join the threads

    @classmethod
    def killSlave( cls ):
        pid = cls.slave.popen.pid
        os.kill( pid, 9 )

    @classmethod
    def startSlave( cls ):
        cls.slave.run( )

    def setUp( self ):
        # shutil.rmtree("/tmp/mesos/")
        self.startDir = os.getcwd( )
        self.tempDir = tempfile.mkdtemp( )
        print "Using %s for files and directories created by this test run" % self.tempDir
        os.chdir( self.tempDir )

    def tearDown( self ):
        os.chdir( self.startDir )
        shutil.rmtree( self.tempDir )

    def test_hello_world( self ):
        dir = os.path.dirname( os.path.abspath( __file__ ) )
        subprocess.check_call("python {}/helloWorld.py --batchSystem=mesos --logLevel=DEBUG".format(dir), shell=True)
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

    # FIXME: Make this work or remove

    if False:
        def test_resume( self ):
            mainT = threading.Thread( target=self.__do_test_stress, args=(False, 3) )
            mainT.start( )
            sleep(3)
            # This isn't killing the slave. we need possibly kill -KILL subprocess call with pid.
            print "killing"
            MesosTest.killSlave( )
            print "killed"
            MesosTest.startSlave( )
            self.assertTrue( os.path.isfile( "./hello_world_child2.txt" ) )
            self.assertTrue( os.path.isfile( "./hello_world_follow.txt" ) )
