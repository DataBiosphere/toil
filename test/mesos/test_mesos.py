import tempfile
import shutil
import unittest
import os
import subprocess
import threading
import sys
from time import sleep

from jobTree.test.mesos.StressTest import main as stressMain
from jobTree.test import JobTreeTest

lock = threading.Lock()
class TestMesos( JobTreeTest ):

    class MesosMasterThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.popen = None

        def run(self):
            with lock:
                self.popen = subprocess.Popen(['/usr/local/sbin/mesos-master', '--registry=in_memory', '--ip=127.0.0.1'])
            self.popen.wait()


    class MesosSlaveThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.popen = None

        def run(self):
            with lock:
                self.popen = subprocess.Popen(['/usr/local/sbin/mesos-slave', '--ip=127.0.0.1', '--master=127.0.0.1:5050'])
            self.popen.wait()

    master = MesosMasterThread( )
    slave = MesosSlaveThread( )

    @classmethod
    def setUpClass( cls ):
        super( TestMesos, cls ).setUpClass( )
        # FIXME: avoid daemon threads use join
        cls.master.setDaemon( True )
        cls.slave.setDaemon( True )
        cls.master.start( )
        cls.slave.start( )

    @classmethod
    def tearDownClass( cls ):
        super( TestMesos, cls ).tearDownClass( )
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
        dir = os.path.abspath( os.path.dirname( __file__ ) )
        subprocess.check_call("python {}/jobTree_HelloWorld.py --batchSystem=mesos --logLevel=DEBUG".format(dir), shell=True)
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
        # the second argument is the number of targets. Badexecutor fails odd tasks, so certain numbers of tasks
        # may never finish because of the "Despite" bug/feature
        self.__do_test_stress( True, 2 )

    @unittest.skip
    def test_resume( self ):
        mainT = threading.Thread( target=self.__do_test_stress, args=(False, 3) )
        mainT.start( )
        sleep(3)
        # This isn't killing the slave. we need possibly kill -KILL subprocess call with pid.
        print "killing"
        TestMesos.killSlave( )
        print "killed"
        TestMesos.startSlave( )
        self.assertTrue( os.path.isfile( "./hello_world_child2.txt" ) )
        self.assertTrue( os.path.isfile( "./hello_world_follow.txt" ) )
