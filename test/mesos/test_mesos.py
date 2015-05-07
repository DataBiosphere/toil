import tempfile
import shutil
import unittest
import os
import subprocess
import threading

from jobTree.test.mesos.ResumeTest import run as testRun
from jobTree.test.mesos.StressTest import main as stressMain
from jobTree.test import JobTreeTest


class TestMesos(JobTreeTest):

    class MesosMasterThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.popen = subprocess.Popen("mesos-master --registry=in_memory --ip=127.0.0.1", shell=True)
            # FIXME: add blocking wait

    class MesosSlaveThread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)
            self.popen = None

        def run(self):
            self.popen = subprocess.Popen("mesos-slave --ip=127.0.0.1 --master=127.0.0.1:5050", shell=True)
            # FIXME: add blocking wait

    master=MesosMasterThread()
    slave=MesosSlaveThread()

    @classmethod
    def setUpClass(cls):
        super( TestMesos, cls).setUpClass()
        # FIXME: avoid daemon threads use join
        cls.master.setDaemon(True)
        cls.slave.setDaemon(True)
        cls.master.start()
        cls.slave.start()

    @classmethod
    def tearDownClass(cls):
        super( TestMesos, cls).tearDownClass()
        cls.master.popen.kill()
        cls.slave.popen.kill()
        # FIMXE: join the threads

    @classmethod
    def killSlave(cls):
        pid = cls.slave.popen.pid
        os.kill(pid, 9)

    @classmethod
    def startSlave(cls):
        cls.slave.run()

    def setUp(self):
        # subprocess.check_call("rm -rf /tmp/mesos/")
        self.startDir=os.getcwd()
        self.tempDir=tempfile.mkdtemp()
        os.chdir(self.tempDir)

    def tearDown(self):
        os.chdir(self.startDir)
        shutil.rmtree(self.tempDir)

    def test_hello_world(self):
        dir = os.path.abspath(os.path.dirname(__file__))
        subprocess.check_call("python {}/jobTree_HelloWorld.py --batchSystem=mesos --logLevel=DEBUG".format(dir), shell=True)
        self.assertTrue(os.path.isfile("./bar_bam.txt"))

    def test_class_script(self):
        dir = os.path.abspath(os.path.dirname(__file__))
        subprocess.check_call("python {}/LongTest.py --batchSystem=mesos".format(dir), shell=True)
        self.assertTrue(os.path.isfile("./hello_world_child2.txt"))
        self.assertTrue(os.path.isfile("./hello_world_follow.txt"))

    def test_stress(self):
        """
        Set task number to number of files you wish to create. Actual number of targets is targets+2
        Right now task is set to fail 1/2 tries. To change this, go to badExecutor launchTask method
        """
        numTargets=5
        stressMain(numTargets,useBadExecutor=True)
        for i in range (0,numTargets):
            self.assertTrue(os.path.isfile("./hello_world_child{}.txt".format(i)), "actual files: {}".format(os.listdir(".")))
            self.assertTrue(os.path.isfile("./hello_world_follow{}.txt".format(i)),  "actual files: {}".format(os.listdir(".")))

    @unittest.skip
    def test_resume(self):
        mainT = threading.Thread(target=testRun,args=(3,))
        mainT.start()
        #This isn't killing the slave. we need possibly kill -KILL subprocess call with pid.
        print "killing"
        TestMesos.killSlave()
        print "killed"
        TestMesos.startSlave()
        mainT.join()
        self.assertTrue(os.path.isfile("./hello_world_child2.txt"))
        self.assertTrue(os.path.isfile("./hello_world_follow.txt"))
