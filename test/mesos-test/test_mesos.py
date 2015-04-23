import tempfile
import shutil
import unittest
import os
from jobTree_HelloWorld import main as testMain
import subprocess
import threading

def startMesosMaster():
    subprocess.check_call("mesos-master --registry=in_memory --ip=127.0.0.1", shell=True)

def startMesosSlave():
    subprocess.check_call("mesos-slave --ip=127.0.0.1 --master=127.0.0.1:5050", shell=True)

class TestMesos(unittest.TestCase):
    mesosMasterThread = threading.Thread(target=startMesosMaster)
    mesosSlaveThread = threading.Thread(target=startMesosSlave)

    @classmethod
    def setUpClass(cls):
        # unittest.TestCase.setUpClass(self)
        # cls.startDir= os.getcwd()
        # cls.tempDir=None
        cls.mesosMasterThread.setDaemon(True)
        cls.mesosSlaveThread.setDaemon(True)
        cls.mesosMasterThread.start()
        cls.mesosSlaveThread.start()

    # @classmethod
    # def tearDownClass(cls):
    #     # unittest.TestCase.setUpClass(self)
    #     os.chdir(cls.startDir)

    def setUp(self):
        self.startDir=os.getcwd()
        self.tempDir=tempfile.mkdtemp()
        os.chdir(self.tempDir)

    def tearDown(self):
        os.chdir(self.startDir)
        shutil.rmtree(self.tempDir)

    def test_hello_world(self):
        dir = os.path.abspath(os.path.dirname(__file__))
        print(dir)
        subprocess.check_call("python {}/jobTree_HelloWorld.py --batchSystem=mesos".format(dir), shell=True)
        self.assertTrue(os.path.isfile("./hello_world.txt"))
        self.assertTrue(os.path.isfile("./hello_world_child.txt"))