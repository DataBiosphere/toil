from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
import logging
import shutil
import threading
import tempfile
import os
import time
import subprocess
import multiprocessing

log = logging.getLogger(__name__)


class MesosTestSupport(object):
    """
    A mixin for test cases that need a running Mesos master and slave on the local host
    """
    def _startMesos(self, numCores=None):
        if numCores is None:
            numCores = multiprocessing.cpu_count()
        self.mesosDir = tempfile.gettempdir() + '/mesos'
        try:
            shutil.rmtree(self.mesosDir, ignore_errors=True)
        except OSError as err:
            if err.errno != 2:
                raise
        # Setup credentials for the test on the fly
        self.mesosCredentials = tempfile.mkstemp()[1]
        with open(self.mesosCredentials, 'w') as cF:
            cF.write('toil liot')
        self.master = self.MesosMasterThread(numCores, mesosCredentials=self.mesosCredentials)
        self.master.start()
        self.slave = self.MesosSlaveThread(numCores, mesosDir=self.mesosDir)
        self.slave.start()
        while self.master.popen is None or self.slave.popen is None:
            log.info("Waiting for master and slave processes")
            time.sleep(.1)

    def _stopMesos(self):
        self.slave.popen.kill()
        self.slave.join()
        self.master.popen.kill()
        self.master.join()
        shutil.rmtree(self.mesosDir, ignore_errors=True)
        os.remove(self.mesosCredentials)

    class MesosThread(threading.Thread):
        __metaclass__ = ABCMeta

        # Lock is used because subprocess is NOT thread safe: http://tinyurl.com/pkp5pgq
        lock = threading.Lock()

        def __init__(self, numCores, mesosCredentials=None, mesosDir=None):
            threading.Thread.__init__(self)
            self.numCores = numCores
            self.mesosCredentials = mesosCredentials
            self.mesosDir = mesosDir
            self.popen = None

        @abstractmethod
        def mesosCommand(self):
            raise NotImplementedError

        def run(self):
            with self.lock:
                self.popen = subprocess.Popen(self.mesosCommand())
            self.popen.wait()
            log.info('Exiting %s', self.__class__.__name__)


    class MesosMasterThread(MesosThread):
        def mesosCommand(self):
            return ['mesos-master',
                    '--registry=in_memory',
                    '--ip=127.0.0.1',
                    '--port=5050',
                    '--allocation_interval=500ms',
                    '--credentials=' + self.mesosCredentials]

    class MesosSlaveThread(MesosThread):
        def mesosCommand(self):
            diskStats = os.statvfs(os.path.dirname(self.mesosDir))
            totalDiskSize = round(diskStats.f_frsize * diskStats.f_bavail / 1024 / 1024.0, 2)
            if totalDiskSize <= 5120:
                totalDiskSize /= 2
            else:
                totalDiskSize -= 5120
            # NB: The --resources parameter forces this test to use a predictable number of
            # cores, independent of how many cores the system running the test actually has.
            return ['mesos-slave',
                    '--ip=127.0.0.1',
                    '--master=127.0.0.1:5050',
                    '--work_dir=' + self.mesosDir,
                    '--resources=cpus(*):%i;disk(*):%s' % (self.numCores, totalDiskSize),
                    '--executor_shutdown_grace_period=60secs']
