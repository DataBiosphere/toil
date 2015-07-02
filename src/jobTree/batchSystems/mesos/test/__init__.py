from abc import ABCMeta, abstractmethod
import logging
import shutil
import threading
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
        shutil.rmtree('/tmp/mesos', ignore_errors=True)
        self.master = self.MesosMasterThread(numCores)
        self.master.start()
        self.slave = self.MesosSlaveThread(numCores)
        self.slave.start()
        while self.master.popen is None or self.slave.popen is None:
            log.info("Waiting for master and slave processes")
            time.sleep(.1)

    def _stopMesos(self):
        self.slave.popen.kill()
        self.slave.join()
        self.master.popen.kill()
        self.master.join()

    class MesosThread(threading.Thread):
        __metaclass__ = ABCMeta

        # Lock is used because subprocess is NOT thread safe: http://tinyurl.com/pkp5pgq
        lock = threading.Lock()

        def __init__(self, numCores):
            threading.Thread.__init__(self)
            self.numCores = numCores
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
                    '--allocation_interval=100ms']

    class MesosSlaveThread(MesosThread):
        def mesosCommand(self):
            # NB: The --resources parameter forces this test to use a predictable number of cores, independent of how
            # many cores the system running the test actually has.
            return ['mesos-slave',
                    '--ip=127.0.0.1',
                    '--master=127.0.0.1:5050',
                    '--resources=cpus(*):%i' % self.numCores]
