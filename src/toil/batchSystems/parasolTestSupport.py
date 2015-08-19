from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
import logging
import shutil
import threading
import time
import subprocess
import multiprocessing
import os
from toil.lib.bioio import getTempFile

log = logging.getLogger(__name__)


class ParasolTestSupport(object):
    """
    For test cases that need a running Parasol leader and worker on the local host
    """
    def _startParasol(self, numCores=None):
        if numCores is None:
            numCores = multiprocessing.cpu_count()
        self.machineList = os.path.join(os.getcwd(), "machineList.txt")
        with open(self.machineList, "w") as out:
            out.write("localhost 4 3000 /tmp /scratch 36000 r1")

        self.leader = self.ParasolLeaderThread(numCores)
        self.leader.start()
        self.worker = self.ParasolWorkerThread(numCores)
        self.worker.start()
        while self.leader.popen is None or self.worker.popen is None:
            log.info("Waiting for leader and worker processes")
            time.sleep(.1)

    def _stopParasol(self):
        self.worker.popen.kill()
        self.worker.join()
        self.leader.popen.kill()
        self.leader.join()
        os.remove(self.machineList)
        if os.path.exists("para.results"):
            os.remove("para.results")
        if os.path.exists("parasol.jid"):
            os.remove("parasol.jid")

    class ParasolThread(threading.Thread):
        __metaclass__ = ABCMeta

        # Lock is used because subprocess is NOT thread safe: http://tinyurl.com/pkp5pgq
        lock = threading.Lock()

        def __init__(self, numCores):
            threading.Thread.__init__(self)
            self.numCores = numCores
            self.popen = None
            

        @abstractmethod
        def parasolCommand(self):
            raise NotImplementedError

        def run(self):
            with self.lock:
                self.popen = subprocess.Popen(self.parasolCommand())
            self.popen.wait()
            log.info('Exiting %s', self.__class__.__name__)

    class ParasolLeaderThread(ParasolThread):
        def parasolCommand(self):
            return ['paraHub',
                    '-debug',
                    os.path.join(os.getcwd(), "machineList.txt")]

    class ParasolWorkerThread(ParasolThread):
        def parasolCommand(self):
            return ['paraNode',
                    '-cpu=4',
                    '-debug',
                    'start']
