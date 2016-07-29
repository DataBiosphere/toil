from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
import logging
import shutil
import threading
import subprocess
import multiprocessing

from bd2k.util.processes import which
from bd2k.util.threading import ExceptionalThread

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

    def _stopMesos(self):
        self.slave.popen.kill()
        self.slave.join()
        self.master.popen.kill()
        self.master.join()

    class MesosThread(ExceptionalThread):
        __metaclass__ = ABCMeta

        # Lock is used because subprocess is NOT thread safe: http://tinyurl.com/pkp5pgq
        lock = threading.Lock()

        def __init__(self, numCores):
            threading.Thread.__init__(self)
            self.numCores = numCores
            with self.lock:
                self.popen = subprocess.Popen(self.mesosCommand())

        @abstractmethod
        def mesosCommand(self):
            raise NotImplementedError

        def tryRun(self):
            self.popen.wait()
            log.info('Exiting %s', self.__class__.__name__)

        def findMesosBinary(self, name):
            try:
                return next(which(name))
            except StopIteration:
                try:
                    # Special case for users of PyCharm on OS X. This is where Homebrew installs
                    # it. It's hard to set PATH for PyCharm (or any GUI app) on OS X so let's
                    # make it easy for those poor souls.
                    return next(which(name, path=['/usr/local/sbin']))
                except StopIteration:
                    raise RuntimeError("Cannot find the '%s' binary. Make sure Mesos is installed "
                                       "and it's 'bin' directory is present on the PATH." % name)

    class MesosMasterThread(MesosThread):
        def mesosCommand(self):
            return [self.findMesosBinary('mesos-master'),
                    '--registry=in_memory',
                    '--ip=127.0.0.1',
                    '--port=5050',
                    '--allocation_interval=500ms']

    class MesosSlaveThread(MesosThread):
        def mesosCommand(self):
            # NB: The --resources parameter forces this test to use a predictable number of
            # cores, independent of how many cores the system running the test actually has.
            return [self.findMesosBinary('mesos-slave'),
                    '--ip=127.0.0.1',
                    '--master=127.0.0.1:5050',
                    '--attributes=preemptable:False',
                    '--resources=cpus(*):%i' % self.numCores]
