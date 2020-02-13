from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

from builtins import next
from builtins import object
from abc import ABCMeta, abstractmethod
import logging
import shutil
import threading
from toil import subprocess
from past.builtins import basestring
from six.moves.urllib.request import urlopen
from contextlib import closing
import time

from toil.lib.retry import retry
from toil import which  # replace with shutil.which() directly; python3 only
from toil.lib.threading import ExceptionalThread, cpu_count
from future.utils import with_metaclass

log = logging.getLogger(__name__)


class MesosTestSupport(object):
    """
    A mixin for test cases that need a running Mesos master and agent on the local host
    """

    def _startMesos(self, numCores=None):
        if numCores is None:
            numCores = cpu_count()
        shutil.rmtree('/tmp/mesos', ignore_errors=True)
        self.master = self.MesosMasterThread(numCores)
        self.master.start()
        self.agent = self.MesosAgentThread(numCores)
        self.agent.start()
        
        # Wait for the master to come up.
        # Bad Things will happen if the master is not yet ready when Toil tries to use it.
        for attempt in retry(predicate=lambda e: True):
            with attempt:
                log.info('Checking if Mesos is ready...')
                with closing(urlopen('http://127.0.0.1:5050/version')) as content:
                    content.read()
        
        log.info('Mesos is ready! Running test.')

    def _stopProcess(self, process, timeout=10):
        """
        Gracefully stop a process on a timeout, given the Popen object for the process.
        """
        
        process.terminate()
        waited = 0
        while waited < timeout and process.poll() is None:
            time.sleep(1)
            waited += 1
        if process.poll() is None:
            # It didn't shut down gracefully
            log.warning('Forcibly killing child which ignored SIGTERM')
            process.kill()

    def _stopMesos(self):
        self._stopProcess(self.agent.popen)
        self.agent.join()
        self._stopProcess(self.master.popen)
        self.master.join()

    class MesosThread(with_metaclass(ABCMeta, ExceptionalThread)):
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

        def findMesosBinary(self, names):
            if isinstance(names, basestring):
                # Handle a single string
                names = [names]
        
            for name in names:
                try:
                    return which(name)
                except StopIteration:
                    try:
                        # Special case for users of PyCharm on OS X. This is where Homebrew installs
                        # it. It's hard to set PATH for PyCharm (or any GUI app) on OS X so let's
                        # make it easy for those poor souls.
                        return which(name, path='/usr/local/sbin')
                    except StopIteration:
                        pass
            
            # If we get here, nothing we can use is present. We need to complain.
            if len(names) == 1:
                sought = "binary '%s'" % names[0]
            else:
                sought = 'any binary in %s' % str(names)
            
            raise RuntimeError("Cannot find %s. Make sure Mesos is installed "
                                "and it's 'bin' directory is present on the PATH." % sought)

    class MesosMasterThread(MesosThread):
        def mesosCommand(self):
            return [self.findMesosBinary('mesos-master'),
                    '--registry=in_memory',
                    '--ip=127.0.0.1',
                    '--port=5050',
                    '--allocation_interval=500ms']

    class MesosAgentThread(MesosThread):
        def mesosCommand(self):
            # NB: The --resources parameter forces this test to use a predictable number of
            # cores, independent of how many cores the system running the test actually has.
            # We also make sure to point it explicitly at the right temp work directory, and
            # to disable systemd support because we have to be root to make systemd make us
            # things and we probably aren't when testing.
            return [self.findMesosBinary(['mesos-agent', 'mesos-slave']),
                    '--ip=127.0.0.1',
                    '--master=127.0.0.1:5050',
                    '--attributes=preemptable:False',
                    '--resources=cpus(*):%i' % self.numCores,
                    '--work_dir=/tmp/mesos',
                    '--no-systemd_enable_support']
