from __future__ import absolute_import
import logging
import tempfile
import threading
import time
import subprocess
import multiprocessing
import os
from bd2k.util.files import rm_f
from bd2k.util.objects import InnerClass

from toil import physicalMemory

log = logging.getLogger(__name__)


class ParasolTestSupport(object):
    """
    For test cases that need a running Parasol leader and worker on the local host
    """

    def _startParasol(self, numCores=None, memory=None):
        if numCores is None:
            numCores = multiprocessing.cpu_count()
        if memory is None:
            memory = physicalMemory()
        self.numCores = numCores
        self.memory = memory
        self.leader = self.ParasolLeaderThread()
        self.leader.start()
        self.worker = self.ParasolWorkerThread()
        self.worker.start()
        while self.leader.popen is None or self.worker.popen is None:
            log.info('Waiting for leader and worker processes')
            time.sleep(.1)

    def _stopParasol(self):
        self.worker.popen.kill()
        self.worker.join()
        self.leader.popen.kill()
        self.leader.join()
        for path in ('para.results', 'parasol.jid'):
            rm_f(path)

    class ParasolThread(threading.Thread):

        # Lock is used because subprocess is NOT thread safe: http://tinyurl.com/pkp5pgq
        lock = threading.Lock()

        def __init__(self):
            threading.Thread.__init__(self)
            self.popen = None

        def parasolCommand(self):
            raise NotImplementedError

        def run(self):
            command = self.parasolCommand()
            with self.lock:
                self.popen = subprocess.Popen(command)
            status = self.popen.wait()
            if status != 0:
                log.error("Command '%s' failed with %i.", command, status)
                raise subprocess.CalledProcessError(status, command)
            log.info('Exiting %s', self.__class__.__name__)

    @InnerClass
    class ParasolLeaderThread(ParasolThread):

        def __init__(self):
            super(ParasolTestSupport.ParasolLeaderThread, self).__init__()
            self.machineList = None

        def run(self):
            with tempfile.NamedTemporaryFile(prefix='machineList.txt', mode='w') as f:
                self.machineList = f.name
                # name - Network name
                # cpus - Number of CPUs we can use
                # ramSize - Megabytes of memory
                # tempDir - Location of (local) temp dir
                # localDir - Location of local data dir
                # localSize - Megabytes of local disk
                # switchName - Name of switch this is on
                f.write('localhost {numCores} {ramSize} {tempDir} {tempDir} 1024 foo'.format(
                    numCores=self.outer.numCores,
                    tempDir=tempfile.gettempdir(),
                    ramSize=self.outer.memory / 1024 / 1024))
                f.flush()
                super(ParasolTestSupport.ParasolLeaderThread, self).run()

        def parasolCommand(self):
            return ['paraHub',
                    '-spokes=1',
                    '-debug',
                    self.machineList]

    @InnerClass
    class ParasolWorkerThread(ParasolThread):
        def parasolCommand(self):
            return ['paraNode',
                    '-cpu=%i' % self.outer.numCores,
                    '-randomDelay=0',
                    '-debug',
                    'start']
