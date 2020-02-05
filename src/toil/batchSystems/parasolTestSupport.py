# Copyright (C) 2015-2018 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from builtins import object
import logging
import tempfile
import threading
import time

import subprocess
import signal
import os
import errno
from toil.lib.objects import InnerClass
from toil.lib.threading import cpu_count

from toil import physicalMemory

log = logging.getLogger(__name__)

def rm_f(path):
    """Remove the file at the given path with os.remove(), ignoring errors caused by the file's absence."""
    try:
        os.remove(path)
    except OSError as e:
        if e.errno == errno.ENOENT:
            pass
        else:
            raise

class ParasolTestSupport(object):
    """
    For test cases that need a running Parasol leader and worker on the local host
    """

    def _startParasol(self, numCores=None, memory=None):
        if numCores is None:
            numCores = cpu_count()
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
            if status != 0 and status != -signal.SIGKILL:
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
