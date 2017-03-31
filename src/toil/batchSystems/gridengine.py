# Copyright (C) 2015-2016 Regents of the University of California
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
import logging
import os
import subprocess
import time
import math

# Python 3 compatibility imports
from six import iteritems

from toil.batchSystems import MemoryString
from toil.batchSystems.drmaaBatchSystem import AbstractDRMAABatchSystem

logger = logging.getLogger(__name__)

class GridEngineBatchSystem(AbstractDRMAABatchSystem):
    """
    The interface for SGE aka Sun GridEngine.
    """

    @classmethod
    def getWaitDuration(cls):
        return 0.0

    @classmethod
    def obtainSystemConstants(cls):
        lines = filter(None, map(str.strip, subprocess.check_output(["qhost"]).split('\n')))
        line = lines[0]
        items = line.strip().split()
        num_columns = len(items)
        cpu_index = None
        mem_index = None
        for i in range(num_columns):
            if items[i] == 'NCPU':
                cpu_index = i
            elif items[i] == 'MEMTOT':
                mem_index = i
        if cpu_index is None or mem_index is None:
            RuntimeError('qhost command does not return NCPU or MEMTOT columns')
        maxCPU = 0
        maxMEM = MemoryString("0")
        for line in lines[2:]:
            items = line.strip().split()
            if len(items) < num_columns:
                RuntimeError('qhost output has a varying number of columns')
            if items[cpu_index] != '-' and items[cpu_index] > maxCPU:
                maxCPU = items[cpu_index]
            if items[mem_index] != '-' and MemoryString(items[mem_index]) > maxMEM:
                maxMEM = MemoryString(items[mem_index])
        if maxCPU is 0 or maxMEM is 0:
            RuntimeError('qhost returned null NCPU or MEMTOT info')
        return maxCPU, maxMEM

    def timeElapsed(self, jobIDs):
        times = {}
        currentjobs = {self.jobs[str(jid)]: jid for jid in jobIDs}
        process = subprocess.Popen(["qstat"], stdout=subprocess.PIPE)
        stdout, stderr = process.communicate()

        for currline in stdout.split('\n'):
            items = currline.strip().split()
            if items:
                if items[0] in currentjobs and items[4] == 'r':
                    jobstart = " ".join(items[5:7])
                    jobstart = time.mktime(time.strptime(jobstart, "%m/%d/%Y %H:%M:%S"))
                    times[currentjobs[items[0]]] = time.time() - jobstart
        return times

    def nativeSpec(self, jobNode):
        nativeArgs = {'-V': '', '-b': 'y', '-terse': ''}
        userArgs = os.getenv('TOIL_GRIDENGINE_ARGS')
        memstr = str(jobNode.memory / 1024) + 'K'
        if '%MEMORY%' in  userArgs:
            userArgs = userArgs.replace('%MEMORY%', memstr)
        else:
            nativeArgs['-hard'] = ''
            nativeArgs['-l'] = 'vf=' + memstr + ',h_vmem=' + memstr
        if '%DISK%' in userArgs:
            userArgs = userArgs.replace('%DISK%', str(jobNode.disk))
        userArgs = userArgs.split()
        i = 0
        while i < len(userArgs):
            if userArgs[i] == '-pe':
                raise ValueError("Unexpected pe specification in" +
                                 " TOIL_GRIDENGINE_ARGS: %s" % userArgs[i])
            if not userArgs[i].startswith('-'):
                raise ValueError("Unexpected bare argument in" +
                                 " TOIL_GRIDENGINE_ARGS: %s" % userArgs[i])
            if i < len(userArgs) - 1 and not userArgs[i+1].startswith('-'):
                if userArgs[i] == '-l' and '-l' in nativeArgs:
                    nativeArgs['-l'] += ',' + userArgs[i+1]
                else:
                    nativeArgs[userArgs[i]] = userArgs[i+1]
                i += 2
            else:
                nativeArgs[userArgs[i]] = ''
                i += 1
        if self.environment:
            nativeArgs['-v'] = ','.join(k + '=' + v for k, v in iteritems(self.environment))
        if jobNode.cores is not None and math.ceil(jobNode.cores) > 1:
                peConfig = os.getenv('TOIL_GRIDENGINE_PE') or 'shm'
                nativeArgs['-pe'] = peConfig + ' ' + str(int(math.ceil(jobNode.cores)))
        return ' '.join([k if v == '' else k + ' ' + v for k, v in iteritems(nativeArgs)])
