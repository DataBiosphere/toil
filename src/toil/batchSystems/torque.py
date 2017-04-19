# Copyright (C) 2015 UCSC Computational Genomics Lab
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
from pipes import quote
import subprocess
import time
import math
import sys
import xml.etree.ElementTree as ET
import tempfile

from toil.batchSystems import MemoryString
from toil.batchSystems.abstractGridEngineBatchSystem import AbstractGridEngineBatchSystem

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



class TorqueBatchSystem(AbstractGridEngineBatchSystem):
    


    # class-specific Worker
    class Worker(AbstractGridEngineBatchSystem.Worker):

        """
        Torque-specific AbstractGridEngineWorker methods
        """
        def getRunningJobIDs(self):
            times = {}
            currentjobs = dict((str(self.batchJobIDs[x][0]), x) for x in self.runningJobs)
            process = subprocess.Popen(["qstat"], stdout=subprocess.PIPE)
            stdout, stderr = process.communicate()

            # qstat supports XML output which is more comprehensive, but PBSPro does not support it 
            for currline in stdout.split('\n'):
                items = currline.strip().split()
                if items:
                    jobid = items[0].strip().split('.')[0]
                    if jobid in currentjobs and items[4] == 'R':
                        walltime = items[3]
                        # normal qstat has a quirk with job time where it reports '0'
                        # when initially running; this catches this case
                        if walltime == '0':
                            walltime = time.mktime(time.strptime(walltime, "%S"))
                        else:
                            walltime = time.mktime(time.strptime(walltime, "%H:%M:%S"))
                        times[currentjobs[jobid]] = walltime

            return times

        def killJob(self, jobID):
            subprocess.check_call(['qdel', self.getBatchSystemID(jobID)])

        def prepareSubmission(self, cpu, memory, jobID, command):
            return self.prepareQsub(cpu, memory, jobID) + [self.generateTorqueWrapper(command)]

        def submitJob(self, subLine):
            process = subprocess.Popen(subLine, stdout=subprocess.PIPE)
            so, se = process.communicate()
            # TODO: the full URI here may be needed on complex setups, stripping
            # down to integer job ID only may be bad long-term
            result = int(so.strip().split('.')[0])
            return result

        def getJobExitCode(self, torqueJobID):
            args = ["qstat", "-f", str(torqueJobID)]

            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in process.stdout:
                line = line.strip()
                if line.startswith("failed") and int(line.split()[1]) == 1:
                    return 1
                if line.startswith("exit_status"):
                    status = line.split(' = ')[1]
                    logger.debug('Exit Status: ' + status)
                    return int(status)

        """
        Implementation-specific helper methods
        """
        def prepareQsub(self, cpu, mem, jobID):

            # TODO: passing $PWD on command line not working for -d, resorting to
            # $PBS_O_WORKDIR but maybe should fix this here instead of in script?

            # TODO: we previosuly trashed the stderr/stdout, as in the commented
            # code, but these may be retained by others, particularly for debugging.
            # Maybe an option or attribute w/ a location for storing the logs?

            # qsubline = ['qsub', '-V', '-j', 'oe', '-o', '/dev/null',
            #             '-e', '/dev/null', '-N', 'toil_job_{}'.format(jobID)]

            qsubline = ['qsub', '-V', '-N', 'toil_job_{}'.format(jobID)]

            if self.boss.environment:
                qsubline.append('-v')
                qsubline.append(','.join(k + '=' + quote(os.environ[k] if v is None else v)
                                         for k, v in self.boss.environment.iteritems()))

            reqline = list()
            if mem is not None:
                memStr = str(mem / 1024) + 'K'
                reqline += ['-l mem=' + memStr]

            if cpu is not None and math.ceil(cpu) > 1:
                qsubline.extend(['-l ncpus=' + str(int(math.ceil(cpu)))])

            return qsubline

        def generateTorqueWrapper(self, command):
            """
            A very simple script generator that just wraps the command given; for
            now this goes to default tempdir
            """
            _, tmpFile = tempfile.mkstemp(suffix='.sh', prefix='torque_wrapper')
            fh = open(tmpFile , 'w')
            fh.write("$!/bin/bash\n\n")
            fh.write("cd $PBS_O_WORKDIR\n\n")
            fh.write(command + "\n")
            fh.close
            return tmpFile

    @classmethod
    def obtainSystemConstants(cls):

        # See: https://github.com/BD2KGenomics/toil/pull/1617#issuecomment-293525747
        logger.debug("PBS/Torque does not need obtainSystemConstants to assess global \
                    cluster resources.")


        #return maxCPU, maxMEM
        return None, None
