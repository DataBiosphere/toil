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
import shlex
import xml.etree.ElementTree as ET
import tempfile

from toil.batchSystems import MemoryString
from toil.batchSystems.abstractGridEngineBatchSystem import AbstractGridEngineBatchSystem

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



        

class TorqueBatchSystem(AbstractGridEngineBatchSystem):


    # class-specific Worker
    class Worker(AbstractGridEngineBatchSystem.Worker):

        def __init__(self, newJobsQueue, updatedJobsQueue, killQueue, killedJobsQueue, boss):
            super(self.__class__, self).__init__(newJobsQueue, updatedJobsQueue, killQueue, killedJobsQueue, boss)
            self._version = self._pbsVersion()

        def _pbsVersion(self):
            """ Determines PBS/Torque version via pbsnodes
            """
            try:
                out = subprocess.check_output(["pbsnodes", "--version"])

                if "PBSPro" in out:
                     logger.debug("PBS Pro proprietary Torque version detected")
                     self._version = "pro"
                else:
                     logger.debug("Torque OSS version detected")
                     self._version = "oss"
            except subprocess.CalledProcessError as e:
               if e.returncode != 0:
                    logger.error("Could not determine PBS/Torque version")

            return self._version
        

        """
        Torque-specific AbstractGridEngineWorker methods
        """
        def getRunningJobIDs(self):
            times = {}
            
            currentjobs = dict((str(self.batchJobIDs[x][0].strip()), x) for x in self.runningJobs)
            logger.debug("getRunningJobIDs current jobs are: " + str(currentjobs))
            # Limit qstat to current username to avoid clogging the batch system on heavily loaded clusters
            #job_user = os.environ.get('USER')
            #process = subprocess.Popen(['qstat', '-u', job_user], stdout=subprocess.PIPE)
            # -x shows exit status in PBSPro, not XML output like OSS PBS
            if self._version == "pro":
                process = subprocess.Popen(['qstat', '-x'], stdout=subprocess.PIPE)
            elif self._version == "oss":
                process = subprocess.Popen(['qstat'], stdout=subprocess.PIPE)


            stdout, stderr = process.communicate()

            # qstat supports XML output which is more comprehensive, but PBSPro does not support it 
            # so instead we stick with plain commandline qstat tabular outputs
            for currline in stdout.split('\n'):
                items = currline.strip().split()
                if items:
                    jobid = items[0].strip()
                    if jobid in currentjobs:
                        logger.debug("getRunningJobIDs job status for is: " + items[4])
                    if jobid in currentjobs and items[4] == 'R':
                        walltime = items[3]
                        logger.debug("getRunningJobIDs qstat reported walltime is: " + walltime)
                        # normal qstat has a quirk with job time where it reports '0'
                        # when initially running; this catches this case
                        if walltime == '0':
                            walltime = time.mktime(time.strptime(walltime, "%S"))
                        else:
                            walltime = time.mktime(time.strptime(walltime, "%H:%M:%S"))
                        times[currentjobs[jobid]] = walltime

            logger.debug("Job times from qstat are: " + str(times))
            return times

        def getUpdatedBatchJob(self, maxWait):
            try:
                logger.debug("getUpdatedBatchJob: Job updates")
                pbsJobID, retcode = self.updatedJobsQueue.get(timeout=maxWait)
                self.updatedJobsQueue.task_done()
                jobID, retcode = (self.jobIDs[pbsJobID], retcode)
                self.currentjobs -= {self.jobIDs[pbsJobID]}
            except Empty:
                logger.debug("getUpdatedBatchJob: Job queue is empty")
                pass
            else:
                return jobID, retcode, None

        def killJob(self, jobID):
            subprocess.check_call(['qdel', self.getBatchSystemID(jobID)])

        def prepareSubmission(self, cpu, memory, jobID, command):
            return self.prepareQsub(cpu, memory, jobID) + [self.generateTorqueWrapper(command)]

        def submitJob(self, subLine):
            process = subprocess.Popen(subLine, stdout=subprocess.PIPE)
            so, se = process.communicate()
            return so

        def getJobExitCode(self, torqueJobID):
            if self._version == "pro":
                args = ["qstat", "-x", "-f", str(torqueJobID).split('.')[0]]
            elif self._version == "oss":
                args = ["qstat", "-f", str(torqueJobID).split('.')[0]]

            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in process.stdout:
                line = line.strip()
                #logger.debug("getJobExitCode exit status: " + line)
                # Case differences due to PBSPro vs OSS Torque qstat outputs
                if line.startswith("failed") or line.startswith("FAILED") and int(line.split()[1]) == 1:
                    return 1
                if line.startswith("exit_status") or line.startswith("Exit_status"):
                    status = line.split(' = ')[1]
                    logger.debug('Exit Status: ' + status)
                    return int(status)
                if 'unknown job id' in line.lower():
                    # some clusters configure Torque to forget everything about just
                    # finished jobs instantly, apparently for performance reasons
                    logger.debug('Batch system no longer remembers about job {}'.format(torqueJobID))
                    # return assumed success; status files should reveal failure
                    return 0
            return None

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

            qsubline = ['qsub', '-S', '/bin/sh', '-V', '-N', 'toil_job_{}'.format(jobID)]

            if self.boss.environment:
                qsubline.append('-v')
                qsubline.append(','.join(k + '=' + quote(os.environ[k] if v is None else v)
                                         for k, v in self.boss.environment.iteritems()))

            reqline = list()
            if mem is not None:
                memStr = str(mem / 1024) + 'K'
                reqline.append('mem=' + memStr)

            if cpu is not None and math.ceil(cpu) > 1:
                reqline.append('ncpus=' + str(int(math.ceil(cpu))))

            # Other resource requirements can be passed through the environment (see man qsub)
            reqlineEnv = os.getenv('TOIL_TORQUE_REQS')
            if reqlineEnv is not None:
                logger.debug("Additional Torque resource requirements appended to qsub from "\
                        "TOIL_TORQUE_REQS env. variable: {}".format(reqlineEnv))
                if ("mem=" in reqlineEnv) or ("nodes=" in reqlineEnv) or ("ppn=" in reqlineEnv):
                    raise ValueError("Incompatible resource arguments ('mem=', 'nodes=', 'ppn='): {}".format(reqlineEnv))

                reqline.append(reqlineEnv)
            
            if reqline:
                qsubline += ['-l',','.join(reqline)]
            
            # All other qsub parameters can be passed through the environment (see man qsub).
            # No attempt is made to parse them out here and check that they do not conflict
            # with those that we already constructed above
            arglineEnv = os.getenv('TOIL_TORQUE_ARGS')
            if arglineEnv is not None:
                logger.debug("Native Torque options appended to qsub from TOIL_TORQUE_ARGS env. variable: {}".\
                        format(arglineEnv))
                if ("mem=" in arglineEnv) or ("nodes=" in arglineEnv) or ("ppn=" in arglineEnv):
                    raise ValueError("Incompatible resource arguments ('mem=', 'nodes=', 'ppn='): {}".format(arglineEnv))
                qsubline += shlex.split(arglineEnv)

            return qsubline

        def generateTorqueWrapper(self, command):
            """
            A very simple script generator that just wraps the command given; for
            now this goes to default tempdir
            """
            _, tmpFile = tempfile.mkstemp(suffix='.sh', prefix='torque_wrapper')
            fh = open(tmpFile , 'w')
            fh.write("#!/bin/sh\n")
            fh.write("cd $PBS_O_WORKDIR\n\n")
            fh.write(command + "\n")

            fh.close
            
            return tmpFile


    @classmethod
    def obtainSystemConstants(cls):

        # See: https://github.com/BD2KGenomics/toil/pull/1617#issuecomment-293525747
        logger.debug("PBS/Torque does not need obtainSystemConstants to assess global cluster resources.")


        #return maxCPU, maxMEM
        return None, None
