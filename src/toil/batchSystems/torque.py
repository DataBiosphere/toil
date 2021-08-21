# Copyright (C) 2015-2021 Regents of the University of California
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
import logging
import math
import os
import shlex
import tempfile
import time
from pipes import quote
from queue import Empty
from typing import Optional, List, Dict

from toil.batchSystems.abstractGridEngineBatchSystem import (AbstractGridEngineBatchSystem,
                                                             UpdatedBatchJobInfo)
from toil.lib.conversions import hms_duration_to_seconds
from toil.lib.misc import CalledProcessErrorStderr, call_command

logger = logging.getLogger(__name__)


class TorqueBatchSystem(AbstractGridEngineBatchSystem):

    # class-specific Worker
    class Worker(AbstractGridEngineBatchSystem.Worker):

        def __init__(self, newJobsQueue, updatedJobsQueue, killQueue, killedJobsQueue, boss):
            super().__init__(newJobsQueue, updatedJobsQueue, killQueue, killedJobsQueue, boss)
            self._version = self._pbsVersion()

        def _pbsVersion(self):
            """ Determines PBS/Torque version via pbsnodes
            """
            try:
                out = call_command(["pbsnodes", "--version"])
                if "PBSPro" in out:
                     logger.debug("PBS Pro proprietary Torque version detected")
                     self._version = "pro"
                else:
                     logger.debug("Torque OSS version detected")
                     self._version = "oss"
            except CalledProcessErrorStderr as e:
               if e.returncode != 0:
                    logger.error("Could not determine PBS/Torque version")

            return self._version

        """
        Torque-specific AbstractGridEngineWorker methods
        """
        def getRunningJobIDs(self):
            times = {}
            with self.runningJobsLock:
                currentjobs = dict((str(self.batchJobIDs[x][0].strip()), x) for x in self.runningJobs)
            logger.debug("getRunningJobIDs current jobs are: " + str(currentjobs))
            # Skip running qstat if we don't have any current jobs
            if not currentjobs:
                return times
            # Only query for job IDs to avoid clogging the batch system on heavily loaded clusters
            # PBS plain qstat will return every running job on the system.
            jobids = sorted(list(currentjobs.keys()))
            if self._version == "pro":
                stdout = call_command(['qstat', '-x'] + jobids)
            elif self._version == "oss":
                stdout = call_command(['qstat'] + jobids)

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
                            walltime = hms_duration_to_seconds(walltime)
                        times[currentjobs[jobid]] = walltime

            logger.debug("Job times from qstat are: " + str(times))
            return times

        def getUpdatedBatchJob(self, maxWait):
            try:
                logger.debug("getUpdatedBatchJob: Job updates")
                item = self.updatedJobsQueue.get(timeout=maxWait)
                self.updatedJobsQueue.task_done()
                jobID, retcode = (self.jobIDs[item.jobID], item.exitStatus)
                self.currentjobs -= {self.jobIDs[item.jobID]}
            except Empty:
                logger.debug("getUpdatedBatchJob: Job queue is empty")
            else:
                return UpdatedBatchJobInfo(jobID=jobID, exitStatus=retcode, wallTime=None, exitReason=None)

        def killJob(self, jobID):
            call_command(['qdel', self.getBatchSystemID(jobID)])

        def prepareSubmission(self,
                              cpu: int,
                              memory: int,
                              jobID: int,
                              command: str,
                              jobName: str,
                              job_environment: Optional[Dict[str, str]] = None) -> List[str]:
            return self.prepareQsub(cpu, memory, jobID, job_environment) + [self.generateTorqueWrapper(command, jobID)]

        def submitJob(self, subLine):
            return call_command(subLine)

        def getJobExitCode(self, torqueJobID):
            if self._version == "pro":
                args = ["qstat", "-x", "-f", str(torqueJobID).split('.')[0]]
            elif self._version == "oss":
                args = ["qstat", "-f", str(torqueJobID).split('.')[0]]

            stdout = call_command(args)
            for line in stdout.split('\n'):
                line = line.strip()
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
        def prepareQsub(self,
                        cpu: int,
                        mem: int,
                        jobID: int,
                        job_environment: Optional[Dict[str, str]]) -> List[str]:

            # TODO: passing $PWD on command line not working for -d, resorting to
            # $PBS_O_WORKDIR but maybe should fix this here instead of in script?

            qsubline = ['qsub', '-S', '/bin/sh', '-V', '-N', 'toil_job_{}'.format(jobID)]

            environment = self.boss.environment.copy()
            if job_environment:
                environment.update(job_environment)

            if environment:
                qsubline.append('-v')
                qsubline.append(','.join(k + '=' + quote(os.environ[k] if v is None else v)
                                         for k, v in self.boss.environment.items()))

            reqline = list()
            if mem is not None:
                reqline.append('mem={}K'.format(mem // 1024))

            if cpu is not None and math.ceil(cpu) > 1:
                reqline.append('nodes=1:ppn=' + str(int(math.ceil(cpu))))

            # Other resource requirements can be passed through the environment (see man qsub)
            reqlineEnv = os.getenv('TOIL_TORQUE_REQS')
            if reqlineEnv is not None:
                logger.debug("Additional Torque resource requirements appended to qsub from "
                             "TOIL_TORQUE_REQS env. variable: {}".format(reqlineEnv))
                if ("mem=" in reqlineEnv) or ("nodes=" in reqlineEnv) or ("ppn=" in reqlineEnv):
                    raise ValueError("Incompatible resource arguments ('mem=', 'nodes=', 'ppn='): {}".format(reqlineEnv))

                reqline.append(reqlineEnv)

            if reqline:
                qsubline += ['-l', ','.join(reqline)]

            # All other qsub parameters can be passed through the environment (see man qsub).
            # No attempt is made to parse them out here and check that they do not conflict
            # with those that we already constructed above
            arglineEnv = os.getenv('TOIL_TORQUE_ARGS')
            if arglineEnv is not None:
                logger.debug("Native Torque options appended to qsub from TOIL_TORQUE_ARGS env. variable: {}"
                             .format(arglineEnv))
                if ("mem=" in arglineEnv) or ("nodes=" in arglineEnv) or ("ppn=" in arglineEnv):
                    raise ValueError("Incompatible resource arguments ('mem=', 'nodes=', 'ppn='): {}".format(arglineEnv))
                qsubline += shlex.split(arglineEnv)

            return qsubline

        def generateTorqueWrapper(self, command, jobID):
            """
            A very simple script generator that just wraps the command given; for
            now this goes to default tempdir
            """
            stdoutfile: str = self.boss.formatStdOutErrPath(jobID, r'${PBS_JOBID}', 'out')
            stderrfile: str = self.boss.formatStdOutErrPath(jobID, r'${PBS_JOBID}', 'err')

            _, tmp_file = tempfile.mkstemp(suffix='.sh', prefix='torque_wrapper')
            with open(tmp_file, 'w') as f:
                f.write("#!/bin/sh\n")
                f.write("#PBS -o {}\n".format(stdoutfile))
                f.write("#PBS -e {}\n".format(stderrfile))
                f.write("cd $PBS_O_WORKDIR\n\n")
                f.write(command + "\n")

            return tmp_file
