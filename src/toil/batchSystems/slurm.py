# Copyright (c) 2016 Duke Center for Genomic and Computational Biology
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
from pipes import quote
from typing import List, Dict, Optional

from toil.batchSystems.abstractGridEngineBatchSystem import AbstractGridEngineBatchSystem
from toil.lib.misc import CalledProcessErrorStderr, call_command

logger = logging.getLogger(__name__)


class SlurmBatchSystem(AbstractGridEngineBatchSystem):

    class Worker(AbstractGridEngineBatchSystem.Worker):

        def getRunningJobIDs(self):
            # Should return a dictionary of Job IDs and number of seconds
            times = {}
            with self.runningJobsLock:
                currentjobs = dict((str(self.batchJobIDs[x][0]), x) for x in self.runningJobs)
            # currentjobs is a dictionary that maps a slurm job id (string) to our own internal job id
            # squeue arguments:
            # -h for no header
            # --format to get jobid i, state %t and time days-hours:minutes:seconds

            lines = call_command(['squeue', '-h', '--format', '%i %t %M']).split('\n')
            for line in lines:
                values = line.split()
                if len(values) < 3:
                    continue
                slurm_jobid, state, elapsed_time = values
                if slurm_jobid in currentjobs and state == 'R':
                    seconds_running = self.parse_elapsed(elapsed_time)
                    times[currentjobs[slurm_jobid]] = seconds_running

            return times

        def killJob(self, jobID):
            call_command(['scancel', self.getBatchSystemID(jobID)])

        def prepareSubmission(self,
                              cpu: int,
                              memory: int,
                              jobID: int,
                              command: str,
                              jobName: str,
                              job_environment: Optional[Dict[str, str]] = None) -> List[str]:
            return self.prepareSbatch(cpu, memory, jobID, jobName, job_environment) + ['--wrap={}'.format(command)]

        def submitJob(self, subLine):
            try:
                output = call_command(subLine)
                # sbatch prints a line like 'Submitted batch job 2954103'
                result = int(output.strip().split()[-1])
                logger.debug("sbatch submitted job %d", result)
                return result
            except OSError as e:
                logger.error("sbatch command failed")
                raise e

        def getJobExitCode(self, slurmJobID):
            logger.debug(f"Getting exit code for slurm job: {slurmJobID}")

            try:
                state, rc = self._getJobDetailsFromSacct(slurmJobID)
            except CalledProcessErrorStderr:
                # no accounting system or some other error
                state, rc = self._getJobDetailsFromScontrol(slurmJobID)

            logger.debug("s job state is %s", state)
            # If Job is in a running state, return None to indicate we don't have an update
            if state in ('PENDING', 'RUNNING', 'CONFIGURING', 'COMPLETING', 'RESIZING', 'SUSPENDED'):
                return None

            return rc

        def _getJobDetailsFromSacct(self, slurmJobID):
            # SLURM job exit codes are obtained by running sacct.
            args = ['sacct',
                    '-n', # no header
                    '-j', str(slurmJobID), # job
                    '--format', 'State,ExitCode', # specify output columns
                    '-P', # separate columns with pipes
                    '-S', '1970-01-01'] # override start time limit

            stdout = call_command(args)
            for line in stdout.split('\n'):
                logger.debug("%s output %s", args[0], line)
                values = line.strip().split('|')
                if len(values) < 2:
                    continue
                state, exitcode = values
                logger.debug("sacct job state is %s", state)
                # If Job is in a running state, return None to indicate we don't have an update
                status, signal = [int(n) for n in exitcode.split(':')]
                if signal > 0:
                    # A non-zero signal may indicate e.g. an out-of-memory killed job
                    status = 128 + signal
                logger.debug("sacct exit code is %s, returning status %d", exitcode, status)
                return state, status
            logger.debug("Did not find exit code for job in sacct output")
            return None, None

        def _getJobDetailsFromScontrol(self, slurmJobID):
            args = ['scontrol',
                    'show',
                    'job',
                    str(slurmJobID)]

            stdout = call_command(args)
            if isinstance(stdout, str):
                values = stdout.strip().split()
            elif isinstance(stdout, bytes):
                values = stdout.decode('utf-8').strip().split()

            # If job information is not available an error is issued:
            # slurm_load_jobs error: Invalid job id specified
            # There is no job information, so exit.
            if len(values) > 0 and values[0] == 'slurm_load_jobs':
                return (None, None)

            job = dict()
            for item in values:
                logger.debug(f"{args[0]} output {item}")

                # Output is in the form of many key=value pairs, multiple pairs on each line
                # and multiple lines in the output. Each pair is pulled out of each line and
                # added to a dictionary
                for v in values:
                    bits = v.split('=')
                    job[bits[0]] = bits[1]

            state = job['JobState']
            try:
                exitcode = job['ExitCode']
                if exitcode is not None:
                    status, signal = [int(n) for n in exitcode.split(':')]
                    if signal > 0:
                        # A non-zero signal may indicate e.g. an out-of-memory killed job
                        status = 128 + signal
                    logger.debug("scontrol exit code is %s, returning status %d", exitcode, status)
                    rc = status
                else:
                    rc = None
            except KeyError:
                rc = None

            return state, rc

        """
        Implementation-specific helper methods
        """

        def prepareSbatch(self,
                          cpu: int,
                          mem: int,
                          jobID: int,
                          jobName: str,
                          job_environment: Optional[Dict[str, str]]) -> List[str]:

            #  Returns the sbatch command line before the script to run
            sbatch_line = ['sbatch', '-J', 'toil_job_{}_{}'.format(jobID, jobName)]

            environment = {}
            environment.update(self.boss.environment)
            if job_environment:
                environment.update(job_environment)

            if environment:
                argList = []

                for k, v in environment.items():
                    quoted_value = quote(os.environ[k] if v is None else v)
                    argList.append('{}={}'.format(k, quoted_value))

                sbatch_line.append('--export=' + ','.join(argList))

            if mem is not None and self.boss.config.allocate_mem:
                # memory passed in is in bytes, but slurm expects megabytes
                sbatch_line.append(f'--mem={math.ceil(mem / 2 ** 20)}')
            if cpu is not None:
                sbatch_line.append(f'--cpus-per-task={math.ceil(cpu)}')

            stdoutfile: str = self.boss.formatStdOutErrPath(jobID, '%j', 'out')
            stderrfile: str = self.boss.formatStdOutErrPath(jobID, '%j', 'err')
            sbatch_line.extend(['-o', stdoutfile, '-e', stderrfile])

            # "Native extensions" for SLURM (see DRMAA or SAGA)
            nativeConfig = os.getenv('TOIL_SLURM_ARGS')
            if nativeConfig is not None:
                logger.debug("Native SLURM options appended to sbatch from TOIL_SLURM_ARGS env. variable: {}".format(nativeConfig))
                if ("--mem" in nativeConfig) or ("--cpus-per-task" in nativeConfig):
                    raise ValueError("Some resource arguments are incompatible: {}".format(nativeConfig))

                sbatch_line.extend(nativeConfig.split())

            return sbatch_line

        def parse_elapsed(self, elapsed):
            # slurm returns elapsed time in days-hours:minutes:seconds format
            # Sometimes it will only return minutes:seconds, so days may be omitted
            # For ease of calculating, we'll make sure all the delimeters are ':'
            # Then reverse the list so that we're always counting up from seconds -> minutes -> hours -> days
            total_seconds = 0
            try:
                elapsed = elapsed.replace('-', ':').split(':')
                elapsed.reverse()
                seconds_per_unit = [1, 60, 3600, 86400]
                for index, multiplier in enumerate(seconds_per_unit):
                    if index < len(elapsed):
                        total_seconds += multiplier * int(elapsed[index])
            except ValueError:
                pass  # slurm may return INVALID instead of a time
            return total_seconds

    """
    The interface for SLURM
    """

    @classmethod
    def getWaitDuration(cls):
        # Extract the slurm batchsystem config for the appropriate value
        wait_duration_seconds = 1
        lines = call_command(['scontrol', 'show', 'config']).split('\n')
        time_value_list = []
        for line in lines:
            values = line.split()
            if len(values) > 0 and (values[0] == "SchedulerTimeSlice" or values[0] == "AcctGatherNodeFreq"):
                time_name = values[values.index('=')+1:][1]
                time_value = int(values[values.index('=')+1:][0])
                if time_name == 'min':
                    time_value *= 60
                # Add a 20% ceiling on the wait duration relative to the scheduler update duration
                time_value_list.append(math.ceil(time_value*1.2))
        return max(time_value_list)
