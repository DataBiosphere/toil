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

        def coalesce_job_exit_codes(self, batch_job_id_list: list) -> list:
            logger.debug(f"Getting exit codes for slurm jobs: {batch_job_id_list}")
            exit_codes = []
            status_dict = self._get_job_details(batch_job_id_list)
            for job_id, status in status_dict.items():
                exit_codes.append(self._get_job_return_code(status))
            return exit_codes

        def getJobExitCode(self, slurmJobID):
            logger.debug(f"Getting exit code for slurm job: {slurmJobID}")
            status_dict = self._get_job_details([slurmJobID])
            status = status_dict[slurmJobID]
            return self._get_job_return_code(status)

        def _get_job_details(self, batch_job_id_list: list) -> dict:
            """
            Helper function for `getJobExitCode` and `coalesce_job_exit_codes`.
            Fetch job details from Slurm's accounting system or job control system.
            Return a dict of job statuses. Key is the job-id, value is a tuple containing the
            job's state and exit code.
            """
            try:
                status_dict = self._getJobDetailsFromSacct(batch_job_id_list)
            except CalledProcessErrorStderr:
                status_dict = self._getJobDetailsFromScontrol(batch_job_id_list)
            return status_dict

        def _get_job_return_code(self, status: tuple) -> list:
            """
            Helper function for `getJobExitCode` and `coalesce_job_exit_codes`.
            The tuple `status` contains the job's state and it's return code.
            Return the job's return code if it's completed, otherwise return None.
            """
            state, rc = status
            # If job is in a running state, set return code to None to indicate we don't have
            # an update.
            if state in ('PENDING', 'RUNNING', 'CONFIGURING', 'COMPLETING', 'RESIZING', 'SUSPENDED'):
                rc = None
            return rc

        def _getJobDetailsFromSacct(self, batch_job_id_list: list) -> dict:
            # SLURM job exit codes are obtained by running sacct.
            job_ids = ",".join(str(id) for id in batch_job_id_list)
            args = ['sacct',
                    '-n',  # no header
                    '-j', job_ids,  # job
                    '--format', 'JobIDRaw,State,ExitCode',  # specify output columns
                    '-P',  # separate columns with pipes
                    '-S', '1970-01-01']  # override start time limit
            stdout = call_command(args)

            # Collect the job statuses in a dict; key is the job-id, value is a tuple containing
            # job state and exit status. Initialize dict before processing output of `sacct`.
            job_status = {}
            for id in batch_job_id_list:
                job_status[id] = (None, None)

            for line in stdout.splitlines():
                logger.debug(f"{args[0]} output {line}")
                values = line.strip().split('|')
                if len(values) < 3:
                    continue
                job_id, state, exitcode = values
                logger.debug(f"{args[0]} state of job {job_id} is {state}")
                # JobIDRaw is in the form JobID[.JobStep]
                job_id = int(job_id.split(".")[0])
                status, signal = [int(n) for n in exitcode.split(':')]
                if signal > 0:
                    # A non-zero signal may indicate e.g. an out-of-memory killed job
                    status = 128 + signal
                logger.debug(f"{args[0]} exit code of job {job_id} is {exitcode}, "
                             f"return status {status}")
                job_status[job_id] = state, status
            logger.debug(f"{args[0]} returning job statuses: {job_status}")
            return job_status

        def _getJobDetailsFromScontrol(self, batch_job_id_list: list) -> dict:
            # `scontrol` can only return information about a single job,
            # or all the jobs it knows about.
            args = ['scontrol',
                    'show',
                    'job']
            if len(batch_job_id_list) == 1:
                args.append(str(batch_job_id_list[0]))

            stdout = call_command(args)

            # Job records are separated by a blank line.
            if isinstance(stdout, str):
                job_records = stdout.strip().split('\n\n')
            elif isinstance(stdout, bytes):
                job_records = stdout.decode('utf-8').strip().split('\n\n')

            # Collect the job statuses in a dict; key is the job-id, value is a tuple containing
            # job state and exit status. Initialize dict before processing output of `scontrol`.
            job_status = {}
            for id in batch_job_id_list:
                job_status[id] = (None, None)

            # `scontrol` will report "No jobs in the system", if there are no jobs in the system,
            # and if no job-id was passed as argument to `scontrol`.
            if len(job_records) > 0 and job_records[0] == "No jobs in the system":
                return job_status

            for record in job_records:
                job = dict()
                for line in record.splitlines():
                    for item in line.split():
                        logger.debug(f"{args[0]} output {item}")
                        # Output is in the form of many key=value pairs, multiple pairs on each line
                        # and multiple lines in the output. Each pair is pulled out of each line and
                        # added to a dictionary.
                        # Note: In some cases, the value itself may contain white-space. So, if we find
                        # a key without a value, we consider that key part of the previous value.
                        bits = item.split('=', 1)
                        if len(bits) == 1:
                            job[key] += ' ' + bits[0]
                        else:
                            key = bits[0]
                            job[key] = bits[1]
                    # The first line of the record contains the JobId. Stop processing the remainder
                    # of this record, if we're not interested in this job.
                    job_id = int(job['JobId'])
                    if job_id not in batch_job_id_list:
                        logger.debug(f"{args[0]} job {job_id} is not in the list")
                        break
                if job_id not in batch_job_id_list:
                    continue
                state = job['JobState']
                try:
                    exitcode = job['ExitCode']
                    if exitcode is not None:
                        status, signal = [int(n) for n in exitcode.split(':')]
                        if signal > 0:
                            # A non-zero signal may indicate e.g. an out-of-memory killed job
                            status = 128 + signal
                        logger.debug(f"{args[0]} exit code of job {job_id} is {exitcode}, "
                                     f"return status {status}")
                        rc = status
                    else:
                        rc = None
                except KeyError:
                    rc = None
                job_status[job_id] = (state, rc)
            logger.debug(f"{args[0]} returning job statuses: {job_status}")
            return job_status

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
