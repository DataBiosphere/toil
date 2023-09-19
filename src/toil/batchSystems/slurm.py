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
from argparse import ArgumentParser, _ArgumentGroup
from shlex import quote
from typing import Callable, Dict, List, Optional, TypeVar, Union

from toil.batchSystems.abstractGridEngineBatchSystem import \
    AbstractGridEngineBatchSystem
from toil.batchSystems.options import OptionSetter
from toil.lib.misc import CalledProcessErrorStderr, call_command
from toil.job import Requirer

logger = logging.getLogger(__name__)


class SlurmBatchSystem(AbstractGridEngineBatchSystem):

    class Worker(AbstractGridEngineBatchSystem.Worker):

        def getRunningJobIDs(self):
            # Should return a dictionary of Job IDs and number of seconds
            times = {}
            with self.runningJobsLock:
                currentjobs = {str(self.batchJobIDs[x][0]): x for x in self.runningJobs}
            # currentjobs is a dictionary that maps a slurm job id (string) to our own internal job id
            # squeue arguments:
            # -h for no header
            # --format to get jobid i, state %t and time days-hours:minutes:seconds

            lines = call_command(['squeue', '-h', '--format', '%i %t %M'], quiet=True).split('\n')
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
                              job_environment: Optional[Dict[str, str]] = None,
                              gpus: Optional[int] = None) -> List[str]:
            return self.prepareSbatch(cpu, memory, jobID, jobName, job_environment, gpus) + [f'--wrap={command}']

        def submitJob(self, subLine):
            try:
                # Slurm is not quite clever enough to follow the XDG spec on
                # its own. If the submission command sees e.g. XDG_RUNTIME_DIR
                # in our environment, it will send it along (especially with
                # --export=ALL), even though it makes a promise to the job that
                # Slurm isn't going to keep. It also has a tendency to create
                # /run/user/<uid> *at the start* of a job, but *not* keep it
                # around for the duration of the job.
                #
                # So we hide the whole XDG universe from Slurm before we make
                # the submission.
                # Might as well hide DBUS also.
                # This doesn't get us a trustworthy XDG session in Slurm, but
                # it does let us see the one Slurm tries to give us.
                no_session_environment = os.environ.copy()
                session_names = [n for n in no_session_environment.keys() if n.startswith('XDG_') or n.startswith('DBUS_')]
                for name in session_names:
                    del no_session_environment[name]

                output = call_command(subLine, env=no_session_environment)
                # sbatch prints a line like 'Submitted batch job 2954103'
                result = int(output.strip().split()[-1])
                logger.debug("sbatch submitted job %d", result)
                return result
            except OSError as e:
                logger.error("sbatch command failed")
                raise e

        def coalesce_job_exit_codes(self, batch_job_id_list: list) -> list:
            """
            Collect all job exit codes in a single call.
            :param batch_job_id_list: list of Job ID strings, where each string has the form
            "<job>[.<task>]".
            :return: list of job exit codes, associated with the list of job IDs.
            """
            logger.debug("Getting exit codes for slurm jobs: %s", batch_job_id_list)
            # Convert batch_job_id_list to list of integer job IDs.
            job_id_list = [int(id.split('.')[0]) for id in batch_job_id_list]
            status_dict = self._get_job_details(job_id_list)
            exit_codes = []
            for _, status in status_dict.items():
                exit_codes.append(self._get_job_return_code(status))
            return exit_codes

        def getJobExitCode(self, batchJobID: str) -> int:
            """
            Get job exit code for given batch job ID.
            :param batchJobID: string of the form "<job>[.<task>]".
            :return: integer job exit code.
            """
            logger.debug("Getting exit code for slurm job: %s", batchJobID)
            # Convert batchJobID to an integer job ID.
            job_id = int(batchJobID.split('.')[0])
            status_dict = self._get_job_details([job_id])
            status = status_dict[job_id]
            return self._get_job_return_code(status)

        def _get_job_details(self, job_id_list: list) -> dict:
            """
            Helper function for `getJobExitCode` and `coalesce_job_exit_codes`.
            Fetch job details from Slurm's accounting system or job control system.
            :param job_id_list: list of integer Job IDs.
            :return: dict of job statuses, where key is the integer job ID, and value is a tuple
            containing the job's state and exit code.
            """
            try:
                status_dict = self._getJobDetailsFromSacct(job_id_list)
            except CalledProcessErrorStderr:
                status_dict = self._getJobDetailsFromScontrol(job_id_list)
            return status_dict

        def _get_job_return_code(self, status: tuple) -> list:
            """
            Helper function for `getJobExitCode` and `coalesce_job_exit_codes`.
            :param status: tuple containing the job's state and it's return code.
            :return: the job's return code if it's completed, otherwise None.
            """
            state, rc = status
            # If job is in a running state, set return code to None to indicate we don't have
            # an update.
            if state in ('PENDING', 'RUNNING', 'CONFIGURING', 'COMPLETING', 'RESIZING', 'SUSPENDED'):
                rc = None
            return rc

        def _getJobDetailsFromSacct(self, job_id_list: list) -> dict:
            """
            Get SLURM job exit codes for the jobs in `job_id_list` by running `sacct`.
            :param job_id_list: list of integer batch job IDs.
            :return: dict of job statuses, where key is the job-id, and value is a tuple
            containing the job's state and exit code.
            """
            job_ids = ",".join(str(id) for id in job_id_list)
            args = ['sacct',
                    '-n',  # no header
                    '-j', job_ids,  # job
                    '--format', 'JobIDRaw,State,ExitCode',  # specify output columns
                    '-P',  # separate columns with pipes
                    '-S', '1970-01-01']  # override start time limit
            stdout = call_command(args, quiet=True)

            # Collect the job statuses in a dict; key is the job-id, value is a tuple containing
            # job state and exit status. Initialize dict before processing output of `sacct`.
            job_statuses = {}
            for job_id in job_id_list:
                job_statuses[job_id] = (None, None)

            for line in stdout.splitlines():
                values = line.strip().split('|')
                if len(values) < 3:
                    continue
                job_id_raw, state, exitcode = values
                logger.debug("%s state of job %s is %s", args[0], job_id_raw, state)
                # JobIDRaw is in the form JobID[.JobStep]; we're not interested in job steps.
                job_id_parts = job_id_raw.split(".")
                if len(job_id_parts) > 1:
                    continue
                job_id = int(job_id_parts[0])
                status, signal = (int(n) for n in exitcode.split(':'))
                if signal > 0:
                    # A non-zero signal may indicate e.g. an out-of-memory killed job
                    status = 128 + signal
                logger.debug("%s exit code of job %d is %s, return status %d",
                             args[0], job_id, exitcode, status)
                job_statuses[job_id] = state, status
            logger.debug("%s returning job statuses: %s", args[0], job_statuses)
            return job_statuses

        def _getJobDetailsFromScontrol(self, job_id_list: list) -> dict:
            """
            Get SLURM job exit codes for the jobs in `job_id_list` by running `scontrol`.
            :param job_id_list: list of integer batch job IDs.
            :return: dict of job statuses, where key is the job-id, and value is a tuple
            containing the job's state and exit code.
            """
            args = ['scontrol',
                    'show',
                    'job']
            # `scontrol` can only return information about a single job,
            # or all the jobs it knows about.
            if len(job_id_list) == 1:
                args.append(str(job_id_list[0]))

            stdout = call_command(args, quiet=True)

            # Job records are separated by a blank line.
            if isinstance(stdout, str):
                job_records = stdout.strip().split('\n\n')
            elif isinstance(stdout, bytes):
                job_records = stdout.decode('utf-8').strip().split('\n\n')

            # Collect the job statuses in a dict; key is the job-id, value is a tuple containing
            # job state and exit status. Initialize dict before processing output of `scontrol`.
            job_statuses = {}
            for job_id in job_id_list:
                job_statuses[job_id] = (None, None)

            # `scontrol` will report "No jobs in the system", if there are no jobs in the system,
            # and if no job-id was passed as argument to `scontrol`.
            if len(job_records) > 0 and job_records[0] == "No jobs in the system":
                return job_statuses

            for record in job_records:
                job = {}
                for line in record.splitlines():
                    for item in line.split():
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
                    if job_id not in job_id_list:
                        logger.debug("%s job %d is not in the list", args[0], job_id)
                        break
                if job_id not in job_id_list:
                    continue
                state = job['JobState']
                logger.debug("%s state of job %s is %s", args[0], job_id, state)
                try:
                    exitcode = job['ExitCode']
                    if exitcode is not None:
                        status, signal = (int(n) for n in exitcode.split(':'))
                        if signal > 0:
                            # A non-zero signal may indicate e.g. an out-of-memory killed job
                            status = 128 + signal
                        logger.debug("%s exit code of job %d is %s, return status %d",
                                     args[0], job_id, exitcode, status)
                        rc = status
                    else:
                        rc = None
                except KeyError:
                    rc = None
                job_statuses[job_id] = (state, rc)
            logger.debug("%s returning job statuses: %s", args[0], job_statuses)
            return job_statuses

        ###
        ### Implementation-specific helper methods
        ###

        def prepareSbatch(self,
                          cpu: int,
                          mem: int,
                          jobID: int,
                          jobName: str,
                          job_environment: Optional[Dict[str, str]],
                          gpus: Optional[int]) -> List[str]:

            #  Returns the sbatch command line before the script to run
            sbatch_line = ['sbatch', '-J', f'toil_job_{jobID}_{jobName}']
            if gpus:
                sbatch_line = sbatch_line[:1] + [f'--gres=gpu:{gpus}'] + sbatch_line[1:]
            environment = {}
            environment.update(self.boss.environment)
            if job_environment:
                environment.update(job_environment)

            # "Native extensions" for SLURM (see DRMAA or SAGA)
            nativeConfig = os.getenv('TOIL_SLURM_ARGS')

            # --export=[ALL,]<environment_toil_variables>
            set_exports = "--export=ALL"

            if nativeConfig is not None:
                logger.debug("Native SLURM options appended to sbatch from TOIL_SLURM_ARGS env. variable: %s", nativeConfig)

                for arg in nativeConfig.split():
                    if arg.startswith("--mem") or arg.startswith("--cpus-per-task"):
                        raise ValueError(f"Some resource arguments are incompatible: {nativeConfig}")
                    # repleace default behaviour by the one stated at TOIL_SLURM_ARGS
                    if arg.startswith("--export"):
                        set_exports = arg
                sbatch_line.extend(nativeConfig.split())

            if environment:
                argList = []

                for k, v in environment.items():
                    quoted_value = quote(os.environ[k] if v is None else v)
                    argList.append(f'{k}={quoted_value}')

                set_exports += ',' + ','.join(argList)

            # add --export to the sbatch
            sbatch_line.append(set_exports)

            parallel_env = os.getenv('TOIL_SLURM_PE')
            if cpu and cpu > 1 and parallel_env:
                sbatch_line.append(f'--partition={parallel_env}')

            if mem is not None and self.boss.config.allocate_mem:
                # memory passed in is in bytes, but slurm expects megabytes
                sbatch_line.append(f'--mem={math.ceil(mem / 2 ** 20)}')
            if cpu is not None:
                sbatch_line.append(f'--cpus-per-task={math.ceil(cpu)}')

            stdoutfile: str = self.boss.format_std_out_err_path(jobID, '%j', 'out')
            stderrfile: str = self.boss.format_std_out_err_path(jobID, '%j', 'err')
            sbatch_line.extend(['-o', stdoutfile, '-e', stderrfile])

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

    def _check_accelerator_request(self, requirer: Requirer) -> None:
        for accelerator in requirer.accelerators:
            if accelerator['kind'] != 'gpu':
                raise InsufficientSystemResources(requirer, 'accelerators', details=
                                                  [
                                                      f'The accelerator {accelerator} could not be provided'
                                                      'The Toil Slurm batch system only supports gpu accelerators at the moment.'
                                                  ])

    ###
    ### The interface for SLURM
    ###

    # `scontrol show config` can get us the slurm config, and there are values
    # SchedulerTimeSlice and AcctGatherNodeFreq in there, but
    # SchedulerTimeSlice is for time-sharing preemtion and AcctGatherNodeFreq
    # is for reporting resource statistics (and can be 0). Slurm does not
    # actually seem to have a scheduling granularity or tick rate. So we don't
    # implement getWaitDuration().

    @classmethod
    def add_options(cls, parser: Union[ArgumentParser, _ArgumentGroup]):
        allocate_mem = parser.add_mutually_exclusive_group()
        allocate_mem_help = ("A flag that can block allocating memory with '--mem' for job submissions "
                             "on SLURM since some system servers may reject any job request that "
                             "explicitly specifies the memory allocation.  The default is to always allocate memory.")
        allocate_mem.add_argument("--dont_allocate_mem", action='store_false', dest="allocate_mem", help=allocate_mem_help)
        allocate_mem.add_argument("--allocate_mem", action='store_true', dest="allocate_mem", help=allocate_mem_help)
        allocate_mem.set_defaults(allocate_mem=True)

    OptionType = TypeVar('OptionType')
    @classmethod
    def setOptions(cls, setOption: OptionSetter) -> None:
        setOption("allocate_mem", bool, default=False)

