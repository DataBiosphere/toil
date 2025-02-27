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
from __future__ import annotations

import errno
import logging
import math
import os
import sys
from argparse import SUPPRESS, ArgumentParser, _ArgumentGroup
from shlex import quote
from typing import NamedTuple, TypeVar

from toil.batchSystems.abstractBatchSystem import (
    EXIT_STATUS_UNAVAILABLE_VALUE,
    BatchJobExitReason,
    InsufficientSystemResources,
)
from toil.batchSystems.abstractGridEngineBatchSystem import (
    AbstractGridEngineBatchSystem,
)
from toil.batchSystems.options import OptionSetter
from toil.bus import get_job_kind
from toil.common import Config
from toil.job import JobDescription, Requirer
from toil.lib.conversions import strtobool
from toil.lib.misc import CalledProcessErrorStderr, call_command
from toil.statsAndLogging import TRACE

logger = logging.getLogger(__name__)

# We have a complete list of Slurm states. States not in one of these aren't
# allowed. See <https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES>

# If a job is in one of these states, Slurm can't run it anymore.
# We don't include states where the job is held or paused here;
# those mean it could run and needs to wait for someone to un-hold
# it, so Toil should wait for it.
#
# We map from each terminal state to the Toil-ontology exit reason.
TERMINAL_STATES: dict[str, BatchJobExitReason] = {
    "BOOT_FAIL": BatchJobExitReason.LOST,
    "CANCELLED": BatchJobExitReason.KILLED,
    "COMPLETED": BatchJobExitReason.FINISHED,
    "DEADLINE": BatchJobExitReason.KILLED,
    "FAILED": BatchJobExitReason.FAILED,
    "NODE_FAIL": BatchJobExitReason.LOST,
    "OUT_OF_MEMORY": BatchJobExitReason.MEMLIMIT,
    "PREEMPTED": BatchJobExitReason.KILLED,
    "REVOKED": BatchJobExitReason.KILLED,
    "SPECIAL_EXIT": BatchJobExitReason.FAILED,
    "TIMEOUT": BatchJobExitReason.KILLED,
}

# If a job is in one of these states, it might eventually move to a different
# state.
NONTERMINAL_STATES: set[str] = {
    "CONFIGURING",
    "COMPLETING",
    "PENDING",
    "RUNNING",
    "RESV_DEL_HOLD",
    "REQUEUE_FED",
    "REQUEUE_HOLD",
    "REQUEUED",
    "RESIZING",
    "SIGNALING",
    "STAGE_OUT",
    "STOPPED",
    "SUSPENDED",
}


def parse_slurm_time(slurm_time: str) -> int:
    """
    Parse a Slurm-style time duration like 7-00:00:00 to a number of seconds.

    Raises ValueError if not parseable.
    """
    # slurm returns time in days-hours:minutes:seconds format
    # Sometimes it will only return minutes:seconds, so days may be omitted
    # For ease of calculating, we'll make sure all the delimeters are ':'
    # Then reverse the list so that we're always counting up from seconds -> minutes -> hours -> days
    total_seconds = 0
    elapsed_split: list[str] = slurm_time.replace("-", ":").split(":")
    elapsed_split.reverse()
    seconds_per_unit = [1, 60, 3600, 86400]
    for index, multiplier in enumerate(seconds_per_unit):
        if index < len(elapsed_split):
            total_seconds += multiplier * int(elapsed_split[index])
    return total_seconds


class SlurmBatchSystem(AbstractGridEngineBatchSystem):
    class PartitionInfo(NamedTuple):
        partition_name: str
        gres: bool
        time_limit: float
        priority: int
        cpus: str
        memory: str

    class PartitionSet:
        """
        Set of available partitions detected on the slurm batch system
        """

        default_gpu_partition: SlurmBatchSystem.PartitionInfo | None
        all_partitions: list[SlurmBatchSystem.PartitionInfo]
        gpu_partitions: set[str]

        def __init__(self) -> None:
            self._get_partition_info()
            self._get_gpu_partitions()

        def _get_gpu_partitions(self) -> None:
            """
            Get all available GPU partitions. Also get the default GPU partition.
            :return: None
            """
            gpu_partitions = [
                partition for partition in self.all_partitions if partition.gres
            ]
            self.gpu_partitions = {p.partition_name for p in gpu_partitions}
            # Grab the lowest priority GPU partition
            # If no GPU partitions are available, then set the default to None
            self.default_gpu_partition = None
            if len(gpu_partitions) > 0:
                self.default_gpu_partition = sorted(
                    gpu_partitions, key=lambda x: x.priority
                )[0]

        def _get_partition_info(self) -> None:
            """
            Call the Slurm batch system with sinfo to grab all available partitions.
            Then parse the output and store all available Slurm partitions
            :return: None
            """
            sinfo_command = ["sinfo", "-a", "-o", "%P %G %l %p %c %m"]

            sinfo = call_command(sinfo_command)

            parsed_partitions = []
            for line in sinfo.split("\n")[1:]:
                if line.strip():
                    partition_name, gres, time, priority, cpus, memory = line.split(" ")
                    try:
                        # Parse time to a number so we can compute on it
                        partition_time: float = parse_slurm_time(time)
                    except ValueError:
                        # Maybe time is unlimited?
                        partition_time = float("inf")
                    try:
                        # Parse priority to an int so we can sort on it
                        partition_priority = int(priority)
                    except ValueError:
                        logger.warning(
                            "Could not parse priority %s for partition %s, assuming high priority",
                            partition_name,
                            priority,
                        )
                        partition_priority = sys.maxsize
                    parsed_partitions.append(
                        SlurmBatchSystem.PartitionInfo(
                            partition_name.rstrip("*"),
                            gres != "(null)",
                            partition_time,
                            partition_priority,
                            cpus,
                            memory,
                        )
                    )
            self.all_partitions = parsed_partitions

        def get_partition(self, time_limit: float | None) -> str | None:
            """
            Get the partition name to use for a job with the given time limit.
            """

            if time_limit is None:
                # Just use Slurm's default
                return None

            winning_partition = None
            for partition in self.all_partitions:
                if partition.time_limit >= time_limit and (
                    winning_partition is None
                    or partition.time_limit < winning_partition.time_limit
                ):
                    # If this partition can fit the job and is faster than the current winner, take it
                    winning_partition = partition
            # TODO: Store partitions in a better indexed way
            if winning_partition is None and len(self.all_partitions) > 0:
                # We have partitions and none of them can fit this
                raise RuntimeError(
                    "Could not find a Slurm partition that can fit a job that runs for {time_limit} seconds"
                )

            if winning_partition is None:
                return None
            else:
                return winning_partition.partition_name

    class GridEngineThread(AbstractGridEngineBatchSystem.GridEngineThread):
        # Our boss is always the enclosing class
        boss: SlurmBatchSystem

        def getRunningJobIDs(self) -> dict[int, int]:
            # Should return a dictionary of Job IDs and number of seconds
            times = {}
            with self.runningJobsLock:
                currentjobs: dict[str, int] = {
                    str(self.batchJobIDs[x][0]): x for x in self.runningJobs
                }
            # currentjobs is a dictionary that maps a slurm job id (string) to our own internal job id
            # squeue arguments:
            # -h for no header
            # --format to get jobid i, state %t and time days-hours:minutes:seconds

            lines = call_command(
                ["squeue", "-h", "--format", "%i %t %M"], quiet=True
            ).split("\n")
            for line in lines:
                values = line.split()
                if len(values) < 3:
                    continue
                slurm_jobid, state, elapsed_time = values
                if slurm_jobid in currentjobs and state == "R":
                    try:
                        seconds_running = parse_slurm_time(elapsed_time)
                    except ValueError:
                        # slurm may return INVALID instead of a time
                        seconds_running = 0
                    times[currentjobs[slurm_jobid]] = seconds_running

            return times

        def killJob(self, jobID: int) -> None:
            call_command(["scancel", self.getBatchSystemID(jobID)])

        def prepareSubmission(
            self,
            cpu: int,
            memory: int,
            jobID: int,
            command: str,
            jobName: str,
            job_environment: dict[str, str] | None = None,
            gpus: int | None = None,
        ) -> list[str]:
            # Make sure to use exec so we can get Slurm's signals in the Toil
            # worker instead of having an intervening Bash
            return self.prepareSbatch(
                cpu, memory, jobID, jobName, job_environment, gpus
            ) + [f"--wrap=exec {command}"]

        def submitJob(self, subLine: list[str]) -> int:
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
                session_names = [
                    n
                    for n in no_session_environment.keys()
                    if n.startswith("XDG_") or n.startswith("DBUS_")
                ]
                for name in session_names:
                    del no_session_environment[name]

                output = call_command(subLine, env=no_session_environment)
                # sbatch prints a line like 'Submitted batch job 2954103'
                result = int(output.strip().split()[-1])
                logger.debug("sbatch submitted job %d", result)
                return result
            except OSError as e:
                logger.error(f"sbatch command failed with error: {e}")
                raise e

        def coalesce_job_exit_codes(
            self, batch_job_id_list: list[str]
        ) -> list[int | tuple[int, BatchJobExitReason | None] | None]:
            """
            Collect all job exit codes in a single call.
            :param batch_job_id_list: list of Job ID strings, where each string has the form
            "<job>[.<task>]".
            :return: list of job exit codes or exit code, exit reason pairs associated with the list of job IDs.
            """
            logger.log(
                TRACE, "Getting exit codes for slurm jobs: %s", batch_job_id_list
            )
            # Convert batch_job_id_list to list of integer job IDs.
            job_id_list = [int(id.split(".")[0]) for id in batch_job_id_list]
            status_dict = self._get_job_details(job_id_list)
            exit_codes: list[int | tuple[int, BatchJobExitReason | None] | None] = []
            for _, status in status_dict.items():
                exit_codes.append(self._get_job_return_code(status))
            return exit_codes

        def getJobExitCode(
            self, batchJobID: str
        ) -> int | tuple[int, BatchJobExitReason | None] | None:
            """
            Get job exit code for given batch job ID.
            :param batchJobID: string of the form "<job>[.<task>]".
            :return: integer job exit code.
            """
            logger.log(TRACE, "Getting exit code for slurm job: %s", batchJobID)
            # Convert batchJobID to an integer job ID.
            job_id = int(batchJobID.split(".")[0])
            status_dict = self._get_job_details([job_id])
            status = status_dict[job_id]
            return self._get_job_return_code(status)

        def _get_job_details(
            self, job_id_list: list[int]
        ) -> dict[int, tuple[str | None, int | None]]:
            """
            Helper function for `getJobExitCode` and `coalesce_job_exit_codes`.
            Fetch job details from Slurm's accounting system or job control system.
            :param job_id_list: list of integer Job IDs.
            :return: dict of job statuses, where key is the integer job ID, and value is a tuple
            containing the job's state and exit code.
            """
            try:
                status_dict = self._getJobDetailsFromSacct(job_id_list)
            except (CalledProcessErrorStderr, OSError) as e:
                if isinstance(e, OSError):
                    logger.warning("Could not run sacct: %s", e)
                status_dict = self._getJobDetailsFromScontrol(job_id_list)
            return status_dict

        def _get_job_return_code(
            self, status: tuple[str | None, int | None]
        ) -> int | tuple[int, BatchJobExitReason | None] | None:
            """
            Given a Slurm return code, status pair, summarize them into a Toil return code, exit reason pair.

            The return code may have already been OR'd with the 128-offset
            Slurm-reported signal.

            Slurm will report return codes of 0 even if jobs time out instead
            of succeeding:

                2093597|TIMEOUT|0:0
                2093597.batch|CANCELLED|0:15

            So we guarantee here that, if the Slurm status string is not a
            successful one as defined in
            <https://slurm.schedmd.com/squeue.html#SECTION_JOB-STATE-CODES>, we
            will not return a successful return code.

            Helper function for `getJobExitCode` and `coalesce_job_exit_codes`.
            :param status: tuple containing the job's state and it's return code from Slurm.
            :return: the job's return code for Toil if it's completed, otherwise None.
            """
            state, rc = status

            if state not in TERMINAL_STATES:
                # Don't treat the job as exited yet
                return None

            exit_reason = TERMINAL_STATES[state]

            if exit_reason == BatchJobExitReason.FINISHED:
                # The only state that should produce a 0 ever is COMPLETED. So
                # if the job is COMPLETED and the exit reason is thus FINISHED,
                # pass along the code it has.
                return (rc, exit_reason)  # type: ignore[return-value] # mypy doesn't understand enums well

            if rc == 0:
                # The job claims to be in a state other than COMPLETED, but
                # also to have not encountered a problem. Say the exit status
                # is unavailable.
                return (EXIT_STATUS_UNAVAILABLE_VALUE, exit_reason)

            # If the code is nonzero, pass it along.
            return (rc, exit_reason)  # type: ignore[return-value] # mypy doesn't understand enums well

        def _canonicalize_state(self, state: str) -> str:
            """
            Turn a state string form SLURM into just the state token like "CANCELED".
            """

            # Slurm will sometimes send something like "CANCELED by 30065" in
            # the state column for some reason.

            state_token = state

            if " " in state_token:
                state_token = state.split(" ", 1)[0]

            if (
                state_token not in TERMINAL_STATES
                and state_token not in NONTERMINAL_STATES
            ):
                raise RuntimeError("Toil job in unimplemented Slurm state " + state)

            return state_token

        def _getJobDetailsFromSacct(
            self, job_id_list: list[int]
        ) -> dict[int, tuple[str | None, int | None]]:
            """
            Get SLURM job exit codes for the jobs in `job_id_list` by running `sacct`.
            :param job_id_list: list of integer batch job IDs.
            :return: dict of job statuses, where key is the job-id, and value is a tuple
            containing the job's state and exit code.
            """
            job_ids = ",".join(str(id) for id in job_id_list)
            args = [
                "sacct",
                "-n",  # no header
                "-j",
                job_ids,  # job
                "--format",
                "JobIDRaw,State,ExitCode",  # specify output columns
                "-P",  # separate columns with pipes
                "-S",
                "1970-01-01",
            ]  # override start time limit

            # Collect the job statuses in a dict; key is the job-id, value is a tuple containing
            # job state and exit status. Initialize dict before processing output of `sacct`.
            job_statuses: dict[int, tuple[str | None, int | None]] = {}

            try:
                stdout = call_command(args, quiet=True)
            except OSError as e:
                if e.errno == errno.E2BIG:
                    # Argument list is too big, recurse on half the argument list
                    if len(job_id_list) == 1:
                        # 1 is too big, we can't recurse further, bail out
                        raise
                    job_statuses.update(self._getJobDetailsFromSacct(job_id_list[:len(job_id_list)//2]))
                    job_statuses.update(self._getJobDetailsFromSacct(job_id_list[len(job_id_list)//2:]))
                    return job_statuses
                else:
                    raise

            for job_id in job_id_list:
                job_statuses[job_id] = (None, None)

            for line in stdout.splitlines():
                values = line.strip().split("|")
                if len(values) < 3:
                    continue
                state: str
                job_id_raw, state, exitcode = values
                state = self._canonicalize_state(state)
                logger.log(
                    TRACE, "%s state of job %s is %s", args[0], job_id_raw, state
                )
                # JobIDRaw is in the form JobID[.JobStep]; we're not interested in job steps.
                job_id_parts = job_id_raw.split(".")
                if len(job_id_parts) > 1:
                    continue
                job_id = int(job_id_parts[0])
                status: int
                signal: int
                status, signal = (int(n) for n in exitcode.split(":"))
                if signal > 0:
                    # A non-zero signal may indicate e.g. an out-of-memory killed job
                    status = 128 + signal
                logger.log(
                    TRACE,
                    "%s exit code of job %d is %s, return status %d",
                    args[0],
                    job_id,
                    exitcode,
                    status,
                )
                job_statuses[job_id] = state, status
            logger.log(TRACE, "%s returning job statuses: %s", args[0], job_statuses)
            return job_statuses

        def _getJobDetailsFromScontrol(
            self, job_id_list: list[int]
        ) -> dict[int, tuple[str | None, int | None]]:
            """
            Get SLURM job exit codes for the jobs in `job_id_list` by running `scontrol`.
            :param job_id_list: list of integer batch job IDs.
            :return: dict of job statuses, where key is the job-id, and value is a tuple
            containing the job's state and exit code.
            """
            args = ["scontrol", "show", "job"]
            # `scontrol` can only return information about a single job,
            # or all the jobs it knows about.
            if len(job_id_list) == 1:
                args.append(str(job_id_list[0]))

            stdout = call_command(args, quiet=True)

            # Job records are separated by a blank line.
            job_records = None
            if isinstance(stdout, str):
                job_records = stdout.strip().split("\n\n")
            elif isinstance(stdout, bytes):
                job_records = stdout.decode("utf-8").strip().split("\n\n")

            # Collect the job statuses in a dict; key is the job-id, value is a tuple containing
            # job state and exit status. Initialize dict before processing output of `scontrol`.
            job_statuses: dict[int, tuple[str | None, int | None]] = {}
            job_id: int | None
            for job_id in job_id_list:
                job_statuses[job_id] = (None, None)

            # `scontrol` will report "No jobs in the system", if there are no jobs in the system,
            # and if no job-id was passed as argument to `scontrol`.
            if len(job_records) > 0 and job_records[0] == "No jobs in the system":
                return job_statuses

            for record in job_records:
                job: dict[str, str] = {}
                job_id = None
                for line in record.splitlines():
                    for item in line.split():
                        # Output is in the form of many key=value pairs, multiple pairs on each line
                        # and multiple lines in the output. Each pair is pulled out of each line and
                        # added to a dictionary.
                        # Note: In some cases, the value itself may contain white-space. So, if we find
                        # a key without a value, we consider that key part of the previous value.
                        bits = item.split("=", 1)
                        if len(bits) == 1:
                            job[key] += " " + bits[0]  # type: ignore[has-type]  # we depend on the previous iteration to populate key
                        else:
                            key = bits[0]
                            job[key] = bits[1]
                    # The first line of the record contains the JobId. Stop processing the remainder
                    # of this record, if we're not interested in this job.
                    job_id = int(job["JobId"])
                    if job_id not in job_id_list:
                        logger.log(
                            TRACE, "%s job %d is not in the list", args[0], job_id
                        )
                        break
                if job_id is None or job_id not in job_id_list:
                    continue
                state = job["JobState"]
                state = self._canonicalize_state(state)
                logger.log(TRACE, "%s state of job %s is %s", args[0], job_id, state)
                try:
                    exitcode = job["ExitCode"]
                    if exitcode is not None:
                        status, signal = (int(n) for n in exitcode.split(":"))
                        if signal > 0:
                            # A non-zero signal may indicate e.g. an out-of-memory killed job
                            status = 128 + signal
                        logger.log(
                            TRACE,
                            "%s exit code of job %d is %s, return status %d",
                            args[0],
                            job_id,
                            exitcode,
                            status,
                        )
                        rc = status
                    else:
                        rc = None
                except KeyError:
                    rc = None
                job_statuses[job_id] = (state, rc)
            logger.log(TRACE, "%s returning job statuses: %s", args[0], job_statuses)
            return job_statuses

        ###
        ### Implementation-specific helper methods
        ###

        def prepareSbatch(
            self,
            cpu: int,
            mem: int,
            jobID: int,
            jobName: str,
            job_environment: dict[str, str] | None,
            gpus: int | None,
        ) -> list[str]:
            """
            Returns the sbatch command line to run to queue the job.
            """

            # Start by naming the job
            sbatch_line = ["sbatch", "-J", f"toil_job_{jobID}_{jobName}"]

            # Make sure the job gets a signal before it disappears so that e.g.
            # container cleanup finally blocks can run. Ask for SIGINT so we
            # can get the default Python KeyboardInterrupt which third-party
            # code is likely to plan for. Make sure to send it to the batch
            # shell process with "B:", not to all the srun steps it launches
            # (because there shouldn't be any). We cunningly replaced the batch
            # shell process with the Toil worker process, so Toil should be
            # able to get the signal.
            #
            # TODO: Add a way to detect when the job failed because it
            # responded to this signal and use the right exit reason for it.
            sbatch_line.append("--signal=B:INT@30")

            environment = {}
            environment.update(self.boss.environment)
            if job_environment:
                environment.update(job_environment)

            # "Native extensions" for SLURM (see DRMAA or SAGA)
            # Also any extra arguments from --slurmArgs or TOIL_SLURM_ARGS
            nativeConfig: str = self.boss.config.slurm_args  # type: ignore[attr-defined]

            # --export=[ALL,]<environment_toil_variables>
            set_exports = "--export=ALL"

            if nativeConfig is not None:
                logger.debug(
                    "Native SLURM options appended to sbatch: %s", nativeConfig
                )

                for arg in nativeConfig.split():
                    if arg.startswith("--mem") or arg.startswith("--cpus-per-task"):
                        raise ValueError(
                            f"Some resource arguments are incompatible: {nativeConfig}"
                        )
                    # repleace default behaviour by the one stated at TOIL_SLURM_ARGS
                    if arg.startswith("--export"):
                        set_exports = arg
                sbatch_line.extend(nativeConfig.split())

            if environment:
                argList = []

                for k, v in environment.items():
                    quoted_value = quote(os.environ[k] if v is None else v)
                    argList.append(f"{k}={quoted_value}")

                set_exports += "," + ",".join(argList)

            # add --export to the sbatch
            sbatch_line.append(set_exports)

            parallel_env: str = self.boss.config.slurm_pe  # type: ignore[attr-defined]
            if cpu and cpu > 1 and parallel_env:
                sbatch_line.append(f"--partition={parallel_env}")

            if mem is not None and self.boss.config.slurm_allocate_mem:  # type: ignore[attr-defined]
                # memory passed in is in bytes, but slurm expects megabytes
                sbatch_line.append(f"--mem={math.ceil(mem / 2 ** 20)}")
            if cpu is not None:
                sbatch_line.append(f"--cpus-per-task={math.ceil(cpu)}")

            time_limit: int = self.boss.config.slurm_time  # type: ignore[attr-defined]
            if time_limit is not None:
                # Put all the seconds in the seconds slot
                sbatch_line.append(f"--time=0:{time_limit}")

            if gpus:
                # This block will add a gpu supported partition only if no partition is supplied by the user
                sbatch_line = sbatch_line[:1] + [f"--gres=gpu:{gpus}"] + sbatch_line[1:]
                if not any(option.startswith("--partition") for option in sbatch_line):
                    # no partition specified, so specify one
                    # try to get the name of the lowest priority gpu supported partition
                    lowest_gpu_partition = self.boss.partitions.default_gpu_partition
                    if lowest_gpu_partition is None:
                        # no gpu partitions are available, raise an error
                        raise RuntimeError(
                            f"The job {jobName} is requesting GPUs, but the Slurm cluster does not appear to have an accessible partition with GPUs"
                        )
                    if (
                        time_limit is not None
                        and lowest_gpu_partition.time_limit < time_limit
                    ):
                        # TODO: find the lowest-priority GPU partition that has at least each job's time limit!
                        logger.warning(
                            "Trying to submit a job that needs %s seconds to partition %s that has a limit of %s seconds",
                            time_limit,
                            lowest_gpu_partition.partition_name,
                            lowest_gpu_partition.time_limit,
                        )
                    sbatch_line.append(
                        f"--partition={lowest_gpu_partition.partition_name}"
                    )
                else:
                    # there is a partition specified already, check if the partition has GPUs
                    for i, option in enumerate(sbatch_line):
                        if option.startswith("--partition"):
                            # grab the partition name depending on if it's specified via an "=" or a space
                            if "=" in option:
                                partition_name = option[len("--partition=") :]
                            else:
                                partition_name = option[i + 1]
                            available_gpu_partitions = (
                                self.boss.partitions.gpu_partitions
                            )
                            if partition_name not in available_gpu_partitions:
                                # the specified partition is not compatible, so warn the user that the job may not work
                                logger.warning(
                                    f"Job {jobName} needs {gpus} GPUs, but specified partition {partition_name} is incompatible. This job may not work."
                                    f"Try specifying one of these partitions instead: {', '.join(available_gpu_partitions)}."
                                )
                            break

            if not any(option.startswith("--partition") for option in sbatch_line):
                # Pick a partition ourselves
                chosen_partition = self.boss.partitions.get_partition(time_limit)
                if chosen_partition is not None:
                    # Route to that partition
                    sbatch_line.append(f"--partition={chosen_partition}")

            stdoutfile: str = self.boss.format_std_out_err_path(jobID, "%j", "out")
            stderrfile: str = self.boss.format_std_out_err_path(jobID, "%j", "err")
            sbatch_line.extend(["-o", stdoutfile, "-e", stderrfile])

            return sbatch_line

    def __init__(
        self, config: Config, maxCores: float, maxMemory: int, maxDisk: int
    ) -> None:
        super().__init__(config, maxCores, maxMemory, maxDisk)
        self.partitions = SlurmBatchSystem.PartitionSet()

    # Override issuing jobs so we can check if we need to use Slurm's magic
    # whole-node-memory feature.
    def issueBatchJob(
        self,
        command: str,
        job_desc: JobDescription,
        job_environment: dict[str, str] | None = None,
    ) -> int:
        # Avoid submitting internal jobs to the batch queue, handle locally
        local_id = self.handleLocalJob(command, job_desc)
        if local_id is not None:
            return local_id
        else:
            self.check_resource_request(job_desc)
            gpus = self.count_needed_gpus(job_desc)
            job_id = self.getNextJobID()
            self.currentJobs.add(job_id)

            if "memory" not in job_desc.requirements and self.config.slurm_default_all_mem:  # type: ignore[attr-defined]
                # The job doesn't have its own memory requirement, and we are
                # defaulting to whole node memory. Use Slurm's 0-memory sentinel.
                memory = 0
            else:
                # Use the memory actually on the job, or the Toil default memory
                memory = job_desc.memory

            self.newJobsQueue.put(
                (
                    job_id,
                    job_desc.cores,
                    memory,
                    command,
                    get_job_kind(job_desc.get_names()),
                    job_environment,
                    gpus,
                )
            )
            logger.debug(
                "Issued the job command: %s with job id: %s and job name %s",
                command,
                str(job_id),
                get_job_kind(job_desc.get_names()),
            )
        return job_id

    def _check_accelerator_request(self, requirer: Requirer) -> None:
        for accelerator in requirer.accelerators:
            if accelerator["kind"] != "gpu":
                raise InsufficientSystemResources(
                    requirer,
                    "accelerators",
                    details=[
                        f"The accelerator {accelerator} could not be provided"
                        "The Toil Slurm batch system only supports gpu accelerators at the moment."
                    ],
                )

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
    def add_options(cls, parser: ArgumentParser | _ArgumentGroup) -> None:

        parser.add_argument(
            "--slurmAllocateMem",
            dest="slurm_allocate_mem",
            type=strtobool,
            default=True,
            env_var="TOIL_SLURM_ALLOCATE_MEM",
            help="If False, do not use --mem. Used as a workaround for Slurm clusters that reject jobs "
            "with memory allocations.",
        )
        # Keep these deprcated options for backward compatibility
        parser.add_argument(
            "--dont_allocate_mem",
            action="store_false",
            dest="slurm_allocate_mem",
            help=SUPPRESS,
        )
        parser.add_argument(
            "--allocate_mem",
            action="store_true",
            dest="slurm_allocate_mem",
            help=SUPPRESS,
        )

        parser.add_argument(
            "--slurmDefaultAllMem",
            dest="slurm_default_all_mem",
            type=strtobool,
            default=False,
            env_var="TOIL_SLURM_DEFAULT_ALL_MEM",
            help="If True, assign Toil jobs without their own memory requirements all available "
            "memory on a Slurm node (via Slurm --mem=0).",
        )
        parser.add_argument(
            "--slurmTime",
            dest="slurm_time",
            type=parse_slurm_time,
            default=None,
            env_var="TOIL_SLURM_TIME",
            help="Slurm job time limit, in [DD-]HH:MM:SS format.",
        )
        parser.add_argument(
            "--slurmPE",
            dest="slurm_pe",
            default=None,
            env_var="TOIL_SLURM_PE",
            help="Special partition to send Slurm jobs to if they ask for more than 1 CPU.",
        )
        parser.add_argument(
            "--slurmArgs",
            dest="slurm_args",
            default="",
            env_var="TOIL_SLURM_ARGS",
            help="Extra arguments to pass to Slurm.",
        )

    OptionType = TypeVar("OptionType")

    @classmethod
    def setOptions(cls, setOption: OptionSetter) -> None:
        setOption("slurm_allocate_mem")
        setOption("slurm_default_all_mem")
        setOption("slurm_time")
        setOption("slurm_pe")
        setOption("slurm_args")
