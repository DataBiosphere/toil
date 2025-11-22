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
import shlex
import sys
from argparse import SUPPRESS, ArgumentParser, _ArgumentGroup
from collections.abc import Callable
from datetime import datetime, timedelta
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


# For parsing user-provided option overrides (or self-generated
# options) for sbatch, we need a way to recognize long, long-with-equals, and
# short forms.
def option_detector(long: str, short: str | None = None) -> Callable[[str], bool]:
    """
    Get a function that returns true if it sees the long or short
    option.
    """

    def is_match(option: str) -> bool:
        return (
            option == f"--{long}"
            or option.startswith(f"--{long}=")
            or (short is not None and option == f"-{short}")
        )

    return is_match


def any_option_detector(options: list[str | tuple[str, str]]) -> Callable[[str], bool]:
    """
    Get a function that returns true if it sees any of the long
    options or long or short option pairs.
    """
    detectors = [
        option_detector(o) if isinstance(o, str) else option_detector(*o)
        for o in options
    ]

    def is_match(option: str) -> bool:
        for detector in detectors:
            if detector(option):
                return True
        return False

    return is_match


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

            :param time_limit: Time limit in seconds.
            """

            if time_limit is None:
                # Just use Slurm's default
                return None

            winning_partition = None
            for partition in self.all_partitions:
                if partition.time_limit < time_limit:
                    # Can't use this
                    continue
                if winning_partition is None:
                    # Anything beats None
                    winning_partition = partition
                    continue
                if partition.gres and not winning_partition.gres:
                    # Never use a partition witn GRES if you can avoid it
                    continue
                elif not partition.gres and winning_partition.gres:
                    # Never keep a partition with GRES if we find one without
                    winning_partition = partition
                    continue
                if partition.priority > winning_partition.priority:
                    # After that, don't raise priority
                    continue
                elif partition.priority < winning_partition.priority:
                    # And always lower it
                    winning_partition = partition
                    continue
                if partition.time_limit < winning_partition.time_limit:
                    # Finally, lower time limit
                    winning_partition = partition

            # TODO: Store partitions in a better indexed way
            if winning_partition is None and len(self.all_partitions) > 0:
                # We have partitions and none of them can fit this
                raise RuntimeError(
                    f"Could not find a Slurm partition that can fit a job that runs for {time_limit} seconds"
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

            :param batch_job_id_list: list of Job ID strings, where each string
                has the form ``<job>[.<task>]``.

            :return: list of job exit codes or exit code, exit reason pairs
                associated with the list of job IDs.

            :raises CalledProcessErrorStderr: if communicating with Slurm went
                wrong.

            :raises OSError: if job details are not available because a Slurm
                command could not start.
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
            :return: dict of job statuses, where key is the integer job ID, and
                value is a tuple containing the job's state and exit code.
            :raises CalledProcessErrorStderr: if communicating with Slurm went
                wrong.
            :raises OSError: if job details are not available because a Slurm
                command could not start.
            """

            status_dict = {}
            scontrol_problem: Exception | None = None

            try:
                # Get all the job details we can from scontrol, which we think
                # might be faster/less dangerous than sacct searching, even
                # though it can't be aimed at more than one job.
                status_dict.update(self._getJobDetailsFromScontrol(job_id_list))
            except (CalledProcessErrorStderr, OSError) as e:
                if isinstance(e, OSError):
                    logger.warning("Could not run scontrol: %s", e)
                else:
                    logger.warning("Error from scontrol: %s", e)
                scontrol_problem = e

            logger.debug("After scontrol, got statuses: %s", status_dict)

            # See what's not handy in scontrol (or everything if we couldn't
            # call it).
            sacct_job_id_list = self._remaining_jobs(job_id_list, status_dict)

            logger.debug("Remaining jobs to find out about: %s", sacct_job_id_list)

            try:
                # Ask sacct about those jobs
                status_dict.update(self._getJobDetailsFromSacct(sacct_job_id_list))
            except (CalledProcessErrorStderr, OSError) as e:
                if isinstance(e, OSError):
                    logger.warning("Could not run sacct: %s", e)
                else:
                    logger.warning("Error from sacct: %s", e)
                if scontrol_problem is not None:
                    # Neither approach worked at all
                    raise

            # One of the methods worked, so we have at least (None, None)
            # values filled in for all jobs.
            assert len(status_dict) == len(job_id_list)

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

        def _remaining_jobs(
            self,
            job_id_list: list[int],
            job_details: dict[int, tuple[str | None, int | None]],
        ) -> list[int]:
            """
            Given a list of job IDs and a list of job details (state and exit
            code), get the list of job IDs where the details are (None, None)
            (or are missing).
            """
            return [
                j
                for j in job_id_list
                if job_details.get(j, (None, None)) == (None, None)
            ]

        def _getJobDetailsFromSacct(
            self,
            job_id_list: list[int],
        ) -> dict[int, tuple[str | None, int | None]]:
            """
            Get SLURM job exit codes for the jobs in `job_id_list` by running `sacct`.

            Handles querying manageable time periods until all jobs have information.

            There is no guarantee of inter-job consistency: one job may really
            finish after another, but we might see the earlier-finishing job
            still running and the later-finishing job finished.

            :param job_id_list: list of integer batch job IDs.
            :return: dict of job statuses, where key is the job-id, and value
                is a tuple containing the job's state and exit code. Jobs with
                no information reported from Slurm will have (None, None).
            """

            # Pick a now
            now = datetime.now().astimezone(None)
            # Decide when to start the search (first copy of past midnight)
            begin_time = now.replace(hour=0, minute=0, second=0, microsecond=0, fold=0)
            # And when to end (a day after that)
            end_time = begin_time + timedelta(days=1)
            while end_time < now:
                # If something goes really weird, advance up to our chosen now
                end_time += timedelta(days=1)
            # If we don't go around the loop at least once, we might end up
            # with an empty dict being returned, which shouldn't happen. We
            # need the (None, None) entries for jobs we can't find.
            assert end_time >= self.boss.start_time

            results: dict[int, tuple[str | None, int | None]] = {}

            while len(job_id_list) > 0 and end_time >= self.boss.start_time:
                # There are still jobs to look for and our search isn't
                # exclusively for stuff that only existed before our workflow
                # started.
                results.update(
                    self._get_job_details_from_sacct_for_range(
                        job_id_list, begin_time, end_time
                    )
                )
                job_id_list = self._remaining_jobs(job_id_list, results)
                # If we have to search again, search the previous day. But
                # overlap a tiny bit so the endpoints don't exactly match, in
                # case Slurm is not working with inclusive intervals.
                # TODO: is Slurm working with inclusive intervals?
                end_time = begin_time + timedelta(seconds=1)
                begin_time = end_time - timedelta(days=1, seconds=1)

            if end_time < self.boss.start_time and len(job_id_list) > 0:
                # This is suspicious.
                logger.warning(
                    "Could not find any information from sacct after "
                    "workflow start at %s about jobs: %s",
                    self.boss.start_time.isoformat(),
                    job_id_list,
                )

            return results

        def _get_job_details_from_sacct_for_range(
            self,
            job_id_list: list[int],
            begin_time: datetime,
            end_time: datetime,
        ) -> dict[int, tuple[str | None, int | None]]:
            """
            Get SLURM job exit codes for the jobs in `job_id_list` by running `sacct`.

            Internally, Slurm's accounting thinks in wall clock time, so for
            efficiency you need to only search relevant real-time periods.

            :param job_id_list: list of integer batch job IDs.
            :param begin_time: An aware datetime of the earliest time to search
            :param end_time: An aware datetime of the latest time to search
            :return: dict of job statuses, where key is the job-id, and value
                is a tuple containing the job's state and exit code. Jobs with
                no information reported from Slurm will have (None, None).
            """

            assert begin_time.tzinfo is not None, "begin_time must be aware"
            assert end_time.tzinfo is not None, "end_time must be aware"

            def stringify(t: datetime) -> str:
                """
                Convert an aware time local time, and format it *without* a
                trailing time zone indicator.
                """
                # TODO: What happens when we get an aware time that's ambiguous
                # in local time? Or when the local timezone changes while we're
                # sending things to Slurm or doing a progressive search back?
                naive_t = t.astimezone(None).replace(tzinfo=None)
                return naive_t.isoformat(timespec="seconds")

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
                stringify(begin_time),
                "-E",
                stringify(end_time),
            ]

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
                    job_statuses.update(
                        self._get_job_details_from_sacct_for_range(
                            job_id_list[: len(job_id_list) // 2],
                            begin_time,
                            end_time,
                        )
                    )
                    job_statuses.update(
                        self._get_job_details_from_sacct_for_range(
                            job_id_list[len(job_id_list) // 2 :],
                            begin_time,
                            end_time,
                        )
                    )
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

            is_any_mem_option = any_option_detector(
                ["mem", "mem-per-cpu", "mem-per-gpu"]
            )
            is_any_cpus_option = any_option_detector(
                [("cpus-per-task", "c"), "cpus-per-gpu"]
            )
            is_export_option = option_detector("export")
            is_export_file_option = option_detector("export-file")
            is_time_option = option_detector("time", "t")
            is_partition_option = option_detector("partition", "p")

            # We will fill these in with stuff parsed from TOIL_SLURM_ARGS, or
            # with our own determinations if they aren't there.

            # --export=[ALL,]<environment_toil_variables>
            export_all = True
            export_list = []  # Some items here may be multiple comma-separated values
            time_limit: int | None = self.boss.config.slurm_time  # type: ignore[attr-defined]
            partition: str | None = None

            if nativeConfig is not None:
                logger.debug(
                    "Native SLURM options appended to sbatch: %s", nativeConfig
                )

                # Do a mini argument parse to pull out export and parse time if
                # needed
                args = shlex.split(nativeConfig)
                i = 0
                while i < len(args):
                    arg = args[i]
                    if is_any_mem_option(arg) or is_any_cpus_option(arg):
                        # Prohibit arguments that set CPUs or memory
                        raise ValueError(
                            f"Cannot use Slurm argument {arg} which conflicts "
                            f"with Toil's own arguments to Slurm"
                        )
                    elif is_export_option(arg):
                        # Capture the export argument value so we can modify it
                        export_all = False
                        if "=" not in arg:
                            if i + 1 >= len(args):
                                raise ValueError(
                                    f"No value supplied for Slurm {arg} argument"
                                )
                            i += 1
                            export_list.append(args[i])
                        else:
                            export_list.append(arg.split("=", 1)[1])
                    elif is_export_file_option(arg):
                        # Keep --export-file but turn off --export=ALL in that
                        # case.
                        export_all = False
                        sbatch_line.append(arg)
                    elif is_time_option(arg):
                        # Capture the time limit in seconds so we can use it for picking a partition
                        if "=" not in arg:
                            if i + 1 >= len(args):
                                raise ValueError(
                                    f"No value supplied for Slurm {arg} argument"
                                )
                            i += 1
                            time_string = args[i]
                        else:
                            time_string = arg.split("=", 1)[1]
                        time_limit = parse_slurm_time(time_string)
                    elif is_partition_option(arg):
                        # Capture the partition so we can run checks on it and know not to assign one
                        if "=" not in arg:
                            if i + 1 >= len(args):
                                raise ValueError(
                                    f"No value supplied for Slurm {arg} argument"
                                )
                            i += 1
                            partition = args[i]
                        else:
                            partition = arg.split("=", 1)[1]
                    else:
                        # Other arguments pass through.
                        sbatch_line.append(arg)
                    i += 1

            if export_all:
                # We don't have any export overrides so we ened to start with
                # an ALL
                export_list.append("ALL")

            if environment:
                argList = []

                for k, v in environment.items():
                    # TODO: The sbatch man page doesn't say we can quote these;
                    # if we need to send characters like , itself we need to
                    # use --export-file and clean it up when the command has
                    # been issued.
                    quoted_value = shlex.quote(os.environ[k] if v is None else v)
                    argList.append(f"{k}={quoted_value}")

                export_list.extend(argList)

            # If partition isn't set and we have a GPU partition override
            # that applies, apply it
            gpu_partition_override: str | None = self.boss.config.slurm_gpu_partition  # type: ignore[attr-defined]
            if partition is None and gpus and gpu_partition_override:
                partition = gpu_partition_override

            # If partition isn't set and we have a parallel partition override
            # that applies, apply it
            parallel_env: str | None = self.boss.config.slurm_pe  # type: ignore[attr-defined]
            if partition is None and cpu and cpu > 1 and parallel_env:
                partition = parallel_env

            # If partition isn't set and we have a general partition override
            # that applies, apply it
            partition_override: str | None = self.boss.config.slurm_partition  # type: ignore[attr-defined]
            if partition is None and partition_override:
                partition = partition_override

            if partition is None and gpus:
                # Send to a GPU partition
                gpu_partition = self.boss.partitions.default_gpu_partition
                if gpu_partition is None:
                    # no gpu partitions are available, raise an error
                    raise RuntimeError(
                        f"The job {jobName} is requesting GPUs, but the Slurm cluster does not appear to have an accessible partition with GPUs"
                    )
                if time_limit is not None and gpu_partition.time_limit < time_limit:
                    # TODO: find the lowest-priority GPU partition that has at least each job's time limit!
                    logger.warning(
                        "Trying to submit a job that needs %s seconds to partition %s that has a limit of %s seconds",
                        time_limit,
                        gpu_partition.partition_name,
                        gpu_partition.time_limit,
                    )
                partition = gpu_partition.partition_name

            if partition is None:
                # Pick a partition based on time limit
                partition = self.boss.partitions.get_partition(time_limit)

            # Now generate all the arguments
            if len(export_list) > 0:
                # add --export to the sbatch
                sbatch_line.append("--export=" + ",".join(export_list))
            if partition is not None:
                sbatch_line.append(f"--partition={partition}")
            if gpus:
                # Generate GPU assignment argument
                sbatch_line.append(f"--gres=gpu:{gpus}")
                if (
                    partition is not None
                    and partition not in self.boss.partitions.gpu_partitions
                ):
                    # the specified partition is not compatible, so warn the user that the job may not work
                    logger.warning(
                        f"Job {jobName} needs GPUs, but specified partition {partition} does not have them. This job may not work."
                        f"Try specifying one of these partitions instead: {', '.join(self.boss.partitions.gpu_partitions)}."
                    )
            if mem is not None and self.boss.config.slurm_allocate_mem:  # type: ignore[attr-defined]
                # memory passed in is in bytes, but slurm expects megabytes
                sbatch_line.append(f"--mem={math.ceil(mem / 2 ** 20)}")
            if cpu is not None:
                sbatch_line.append(f"--cpus-per-task={math.ceil(cpu)}")
            if time_limit is not None:
                # Put all the seconds in the seconds slot
                sbatch_line.append(f"--time=0:{time_limit}")

            stdoutfile: str = self.boss.format_std_out_err_path(jobID, "%j", "out")
            stderrfile: str = self.boss.format_std_out_err_path(jobID, "%j", "err")
            sbatch_line.extend(["-o", stdoutfile, "-e", stderrfile])

            return sbatch_line

    def __init__(
        self, config: Config, maxCores: float, maxMemory: float, maxDisk: float
    ) -> None:
        super().__init__(config, maxCores, maxMemory, maxDisk)
        self.partitions = SlurmBatchSystem.PartitionSet()
        # Record when the workflow started, so we know when to stop looking for
        # jobs we ran.
        self.start_time = datetime.now().astimezone(None)

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
            "--slurmPartition",
            dest="slurm_partition",
            default=None,
            env_var="TOIL_SLURM_PARTITION",
            help="Partition to send Slurm jobs to.",
        )
        parser.add_argument(
            "--slurmGPUPartition",
            dest="slurm_gpu_partition",
            default=None,
            env_var="TOIL_SLURM_GPU_PARTITION",
            help="Partition to send Slurm jobs to if they ask for GPUs.",
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
        setOption("slurm_partition")
        setOption("slurm_gpu_partition")
        setOption("slurm_pe")
        setOption("slurm_args")
