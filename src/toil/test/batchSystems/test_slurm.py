import errno
import logging
import sys
import textwrap
from datetime import datetime, timedelta
from queue import Queue
from unittest import mock

import pytest

import toil.batchSystems.slurm
from toil.batchSystems.abstractBatchSystem import (
    EXIT_STATUS_UNAVAILABLE_VALUE,
    BatchJobExitReason,
    BatchSystemSupport,
    InsufficientSystemResources,
)
from toil.common import Config
from toil.job import Requirer
from toil.lib.misc import CalledProcessErrorStderr
from toil.worker import WALLTIME_SIGNAL, WALLTIME_EXIT_CODE

from toil.test import ToilTest

logger = logging.getLogger(__name__)


# TODO: Come up with a better way to mock the commands then monkey-patching the
# command-calling functions.

# To convincingly test jobs in the past relative to the time Toil goes looking
# for them, we give our fake jobs times relative to the module load time, which
# we hope is not days and days away from the time the tests actually run.
JOB_BASE_TIME = datetime.now().astimezone(None) - timedelta(days=5)


def call_either(args, **_) -> str:
    """
    Pretend to call either sacct or scontrol as appropriate.
    """
    if args[0] == "sacct":
        return call_sacct(args)
    elif args[0] == "scontrol":
        return call_scontrol(args)
    else:
        raise RuntimeError(f"Cannot fake command call: {args}")


def call_sacct(args, **_) -> str:
    """
    The arguments passed to `call_command` when executing `sacct` are something like:
    ['sacct', '-n', '-j', '<comma-separated list of job-ids>', '--format',
    'JobIDRaw,State,ExitCode', '-P', '-S', '1970-01-01']
    The multi-line output is something like::

        1234|COMPLETED|0:0
        1234.batch|COMPLETED|0:0
        1235|PENDING|0:0
        1236|FAILED|0:2
        1236.extern|COMPLETED|0:0
    """
    logger.info("sacct call: %s", args)
    if sum(len(a) for a in args) > 1000:
        # Simulate if the argument list is too long
        raise OSError(errno.E2BIG, "Argument list is too long")
    # Fake output per fake job-id.
    sacct_info = {
        609663: "609663|FAILED|0:2\n609663.extern|COMPLETED|0:0\n",
        754725: "754725|TIMEOUT|0:0\n754725.extern|COMPLETED|0:0\n754725.0|COMPLETED|0:0\n",
        765096: "765096|FAILED|0:9\n765096.extern|COMPLETED|0:0\n765096.0|CANCELLED by 54386|0:9\n",
        767925: "767925|FAILED|2:0\n767925.extern|COMPLETED|0:0\n767925.0|FAILED|2:0\n",
        785023: "785023|FAILED|127:0\n785023.batch|FAILED|127:0\n785023.extern|COMPLETED|0:0\n",
        789456: "789456|FAILED|1:0\n",
        789457: f"789457|FAILED|{WALLTIME_EXIT_CODE}:0\n789457.extern|COMPLETED|0:0\n",
        789724: "789724|RUNNING|0:0\n789724.batch|RUNNING|0:0\n789724.extern|RUNNING|0:0\n",
        789868: "789868|PENDING|0:0\n",
        789869: "789869|COMPLETED|0:0\n789869.batch|COMPLETED|0:0\n789869.extern|COMPLETED|0:0\n",
    }
    # And time we say the job was at
    job_time = {
        609663: JOB_BASE_TIME + timedelta(days=1),
        754725: JOB_BASE_TIME + timedelta(days=1),
        765096: JOB_BASE_TIME + timedelta(days=2),
        767925: JOB_BASE_TIME + timedelta(days=2),
        785023: JOB_BASE_TIME + timedelta(days=3),
        789456: JOB_BASE_TIME + timedelta(days=3),
        789457: JOB_BASE_TIME + timedelta(days=3),
        789724: JOB_BASE_TIME + timedelta(days=4),
        789868: JOB_BASE_TIME + timedelta(days=4),
        789869: JOB_BASE_TIME + timedelta(days=4),
    }

    # See if they asked for a job list
    try:
        j_index = args.index("-j")
        job_ids = [int(job_id) for job_id in args[j_index + 1].split(",")]
    except ValueError:
        # We're not restricting to a list of jobs.
        job_ids = list(sacct_info.keys())
    # See if they asked for start or end times
    try:
        flag_index = args.index("-S")
        begin_time = datetime.fromisoformat(args[flag_index + 1]).astimezone(None)
    except ValueError:
        # By default, Slurm uses today at midnight
        begin_time = (
            datetime.now()
            .astimezone(None)
            .replace(hour=0, minute=0, second=0, microsecond=0, fold=0)
        )
    try:
        flag_index = args.index("-E")
        end_time = datetime.fromisoformat(args[flag_index + 1]).astimezone(None)
    except ValueError:
        end_time = None

    stdout = ""
    # Glue the fake outputs for the request job-ids together in a single string
    for job_id in job_ids:
        if job_id not in sacct_info:
            # Not a job we know of.
            continue
        if begin_time is not None and begin_time > job_time[job_id]:
            # Skip this job as being too early
            continue
        if end_time is not None and end_time < job_time[job_id]:
            # Skip this job as being too late
            continue
        stdout += sacct_info[job_id]
    return stdout


def call_scontrol(args, **_) -> str:
    """
    The arguments passed to `call_command` when executing `scontrol` are:
    ``['scontrol', 'show', 'job']`` or ``['scontrol', 'show', 'job', '<job-id>']``
    """
    logger.info("scontrol call: %s", args)
    job_id = int(args[3]) if len(args) > 3 else None
    # Fake output per fake job-id.
    # scontrol only shows recent jobs, so we have fewer/different jobs here
    # than for sacct.
    scontrol_info = {
        787204: textwrap.dedent(
            """\
            JobId=787204 JobName=toil_job_6_CWLJob
               UserId=rapthor-mloose(54386) GroupId=rapthor-mloose(54038) MCS_label=N/A
               Priority=11067 Nice=0 Account=rapthor QOS=normal
               JobState=COMPLETED Reason=None Dependency=(null)
               Requeue=0 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0
               RunTime=00:00:05 TimeLimit=5-00:00:00 TimeMin=N/A
               SubmitTime=2021-10-11T17:20:42 EligibleTime=2021-10-11T17:20:42
               AccrueTime=2021-10-11T17:20:42
               StartTime=2021-10-11T17:20:43 EndTime=2021-10-11T17:20:48 Deadline=N/A
               SuspendTime=None SecsPreSuspend=0 LastSchedEval=2021-10-11T17:20:43
               Partition=normal AllocNode:Sid=batch-01:1912150
               ReqNodeList=(null) ExcNodeList=(null)
               NodeList=wn-hb-01
               BatchHost=wn-hb-01
               NumNodes=1 NumCPUs=1 NumTasks=0 CPUs/Task=1 ReqB:S:C:T=0:0:*:*
               TRES=cpu=1,mem=2G,node=1,billing=1
               Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=*
               MinCPUsNode=1 MinMemoryNode=2G MinTmpDiskNode=0
               Features=(null) DelayBoot=00:00:00
               OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null)
               Command=(null)
               WorkDir=/home/rapthor-mloose/code/toil/cwl-v1.2
               StdErr=/home/rapthor-mloose/code/toil/cwl-v1.2/tmp/toil_19512746-a9f4-4b99-b9ff-48ca5c1b661c.6.787204.err.log
               StdIn=/dev/null
               StdOut=/home/rapthor-mloose/code/toil/cwl-v1.2/tmp/toil_19512746-a9f4-4b99-b9ff-48ca5c1b661c.6.787204.out.log
               Power=
               NtasksPerTRES:0
            """
        ),
        789724: textwrap.dedent(
            """\
            JobId=789724 JobName=run_prefactor-cwltool.sh
               UserId=rapthor-mloose(54386) GroupId=rapthor-mloose(54038) MCS_label=N/A
               Priority=7905 Nice=0 Account=rapthor QOS=normal
               JobState=RUNNING Reason=None Dependency=(null)
               Requeue=0 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0
               RunTime=17:22:59 TimeLimit=5-00:00:00 TimeMin=N/A
               SubmitTime=2021-10-14T17:37:17 EligibleTime=2021-10-14T17:37:17
               AccrueTime=2021-10-14T17:37:17
               StartTime=2021-10-14T17:37:18 EndTime=2021-10-19T17:37:18 Deadline=N/A
               SuspendTime=None SecsPreSuspend=0 LastSchedEval=2021-10-14T17:37:18
               Partition=normal AllocNode:Sid=batch-01:2814774
               ReqNodeList=(null) ExcNodeList=wn-ca-[01-02],wn-db-[01-06]
               NodeList=wn-ha-01
               BatchHost=wn-ha-01
               NumNodes=1 NumCPUs=20 NumTasks=1 CPUs/Task=20 ReqB:S:C:T=0:0:*:*
               TRES=cpu=20,mem=160000M,node=1,billing=20
               Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=*
               MinCPUsNode=20 MinMemoryCPU=8000M MinTmpDiskNode=0
               Features=(null) DelayBoot=00:00:00
               OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null)
               Command=/project/rapthor/Software/prefactor/sbin/run_prefactor-cwltool.sh L721962 HBA_target
               WorkDir=/project/rapthor/Share/prefactor/L721962
               StdErr=/project/rapthor/Share/prefactor/L721962/slurm-789724.out
               StdIn=/dev/null
               StdOut=/project/rapthor/Share/prefactor/L721962/slurm-789724.out
               Power=
               NtasksPerTRES:0
            """
        ),
        789728: textwrap.dedent(
            """\
            JobId=789728 JobName=sleep.sh
               UserId=rapthor-mloose(54386) GroupId=rapthor-mloose(54038) MCS_label=N/A
               Priority=8005 Nice=0 Account=rapthor QOS=normal
               JobState=PENDING Reason=ReqNodeNotAvail,_UnavailableNodes:wn-db-05 Dependency=(null)
               Requeue=0 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0
               RunTime=00:00:00 TimeLimit=5-00:00:00 TimeMin=N/A
               SubmitTime=2021-10-14T18:08:11 EligibleTime=2021-10-14T18:08:11
               AccrueTime=2021-10-14T18:08:11
               StartTime=Unknown EndTime=Unknown Deadline=N/A
               SuspendTime=None SecsPreSuspend=0 LastSchedEval=2021-10-15T11:00:07
               Partition=normal AllocNode:Sid=batch-01:2814774
               ReqNodeList=wn-db-05 ExcNodeList=(null)
               NodeList=(null)
               NumNodes=1 NumCPUs=1 NumTasks=1 CPUs/Task=1 ReqB:S:C:T=0:0:*:*
               TRES=cpu=1,mem=8000M,node=1,billing=1
               Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=*
               MinCPUsNode=1 MinMemoryCPU=8000M MinTmpDiskNode=0
               Features=(null) DelayBoot=00:00:00
               OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null)
               Command=/home/rapthor-mloose/tmp/sleep.sh
               WorkDir=/home/rapthor-mloose/tmp
               StdErr=/home/rapthor-mloose/tmp/slurm-789728.out
               StdIn=/dev/null
               StdOut=/home/rapthor-mloose/tmp/slurm-789728.out
               Power=
               NtasksPerTRES:0
            """
        ),
        790001: textwrap.dedent(
            """\
            JobId=790001 JobName=somebody_elses_job
               UserId=someone(1000) GroupId=someone(1000) MCS_label=N/A
               Comment=first line of a comment
            
            second line of the comment, after a blank line
               JobState=RUNNING Reason=None Dependency=(null)
               Requeue=0 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0
            """
        ),
    }
    if job_id is not None:
        try:
            stdout = scontrol_info[job_id]
        except KeyError:
            raise CalledProcessErrorStderr(
                1, "slurm_load_jobs error: Invalid job id specified"
            )
    else:
        # Glue the fake outputs for the request job-ids together in a single string
        stdout = ""
        for value in scontrol_info.values():
            stdout += value + "\n"
    return stdout

def call_scontrol_no_jobs(args, **_) -> str:
    """
    Return the output scontrol would produce if called when no jobs exist.
    """

    return "No jobs in the system\n"


def call_sacct_raises(*_):
    """
    Fake that the `sacct` command fails by raising a `CalledProcessErrorStderr`
    """
    raise CalledProcessErrorStderr(
        1, "sacct: error: Problem talking to the database: " "Connection timed out"
    )


def call_sinfo(*_) -> str:
    """
    Simulate asking for partition info from Slurm
    """
    stdout = textwrap.dedent(
        """\
        PARTITION GRES TIMELIMIT PRIO_TIER CPUS MEMORY
        short* (null) 1:00:00 500 256+ 1996800+
        medium (null) 12:00:00 500 256+ 1996800+
        long (null) 14-00:00:00 500 256+ 1996800+
        gpu gpu:A100:8 7-00:00:00 5000 256 996800
        gpu gpu:A5500:8 7-00:00:00 5000 256 1996800
        high_priority gpu:A5500:8 7-00:00:00 65000 256 1996800
        high_priority (null) 7-00:00:00 65000 256+ 1996800+
        simple_nodelist gpu:A100:8 1:00 65000 256 996800
        simple_nodelist gpu:A5500:8 1:00 65000 256 1996800
        simple_nodelist (null) 1:00 65000 256+ 1996800+
        """
    )
    return stdout


class FakeBatchSystem(BatchSystemSupport):
    """
    Class that implements a minimal Batch System, needed to create a Worker (see below).
    """

    def __init__(self):
        super().__init__(self.__fake_config(), float("inf"), sys.maxsize, sys.maxsize)
        # Pretend to be a workflow that started before we pretend the jobs
        # we pretend to have ran.
        self.start_time = JOB_BASE_TIME - timedelta(hours=2)

    def getWaitDuration(self):
        return 10

    def __fake_config(self):
        """
        Returns a dummy config for the batch system tests.  We need a workflowID to be set up
        since we are running tests without setting up a jobstore. This is the class version
        to be used when an instance is not available.

        :rtype: toil.common.Config
        """
        config = Config()
        from uuid import uuid4

        config.workflowID = str(uuid4())
        config.cleanWorkDir = "always"
        toil.batchSystems.slurm.SlurmBatchSystem.setOptions(
            lambda o: setattr(config, o, None)
        )
        return config


# Make the mock class not have abstract methods anymore, even though we don't
# implement them. See <https://stackoverflow.com/a/17345619>.
FakeBatchSystem.__abstractmethods__ = set()


class SlurmTest(ToilTest):
    """
    Class for unit-testing SlurmBatchSystem
    """

    def setUp(self):
        self.monkeypatch = pytest.MonkeyPatch()
        self.worker = toil.batchSystems.slurm.SlurmBatchSystem.GridEngineThread(
            newJobsQueue=Queue(),
            updatedJobsQueue=Queue(),
            killQueue=Queue(),
            killedJobsQueue=Queue(),
            boss=FakeBatchSystem(),
        )

    ####
    #### tests for _getJobDetailsFromSacct()
    ####

    def test_getJobDetailsFromSacct_one_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
        expected_result = {785023: ("FAILED", 127)}
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobDetailsFromSacct_one_not_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
        expected_result = {1234: (None, None)}
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobDetailsFromSacct_many_all_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
        expected_result = {
            754725: ("TIMEOUT", 0),
            789456: ("FAILED", 1),
            789724: ("RUNNING", 0),
            789868: ("PENDING", 0),
            789869: ("COMPLETED", 0),
        }
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobDetailsFromSacct_many_some_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
        expected_result = {
            609663: ("FAILED", 130),
            767925: ("FAILED", 2),
            1234: (None, None),
            1235: (None, None),
            765096: ("FAILED", 137),
        }
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobDetailsFromSacct_many_none_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
        expected_result = {1234: (None, None), 1235: (None, None), 1236: (None, None)}
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobDetailsFromSacct_argument_list_too_big(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
        expected_result = {i: (None, None) for i in range(2000)}
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    ####
    #### tests for _getJobDetailsFromScontrol()
    ####

    def test_getJobDetailsFromScontrol_one_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol)
        expected_result = {789724: ("RUNNING", 0)}
        result = self.worker._getJobDetailsFromScontrol(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobDetailsFromScontrol_one_not_exists(self):
        """
        Asking for the job details of a single job that `scontrol` doesn't know about should
        raise an exception.
        """
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol)
        expected_result = {1234: (None, None)}
        try:
            _ = self.worker._getJobDetailsFromScontrol(list(expected_result))
        except CalledProcessErrorStderr:
            pass
        else:
            assert False, "Expected exception CalledProcessErrorStderr"

    def test_getJobDetailsFromScontrol_many_all_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol)
        expected_result = {
            787204: ("COMPLETED", 0),
            789724: ("RUNNING", 0),
            789728: ("PENDING", 0),
        }
        result = self.worker._getJobDetailsFromScontrol(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobDetailsFromScontrol_many_some_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol)
        expected_result = {
            787204: ("COMPLETED", 0),
            789724: ("RUNNING", 0),
            1234: (None, None),
        }
        result = self.worker._getJobDetailsFromScontrol(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobDetailsFromScontrol_many_none_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol)
        expected_result = {1234: (None, None), 1235: (None, None), 1236: (None, None)}
        result = self.worker._getJobDetailsFromScontrol(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobDetailsFromScontrol_one_unescaped_newlines(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol)
        expected_result = {
            790001: ("RUNNING", 0),
        }
        result = self.worker._getJobDetailsFromScontrol(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobDetailsFromScontrol_no_jobs(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol_no_jobs)
        expected_result = {1234: (None, None), 1235: (None, None), 1236: (None, None)}
        result = self.worker._getJobDetailsFromScontrol(list(expected_result))
        assert result == expected_result, f"{result} != {expected_result}"

    ###
    ### tests for getJobExitCode
    ###

    def test_getJobExitCode_job_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_id = "785023"  # FAILED
        expected_result = (127, BatchJobExitReason.FAILED)
        result = self.worker.getJobExitCode(job_id)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobExitCode_job_timed_out(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_id = "754725"  # TIMEOUT
        expected_result = (EXIT_STATUS_UNAVAILABLE_VALUE, BatchJobExitReason.TIMELIMIT)
        result = self.worker.getJobExitCode(job_id)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobExitCode_job_reported_timeout(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_id = "789457"  # FAILED with the WALLTIME_EXIT_CODE exit code
        expected_result = (
            WALLTIME_EXIT_CODE,
            BatchJobExitReason.TIMELIMIT,
        )
        result = self.worker.getJobExitCode(job_id)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobExitCode_job_not_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_id = "1234"  # Non-existent
        expected_result = None
        result = self.worker.getJobExitCode(job_id)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobExitCode_sacct_raises_job_exists(self):
        """
        This test forces the use of `scontrol` to get job information, by letting `sacct`
        raise an exception.
        """
        self.monkeypatch.setattr(
            self.worker, "_getJobDetailsFromSacct", call_sacct_raises
        )
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_id = "787204"  # COMPLETED
        expected_result = (0, BatchJobExitReason.FINISHED)
        result = self.worker.getJobExitCode(job_id)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobExitCode_sacct_raises_job_not_exists(self):
        """
        This test forces the use of `scontrol` to get job information, by letting `sacct`
        raise an exception. Next, `scontrol` should also raise because it doesn't know the job.
        """
        self.monkeypatch.setattr(
            self.worker, "_getJobDetailsFromSacct", call_sacct_raises
        )
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_id = "1234"  # Non-existent
        try:
            _ = self.worker.getJobExitCode(job_id)
        except CalledProcessErrorStderr:
            pass
        else:
            assert False, "Exception CalledProcessErrorStderr not raised"

    ###
    ### Tests for coalesce_job_exit_codes
    ###

    def test_coalesce_job_exit_codes_one_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_ids = ["785023"]  # FAILED
        expected_result = [(127, BatchJobExitReason.FAILED)]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_coalesce_job_exit_codes_one_not_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_ids = ["1234"]  # Non-existent
        expected_result = [None]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_coalesce_job_exit_codes_many_all_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_ids = [
            "754725",  # TIMEOUT,
            "789456",  # FAILED,
            "789724",  # RUNNING,
            "789868",  # PENDING,
            "789869",
        ]  # COMPLETED
        # RUNNING and PENDING jobs should return None
        expected_result = [
            (EXIT_STATUS_UNAVAILABLE_VALUE, BatchJobExitReason.TIMELIMIT),
            (1, BatchJobExitReason.FAILED),
            None,
            None,
            (0, BatchJobExitReason.FINISHED),
        ]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_coalesce_job_exit_codes_mix_sacct_scontrol(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_ids = [
            "609663",  # FAILED, in sacct only
            "789869",  # COMPLETED, in sacct only
            "787204",  # COMPLETED, in scontrol only
            "789724",  # RUNNING, in scontrol only
        ]
        # RUNNING and PENDING jobs should return None
        expected_result = [
            (130, BatchJobExitReason.FAILED),
            (0, BatchJobExitReason.FINISHED),
            (0, BatchJobExitReason.FINISHED),
            None,
        ]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_coalesce_job_exit_codes_some_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_ids = [
            "609663",  # FAILED (SIGINT)
            "767925",  # FAILED,
            "789724",  # RUNNING,
            "999999",  # Non-existent,
            "789869",
        ]  # COMPLETED
        # RUNNING job should return None
        expected_result = [
            (130, BatchJobExitReason.FAILED),
            (2, BatchJobExitReason.FAILED),
            None,
            None,
            (0, BatchJobExitReason.FINISHED),
        ]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_coalesce_job_exit_codes_sacct_raises_job_exists(self):
        """
        This test forces the use of `scontrol` to get job information, by letting `sacct`
        raise an exception.
        """
        self.monkeypatch.setattr(
            self.worker, "_getJobDetailsFromSacct", call_sacct_raises
        )
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_ids = ["787204"]  # COMPLETED
        expected_result = [(0, BatchJobExitReason.FINISHED)]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_coalesce_job_exit_codes_sacct_raises_job_not_exists(self):
        """
        This test forces the use of `scontrol` to get job information, by letting `sacct`
        raise an exception. Next, `scontrol` should also raise because it doesn't know the job.
        """
        self.monkeypatch.setattr(
            self.worker, "_getJobDetailsFromSacct", call_sacct_raises
        )
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_either)
        job_ids = ["1234"]  # Non-existent
        try:
            _ = self.worker.coalesce_job_exit_codes(job_ids)
        except CalledProcessErrorStderr:
            pass
        else:
            assert False, "Exception CalledProcessErrorStderr not raised"

    ###
    ### Tests for partition selection
    ##

    def test_PartitionSet_get_partition(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sinfo)
        ps = toil.batchSystems.slurm.SlurmBatchSystem.PartitionSet()

        # At zero. short will win because simple_nodelist has higher priority.
        self.assertEqual(ps.get_partition(0), "short")
        # Easily within the partition
        self.assertEqual(ps.get_partition(10 * 60), "short")
        # Exactly on the boundary
        self.assertEqual(ps.get_partition(60 * 60), "short")
        # Well within the next partition
        self.assertEqual(ps.get_partition(2 * 60 * 60), "medium")
        # Can only fit in long
        self.assertEqual(ps.get_partition(8 * 24 * 60 * 60), "long")
        # Could fit in gpu or long
        self.assertEqual(ps.get_partition(6 * 24 * 60 * 60), "long")
        # Can't fit in anything
        with self.assertRaises(Exception):
            ps.get_partition(365 * 24 * 60 * 60)

    def test_PartitionSet_default_gpu_partition(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sinfo)
        ps = toil.batchSystems.slurm.SlurmBatchSystem.PartitionSet()

        # Make sure we picked the useful-length GPU partition and not the super
        # short one.
        self.assertEqual(ps.default_gpu_partition.partition_name, "gpu")

    def test_prepareSbatch_partition(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sinfo)
        ps = toil.batchSystems.slurm.SlurmBatchSystem.PartitionSet()
        self.worker.boss.partitions = ps
        # This is in seconds
        self.worker.boss.config.slurm_time = 30

        # Without a partition override in the environment, we should get the
        # "short" partition for this job
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--partition=short" in command

        # With a partition override, we should not. But the override will be rewritten.
        self.worker.boss.config.slurm_args = (
            "--something --partition foo --somethingElse"
        )
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--partition=short" not in command
        assert "--partition=foo" in command

        # All ways of setting partition should work, including =
        self.worker.boss.config.slurm_args = (
            "--something --partition=foo --somethingElse"
        )
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--partition=short" not in command
        assert "--partition=foo" in command

        # And short options
        self.worker.boss.config.slurm_args = "--something -p foo --somethingElse"
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--partition=short" not in command
        assert "--partition=foo" in command

        # Partition settings from the config should override automatic selection
        self.worker.boss.config.slurm_partition = "foobar"
        self.worker.boss.config.slurm_args = "--something --somethingElse"
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--partition=foobar" in command

        # But they should be overridden by the argument overrides
        self.worker.boss.config.slurm_args = (
            "--something --partition=baz --somethingElse"
        )
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--partition=baz" in command

    def test_prepareSbatch_time(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sinfo)
        ps = toil.batchSystems.slurm.SlurmBatchSystem.PartitionSet()
        self.worker.boss.partitions = ps
        # This is in seconds
        self.worker.boss.config.slurm_time = 30

        # Without a time override in the environment, we should use the normal
        # time and the "short" partition
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        logger.debug("Command: %s", command)
        assert "--time=0:30" in command
        assert "--partition=short" in command

        # With a time override, we should use it, slightly translated, and it
        # should change the selected partition.
        self.worker.boss.config.slurm_args = (
            "--something --time 10:00:00 --somethingElse"
        )
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        logger.debug("Command: %s", command)
        assert "--partition=medium" in command
        assert "--time=0:36000" in command

        # All ways of setting time should work, including =
        self.worker.boss.config.slurm_args = (
            "--something --time=10:00:00 --somethingElse"
        )
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        logger.debug("Command: %s", command)
        assert "--partition=medium" in command
        assert "--time=0:36000" in command

        # And short options
        self.worker.boss.config.slurm_args = "--something -t 10:00:00 --somethingElse"
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        logger.debug("Command: %s", command)
        assert "--partition=medium" in command
        assert "--time=0:36000" in command

    def test_prepareSbatch_accelerator_partition(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sinfo)
        ps = toil.batchSystems.slurm.SlurmBatchSystem.PartitionSet()
        self.worker.boss.partitions = ps
        self.worker.boss.config.slurm_accelerator_args = [("fpga", ["--partition=fpgas"])]
        fpga = [{"kind": "fpga", "count": 1}]

        # A job needing a mapped accelerator goes to its partition
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, fpga)
        assert "--partition=fpgas" in command
        assert not any(arg.startswith("--gres") for arg in command)

        # Even over the configured default partitions
        self.worker.boss.config.slurm_partition = "foobar"
        self.worker.boss.config.slurm_gpu_partition = "gpu"
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, fpga)
        assert "--partition=fpgas" in command

        # GPUs alongside still get requested through GRES
        command = self.worker.prepareSbatch(
            1, 100, 5, 0, "job5", None, fpga + [{"kind": "gpu", "count": 2}]
        )
        assert "--partition=fpgas" in command
        assert "--gres=gpu:2" in command

        # Jobs without that accelerator are unaffected
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--partition=foobar" in command

        # A matching partition in the argument overrides is fine
        self.worker.boss.config.slurm_args = "--partition=fpgas"
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, fpga)
        assert "--partition=fpgas" in command

        # But a different one would send the job where its accelerator isn't
        self.worker.boss.config.slurm_args = "-p baz"
        with self.assertRaises(ValueError):
            self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, fpga)

        # And so would an accelerator kind without arguments
        self.worker.boss.config.slurm_args = ""
        with self.assertRaises(ValueError):
            self.worker.prepareSbatch(
                1, 100, 5, 0, "job5", None, [{"kind": "qpu", "count": 1}]
            )

        # Or two kinds in different partitions
        self.worker.boss.config.slurm_accelerator_args = [
            ("fpga", ["--partition=fpgas"]), ("qpu", ["-p", "qpus"])
        ]
        with self.assertRaisesRegex(ValueError, "only run in one"):
            self.worker.prepareSbatch(
                1, 100, 5, 0, "job5", None, fpga + [{"kind": "qpu", "count": 1}]
            )

    def test_prepareSbatch_accelerator_args(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sinfo)
        ps = toil.batchSystems.slurm.SlurmBatchSystem.PartitionSet()
        self.worker.boss.partitions = ps
        self.worker.boss.config.slurm_partition = "foobar"
        self.worker.boss.config.slurm_accelerator_args = [
            ("qpu", ["--qpu=sim", "--comment=quantum"])
        ]
        qpu = [{"kind": "qpu", "count": 1}]

        # A site's own options for an accelerator reach its jobs' sbatch, and
        # without a partition among them the job keeps the default one
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, qpu)
        assert "--qpu=sim" in command and "--comment=quantum" in command
        assert "--partition=foobar" in command

        # Jobs without that accelerator don't get them
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--qpu=sim" not in command

        # Options Toil sets itself can't be given for an accelerator
        self.worker.boss.config.slurm_accelerator_args = [("qpu", ["--mem=10G"])]
        with self.assertRaises(ValueError):
            self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, qpu)

    def test_prepareSbatch_accelerator_properties(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sinfo)
        ps = toil.batchSystems.slurm.SlurmBatchSystem.PartitionSet()
        self.worker.boss.partitions = ps
        self.worker.boss.config.slurm_accelerator_args = [
            toil.batchSystems.slurm.parse_accelerator_args(
                "qpu=--qpu={backend} --max-qubits={qubits} --comment={{literal}}"
            )
        ]

        # Each job's accelerator properties fill the arguments for its kind
        small = [{"kind": "qpu", "count": 1, "properties": {"backend": "sim", "qubits": "3"}}]
        large = [{"kind": "qpu", "count": 1, "properties": {"backend": "sim", "qubits": "30"}}]
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, small)
        assert "--qpu=sim" in command and "--max-qubits=3" in command
        assert "--comment={literal}" in command
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job6", None, large)
        assert "--max-qubits=30" in command

        # A kind every node provides needs no arguments
        self.worker.boss.config.slurm_accelerator_args = [
            toil.batchSystems.slurm.parse_accelerator_args("qpu=")
        ]
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job8", None, small)
        assert not any(arg.startswith("--qpu") for arg in command)
        self.worker.boss.config.slurm_accelerator_args = [
            toil.batchSystems.slurm.parse_accelerator_args(
                "qpu=--qpu={backend} --max-qubits={qubits} --comment={{literal}}"
            )
        ]

        # A property the arguments need but the job doesn't give is refused by name
        missing = [{"kind": "qpu", "count": 1, "properties": {"backend": "sim"}}]
        with self.assertRaisesRegex(ValueError, "accelerator property qubits"):
            self.worker.prepareSbatch(1, 100, 5, 0, "job7", None, missing)

    def test_check_accelerator_request(self):
        boss = FakeBatchSystem()
        boss.config.slurm_accelerator_args = [("fpga", ["--partition=fpgas"])]

        def check(accelerators):
            requirer = Requirer({"accelerators": accelerators})
            toil.batchSystems.slurm.SlurmBatchSystem._check_accelerator_request(
                boss, requirer
            )

        # GPUs and mapped kinds can be provided
        check([{"kind": "gpu", "count": 1}])
        check([{"kind": "fpga", "count": 1}])
        check([{"kind": "fpga", "count": 1}, {"kind": "gpu", "count": 1}])

        # Unmapped kinds can't, and the error says how to map them
        with self.assertRaisesRegex(
            InsufficientSystemResources, "--slurmAcceleratorArgs qpu=ARGS"
        ):
            check([{"kind": "qpu", "count": 1}])

        # Kinds whose arguments need properties can't be had without them
        boss.config.slurm_accelerator_args = [("qpu", ["--qpu={backend}"])]
        check([{"kind": "qpu", "count": 1, "properties": {"backend": "sim"}}])
        with self.assertRaisesRegex(
            InsufficientSystemResources, "accelerator property backend"
        ):
            check([{"kind": "qpu", "count": 1}])

    def test_prepareSbatch_export(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sinfo)
        ps = toil.batchSystems.slurm.SlurmBatchSystem.PartitionSet()
        self.worker.boss.partitions = ps

        # Without any overrides, we need --export=ALL
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--export=ALL" in command

        # With overrides, we don't get --export=ALL
        self.worker.boss.config.slurm_args = "--export=foo"
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--export=ALL" not in command

        # With --export-file, we don't get --export=ALL as documented.
        self.worker.boss.config.slurm_args = "--export-file=./thefile.txt"
        command = self.worker.prepareSbatch(1, 100, 5, 0, "job5", None, None)
        assert "--export=ALL" not in command

    def test_option_detector(self):
        detector = toil.batchSystems.slurm.option_detector("foobar", "f")

        self.assertTrue(detector("--foobar"))
        self.assertTrue(detector("--foobar=1"))
        self.assertTrue(detector("-f"))
        self.assertFalse(detector("-F"))
        self.assertFalse(detector("--foo-bar"))
        self.assertFalse(detector("foobar"))
        self.assertFalse(detector("-foobar"))
        self.assertFalse(detector("f"))

    def test_any_option_detector(self):
        detector = toil.batchSystems.slurm.any_option_detector([])
        self.assertFalse(detector("--anything"))

        detector = toil.batchSystems.slurm.any_option_detector(
            [("foobar", "f"), "many-bothans", ("bazz-only", "B")]
        )

        self.assertTrue(detector("--foobar"))
        self.assertTrue(detector("-f"))
        self.assertTrue(detector("--many-bothans=False"))
        self.assertTrue(detector("--bazz-only"))
        self.assertTrue(detector("-B"))
        self.assertFalse(detector("--no-bazz"))
        self.assertFalse(detector("--foo-bar=--bazz-only"))

    def test_parse_slurm_option_value(self):
        args = ["--qos", "high", "--partition=gpu"]
        value, index = toil.batchSystems.slurm.parse_slurm_option_value(args, 0)
        self.assertEqual(value, "high")
        self.assertEqual(index, 1)

        value, index = toil.batchSystems.slurm.parse_slurm_option_value(args, 2)
        self.assertEqual(value, "gpu")
        self.assertEqual(index, 2)

        with self.assertRaises(ValueError):
            toil.batchSystems.slurm.parse_slurm_option_value(["--qos"], 0)

    def test_sinfo_not_permitted(self):
        """Test when ``sinfo`` is not available.

        Certain HPC platforms may prohibit Slurm commands that give access to
        other users' jobs. E.g., BSC MN4, MN5. See issue #5461.
        """
        error_raised = CalledProcessErrorStderr(1, 'sinfo')
        with (
            mock.patch("toil.batchSystems.slurm.call_command", side_effect=error_raised),
            mock.patch("toil.batchSystems.slurm.SlurmBatchSystem.PartitionSet._get_gpu_partitions") as mock_fn
        ):
            partition_set = toil.batchSystems.slurm.SlurmBatchSystem.PartitionSet()


        assert partition_set.get_partition(120.0) is None

        self.assertIsNone(partition_set.all_partitions)


    ###
    ### Tests for parse_slurm_time
    ###

    def test_parse_slurm_time_minutes(self):
        """Test parsing 'minutes' format."""
        result = toil.batchSystems.slurm.parse_slurm_time("5")
        self.assertEqual(result, 5 * 60)

        result = toil.batchSystems.slurm.parse_slurm_time("30")
        self.assertEqual(result, 30 * 60)

        result = toil.batchSystems.slurm.parse_slurm_time("120")
        self.assertEqual(result, 120 * 60)

    def test_parse_slurm_time_minutes_seconds(self):
        """Test parsing 'minutes:seconds' format."""
        result = toil.batchSystems.slurm.parse_slurm_time("5:30")
        self.assertEqual(result, 5 * 60 + 30)

        result = toil.batchSystems.slurm.parse_slurm_time("10:00")
        self.assertEqual(result, 10 * 60)

        result = toil.batchSystems.slurm.parse_slurm_time("0:45")
        self.assertEqual(result, 45)

    def test_parse_slurm_time_hours_minutes_seconds(self):
        """Test parsing 'hours:minutes:seconds' format."""
        result = toil.batchSystems.slurm.parse_slurm_time("1:30:00")
        self.assertEqual(result, 1 * 3600 + 30 * 60)

        result = toil.batchSystems.slurm.parse_slurm_time("2:15:30")
        self.assertEqual(result, 2 * 3600 + 15 * 60 + 30)

        result = toil.batchSystems.slurm.parse_slurm_time("10:00:00")
        self.assertEqual(result, 10 * 3600)

    def test_parse_slurm_time_days_hours(self):
        """Test parsing 'days-hours' format."""
        result = toil.batchSystems.slurm.parse_slurm_time("3-0")
        self.assertEqual(result, 3 * 86400)

        result = toil.batchSystems.slurm.parse_slurm_time("1-12")
        self.assertEqual(result, 1 * 86400 + 12 * 3600)

        result = toil.batchSystems.slurm.parse_slurm_time("7-00")
        self.assertEqual(result, 7 * 86400)

    def test_parse_slurm_time_days_hours_minutes(self):
        """Test parsing 'days-hours:minutes' format."""
        result = toil.batchSystems.slurm.parse_slurm_time("3-0:00")
        self.assertEqual(result, 3 * 86400)

        result = toil.batchSystems.slurm.parse_slurm_time("3-00:00")
        self.assertEqual(result, 3 * 86400)

        result = toil.batchSystems.slurm.parse_slurm_time("1-6:30")
        self.assertEqual(result, 1 * 86400 + 6 * 3600 + 30 * 60)

        result = toil.batchSystems.slurm.parse_slurm_time("2-12:45")
        self.assertEqual(result, 2 * 86400 + 12 * 3600 + 45 * 60)

    def test_parse_slurm_time_days_hours_minutes_seconds(self):
        """Test parsing 'days-hours:minutes:seconds' format."""
        result = toil.batchSystems.slurm.parse_slurm_time("7-00:00:00")
        self.assertEqual(result, 7 * 86400)

        result = toil.batchSystems.slurm.parse_slurm_time("1-02:30:45")
        self.assertEqual(result, 1 * 86400 + 2 * 3600 + 30 * 60 + 45)

        result = toil.batchSystems.slurm.parse_slurm_time("5-12:00:00")
        self.assertEqual(result, 5 * 86400 + 12 * 3600)

        result = toil.batchSystems.slurm.parse_slurm_time("0-01:30:00")
        self.assertEqual(result, 1 * 3600 + 30 * 60)

    def test_parse_slurm_time_edge_cases(self):
        """Test edge cases and zero values."""
        result = toil.batchSystems.slurm.parse_slurm_time("0-00:00:00")
        self.assertEqual(result, 0)

        result = toil.batchSystems.slurm.parse_slurm_time("0")
        self.assertEqual(result, 0)

        result = toil.batchSystems.slurm.parse_slurm_time("365-00:00:00")
        self.assertEqual(result, 365 * 86400)

    def _fake_qos(self, associations: str, partitions: str, qos_limits: dict[str, str]):
        def call(args, **_) -> str:
            if args[:4] == ["sacctmgr", "-nP", "show", "assoc"]:
                return associations
            if args[:4] == ["scontrol", "-o", "show", "partition"]:
                return partitions
            if args[:4] == ["sacctmgr", "-nP", "show", "qos"]:
                names = args[5].removeprefix("name=").split(",")
                return "".join(qos_limits[name] + "\n" for name in names)
            raise RuntimeError(f"Cannot fake command call: {args}")

        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call)

    def test_qos_submit_limit_takes_the_smallest_limit(self):
        self._fake_qos(
            "normal|normal,long|20\n",
            "PartitionName=small QoS=small_qos Default=YES\n",
            {"normal": "", "small_qos": "9"},
        )
        self.assertEqual(toil.batchSystems.slurm.qos_submit_limit("u", None, None), 9)

    def test_qos_submit_limit_uses_the_given_qos_and_partition(self):
        self._fake_qos(
            "normal|normal,big|\n",
            "PartitionName=large QoS=N/A Default=NO\n",
            {"big": "50", "normal": "4"},
        )
        self.assertEqual(
            toil.batchSystems.slurm.qos_submit_limit("u", "big", "large"), 50
        )

    def test_qos_submit_limit_without_limits(self):
        self._fake_qos(
            "||\n",
            "PartitionName=standard QoS=N/A Default=YES\n",
            {"normal": ""},
        )
        self.assertIsNone(toil.batchSystems.slurm.qos_submit_limit("u", None, None))
