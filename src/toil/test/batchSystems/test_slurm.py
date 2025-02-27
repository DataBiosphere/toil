import errno
import textwrap
from queue import Queue

import pytest

import toil.batchSystems.slurm
from toil.batchSystems.abstractBatchSystem import (
    EXIT_STATUS_UNAVAILABLE_VALUE,
    BatchJobExitReason,
)
from toil.common import Config
from toil.lib.misc import CalledProcessErrorStderr
from toil.test import ToilTest

# TODO: Come up with a better way to mock the commands then monkey-patching the
# command-calling functions.


def call_sacct(args, **_) -> str:
    """
    The arguments passed to `call_command` when executing `sacct` are:
    ['sacct', '-n', '-j', '<comma-separated list of job-ids>', '--format',
    'JobIDRaw,State,ExitCode', '-P', '-S', '1970-01-01']
    The multi-line output is something like::

        1234|COMPLETED|0:0
        1234.batch|COMPLETED|0:0
        1235|PENDING|0:0
        1236|FAILED|0:2
        1236.extern|COMPLETED|0:0
    """
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
        789724: "789724|RUNNING|0:0\n789724.batch|RUNNING|0:0\n789724.extern|RUNNING|0:0\n",
        789868: "789868|PENDING|0:0\n",
        789869: "789869|COMPLETED|0:0\n789869.batch|COMPLETED|0:0\n789869.extern|COMPLETED|0:0\n",
    }
    job_ids = [int(job_id) for job_id in args[3].split(",")]
    stdout = ""
    # Glue the fake outputs for the request job-ids together in a single string
    for job_id in job_ids:
        stdout += sacct_info.get(job_id, "")
    return stdout


def call_scontrol(args, **_) -> str:
    """
    The arguments passed to `call_command` when executing `scontrol` are:
    ``['scontrol', 'show', 'job']`` or ``['scontrol', 'show', 'job', '<job-id>']``
    """
    job_id = int(args[3]) if len(args) > 3 else None
    # Fake output per fake job-id.
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


def call_sacct_raises(*_):
    """
    Fake that the `sacct` command fails by raising a `CalledProcessErrorStderr`
    """
    raise CalledProcessErrorStderr(
        1, "sacct: error: Problem talking to the database: " "Connection timed out"
    )


class FakeBatchSystem:
    """
    Class that implements a minimal Batch System, needed to create a Worker (see below).
    """

    def __init__(self):
        self.config = self.__fake_config()

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
        return config


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

    ###
    ### tests for getJobExitCode
    ###

    def test_getJobExitCode_job_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
        job_id = "785023"  # FAILED
        expected_result = (127, BatchJobExitReason.FAILED)
        result = self.worker.getJobExitCode(job_id)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_getJobExitCode_job_not_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
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
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol)
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
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol)
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
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
        job_ids = ["785023"]  # FAILED
        expected_result = [(127, BatchJobExitReason.FAILED)]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_coalesce_job_exit_codes_one_not_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
        job_ids = ["1234"]  # Non-existent
        expected_result = [None]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_coalesce_job_exit_codes_many_all_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
        job_ids = [
            "754725",  # TIMEOUT,
            "789456",  # FAILED,
            "789724",  # RUNNING,
            "789868",  # PENDING,
            "789869",
        ]  # COMPLETED
        # RUNNING and PENDING jobs should return None
        expected_result = [
            (EXIT_STATUS_UNAVAILABLE_VALUE, BatchJobExitReason.KILLED),
            (1, BatchJobExitReason.FAILED),
            None,
            None,
            (0, BatchJobExitReason.FINISHED),
        ]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_coalesce_job_exit_codes_some_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_sacct)
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
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol)
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
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_scontrol)
        job_ids = ["1234"]  # Non-existent
        try:
            _ = self.worker.coalesce_job_exit_codes(job_ids)
        except CalledProcessErrorStderr:
            pass
        else:
            assert False, "Exception CalledProcessErrorStderr not raised"
