import pytest
import textwrap
from queue import Queue

from toil.common import Config
from toil.lib.misc import CalledProcessErrorStderr
from toil.test import ToilTest
import toil.batchSystems.slurm


def call_command_sacct(args) -> str:
    """
    The arguments passed to `call_command` when executing `sacct` are:
    ['sacct', '-n', '-j', '<comma-separated list of job-ids>', '--format',
    'JobIDRaw,State,ExitCode', '-P', '-S', '1970-01-01']
    The multi-line output is something like:
        1234|COMPLETED|0:0
        1234.batch|COMPLETED|0:0
        1235|PENDING|0:0
        1236|FAILED|0:2
        1236.extern|COMPLETED|0:0
    """
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
    job_ids = [int(job_id) for job_id in args[3].split(',')]
    stdout = ""
    # Glue the fake outputs for the request job-ids together in a single string
    for job_id in job_ids:
        stdout += sacct_info.get(job_id, "")
    return stdout


def call_command_scontrol(args) -> str:
    """
    The arguments passed to `call_command` when executing `scontrol` are:
    ['scontrol', 'show', 'job'] or ['scontrol', 'show', 'job', '<job-id>']
    """
    job_id = int(args[3]) if len(args) > 3 else None
    # Fake output per fake job-id.
    scontrol_info = {
        789724: textwrap.dedent("""\
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
            """),
        789728: textwrap.dedent("""\
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
            """),
        789867: textwrap.dedent("""\
            JobId=789867 JobName=sleep.sh
               UserId=rapthor-mloose(54386) GroupId=rapthor-mloose(54038) MCS_label=N/A
               Priority=7908 Nice=0 Account=rapthor QOS=normal
               JobState=PENDING Reason=ReqNodeNotAvail,_UnavailableNodes:wn-ha-04 Dependency=(null)
               Requeue=0 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0
               RunTime=00:00:00 TimeLimit=5-00:00:00 TimeMin=N/A
               SubmitTime=2021-10-15T10:27:59 EligibleTime=2021-10-15T10:27:59
               AccrueTime=2021-10-15T10:27:59
               StartTime=Unknown EndTime=Unknown Deadline=N/A
               SuspendTime=None SecsPreSuspend=0 LastSchedEval=2021-10-15T11:00:07
               Partition=normal AllocNode:Sid=batch-01:2460968
               ReqNodeList=wn-ha-[01-05],wn-hb-[01-05] ExcNodeList=(null)
               NodeList=(null)
               NumNodes=10 NumCPUs=10 NumTasks=10 CPUs/Task=1 ReqB:S:C:T=0:0:*:*
               TRES=cpu=10,mem=80000M,node=10,billing=10
               Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=*
               MinCPUsNode=1 MinMemoryCPU=8000M MinTmpDiskNode=0
               Features=(null) DelayBoot=00:00:00
               OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null)
               Command=/home/rapthor-mloose/tmp/sleep.sh
               WorkDir=/home/rapthor-mloose/tmp
               StdErr=/home/rapthor-mloose/tmp/slurm-789867.out
               StdIn=/dev/null
               StdOut=/home/rapthor-mloose/tmp/slurm-789867.out
               Power=
               NtasksPerTRES:0
            """)
    }
    if job_id is not None:
        try:
            stdout = scontrol_info[job_id]
        except KeyError:
            raise CalledProcessErrorStderr(1, "slurm_load_jobs error: Invalid job id specified")
    else:
        # Glue the fake outputs for the request job-ids together in a single string
        stdout = ""
        for value in scontrol_info.values():
            stdout += value + '\n'
    return stdout


class FakeBatchSystem(object):
    """
    Class that implements a minimal Batch System, needed to create a Worker (see below).
    """

    def __init__(self):
        self.config = self.__fake_config()

    def getWaitDuration(self):
        return 10;

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
        config.cleanWorkDir = 'always'
        return config


class SlurmTest(ToilTest):
    """
    Class for unit-testing SlurmBatchSystem
    """

    def setUp(self):
        self.monkeypatch = pytest.MonkeyPatch()
        self.worker = toil.batchSystems.slurm.SlurmBatchSystem.Worker(
            newJobsQueue=Queue(),
            updatedJobsQueue=Queue(),
            killQueue=Queue(),
            killedJobsQueue=Queue(),
            boss=FakeBatchSystem())

    ####
    #### tests for _getJobDetailsFromSacct()
    ####

    def test_getJobDetailsFromSacct_one_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_command_sacct)
        expected_result = {785023: ("FAILED", 127)}
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, "{} != {}".format(result, expected_result)

    def test_getJobDetailsFromSacct_one_not_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_command_sacct)
        expected_result = {1234: (None, None)}
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, "{} != {}".format(result, expected_result)

    def test_getJobDetailsFromSacct_many_all_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_command_sacct)
        expected_result = {754725: ("TIMEOUT", 0), 789456: ("FAILED", 1), 789724: ("RUNNING", 0),
                           789868: ("PENDING", 0), 789869: ("COMPLETED", 0)}
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, "{} != {}".format(result, expected_result)

    def test_getJobDetailsFromSacct_many_some_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_command_sacct)
        expected_result = {609663: ("FAILED", 130), 767925: ("FAILED", 2), 1234: (None, None),
                           1235: (None, None), 765096: ("FAILED", 137)}
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, "{} != {}".format(result, expected_result)

    def test_getJobDetailsFromSacct_many_none_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_command_sacct)
        expected_result = {1234: (None, None), 1235: (None, None), 1236: (None, None)}
        result = self.worker._getJobDetailsFromSacct(list(expected_result))
        assert result == expected_result, "{} != {}".format(result, expected_result)

    ####
    #### tests for _getJobDetailsFromScontrol()
    ####

    def test_getJobDetailsFromScontrol_one_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_command_scontrol)
        expected_result = {789724: ("RUNNING", 0)}
        result = self.worker._getJobDetailsFromScontrol(list(expected_result))
        assert result == expected_result, "{} != {}".format(result, expected_result)

    def test_getJobDetailsFromScontrol_one_not_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_command_scontrol)
        expected_result = {1234: (None, None)}
        try:
            result = self.worker._getJobDetailsFromScontrol(list(expected_result))
            assert False, "Expected exception CalledProcessErrorStderr"
        except CalledProcessErrorStderr:
            assert True

    def test_getJobDetailsFromScontrol_many_all_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_command_scontrol)
        expected_result = {789724: ("RUNNING", 0), 789728: ("PENDING", 0), 789867: ("PENDING", 0)}
        result = self.worker._getJobDetailsFromScontrol(list(expected_result))
        assert result == expected_result, "{} != {}".format(result, expected_result)

    def test_getJobDetailsFromScontrol_many_some_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_command_scontrol)
        expected_result = {789724: ("RUNNING", 0), 1234: (None, None), 789867: ("PENDING", 0)}
        result = self.worker._getJobDetailsFromScontrol(list(expected_result))
        assert result == expected_result, "{} != {}".format(result, expected_result)

    def test_getJobDetailsFromScontrol_many_none_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.slurm, "call_command", call_command_scontrol)
        expected_result = {1234: (None, None), 1235: (None, None), 1236: (None, None)}
        result = self.worker._getJobDetailsFromScontrol(list(expected_result))
        assert result == expected_result, "{} != {}".format(result, expected_result)
