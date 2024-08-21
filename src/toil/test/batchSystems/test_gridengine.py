import textwrap
from queue import Queue

import pytest

import toil.batchSystems.gridengine
from toil.batchSystems.abstractGridEngineBatchSystem import ExceededRetryAttempts
from toil.common import Config
from toil.lib.misc import CalledProcessErrorStderr
from toil.test import ToilTest


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
        config.cleanWorkDir = 'always'
        return config

    def with_retries(self, operation, *args, **kwargs):
        """
        The grid engine batch system needs a with_retries function when running the GridEngineThread, so fake one
        """
        return operation(*args, **kwargs)


def call_qstat_or_qacct(args, **_):
    # example outputs taken from https://2021.help.altair.com/2021.1/AltairGridEngine/8.7.0/UsersGuideGE.pdf
    qacct_info = {}
    job_id_info = {1: {"failed": True, "exit_code": 0, "completed": True}, 2: {"failed": True, "exit_code": 2, "completed": True},
                   3: {"failed": False, "exit_code": 0, "completed": True}, 4: {"failed": False, "exit_code": 10, "completed": True},
                   5: {"failed": False, "exit_code": 0, "completed": False}}
    for job_id, status_info in job_id_info.items():
        failed = 1 if status_info["failed"] else 0
        exit_status = status_info["exit_code"]
        qacct_info[job_id] = textwrap.dedent(f"""\
            ==============================================================
            qname all.q
            hostname kailua
            group users
            owner jondoe
            project NONE
            department defaultdepartment
            jobname Sleeper
            jobnumber 10
            taskid undefined
            account sge
            priority 0
            qsub_time Thu Mar 10 19:58:35 2011
            start_time Thu Mar 10 19:58:42 2011
            end_time Thu Mar 10 19:59:43 2011
            granted_pe NONE
            slots 1
            failed {failed}
            exit_status {exit_status}
            ru_wallclock 61
            ru_utime 0.070
            ru_stime 0.050
            ru_maxrss 1220
            ru_ixrss 0
            ru_ismrss 0
            ru_idrss 0
        """)
    if args[0] == "qstat":
        # This is guess for what qstat will return given a job. I'm unable to find an example for qstat.
        # This also assumes the second argument args[1] is -j, as that is what we try to use
        job_id = int(args[2])
        if job_id not in job_id_info.keys() or job_id_info[job_id]["completed"]:
            stderr = f"Following jobs do not exist {job_id}"
        else:
            # This is not the output of qstat when the job is running, and is just a guess
            # We test on the existence of the string "Following jobs do not exist", so this should be okay for now
            stderr = f"Job exists {job_id}"
        raise CalledProcessErrorStderr(2, args, stderr=stderr)
    elif args[0] == "qacct":
        if args[1] != "-j":
            # Documentation for qacct says if -j is not found then all jobs are listed
            # https://gridscheduler.sourceforge.net/htmlman/htmlman1/qacct.html
            # This is a guess for the output of qacct. We don't have a SGE cluster and I can't find a bare qacct example output online
            qacct_response = "\n".join(qacct_info.values())
        else:
            job_id = int(args[2])
            if job_id not in job_id_info.keys():
                # This is a guess of the behavior when the job does not exist. Since the behavior is unknown, this is not currently tested
                return ""
            qacct_response = qacct_info[job_id]

        return qacct_response


class GridEngineTest(ToilTest):
    """
    Class for unit-testing GridEngineBatchSystem
    """

    def setUp(self):
        self.monkeypatch = pytest.MonkeyPatch()
        self.worker = toil.batchSystems.gridengine.GridEngineBatchSystem.GridEngineThread(
            newJobsQueue=Queue(),
            updatedJobsQueue=Queue(),
            killQueue=Queue(),
            killedJobsQueue=Queue(),
            boss=FakeBatchSystem())

    ###
    ### Tests for coalesce_job_exit_codes for gridengine.
    ###

    def test_coalesce_job_exit_codes_one_exists(self):
        self.monkeypatch.setattr(toil.batchSystems.gridengine, "call_command", call_qstat_or_qacct)
        job_ids = ['1']  # FAILED
        expected_result = [1]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

    def test_coalesce_job_exit_codes_one_still_running(self):
        self.monkeypatch.setattr(toil.batchSystems.gridengine, "call_command", call_qstat_or_qacct)
        job_ids = ['5']  # Still running. We currently raise an exception when this happens
        try:
            self.worker.coalesce_job_exit_codes(job_ids)
        except ExceededRetryAttempts:
            pass
        else:
            raise RuntimeError("Test did not raise an exception!")

    def test_coalesce_job_exit_codes_many_all_exist(self):
        self.monkeypatch.setattr(toil.batchSystems.gridengine, "call_command", call_qstat_or_qacct)
        job_ids = ['1',  # FAILED,
                   '2',  # FAILED (with exit code that we ignore),
                   '3',  # SUCCEEDED,
                   '4']  # EXIT CODE 10
        # RUNNING and PENDING jobs should return None
        expected_result = [
            1,
            1,
            0,
            10
        ]
        result = self.worker.coalesce_job_exit_codes(job_ids)
        assert result == expected_result, f"{result} != {expected_result}"

