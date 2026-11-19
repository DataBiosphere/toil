import sys
from queue import Queue
from uuid import uuid4

import pytest

import toil.batchSystems.torque
from toil.batchSystems.abstractBatchSystem import BatchSystemSupport
from toil.common import Config


class FakeBatchSystem(BatchSystemSupport):
    """
    Minimal batch system a Torque worker can be created for.
    """

    def __init__(self):
        config = Config()
        config.workflowID = str(uuid4())
        config.cleanWorkDir = "always"
        toil.batchSystems.torque.TorqueBatchSystem.setOptions(
            lambda o: setattr(config, o, None)
        )
        super().__init__(config, float("inf"), sys.maxsize, sys.maxsize)

    def getWaitDuration(self):
        return 10


FakeBatchSystem.__abstractmethods__ = set()


def test_pbs_version(monkeypatch: pytest.MonkeyPatch) -> None:
    for output, version in [
        ("pbs_version = 2026.1.0.20260821184519\n", "pro"),
        ("PBSPro_13.1.0.160576\n", "pro"),
        ("Version: 6.1.3\n", "oss"),
    ]:
        monkeypatch.setattr(
            toil.batchSystems.torque, "call_command", lambda args, **_: output
        )
        worker = toil.batchSystems.torque.TorqueBatchSystem.GridEngineThread(
            newJobsQueue=Queue(),
            updatedJobsQueue=Queue(),
            killQueue=Queue(),
            killedJobsQueue=Queue(),
            boss=FakeBatchSystem(),
        )
        assert worker._version == version, output
