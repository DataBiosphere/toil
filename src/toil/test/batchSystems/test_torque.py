from types import SimpleNamespace
from typing import cast

import pytest

import toil.batchSystems.torque
from toil.batchSystems.abstractGridEngineBatchSystem import (
    AbstractGridEngineBatchSystem,
)
from toil.batchSystems.torque import TorqueBatchSystem


def detected_version(monkeypatch: pytest.MonkeyPatch, output: str) -> str:
    monkeypatch.setattr(
        toil.batchSystems.torque, "call_command", lambda args, **_: output
    )
    worker = TorqueBatchSystem.GridEngineThread.__new__(
        TorqueBatchSystem.GridEngineThread
    )
    return worker._pbsVersion()


def test_pbs_version_line_is_pbs_pro(monkeypatch: pytest.MonkeyPatch) -> None:
    output = "pbs_version = 2026.1.0.20260821184519\n"
    assert detected_version(monkeypatch, output) == "pro"


def test_empty_stdout_is_oss_torque(monkeypatch: pytest.MonkeyPatch) -> None:
    assert detected_version(monkeypatch, "") == "oss"


def resource_request(
    monkeypatch: pytest.MonkeyPatch, version: str, walltime: int
) -> str | None:
    monkeypatch.setattr(
        TorqueBatchSystem.GridEngineThread,
        "generateTorqueWrapper",
        lambda self, command, jobID: "wrapper.sh",
    )
    worker = TorqueBatchSystem.GridEngineThread.__new__(
        TorqueBatchSystem.GridEngineThread
    )
    worker._version = version
    worker.boss = cast(AbstractGridEngineBatchSystem, SimpleNamespace(environment={}))
    command = worker.prepareSubmission(1, 2**30, 0, walltime, 5, "true", "job5")
    return command[command.index("-l") + 1] if "-l" in command else None


@pytest.mark.parametrize(
    "version,walltime,expected",
    [
        ("pro", 3725, "select=1:mem=1048576K,walltime=1:02:05"),
        ("pro", 0, "select=1:mem=1048576K"),
        ("oss", 90000, "mem=1048576K,walltime=25:00:00"),
        ("oss", 0, "mem=1048576K"),
    ],
)
def test_walltime_is_requested_when_set(
    monkeypatch: pytest.MonkeyPatch, version: str, walltime: int, expected: str
) -> None:
    assert resource_request(monkeypatch, version, walltime) == expected


def test_walltime_in_torque_reqs_is_refused_for_a_job_with_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TOIL_TORQUE_REQS", "walltime=2:00:00")
    with pytest.raises(ValueError, match="Incompatible"):
        resource_request(monkeypatch, "pro", 60)
    assert (
        resource_request(monkeypatch, "pro", 0)
        == "select=1:mem=1048576K,walltime=2:00:00"
    )
