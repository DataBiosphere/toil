import pytest

import toil.batchSystems.torque
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
