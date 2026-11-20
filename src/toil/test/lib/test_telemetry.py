# Copyright (C) 2015-2026 Regents of the University of California
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
"""Tests for the OpenTelemetry traces and metrics of toil.lib.telemetry."""
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from collections.abc import Iterator
from typing import Any

import pytest

from toil.batchSystems.abstractBatchSystem import BatchJobExitReason
from toil.batchSystems.slurm import is_submit_limit_refusal, sbatch_partition
from toil.batchSystems.torque import TorqueBatchSystem
from toil.common import Toil
from toil.job import Job, JobDescription
from toil import job_telemetry
from toil.lib import telemetry
from toil.lib.misc import CalledProcessErrorStderr
from toil.test import needs_cwl

pytest.importorskip("opentelemetry.sdk")

from toil.lib.telemetry import _otlp_json  # noqa: E402

INHERITED_TRACE = "1" * 32
INHERITED_SPAN = "2" * 16

# Labels that would make a metric series per run, job or step.
ID_LABELS = {
    "step.id",
    "step.scatter.index",
    "step.loop.iteration",
    "batch.job.id",
    "toil.job.batch_system_id",
    "toil.workflow.id",
    "job.attempt",
    "job.name",
}


def _value(attribute: dict[str, Any]) -> Any:
    return next(iter(attribute["value"].values()))


def read_spans(directory: Path) -> list[dict[str, Any]]:
    """Read the spans in a directory, adding ``service`` and plain ``attrs``."""
    spans = []
    for path in sorted(directory.glob("*.spans.jsonl")):
        for line in path.read_text().splitlines():
            for resource_spans in json.loads(line)["resourceSpans"]:
                resource = {
                    a["key"]: _value(a)
                    for a in resource_spans["resource"]["attributes"]
                }
                for scope_spans in resource_spans["scopeSpans"]:
                    for span in scope_spans["spans"]:
                        span["service"] = resource["service.name"]
                        span["attrs"] = {
                            a["key"]: _value(a) for a in span.get("attributes", [])
                        }
                        span["event_names"] = [e["name"] for e in span.get("events", [])]
                        spans.append(span)
    return spans


def read_metrics(directory: Path) -> dict[str, list[dict[str, Any]]]:
    """Read the metric points in a directory by name, adding plain ``labels``."""
    points: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(directory.glob("*.metrics.jsonl")):
        for line in path.read_text().splitlines():
            for resource_metrics in json.loads(line)["resourceMetrics"]:
                for scope_metrics in resource_metrics["scopeMetrics"]:
                    for metric in scope_metrics["metrics"]:
                        kind = next(
                            k for k in ("sum", "histogram", "gauge") if k in metric
                        )
                        for point in metric[kind]["dataPoints"]:
                            point["labels"] = {
                                a["key"]: _value(a) for a in point.get("attributes", [])
                            }
                            point["unit"] = metric.get("unit")
                            points.setdefault(metric["name"], []).append(point)
    return points


def total(points: list[dict[str, Any]]) -> int:
    """Add up counter values or histogram counts."""
    return sum(int(p.get("asInt", p.get("count", 0))) for p in points)


@pytest.fixture(autouse=True)
def no_telemetry_left_running() -> Iterator[None]:
    yield
    job_telemetry.end_worker(1)
    job_telemetry.end_leader("failed")


@pytest.fixture
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if name.startswith("OTEL_") or name in (
            telemetry.FILE_DIRECTORY_VARIABLE,
            *telemetry.TRACE_CONTEXT_VARIABLES.values(),
            *_otlp_json.BATCH_JOB_ID_VARIABLES,
        ):
            monkeypatch.delenv(name)


@pytest.fixture
def telemetry_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_environment: None
) -> Path:
    directory = tmp_path / "telemetry"
    monkeypatch.setenv(telemetry.FILE_DIRECTORY_VARIABLE, str(directory))
    return directory


def a_job(job_name: str = "CWLJob") -> JobDescription:
    job = JobDescription(
        requirements={
            "cores": 2,
            "memory": 2**30,
            "disk": 2**20,
            "accelerators": [],
            "preemptible": False,
        },
        jobName=job_name,
    )
    job.telemetry_attributes = {"step.id": "outer/inner", "step.scatter.index": 3}
    return job


@pytest.mark.parametrize(
    "environment, wanted",
    [
        ({}, False),
        ({"TOIL_OTEL_FILE_DIR": "/d"}, True),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318"}, True),
        ({"OTEL_TRACES_EXPORTER": "otlp"}, True),
        ({"OTEL_TRACES_EXPORTER": "none", "OTEL_METRICS_EXPORTER": "none",
          "OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318"}, False),
        ({"OTEL_SDK_DISABLED": "true", "TOIL_OTEL_FILE_DIR": "/d"}, False),
    ],
)
def test_telemetry_is_on_only_when_the_environment_asks(
    environment: dict[str, str], wanted: bool
) -> None:
    assert telemetry.requested(environment) is wanted


def test_accessors_are_no_ops_when_off(clean_environment: None) -> None:
    assert not telemetry.configure("toil-test")
    assert not telemetry.enabled()
    telemetry.tracer().start_span("span").end()
    telemetry.counter("count").add(1)
    telemetry.duration_histogram("seconds").record(1.0)


def test_spans_go_to_providers_a_test_supplies(clean_environment: None) -> None:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    telemetry.use_providers(tracer_provider, MeterProvider())
    leader = job_telemetry.LeaderTelemetry("slurm", {"workflow.name": "w"})
    leader.job_issued(1, leader.job_issuing(a_job()))
    leader.job_ended(1, 0, BatchJobExitReason.FINISHED, None, runs_again=False)
    leader.end("success")
    telemetry.shutdown()

    assert [span.name for span in exporter.get_finished_spans()] == [
        "job.attempt",
        "workflow.run",
    ]


def test_export_files_are_named_by_service_pid_and_bare_batch_job_id(
    monkeypatch: pytest.MonkeyPatch, clean_environment: None
) -> None:
    pid = os.getpid()
    assert _otlp_json.export_file_name("toil-leader", "spans") == f"toil-leader-0-{pid}.spans.jsonl"
    monkeypatch.setenv("PBS_JOBID", "1234.pbs-server")
    assert (
        _otlp_json.export_file_name("toil-worker", "metrics")
        == f"toil-worker-0-{pid}.1234.metrics.jsonl"
    )


def test_ids_are_written_as_hex() -> None:
    message = {"spans": [{"traceId": "AAEC", "spanId": "/w==", "name": "AAEC"}]}
    assert _otlp_json.hex_ids(message) == {
        "spans": [{"traceId": "000102", "spanId": "ff", "name": "AAEC"}]
    }


def test_worker_takes_leader_trace_context_unless_leader_records_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    traced = {"TRACEPARENT": f"00-{INHERITED_TRACE}-{INHERITED_SPAN}-01"}
    assert job_telemetry.worker_takes_leader_trace_context(traced)
    assert not job_telemetry.worker_takes_leader_trace_context(
        {**traced, "TOIL_OTEL_FILE_DIR": "/d"}
    )
    monkeypatch.setattr(telemetry, "OTEL_AVAILABLE", False)
    assert job_telemetry.worker_takes_leader_trace_context(
        {**traced, "TOIL_OTEL_FILE_DIR": "/d"}
    )


def test_nothing_starts_unless_asked(clean_environment: None) -> None:
    assert job_telemetry.start_leader("slurm", {}) is None
    assert job_telemetry.start_worker("slurm") is None


def test_leader_records_each_job_attempt(
    telemetry_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "TRACEPARENT", f"00-{INHERITED_TRACE}-{INHERITED_SPAN}-01"
    )
    workflow = job_telemetry.start_leader("slurm", {"workflow.name": "w"})
    assert workflow is not None
    job = a_job()

    first = workflow.job_issuing(job)
    # Reports before the batch system ID is known.
    workflow.job_refused(
        7, "submit_limit", "sbatch: error: QOSMaxSubmitJobPerUserLimit", "short"
    )
    workflow.job_submitted(7, "4242", "short")
    workflow.job_issued(7, first)
    workflow.jobs_running({7: 5.0})
    workflow.job_ended(7, 1, BatchJobExitReason.FAILED, None, runs_again=True)

    workflow.job_ready(str(job.jobStoreID))
    second = workflow.job_issuing(job)
    workflow.job_issued(8, second)
    workflow.job_ended(8, 0, BatchJobExitReason.FINISHED, 3.0, runs_again=False)
    job_telemetry.end_leader("success")

    spans = read_spans(telemetry_dir)
    (run,) = [s for s in spans if s["name"] == "workflow.run"]
    assert run["service"] == "toil-leader"
    assert run["traceId"] == INHERITED_TRACE
    assert run["parentSpanId"] == INHERITED_SPAN
    assert run["attrs"]["outcome"] == "success"

    attempts = sorted(
        (s for s in spans if s["name"] == "job.attempt"),
        key=lambda s: int(s["attrs"]["job.attempt"]),
    )
    assert [a["parentSpanId"] for a in attempts] == [run["spanId"]] * 2
    failed, succeeded = (a["attrs"] for a in attempts)
    assert failed == {
        "job.name": "CWLJob",
        "step.id": "outer/inner",
        "step.scatter.index": "3",
        "request.cpu": 2.0,
        "request.memory": str(2**30),
        "job.attempt": "1",
        "batch.system": "slurm",
        "toil.job.batch_system_id": "7",
        "batch.job.id": "4242",
        "batch.queue": "short",
        "started.observed": False,
        "process.exit.code": "1",
        "outcome": "failed",
        "job.exit.reason": "failed",
    }
    assert attempts[0]["event_names"] == [
        "issued", "refused", "submitted", "started", "ended", "retried",
    ]
    (refused,) = [e for e in attempts[0]["events"] if e["name"] == "refused"]
    assert {a["key"]: _value(a) for a in refused["attributes"]} == {
        "refusal.reason": "submit_limit",
        "refusal.detail": "sbatch: error: QOSMaxSubmitJobPerUserLimit",
    }
    assert succeeded["job.attempt"] == "2"
    assert succeeded["outcome"] == "success"
    assert attempts[1]["event_names"] == ["issued", "started", "ended"]

    assert first.environment == {
        "TRACEPARENT": f"00-{INHERITED_TRACE}-{attempts[0]['spanId']}-01"
    }

    metrics = read_metrics(telemetry_dir)
    assert total(metrics["workflow.jobs.issued"]) == 2
    assert total(metrics["workflow.jobs.retried"]) == 1
    assert "workflow.jobs.failed" not in metrics
    (refusals,) = metrics["workflow.jobs.refused"]
    assert refusals["labels"] == {
        "batch.system": "slurm",
        "batch.queue": "short",
        "refusal.reason": "submit_limit",
    }
    for name in ("submit_delay", "queue_wait", "dispatch_gap"):
        assert total(metrics[f"workflow.job.{name}"]) == 1, name
        assert metrics[f"workflow.job.{name}"][0]["unit"] == "s"
        assert metrics[f"workflow.job.{name}"][0]["explicitBounds"] == [
            float(b) for b in telemetry.DURATION_BUCKETS
        ]
    assert {"process.cpu.time", "process.memory.usage"} <= set(metrics)
    for points in metrics.values():
        for point in points:
            assert not ID_LABELS & set(point["labels"])


def test_a_job_that_fails_for_good_is_counted_with_its_reason(
    telemetry_dir: Path,
) -> None:
    workflow = job_telemetry.start_leader("slurm", {})
    assert workflow is not None
    workflow.job_issued(1, workflow.job_issuing(a_job()))
    workflow.job_ended(1, 137, BatchJobExitReason.MEMLIMIT, None, runs_again=False)
    job_telemetry.end_leader("failed")

    (failure,) = read_metrics(telemetry_dir)["workflow.jobs.failed"]
    assert failure["labels"] == {
        "batch.system": "slurm",
        "job.exit.reason": "memlimit",
        "outcome": "failed",
    }
    (run,) = [s for s in read_spans(telemetry_dir) if s["name"] == "workflow.run"]
    assert run["attrs"]["outcome"] == "failed"


def test_worker_spans_nest_under_the_job_attempt(
    telemetry_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TRACEPARENT", f"00-{INHERITED_TRACE}-{INHERITED_SPAN}-01")
    worker = job_telemetry.start_worker("slurm")
    assert worker is not None
    with worker.job_run(a_job()):
        seen_by_job = os.environ["TRACEPARENT"]
    assert os.environ["TRACEPARENT"] == f"00-{INHERITED_TRACE}-{INHERITED_SPAN}-01"
    job_telemetry.end_worker(0)

    spans = {s["name"]: s for s in read_spans(telemetry_dir)}
    process, run = spans["job.process"], spans["job.run"]
    assert process["service"] == run["service"] == "toil-worker"
    assert process["traceId"] == run["traceId"] == INHERITED_TRACE
    assert process["parentSpanId"] == INHERITED_SPAN
    assert run["parentSpanId"] == process["spanId"]
    assert run["attrs"] == {
        "job.name": "CWLJob",
        "step.id": "outer/inner",
        "step.scatter.index": "3",
    }
    assert seen_by_job == f"00-{INHERITED_TRACE}-{run['spanId']}-01"

    (overhead,) = read_metrics(telemetry_dir)["workflow.job.wrapper_overhead"]
    assert overhead["labels"] == {"batch.system": "slurm"}
    assert int(overhead["count"]) == 1


def test_a_worker_inside_the_leader_adds_no_process_span(telemetry_dir: Path) -> None:
    assert job_telemetry.start_leader("single_machine", {}) is not None
    assert job_telemetry.start_worker("single_machine") is None
    job_telemetry.end_leader("success")


def test_slurm_partition_is_read_from_the_sbatch_line() -> None:
    assert sbatch_partition(["sbatch", "-J", "x"]) is None
    assert sbatch_partition(["sbatch", "-p", "a", "--partition=b"]) == "b"
    assert sbatch_partition(["sbatch", "--partition", "c", "--parsable"]) == "c"
    assert sbatch_partition(["sbatch", "-pd"]) == "d"


def test_slurm_submit_limits_are_told_from_other_refusals() -> None:
    def refusal(stderr: str) -> CalledProcessErrorStderr:
        return CalledProcessErrorStderr(1, ["sbatch"], stderr=stderr)

    accounting_policy = (
        "sbatch: error: Batch job submission failed: Job violates "
        "accounting/QOS policy (job submit limit, user's size and/or time limits)\n"
    )
    for reason in ("QOSMaxSubmitJobPerUserLimit", "AssocGrpSubmitJobsLimit"):
        assert is_submit_limit_refusal(
            refusal(f"sbatch: error: {reason}\n" + accounting_policy)
        )
    assert not is_submit_limit_refusal(
        refusal("sbatch: error: QOSMaxWallDurationPerJobLimit\n" + accounting_policy)
    )
    assert not is_submit_limit_refusal(
        refusal("sbatch: error: Batch job submission failed: Socket timed out\n")
    )


def test_torque_queue_is_read_from_the_qsub_line() -> None:
    queue = TorqueBatchSystem.GridEngineThread.submission_queue
    assert queue(None, ["qsub", "-V", "-q", "batch"]) == "batch"  # type: ignore[arg-type]
    assert queue(None, ["qsub", "-qdebug"]) == "debug"  # type: ignore[arg-type]
    assert queue(None, ["qsub", "-V"]) is None  # type: ignore[arg-type]


def _child(job: Job) -> None:
    pass


def _root(job: Job) -> None:
    job.addChildJobFn(_child)


def test_a_workflow_run_has_a_trace_down_to_each_job(
    telemetry_dir: Path, tmp_path: Path
) -> None:
    options = Job.Runner.getDefaultOptions(str(tmp_path / "jobstore"))
    options.logLevel = "INFO"
    options.batchSystem = "single_machine"
    with Toil(options) as toil:
        toil.start(Job.wrapJobFn(_root))

    spans = read_spans(telemetry_dir)
    by_name = Counter(s["name"] for s in spans)
    assert by_name["workflow.run"] == 1
    (run,) = [s for s in spans if s["name"] == "workflow.run"]
    assert {s["traceId"] for s in spans} == {run["traceId"]}
    ids = {s["spanId"]: s for s in spans}
    for span in spans:
        if span["name"] == "job.attempt":
            assert span["parentSpanId"] == run["spanId"]
        elif span["name"] == "job.process":
            assert ids[span["parentSpanId"]]["name"] == "job.attempt"
        elif span["name"] == "job.run":
            assert ids[span["parentSpanId"]]["name"] == "job.process"
    assert {s["attrs"]["job.name"] for s in spans if s["name"] == "job.run"} == {
        "_root",
        "_child",
    }
    services = {
        path.name.split("-0-")[0] for path in telemetry_dir.glob("*.spans.jsonl")
    }
    assert services == {"toil-leader", "toil-worker"}


@needs_cwl
def test_cwl_tool_jobs_report_their_step_and_trace(
    telemetry_dir: Path, tmp_path: Path
) -> None:
    workflow = Path(__file__).parent.parent / "cwl" / "telemetry_steps.cwl"
    result = subprocess.run(
        [
            os.path.join(os.path.dirname(sys.executable), "toil-cwl-runner"),
            "--jobStore",
            str(tmp_path / "jobstore"),
            "--outdir",
            str(tmp_path / "out"),
            str(workflow),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    outputs = json.loads(result.stdout)

    spans = read_spans(telemetry_dir)
    (run,) = [s for s in spans if s["name"] == "workflow.run"]
    assert run["attrs"]["workflow.uri"].endswith("telemetry_steps.cwl")
    tool_runs = {
        s["spanId"]: s["attrs"]
        for s in spans
        if s["name"] == "job.run" and s["attrs"]["job.name"] == "CWLJob"
    }
    places = sorted(
        (a["step.id"], a.get("step.scatter.index", "")) for a in tool_runs.values()
    )
    assert places == [
        ("outer/inner", ""),
        ("s1", ""),
        ("sc", "0"),
        ("sc", "1"),
        ("sc", "2"),
    ]

    def place_of(output: str) -> tuple[str, str]:
        traceparent = output.split("TRACEPARENT=")[1].strip()
        assert traceparent.split("-")[1] == run["traceId"]
        attributes = tool_runs[traceparent.split("-")[2]]
        return attributes["step.id"], attributes.get("step.scatter.index", "")

    assert place_of(outputs["a"]) == ("s1", "")
    assert place_of(outputs["b"]) == ("outer/inner", "")
    assert [place_of(o) for o in outputs["c"]] == [
        ("sc", "0"),
        ("sc", "1"),
        ("sc", "2"),
    ]
