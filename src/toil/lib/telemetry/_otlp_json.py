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
"""
OTLP/JSON lines file exporters: one export request per line.

Needs the OpenTelemetry SDK, so import it only once telemetry is on.
"""
from __future__ import annotations

import base64
import binascii
import json
import os
import threading
from collections.abc import Mapping, Sequence
from typing import IO, Any

from google.protobuf.json_format import MessageToDict  # type: ignore[import-untyped]
from opentelemetry.exporter.otlp.proto.common.metrics_encoder import encode_metrics
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.metrics import (
    Counter,
    Histogram,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
)
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    MetricExporter,
    MetricExportResult,
)
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

BATCH_JOB_ID_VARIABLES = ("SLURM_JOB_ID", "PBS_JOBID", "LSB_JOBID")

# Deltas, so files from many processes add up.
DELTA_TEMPORALITY: dict[type, AggregationTemporality] = {
    Counter: AggregationTemporality.DELTA,
    Histogram: AggregationTemporality.DELTA,
    ObservableCounter: AggregationTemporality.DELTA,
    UpDownCounter: AggregationTemporality.CUMULATIVE,
    ObservableUpDownCounter: AggregationTemporality.CUMULATIVE,
    ObservableGauge: AggregationTemporality.CUMULATIVE,
}


def batch_job_id(environment: Mapping[str, str] = os.environ) -> str | None:
    """
    Get the ID of the scheduler job this process runs in, without a PBS server
    suffix.
    """
    for variable in BATCH_JOB_ID_VARIABLES:
        value = environment.get(variable)
        if value:
            return value.split(".")[0]
    return None


def export_file_name(service: str, kind: str) -> str:
    """
    Get the name of this process's ``kind`` file.

    It is ``<service>-0-<pid>[.<batch job id>].<kind>.jsonl``.
    """
    job = batch_job_id()
    return f"{service}-0-{os.getpid()}" + (f".{job}" if job else "") + f".{kind}.jsonl"


def open_export_stream(directory: str, service: str, kind: str) -> IO[str]:
    """Open this process's ``kind`` file in ``directory``."""
    os.makedirs(directory, exist_ok=True)
    return open(
        os.path.join(directory, export_file_name(service, kind)), "a", encoding="utf-8"
    )


def hex_ids(node: Any) -> Any:
    """Rewrite the base64 trace and span ids in ``node`` as hex, as OTLP/JSON needs."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ("traceId", "spanId", "parentSpanId") and isinstance(value, str):
                try:
                    node[key] = base64.b64decode(value, validate=True).hex()
                except (binascii.Error, ValueError):
                    pass
            else:
                hex_ids(value)
    elif isinstance(node, list):
        for item in node:
            hex_ids(item)
    return node


def _write(stream: IO[str], lock: threading.Lock, message: Any) -> None:
    line = json.dumps(
        hex_ids(MessageToDict(message, use_integers_for_enums=True)),
        separators=(",", ":"),
    )
    with lock:
        stream.write(line + "\n")
        stream.flush()


class OtlpJsonFileSpanExporter(SpanExporter):
    """Span exporter writing each batch as one OTLP/JSON line."""

    def __init__(self, stream: IO[str]) -> None:
        self._stream = stream
        self._lock = threading.Lock()

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        if spans:
            _write(self._stream, self._lock, encode_spans(spans))
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        with self._lock:
            self._stream.close()


class OtlpJsonFileMetricExporter(MetricExporter):
    """Metric exporter writing each collection as one OTLP/JSON line."""

    def __init__(self, stream: IO[str]) -> None:
        super().__init__(preferred_temporality=DELTA_TEMPORALITY)
        self._stream = stream
        self._lock = threading.Lock()

    def export(
        self, metrics_data: Any, timeout_millis: float = 10_000, **kwargs: Any
    ) -> MetricExportResult:
        if any(
            scope.metrics
            for resource in metrics_data.resource_metrics
            for scope in resource.scope_metrics
        ):
            _write(self._stream, self._lock, encode_metrics(metrics_data))
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return True

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
        with self._lock:
            self._stream.close()
