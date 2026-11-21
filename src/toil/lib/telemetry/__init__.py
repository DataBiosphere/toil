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
OpenTelemetry setup for a Toil process; see docs/running/telemetry.rst.

This package records nothing itself. Instrumented code uses the accessors
below, which are no-ops while telemetry is off or the SDK is missing.
"""
from __future__ import annotations

import logging
import os
import socket
import threading
from collections.abc import Iterator, Mapping
from typing import Any

import psutil

logger = logging.getLogger(__name__)

FILE_DIRECTORY_VARIABLE = "TOIL_OTEL_FILE_DIR"
TRACE_CONTEXT_VARIABLES = {"traceparent": "TRACEPARENT", "tracestate": "TRACESTATE"}
DURATION_BUCKETS = (0.01, 0.1, 1, 10, 60, 300, 1800, 3600, 14400)
SCOPE = "toil"

try:
    import opentelemetry.sdk  # noqa: F401

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False


class _State:
    """Process-wide telemetry state, guarded by _LOCK."""

    def __init__(self) -> None:
        self.tracer_provider: Any = None
        self.meter_provider: Any = None
        self.tracer: Any = None
        self.meter: Any = None
        self.instruments: dict[str, Any] = {}


_STATE = _State()
_LOCK = threading.Lock()


class _NoopSpan:
    def __enter__(self) -> _NoopSpan:
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def is_recording(self) -> bool:
        return False

    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def add_event(self, name: str, attributes: Any = None, timestamp: Any = None) -> None:
        pass

    def set_status(self, status: Any, description: Any = None) -> None:
        pass

    def end(self, end_time: Any = None) -> None:
        pass


class _NoopTracer:
    def start_span(self, name: str, **kwargs: Any) -> _NoopSpan:
        return _NoopSpan()

    def start_as_current_span(self, name: str, **kwargs: Any) -> _NoopSpan:
        return _NoopSpan()


class _NoopInstrument:
    def add(self, amount: float, attributes: Any = None) -> None:
        pass

    def record(self, amount: float, attributes: Any = None) -> None:
        pass


_NOOP_TRACER = _NoopTracer()
_NOOP_INSTRUMENT = _NoopInstrument()


def otlp_wanted(signal: str, environment: Mapping[str, str] = os.environ) -> bool:
    """
    Return whether the environment sends ``signal`` (``traces`` or ``metrics``)
    over OTLP.
    """
    chosen = environment.get(f"OTEL_{signal.upper()}_EXPORTER", "").strip().lower()
    if chosen:
        return "otlp" in [name.strip() for name in chosen.split(",")]
    return bool(
        environment.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        or environment.get(f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT")
    )


def requested(environment: Mapping[str, str] = os.environ) -> bool:
    """Return whether the environment turns telemetry on."""
    if environment.get("OTEL_SDK_DISABLED", "").strip().lower() == "true":
        return False
    return bool(
        environment.get(FILE_DIRECTORY_VARIABLE)
        or otlp_wanted("traces", environment)
        or otlp_wanted("metrics", environment)
    )


def configure(service_name: str) -> bool:
    """
    Set up telemetry for this process as ``service_name``, if the environment
    asks for it. Does nothing if already set up.

    :return: Whether telemetry is on.
    """
    with _LOCK:
        if _STATE.tracer is not None:
            return True
        if not requested():
            return False
        if not OTEL_AVAILABLE:
            logger.warning(
                "OpenTelemetry is requested but its SDK is not installed; "
                "install Toil's otel extra"
            )
            return False
        _build_providers(service_name)
        return True


def _build_providers(service_name: str) -> None:
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    from toil.version import version

    # These win over OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME.
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": version,
            "process.pid": os.getpid(),
            "host.name": socket.gethostname(),
        }
    )
    tracer_provider = TracerProvider(resource=resource)
    metric_exporters: list[Any] = []
    directory = os.environ.get(FILE_DIRECTORY_VARIABLE)
    if directory:
        from ._otlp_json import (
            OtlpJsonFileMetricExporter,
            OtlpJsonFileSpanExporter,
            open_export_stream,
        )

        tracer_provider.add_span_processor(
            BatchSpanProcessor(
                OtlpJsonFileSpanExporter(
                    open_export_stream(directory, service_name, "spans")
                )
            )
        )
        metric_exporters.append(
            OtlpJsonFileMetricExporter(
                open_export_stream(directory, service_name, "metrics")
            )
        )
    if otlp_wanted("traces") or otlp_wanted("metrics"):
        protocol = os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
        if protocol != "http/protobuf":
            logger.warning("Toil sends OTLP as http/protobuf only, not %s", protocol)
    if otlp_wanted("traces"):
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )

        tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    if otlp_wanted("metrics"):
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )

        metric_exporters.append(OTLPMetricExporter())
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(exporter) for exporter in metric_exporters
        ],
    )
    _adopt(tracer_provider, meter_provider)


def _adopt(tracer_provider: Any, meter_provider: Any) -> None:
    from toil.version import version

    _STATE.tracer_provider = tracer_provider
    _STATE.meter_provider = meter_provider
    _STATE.tracer = tracer_provider.get_tracer(SCOPE, version)
    _STATE.meter = meter_provider.get_meter(SCOPE, version)
    _STATE.instruments = {}
    _observe_process(_STATE.meter)


def use_providers(tracer_provider: Any, meter_provider: Any) -> None:
    """Use providers built elsewhere, such as a test's, instead of configuring."""
    with _LOCK:
        _adopt(tracer_provider, meter_provider)


def _observe_process(meter: Any) -> None:
    from opentelemetry.metrics import CallbackOptions, Observation

    process = psutil.Process()

    def cpu_time(options: CallbackOptions) -> Iterator[Observation]:
        times = process.cpu_times()
        yield Observation(times.user, {"cpu.mode": "user"})
        yield Observation(times.system, {"cpu.mode": "system"})

    def memory(options: CallbackOptions) -> Iterator[Observation]:
        yield Observation(process.memory_info().rss)

    def virtual_memory(options: CallbackOptions) -> Iterator[Observation]:
        yield Observation(process.memory_info().vms)

    meter.create_observable_counter("process.cpu.time", [cpu_time], unit="s")
    meter.create_observable_up_down_counter("process.memory.usage", [memory], unit="By")
    meter.create_observable_up_down_counter(
        "process.memory.virtual", [virtual_memory], unit="By"
    )


def enabled() -> bool:
    """Return whether telemetry is on in this process."""
    return _STATE.tracer is not None


def tracer() -> Any:
    """Get Toil's tracer, or a no-op one when telemetry is off."""
    return _STATE.tracer if _STATE.tracer is not None else _NOOP_TRACER


def counter(name: str, unit: str = "{job}") -> Any:
    """Get the named counter, or a no-op one when telemetry is off."""
    if _STATE.meter is None:
        return _NOOP_INSTRUMENT
    with _LOCK:
        if name not in _STATE.instruments:
            _STATE.instruments[name] = _STATE.meter.create_counter(name, unit=unit)
        return _STATE.instruments[name]


def duration_histogram(name: str) -> Any:
    """Get the named histogram of seconds, or a no-op one when telemetry is off."""
    if _STATE.meter is None:
        return _NOOP_INSTRUMENT
    with _LOCK:
        if name not in _STATE.instruments:
            _STATE.instruments[name] = _STATE.meter.create_histogram(
                name,
                unit="s",
                explicit_bucket_boundaries_advisory=list(DURATION_BUCKETS),
            )
        return _STATE.instruments[name]


def shutdown() -> None:
    """Export everything recorded and turn telemetry off."""
    with _LOCK:
        for provider in (_STATE.tracer_provider, _STATE.meter_provider):
            if provider is not None:
                provider.shutdown()
        _STATE.tracer_provider = _STATE.meter_provider = None
        _STATE.tracer = _STATE.meter = None
        _STATE.instruments = {}


def inherited_context() -> Any:
    """Get the trace context this process got as ``TRACEPARENT``."""
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    carrier = {
        field: os.environ[variable]
        for field, variable in TRACE_CONTEXT_VARIABLES.items()
        if os.environ.get(variable)
    }
    return TraceContextTextMapPropagator().extract(carrier)


def trace_context_variables(span: Any) -> dict[str, str]:
    """
    Get the ``TRACEPARENT`` and ``TRACESTATE`` that make ``span`` a process's
    parent.
    """
    from opentelemetry.trace import set_span_in_context
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier, set_span_in_context(span))
    return {
        variable: carrier[field]
        for field, variable in TRACE_CONTEXT_VARIABLES.items()
        if field in carrier
    }
