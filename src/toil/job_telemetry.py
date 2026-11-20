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
"""Spans and metrics of a workflow's jobs, from the leader and from workers."""
from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import psutil

from toil.lib import telemetry

if TYPE_CHECKING:
    from toil.batchSystems.abstractBatchSystem import BatchJobExitReason
    from toil.job import JobDescription

LEADER_SERVICE = "toil-leader"
WORKER_SERVICE = "toil-worker"

workflow_attributes: dict[str, Any] = {}
"""
Extra ``workflow.run`` attributes a workflow runner sets, such as
``workflow.uri``.
"""


def _nanoseconds(seconds: float) -> int:
    return int(seconds * 1e9)


def outcome(exit_code: int, exit_reason: BatchJobExitReason | None) -> str:
    """Get how a job or run ended: ``success``, ``failed`` or ``cancelled``."""
    from toil.batchSystems.abstractBatchSystem import BatchJobExitReason

    if exit_code == 0:
        return "success"
    if exit_reason == BatchJobExitReason.KILLED:
        return "cancelled"
    return "failed"


def _job_attributes(job: JobDescription) -> dict[str, Any]:
    """Get span attributes for a job's name and the resources it requests."""
    attributes: dict[str, Any] = {"job.name": job.jobName}

    def request(name: str, get: Callable[[], Any]) -> None:
        try:
            value = get()
        except AttributeError:
            return
        if value:
            attributes[name] = value

    request("request.cpu", lambda: float(job.cores))
    request("request.memory", lambda: int(job.memory))
    request(
        "request.gpu",
        lambda: sum(
            accelerator["count"]
            for accelerator in job.accelerators
            if accelerator["kind"] == "gpu"
        ),
    )
    request("request.timelimit", lambda: int(job.walltime))
    attributes.update(getattr(job, "telemetry_attributes", {}))
    return attributes


class _Attempt:
    """One issued batch job."""

    def __init__(
        self, span: Any, job_store_id: str, labels: dict[str, str], issued: float
    ) -> None:
        self.span = span
        self.job_store_id = job_store_id
        self.labels = labels
        self.issued = issued
        # sbatch --export and qsub -v split on commas.
        self.environment = {
            name: value
            for name, value in telemetry.trace_context_variables(span).items()
            if "," not in value
        }
        self.submitted: float | None = None
        self.started: float | None = None


class LeaderTelemetry:
    """The leader's spans and metrics for one run."""

    def __init__(self, batch_system: str, attributes: Mapping[str, Any]) -> None:
        self._batch_system = batch_system
        self._lock = threading.Lock()
        self._attempts: dict[int, _Attempt] = {}
        self._early: dict[int, list[Callable[[], None]]] = {}
        self._failures: dict[str, int] = {}
        self._last_ended: float | None = None
        self._upstream_ended: dict[str, float] = {}
        self.run = telemetry.tracer().start_span(
            "workflow.run",
            context=telemetry.inherited_context(),
            attributes=dict(attributes),
        )

    def job_issuing(
        self, job: JobDescription, batch_system: str | None = None
    ) -> _Attempt:
        """
        Start a job's span before it is issued.

        :param batch_system: The batch system running the job, if not the workflow's.
        :return: The attempt, whose ``environment`` carries the job's ``TRACEPARENT``.
        """
        from opentelemetry.trace import set_span_in_context

        system = batch_system or self._batch_system
        job_store_id = str(job.jobStoreID)
        now = time.time()
        with self._lock:
            attempt_number = self._failures.get(job_store_id, 0) + 1
            upstream_ended = self._upstream_ended.pop(job_store_id, None)
        attributes = _job_attributes(job)
        attributes.update({"job.attempt": attempt_number, "batch.system": system})
        span = telemetry.tracer().start_span(
            "job.attempt",
            context=set_span_in_context(self.run),
            attributes=attributes,
            start_time=_nanoseconds(now),
        )
        span.add_event("issued", timestamp=_nanoseconds(now))
        attempt = _Attempt(span, job_store_id, {"batch.system": system}, now)
        telemetry.counter("workflow.jobs.issued").add(1, attempt.labels)
        if upstream_ended is not None:
            telemetry.duration_histogram("workflow.job.dispatch_gap").record(
                max(0.0, now - upstream_ended), attempt.labels
            )
        return attempt

    def job_ready(self, job_store_id: str) -> None:
        """Record that a job is ready to issue, as of the last batch job end."""
        with self._lock:
            if self._last_ended is not None:
                self._upstream_ended[job_store_id] = self._last_ended

    def job_issued(self, batch_system_id: int, attempt: _Attempt) -> None:
        """Track an attempt by its batch system ID."""
        attempt.span.set_attribute("toil.job.batch_system_id", batch_system_id)
        with self._lock:
            self._attempts[batch_system_id] = attempt
            # Reports can arrive from the batch system's thread before this.
            early = self._early.pop(batch_system_id, [])
        for report in early:
            report()

    def _attempt(self, batch_system_id: int, report: Callable[[], None]) -> _Attempt | None:
        """
        Get the attempt with this ID, or keep ``report`` for when it is issued
        and return None.
        """
        with self._lock:
            attempt = self._attempts.get(batch_system_id)
            if attempt is None:
                self._early.setdefault(batch_system_id, []).append(report)
            return attempt

    def job_submitted(
        self, batch_system_id: int, scheduler_job_id: str, queue: str | None
    ) -> None:
        """Record that the scheduler accepted a job."""
        now = time.time()

        def report() -> None:
            with self._lock:
                attempt = self._attempts.get(batch_system_id)
                if attempt is None or attempt.submitted is not None:
                    return
                attempt.submitted = now
                if queue:
                    attempt.labels["batch.queue"] = queue
            attempt.span.set_attribute("batch.job.id", scheduler_job_id)
            if queue:
                attempt.span.set_attribute("batch.queue", queue)
            attempt.span.add_event("submitted", timestamp=_nanoseconds(now))
            telemetry.duration_histogram("workflow.job.submit_delay").record(
                max(0.0, now - attempt.issued), attempt.labels
            )

        if self._attempt(batch_system_id, report) is not None:
            report()

    def job_refused(
        self, batch_system_id: int, reason: str, detail: str, queue: str | None
    ) -> None:
        """
        Record a refused submission.

        :param reason: ``submit_limit`` or ``other``.
        """
        now = time.time()

        def report() -> None:
            attempt = self._attempts[batch_system_id]
            if queue:
                attempt.labels["batch.queue"] = queue
                attempt.span.set_attribute("batch.queue", queue)
            attempt.span.add_event(
                "refused",
                {"refusal.reason": reason, "refusal.detail": detail},
                timestamp=_nanoseconds(now),
            )
            telemetry.counter("workflow.jobs.refused").add(
                1, {**attempt.labels, "refusal.reason": reason}
            )

        if self._attempt(batch_system_id, report) is not None:
            report()

    def jobs_running(self, seconds_running: Mapping[int, float]) -> None:
        """Record the start of each job, given how long it has been running."""
        now = time.time()
        for batch_system_id, seconds in seconds_running.items():
            self._started(batch_system_id, now - seconds)

    def _started(self, batch_system_id: int, when: float) -> None:
        with self._lock:
            attempt = self._attempts.get(batch_system_id)
            if attempt is None or attempt.started is not None:
                return
            attempt.started = when
        attempt.span.set_attribute("started.observed", False)
        attempt.span.add_event("started", timestamp=_nanoseconds(when))
        if attempt.submitted is not None:
            telemetry.duration_histogram("workflow.job.queue_wait").record(
                max(0.0, when - attempt.submitted), attempt.labels
            )

    def job_ended(
        self,
        batch_system_id: int,
        exit_code: int,
        exit_reason: BatchJobExitReason | None,
        wall_time: float | None,
        runs_again: bool,
    ) -> None:
        """
        End a job's span.

        :param runs_again: Whether the job will be issued again.
        """
        from opentelemetry.trace import Status, StatusCode

        now = time.time()
        if wall_time is not None:
            self._started(batch_system_id, now - wall_time)
        with self._lock:
            self._last_ended = now
            attempt = self._attempts.pop(batch_system_id, None)
            if attempt is None:
                return
            if exit_code != 0:
                self._failures[attempt.job_store_id] = (
                    self._failures.get(attempt.job_store_id, 0) + 1
                )
        result = outcome(exit_code, exit_reason)
        span = attempt.span
        span.set_attribute("process.exit.code", exit_code)
        span.set_attribute("outcome", result)
        if exit_reason is not None:
            span.set_attribute("job.exit.reason", exit_reason.name.lower())
        elif exit_code == 0:
            span.set_attribute("job.exit.reason", "finished")
        span.add_event("ended", timestamp=_nanoseconds(now))
        if exit_code != 0:
            labels = dict(attempt.labels)
            if runs_again:
                span.add_event("retried", timestamp=_nanoseconds(now))
                telemetry.counter("workflow.jobs.retried").add(1, labels)
            else:
                if exit_reason is not None:
                    labels["job.exit.reason"] = exit_reason.name.lower()
                labels["outcome"] = result
                telemetry.counter("workflow.jobs.failed").add(1, labels)
            span.set_status(Status(StatusCode.ERROR))
        span.end(end_time=_nanoseconds(now))

    def end(self, result: str) -> None:
        """End the run's span and any open job spans."""
        from opentelemetry.trace import Status, StatusCode

        with self._lock:
            open_attempts = list(self._attempts.values())
            self._attempts.clear()
        for attempt in open_attempts:
            attempt.span.set_attribute("outcome", "cancelled")
            attempt.span.end()
        self.run.set_attribute("outcome", result)
        if result != "success":
            self.run.set_status(Status(StatusCode.ERROR))
        self.run.end()


class WorkerTelemetry:
    """A worker's spans and overhead."""

    def __init__(self, batch_system: str) -> None:
        self._labels = {"batch.system": batch_system}
        self._started = psutil.Process().create_time()
        self._job_seconds = 0.0
        self.span = telemetry.tracer().start_span(
            "job.process",
            context=telemetry.inherited_context(),
            start_time=_nanoseconds(self._started),
        )

    @contextmanager
    def job_run(self, job: JobDescription) -> Iterator[None]:
        """
        Run a job inside a ``job.run`` span, which its processes get as
        ``TRACEPARENT``.
        """
        from opentelemetry.trace import Status, StatusCode, set_span_in_context

        span = telemetry.tracer().start_span(
            "job.run",
            context=set_span_in_context(self.span),
            attributes={
                key: value
                for key, value in _job_attributes(job).items()
                if not key.startswith("request.")
            },
        )
        variables = telemetry.TRACE_CONTEXT_VARIABLES.values()
        saved = {name: os.environ.get(name) for name in variables}
        os.environ.update(telemetry.trace_context_variables(span))
        began = time.time()
        try:
            yield
        except BaseException:
            span.set_status(Status(StatusCode.ERROR))
            raise
        finally:
            self._job_seconds += time.time() - began
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            span.end()

    def end(self, exit_code: int) -> None:
        """End the span and record overhead."""
        from opentelemetry.trace import Status, StatusCode

        now = time.time()
        self.span.set_attribute("process.exit.code", exit_code)
        if exit_code != 0:
            self.span.set_status(Status(StatusCode.ERROR))
        self.span.end(end_time=_nanoseconds(now))
        telemetry.duration_histogram("workflow.job.wrapper_overhead").record(
            max(0.0, now - self._started - self._job_seconds), self._labels
        )


def worker_takes_leader_trace_context(leader_environment: Mapping[str, str]) -> bool:
    """
    Return whether a worker takes ``TRACEPARENT`` and ``TRACESTATE`` from the
    leader's saved environment.

    It doesn't when the leader records telemetry, because the leader then
    gives each job its own.
    """
    return not (telemetry.requested(leader_environment) and telemetry.OTEL_AVAILABLE)


_leader: LeaderTelemetry | None = None
_worker: WorkerTelemetry | None = None


def start_leader(batch_system: str, attributes: Mapping[str, Any]) -> LeaderTelemetry | None:
    """Start the leader's telemetry, if requested."""
    global _leader
    if not telemetry.configure(LEADER_SERVICE):
        return None
    _leader = LeaderTelemetry(batch_system, {**attributes, **workflow_attributes})
    return _leader


def leader() -> LeaderTelemetry | None:
    """Get the leader's telemetry, if it is on."""
    return _leader


def end_leader(result: str) -> None:
    """End the leader's telemetry and export it."""
    global _leader
    if _leader is not None:
        _leader.end(result)
        _leader = None
    workflow_attributes.clear()
    telemetry.shutdown()


def start_worker(batch_system: str) -> WorkerTelemetry | None:
    """Start a worker's telemetry, if requested and not inside the leader."""
    global _worker
    if telemetry.enabled() or not telemetry.configure(WORKER_SERVICE):
        return None
    _worker = WorkerTelemetry(batch_system)
    return _worker


def worker() -> WorkerTelemetry | None:
    """Get the worker's telemetry, if it is on."""
    return _worker


def end_worker(exit_code: int) -> None:
    """End the worker's telemetry and export it."""
    global _worker
    if _worker is not None:
        _worker.end(exit_code)
        _worker = None
        telemetry.shutdown()
