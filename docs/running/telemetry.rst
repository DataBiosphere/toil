.. _telemetry:

OpenTelemetry
=============

With the ``otel`` extra, Toil records `OpenTelemetry`_ traces and metrics of
the leader and its workers when the environment turns them on:

- ``OTEL_TRACES_EXPORTER=otlp``, ``OTEL_METRICS_EXPORTER=otlp``, or an
  ``OTEL_EXPORTER_OTLP_ENDPOINT`` sends them over OTLP/HTTP.
- ``TOIL_OTEL_FILE_DIR`` writes each process's spans and metrics as OTLP/JSON
  lines files in that directory.
- ``OTEL_SDK_DISABLED=true`` turns them off.

Workers inherit these settings from the leader. ``service.name`` is
``toil-leader`` or ``toil-worker``.

.. _OpenTelemetry: https://opentelemetry.io/

Spans
-----

``workflow.run``
    The leader's run, under any inherited ``TRACEPARENT``.

``job.attempt``
    Each batch job, with ``issued``, ``submitted``, ``started``, ``ended``,
    ``retried`` and ``refused`` events. Attributes: ``job.name``,
    ``step.id``, ``step.scatter.index`` and ``step.loop.iteration`` (CWL),
    ``job.attempt``, ``batch.system``, ``batch.queue``, ``batch.job.id``,
    ``request.cpu``, ``request.memory``, ``request.gpu``,
    ``request.timelimit``, ``process.exit.code``, ``job.exit.reason`` and
    ``outcome``.

``job.process``
    The worker, which gets its ``job.attempt`` as ``TRACEPARENT``.

``job.run``
    Each Toil job the worker runs. The job's own processes get it as
    ``TRACEPARENT``.

The leader learns a job's start from how long the batch system says it has
run, polled at most once per ``--statePollingWait``, so ``started`` is
approximate.

Metrics
-------

====================================  =========  =================================================
Name                                  Unit       Measures
====================================  =========  =================================================
``workflow.jobs.issued``              ``{job}``  Batch jobs issued
``workflow.jobs.retried``             ``{job}``  Failed batch jobs that run again
``workflow.jobs.refused``             ``{job}``  Refused submissions, by ``refusal.reason``
``workflow.jobs.failed``              ``{job}``  Failed batch jobs that don't run again
``workflow.job.dispatch_gap``         ``s``      Upstream batch job ended to job issued
``workflow.job.submit_delay``         ``s``      Issued to submitted
``workflow.job.queue_wait``           ``s``      Submitted to started
``workflow.job.wrapper_overhead``     ``s``      Worker time outside its jobs
``process.cpu.time``                  ``s``      CPU time of each Toil process
``process.memory.usage``              ``By``     Resident memory of each Toil process
``process.memory.virtual``            ``By``     Virtual memory of each Toil process
====================================  =========  =================================================

Labels are ``batch.system``, ``batch.queue``, ``refusal.reason``
(``submit_limit`` or ``other``), ``job.exit.reason`` and ``outcome``, never
a run, job or step id.
