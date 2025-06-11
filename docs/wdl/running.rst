.. _runWdl:

Running WDL Workflows
=====================

Toil has beta support for running WDL workflows, using the ``toil-wdl-runner``
command. This command comes with the ``[wdl]`` extra; see :ref:`extras` for how
to install it if you do not have it.

You can run WDL workflows with ``toil-wdl-runner``. Currently,
``toil-wdl-runner`` works by using MiniWDL_ to parse and interpret the WDL
workflow, and has support for workflows in WDL 1.0 or later (which are required
to declare a ``version``, and which use ``inputs`` and ``outputs`` sections).

.. _`MiniWDL`: https://github.com/chanzuckerberg/miniwdl/#miniwdl

.. tip::
   The last release of Toil that supported unversioned, ``draft-2`` WDL workflows was `5.12.0`_.

Toil is, for compatible workflows, a drop-in replacement for the `Cromwell`_ WDL runner.
Instead of running a workflow with Cromwell::

    java -jar Cromwell.jar run myWorkflow.wdl --inputs myWorkflow_inputs.json

You can run the workflow with ``toil-wdl-runner``::

    toil-wdl-runner myWorkflow.wdl --input myWorkflow_inputs.json

(We're here running Toil with ``--input``, but it can also accept the
Cromwell-style ``--inputs``.)

This will default to executing on the current machine, with a job store in an
automatically determined temporary location, but you can add a few Toil options
to use other Toil-supported batch systems, such as Kubernetes::

    toil-wdl-runner --jobStore aws:us-west-2:wdl-job-store --batchSystem kubernetes myWorkflow.wdl --input myWorkflow_inputs.json

For Toil, the ``--input`` is optional, and inputs can be passed as a positional
argument::

    toil-wdl-runner myWorkflow.wdl myWorkflow_inputs.json

You can also run workflows from URLs. For example, to run the MiniWDL self test
workflow, you can do::

    toil-wdl-runner https://raw.githubusercontent.com/DataBiosphere/toil/36b54c45e8554ded5093bcdd03edb2f6b0d93887/src/toil/test/wdl/miniwdl_self_test/self_test.wdl https://raw.githubusercontent.com/DataBiosphere/toil/36b54c45e8554ded5093bcdd03edb2f6b0d93887/src/toil/test/wdl/miniwdl_self_test/inputs.json

.. _`5.12.0`: https://github.com/DataBiosphere/toil/releases/tag/releases%2F5.12.0
.. _`Cromwell`: https://github.com/broadinstitute/cromwell#readme

.. _wdlOptions:

Toil WDL Runner Options
-----------------------

``--jobStore``: Specifies where to keep the Toil state information while
running the workflow. Must be accessible from all machines.

``-o`` or ``--outputDirectory``: Specifies the output folder or URI prefix to
save workflow output files in. Defaults to a new directory in the current
directory.

``-m`` or ``--outputFile``: Specifies a JSON file name or URI to save workflow
output values at. Defaults to standard output.

``-i``, ``--input``, or ``--inputs``: Alternative to the positional argument for the
input JSON file, for compatibility with other WDL runners.

``--outputDialect``: Specifies an output format dialect. Can be
``cromwell`` to just return the workflow's output values as JSON or ``miniwdl``
to nest that under an ``outputs`` key and includes a ``dir`` key.

``--referenceInputs``: Specifies whether input files to Toil should be passed
around by URL reference instead of being imported into Toil's storage. Defaults
to off. Can be ``True`` or ``False`` or other similar words.

``--container``: Specifies the container engine to use to run tasks. By default
this is ``auto``, which tries Singularity if it is installed and Docker if it
isn't. Can also be set to ``docker`` or ``singularity`` explicitly.

``--allCallOutputs``: Specifies whether outputs from all calls in a workflow
should be included alongside the outputs from the ``output`` section, when an
``output`` section is defined. For strict WDL spec compliance, should be set to
``False``. Usually defaults to ``False``. If the workflow includes metadata for
the `Cromwell Output Organizer (croo)`_, will default to ``True``.

.. _`Cromwell Output Organizer (croo)`: https://github.com/ENCODE-DCC/croo

``--strict``: Specifies whether Toil should immediately exit on a lint warning. By default, this is false.

``--quantCheck``: Specifies whether quantifier validation type checking should be enabled.
Disabling this is useful for workflows which Cromwell can run but don't fully comply with the WDL spec.
By default, this is true.

``--runImportsOnWorkers``: Run file imports on workers. This is useful if the leader is not network optimized
and lots of downloads are necessary. By default, this is false.

``--importWorkersBatchSize``: Requires ``--runImportsOnWorkers`` to be true. Specify the target batch size in bytes for batched imports.
As many files as can fit will go into each batch import job. This also accepts abbreviations, such as ``G`` or ``Gi``.

``--importWorkersDisk``: Requires ``--runImportsOnWorkers`` to be true. Specify the disk size each import worker will get.
This usually will not need to be set as Toil will attempt to use file streaming when downloading files.
If not possible, for example, when downloading from AWS to a GCE job store,
this should be set to the largest file size of all files to import. By default, this is 1 MiB.

Any number of other Toil options may also be specified. For defined Toil options,
see :ref:`commandRef`.

.. _logging:

Managing Workflow Logs
----------------------

At the default settings, if a WDL task succeeds, the standard output and
standard error will be printed in the ``toil-wdl-runner`` output, unless they
are captured by the workflow (with the ``stdout()`` and ``stderr()`` WDL
built-in functions). If a WDL task fails, they will be printed whether they
were meant to be captured or not. Complete logs from Toil for failed jobs will
also be printed.

If you would like to save the logs organized by WDL task, you can use the
``--writeLogs`` or ``--writeLogsGzip`` options to specify a directory where the
log files should be saved. Log files will be named after the same dotted,
hierarchical workflow and task names used to set values from the input JSON,
except that scatters will add an additional numerical component. In addition
to the logs for WDL tasks, Toil job logs for failed jobs will also appear here
when running at the default log level.

For example, if you run::

    toil-wdl-runner --writeLogs logs https://raw.githubusercontent.com/DataBiosphere/toil/36b54c45e8554ded5093bcdd03edb2f6b0d93887/src/toil/test/wdl/miniwdl_self_test/self_test.wdl https://raw.githubusercontent.com/DataBiosphere/toil/36b54c45e8554ded5093bcdd03edb2f6b0d93887/src/toil/test/wdl/miniwdl_self_test/inputs.json

You will end up with a ``logs/`` directory containing::

    hello_caller.0.hello.stderr_000.log
    hello_caller.1.hello.stderr_000.log
    hello_caller.2.hello.stderr_000.log

The final number is a sequential counter: if a step has to be retried, or if
you run the workflow multiple times without clearing out the logs directory, it
will increment.

Enabling WDL Call Cache
-----------------------

Toil can cache the task and workflow outputs to use outputs of already ran tasks and workflows.
This can save time when debugging long running workflows where a later task fails. However, this is only guaranteed for
running locally and can use up a considerable amount of disk space.

To use, set the following environment variables before running the workflow::

    export MINIWDL__CALL_CACHE__PUT=True
    export MINIWDL__CALL_CACHE__GET=True
    export MINIWDL__CALL_CACHE__DIR=/absolute_path/to/cache

The path to the cache directory must be an absolute path.

For setting up call cache permanently, see the `MiniWDL call cache`_ documentation.

.. _`MiniWDL call cache`:https://miniwdl.readthedocs.io/en/latest/runner_reference.html#call-cache