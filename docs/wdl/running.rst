.. _runWdl:

Running WDL with Toil
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

    toil-wdl-runner myWorkflow.wdl --inputs myWorkflow_inputs.json

This will default to executing on the current machine, with a job store in an
automatically determined temporary location, but you can add a few Toil options
to use other Toil-supported batch systems, such as Kubernetes::

    toil-wdl-runner --jobStore aws:us-west-2:wdl-job-store --batchSystem kubernetes myWorkflow.wdl --inputs myWorkflow_inputs.json

For Toil, the ``--inputs`` is optional, and inputs can be passed as a positional
argument::

    toil-wdl-runner myWorkflow.wdl myWorkflow_inputs.json

You can also run workflows from URLs. For example, to run the MiniWDL self test
workflow, you can do::

    toil-wdl-runner https://raw.githubusercontent.com/DataBiosphere/toil/36b54c45e8554ded5093bcdd03edb2f6b0d93887/src/toil/test/wdl/miniwdl_self_test/self_test.wdl https://raw.githubusercontent.com/DataBiosphere/toil/36b54c45e8554ded5093bcdd03edb2f6b0d93887/src/toil/test/wdl/miniwdl_self_test/inputs.json

.. _`5.12.0`: https://github.com/DataBiosphere/toil/releases/tag/releases%2F5.12.0
.. _`Cromwell`: https://github.com/broadinstitute/cromwell#readme

Toil WDL Runner Options
-----------------------

``--jobStore``: Specifies where to keep the Toil state information while
running the workflow. Must be accessible from all machines.

``-o`` or ``--outputDirectory``: Specifies the output folder or URI prefix to
save workflow output files in. Defaults to a new directory in the current
directory.

``-m`` or ``--outputFile``: Specifies a JSON file name or URI to save workflow
output values at. Defaults to standard output.

``-i`` or ``--input``: Alternative to the positional argument for the
input JSON file, for compatibility with other WDL runners.

``--outputDialect``: Specifies an output format dialect. Can be
``cromwell`` to just return the workflow's output values as JSON or ``miniwdl``
to nest that under an ``outputs`` key and includes a ``dir`` key.

Any number of other Toil options may also be specified. For defined Toil options,
see :ref:`commandRef`.




