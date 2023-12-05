.. _wdl:

WDL in Toil
***********

Toil has beta support for running WDL workflows, using the ``toil-wdl-runner``
command.

Running WDL with Toil
---------------------

You can run WDL workflows with ``toil-wdl-runner``. Currently,
``toil-wdl-runner`` works by using MiniWDL_ to parse and interpret the WDL
workflow, and has support for workflows in WDL 1.0 or later (which are required
to declare a ``version``, and which use ``inputs`` and ``outputs`` sections).

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

Writing WDL with Toil
---------------------

Toil can be used as a development tool for writing and locally testing WDL
workflows. These workflows can then be run on Toil against a cloud or cluster
backend, or used with other WDL implementations such as `Terra`_, `Cromwell`_,
or `MiniWDL`_.

.. _`Terra`: https://support.terra.bio/hc/en-us/sections/360004147011-Workflows
.. _`Cromwell`: https://github.com/broadinstitute/cromwell#readme
.. _`MiniWDL`: https://github.com/chanzuckerberg/miniwdl/#miniwdl

The easiest way to get started with writing WDL workflows is by following a tutorial.

Using the UCSC Genomics Institute Tutorial
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The UCSC Genomics Institute (home of the Toil project) has `a tutorial on writing WDL workflows with Toil`_.
You can follow this tutorial to be walked through writing your own WDL workflow
with Toil. They also have `tips on debugging WDL workflows with Toil`_.

These tutorials and tips are aimed at users looking to run WDL workflows with
Toil in a Slurm environment, but they can also apply in other situations.

.. _`a tutorial on writing WDL workflows with Toil`: https://giwiki.gi.ucsc.edu/index.php?title=Phoenix_WDL_Tutorial#Writing_your_own_workflow
.. _`tips on debugging WDL workflows with Toil`: https://giwiki.gi.ucsc.edu/index.php?title=Phoenix_WDL_Tutorial#Debugging_Workflows

Using the Official WDL tutorials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also learn to write WDL workflows for Toil by following the `official WDL tutorials`_.

When you reach the point of `executing your workflow`_, instead of running with
Cromwell::

    java -jar Cromwell.jar run myWorkflow.wdl --inputs myWorkflow_inputs.json

you can instead run with ``toil-wdl-runner``::

    toil-wdl-runner myWorkflow.wdl --inputs myWorkflow_inputs.json

.. _`official WDL tutorials`: https://wdl-docs.readthedocs.io/en/stable/
.. _`executing your workflow`: https://wdl-docs.readthedocs.io/en/stable/WDL/execute/

Using the Learn WDL Video Tutorials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For people who prefer video tutorials, Lynn Langit has a `Learn WDL video course`_
that will teach you how to write and run WDL workflows. The course is taught
using Cromwell, but Toil should also be compatible with the course's workflows.

.. _`Learn WDL video course`: https://www.youtube.com/playlist?list=PL4Q4HssKcxYv5syJKUKRrD8Fbd-_CnxTM

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


WDL Specifications
------------------
WDL language specifications can be found here: https://github.com/openwdl/wdl/blob/main/versions/1.1/SPEC.md

Toil is not yet fully conformant with the WDL specification, but it inherits most of the functionality of `MiniWDL`_.

.. _`MiniWDL`: https://github.com/chanzuckerberg/miniwdl/#miniwdl


