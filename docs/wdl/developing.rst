.. _devWdl:

Developing a WDL Workflow
=========================

Toil can be used as a development tool for writing and locally testing WDL
workflows. These workflows can then be run on Toil against a cloud or cluster
backend, or used with other WDL implementations such as `Terra`_, `Cromwell`_,
or `MiniWDL`_.

.. _`Terra`: https://support.terra.bio/hc/en-us/sections/360004147011-Workflows
.. _`Cromwell`: https://github.com/broadinstitute/cromwell#readme
.. _`MiniWDL`: https://github.com/chanzuckerberg/miniwdl/#miniwdl

The easiest way to get started with writing WDL workflows is by following a tutorial.

Using the UCSC Genomics Institute Tutorial
------------------------------------------

The UCSC Genomics Institute (home of the Toil project) has `a tutorial on writing WDL workflows with Toil`_.
You can follow this tutorial to be walked through writing your own WDL workflow
with Toil. They also have `tips on debugging WDL workflows with Toil`_.

These tutorials and tips are aimed at users looking to run WDL workflows with
Toil in a Slurm environment, but they can also apply in other situations.

.. _`a tutorial on writing WDL workflows with Toil`: https://giwiki.gi.ucsc.edu/index.php?title=Phoenix_WDL_Tutorial#Writing_your_own_workflow
.. _`tips on debugging WDL workflows with Toil`: https://giwiki.gi.ucsc.edu/index.php?title=Phoenix_WDL_Tutorial#Debugging_Workflows

Using the Official WDL tutorials
--------------------------------

You can also learn to write WDL workflows for Toil by following the `official WDL tutorials`_.

When you reach the point of `executing your workflow`_, instead of running with
Cromwell::

    java -jar Cromwell.jar run myWorkflow.wdl --inputs myWorkflow_inputs.json

you can instead run with ``toil-wdl-runner``::

    toil-wdl-runner myWorkflow.wdl --inputs myWorkflow_inputs.json

.. _`official WDL tutorials`: https://wdl-docs.readthedocs.io/en/stable/
.. _`executing your workflow`: https://wdl-docs.readthedocs.io/en/stable/WDL/execute/

Using the Learn WDL Video Tutorials
-----------------------------------

For people who prefer video tutorials, Lynn Langit has a `Learn WDL video course`_
that will teach you how to write and run WDL workflows. The course is taught
using Cromwell, but Toil should also be compatible with the course's workflows.

.. _`Learn WDL video course`: https://www.youtube.com/playlist?list=PL4Q4HssKcxYv5syJKUKRrD8Fbd-_CnxTM

WDL Specifications
------------------
WDL language specifications can be found here: https://github.com/openwdl/wdl/blob/main/versions/1.1/SPEC.md

Toil is not yet fully conformant with the WDL specification, but it inherits most of the functionality of `MiniWDL`_.

.. _`MiniWDL`: https://github.com/chanzuckerberg/miniwdl/#miniwdl

