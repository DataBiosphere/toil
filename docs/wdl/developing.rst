.. _devWdl:

Developing WDL Workflows
========================

Toil can be used as a development tool for writing and locally testing WDL
workflows. These workflows can then be run on Toil against a cloud or cluster
backend, or used with other WDL implementations such as `Terra`_, `Cromwell`_,
or `MiniWDL`_.

.. _`Terra`: https://support.terra.bio/hc/en-us/sections/360004147011-Workflows
.. _`Cromwell`: https://github.com/broadinstitute/cromwell#readme
.. _`MiniWDL`: https://github.com/chanzuckerberg/miniwdl/#miniwdl

Learning WDL
------------

To learn to write WDL workflows in general, you should take the :ref:`tutorialWdl`.

There are other tutorials available in the `resources section <resourcesWdl>`__.

.. _debuggingWdl:

Debugging WDL Workflows
-----------------------

When a WDL workflow breaks, many of the `general Toil debugging strategies <debugging>`__, like identifying failed jobs with ``toil status --failed`` or running individual jobs locally with ``toil debug-job``, can be useful. These are easier if you use a manually specified job store, by passing ``--jobStore`` to ``toil-wdl-runner``, so that you have the job store path handy for the debugging tools.

There are also WDL-specific debugging strategies.

Reading WDL Workflow Logs
~~~~~~~~~~~~~~~~~~~~~~~~~

When a WDL workflow fails, you are likely to see a message like this::

    WDL.runtime.error.CommandFailed: task command failed with exit status 1
    [2023-07-16T16:23:54-0700] [MainThread] [E] [toil.worker] Exiting the worker because of a failed job on host phoenix-15.prism

This means that the command line command specified by one of your WDL tasks exited with a failing (i.e. nonzero) exit code, which will happen when either the command line command is written wrong, or when the error detection code in the tool you are trying to run detects and reports an error.

Go up higher in the log until you find lines that look like::

    [2024-01-16T20:12:19-0500] [Thread-3 (statsAndLoggingAggregator)] [I] [toil.statsAndLogging] hello_caller.0.hello.stderr follows:

And::

    [2024-01-16T20:12:19-0500] [Thread-3 (statsAndLoggingAggregator)] [I] [toil.statsAndLogging] hello_caller.0.hello.stdout follows:

These will be followed by the standard error and standard output log data from the task's command. There may be useful information (such as an error message from the underlying tool) in there.

If you would like individual task logs to be saved separately for later reference, you can use the ``--writeLogs`` option to specify a directory to store them. For more information, see :ref:`logging`.

Finding Uploaded Files
~~~~~~~~~~~~~~~~~~~~~~

If you want to find files that were uploaded from a WDL job, look for lines like this in the job's `debug log <debuggingLog>`__::

    [2023-07-16T15:58:39-0700] [MainThread] [D] [toil.wdl.wdltoil] Virtualized /data/tmp/2846b6012e3e5535add03b363950dd78/cb23/197c/work/bamPerChrs/Sample.chr14.bam as WDL file toilfile:2703483274%3A0%3Afiles%2Ffor-job%2Fkind-WDLTaskJob%2Finstance-b4c5x6hq%2Ffile-c4e4f1b16ddf4c2ab92c2868421f3351%2FSample.chr14.bam/Sample.chr14.bam

If you are using a file job store, you can grab that long URI::

    toilfile:2703483274%3A0%3Afiles%2Ffor-job%2Fkind-WDLTaskJob%2Finstance-b4c5x6hq%2Ffile-c4e4f1b16ddf4c2ab92c2868421f3351%2FSample.chr14.bam/Sample.chr14.bam

Then URL-decode it with, for example, https://www.urldecoder.io/, getting this::

    toilfile:2703483274:0:files/for-job/kind-WDLTaskJob/instance-b4c5x6hq/file-c4e4f1b16ddf4c2ab92c2868421f3351/Sample.chr14.bam/Sample.chr14.bam

Then you can take the part after the last colon::

    files/for-job/kind-WDLTaskJob/instance-b4c5x6hq/file-c4e4f1b16ddf4c2ab92c2868421f3351/Sample.chr14.bam/Sample.chr14.bam

That is the path relative to the job store directory where this file can be found.

Another approach would be to use ``find -iname 'Sample.chr14.bam'`` on a file job store, if you know the name the file had when it was uploaded.

Workflow Authoring Tips
-----------------------

Here are some tips for writing WDL workflows. You can also consult the `OpenWDL Cookbook`_.

Deleting Files
~~~~~~~~~~~~~~
WDL doesn't have a built-in way to delete files; if you run a task that deletes a file, it will still exist in Toil's job store storage.

Toil recently gained support for deleting files at the *end* of WDL workflows. If you have a large file that you only need for part of your workflow, consider writing the part that creates and uses it as a separate child ``workflow`` and invoking it with ``call``. Then the file will be cleaned up when the child workflow ends, leaving more space for files created in the parent workflow.

.. _resourcesWdl:

External WDL Resources
----------------------

Here are some other resources a WDL developer might find useful.


Official WDL Tutorials
~~~~~~~~~~~~~~~~~~~~~~

You can also learn to write WDL workflows for Toil by following the `official WDL quickstart guide`_.

Once you have your workflow ``.wdl`` file and your input ``.json``, you can run the workflow on the inputs with ``toil-wdl-runner``::

    toil-wdl-runner myWorkflow.wdl --input myWorkflow_inputs.json

.. _`official WDL quickstart guide`: https://docs.openwdl.org/getting-started/quickstart.html

Learn WDL Video Tutorials
~~~~~~~~~~~~~~~~~~~~~~~~~

For people who prefer video tutorials, Lynn Langit has a `Learn WDL Video Course`_
that will teach you how to write and run WDL workflows. The course is taught
using Cromwell, but Toil should also be compatible with the course's workflows.

.. _`Learn WDL video course`: https://www.youtube.com/playlist?list=PL4Q4HssKcxYv5syJKUKRrD8Fbd-_CnxTM

OpenWDL Cookbook
~~~~~~~~~~~~~~~~

The `OpenWDL Cookbook`_ contains example solutions to a variety of tricky workflow authoring problems.

.. _`OpenWDL Cookbook`: https://github.com/openwdl/cookbook

WDL Specifications
~~~~~~~~~~~~~~~~~~
WDL language specifications can be found here: https://github.com/openwdl/wdl/blob/main/versions/1.1/SPEC.md

Toil is not yet fully conformant with the WDL specification (see :ref:`conformanceWdl`), but it inherits most of the functionality of `MiniWDL`_.

.. _`MiniWDL`: https://github.com/chanzuckerberg/miniwdl/#miniwdl

UCSC Genomics Institute Wiki
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The UCSC Genomics Institute (home of the Toil project) has `tips on debugging
WDL workflows with Toil`_. They are designed for one particular Slurm cluster
environment, but might be applicable elsewhere.

.. _`tips on debugging WDL workflows with Toil`: https://giwiki.gi.ucsc.edu/index.php?title=Phoenix_WDL_Tutorial#Debugging_Workflows

