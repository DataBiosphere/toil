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

Using the Old WDL Compiler
--------------------------

Up through Toil 5.9.2, ``toil-wdl-runner`` worked by compiling the WDL code to
a Toil Python workflow, and executing that. The old compiler is
still available as ``toil-wdl-runner-old``.

The compiler implements:
 * Scatter
 * Many Built-In Functions
 * Docker Calls
 * Handles Priority, and Output File Wrangling
 * Currently Handles Primitives and Arrays

The compiler DOES NOT implement:
 * Robust cloud autoscaling
 * WDL files that ``import`` other WDL files (including URI handling for 'http://' and 'https://')

Recommended best practice when running wdl files with ``toil-wdl-runner-old`` is to first use the Broad's wdltool for syntax validation and generating
the needed json input file.  Full documentation can be found in the repository_, and a precompiled jar binary can be
downloaded here: wdltool_ (this requires java7_).

.. _repository: https://github.com/broadinstitute/wdltool
.. _wdltool: https://github.com/broadinstitute/wdltool/releases
.. _java7: http://www.oracle.com/technetwork/java/javase/downloads/java-archive-downloads-javase7-521261.html

That means two steps.  First, make sure your wdl file is valid and devoid of syntax errors by running::

    java -jar wdltool.jar validate example_wdlfile.wdl

Second, generate a complementary json file if your wdl file needs one.  This json will contain keys for every necessary
input that your wdl file needs to run::

    java -jar wdltool.jar inputs example_wdlfile.wdl

When this json template is generated, open the file, and fill in values as necessary by hand.  WDL files all require
json files to accompany them.  If no variable inputs are needed, a json file containing only '{}' may be required.

Once a wdl file is validated and has an appropriate json file, workflows can be compiled and run using::

    toil-wdl-runner-old example_wdlfile.wdl example_jsonfile.json

Toil WDL Compiler Options
~~~~~~~~~~~~~~~~~~~~~~~~~
``-o`` or ``--outdir``: Specifies the output folder, and defaults to the current working directory if
not specified by the user.

``--dev_mode``: Creates "AST.out", which holds a printed AST of the wdl file and "mappings.out", which holds the
printed task, workflow, csv, and tsv dictionaries generated by the parser. Also saves the compiled toil python workflow
file for debugging.

Any number of arbitrary options may also be specified.  These options will not be parsed immediately, but passed down
as toil options once the wdl/json files are processed.  For valid toil options, see the documentation:
http://toil.readthedocs.io/en/latest/running/cliOptions.html

Compiler Example: ENCODE Example from ENCODE-DCC
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For this example, we will run a WDL draft-2 workflow. This version is too old
to be supported by ``toil-wdl-runner``, so we will need to use
``toil-wdl-runner-old``.

To follow this example, you will need docker installed.  The original workflow can be found here:
https://github.com/ENCODE-DCC/pipeline-container

We've included the wdl file and data files in the toil repository needed to run this example.  First, download
the example code_ and unzip.  The file needed is "testENCODE/encode_mapping_workflow.wdl".

Next, use wdltool_ (this requires java7_) to validate this file::

    java -jar wdltool.jar validate encode_mapping_workflow.wdl

Next, use wdltool to generate a json file for this wdl file::

    java -jar wdltool.jar inputs encode_mapping_workflow.wdl

This json file once opened should look like this::

    {
    "encode_mapping_workflow.fastqs": "Array[File]",
    "encode_mapping_workflow.trimming_parameter": "String",
    "encode_mapping_workflow.reference": "File"
    }

You will need to edit this file to replace the types (like ``Array[File]``) with values of those types.

The trimming_parameter should be set to 'native'.

For the file parameters, download the example data_ and unzip.  Inside are two data files required for the run::

    ENCODE_data/reference/GRCh38_chr21_bwa.tar.gz
    ENCODE_data/ENCFF000VOL_chr21.fq.gz

Editing the json to include these as inputs, the json should now look something like this::

    {
    "encode_mapping_workflow.fastqs": ["/path/to/unzipped/ENCODE_data/ENCFF000VOL_chr21.fq.gz"],
    "encode_mapping_workflow.trimming_parameter": "native",
    "encode_mapping_workflow.reference": "/path/to/unzipped/ENCODE_data/reference/GRCh38_chr21_bwa.tar.gz"
    }

The wdl and json files can now be run using the command::

    toil-wdl-runner-old encode_mapping_workflow.wdl encode_mapping_workflow.json

This should deposit the output files in the user's current working directory (to change this, specify a new directory
with the ``-o`` option).

.. _code: https://toil-datasets.s3.amazonaws.com/wdl_templates.zip
.. _data: https://toil-datasets.s3.amazonaws.com/ENCODE_data.zip

Compiler Example: GATK Examples from the Broad
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Terra hosts some example documentation for using early, pre-1.0 versions of WDL, originally authored by the Broad:
https://support.terra.bio/hc/en-us/sections/360007347652?name=wdl-tutorials

One can follow along with these tutorials, write their own old-style WDL files following the directions and run them using either
Cromwell or Toil's old WDL compiler.  For example, in tutorial 1, if you've followed along and named your wdl file 'helloHaplotypeCall.wdl',
then once you've validated your wdl file using wdltool_ (this requires java7_) using::

    java -jar wdltool.jar validate helloHaplotypeCaller.wdl

and generated a ``json`` file (and subsequently typed in appropriate file paths and variables) using::

    java -jar wdltool.jar inputs helloHaplotypeCaller.wdl

.. note::
        Absolute filepath inputs are recommended for local testing with the Toil WDL compiler.

then the WDL script can be compiled and run using::

    toil-wdl-runner-old helloHaplotypeCaller.wdl helloHaplotypeCaller_inputs.json


