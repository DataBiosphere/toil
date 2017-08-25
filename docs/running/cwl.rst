.. _cwl:

CWL in Toil
===========

The Common Workflow Language (CWL) is an emerging standard for writing workflows
that are portable across multiple workflow engines and platforms.

Toil has nearly full support for the v1.0.1 specification, only lacking the
following feature:

- Complex file inputs – from ExpressionTool or a default value, both of which do
  not yet get cleanly staged into Toil file management.

Running CWL Locally
-------------------

To run in local batch mode, provide the CWL file and the input object file::

    $ cwltoil example.cwl example-job.yml

For a simple example of CWL with Toil see :ref:`cwlquickstart`.

Running CWL in the Cloud
------------------------

To run in cloud and HPC configurations, you may need to provide additional
command line parameters to select and configure the batch system to use.

To run a CWL workflow in AWS with toil see :ref:`awscwl`.

.. _File literals: http://www.commonwl.org/v1.0/CommandLineTool.html#File
.. _Directory: http://www.commonwl.org/v1.0/CommandLineTool.html#Directory
.. _secondaryFiles: http://www.commonwl.org/v1.0/CommandLineTool.html#CommandInputParameter
.. _InitialWorkDirRequirement: http://www.commonwl.org/v1.0/CommandLineTool.html#InitialWorkDirRequirement
