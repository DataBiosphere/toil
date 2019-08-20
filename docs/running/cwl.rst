.. _cwl:

CWL in Toil
===========

The Common Workflow Language (CWL) is an emerging standard for writing workflows
that are portable across multiple workflow engines and platforms.
Toil has full support for the CWL v1.0.1 specification.

Running CWL Locally
-------------------

To run in local batch mode, provide the CWL file and the input object file::

    $ toil-cwl-runner example.cwl example-job.yml

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

Running CWL within Toil Scripts
------------------------------------

A CWL workflow can be run indirectly in a native Toil script. However, this is not the :ref:`standard <cwl>` way to run
CWL workflows with Toil and doing so comes at the cost of job efficiency. For some use cases, such as running one process on
multiple files, it may be useful. For example, if you want to run a CWL workflow with 3 YML files specifying different
samples inputs, it could look something like:

.. literalinclude:: ../../src/toil/test/docs/scripts/tutorial_cwlexample.py
