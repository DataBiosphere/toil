.. _hpcEnvironmentsOverview:

HPC Environments
================

Toil is a flexible framework that can be leveraged in a variety of environments, including high-performance computing (HPC) environments.
Toil provides support for a number of batch systems, including `Grid Engine`_, `Slurm`_, `Torque`_ and `LSF`_, which are popular schedulers used in these environments.
Toil also supports `HTCondor`_, which is a popular scheduler for high-throughput computing (HTC).
To use one of these batch systems specify the "-\\-batchSystem" argument to the toil script.

Due to the cost and complexity of maintaining support for these schedulers we currently consider them to be "community supported", that is the core development team does not regularly test or develop support for these systems. However, there are members of the Toil community currently deploying Toil in HPC environments and we welcome external contributions.

Developing the support of a new or existing batch system involves extending the abstract batch system class :class:`toil.batchSystems.abstractBatchSystem.AbstractBatchSystem`.

Standard Output/Error from Batch System Jobs
--------------------------------------------

Standard output and error from batch system jobs (except for the Parasol and Mesos batch systems) are redirected to files in the ``toil-<workflowID>`` directory created within the temporary directory specified by the ``--workDir`` option; see :ref:`optionsRef`.
Each file is named as follows: ``toil_job_<Toil job ID>_batch_<name of batch system>_<job ID from batch system>_<file description>.log``, where ``<file description>`` is ``std_output`` for standard output, and ``std_error`` for standard error.
HTCondor will also write job event log files with ``<file description> = job_events``.

If capturing standard output and error is desired, ``--workDir`` will generally need to be on a shared file system; otherwise if these are written to local temporary directories on each node (e.g. ``/tmp``) Toil will not be able to retrieve them.
Alternatively, the ``--noStdOutErr`` option forces Toil to discard all standard output and error from batch system jobs.

.. _Grid Engine: http://www.univa.com/oracle

.. _Slurm: https://www.schedmd.com/

.. _Torque: http://www.adaptivecomputing.com/products/open-source/torque/

.. _LSF: https://en.wikipedia.org/wiki/Platform_LSF

.. _HTCondor: https://research.cs.wisc.edu/htcondor/
