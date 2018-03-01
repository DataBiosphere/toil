.. _runningOverview:

Overview
========
This section describes how to run Toil in various environments, including :ref:`locally <fileJobStore>`,
:ref:`in the cloud <cloudOverview>`, and using :ref:`CWL <cwl>`.

Toil is built in a modular way so that it can be used on lots of different systems and with different configurations.
The three configurable pieces are the

 - :ref:`jobStoreOverview`
 - :ref:`batchsystemOverview` and
 - :ref:`provisionerOverview`

Specifically, the running Toil section documents detail for the following:

.. _jobStoreOverview:

Job Store
---------

The job store is a storage abstraction which contains all of the information used in a Toil run. This includes all
of the files used by jobs in the workflow and also the details of the progress of the run. If a workflow crashes
or fails, the job store contains all of the information necessary to resume with minimal repetition of work.

Several different job stores are supported.

.. _fileJobStore:

File Job Store
~~~~~~~~~~~~~~

The file job store keeps the workflow information in a directory on the machine where the workflow is launched.
This is the simplest and most convenient job store for testing or for small runs.

For an example that uses the file job store, see :ref:`quickstart`.

Cloud Job Stores
~~~~~~~~~~~~~~~~

Toil also supports using different cloud storage systems as the job store. Currently

 - :ref:`awsJobStore`
 - :ref:`azureJobStore` and
 - Google are supported

.. FIXME, ADD LINK TO GOOG JOBSTORE INSTRUCTIONS

are supported. These use cloud buckets to house all of the files. This is useful if there are several different
worker machines all running jobs that need to access the job store.

.. _batchSystemOverview:

Batch System
------------

Toil supports several different batch systems which can manage the workers if there are multiple and will schedule
jobs. See :ref:`batchsysteminterface` for a more detailed description of different batch systems.

.. _provisionerOverview:

Provisioner
-----------

The Toil provisioner provides a tool set for running a Toil workflow on a particular cloud platform.

The :ref:`clusterRef` are command line tools used to provision nodes in your desired cloud platform.
They allows you to launch nodes, ssh to the leader, and rsync files back and forth.

For detailed instructions for using the provisioner see :ref:`runningAWS` or :ref:`runningGCE`. An
Azure provisioner is in the works and coming soon. For more details see the `Azure provisioner github ticket`_.

.. _Azure provisioner github ticket: https://github.com/BD2KGenomics/toil/pull/1912

